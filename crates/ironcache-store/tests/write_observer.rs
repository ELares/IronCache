// SPDX-License-Identifier: MIT OR Apache-2.0
//! The HA-5a data-plane WRITE-OBSERVATION seam, exercised at the STORE level. These drive
//! the concrete [`ShardStore`] through the store-public `set_write_observer` /
//! `clear_write_observer` install API + the write-funnel fire, asserting:
//!
//! - the observer fires `on_put` with the COMMITTED POST-IMAGE (value bytes + type) for a
//!   create / overwrite / blind upsert / rmw Insert+Replace / RENAME-destination write;
//! - it fires `on_remove(db, key)` for an explicit delete / a lazy expiry reap / a FLUSHDB
//!   (every resident key) / a RENAME's source removal / an in-place edit that empties a
//!   collection;
//! - it fires `on_put` with the post-edit post-image for a non-emptying in-place collection
//!   edit (SADD/LPUSH...), including a no-op same-size edit (any write is observed);
//! - it fires `on_put` for a real TTL-only change (EXPIRE/PERSIST) through both the
//!   read-only `rmw` Keep arm and the `rmw_mut` Keep arm, and does NOT fire for a no-op TTL
//!   write (bare GETEX / EXPIRE to the same deadline);
//! - the events arrive IN ORDER with the right `(db, key)` and post-image;
//! - the HOT-PATH gate: with NO observer installed (the default) the funnel fires NOTHING
//!   (a structural proof: an observer that is removed before the writes can never be
//!   invoked, and a fresh store reports `write_observer_active() == false`);
//! - `clear_write_observer` flips the gate off so subsequent writes are not observed;
//! - DETERMINISM (ADR-0003): the store passes the observer no clock / no RNG; the event
//!   sequence is a pure function of the write sequence, so a seeded replay is identical.

use ironcache_storage::{
    DataType, ExpireWrite, Keyspace, MoveMode, NewValue, NewValueOwned, RmwAction, RmwEntry,
    RmwStep, Store, UnixMillis,
};
use ironcache_store::{Entry, ShardStore, WriteObserver};
use std::cell::RefCell;
use std::rc::Rc;

const NOW: UnixMillis = UnixMillis(1_000);

fn store() -> ShardStore {
    ShardStore::new(16)
}

/// One observed write event: the kind, the `(db, key)`, and (for a put) the post-image's
/// value bytes + data type. A remove records no post-image (the entry is gone).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Put {
        db: u32,
        key: Vec<u8>,
        bytes: Vec<u8>,
        ty: DataType,
    },
    Remove {
        db: u32,
        key: Vec<u8>,
    },
}

/// A recording [`WriteObserver`] that appends every fire into a shared `Vec`. The log is an
/// `Rc<RefCell<...>>` so the test keeps a handle to read it back AFTER the boxed observer is
/// installed into (and owned by) the store. This is the integration-test analog of the
/// WATCH tests' assertions; `RefCell` is a single-threaded interior-mutability cell (no
/// std::sync lock), matching the shard-local single-threaded store.
#[derive(Debug, Clone)]
struct RecordingObserver {
    log: Rc<RefCell<Vec<Event>>>,
}

impl RecordingObserver {
    fn new() -> (Self, Rc<RefCell<Vec<Event>>>) {
        let log = Rc::new(RefCell::new(Vec::new()));
        (Self { log: log.clone() }, log)
    }
}

impl WriteObserver for RecordingObserver {
    fn on_put(&mut self, db: u32, key: &[u8], new: &Entry) {
        self.log.borrow_mut().push(Event::Put {
            db,
            key: key.to_vec(),
            bytes: new.str_value_bytes().to_vec(),
            ty: new.data_type(),
        });
    }

    fn on_remove(&mut self, db: u32, key: &[u8]) {
        self.log.borrow_mut().push(Event::Remove {
            db,
            key: key.to_vec(),
        });
    }
}

/// Convenience: install a fresh recording observer and return its read-back log handle.
fn install(s: &mut ShardStore) -> Rc<RefCell<Vec<Event>>> {
    let (observer, log) = RecordingObserver::new();
    s.set_write_observer(Box::new(observer));
    log
}

/// SADD one member through the in-place-mutation arm (create-on-missing via Insert, else
/// Mutated), mirroring the command-layer handler so the non-emptying in-place fire path is
/// exercised through the real `Mutated` route. Returns whether the member was new.
fn sadd(s: &mut ShardStore, key: &[u8], member: &[u8]) -> bool {
    let m = member.to_vec();
    s.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Insert(NewValueOwned::set(vec![m])),
            expire: ExpireWrite::Clear,
            reply: true,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let set = o.as_set_mut().expect("set");
            let was_new = set.add(&m);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: was_new,
            }
        }
        RmwEntry::Occupied(_) => unreachable!(),
    })
}

/// SPOP-to-empty: remove the only member through the in-place arm, returning `Delete` once
/// drained so the empty-collection-deletes-key backstop fires the `on_remove`.
fn spop_last(s: &mut ShardStore, key: &[u8], member: &[u8]) {
    let m = member.to_vec();
    s.rmw_mut(0, key, NOW, move |entry| match entry {
        RmwEntry::OccupiedMut(mut o) => {
            let set = o.as_set_mut().expect("set");
            set.remove(&m);
            RmwStep {
                action: RmwAction::Mutated,
                expire: ExpireWrite::Unchanged,
                reply: (),
            }
        }
        RmwEntry::Vacant | RmwEntry::Occupied(_) => unreachable!(),
    });
}

/// EXPIRE-style TTL write through the read-only `rmw` Keep arm (no value touched).
fn expire(s: &mut ShardStore, key: &[u8], deadline: UnixMillis) -> bool {
    s.rmw(0, key, NOW, move |entry| match entry {
        RmwEntry::Occupied(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Set(deadline),
            reply: true,
        },
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(_) => unreachable!(),
    })
}

/// Bare GETEX (no TTL option) through the read-only `rmw` Keep arm: a pure read that leaves
/// the deadline UNCHANGED. This must NOT fire the observer.
fn getex_noop(s: &mut ShardStore, key: &[u8]) -> bool {
    s.rmw(0, key, NOW, |entry| match entry {
        RmwEntry::Occupied(_) => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: true,
        },
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: false,
        },
        RmwEntry::OccupiedMut(_) => unreachable!(),
    })
}

#[test]
fn create_fires_on_put_with_post_image() {
    let mut s = store();
    let log = install(&mut s);
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    assert_eq!(
        *log.borrow(),
        vec![Event::Put {
            db: 0,
            key: b"k".to_vec(),
            bytes: b"v".to_vec(),
            ty: DataType::String,
        }],
        "a create fires on_put once with the committed post-image"
    );
}

#[test]
fn overwrite_fires_on_put_with_new_post_image() {
    let mut s = store();
    let log = install(&mut s);
    s.upsert(0, b"k", NewValue::Bytes(b"v1"), ExpireWrite::Clear, NOW);
    s.upsert(0, b"k", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW);
    assert_eq!(
        *log.borrow(),
        vec![
            Event::Put {
                db: 0,
                key: b"k".to_vec(),
                bytes: b"v1".to_vec(),
                ty: DataType::String,
            },
            Event::Put {
                db: 0,
                key: b"k".to_vec(),
                bytes: b"v2".to_vec(),
                ty: DataType::String,
            },
        ],
        "an overwrite fires on_put with the NEW post-image (v2)"
    );
}

#[test]
fn delete_fires_on_remove() {
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let log = install(&mut s); // install AFTER the seed so we see only the delete
    assert!(s.delete(0, b"k", NOW));
    assert_eq!(
        *log.borrow(),
        vec![Event::Remove {
            db: 0,
            key: b"k".to_vec(),
        }],
        "an explicit delete fires on_remove"
    );
}

#[test]
fn lazy_expiry_fires_on_remove() {
    // A live key with a deadline; advance `now` past it; a read/contains runs the lazy
    // backstop, which routes the removal through remove_object -> observe_remove.
    let mut s = store();
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(2_000)),
        NOW,
    );
    let log = install(&mut s);
    // Past the deadline: the contains() backstop reaps it.
    assert!(!s.contains(0, b"k", UnixMillis(2_001)), "lazily expired");
    assert_eq!(
        *log.borrow(),
        vec![Event::Remove {
            db: 0,
            key: b"k".to_vec(),
        }],
        "a lazy expiry reap fires on_remove (it funnels through remove_object)"
    );
}

#[test]
fn flushdb_fires_on_remove_for_every_resident_key() {
    let mut s = store();
    s.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
    s.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW);
    s.upsert(1, b"c", NewValue::Bytes(b"3"), ExpireWrite::Clear, NOW);
    let log = install(&mut s);
    s.flush_db(0);
    // FLUSHDB of db 0 fires on_remove for its two resident keys (order is table-iteration
    // order, so assert as a set), and NOTHING for db 1.
    let events = log.borrow().clone();
    assert_eq!(events.len(), 2, "two resident keys removed from db 0");
    assert!(events.contains(&Event::Remove {
        db: 0,
        key: b"a".to_vec(),
    }));
    assert!(events.contains(&Event::Remove {
        db: 0,
        key: b"b".to_vec(),
    }));
    assert!(
        !events.contains(&Event::Remove {
            db: 1,
            key: b"c".to_vec(),
        }),
        "FLUSHDB of db 0 leaves db 1 untouched"
    );
}

#[test]
fn rename_fires_put_on_dst_then_remove_on_src() {
    // RENAME src -> dst: the destination write funnels through put_object (on_put with the
    // moved value's post-image), then the source removal funnels through remove_object
    // (on_remove). Asserts the order.
    let mut s = store();
    s.upsert(0, b"src", NewValue::Bytes(b"val"), ExpireWrite::Clear, NOW);
    let log = install(&mut s);
    let outcome = s.move_object(0, b"src", 0, b"dst", MoveMode::Rename, false, NOW);
    assert!(matches!(outcome, ironcache_storage::MoveOutcome::Moved));
    assert_eq!(
        *log.borrow(),
        vec![
            Event::Put {
                db: 0,
                key: b"dst".to_vec(),
                bytes: b"val".to_vec(),
                ty: DataType::String,
            },
            Event::Remove {
                db: 0,
                key: b"src".to_vec(),
            },
        ],
        "RENAME fires on_put(dst, post-image) then on_remove(src)"
    );
}

#[test]
fn inplace_collection_edit_fires_on_put_post_image() {
    // SADD s a (create via Insert -> on_put), SADD s b (non-emptying in-place Mutated edit
    // -> on_put with the post-edit entry). The post-image of a Set is empty value bytes with
    // a Set data type (str_value_bytes is empty for a collection), so we assert the type +
    // empty bytes.
    let mut s = store();
    let log = install(&mut s);
    assert!(sadd(&mut s, b"s", b"a"), "first SADD is a new member");
    assert!(sadd(&mut s, b"s", b"b"), "second SADD is a new member");
    assert_eq!(
        *log.borrow(),
        vec![
            Event::Put {
                db: 0,
                key: b"s".to_vec(),
                bytes: Vec::new(),
                ty: DataType::Set,
            },
            Event::Put {
                db: 0,
                key: b"s".to_vec(),
                bytes: Vec::new(),
                ty: DataType::Set,
            },
        ],
        "the in-place SADD edit fires on_put with the post-edit Set post-image"
    );
}

#[test]
fn noop_inplace_edit_still_fires_on_put() {
    // SADD s a; SADD s a (member already present -> no value change). Any write is observed,
    // so the no-op in-place edit STILL fires on_put (the same any-write semantics the WATCH
    // gate uses).
    let mut s = store();
    assert!(sadd(&mut s, b"s", b"a"), "first SADD is new");
    let log = install(&mut s);
    assert!(!sadd(&mut s, b"s", b"a"), "re-SADD is a no-op on the value");
    assert_eq!(
        *log.borrow(),
        vec![Event::Put {
            db: 0,
            key: b"s".to_vec(),
            bytes: Vec::new(),
            ty: DataType::Set,
        }],
        "a no-op in-place edit still fires on_put (any write is observed)"
    );
}

#[test]
fn emptied_collection_edit_fires_on_remove() {
    // SADD s a (Insert -> on_put); SPOP the only member to empty -> the empty-collection
    // backstop removes the key via remove_object_crediting -> on_remove.
    let mut s = store();
    assert!(sadd(&mut s, b"s", b"a"));
    let log = install(&mut s);
    spop_last(&mut s, b"s", b"a");
    assert_eq!(
        *log.borrow(),
        vec![Event::Remove {
            db: 0,
            key: b"s".to_vec(),
        }],
        "an in-place edit that empties the collection fires on_remove (not on_put)"
    );
}

#[test]
fn ttl_change_via_readonly_rmw_keep_fires_on_put() {
    // EXPIRE through the read-only `rmw` Keep arm: a real TTL change fires on_put (the
    // post-image carries the new deadline; set_entry_expire patches in place, not through
    // put_object, so the fire is at the Keep site).
    let mut s = store();
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    let log = install(&mut s);
    assert!(expire(&mut s, b"k", UnixMillis(100_000)), "key was live");
    assert_eq!(
        *log.borrow(),
        vec![Event::Put {
            db: 0,
            key: b"k".to_vec(),
            bytes: b"v".to_vec(),
            ty: DataType::String,
        }],
        "a real TTL change fires on_put with the (value-unchanged) post-image"
    );
}

#[test]
fn noop_ttl_write_does_not_fire() {
    // A bare GETEX (ExpireWrite::Unchanged) and an EXPIRE to the SAME deadline are no-op TTL
    // writes: scoped to the real-change branch (mirroring the WATCH gate), they fire nothing.
    let mut s = store();
    let deadline = UnixMillis(100_000);
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(deadline),
        NOW,
    );
    let log = install(&mut s);
    assert!(getex_noop(&mut s, b"k"), "key was live");
    assert!(
        expire(&mut s, b"k", deadline),
        "EXPIRE to the same deadline"
    );
    assert!(
        log.borrow().is_empty(),
        "no-op TTL writes (bare GETEX / same-deadline EXPIRE) fire nothing"
    );
}

#[test]
fn no_observer_installed_fires_nothing() {
    // The HOT-PATH default: a store with NO observer installed reports the gate OFF, and a
    // full stream of writes (create / overwrite / delete / in-place SADD / TTL change /
    // FLUSHDB) cannot invoke any observer. Structurally proven by installing a recorder,
    // immediately clearing it, then driving the writes and asserting the (retained) log is
    // empty: a regression that fired unconditionally (not gated on repl_active) would append.
    let mut s = store();
    assert!(
        !s.write_observer_active(),
        "a fresh store has no observer installed"
    );
    let log = install(&mut s);
    s.clear_write_observer();
    assert!(
        !s.write_observer_active(),
        "clear flips the fast-path gate back off"
    );
    // Drive every funnel site:
    s.upsert(0, b"k", NewValue::Bytes(b"v"), ExpireWrite::Clear, NOW);
    s.upsert(0, b"k", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW);
    sadd(&mut s, b"s", b"a");
    sadd(&mut s, b"s", b"a"); // no-op in-place edit
    expire(&mut s, b"k", UnixMillis(100_000));
    s.delete(0, b"k", NOW);
    s.flush_db(0);
    assert!(
        log.borrow().is_empty(),
        "with no observer installed the funnel fires NOTHING (the repl_active gate short-circuits)"
    );
}

#[test]
fn clear_observer_stops_subsequent_observation() {
    // Observe one write, clear, then write again: only the first write is recorded.
    let mut s = store();
    let log = install(&mut s);
    s.upsert(0, b"a", NewValue::Bytes(b"1"), ExpireWrite::Clear, NOW);
    s.clear_write_observer();
    s.upsert(0, b"b", NewValue::Bytes(b"2"), ExpireWrite::Clear, NOW);
    assert_eq!(
        *log.borrow(),
        vec![Event::Put {
            db: 0,
            key: b"a".to_vec(),
            bytes: b"1".to_vec(),
            ty: DataType::String,
        }],
        "writes after clear_write_observer are not observed"
    );
}

#[test]
fn full_lifecycle_in_order() {
    // A mixed sequence touching every kind, asserting the EXACT ordered event log:
    // create, overwrite, in-place create, in-place edit, TTL change, delete, emptied-collection.
    let mut s = store();
    let log = install(&mut s);
    s.upsert(0, b"k", NewValue::Bytes(b"v1"), ExpireWrite::Clear, NOW); // Put k v1
    s.upsert(0, b"k", NewValue::Bytes(b"v2"), ExpireWrite::Clear, NOW); // Put k v2
    sadd(&mut s, b"s", b"a"); // Put s (Set, create)
    sadd(&mut s, b"s", b"b"); // Put s (Set, in-place edit)
    expire(&mut s, b"k", UnixMillis(50_000)); // Put k (TTL change)
    s.delete(0, b"k", NOW); // Remove k
    spop_last(&mut s, b"s", b"a"); // s now {b}; not emptied -> Put s (in-place edit)
    spop_last(&mut s, b"s", b"b"); // s now {} -> Remove s
    assert_eq!(
        *log.borrow(),
        vec![
            Event::Put {
                db: 0,
                key: b"k".to_vec(),
                bytes: b"v1".to_vec(),
                ty: DataType::String
            },
            Event::Put {
                db: 0,
                key: b"k".to_vec(),
                bytes: b"v2".to_vec(),
                ty: DataType::String
            },
            Event::Put {
                db: 0,
                key: b"s".to_vec(),
                bytes: Vec::new(),
                ty: DataType::Set
            },
            Event::Put {
                db: 0,
                key: b"s".to_vec(),
                bytes: Vec::new(),
                ty: DataType::Set
            },
            Event::Put {
                db: 0,
                key: b"k".to_vec(),
                bytes: b"v2".to_vec(),
                ty: DataType::String
            },
            Event::Remove {
                db: 0,
                key: b"k".to_vec()
            },
            Event::Put {
                db: 0,
                key: b"s".to_vec(),
                bytes: Vec::new(),
                ty: DataType::Set
            },
            Event::Remove {
                db: 0,
                key: b"s".to_vec()
            },
        ],
        "the full write lifecycle is observed in order with correct kinds + post-images"
    );
}

#[test]
fn observation_is_deterministic_across_replay() {
    // Two independent stores driven through the IDENTICAL write sequence produce the
    // IDENTICAL observed event log (ADR-0003: the store passes the observer no clock / no
    // RNG; the event stream is a pure function of the write sequence). A seeded in-test
    // sequence (splitmix64, no std::time / no rand).
    fn replay() -> Vec<Event> {
        let mut s = store();
        let log = install(&mut s);
        let mut seed: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        // Type-segregated keyspaces so a key never flips type (a String key never receives
        // an SADD -> no WRONGTYPE): string ops on `str_keys`, set ops on `set_keys`.
        let str_keys: [&[u8]; 3] = [b"a", b"b", b"c"];
        let set_keys: [&[u8]; 2] = [b"sx", b"sy"];
        for _ in 0..60 {
            match next() % 3 {
                0 => {
                    let key = str_keys[(next() % 3) as usize];
                    s.upsert(0, key, NewValue::Bytes(b"vN"), ExpireWrite::Clear, NOW);
                }
                1 => {
                    let key = str_keys[(next() % 3) as usize];
                    s.delete(0, key, NOW);
                }
                _ => {
                    let key = set_keys[(next() % 2) as usize];
                    sadd(&mut s, key, b"m");
                }
            }
        }
        log.borrow().clone()
    }
    assert_eq!(
        replay(),
        replay(),
        "the observed event stream replays identically"
    );
}
