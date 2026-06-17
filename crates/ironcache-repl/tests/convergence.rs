// SPDX-License-Identifier: MIT OR Apache-2.0
//! THE HA-7c CONVERGENCE GATE: a store-level, seeded harness that drives a randomized write
//! workload on a primary with the [`ReplObserver`] installed, ships the steady-state tail
//! across a CONTROLLABLE in-memory link (delay / partition / heal / forced full-resync), and
//! asserts the replica keyspace CONVERGES to the primary's key-for-key after a final heal +
//! drain. Run over many seeds, this is the merge-blocking proof that the steady-state
//! stream-and-apply is correct (eventual convergence, no gaps / dups) under reorder-free
//! delay / partition / heal / resync.
//!
//! ## Not the SimNode harness
//!
//! This is deliberately STORE-LEVEL, not the `ironcache-sim` SimNode DST (which HA-7a's
//! `dst_link.rs` uses for the heartbeat/cursor link). Here the unit under test is the data
//! path: the observer ring, [`drain_and_ship`], and [`ReplicaApplier`] driven against two
//! real [`ShardStore`]s, with a hand-built faulty link between them. The link is a FIFO queue
//! (reorder-free, matching a TCP byte stream): faults are DELAY (hold frames), PARTITION (drop
//! in-flight + tear the link), HEAL (reconnect + resume from the replica's acked offset), and
//! a forced full-resync (overflow the bounded ring).
//!
//! ## Determinism (ADR-0003)
//!
//! Every random choice -- the workload op, the key, the value, the TTL, and the fault schedule
//! -- is drawn from a SEEDED [`SplitMix64`] (the `ironcache-env` determinism seam), NEVER a
//! clock or the `rand` crate. A run is byte-reproducible from its seed, so a failing seed is a
//! deterministic, minimizable repro.

use ironcache_env::{Rng, SplitMix64};
use ironcache_repl::observer::{ReplObserver, ReplRing};
use ironcache_repl::{
    ApplyOutcome, Frame, ReplOffset, ReplicaApplier, ShipOutcome, drain_and_ship,
};
use ironcache_storage::{
    ExpireWrite, NewValue, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
    ZAddFlags,
};
use ironcache_store::{ShardStore, SnapshotCursor};

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

const DBS: u32 = 4;
const NOW: UnixMillis = UnixMillis(10_000);

// ---------------------------------------------------------------------------
// Store fingerprint (faithful keyspace equality).
// ---------------------------------------------------------------------------

/// Drain a store's whole snapshot into a sorted, comparable `(db, key, kvcodec-bytes)` form.
/// The kvcodec bytes carry type + encoding + TTL + value, so two stores are EQUAL iff their
/// fingerprints are equal -- a faithful keyspace comparison without naming the private value
/// internals. (Same technique as the HA-7b full-sync test.)
fn fingerprint<E: ironcache_storage::EvictionHook, A: ironcache_storage::AccountingHook>(
    s: &ShardStore<E, A>,
) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
    let databases = s.databases();
    let mut cursor = SnapshotCursor::START;
    let mut out = Vec::new();
    let mut guard = 0;
    while !cursor.is_done(databases) {
        let (chunk, next) = s.snapshot_chunk(cursor, 64, NOW);
        for (db, key, kv) in chunk {
            out.push((db, key.into_vec(), ironcache_repl::encode_kvobj(&kv)));
        }
        cursor = next;
        guard += 1;
        assert!(guard < 1_000_000, "fingerprint drain terminates");
    }
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// The faulty in-memory link (a reorder-free FIFO pipe with injectable faults).
// ---------------------------------------------------------------------------

/// A controllable in-memory link from the primary's ship sink to the replica's apply loop.
/// A FIFO queue (reorder-free, like a TCP byte stream); a fault drops the in-flight contents
/// and marks the link DOWN until a heal.
struct FaultyLink {
    /// Frames shipped by the primary, awaiting delivery to the replica (in order).
    inflight: Vec<Frame>,
    /// Whether the link is currently UP (a partition sets it down; a heal brings it up).
    up: bool,
}

impl FaultyLink {
    fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(FaultyLink {
            inflight: Vec::new(),
            up: true,
        }))
    }
}

/// Run an always-Ready future to completion on the stable no-op waker (the in-memory link
/// never pends; the crate forbids unsafe so we use the stable `Waker::noop`).
fn block_on<F: Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("convergence harness future pended; the link is synchronous"),
    }
}

// ---------------------------------------------------------------------------
// The randomized write workload.
// ---------------------------------------------------------------------------

/// A small key universe so overwrites, deletes, and collection edits collide on the same keys
/// (which is where convergence bugs hide: an overwrite that races a delete, a TTL that races a
/// value write). Keys are spread across all DBs.
fn pick_key(rng: &mut SplitMix64) -> (u32, Vec<u8>) {
    let db = (rng.gen_below(u64::from(DBS))) as u32;
    let k = rng.gen_below(12); // 12 keys per db
    (db, format!("key-{k:02}").into_bytes())
}

/// Build an `RmwStep` with no TTL effect (the common shape in the collection edits below).
fn step(action: RmwAction) -> RmwStep<()> {
    RmwStep {
        action,
        expire: ExpireWrite::Keep,
        reply: (),
    }
}

/// Seed-or-edit one collection key in place via `rmw_mut`: `seed` builds a fresh value on a
/// Vacant key (or a wrong-type Replace), `edit` mutates the live typed value in place and
/// returns whether the in-place edit applied (false -> a wrong-type key, overwrite with a
/// fresh `seed()`). Drives the OccupiedMut in-place-mutation arm + the type-flip overwrite.
fn collection_edit(
    store: &mut ShardStore,
    db: u32,
    key: &[u8],
    seed: impl FnOnce() -> NewValueOwned + Clone,
    edit: impl FnOnce(&mut ironcache_storage::OccupiedEntryMut<'_>) -> bool,
) {
    let seed2 = seed.clone();
    store.rmw_mut(db, key, NOW, move |entry| match entry {
        RmwEntry::Vacant => step(RmwAction::Insert(seed())),
        RmwEntry::OccupiedMut(mut m) => {
            if edit(&mut m) {
                step(RmwAction::Mutated)
            } else {
                step(RmwAction::Replace(seed2()))
            }
        }
        RmwEntry::Occupied(_) => unreachable!("rmw_mut yields OccupiedMut for a live key"),
    });
}

/// Apply ONE random write to the primary store, dispatching string-family ops (case 0-3, 8-9)
/// to [`apply_string_write`] and the in-place collection edits (case 4-7) to
/// [`apply_collection_write`]. The observer fires for each, enqueuing the tail op.
fn apply_random_write(store: &mut ShardStore, rng: &mut SplitMix64) {
    let (db, key) = pick_key(rng);
    let which = rng.gen_below(10);
    if (4..=7).contains(&which) {
        apply_collection_write(store, db, &key, which, rng);
    } else {
        apply_string_write(store, db, &key, which, rng);
    }
}

/// String-family writes: blind SET (bytes / int), SET-with-future-TTL, DEL, a list POP (the
/// empty-collection-deletes-key edge), and a PERSIST/EXPIRE-style TTL-only change.
fn apply_string_write(
    store: &mut ShardStore,
    db: u32,
    key: &[u8],
    which: u64,
    rng: &mut SplitMix64,
) {
    match which {
        0 => {
            let v = format!("v{}", rng.next_u64() % 1000).into_bytes();
            store.upsert(db, key, NewValue::Bytes(&v), ExpireWrite::Clear, NOW);
        }
        1 => {
            let n = (rng.next_u64() % 100_000) as i64 - 50_000;
            store.upsert(db, key, NewValue::Int(n), ExpireWrite::Clear, NOW);
        }
        2 => {
            // SET with a FUTURE TTL (the key stays live; the deadline must replicate).
            let v = format!("ttl{}", rng.next_u64() % 100).into_bytes();
            store.upsert(
                db,
                key,
                NewValue::Bytes(&v),
                ExpireWrite::Set(UnixMillis(NOW.0 + 1 + rng.gen_below(100_000))),
                NOW,
            );
        }
        3 => {
            store.delete(db, key, NOW);
        }
        8 => {
            // A list POP (drains toward empty -> the empty-collection-deletes-key edge).
            store.rmw_mut(db, key, NOW, |entry| match entry {
                RmwEntry::OccupiedMut(mut m) => {
                    if let Some(l) = m.as_list_mut() {
                        l.pop_front();
                        step(RmwAction::Mutated)
                    } else {
                        step(RmwAction::Keep)
                    }
                }
                _ => step(RmwAction::Keep),
            });
        }
        _ => {
            // A PERSIST/EXPIRE-style TTL-only change on an existing key (no value change).
            let set_ttl = rng.gen_below(2) == 0;
            store.rmw_mut(db, key, NOW, move |entry| {
                let expire = if set_ttl {
                    ExpireWrite::Set(UnixMillis(NOW.0 + 1 + 5_000))
                } else {
                    ExpireWrite::Clear
                };
                match entry {
                    RmwEntry::Vacant => step(RmwAction::Keep),
                    _ => RmwStep {
                        action: RmwAction::Keep,
                        expire,
                        reply: (),
                    },
                }
            });
        }
    }
}

/// In-place collection edits via `rmw_mut`: a list push, a hash field set, a set add (numeric,
/// the intset edge), and a zset add. Each seeds a fresh value on a Vacant key and overwrites a
/// wrong-type key, exercising the type-flip + the small->large encoding ratchet.
fn apply_collection_write(
    store: &mut ShardStore,
    db: u32,
    key: &[u8],
    which: u64,
    rng: &mut SplitMix64,
) {
    match which {
        4 => {
            let elem = format!("e{}", rng.next_u64() % 50).into_bytes();
            let e2 = elem.clone();
            collection_edit(
                store,
                db,
                key,
                move || NewValueOwned::list(vec![elem.clone()]),
                move |m| {
                    m.as_list_mut().is_some_and(|l| {
                        l.push_back(&e2);
                        true
                    })
                },
            );
        }
        5 => {
            let f = format!("f{}", rng.next_u64() % 8).into_bytes();
            let v = format!("hv{}", rng.next_u64() % 50).into_bytes();
            let (f2, v2) = (f.clone(), v.clone());
            collection_edit(
                store,
                db,
                key,
                move || NewValueOwned::hash(vec![(f.clone(), v.clone())]),
                move |m| {
                    m.as_hash_mut().is_some_and(|h| {
                        h.set(&f2, &v2);
                        true
                    })
                },
            );
        }
        6 => {
            let mem = format!("{}", rng.next_u64() % 30).into_bytes();
            let m2 = mem.clone();
            collection_edit(
                store,
                db,
                key,
                move || NewValueOwned::set(vec![mem.clone()]),
                move |m| {
                    m.as_set_mut().is_some_and(|s| {
                        s.add(&m2);
                        true
                    })
                },
            );
        }
        _ => {
            let mem = format!("zm{}", rng.next_u64() % 20).into_bytes();
            let zscore = (rng.next_u64() % 1000) as f64 / 10.0;
            let m2 = mem.clone();
            collection_edit(
                store,
                db,
                key,
                move || NewValueOwned::zset(vec![(mem.clone(), zscore)]),
                move |m| {
                    m.as_zset_mut().is_some_and(|z| {
                        z.add(&m2, zscore, ZAddFlags::default());
                        true
                    })
                },
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The convergence engine: primary + replica + faulty link, one seed.
// ---------------------------------------------------------------------------

/// A full HA-7b full-sync from `primary` into a FRESH replica store, returning the loaded
/// store + the cut offset. Synchronous (the in-memory channel is always Ready). This is the
/// recovery the replica runs on a gap.
fn full_resync(primary: &ShardStore, cut: ReplOffset) -> (ShardStore, ReplOffset) {
    let replid = ironcache_repl::ReplId::from_bytes([0x7c; 20]);
    let chan: Rc<RefCell<std::collections::VecDeque<Frame>>> =
        Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let dc = Rc::clone(&chan);
    block_on(ironcache_repl::drive_full_sync(
        primary,
        replid,
        cut,
        NOW,
        16,
        move |f| {
            let c = Rc::clone(&dc);
            async move {
                c.borrow_mut().push_back(f);
                Ok(())
            }
        },
    ))
    .expect("the in-memory full-sync drive completes");
    let rc = Rc::clone(&chan);
    let loaded = block_on(ironcache_repl::receive_full_sync(
        || ShardStore::new(DBS),
        move || {
            let c = Rc::clone(&rc);
            async move { c.borrow_mut().pop_front() }
        },
    ))
    .expect("the in-memory full-sync receive completes");
    (loaded.store, loaded.end_offset)
}

/// The one-seed convergence engine: a primary (with the observer ring) + a replica store +
/// applier + the faulty link. Methods drive one step and the final heal/drain; the duplicated
/// full-resync recovery is funneled through [`Harness::full_resync`].
struct Harness {
    primary: ShardStore,
    ring: Rc<RefCell<ReplRing>>,
    replica: ShardStore,
    applier: ReplicaApplier,
    link: Rc<RefCell<FaultyLink>>,
    counters: Counters,
}

impl Harness {
    /// A fresh harness for `seed`: the bounded ring is intentionally SMALL relative to the
    /// burst sizes so overflow (and thus forced full-resync) actually happens; the cap is
    /// drawn per seed for variety. The replica starts EMPTY and immediately full-syncs.
    fn new(seed: u64) -> Self {
        let cap = 4 + (seed % 28) as usize;
        let ring = ReplRing::new(cap, ReplOffset::ZERO);
        let mut primary = ShardStore::new(DBS);
        primary.set_write_observer(ReplObserver::boxed(Rc::clone(&ring)));
        let (replica, _cut) = full_resync(&primary, ring.borrow().head());
        Harness {
            primary,
            ring,
            replica,
            applier: ReplicaApplier::new(ReplOffset::ZERO),
            link: FaultyLink::new(),
            counters: Counters::default(),
        }
    }

    /// The recovery on ANY gap (ring overflow, an applied-stream hole, or a reconnect the
    /// primary cannot serve): discard the replica, re-load the snapshot at the primary head,
    /// rebase the ring at the cut, and drop the now-stale in-flight tail. Counts a resync.
    fn full_resync(&mut self) {
        self.counters.resyncs += 1;
        let head = self.ring.borrow().head();
        let (store, cut) = full_resync(&self.primary, head);
        self.replica = store;
        self.applier = ReplicaApplier::new(cut);
        self.ring.borrow_mut().take_resync();
        self.ring.borrow_mut().rebase(head);
        self.link.borrow_mut().inflight.clear();
    }

    /// Ship a bounded batch from the ring into the link (a send fails if the link is down).
    /// Returns the outcome so the caller can full-resync on overflow.
    fn ship(&self, max: usize) -> ShipOutcome {
        let sink = Rc::clone(&self.link);
        block_on(drain_and_ship(&self.ring, max, move |f| {
            let l = Rc::clone(&sink);
            async move {
                if l.borrow().up {
                    l.borrow_mut().inflight.push(f);
                    Ok(())
                } else {
                    Err(())
                }
            }
        }))
    }

    /// Apply a slice of in-flight frames to the replica; returns true on a GAP (caller
    /// full-resyncs). On clean apply the caller acks.
    fn apply_frames(&mut self, frames: Vec<Frame>) -> bool {
        for f in frames {
            if self.applier.apply(&mut self.replica, f, NOW) == ApplyOutcome::Gap {
                return true;
            }
        }
        false
    }

    /// One randomized step: a write burst, a maybe-delayed ship, a partial delivery+apply+ack,
    /// then a partition/heal fault.
    fn step(&mut self, rng: &mut SplitMix64) {
        let burst = 1 + rng.gen_below(6) as usize;
        for _ in 0..burst {
            apply_random_write(&mut self.primary, rng);
        }

        // SHIP (unless DELAYED this step, which lets the ring grow), only while the link is up.
        let up = self.link.borrow().up;
        if up && rng.gen_below(4) != 0 {
            let max_batch = 1 + rng.gen_below(8) as usize;
            if self.ship(max_batch) == ShipOutcome::ResyncNeeded {
                self.full_resync();
            }
        }

        // DELIVER a random prefix of the in-flight queue (the rest stays "in transit").
        if self.link.borrow().up {
            let inflight_len = self.link.borrow().inflight.len();
            let deliver = if inflight_len == 0 {
                0
            } else {
                1 + rng.gen_below(inflight_len as u64) as usize
            };
            let frames: Vec<Frame> = self.link.borrow_mut().inflight.drain(..deliver).collect();
            if self.apply_frames(frames) {
                self.full_resync();
            } else {
                // ACK the applied offset back (prunes the ring so it does not perpetually
                // overflow under steady-state running).
                self.ring.borrow_mut().ack(self.applier.applied());
            }
        }

        self.inject_fault(rng);
    }

    /// PARTITION (tear the link, drop in-flight) or HEAL (reconnect + resume from the
    /// replica's acked offset, or full-resync if the primary cannot serve it).
    fn inject_fault(&mut self, rng: &mut SplitMix64) {
        match rng.gen_below(12) {
            0 if self.link.borrow().up => {
                self.link.borrow_mut().up = false;
                self.link.borrow_mut().inflight.clear();
                self.counters.partitions += 1;
            }
            1 | 2 if !self.link.borrow().up => {
                self.link.borrow_mut().up = true;
                self.counters.heals += 1;
                self.reconnect_or_resync();
            }
            _ => {}
        }
    }

    /// On a (re)connect: resume the primary's send from the replica's acked offset if the ring
    /// still holds the resume window, else full-resync.
    fn reconnect_or_resync(&mut self) {
        let resume_from = self.applier.applied();
        if self.ring.borrow().can_serve_from(resume_from) && !self.ring.borrow().needs_resync() {
            self.ring.borrow_mut().rewind_send(resume_from);
        } else {
            self.full_resync();
        }
    }

    /// FINAL HEAL + FULL DRAIN: bring the link up, resume/resync, then ship + apply everything
    /// until the replica has applied through the primary head and the link is empty.
    fn drain_to_quiescence(&mut self, seed: u64) {
        self.link.borrow_mut().up = true;
        self.reconnect_or_resync();
        let mut guard = 0;
        loop {
            if self.ship(usize::MAX) == ShipOutcome::ResyncNeeded {
                self.full_resync();
            }
            let frames: Vec<Frame> = self.link.borrow_mut().inflight.drain(..).collect();
            if self.apply_frames(frames) {
                self.full_resync();
            }
            self.ring.borrow_mut().ack(self.applier.applied());

            guard += 1;
            assert!(
                guard < 1000,
                "final drain converges in bounded passes (seed {seed})"
            );
            if self.applier.applied().0 >= self.ring.borrow().head().0
                && self.link.borrow().inflight.is_empty()
            {
                break;
            }
        }
    }
}

/// Run the convergence scenario for one `seed`, returning the final fault counters (for the
/// aggregate coverage assertion). PANICS (failing the test) if the replica does not converge.
fn run_seed(seed: u64) -> Counters {
    let mut rng = SplitMix64::new(seed);
    let mut h = Harness::new(seed);

    let steps = 300 + (seed % 200) as usize;
    for _ in 0..steps {
        h.step(&mut rng);
    }
    h.drain_to_quiescence(seed);

    // ---- THE GATE: the replica keyspace EQUALS the primary's, key-for-key. ----
    let p = fingerprint(&h.primary);
    let r = fingerprint(&h.replica);
    assert_eq!(
        r,
        p,
        "REPLICA DIVERGED at seed {seed}: applied={:?} head={:?} (primary {} keys, replica {} keys)",
        h.applier.applied(),
        h.ring.borrow().head(),
        p.len(),
        r.len(),
    );

    h.counters
}

/// Aggregate fault counters across seeds (to assert the harness actually EXERCISED the faults
/// rather than trivially converging on a never-faulted link).
#[derive(Debug, Default, Clone, Copy)]
struct Counters {
    partitions: usize,
    heals: usize,
    resyncs: usize,
}

/// THE CONVERGENCE GATE: over 250 seeds, the replica converges to the primary under a
/// randomized workload interleaved with delay / partition / heal / forced full-resync. A
/// single diverging seed FAILS the build (and is a deterministic repro). The aggregate
/// asserts the faults were genuinely exercised (partitions, heals, and full-resyncs all
/// happened many times), so a green run is not a vacuous "the link never broke" pass.
#[test]
fn convergence_under_faults_over_many_seeds() {
    const SEEDS: u64 = 250;
    let mut total = Counters::default();
    for seed in 0..SEEDS {
        // Derive each seed deterministically from the loop index (no clock / no RNG crate).
        let s = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(seed + 1) ^ (seed << 17);
        let c = run_seed(s);
        total.partitions += c.partitions;
        total.heals += c.heals;
        total.resyncs += c.resyncs;
    }
    // Surfaced under `--nocapture` to confirm the harness genuinely exercised the faults.
    eprintln!(
        "convergence: {SEEDS} seeds converged; partitions={} heals={} forced-resyncs={}",
        total.partitions, total.heals, total.resyncs
    );
    // The harness must have genuinely exercised every fault class, or the gate is vacuous.
    assert!(
        total.partitions > 100,
        "expected many partitions across {SEEDS} seeds, got {}",
        total.partitions
    );
    assert!(
        total.heals > 50,
        "expected many heals across {SEEDS} seeds, got {}",
        total.heals
    );
    assert!(
        total.resyncs > 50,
        "expected many forced full-resyncs across {SEEDS} seeds, got {}",
        total.resyncs
    );
}
