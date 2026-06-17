// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-7d CARRY-FORWARD 2: a PASSIVE replica reports a due key as absent on read but does NOT
//! physically remove it. Removal on a replica comes ONLY from the replication stream (the
//! primary's StreamDel), matching real-Redis replica semantics, so a READONLY read can never
//! self-remove a key the primary still holds (which would pre-empt the primary's expiry and
//! double-count `expired_keys`). The default (non-passive) path is unchanged.

use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
use ironcache_store::ShardStore;

#[test]
fn passive_replica_read_reports_due_key_absent_without_removing_it() {
    let mut s = ShardStore::new(16);
    // A key whose deadline is t=10.
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(10)),
        UnixMillis(0),
    );

    // Mark the shard a passive replica.
    s.set_passive(true);
    assert!(s.is_passive());

    // At t=100 the key is due. A passive read reports it ABSENT (logically expired)...
    assert!(
        s.read(0, b"k", UnixMillis(100)).is_none(),
        "a due key reads as absent on a passive replica"
    );
    // ...but it is NOT physically removed: no lazy-expiry reclamation is counted.
    assert_eq!(
        s.take_lazy_expired(),
        0,
        "a passive replica must NOT physically remove a key on read"
    );

    // Clear passive (the analog of a promotion). The SAME read at t=100 now physically reaps
    // the key (the active lazy-expiry path runs), proving the entry was still resident the
    // whole time under passive mode.
    s.set_passive(false);
    assert!(s.read(0, b"k", UnixMillis(100)).is_none());
    assert_eq!(
        s.take_lazy_expired(),
        1,
        "after clearing passive, the still-resident due key is reaped on read"
    );
}

#[test]
fn non_passive_store_reaps_due_key_on_read_as_before() {
    // The default (non-passive) lazy-on-read expiry is unchanged: a due key is physically
    // removed on the read that observes it.
    let mut s = ShardStore::new(16);
    s.upsert(
        0,
        b"k",
        NewValue::Bytes(b"v"),
        ExpireWrite::Set(UnixMillis(10)),
        UnixMillis(0),
    );
    assert!(s.read(0, b"k", UnixMillis(100)).is_none());
    assert_eq!(
        s.take_lazy_expired(),
        1,
        "the default path reaps a due key on read"
    );
}
