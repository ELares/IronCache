// SPDX-License-Identifier: MIT OR Apache-2.0
//! The SOURCE-side slot drain for rebalance APPLY (#371, REBALANCE_APPLY.md).
//!
//! A resharding move relocates one slot's keys from the source node to the destination. The source
//! DRAINS the slot in bounded batches: enumerate the slot's live keys
//! (`ironcache_server::cmd_keyspace::keys_in_slot`, the honest cross-shard `GETKEYSINSLOT` read),
//! read each key's full object ([`ironcache_store::ShardStore::get_object`]), and encode it with the
//! self-consistent [`ironcache_repl::encode_kvobj`] codec (the SAME codec persistence + replication
//! use, so it round-trips every value type without needing the Redis-byte-interop `DUMP`/`RESTORE`,
//! #129/#242). The destination replays each pair via `decode_kvobj` + `insert_object` (a later slice).
//!
//! This module is the SOURCE READ HALF only: a pure, per-shard drain over ONE shard's partition (the
//! controller fans it across shards + ships the batches; those are later slices). It lives in this
//! crate (not `ironcache-server`) because it needs the CONCRETE [`ShardStore`], which the server layer
//! is deliberately generic over. Cold resharding path only; nothing here touches the data hot path.

use ironcache_storage::{AccountingHook, EvictionHook, Keyspace, UnixMillis};
use ironcache_store::ShardStore;

/// One drained key ready to ship: its NAME and its `encode_kvobj` bytes (the destination `RESTORE`s
/// it via `decode_kvobj` + `insert_object`).
pub type DrainedKey = (Box<[u8]>, Vec<u8>);

/// Drain up to `limit` of slot `slot`'s live keys from THIS shard's `db` for shipping (#371): each
/// returned pair is `(key, encode_kvobj(the key's KvObj))`. Composes the honest `GETKEYSINSLOT`
/// enumeration (`keys_in_slot`) + [`ShardStore::get_object`] + the self-consistent
/// [`ironcache_repl::encode_kvobj`] codec.
///
/// A key enumerated but concurrently deleted (or lazily expired) between the enumeration and the read
/// is simply skipped (`get_object` returns `None`), so the batch never carries a dead key. The result
/// is bounded by `limit` (a shard need not ship more than the controller asked for). O(keys examined
/// in the slot) on the COLD resharding path; the data hot path and standalone are untouched.
#[must_use]
pub fn drain_slot_batch<E, A>(
    store: &mut ShardStore<E, A>,
    db: u32,
    slot: u16,
    limit: usize,
    now: UnixMillis,
) -> Vec<DrainedKey>
where
    E: EvictionHook,
    A: AccountingHook,
    ShardStore<E, A>: Keyspace,
{
    let keys = ironcache_server::cmd_keyspace::keys_in_slot(store, db, slot, limit, now);
    keys.into_iter()
        .filter_map(|key| {
            store
                .get_object(db, &key, now)
                .map(|obj| (key, ironcache_repl::encode_kvobj(&obj)))
        })
        .collect()
}

/// Apply a drained batch on the DESTINATION shard (#371, REBALANCE_APPLY.md): decode each key's
/// [`ironcache_repl::encode_kvobj`] bytes and reinsert the full object (type + TTL + value) via
/// [`ShardStore::insert_object`], the SAME replay path replication + the snapshot loader use. Returns
/// the number of keys applied.
///
/// IDEMPOTENT by construction: re-applying an already-present key overwrites it with the same bytes
/// (`insert_object` replaces), so a retried batch after a mid-transfer restart is safe (the drain's
/// contract, REBALANCE_APPLY.md). The decoded object's OWN key is authoritative (it is carried inside
/// the encoding); the pair's key name is the transport/routing label. A pair whose bytes fail to
/// decode is skipped (it is not applied and not counted) rather than corrupting the store. Cold
/// resharding path only.
pub fn apply_drained_batch<E, A>(
    store: &mut ShardStore<E, A>,
    db: u32,
    batch: &[DrainedKey],
) -> usize
where
    E: EvictionHook,
    A: AccountingHook,
{
    let mut applied = 0;
    for (_label, encoded) in batch {
        if let Some(obj) = ironcache_repl::decode_kvobj(encoded) {
            store.insert_object(db, obj);
            applied += 1;
        }
    }
    applied
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_protocol::key_slot;
    use ironcache_store::ShardStore;
    use ironcache_store::kvobj::KvObj;

    #[test]
    fn drain_slot_batch_encodes_only_the_slots_keys_and_round_trips() {
        let mut store = ShardStore::new(1);
        let now = UnixMillis(1_000);
        // `{t}i` all share one slot (the hashtag rule); `{other}x` is a different slot.
        let slot = key_slot(b"{t}0");
        for k in [b"{t}0".as_ref(), b"{t}1", b"{t}2"] {
            store.insert_object(0, KvObj::from_bytes(k, b"val", None));
        }
        store.insert_object(0, KvObj::from_bytes(b"{other}x", b"v", None));

        let batch = drain_slot_batch(&mut store, 0, slot, 100, now);
        assert_eq!(batch.len(), 3, "only the slot's 3 keys are drained");
        for (key, encoded) in &batch {
            assert!(key.starts_with(b"{t}"), "a drained key is in the slot");
            // The encoded bytes decode back to the SAME key via the shared codec.
            let obj = ironcache_repl::decode_kvobj(encoded).expect("encoded bytes decode");
            assert_eq!(&obj.key, key, "the decoded object is the drained key");
        }

        // The limit bounds the batch.
        assert_eq!(drain_slot_batch(&mut store, 0, slot, 2, now).len(), 2);
        // A slot with no keys drains empty.
        let empty = key_slot(b"{no-such-tag}");
        assert!(drain_slot_batch(&mut store, 0, empty, 100, now).is_empty());
    }

    #[test]
    fn drain_slot_batch_skips_a_lazily_expired_key() {
        let mut store = ShardStore::new(1);
        let slot = key_slot(b"{e}0");
        store.insert_object(0, KvObj::from_bytes(b"{e}0", b"v", None));
        store.insert_object(0, KvObj::from_bytes(b"{e}1", b"v", Some(UnixMillis(10))));
        // Past the dead key's deadline: only the live key drains.
        let batch = drain_slot_batch(&mut store, 0, slot, 100, UnixMillis(1_000));
        assert_eq!(batch.len(), 1, "the expired key is not drained");
        assert_eq!(&*batch[0].0, b"{e}0");
    }

    #[test]
    fn drain_then_apply_moves_a_slots_keys_byte_identically_between_stores() {
        // The whole source -> destination key move, minus the network: drain a slot on the SOURCE,
        // apply the batch on the DESTINATION, and assert every key arrives BYTE-IDENTICAL (value +
        // TTL), proving the encode/ship/decode/reinsert path preserves the object.
        let mut src = ShardStore::new(1);
        let mut dst = ShardStore::new(1);
        let now = UnixMillis(1_000);
        let slot = key_slot(b"{m}0");
        src.insert_object(0, KvObj::from_bytes(b"{m}0", b"alpha", None));
        src.insert_object(0, KvObj::from_bytes(b"{m}1", b"beta", None));
        // A key with a TTL, to prove the expiry survives the transfer.
        src.insert_object(
            0,
            KvObj::from_bytes(b"{m}2", b"gamma", Some(UnixMillis(9_000_000))),
        );

        let batch = drain_slot_batch(&mut src, 0, slot, 100, now);
        assert_eq!(batch.len(), 3, "all three source keys drain");
        assert_eq!(
            apply_drained_batch(&mut dst, 0, &batch),
            3,
            "all three apply"
        );

        // Each moved key is present on the destination and re-encodes to the EXACT bytes that were
        // shipped (byte-identical object), and its TTL matches.
        for (label, shipped) in &batch {
            let landed = dst.get_object(0, label, now).expect("key moved to dst");
            assert_eq!(
                &ironcache_repl::encode_kvobj(&landed),
                shipped,
                "the destination object is byte-identical to the shipped one"
            );
        }
        assert_eq!(
            dst.get_object(0, b"{m}2", now).unwrap().expire_at,
            Some(UnixMillis(9_000_000)),
            "the TTL survived the transfer"
        );
    }

    #[test]
    fn apply_drained_batch_is_idempotent_and_skips_undecodable_bytes() {
        let mut dst = ShardStore::new(1);
        let obj = KvObj::from_bytes(b"k", b"v", None);
        let encoded = ironcache_repl::encode_kvobj(&obj);
        let batch: Vec<DrainedKey> = vec![
            (Box::from(b"k".as_ref()), encoded.clone()),
            // Garbage bytes that do not decode: skipped, not applied, not a panic.
            (Box::from(b"bad".as_ref()), vec![0xff, 0x00, 0x01]),
        ];
        assert_eq!(
            apply_drained_batch(&mut dst, 0, &batch),
            1,
            "only the decodable key applies"
        );
        assert!(dst.get_object(0, b"k", UnixMillis(0)).is_some());
        // Re-applying the same batch is a no-op overwrite (idempotent), still counting the good key.
        assert_eq!(
            apply_drained_batch(&mut dst, 0, &batch),
            1,
            "re-apply is idempotent"
        );
    }
}
