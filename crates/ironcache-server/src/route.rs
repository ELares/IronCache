// SPDX-License-Identifier: MIT OR Apache-2.0
//! Key -> shard routing for the cross-shard coordinator (COORDINATOR.md #107,
//! ADR-0002/0003).
//!
//! The server is shared-nothing thread-per-core: each shard owns a PARTITION of the
//! keyspace, and a single-key command must run on the shard that OWNS its key. This
//! module is the pure, deterministic routing layer the serve loop consults BEFORE
//! dispatch to decide whether a command runs on the home shard (the fast path) or is
//! hopped to its owning shard (the remote path). It has no async, no I/O, and no
//! shared state, so it is unit-testable in isolation.
//!
//! ## The internal shard owner is ALIGNED to the client-visible cluster slot (#517)
//!
//! [`owner_shard`] maps a key to its owning shard via the SAME CRC16/XMODEM `{hashtag}`
//! cluster slot a client observes (`key_slot`), partitioned CONTIGUOUSLY across shards by
//! [`slot_to_shard`]. This alignment is deliberate (#517): a cluster-aware client that routes a
//! key to the shard owning its slot then HOMES its connection on the owning shard, so
//! `owner_shard(key) == home` and the internal cross-shard hop is skipped entirely (the hop
//! remains only as the fallback for a misrouting client). It also co-locates hash-tagged keys
//! (`{u}.a`, `{u}.b`) on one shard, shrinking the cross-shard fan-out surface.
//!
//! [`hash64`] (FNV-1a) is RETAINED as a pure, seedless byte hash but NO LONGER backs
//! `owner_shard` (it predates the #517 alignment). Before #517 the internal owner was
//! `hash64(key) % n`, deliberately independent of the cluster slot; #517 replaced that with the
//! slot-based owner so the two coincide.
//!
//! ## Determinism (ADR-0003)
//!
//! `owner_shard` is a PURE function of the key bytes: `key_slot` (CRC16 over the hash-tag) is
//! seedless, so a key's owning shard is stable across a seeded replay and two connections on
//! different shards agree on where a key lives. (A randomly-seeded `DefaultHasher`/SipHash would
//! route the same key to different shards on different boots, breaking replay determinism.)

use ironcache_protocol::Request;

/// The FNV-1a 64-bit offset basis (the standard constant).
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// The FNV-1a 64-bit prime (the standard constant).
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Hash `key` with FNV-1a (64-bit), the INTERNAL, non-client-visible shard hash.
///
/// Deterministic and seedless (a pure function of the bytes), per ADR-0003: do NOT
/// substitute `DefaultHasher`/SipHash, which are randomly seeded and would route the
/// same key to different shards across boots. See the module docs for why this is NOT
/// the client-visible CRC16 cluster hash (CLUSTER_CONTRACT.md).
#[must_use]
pub fn hash64(key: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &b in key {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The internal shard that owns cluster hash `slot`, given `n_shards`, as a CONTIGUOUS partition:
/// shard `i` owns the slot range `[i*16384/n, (i+1)*16384/n)`. Contiguity is deliberate (#517): it
/// makes a shard's owned slots coalesce into exactly ONE range, so the single-node `CLUSTER SLOTS`
/// projection is N clean ranges (a `slot % n` interleave would fragment each shard into ~16384/n
/// ranges). Real Redis Cluster likewise advertises contiguous ranges per node.
#[must_use]
pub fn slot_to_shard(slot: u16, n_shards: usize) -> usize {
    let n = n_shards.max(1);
    // slot in [0, 16384); (slot*n)/16384 in [0, n). Widen to avoid overflow (16383 * n).
    (slot as usize * n) / ironcache_protocol::CLUSTER_SLOTS as usize
}

/// The half-open slot range `[start, end)` that internal shard `i` owns under `slot_to_shard`
/// (#517), for the single-node `CLUSTER SLOTS`/`SHARDS`/`NODES` projection. Together the N ranges
/// partition `[0, 16384)` with no gap or overlap.
#[must_use]
pub fn shard_slot_range(shard: usize, n_shards: usize) -> [u16; 2] {
    let n = n_shards.max(1) as u32;
    let slots = u32::from(ironcache_protocol::CLUSTER_SLOTS);
    // ceil(shard*16384/n) .. ceil((shard+1)*16384/n): the inverse of the floor in slot_to_shard,
    // so a slot maps to shard i iff it is in this range.
    let start = (shard as u32 * slots).div_ceil(n);
    let end = ((shard as u32 + 1) * slots).div_ceil(n);
    [
        u16::try_from(start).unwrap_or(0),
        u16::try_from(end.min(slots)).unwrap_or(ironcache_protocol::CLUSTER_SLOTS),
    ]
}

/// The shard that OWNS `key` given `n_shards`: the shard owning `key`'s cluster slot
/// (`slot_to_shard(key_slot(key), n_shards)`).
///
/// As of #517 this is derived from the CLIENT-VISIBLE CRC16 cluster slot (`key_slot`, which applies
/// the `{tag}` hash-tag rule), NOT the old raw-key FNV-1a. Two consequences: (1) hash-tagged keys
/// (`{u}.a`, `{u}.b`) now CO-LOCATE on one internal shard, shrinking the cross-shard fan-out surface
/// for multi-key / spanning commands and letting a hash-tagged `MULTI/EXEC` run home-only; (2) a
/// cluster-aware client that routes `key` to the shard owning its slot HOMES on the owning shard, so
/// the internal cross-shard hop is skipped (the hop remains only as the misroute fallback). Still a
/// pure, seedless function (ADR-0003): `key_slot` is deterministic; the routing is never on the wire.
#[must_use]
pub fn owner_shard(key: &[u8], n_shards: usize) -> usize {
    slot_to_shard(ironcache_protocol::key_slot(key), n_shards)
}

// How a command must be routed across shards (COORDINATOR.md #107). The enum lives in the
// #89 single-source-of-truth command registry ([`crate::command_spec`]); it is re-exported
// here so this module's legacy `route::CommandClass` path (and every external `use
// ironcache_server::route::CommandClass`) keeps working unchanged.
//
// STAGE 1 routes any KEYED command (single- or multi-key) whose keys ALL resolve to ONE
// shard to that shard (via [`command_keys`]); a key-SPANNING multi-key command, and the
// whole-keyspace commands, stay on the home shard (the documented Stage 2 gap), and
// `CommandClass::AlwaysHome` commands (no key / control / conn / txn) stay home always. See
// the variant docs in [`crate::command_spec::CommandClass`] for the per-variant semantics.
pub use crate::command_spec::CommandClass;

/// The key(s) a command operates on, extracted by [`command_keys`] for routing.
///
/// The serve loop turns this into an OWNER-SHARD SET: if every key resolves to one shard
/// the whole command routes there (local fast path if that shard is home, else a remote
/// hop), and if the keys span more than one shard the command stays home (the Stage 2
/// fan-out gap). [`KeySpec::None`] (no routable key, or a command we conservatively do not
/// route) and a malformed/short request both keep the command home.
///
/// The borrowed `&[u8]` keys point into the [`Request`]'s `Bytes` args, so a [`KeySpec`]
/// borrows the request and never copies key bytes (the single-key fast path stays alloc
/// free; the multi-key path allocates only a tiny `Vec` of borrowed slices).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySpec<'a> {
    /// The command has no routable key (or is conservatively kept home): route home.
    None,
    /// Exactly one key (the common case, incl. single-key uses of variadic commands).
    One(&'a [u8]),
    /// Two or more keys (e.g. RENAME, BITOP, DEL k1 k2, SINTERSTORE dst src ...).
    Many(Vec<&'a [u8]>),
}

/// Classify the UPPERCASED command token into its [`CommandClass`].
///
/// This is now a THIN WRAPPER over the #89 single-source-of-truth command registry
/// ([`crate::command_spec::spec_of`]): the class is the `class` field of the command's
/// [`crate::command_spec::CommandSpec`]. An UNKNOWN token maps to
/// [`CommandClass::AlwaysHome`] exactly as the legacy match's `_ =>` arm did, so a command
/// the registry does not know stays on the accepting shard (the home handler then emits the
/// proper unknown-command error).
///
/// CONSERVATIVE BY DESIGN (preserved from the legacy table): a command is
/// [`CommandClass::KeyedSingle`] ONLY when it keys on `args[1]` and runs a
/// [`crate::ConnState`]-free handler. The [`KeyedSingle`](CommandClass::KeyedSingle) set is
/// audited against the keyed-data dispatch arms by
/// [`tests::keyed_single_commands_are_connstate_free`].
#[must_use]
pub fn classify(cmd_upper: &[u8]) -> CommandClass {
    crate::command_spec::spec_of(cmd_upper).map_or(CommandClass::AlwaysHome, |s| s.class)
}

/// The single routing key of a [`CommandClass::KeyedSingle`] command: `args[1]`.
///
/// Returns `None` only for a malformed request that lacks `args[1]` (a 1-element
/// request). The caller has already classified the command as `KeyedSingle`, so on a
/// well-formed command this is always `Some`; a `None` makes the caller fall back to the
/// home path (where the handler returns the proper wrong-arity error), so a malformed
/// keyed command is never mis-routed.
#[must_use]
pub fn single_key(req: &Request) -> Option<&[u8]> {
    req.args.get(1).map(bytes::Bytes::as_ref)
}

/// Extract the routing KEY(s) of `cmd_upper` from `req` (COORDINATOR.md #107, Stage 1).
///
/// This is now a THIN WRAPPER over the #89 single-source-of-truth command registry: it
/// looks the command's [`crate::command_spec::KeySpecKind`] up via
/// [`crate::command_spec::spec_of`] and runs the GENERIC per-pattern extraction
/// ([`crate::command_spec::extract_keys`]); an unknown command (no registry entry) yields
/// [`KeySpec::None`]. The per-pattern extraction logic (the `numkeys` parse, the MSET
/// stride, the dest+sources walk) is preserved EXACTLY, now in one place keyed by
/// `KeySpecKind`, so this function's observable output is byte-identical to the legacy
/// per-command match.
///
/// It returns the key positions Redis's command table defines, so the serve loop can
/// compute the command's owner-shard SET and route the WHOLE command to one shard when
/// every key co-locates (the local fast path if that shard is home, else a single remote
/// hop), or keep it home when the keys SPAN shards (the documented Stage 2 fan-out gap). It
/// is a PURE function of the bytes (no I/O, no state).
///
/// CONSERVATIVE BY DESIGN (preserved): a malformed/short request (an index past the end, an
/// unparseable `numkeys`) returns [`KeySpec::None`] so the home handler emits the proper
/// wrong-arity error rather than the routing layer guessing. The per-`KeySpecKind` key-spec
/// mapping (args[1] only / args[1..] / MSET stride / two keys / dest+sources / BITOP /
/// numkeys-prefixed / OBJECT args[2]) is documented on [`crate::command_spec::KeySpecKind`].
///
/// WATCH is [`CommandClass::AlwaysHome`] with `key_spec = Arg1`, but the serve loop never
/// calls `command_keys` for an AlwaysHome command (it reads WATCH's keys directly), so
/// WATCH is never routed via this path.
#[must_use]
pub fn command_keys<'a>(cmd_upper: &[u8], req: &'a Request) -> KeySpec<'a> {
    match crate::command_spec::spec_of(cmd_upper).map(|s| s.key_spec) {
        Some(kind) => crate::command_spec::extract_keys(kind, req),
        None => KeySpec::None,
    }
}

/// The single owning shard of a command's [`KeySpec`], or `None` if it does not route to
/// exactly one shard (COORDINATOR.md #107, Stage 1).
///
/// - [`KeySpec::None`] -> `None` (no routable key: keep home).
/// - [`KeySpec::One`] -> `Some(owner_shard(key))`.
/// - [`KeySpec::Many`] -> `Some(s)` IFF every key maps to the SAME shard `s`, else `None`
///   (the keys SPAN >1 shard: keep home, the documented Stage 2 fan-out gap).
///
/// The serve loop routes the WHOLE command to the returned shard (local fast path if it is
/// home, else one remote hop); a `None` keeps the command home.
#[must_use]
pub fn owner_shard_set(spec: &KeySpec<'_>, n_shards: usize) -> Option<usize> {
    match spec {
        KeySpec::None => None,
        KeySpec::One(key) => Some(owner_shard(key, n_shards)),
        KeySpec::Many(keys) => {
            // Empty cannot occur (keys_at collapses 0 -> None, 1 -> One), but treat it as
            // "no single owner" defensively rather than indexing.
            let first = owner_shard(keys.first()?, n_shards);
            // SHARD-SPANNING multi-key DATA commands are DEFERRED to Stage 2 (the next PR):
            // true multi-shard fan-out / reassembly is NOT done here. When the keys span
            // more than one shard we return None and the serve loop keeps the command HOME.
            if keys.iter().all(|k| owner_shard(k, n_shards) == first) {
                Some(first)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    #[test]
    fn hash64_matches_known_fnv1a_vectors() {
        // Canonical FNV-1a 64-bit test vectors (the empty string is the offset basis).
        assert_eq!(hash64(b""), FNV_OFFSET_BASIS);
        assert_eq!(hash64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(hash64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn owner_shard_is_deterministic() {
        // The same key always routes to the same shard (seedless, ADR-0003). Hammer it
        // so a randomly-seeded hash (the bug this guards) could not pass by luck.
        for key in [b"foo".as_slice(), b"bar", b"a:b:c", b"", b"\x00\xff"] {
            let first = owner_shard(key, 8);
            for _ in 0..1000 {
                assert_eq!(owner_shard(key, 8), first, "owner_shard not deterministic");
            }
            assert!(first < 8, "owner index in range");
        }
    }

    #[test]
    fn owner_shard_n_equals_one_is_always_zero() {
        // shards=1: every key is owned by shard 0 (the byte-identical-to-today path).
        for key in [b"foo".as_slice(), b"bar", b"baz", b""] {
            assert_eq!(owner_shard(key, 1), 0);
        }
    }

    #[test]
    fn owner_shard_distribution_is_balanced_over_10k_keys() {
        // 10k synthetic keys over n=8: each shard should land within ~50-150% of the
        // mean (1250). FNV-1a spreads short distinct keys well; a badly-seeded or
        // truncated hash would skew this.
        let n = 8usize;
        let total = 10_000usize;
        let mut counts = vec![0usize; n];
        for i in 0..total {
            let key = format!("key:{i}");
            counts[owner_shard(key.as_bytes(), n)] += 1;
        }
        let mean = total / n;
        let lo = mean / 2; // 50%
        let hi = mean + mean / 2; // 150%
        for (shard, &c) in counts.iter().enumerate() {
            assert!(
                c >= lo && c <= hi,
                "shard {shard} got {c} keys, expected within [{lo}, {hi}] (mean {mean})"
            );
        }
    }

    #[test]
    fn owner_shard_is_the_slot_owner_and_respects_hash_tags() {
        // #517: owner_shard is now derived from the CRC16 cluster slot, so it MUST equal
        // slot_to_shard(key_slot(key), n) for every key, and it MUST apply the {tag} hash-tag rule
        // (a key and its hash-tag share a slot -> the same owner shard).
        let n = 8usize;
        for key in [
            b"user:1".as_slice(),
            b"session:abc",
            b"{u1}.name",
            b"{u1}.email",
            b"plainkey",
        ] {
            assert_eq!(
                owner_shard(key, n),
                slot_to_shard(ironcache_protocol::key_slot(key), n),
                "owner_shard must equal the slot owner"
            );
        }
        // HASH-TAG CO-LOCATION (the #517 bonus): keys sharing a `{tag}` land on ONE internal shard,
        // so a hash-tagged multi-key command / MULTI-EXEC stays home-only (no cross-shard fan-out).
        assert_eq!(
            owner_shard(b"{acct42}.balance", n),
            owner_shard(b"{acct42}.history", n),
            "hash-tagged keys must co-locate on one shard"
        );
    }

    #[test]
    fn slot_to_shard_is_total_and_shard_slot_range_partitions_the_space() {
        // Every slot in [0, 16384) maps to exactly one shard in [0, n), and the per-shard ranges
        // partition the whole slot space with no gap or overlap -- the invariant the single-node
        // CLUSTER SLOTS projection relies on.
        for n in [1usize, 2, 3, 4, 8, 15, 16] {
            // Totality + in-range.
            for slot in 0u16..ironcache_protocol::CLUSTER_SLOTS {
                let s = slot_to_shard(slot, n);
                assert!(s < n, "slot {slot} -> shard {s} out of range for n={n}");
            }
            // Ranges partition [0, 16384): range[i].end == range[i+1].start, cover the whole space,
            // and a slot maps to shard i iff it is in range i.
            let mut prev_end = 0u16;
            for i in 0..n {
                let [start, end] = shard_slot_range(i, n);
                assert_eq!(start, prev_end, "gap/overlap at shard {i} for n={n}");
                assert!(
                    end >= start,
                    "empty-or-inverted range at shard {i} for n={n}"
                );
                for slot in start..end {
                    assert_eq!(
                        slot_to_shard(slot, n),
                        i,
                        "slot {slot} not owned by shard {i}"
                    );
                }
                prev_end = end;
            }
            assert_eq!(
                prev_end,
                ironcache_protocol::CLUSTER_SLOTS,
                "ranges must cover 16384 for n={n}"
            );
        }
    }

    #[test]
    fn classify_spot_checks() {
        // KeyedSingle: representative single-key commands.
        for c in [
            b"GET".as_slice(),
            b"SET",
            b"INCR",
            b"APPEND",
            b"EXPIRE",
            b"TTL",
            b"LPUSH",
            b"HSET",
            b"SADD",
            b"ZADD",
            b"SETBIT",
            b"PFADD",
            b"GETEX",
        ] {
            assert_eq!(classify(c), CommandClass::KeyedSingle, "{c:?} KeyedSingle");
        }
        // KeyedMulti: multi-key commands stay home this pass.
        for c in [
            b"DEL".as_slice(),
            b"EXISTS",
            b"RENAME",
            b"COPY",
            b"SINTER",
            b"BITOP",
            b"PFCOUNT",
            b"PFMERGE",
            b"LMOVE",
            b"SMOVE",
            b"OBJECT",
            b"ZUNIONSTORE",
            b"MGET",
            b"MSET",
        ] {
            assert_eq!(classify(c), CommandClass::KeyedMulti, "{c:?} KeyedMulti");
        }
        // WholeKeyspace.
        for c in [
            b"KEYS".as_slice(),
            b"SCAN",
            b"DBSIZE",
            b"FLUSHALL",
            b"FLUSHDB",
            b"RANDOMKEY",
        ] {
            assert_eq!(
                classify(c),
                CommandClass::WholeKeyspace,
                "{c:?} WholeKeyspace"
            );
        }
        // AlwaysHome: control / connection / transaction / probes.
        for c in [
            b"PING".as_slice(),
            b"ECHO",
            b"HELLO",
            b"AUTH",
            b"SELECT",
            b"QUIT",
            b"RESET",
            b"MULTI",
            b"EXEC",
            b"DISCARD",
            b"WATCH",
            b"UNWATCH",
            b"CLIENT",
            b"COMMAND",
            b"INFO",
            b"CONFIG",
            // SWAPDB takes no key (it swaps two whole logical DBs by index): a HOME-only
            // control op this stage, so AlwaysHome (NOT KeyedMulti).
            b"SWAPDB",
            b"FROBNICATE", // unknown command -> home (handler emits the proper error)
        ] {
            assert_eq!(classify(c), CommandClass::AlwaysHome, "{c:?} AlwaysHome");
        }
    }

    #[test]
    fn single_key_is_args_1() {
        assert_eq!(single_key(&req(&[b"GET", b"foo"])), Some(b"foo".as_slice()));
        assert_eq!(
            single_key(&req(&[b"SET", b"k", b"v"])),
            Some(b"k".as_slice())
        );
        // A malformed 1-element request has no args[1]: the caller falls back home.
        assert_eq!(single_key(&req(&[b"GET"])), None);
    }

    /// `command_keys` KEY SPEC: the per-command key positions match the Redis key specs.
    #[test]
    fn command_keys_key_spec_table() {
        // args[1..] all keys: DEL / EXISTS / UNLINK / TOUCH / SINTER / PFCOUNT / PFMERGE.
        assert_eq!(
            command_keys(b"DEL", &req(&[b"DEL", b"a", b"b", b"c"])),
            KeySpec::Many(vec![b"a", b"b", b"c"])
        );
        assert_eq!(
            command_keys(b"EXISTS", &req(&[b"EXISTS", b"k"])),
            KeySpec::One(b"k")
        );
        assert_eq!(
            command_keys(b"PFCOUNT", &req(&[b"PFCOUNT", b"h1", b"h2"])),
            KeySpec::Many(vec![b"h1", b"h2"])
        );
        // BITOP: dest=args[2] + sources=args[3..]; args[1] is the OPERATION, NOT a key.
        assert_eq!(
            command_keys(b"BITOP", &req(&[b"BITOP", b"AND", b"dest", b"s1", b"s2"])),
            KeySpec::Many(vec![b"dest", b"s1", b"s2"])
        );
        // A too-short BITOP (no source) is malformed -> None (home, proper error there).
        assert_eq!(
            command_keys(b"BITOP", &req(&[b"BITOP", b"AND", b"dest"])),
            KeySpec::None
        );
        // ZUNIONSTORE dest numkeys k1 k2: dest joins the routed set.
        assert_eq!(
            command_keys(
                b"ZUNIONSTORE",
                &req(&[b"ZUNIONSTORE", b"dest", b"2", b"k1", b"k2"])
            ),
            KeySpec::Many(vec![b"dest", b"k1", b"k2"])
        );
        // A numkeys that overruns the args is malformed -> None.
        assert_eq!(
            command_keys(
                b"ZUNIONSTORE",
                &req(&[b"ZUNIONSTORE", b"dest", b"5", b"k1"])
            ),
            KeySpec::None
        );
        // ZUNION numkeys k1 k2 (no dest): keys=args[2..2+numkeys].
        assert_eq!(
            command_keys(b"ZUNION", &req(&[b"ZUNION", b"2", b"z1", b"z2"])),
            KeySpec::Many(vec![b"z1", b"z2"])
        );
        // SINTERCARD numkeys k1 k2 [LIMIT n]: the trailing LIMIT is NOT a key.
        assert_eq!(
            command_keys(
                b"SINTERCARD",
                &req(&[b"SINTERCARD", b"2", b"s1", b"s2", b"LIMIT", b"3"])
            ),
            KeySpec::Many(vec![b"s1", b"s2"])
        );
        // MOVE has exactly ONE key (args[1]); args[2] is the destination DB INDEX, not a key.
        assert_eq!(
            command_keys(b"MOVE", &req(&[b"MOVE", b"thekey", b"1"])),
            KeySpec::One(b"thekey")
        );
        // OBJECT <subcommand> <key>: key is args[2].
        assert_eq!(
            command_keys(b"OBJECT", &req(&[b"OBJECT", b"ENCODING", b"thekey"])),
            KeySpec::One(b"thekey")
        );
        // RENAME / two-key commands: args[1] and args[2].
        assert_eq!(
            command_keys(b"RENAME", &req(&[b"RENAME", b"src", b"dst"])),
            KeySpec::Many(vec![b"src", b"dst"])
        );
        assert_eq!(
            command_keys(b"SMOVE", &req(&[b"SMOVE", b"src", b"dst", b"member"])),
            KeySpec::Many(vec![b"src", b"dst"])
        );
        // SINTERSTORE dest src1 src2: dest=args[1] + sources=args[2..].
        assert_eq!(
            command_keys(
                b"SINTERSTORE",
                &req(&[b"SINTERSTORE", b"dest", b"s1", b"s2"])
            ),
            KeySpec::Many(vec![b"dest", b"s1", b"s2"])
        );
        // MGET: all of args[1..] are keys (like DEL).
        assert_eq!(
            command_keys(b"MGET", &req(&[b"MGET", b"a", b"b", b"c"])),
            KeySpec::Many(vec![b"a", b"b", b"c"])
        );
        assert_eq!(
            command_keys(b"MGET", &req(&[b"MGET", b"k"])),
            KeySpec::One(b"k")
        );
        // MSET: keys at args[1], args[3], ... (every other arg); values are NOT keys.
        assert_eq!(
            command_keys(
                b"MSET",
                &req(&[b"MSET", b"k1", b"v1", b"k2", b"v2", b"k3", b"v3"])
            ),
            KeySpec::Many(vec![b"k1", b"k2", b"k3"])
        );
        // A single-pair MSET routes by its one key.
        assert_eq!(
            command_keys(b"MSET", &req(&[b"MSET", b"k", b"v"])),
            KeySpec::One(b"k")
        );
        // A malformed (odd-arg) MSET -> None (home, proper wrong-arity there).
        assert_eq!(
            command_keys(b"MSET", &req(&[b"MSET", b"k1", b"v1", b"k2"])),
            KeySpec::None
        );
        // An empty MSET (no pair) -> None.
        assert_eq!(command_keys(b"MSET", &req(&[b"MSET"])), KeySpec::None);
        // A KeyedSingle command (the fallback arm): args[1] only.
        assert_eq!(
            command_keys(b"GET", &req(&[b"GET", b"k"])),
            KeySpec::One(b"k")
        );
        // SWAPDB takes no key -> the fallback arm reads args[1] (the DB index) but that is a
        // numeric, not a key; SWAPDB is AlwaysHome so the serve loop never calls command_keys
        // for it. command_keys is total, so document its raw output is NOT used for routing.
        // A malformed (1-element) keyed command -> None (home).
        assert_eq!(command_keys(b"DEL", &req(&[b"DEL"])), KeySpec::None);
        assert_eq!(command_keys(b"GET", &req(&[b"GET"])), KeySpec::None);
    }

    #[test]
    fn owner_shard_set_single_and_colocated_and_spanning() {
        // A single key always routes to its owner.
        let one = KeySpec::One(b"k");
        assert_eq!(owner_shard_set(&one, 4), Some(owner_shard(b"k", 4)));
        // None never routes.
        assert_eq!(owner_shard_set(&KeySpec::None, 4), None);
        // shards == 1: every KeySpec collapses to Some(0) (the byte-identical-to-today path).
        assert_eq!(owner_shard_set(&KeySpec::One(b"a"), 1), Some(0));
        assert_eq!(
            owner_shard_set(&KeySpec::Many(vec![b"a", b"b", b"c"]), 1),
            Some(0)
        );
        // Find two keys that LAND on the SAME shard (co-located) and two that SPAN shards,
        // over n=8, then assert the all-same vs spanning behavior directly.
        let n = 8usize;
        let mut same: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut span: Option<(Vec<u8>, Vec<u8>)> = None;
        for i in 0..200u32 {
            for j in (i + 1)..200u32 {
                let a = format!("ck:{i}").into_bytes();
                let b = format!("ck:{j}").into_bytes();
                if owner_shard(&a, n) == owner_shard(&b, n) {
                    same.get_or_insert((a.clone(), b.clone()));
                } else {
                    span.get_or_insert((a.clone(), b.clone()));
                }
            }
        }
        let (a, b) = same.expect("two co-located keys exist over 200 keys / 8 shards");
        let spec = KeySpec::Many(vec![a.as_slice(), b.as_slice()]);
        assert_eq!(
            owner_shard_set(&spec, n),
            Some(owner_shard(&a, n)),
            "co-located multi-key routes to the shared owner"
        );
        let (a, b) = span.expect("two spanning keys exist over 200 keys / 8 shards");
        let spec = KeySpec::Many(vec![a.as_slice(), b.as_slice()]);
        assert_eq!(
            owner_shard_set(&spec, n),
            None,
            "shard-spanning multi-key stays home (the Stage 2 fan-out gap)"
        );
    }

    /// AUDIT (the cross-check the task asks for): every command this module classifies as
    /// [`CommandClass::KeyedSingle`] is a command whose `dispatch_inner` arm runs WITHOUT
    /// touching `ConnState` (it takes only `store`/`wheel`/`db`/`now`, plus `env` for the
    /// RNG-drawing members), so `dispatch_remote_keyed` (which has NO ConnState) can run
    /// the identical arm body remotely. This list is the literal audit of the dispatch
    /// arms (dispatch.rs): each KeyedSingle command below was read in the match and
    /// confirmed to call a `cmd_*` handler with no `state` argument.
    ///
    /// If a future change makes a KeyedSingle command consult ConnState, this audit list
    /// (kept in lockstep with `classify`) and the dispatch arm must move it OUT of
    /// KeyedSingle. The two `KeyedSingle` enumerations (here and in `classify`) are the
    /// single source; a divergence is caught by the `classify` round-trip below.
    // The audit list is one command per line (the exhaustive KeyedSingle set), which is
    // the intended shape for a literal hand-audit; the line-count lint is allowed for that
    // reason (as the dispatch big-match arms allow it for the same "long-but-flat" reason).
    #[allow(clippy::too_many_lines)]
    #[test]
    fn keyed_single_commands_are_connstate_free() {
        // The audited ConnState-free single-key commands (the exact KeyedSingle set).
        const KEYED_SINGLE: &[&[u8]] = &[
            b"GET",
            b"SET",
            b"SETNX",
            b"GETSET",
            b"STRLEN",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"DECRBY",
            b"INCRBYFLOAT",
            b"APPEND",
            b"SETEX",
            b"PSETEX",
            b"GETEX",
            b"TYPE",
            b"EXPIRE",
            b"PEXPIRE",
            b"EXPIREAT",
            b"PEXPIREAT",
            b"TTL",
            b"PTTL",
            b"EXPIRETIME",
            b"PEXPIRETIME",
            b"PERSIST",
            b"LPUSH",
            b"RPUSH",
            b"LPUSHX",
            b"RPUSHX",
            b"LPOP",
            b"RPOP",
            b"LLEN",
            b"LRANGE",
            b"LINDEX",
            b"LSET",
            b"LINSERT",
            b"LREM",
            b"LTRIM",
            b"LPOS",
            b"HSET",
            b"HMSET",
            b"HSETNX",
            b"HGET",
            b"HMGET",
            b"HDEL",
            b"HGETALL",
            b"HKEYS",
            b"HVALS",
            b"HLEN",
            b"HEXISTS",
            b"HSTRLEN",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            b"HRANDFIELD",
            b"HSCAN",
            b"SADD",
            b"SREM",
            b"SMEMBERS",
            b"SISMEMBER",
            b"SMISMEMBER",
            b"SCARD",
            b"SPOP",
            b"SRANDMEMBER",
            b"SSCAN",
            b"ZADD",
            b"ZINCRBY",
            b"ZREM",
            b"ZSCORE",
            b"ZMSCORE",
            b"ZCARD",
            b"ZRANK",
            b"ZREVRANK",
            b"ZCOUNT",
            b"ZLEXCOUNT",
            b"ZRANGE",
            b"ZREVRANGE",
            b"ZRANGEBYSCORE",
            b"ZREVRANGEBYSCORE",
            b"ZRANGEBYLEX",
            b"ZREVRANGEBYLEX",
            b"ZREMRANGEBYRANK",
            b"ZREMRANGEBYSCORE",
            b"ZREMRANGEBYLEX",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"ZRANDMEMBER",
            b"ZSCAN",
            b"SETBIT",
            b"GETBIT",
            b"BITCOUNT",
            b"BITPOS",
            b"BITFIELD",
            b"BITFIELD_RO",
            b"PFADD",
        ];
        for c in KEYED_SINGLE {
            assert_eq!(
                classify(c),
                CommandClass::KeyedSingle,
                "{c:?} must classify KeyedSingle (audit and classify diverged)"
            );
        }
        // And the inverse: no command outside the audit list classifies KeyedSingle by
        // accident among the multi-key / whole-keyspace / control sets we know stay home.
        for c in [
            b"DEL".as_slice(),
            b"BITOP",
            b"PFCOUNT",
            b"OBJECT",
            b"SCAN",
            b"PING",
            b"EXEC",
        ] {
            assert_ne!(
                classify(c),
                CommandClass::KeyedSingle,
                "{c:?} must stay home"
            );
        }
    }
}
