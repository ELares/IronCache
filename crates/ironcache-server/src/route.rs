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
//! ## The hash is the INTERNAL shard hash, NOT the client-visible cluster hash
//!
//! [`hash64`] is FNV-1a, a fast non-cryptographic hash used ONLY to map a key to an
//! owning shard inside this process. It is deliberately NOT the client-visible
//! cluster hash: CRC16/XMODEM with the `{hashtag}` rule (CLUSTER_CONTRACT.md) is
//! RESERVED for the cluster / sharded-pubsub slot assignment a client can observe and
//! depend on. The two MUST NOT be conflated: the internal shard hash may change with
//! the shard count and is never exposed on the wire, whereas the cluster slot hash is
//! a stable wire contract. Keep this hash off any client-facing slot computation.
//!
//! ## Determinism (ADR-0003)
//!
//! [`hash64`] is a PURE function of the key bytes: the same key always hashes to the
//! same value, with no random seed. This is required so a key's owning shard is stable
//! across a seeded replay (a randomly-seeded `DefaultHasher`/SipHash would route the
//! same key to different shards on different boots, breaking replay determinism), and
//! so two connections on different shards agree on where a key lives.

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

/// The shard that OWNS `key` given `n_shards` shards: `hash64(key) % n_shards`.
///
/// # Panics
///
/// Panics in debug builds if `n_shards == 0` (the precondition: a running server has
/// at least one shard). The serve loop computes `n_shards` as `config.shards.max(1)`,
/// so this never fires in practice.
#[must_use]
pub fn owner_shard(key: &[u8], n_shards: usize) -> usize {
    debug_assert!(n_shards >= 1, "owner_shard requires n_shards >= 1");
    let n = n_shards.max(1) as u64;
    usize::try_from(hash64(key) % n).expect("modulo n_shards fits usize")
}

/// How a command must be routed across shards (COORDINATOR.md #107).
///
/// STAGE 1 routes any KEYED command (single- or multi-key) whose keys ALL resolve to ONE
/// shard to that shard (via [`command_keys`]); a key-SPANNING multi-key command, and the
/// whole-keyspace commands, stay on the home shard (the documented Stage 2 gap), and
/// [`CommandClass::AlwaysHome`] commands (no key / control / conn / txn) stay home always.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandClass {
    /// Control / connection / transaction commands (and PING/ECHO/INFO/CONFIG/...).
    /// These never touch a single owned key, so they ALWAYS run on the home shard.
    AlwaysHome,
    /// A single-key data command whose key is `args[1]` and whose handler touches only
    /// the store/wheel/db/now/env-rng (no [`crate::ConnState`]). Routed to the owning
    /// shard via the zero-alloc single-key fast path in the serve loop.
    KeyedSingle,
    /// A multi-key data command (DEL/EXISTS/SINTER/BITOP/PFCOUNT/RENAME/COPY/...). Its
    /// keys are extracted by [`command_keys`]; if they ALL resolve to ONE shard the WHOLE
    /// command routes there, else it stays home (the Stage 2 fan-out gap). Like
    /// [`CommandClass::KeyedSingle`], its handler is [`crate::ConnState`]-free and runs via
    /// the shared keyed-data arms, so it executes correctly on the owning shard.
    KeyedMulti,
    /// A whole-keyspace command (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY). STAGE 1
    /// keeps these home (single-shard scope, as today); a later pass fans them out.
    WholeKeyspace,
}

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
/// CONSERVATIVE BY DESIGN (correctness over coverage this pass): a command is
/// [`CommandClass::KeyedSingle`] ONLY when it has been VERIFIED to (a) key on `args[1]`
/// and (b) run a handler that touches no [`crate::ConnState`]. Anything whose single key
/// is not at `args[1]` (e.g. `OBJECT ENCODING key`, where the key is `args[2]`), anything
/// multi-key, anything whole-keyspace, and anything unrecognized falls into a HOME class
/// so it keeps running on the accepting shard exactly as before this pass.
///
/// The [`KeyedSingle`](CommandClass::KeyedSingle) set is hand-synced with the keyed-data
/// arms in `dispatch::dispatch_keyed_data` and audited by
/// [`tests::keyed_single_commands_are_connstate_free`].
#[must_use]
pub fn classify(cmd_upper: &[u8]) -> CommandClass {
    match cmd_upper {
        // -- KeyedSingle: single-key data commands keyed on args[1], ConnState-free.
        // Strings + numerics + APPEND (all key args[1]).
        b"GET" | b"SET" | b"SETNX" | b"GETSET" | b"STRLEN" | b"INCR" | b"DECR" | b"INCRBY"
        | b"DECRBY" | b"INCRBYFLOAT" | b"APPEND"
        // SET-with-TTL helpers (key args[1]).
        | b"SETEX" | b"PSETEX" | b"GETEX"
        // Single-key keyspace introspection / TTL family (key args[1]).
        | b"TYPE" | b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT" | b"TTL" | b"PTTL"
        | b"EXPIRETIME" | b"PEXPIRETIME" | b"PERSIST"
        // List (all key args[1]); LMOVE/RPOPLPUSH are MULTI-key (src+dst) -> not here.
        | b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" | b"LPOP" | b"RPOP" | b"LLEN"
        | b"LRANGE" | b"LINDEX" | b"LSET" | b"LINSERT" | b"LREM" | b"LTRIM" | b"LPOS"
        // Hash (all key args[1]).
        | b"HSET" | b"HMSET" | b"HSETNX" | b"HGET" | b"HMGET" | b"HDEL" | b"HGETALL"
        | b"HKEYS" | b"HVALS" | b"HLEN" | b"HEXISTS" | b"HSTRLEN" | b"HINCRBY"
        | b"HINCRBYFLOAT" | b"HRANDFIELD" | b"HSCAN"
        // Set single-key ops (key args[1]); SMOVE + the *STORE/algebra reads are MULTI-key.
        | b"SADD" | b"SREM" | b"SMEMBERS" | b"SISMEMBER" | b"SMISMEMBER" | b"SCARD"
        | b"SPOP" | b"SRANDMEMBER" | b"SSCAN"
        // Sorted-set single-key ops (key args[1]); the *STORE/union/inter/diff are MULTI-key.
        | b"ZADD" | b"ZINCRBY" | b"ZREM" | b"ZSCORE" | b"ZMSCORE" | b"ZCARD" | b"ZRANK"
        | b"ZREVRANK" | b"ZCOUNT" | b"ZLEXCOUNT" | b"ZRANGE" | b"ZREVRANGE"
        | b"ZRANGEBYSCORE" | b"ZREVRANGEBYSCORE" | b"ZRANGEBYLEX" | b"ZREVRANGEBYLEX"
        | b"ZREMRANGEBYRANK" | b"ZREMRANGEBYSCORE" | b"ZREMRANGEBYLEX" | b"ZPOPMIN"
        | b"ZPOPMAX" | b"ZRANDMEMBER" | b"ZSCAN"
        // Bitmap single-key ops (key args[1]); BITOP is MULTI-key (dest + sources).
        | b"SETBIT" | b"GETBIT" | b"BITCOUNT" | b"BITPOS" | b"BITFIELD" | b"BITFIELD_RO"
        // HyperLogLog single-key add (key args[1]); PFCOUNT/PFMERGE are MULTI-key.
        | b"PFADD" => CommandClass::KeyedSingle,

        // -- KeyedMulti: multi-key (or non-args[1]-keyed) data commands. STAGE 1 extracts
        // their key(s) via `command_keys` and routes the whole command to the owning shard
        // when every key resolves to ONE shard (e.g. a single-key DEL/EXISTS/PFCOUNT, or a
        // co-located RENAME); a key-SPANNING invocation stays home (the Stage 2 fan-out
        // gap). All these handlers are ConnState-free (`dispatch_keyed_data` arms), so they
        // run correctly on the owning shard. MOVE has exactly ONE key (args[1]; args[2] is
        // the destination DB index, not a key). OBJECT's key is args[2] (the subcommand is
        // args[1]); `command_keys` extracts args[2] so a single OBJECT routes correctly.
        b"DEL" | b"EXISTS" | b"UNLINK" | b"TOUCH"
        | b"RENAME" | b"RENAMENX" | b"COPY" | b"MOVE"
        | b"SMOVE" | b"SINTER" | b"SUNION" | b"SDIFF" | b"SINTERCARD"
        | b"SINTERSTORE" | b"SUNIONSTORE" | b"SDIFFSTORE"
        | b"ZUNION" | b"ZINTER" | b"ZDIFF" | b"ZINTERCARD"
        | b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" | b"ZRANGESTORE"
        | b"LMOVE" | b"RPOPLPUSH"
        | b"BITOP" | b"PFCOUNT" | b"PFMERGE"
        | b"OBJECT" => CommandClass::KeyedMulti,

        // SWAPDB swaps two whole logical DBs by index (no key): it is a HOME-only control
        // operation this stage (a true cross-shard SWAPDB is a later pass), so it is
        // AlwaysHome below, NOT KeyedMulti.

        // -- WholeKeyspace: span the whole keyspace (stay home this pass).
        b"KEYS" | b"SCAN" | b"DBSIZE" | b"FLUSHALL" | b"FLUSHDB" | b"RANDOMKEY" => {
            CommandClass::WholeKeyspace
        }

        // -- AlwaysHome: everything else (control / connection / transaction / probes).
        _ => CommandClass::AlwaysHome,
    }
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

/// Parse `args[i]` as a NON-NEGATIVE decimal integer (a `numkeys`-style count). Returns
/// `None` on a non-numeric / negative / overflowing token, so the caller falls back HOME
/// (where the handler emits the proper error) rather than mis-routing a malformed command.
fn parse_count(arg: &[u8]) -> Option<usize> {
    // ASCII digits only (no sign, no whitespace): a `numkeys` is a bare non-negative int.
    if arg.is_empty() || !arg.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(arg).ok()?.parse::<usize>().ok()
}

/// Collect `req.args[start..]` (all trailing args) as borrowed key slices into a
/// [`KeySpec`], collapsing to [`KeySpec::One`] / [`KeySpec::None`] for 1 / 0 keys so the
/// caller's single-key fast path stays alloc-free.
fn keys_from(req: &Request, start: usize) -> KeySpec<'_> {
    let Some(tail) = req.args.get(start..) else {
        return KeySpec::None; // start past the end: malformed -> home.
    };
    match tail {
        [] => KeySpec::None,
        [one] => KeySpec::One(one.as_ref()),
        many => KeySpec::Many(many.iter().map(bytes::Bytes::as_ref).collect()),
    }
}

/// Collect the CONTIGUOUS range `req.args[start..end]` as borrowed key slices into a
/// [`KeySpec`]. An out-of-range `end` (a `numkeys` that overruns the args) yields
/// [`KeySpec::None`] -> home (the proper error). 0 -> `None`, 1 -> `One`, else `Many`.
fn keys_range(req: &Request, start: usize, end: usize) -> KeySpec<'_> {
    let Some(slice) = req.args.get(start..end) else {
        return KeySpec::None;
    };
    match slice {
        [] => KeySpec::None,
        [one] => KeySpec::One(one.as_ref()),
        many => KeySpec::Many(many.iter().map(bytes::Bytes::as_ref).collect()),
    }
}

/// Collect the args at `idxs` (each an index into `req.args`) as borrowed key slices.
/// An out-of-range index yields [`KeySpec::None`] (a malformed/short request -> home, where
/// the handler emits the proper wrong-arity error). 0 -> `None`, 1 -> `One`, else `Many`.
fn keys_at<'a>(req: &'a Request, idxs: &[usize]) -> KeySpec<'a> {
    let mut keys: Vec<&'a [u8]> = Vec::with_capacity(idxs.len());
    for &i in idxs {
        match req.args.get(i) {
            Some(b) => keys.push(b.as_ref()),
            None => return KeySpec::None,
        }
    }
    match keys.len() {
        0 => KeySpec::None,
        1 => KeySpec::One(keys[0]),
        _ => KeySpec::Many(keys),
    }
}

/// Extract the routing KEY(s) of `cmd_upper` from `req` (COORDINATOR.md #107, Stage 1).
///
/// This is the per-command KEY SPEC: it returns the key positions Redis's command table
/// defines, so the serve loop can compute the command's owner-shard SET and route the
/// WHOLE command to one shard when every key co-locates (the local fast path if that shard
/// is home, else a single remote hop), or keep it home when the keys SPAN shards (the
/// documented Stage 2 fan-out gap). It is a PURE function of the bytes (no I/O, no state).
///
/// CONSERVATIVE BY DESIGN (correctness over coverage): a command whose key positions are
/// not confidently known returns [`KeySpec::None`] (keep home). A malformed/short request
/// (an index past the end, an unparseable `numkeys`) also returns [`KeySpec::None`] so the
/// home handler emits the proper wrong-arity error rather than the routing layer guessing.
///
/// ## Key-spec table (matches the Redis command key specs)
///
/// - `args[1]` only (single key): every [`CommandClass::KeyedSingle`] command, plus MOVE
///   (args[2] is the destination DB index, NOT a key).
/// - `args[1..]` all keys: DEL, EXISTS, UNLINK, TOUCH, SINTER, SUNION, SDIFF, PFCOUNT,
///   PFMERGE.
/// - `args[1]` + `args[2]` (two keys): RENAME, RENAMENX, COPY (options follow the two
///   keys), SMOVE, LMOVE, RPOPLPUSH, ZRANGESTORE (dest, src).
/// - dest `args[1]` + sources `args[2..]`: SINTERSTORE, SUNIONSTORE, SDIFFSTORE.
/// - BITOP: dest `args[2]` + sources `args[3..]` (args[1] is the OPERATION, not a key).
/// - `numkeys`-prefixed: SINTERCARD/ZINTERCARD (numkeys=args[1], keys=args[2..2+numkeys]);
///   ZUNION/ZINTER/ZDIFF (numkeys=args[1], keys=args[2..2+numkeys]);
///   ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE (dest=args[1], numkeys=args[2],
///   keys=args[3..3+numkeys]) -- the dest joins the routed key set so a co-located
///   store routes too.
/// - OBJECT: key at `args[2]` (the subcommand is args[1]).
///
/// WATCH is deliberately ABSENT (it is [`CommandClass::AlwaysHome`]): it must stay on the
/// home shard with the per-connection transaction state (the cross-shard WATCH is a later
/// transaction pass), so it is never routed.
#[must_use]
pub fn command_keys<'a>(cmd_upper: &[u8], req: &'a Request) -> KeySpec<'a> {
    match cmd_upper {
        // dest=args[1], numkeys=args[2], keys=args[3..3+numkeys]; the DEST also joins the
        // routed key set, so a store whose dest + sources co-locate routes to that shard.
        b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" => {
            let Some(numkeys) = req.args.get(2).and_then(|a| parse_count(a)) else {
                return KeySpec::None;
            };
            // Guard against a numkeys that overruns the args (malformed -> home). dest is
            // args[1] and joins the routed key set, so the source span starts at args[3].
            if numkeys == 0 || 3usize.saturating_add(numkeys) > req.args.len() {
                return KeySpec::None;
            }
            let mut idxs = Vec::with_capacity(1 + numkeys);
            idxs.push(1usize); // dest
            idxs.extend(3..3 + numkeys); // source keys
            keys_at(req, &idxs)
        }
        // numkeys=args[1], keys=args[2..2+numkeys]. ZINTERCARD/SINTERCARD have a trailing
        // LIMIT option AFTER the keys, but the keys themselves are exactly the numkeys span.
        b"ZUNION" | b"ZINTER" | b"ZDIFF" | b"ZINTERCARD" | b"SINTERCARD" => {
            let Some(numkeys) = req.args.get(1).and_then(|a| parse_count(a)) else {
                return KeySpec::None;
            };
            if numkeys == 0 || 2usize.saturating_add(numkeys) > req.args.len() {
                return KeySpec::None;
            }
            keys_range(req, 2, 2 + numkeys)
        }
        // BITOP <op> <dest> <src...>: args[1] is the operation (NOT a key); dest=args[2],
        // sources=args[3..].
        b"BITOP" => {
            if req.args.len() < 4 {
                return KeySpec::None;
            }
            keys_from(req, 2)
        }
        // Two keys at args[1], args[2] (extra options, if any, follow the keys).
        b"RENAME" | b"RENAMENX" | b"COPY" | b"SMOVE" | b"LMOVE" | b"RPOPLPUSH" | b"ZRANGESTORE" => {
            keys_at(req, &[1, 2])
        }
        // All of args[1..] are keys. This covers two key-spec shapes that happen to span the
        // SAME index range (so they share one arm):
        //   - dest=args[1] + sources=args[2..]: SINTERSTORE/SUNIONSTORE/SDIFFSTORE (the dest
        //     joins the routed set so a co-located store routes too);
        //   - every arg a key: DEL/EXISTS/UNLINK/TOUCH/SINTER/SUNION/SDIFF/PFCOUNT/PFMERGE.
        b"SINTERSTORE" | b"SUNIONSTORE" | b"SDIFFSTORE" | b"DEL" | b"EXISTS" | b"UNLINK"
        | b"TOUCH" | b"SINTER" | b"SUNION" | b"SDIFF" | b"PFCOUNT" | b"PFMERGE" => {
            keys_from(req, 1)
        }
        // OBJECT <subcommand> <key>: the key is args[2].
        b"OBJECT" => keys_at(req, &[2]),
        // MOVE has exactly ONE key (args[1]); args[2] is the destination DB INDEX, not a
        // key. So it routes by owner(args[1]) like a single-key command.
        b"MOVE" => keys_at(req, &[1]),
        // Anything else: the single-key fast path (every KeyedSingle command), or a command
        // we do not confidently route. A KeyedSingle command keys on args[1]; everything
        // else (control/conn/txn/whole-keyspace, and SWAPDB which takes no key) has no
        // routable key here and stays home. `single_key` already covers the args[1] case for
        // the serve loop's fast path; this arm makes `command_keys` total for completeness.
        _ => single_key(req).map_or(KeySpec::None, KeySpec::One),
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
