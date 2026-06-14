// SPDX-License-Identifier: MIT OR Apache-2.0
//! Object-introspection command handlers (OBJECT_ENCODING_MAPPING.md #40, ADR-0009).
//!
//! `OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ|HELP` reports the synthetic Redis
//! introspection a conformance suite and clients branch on, mapped from IronCache's
//! own representations (ADR-0009 behavioral equivalence): clients see Redis-vocabulary
//! names even though the internal layout differs.
//!
//! ## OBJECT ENCODING: the representation-to-name map and a recorded divergence
//!
//! ENCODING reports `int`/`embstr`/`raw` for strings, read off the value's current
//! [`ironcache_storage::Encoding`] via `encoding_name()` (the collection names land
//! with collections). There is ONE KNOWN BEHAVIORAL DIVERGENCE from Redis recorded
//! per ADR-0009: an `APPEND` whose result stays SHORT reports `embstr` (or `int`)
//! where Redis reports `raw`. Redis converts any APPENDed string to `raw`
//! unconditionally (a side effect of its in-place SDS growth), whereas IronCache's
//! APPEND rebuilds-and-reclassifies through the rmw waist (PR-2b), so a short result
//! reclassifies back to `embstr`/`int`. Fixing it needs the deferred in-place-mutation
//! waist extension (the `Mutate` action that would let APPEND grow a buffer without
//! reclassifying); it is deliberately NOT fixed here. A unit test asserts the CURRENT
//! (divergent) behavior, marked as a known divergence, so the conformance suite can
//! track it.
//!
//! ## REFCOUNT / IDLETIME / FREQ synthesis
//!
//! IDLETIME and FREQ both do the KEY LOOKUP FIRST and ONLY THEN check the LFU gate,
//! matching Redis (objectCommandLookupOrReply replies the null bulk on a missing key
//! BEFORE the maxmemory-policy check). So IDLETIME of a missing key under an LFU policy
//! is NULL (not the under-LFU error) and FREQ of a missing key under a non-LFU policy is
//! NULL (not the requires-LFU error).
//!
//! - REFCOUNT: Redis shares small integer objects (0..=9999) and reports their
//!   refcount as `OBJ_SHARED_REFCOUNT` = 2147483647 (INT_MAX); every other object
//!   reports 1. IronCache does not actually share objects, but reproduces the
//!   OBSERVABLE value: an int-encoded value in 0..=9999 reports 2147483647, else 1.
//! - IDLETIME: integer seconds since last access. IronCache does not track per-key
//!   access time yet, so for a present key it reports 0 (a fresh-access approximation)
//!   under a non-LFU policy, and ERRORS under an LFU policy (Redis: idle time is not
//!   tracked under LFU). The exact-idle-seconds tracking is a later follow-up.
//! - FREQ: the logarithmic access-frequency counter, read from the W-TinyLFU sketch
//!   estimate via the additive `Admit::access_freq` accessor. For a present key it ERRORS
//!   unless an LFU policy is selected (Redis gates FREQ on an `*-lfu` maxmemory-policy).

use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{Admit, Encoding, Store, UnixMillis};

/// `OBJECT <subcommand> [args]` (OBJECT_ENCODING_MAPPING.md #40). The store must
/// implement [`Admit`] too (for the FREQ sketch estimate + the LFU-policy gate).
pub fn cmd_object<S: Store + Admit>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("object"));
    }
    let sub = crate::cmd_util::ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"HELP" => object_help(),
        b"ENCODING" => object_encoding(store, db, now, req),
        b"REFCOUNT" => object_refcount(store, db, now, req),
        b"IDLETIME" => object_idletime(store, db, now, req),
        b"FREQ" => object_freq(store, db, now, req),
        _ => Value::error(ErrorReply::unknown_subcommand(
            "OBJECT",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// `OBJECT ENCODING key` -> a bulk string of the encoding name, or null if the key is
/// absent (Redis replies the null bulk, NOT an error, for a missing key here).
fn object_encoding<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        // A wrong arg count on an OBJECT subcommand is NOT a per-subcommand arity
        // error; Redis falls through to addReplySubcommandSyntaxError (the same wording
        // as an unknown subcommand). Reuse the subcommand token from req.args[1].
        return Value::error(ErrorReply::unknown_subcommand(
            "OBJECT",
            &String::from_utf8_lossy(&req.args[1]),
        ));
    }
    match store.read(db, &req.args[2], now) {
        // The encoding name is read off the value's CURRENT representation
        // (Encoding::encoding_name) per ADR-0009. See the module docs for the recorded
        // APPEND-stays-short divergence (embstr/int here where Redis reports raw).
        Some(v) => Value::bulk(Bytes::from_static(encoding_name_static(v.encoding()))),
        None => Value::Null,
    }
}

/// `OBJECT REFCOUNT key` -> the synthetic refcount (2147483647 for a shared small int
/// 0..=9999, else 1), or null if absent.
fn object_refcount<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        // A wrong arg count falls through to addReplySubcommandSyntaxError (see
        // object_encoding): the unknown-subcommand wording, not a per-sub arity error.
        return Value::error(ErrorReply::unknown_subcommand(
            "OBJECT",
            &String::from_utf8_lossy(&req.args[1]),
        ));
    }
    match store.read(db, &req.args[2], now) {
        Some(v) => {
            // Redis shares integer objects in [0, OBJ_SHARED_INTEGERS) = [0, 10000) and
            // reports their refcount as OBJ_SHARED_REFCOUNT (INT_MAX). Reproduce the
            // OBSERVABLE value: an int-encoded value whose decimal parses into 0..=9999
            // reports 2147483647; everything else reports 1.
            let shared = v.encoding() == Encoding::Int
                && core::str::from_utf8(v.as_bytes())
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .is_some_and(|n| (0..10_000).contains(&n));
            if shared {
                Value::Integer(2_147_483_647)
            } else {
                Value::Integer(1)
            }
        }
        None => Value::Null,
    }
}

/// `OBJECT IDLETIME key` -> integer seconds idle (0 here; per-key access time is not
/// tracked yet), or null if absent. ERRORS under an LFU policy (idle not tracked).
fn object_idletime<S: Store + Admit>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
) -> Value {
    if req.args.len() != 3 {
        // A wrong arg count falls through to addReplySubcommandSyntaxError (see
        // object_encoding): the unknown-subcommand wording, not a per-sub arity error.
        return Value::error(ErrorReply::unknown_subcommand(
            "OBJECT",
            &String::from_utf8_lossy(&req.args[1]),
        ));
    }
    // Redis does the KEY LOOKUP FIRST (objectCommandLookupOrReply replies the null bulk
    // on absence) and ONLY THEN checks the maxmemory-policy LFU gate: key first, then the
    // LFU gate, matching Redis. So IDLETIME of a missing key under an LFU policy is NULL,
    // not the under-LFU error.
    if !store.contains(db, &req.args[2], now) {
        return Value::Null;
    }
    if is_lfu_policy(store) {
        return Value::error(ErrorReply::object_idletime_under_lfu());
    }
    // Per-key last-access time is not tracked yet; report 0 idle seconds (a just-accessed
    // approximation). Exact idle tracking is a later follow-up.
    Value::Integer(0)
}

/// `OBJECT FREQ key` -> the integer access-frequency estimate, or null if absent.
/// ERRORS unless an LFU policy is selected (Redis gates FREQ on `*-lfu`).
fn object_freq<S: Store + Admit>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        // A wrong arg count falls through to addReplySubcommandSyntaxError (see
        // object_encoding): the unknown-subcommand wording, not a per-sub arity error.
        return Value::error(ErrorReply::unknown_subcommand(
            "OBJECT",
            &String::from_utf8_lossy(&req.args[1]),
        ));
    }
    // Redis does the KEY LOOKUP FIRST (objectCommandLookupOrReply replies the null bulk
    // on absence) and ONLY THEN checks the maxmemory-policy LFU gate: key first, then the
    // LFU gate, matching Redis. So FREQ of a missing key under a non-LFU policy is NULL,
    // not the requires-LFU error.
    if !store.contains(db, &req.args[2], now) {
        return Value::Null;
    }
    if !is_lfu_policy(store) {
        return Value::error(ErrorReply::object_freq_requires_lfu());
    }
    // The key exists under an LFU policy; read the sketch estimate via the additive
    // accessor.
    match store.access_freq(db, &req.args[2]) {
        Some(freq) => Value::Integer(i64::from(freq)),
        // Under an LFU policy the accessor always returns Some; if a tracked key is not
        // in the sketch its estimate is 0. A None here would mean the policy is not LFU
        // (already gated above), so this is defensive.
        None => Value::Integer(0),
    }
}

/// Whether the configured maxmemory policy is an LFU-family policy (`*-lfu`). The LFU
/// engine is the only one that tracks access frequency, so OBJECT FREQ succeeds and
/// OBJECT IDLETIME errors exactly under it. We read this off the configured policy name
/// (which round-trips verbatim, ADR-0009) so it tracks the exact configured spelling.
fn is_lfu_policy<S: Admit>(store: &S) -> bool {
    store.policy_name().to_ascii_lowercase().contains("lfu")
}

/// The encoding name as a `'static` byte slice (so the reply borrows no temporary).
fn encoding_name_static(enc: Encoding) -> &'static [u8] {
    enc.encoding_name().as_bytes()
}

/// `OBJECT HELP` -> the help text array (the subcommand summaries, like Redis).
fn object_help() -> Value {
    let lines: &[&str] = &[
        "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "ENCODING <key>",
        "    Return the kind of internal representation used in order to store the value associated with a <key>.",
        "FREQ <key>",
        "    Return the access frequency index of the <key>. The returned integer is proportional to the logarithm of the recent access frequency of the key.",
        "IDLETIME <key>",
        "    Return the idle time of the <key>, that is the approximated number of seconds elapsed since the last access to the key.",
        "REFCOUNT <key>",
        "    Return the number of references of the value associated with the specified <key>.",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(Some(lines.iter().map(|l| Value::bulk_str(l)).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_eviction::{Policy, map_policy_name};
    use ironcache_storage::CountingAccounting;
    use ironcache_store::ShardStore;
    use ironcache_store::kvobj::KvObj;

    type TestStore = ShardStore<Policy, CountingAccounting>;

    /// A store whose configured `maxmemory-policy` is the given Redis name (so
    /// `is_lfu_policy` reflects the real `*-lfu` gate).
    fn store_with_policy(name: &str) -> TestStore {
        let policy = map_policy_name(name, 1).expect("known policy name");
        ShardStore::with_hooks(4, policy, CountingAccounting::new())
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    /// Plant a present String-typed key so the lookup-before-gate ordering can be
    /// exercised on a HIT as well as a miss.
    fn plant(store: &mut TestStore, key: &[u8], val: &[u8]) {
        store.insert_object(0, KvObj::from_bytes(key, val, None));
    }

    #[test]
    fn idletime_missing_key_under_lfu_is_null_not_error() {
        // Redis does the key lookup FIRST and replies the null bulk on absence, BEFORE
        // the LFU gate; so a MISSING key under an LFU policy is NULL, not the under-LFU
        // error. (Present-key-under-LFU still errors, asserted below.)
        let mut store = store_with_policy("allkeys-lfu");
        assert_eq!(
            cmd_object(
                &mut store,
                0,
                UnixMillis(0),
                &req(&[b"OBJECT", b"IDLETIME", b"nope"])
            ),
            Value::Null
        );
        // A PRESENT key under LFU still hits the under-LFU error (gate applies after the
        // successful lookup).
        plant(&mut store, b"k", b"v");
        match cmd_object(
            &mut store,
            0,
            UnixMillis(0),
            &req(&[b"OBJECT", b"IDLETIME", b"k"]),
        ) {
            Value::Error(e) => assert_eq!(e.line(), ErrorReply::object_idletime_under_lfu().line()),
            other => panic!("expected under-LFU error, got {other:?}"),
        }
    }

    #[test]
    fn freq_missing_key_under_non_lfu_is_null_not_error() {
        // Symmetric to IDLETIME: the key lookup is FIRST, so a MISSING key under a
        // non-LFU policy is NULL, not the requires-LFU error.
        let mut store = store_with_policy("allkeys-lru");
        assert_eq!(
            cmd_object(
                &mut store,
                0,
                UnixMillis(0),
                &req(&[b"OBJECT", b"FREQ", b"nope"])
            ),
            Value::Null
        );
        // A PRESENT key under a non-LFU policy still hits the requires-LFU error.
        plant(&mut store, b"k", b"v");
        match cmd_object(
            &mut store,
            0,
            UnixMillis(0),
            &req(&[b"OBJECT", b"FREQ", b"k"]),
        ) {
            Value::Error(e) => assert_eq!(e.line(), ErrorReply::object_freq_requires_lfu().line()),
            other => panic!("expected requires-LFU error, got {other:?}"),
        }
    }

    #[test]
    fn subcommand_wrong_arity_is_unknown_subcommand_wording() {
        // `OBJECT ENCODING` with no key is a wrong arg count, which Redis routes through
        // addReplySubcommandSyntaxError (NOT a per-subcommand arity error): the same
        // wording as an unknown subcommand, with the subcommand token verbatim.
        let mut store = store_with_policy("noeviction");
        match cmd_object(
            &mut store,
            0,
            UnixMillis(0),
            &req(&[b"OBJECT", b"ENCODING"]),
        ) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR unknown subcommand or wrong number of arguments for 'ENCODING'. Try OBJECT HELP."
            ),
            other => panic!("expected subcommand syntax error, got {other:?}"),
        }
    }
}
