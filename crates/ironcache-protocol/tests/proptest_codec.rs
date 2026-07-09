// SPDX-License-Identifier: MIT OR Apache-2.0
//! Property-based tests over the RESP codec (#527 production-readiness M3, TESTING.md).
//!
//! These complement the #541/#534 libFuzzer `decode` gate (fuzz/fuzz_targets/decode.rs)
//! and the differential harness: where the fuzzer SPOT-CHECKS "decode never panics" over a
//! coverage-guided corpus, these `proptest` cases turn the load-bearing invariants into a
//! FAST, DETERMINISTIC, seeded gate that runs on every `cargo test`.
//!
//! ## The codec is ASYMMETRIC (why the round-trip is over the request path)
//!
//! [`ironcache_protocol::encode`] serializes a reply-side [`Value`] into bytes; [`decode`]
//! parses a request-side frame into a [`Request`] (a flat `Vec<Bytes>` argv), NOT back into
//! a `Value`. There is no reply parser in this crate, so a literal `decode(encode(v)) == v`
//! over an arbitrary `Value` is not type-correct. The LOSSLESS round-trip that DOES exist is
//! over the shared wire surface both directions speak: the multibulk-of-bulk-strings frame.
//! `encode` of a `Value::Array(Some(vec_of_BulkString))` emits exactly the `*<n>\r\n$<len>\r\n
//! <bytes>\r\n...` bytes a client sends, and `decode` parses that same shape back into the
//! argv. So the faithful round-trip property is: an arbitrary binary-safe argv, encoded as a
//! multibulk and decoded, yields the IDENTICAL argv (property 1). The reply-side `Value` space
//! is covered separately by the "encode is total (never panics)" property (property 6).
//!
//! Proptest is a `[dev-dependencies]`-only harness (see this crate's Cargo.toml): it never
//! ships. Its test-input PRNG is test-harness randomness, not the engine clock/entropy the
//! determinism invariant governs (ADR-0003); this file references only `proptest`/`prop`
//! APIs, so the `scripts/ci/check-rust-invariants.sh` determinism grep (which bans `rand::`,
//! `getrandom::`, ... tokens in crate source) does not flag it.

use bytes::Bytes;
use ironcache_protocol::{
    CLUSTER_SLOTS, DecodeOutcome, ErrorReply, Limits, ProtoVersion, Value, decode, encode_bulk_ref,
    encode_to_vec, key_slot,
};
use proptest::prelude::*;

/// The tight parser limits the fuzz target uses, so the hardening-cap rejection branches
/// (#138) are reachable from small inputs.
fn tight_limits() -> Limits {
    Limits {
        max_multibulk: 16,
        max_bulk_len: 4096,
        max_inline_len: 1024,
    }
}

/// One binary-safe argument: 0..256 arbitrary bytes (empty, embedded CRLF, NUL, high bytes,
/// invalid UTF-8 all included).
fn arg_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..256)
}

/// A request argv: 1..16 binary-safe args (a `Request` is always non-empty).
fn request_args() -> impl Strategy<Value = Vec<Vec<u8>>> {
    proptest::collection::vec(arg_bytes(), 1..16)
}

/// Encode an argv as the RESP multibulk-of-bulk-strings wire frame: exactly what a client
/// sends and what `decode` parses back (`encode` of an Array-of-BulkStrings emits the
/// `*<n>\r\n$<len>\r\n<bytes>\r\n...` request wire form).
fn encode_request(args: &[Vec<u8>]) -> Vec<u8> {
    let items = args
        .iter()
        .map(|a| Value::BulkString(Some(Bytes::copy_from_slice(a))))
        .collect();
    encode_to_vec(&Value::Array(Some(items)), ProtoVersion::Resp2)
}

/// A recursive strategy spanning (nearly) the whole [`Value`] type space: the RESP3 scalars
/// (including NaN/inf doubles and both null shapes), errors, and the nested aggregates
/// (arrays, sets, pushes, maps, ordered pairs). Bounded depth/size keeps case generation
/// cheap and terminating.
fn arb_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        any::<String>().prop_map(Value::SimpleString),
        any::<i64>().prop_map(Value::Integer),
        proptest::option::of(arg_bytes()).prop_map(|o| Value::BulkString(o.map(Bytes::from))),
        Just(Value::Null),
        any::<f64>().prop_map(Value::Double),
        any::<bool>().prop_map(Value::Boolean),
        any::<String>().prop_map(Value::BigNumber),
        any::<String>().prop_map(|s| Value::Error(ErrorReply::err(s))),
        any::<String>().prop_map(|s| Value::BulkError(ErrorReply::err(s))),
        (any::<[u8; 3]>(), arg_bytes()).prop_map(|(format, d)| Value::VerbatimString {
            format,
            data: Bytes::from(d),
        }),
    ];
    leaf.prop_recursive(4, 32, 6, |inner| {
        prop_oneof![
            proptest::option::of(proptest::collection::vec(inner.clone(), 0..6))
                .prop_map(Value::Array),
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Value::Set),
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Value::Push),
            proptest::collection::vec((inner.clone(), inner.clone()), 0..4).prop_map(Value::Map),
            proptest::collection::vec((inner.clone(), inner), 0..4).prop_map(Value::Pairs),
        ]
    })
}

proptest! {
    // Bounded, deterministic gate: 256 cases per property (fast pure functions). Failure
    // persistence is OFF so no `.proptest-regressions` file is written (the `/repo` mount is
    // read-only under the container test run, and a shrunk counterexample is printed inline).
    #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

    /// PROPERTY 1 (audit invariant: lossless wire codec). An arbitrary binary-safe argv,
    /// encoded as a multibulk frame and decoded, yields the IDENTICAL argv, and the decoder
    /// reports it consumed EXACTLY the framed bytes. This is the request-path round-trip (see
    /// the module docs on why the codec is asymmetric).
    #[test]
    fn request_argv_roundtrips_through_encode_then_decode(args in request_args()) {
        let wire = encode_request(&args);
        match decode(&wire, &Limits::default()) {
            DecodeOutcome::Complete { request, consumed } => {
                let got: Vec<Vec<u8>> = request.args.iter().map(|b| b.to_vec()).collect();
                prop_assert_eq!(got, args);
                prop_assert_eq!(consumed, wire.len());
            }
            other => prop_assert!(false, "expected Complete, got {other:?}"),
        }
    }

    /// PROPERTY 2 (byte-length accounting is consistent, pipelining). Two argvs framed
    /// back-to-back decode ONE AT A TIME: the first decode returns the first argv and consumes
    /// exactly its frame; decoding from the advanced offset returns the second, and the two
    /// consumed counts sum to the total buffer length (no bytes lost or double-counted).
    #[test]
    fn pipelined_frames_decode_one_at_a_time(a in request_args(), b in request_args()) {
        let mut wire = encode_request(&a);
        let first_len = wire.len();
        wire.extend_from_slice(&encode_request(&b));
        match decode(&wire, &Limits::default()) {
            DecodeOutcome::Complete { request, consumed } => {
                let got_a: Vec<Vec<u8>> = request.args.iter().map(|x| x.to_vec()).collect();
                prop_assert_eq!(got_a, a);
                prop_assert_eq!(consumed, first_len);
                match decode(&wire[consumed..], &Limits::default()) {
                    DecodeOutcome::Complete { request, consumed: c2 } => {
                        let got_b: Vec<Vec<u8>> = request.args.iter().map(|x| x.to_vec()).collect();
                        prop_assert_eq!(got_b, b);
                        prop_assert_eq!(consumed + c2, wire.len());
                    }
                    other => prop_assert!(false, "second frame: {other:?}"),
                }
            }
            other => prop_assert!(false, "first frame: {other:?}"),
        }
    }

    /// PROPERTY 3 (audit invariant: decode never panics). For ARBITRARY bytes (malformed RESP,
    /// truncated frames, embedded NUL, invalid UTF-8) under both the production default limits
    /// and the tight caps, `decode` returns a well-formed outcome and NEVER panics / overflows
    /// / infinite-loops. A `Complete` outcome never claims to consume more than it was given.
    #[test]
    fn decode_never_panics_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..1024)) {
        for limits in [Limits::default(), tight_limits()] {
            match decode(&bytes, &limits) {
                DecodeOutcome::Complete { consumed, .. } => {
                    prop_assert!(consumed <= bytes.len(), "consumed {} > input {}", consumed, bytes.len());
                }
                DecodeOutcome::Incomplete | DecodeOutcome::Error(_) => {}
            }
        }
    }

    /// PROPERTY 4 (reject-before-alloc, #542/#596). A bulk-string length HEADER that declares a
    /// huge (or i64-overflowing) size, with NO payload following, is decided FROM THE HEADER
    /// ALONE: a clean `Error` when the declared length exceeds the cap (or overflows i64), or
    /// `Incomplete` when it is within the cap (await the bytes). It is NEVER a panic, a hang, or
    /// an over-allocation of the claimed payload. The two-arm generator straddles the 512 MB
    /// default cap (0..2 GiB) and covers the full u64 overflow space.
    #[test]
    fn huge_bulk_length_header_is_clean_error_never_oom(
        len in prop_oneof![0u64..=(2u64 * 1024 * 1024 * 1024), any::<u64>()]
    ) {
        let limits = Limits::default();
        let header = format!("*1\r\n${len}\r\n"); // NO payload byte follows the header.
        match decode(header.as_bytes(), &limits) {
            DecodeOutcome::Error(_) => {
                // Error only for an over-cap or non-i64 length, never for an in-cap one.
                prop_assert!(i64::try_from(len).map_or(true, |l| l > limits.max_bulk_len));
            }
            DecodeOutcome::Incomplete => {
                // Incomplete implies the length parsed, is non-negative, and is within the cap.
                let l = i64::try_from(len).expect("an in-cap length fits i64");
                prop_assert!(l >= 0 && l <= limits.max_bulk_len);
            }
            DecodeOutcome::Complete { .. } => {
                prop_assert!(false, "a length header with no payload cannot be Complete");
            }
        }
    }

    /// PROPERTY 5 (incremental decoder). Any STRICT prefix of a valid single frame is
    /// `Incomplete` (need more bytes), never `Complete` and never a spurious `Error`: truncating
    /// a well-formed frame only ever withholds bytes, so the decoder waits rather than
    /// misparsing. This pins the non-blocking, never-partially-consume contract.
    #[test]
    fn strict_prefix_of_a_valid_frame_is_incomplete(
        args in request_args(),
        cut in any::<proptest::sample::Index>(),
    ) {
        let wire = encode_request(&args);
        let cut_at = cut.index(wire.len()); // 0..len, i.e. strictly less than the full frame.
        let outcome = decode(&wire[..cut_at], &Limits::default());
        prop_assert!(
            matches!(outcome, DecodeOutcome::Incomplete),
            "prefix {}/{} gave {outcome:?}",
            cut_at,
            wire.len()
        );
    }

    /// PROPERTY 6 (encode is total). `encode` NEVER panics over the whole `Value` type space
    /// (nested aggregates, NaN/inf doubles, both null shapes, errors, verbatim strings) in
    /// BOTH RESP2 and RESP3, and always emits a non-empty frame. This is the reply-side
    /// counterpart to the decode-never-panics property.
    #[test]
    fn encode_never_panics_and_is_nonempty(v in arb_value()) {
        let r2 = encode_to_vec(&v, ProtoVersion::Resp2);
        let r3 = encode_to_vec(&v, ProtoVersion::Resp3);
        prop_assert!(!r2.is_empty());
        prop_assert!(!r3.is_empty());
    }

    /// PROPERTY 7 (#511 GET fast-path equivalence). The by-reference bulk framer
    /// `encode_bulk_ref(data)` produces bytes IDENTICAL to encoding a `Value::BulkString(Some(
    /// data))`, for arbitrary bytes and in both protocol versions (a present bulk string is
    /// proto-independent). Generalizes the fixed-corpus unit test to the whole byte space.
    #[test]
    fn encode_bulk_ref_matches_bulkstring_encoding(data in arg_bytes()) {
        let mut by_ref = Vec::new();
        encode_bulk_ref(&mut by_ref, &data);
        let via_value2 =
            encode_to_vec(&Value::BulkString(Some(Bytes::copy_from_slice(&data))), ProtoVersion::Resp2);
        let via_value3 =
            encode_to_vec(&Value::BulkString(Some(Bytes::copy_from_slice(&data))), ProtoVersion::Resp3);
        prop_assert_eq!(&by_ref, &via_value2);
        prop_assert_eq!(&by_ref, &via_value3);
    }

    /// PROPERTY 8 (cluster slot mapping is total). Every key, including binary keys, maps into
    /// `[0, CLUSTER_SLOTS)`. Generalizes the fixed-corpus `key_slot_is_always_in_range` unit
    /// test over the whole byte space (client-visible routing correctness, CLUSTER_CONTRACT.md).
    #[test]
    fn key_slot_is_always_in_range(key in proptest::collection::vec(any::<u8>(), 0..128)) {
        prop_assert!(key_slot(&key) < CLUSTER_SLOTS);
    }

    /// PROPERTY 9 (hash-tag co-location). For a non-empty tag containing no braces, a key of the
    /// form `{tag}<suffix>` hashes to the SAME slot as the bare `tag` for ANY suffix: the
    /// hash-tag rule extracts the first `{...}` and routes by it, so tagged keys co-locate.
    #[test]
    fn hash_tag_colocates_braced_key_with_bare_tag(
        tag in proptest::collection::vec(any::<u8>().prop_filter("no braces", |b| *b != b'{' && *b != b'}'), 1..32),
        suffix in proptest::collection::vec(any::<u8>(), 0..32),
    ) {
        let mut braced = Vec::with_capacity(tag.len() + suffix.len() + 2);
        braced.push(b'{');
        braced.extend_from_slice(&tag);
        braced.push(b'}');
        braced.extend_from_slice(&suffix);
        prop_assert_eq!(key_slot(&braced), key_slot(&tag));
    }
}
