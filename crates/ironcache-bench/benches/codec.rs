// SPDX-License-Identifier: MIT OR Apache-2.0
//! Codec micro-benches (BENCHMARK.md #8): the RESP request decoder and the reply
//! serializer, the two hottest pure-CPU paths on the wire.
//!
//! Determinism: the frames and values are fixed byte/value literals (no RNG, no
//! clock), so the inputs are identical every run. Criterion does its own timing.

#![forbid(unsafe_code)]

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use ironcache_protocol::{DecodeOutcome, Limits, ProtoVersion, Value, decode, encode_to_vec};
use std::hint::black_box;

/// A representative `SET key value` request as a RESP multibulk frame:
/// `*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n`. This is the canonical hot
/// decode path (the most common write command).
fn set_frame() -> Vec<u8> {
    let mut f = Vec::new();
    f.extend_from_slice(b"*3\r\n");
    f.extend_from_slice(b"$3\r\nSET\r\n");
    f.extend_from_slice(b"$3\r\nkey\r\n");
    f.extend_from_slice(b"$5\r\nvalue\r\n");
    f
}

fn bench_decode(c: &mut Criterion) {
    let frame = set_frame();
    let limits = Limits::default();
    c.bench_function("decode/set_multibulk", |b| {
        b.iter(|| {
            let outcome = decode(black_box(&frame), black_box(&limits));
            // Keep the parse result live so the optimizer cannot elide the work,
            // and assert it actually parsed (a broken bench is worse than none).
            match black_box(outcome) {
                DecodeOutcome::Complete { request, consumed } => {
                    black_box((request.args.len(), consumed));
                }
                _ => unreachable!("the SET frame is a complete request"),
            }
        });
    });
}

fn bench_encode(c: &mut Criterion) {
    // (a) A bulk string reply (e.g. a GET hit), the most common single-value reply.
    let bulk = Value::BulkString(Some(Bytes::from_static(b"hello world value")));
    c.bench_function("encode/bulk_string", |b| {
        b.iter(|| {
            let out = encode_to_vec(black_box(&bulk), black_box(ProtoVersion::Resp2));
            black_box(out);
        });
    });

    // (b) An array reply of bulk strings (e.g. an MGET / LRANGE result), the common
    // multi-value reply shape.
    let array = Value::Array(Some(vec![
        Value::BulkString(Some(Bytes::from_static(b"one"))),
        Value::BulkString(Some(Bytes::from_static(b"two"))),
        Value::BulkString(Some(Bytes::from_static(b"three"))),
        Value::Integer(4),
        Value::SimpleString("OK".to_string()),
    ]));
    c.bench_function("encode/array", |b| {
        b.iter(|| {
            let out = encode_to_vec(black_box(&array), black_box(ProtoVersion::Resp3));
            black_box(out);
        });
    });
}

criterion_group!(benches, bench_decode, bench_encode);
criterion_main!(benches);
