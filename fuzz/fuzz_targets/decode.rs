// SPDX-License-Identifier: MIT OR Apache-2.0
//! cargo-fuzz / libFuzzer target over `ironcache_protocol::decode`, the RESP
//! request-frame decoder (crates/ironcache-protocol/src/decode.rs).
//!
//! The decoder is the FIRST code an unauthenticated peer reaches, and the release
//! profile is `panic = "abort"` (root Cargo.toml), so a single missed decode panic
//! is an uncatchable whole-process crash: a remote parser DoS. This target feeds
//! arbitrary bytes to `decode` and relies on libFuzzer's inherent panic/abort
//! detection to assert the decoder NEVER panics on ANY input. It is the concrete
//! implementation of the "parser fuzz gate" that HARDENING.md and TESTING.md
//! describe (#95, #138, #534).
//!
//! It does not assert on the DecodeOutcome value: any of `Complete` / `Incomplete`
//! / `Error` is a correct, well-behaved result. Only a panic (index-out-of-bounds,
//! arithmetic overflow, `unreachable!`, a failed `debug_assert!`, ...) is a bug,
//! and that is exactly what the harness catches.

#![no_main]

use ironcache_protocol::{decode, Limits};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // 1) The REAL production configuration. The connection read loop (serve.rs)
    //    builds `Limits` from `Limits::default()` (proto-max-bulk-len 512 MB,
    //    multibulk cap 1Mi elements, inline cap 64Ki), overriding only
    //    max_bulk_len from live config. Fuzz that exact shape.
    let _ = decode(data, &Limits::default());

    // 2) A deliberately TIGHT set so the hardening-cap rejection branches
    //    (multibulk-count / bulk-length / inline-length over cap, #138) are
    //    reachable from the SMALL inputs libFuzzer favors, where the 512 MB
    //    default ceiling would otherwise never trip on a realistic corpus.
    let tight = Limits {
        max_multibulk: 16,
        max_bulk_len: 4096,
        max_inline_len: 1024,
    };
    let _ = decode(data, &tight);
});
