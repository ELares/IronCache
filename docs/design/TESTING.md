# Design: Correctness stack (conformance, differential, fuzz, property, DST)

Issue: #95. Sub-task: #96 (Valkey oracle, differential half). Decisions: ADR-0009
(compat tiering), ADR-0003 (determinism / Env seam). Related: #97 (error/reply
oracle), #15 (connection commands), #138 (parser hardening), #8 (benchmark
harness, the efficiency counterpart).

## Goal and scope

A from-scratch Redis-compatible cache earns trust only by proving wire and
semantic compatibility, not by asserting it. This is the headline correctness
design: the conformance oracle, the differential and property machinery, the
parser fuzz gate, and the deterministic-simulation path, each merge-gating. Tenet
order is Compatible first, then Efficient: a test that flatters us but lets an
incompatibility ship is worse than no test. Out of scope: the efficiency harness
(#8) and the detailed Jepsen/fault-injection mechanics (parented here, specified
in #99/#100).

## Design

### Compatibility contract: behavioral equivalence

- The contract is **behavioral equivalence**, not bit-identical replies (ADR-0009
  tiering): reply type, framing/shape, and the error-prefix token must match the
  oracle, but exact error wording is not frozen. This keeps RESP2-vs-RESP3 null
  shaping [resp2-null-encodings] testable without ossifying on benign upstream
  string changes. The error-prefix set treated as contract is fixed: `ERR`,
  `WRONGTYPE`, `NOPROTO`, `NOAUTH`, `EXECABORT` (the exact catalog is ERRORS.md /
  #97). Bit-identical is rejected as over-constraining.

### Conformance oracle: pinned Valkey

- A pinned Valkey tag (the 9.x line [valkey-version-landscape-2026]) is the
  conformance and differential oracle, run in CI. The Valkey fork stays
  RESP-wire-compatible with Redis 7.2 RESP2/RESP3 [valkey-resp-identical] and is
  BSD-3-Clause [valkey-license-bsd3], so it is the license-safe reference we can run
  side by side; relicensed Redis (SSPL/RSAL) is rejected as the oracle to keep the
  corpus and any vendored fixtures free of non-OSI artifacts. Wire identity for the
  specific pinned tag is not trusted blind: the differential suite itself re-verifies
  it, so the pin can advance up the 9.x line without assuming forward identity. The
  per-command suite
  adapts the Valkey TCL `assert_*` vocabulary; `assert_encoding` (which resolves to
  an `OBJECT ENCODING` match [valkey-assert-encoding-vocab]) maps to IronCache's
  own `OBJECT ENCODING` reporting, since internal encodings differ.

### One command spec drives dispatch, validation, and the suite

- A single machine-readable command spec is the source of truth: it generates the
  dispatch table and the arity/reply-type validation so the parser, validator, and
  conformance suite cannot drift. The surface is ~240-246 core commands
  [redis-core-command-count]; the RESP type space is 15 markers
  [resp-type-prefixes] with bulk strings bounded by the tunable `proto-max-bulk-len`
  (512 MB default) [bulk-string-max-512mb].
  Acceptance is that unmodified Redis clients (redis-cli, redis-py, ioredis) run
  green.

### Differential testing against the oracle (#96)

- A fuzzer generates command sequences, replays each against IronCache and the
  pinned Valkey on identical inputs, and diffs the wire-level responses (reply
  type, framing, error prefix, RESP3 type promotions). Any divergence is a logged,
  reproducible artifact (seed + command trace) fed to #97 and the conformance
  corpus. Because Valkey diverged from Redis at the fork and the two evolve
  independently [valkey-fork-origin], per-version diff baselines are retained so a
  new divergence surfaces as a reviewable baseline change, not silent noise. Exact
  pinned tags live in a committed `VALKEY_VERSIONS` table, bumped only by explicit
  PR (never floating tags) [valkey-version-landscape-2026].

### Parser fuzz gate

- A cargo-fuzz/libFuzzer target (`fuzz/fuzz_targets/decode.rs`, #534) feeds
  arbitrary bytes to the request decoder `ironcache_protocol::decode` (the RESP
  request frame parser) and asserts it NEVER panics: because the release profile is
  `panic = "abort"`, a single missed decode panic is an uncatchable whole-process
  crash (a remote parser DoS), so "does not panic on any input" is the property the
  gate proves. It runs both the production `Limits::default()` hardening caps and a
  tightened set, so the size-limit rejection branches (#138) are reachable from
  small inputs. It exercises the decoder's real input surface: multibulk arrays and
  their length/bulk-length edges, inline commands (quoting), and the tolerated
  RESP3 attribute frame (`|n...`); RESP3 push and streamed aggregate types are a
  reply/encode concern and are not on the request-decode path (a possible future
  target, see Open decisions).
- The corpus is a small committed seed set of valid RESP frames under
  `fuzz/corpus/decode/`; libFuzzer mutates from there. The fuzz crate is STANDALONE
  (excluded from the workspace, its own nightly toolchain), so it does not affect
  `cargo build --workspace`, the MSRV/musl gates, or the cargo-deny scan.
- The gate runs BOUNDED per PR (`cargo fuzz run decode -- -max_total_time=60`) in
  the `fuzz` job of `.github/workflows/rust.yml` on Linux. A longer scheduled
  campaign is a reasonable future add; a crash is reproducible and is minimized into
  `fuzz/corpus/decode/` as a regression seed. AFL++ is an optional second engine for
  longer offline campaigns, not a CI gate.

### Property tests and deterministic simulation (DST)

- Property tests (#98) assert invariants that cross commands (round-trip
  encode/decode, TTL monotonicity, type-transition legality) rather than single
  examples.
- DST is **built**, not bought: a Flow/VOPR-style simulator on the deterministic
  runtime, driving all time/network/disk/RNG through the Env seam (ADR-0003,
  RUNTIME.md) so any failure replays byte-identically from a single seed
  [dst-fdb-tigerbeetle-single-seed]. Owning it forces M1 determinism discipline
  and costs nothing per run; Antithesis (buy) is faster to adopt but external and
  recurring. The build choice constrains architecture, so it is settled in M1, not
  retrofitted.

## Open decisions

- Whether to expose Redis-style `OBJECT ENCODING` values verbatim or report native
  encodings and shim the suite (encoding assertions either map or are
  compatibility-shimmed).
- The pinned Valkey tag and the drift-tracking cadence as upstream evolves.
- Fuzz coverage beyond the request decoder (which the #534 `decode` target now
  gates): the reply/encode path and RESP3 push/streamed aggregate types, plus a
  longer scheduled campaign, and the milestone where the DST cost crosses over.

## Acceptance and test hooks

- A per-command conformance suite gates merges, adapting `assert_*` and mapping
  `assert_encoding` to IronCache `OBJECT ENCODING` [valkey-assert-encoding-vocab].
- Dispatch and arity/reply-type validation are generated from the command spec;
  unmodified redis-cli/redis-py/ioredis run green [redis-core-command-count].
- A pinned Valkey tag runs as the differential oracle in CI, failing on any
  byte-level reply divergence; divergences emit reproducible seed+trace artifacts
  [valkey-resp-identical][valkey-version-landscape-2026].
- The cargo-fuzz target (`fuzz/fuzz_targets/decode.rs`) runs BOUNDED per PR
  (`-max_total_time=60`, the `fuzz` job in rust.yml) over `ironcache_protocol::
  decode`, seeded from `fuzz/corpus/decode/`, asserting no input panics the decoder;
  it covers multibulk-length/bulk-length edges, inline commands, and the tolerated
  RESP3 attribute frame [resp-type-prefixes][bulk-string-max-512mb], and a crash is
  minimized into the corpus as a regression seed.
- A seeded DST run replays byte-identically through the Env seam
  [dst-fdb-tigerbeetle-single-seed]; the build-vs-buy decision is recorded with its
  runtime-determinism implication.
- A committed `VALKEY_VERSIONS` table pins the oracle tags; bumps require an
  explicit PR; no SSPL/RSAL Redis artifact is vendored as the oracle
  [valkey-license-bsd3].

## References

- ADR-0003, ADR-0009; issues #96, #97, #98, #99, #100, #15, #138, #8, #1
  (vision); specs PROTOCOL.md, ERRORS.md, RUNTIME.md.
- Claims: [valkey-assert-encoding-vocab], [redis-core-command-count],
  [valkey-resp-identical], [valkey-version-landscape-2026], [resp-type-prefixes],
  [bulk-string-max-512mb], [dst-fdb-tigerbeetle-single-seed], [valkey-license-bsd3],
  [resp2-null-encodings], [valkey-fork-origin].
