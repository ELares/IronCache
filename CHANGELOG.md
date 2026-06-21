# Changelog

All notable changes to IronCache are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project will
adopt [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it ships a
release.

## [Unreleased]

### Added

- IronCache Console: a new `ironcache-console` crate and binary (epic #352,
  issue #353), a SEPARATE server from the data-plane that will discover an
  IronCache deployment, aggregate a cluster-wide view, and serve a monitoring
  dashboard while staying out of the data path. This first slice is the skeleton:
  layered config (TOML plus `IRONCACHE_CONSOLE_*` env plus CLI), structured
  tracing, and a bounded hand-rolled HTTP responder serving `/livez`, `/readyz`,
  and the console's own `/metrics` (so the monitor can be monitored: poll
  success/failure counters and a last-successful-poll age gauge). Node
  acquisition, the single-node topology view, aggregation, the REST API, the UI,
  and TLS land in later PRs (#355, #366, #356, #358, #359).
- Keyspace notifications (PROD-8, Redis `notify-keyspace-events`). On a successful
  mutation (and on a TTL expiry / a maxmemory eviction) the server PUBLISHes the
  Redis keyspace + keyevent events to `__keyspace@<db>__:<key>` (payload = the
  event name, e.g. `set`/`del`/`lpush`/`expire`/`expired`/`evicted`) and
  `__keyevent@<db>__:<event>` (payload = the key), through the EXISTING Pub/Sub
  fan-out, so SUBSCRIBE / PSUBSCRIBE (and cross-shard subscribers) receive them
  exactly like a client PUBLISH. Gated by the `notify-keyspace-events` flag string
  (`K` keyspace, `E` keyevent, the event-class letters `g$lshzxe...`, and `A` =
  `g$lshzxet`), parsed to a compact bitset with the canonical Redis parse/render;
  `CONFIG GET`/`SET notify-keyspace-events` round-trips the canonical flag string,
  and TOML (`notify_keyspace_events`) / `IRONCACHE_NOTIFY_KEYSPACE_EVENTS` seed it
  at boot. DISABLED by default (the empty flag string, the Redis default): the
  emit helper short-circuits on the disabled set BEFORE any work, so the write hot
  path is byte-identical and pays no cost until a non-empty flag is set. The
  `expired`/`evicted` events are wired into the active + lazy TTL reap paths and
  the maxmemory eviction paths. Stream (`t`) and module (`d`) classes are
  recognized for parity but never fire (IronCache has no streams/modules); the
  new-key (`n`) and key-miss (`m`) events are recognized in the flag string but
  not emitted this pass.
- The WRITE-SIDE replication guardrail (`min-replicas-to-write` /
  `min-replicas-max-lag`, ADR-0026): an owner REJECTS a write to a slot it owns
  with `-NOREPLICAS Not enough good replicas to write.` when fewer than
  `min_replicas_to_write` replicas are currently in sync (lag within
  `min_replicas_max_lag`), so an acknowledged write is known to be on at least
  that many replicas, bounding the failover loss window (the read side was
  already bounded by `replica_max_lag`). The primary's per-replica serve tasks
  maintain a node-level in-sync count as a single lock-free `AtomicUsize` (one
  relaxed load on the write path); the check is owned-write-only,
  cluster/raft-mode-only, and DISABLED by default (`min_replicas_to_write = 0`,
  the Redis default), so the write hot path is byte-unchanged at the default.
  Configurable via TOML / `IRONCACHE_MIN_REPLICAS_TO_WRITE` /
  `IRONCACHE_MIN_REPLICAS_MAX_LAG`.
- Fully automated versioning and releases, in two channels (RELEASING.md). A new
  `rolling-release` workflow publishes a calendar-versioned (`YYYY.MMDD.N`)
  GitHub Release on every push to `main`: four reproducible `cargo-zigbuild`
  tarballs (x86_64/aarch64, musl/glibc-2.17), a consolidated `SHA256SUMS`, and a
  keyless Sigstore build-provenance attestation, as a normal release so
  `releases/latest` tracks the newest build (`[skip release]` opts out). The
  published version is stamped into the binary via `IRONCACHE_BUILD_VERSION`
  (read by `option_env!` in `cli::BUILD_VERSION`, with a `build.rs`
  `rerun-if-env-changed` so a warm target re-stamps), so `ironcache --version`
  reports the build without touching `Cargo.lock` (pinned at `0.0.0`).
- The formal `v*` `release` workflow is now a working pipeline rather than a
  scaffold: a changelog gate (`scripts/ci/changelog-unreleased.sh`) that fails
  an empty-changelog release before building, a real CycloneDX SBOM export from
  the embedded `cargo-auditable` data (`auditable2cdx`, #123), a secret-gated
  minisign signature over `SHA256SUMS` (ADR-0020), a keyless Sigstore
  attestation, and `v0.*` prerelease marking. Fixed a `SHA256SUMS` bug where a
  `merge-multiple` artifact-download filename collision left three of the four
  tarballs unchecksummed; the consolidated file is now rebuilt from the tarballs
  and self-checked.
- First engine code (PR-1 "Boot + wire"): a Cargo virtual workspace and a
  minimal-but-real server that accepts RESP connections and answers the Tier-0
  connection commands. Seven crates: `ironcache-env` (the determinism Env seam,
  ADR-0003), `ironcache-protocol` (RESP2/RESP3 codec and the verbatim error
  catalog, PROTOCOL.md/ERRORS.md/ADR-0019), `ironcache-runtime` (the swappable
  Runtime trait and a shared-nothing tokio current-thread, per-core-pinned
  bootstrap with SO_REUSEPORT per-shard accept, RUNTIME.md/ADR-0002),
  `ironcache-config` (layered TOML config with human-size parsing, CONFIG.md),
  `ironcache-observe` (INFO sections and per-shard rollup counters,
  OBSERVABILITY.md), `ironcache-server` (the HELLO-driven connection state
  machine and Tier-0 dispatch: PING/HELLO/AUTH/SELECT/QUIT/RESET/CLIENT/COMMAND/
  INFO/ECHO), and the `ironcache` binary (clap subcommands server|cli|bench|
  check|config|upgrade, the redis-cli argv[0] alias, jemalloc global allocator
  per ADR-0006, graceful SIGINT/SIGTERM shutdown).
- Rust CI workflow (`.github/workflows/rust.yml`) with the docs-only guard idiom:
  fmt, clippy (pedantic, -D warnings), test (ubuntu + macos), MSRV 1.85, a musl
  static build, and grep-based invariant lints
  (`scripts/ci/check-rust-invariants.sh`) enforcing no-fork, the determinism
  Env-seam boundary, shared-nothing locks, and the SPDX header on every source.
- `deny.toml` (cargo-deny license allowlist, advisories, sources),
  `rust-toolchain.toml` (pinned stable channel), and a committed `Cargo.lock`.
- Project scaffolding: dual `MIT OR Apache-2.0` license, Code of Conduct,
  Contributing guide, Governance, and Security policy.
- The vision EPIC and the research and specification issue tree.
- Prior-art research corpus under `docs/research/`, the version-pinned
  `docs/prior-art/claims.yaml`, and the `docs/PRIOR_ART.md` survey.
- Pre-implementation audit (`docs/AUDIT.md`): every issue audited and given a
  verdict comment, 5 prior-art claims re-verified and corrected, 27 split
  sub-issues filed from 9 decomposed parents, and 36 coverage-gap issues filed.

### Security

- In-memory zeroization of plaintext secrets, defense-in-depth (#145, the last
  production-readiness follow-up). The long-lived `cluster_secret` (the one secret
  that cannot be reduced to a hash, since the peer handshake compares its literal
  bytes) is now held in a `Zeroizing<Vec<u8>>` inside `ClusterSecurity`, so it is
  scrubbed from the heap when the node tears down; and the transient plaintext copy
  a `CONFIG SET requirepass` materializes is a `Zeroizing<String>`, scrubbed the
  instant it is hashed to a digest at rest. TLS private keys are already zeroized by
  rustls itself (not double-handled). The `AUTH`/`HELLO AUTH`/`ACL SETUSER >pass`
  transient plaintext is documented as an accepted residual (it lives only in the
  shared/immutable decoded argument and the reused codec read buffer, which is
  `drain`-ed forward to preserve pipelined bytes; scrubbing it risks pipelining for
  marginal gain). The scope decision (what is and is not protected, and why) is in
  `SECURITY.md`, `docs/design/SECRETS.md`, and `docs/THREAT_MODEL.md`. Off the hot
  data path; the auth logic, the wire protocol, and the non-auth hot path are
  byte-unchanged. The `zeroize` crate was already in the lock transitively (rustls),
  so no new crate enters the supply chain.

### Changed

- Corrected 5 prior-art claims in `docs/prior-art/claims.yaml` after
  re-verification (provenance preserved via `verification.reaudited`).

### Fixed

- Cross-shard atomicity for spanning multi-key + move commands (PROD-9): a SILENT
  partial-apply safety bug. On a multi-shard node (the default; shards == cores) a
  2-key src/dst command (RENAME/RENAMENX/COPY/SMOVE/LMOVE/RPOPLPUSH) or a strided
  multi-key command (MSETNX/LMPOP/ZMPOP) whose keys hashed to DIFFERENT internal
  shards used to fall through to the HOME shard and operate on ONLY the home-owned
  subset of the keys, applying a PARTIAL result SILENTLY (a spanning RENAME saw a
  sibling-shard `src` as absent and errored, or wrote `dst` onto the wrong shard --
  a silent lost write; a spanning MSETNX checked + set only the home keys and
  misreported its 1/0). The fix ends every silent partial: SMOVE / LMOVE /
  RPOPLPUSH and MSETNX now apply ATOMICALLY across the owner shards (a home-core
  gather + validate-then-commit, each sub-op a single deadlock-free deterministic
  hop -- the element/member is held on the home core between the source read and
  the dest write so it can never be lost, and MSETNX scans every key's existence
  before any write); RENAME / RENAMENX / COPY / LMPOP / ZMPOP / SORT...STORE (which
  need a value-object cross-shard transfer the engine does not expose yet) now
  FAIL-LOUD with a clear error naming the hash-tag co-location remedy instead of
  silently partial-applying. Co-located (same-shard) and `shards == 1` invocations
  are byte-identical to the single-shard handler. Cross-shard MULTI/EXEC + WATCH
  were already fail-loud (the existing in-MULTI cross-shard + WATCH guards), so no
  silent transaction partial remained. Documented residual divergences from
  single-node Redis (no data loss, narrower than the silent partial they replace):
  a spanning move has a brief transient-visibility window (SMOVE: member momentarily
  in both sets; LMOVE/RPOPLPUSH: element momentarily in neither) but never loses an
  element; if the source-remove hop fails after the dest-add committed, SMOVE now
  compensates (removes from dest) and surfaces the error rather than reporting a
  clean move; and spanning MSETNX has a check-then-write window (a key created
  concurrently between the existence scan and the writes is overwritten) -- use a
  hash tag to co-locate keys for strict single-shard atomicity.
- Removed or relinked broken citations in issue bodies (#83, #88, #97).
