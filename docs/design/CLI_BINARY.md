# Design: Single static binary, CLI, and single-binary operations

Issue: #81. Decisions: ADR-0020 (clap dispatch + signing), ADR-0017 (Simple gate:
musl static, install-to-first-GET). Related: #85 (config), #86 (observability),
#83 (upgrade), #84 (packaging), #22/#105 (TLS in-process).

## Goal and scope

IronCache ships as exactly one reproducible static binary per architecture. That
binary is the server, the CLI, the benchmark tool, the data checker, the config
tool, and its own updater. It boots on `ironcache server` with safe defaults and
zero config, the way Redis runs with no config file by default
[redis-no-config-file-default], and serves its own metrics and TLS in-process
with no sidecar. This is the headline operability thesis; every operator-facing
design hangs off it.

## Design

### One binary, six modes

The clap subcommands (ADR-0020) are `server`, `cli`, `bench`, `check`, `config`,
and `upgrade`. `server` is the daemon; `cli` is the interactive client (with a
`redis-cli` alias); `bench` is the benchmark harness (#8); `check` validates a
data directory; `config` reads/edits the config file; `upgrade` self-updates with
verified rollback (#83). This collapses Redis's six separate binaries
[redis-separate-binaries-symlinks] into one, following Dragonfly's single
self-contained binary [dragonfly-single-binary-gflags].

### Zero-config boot

`ironcache server` with no arguments binds the default port, derives a memory
ceiling from host RAM with eviction on (ADR-0007), and serves, so install to
first `GET` is under the Simple-gate bound (ADR-0017). A config file is optional,
matching Redis [redis-no-config-file-default]; flags and the file are #85.

### Static binary

One static musl binary per architecture (x86_64, aarch64), crt-static on the
default target [rust-musl-crt-static-default], cross-built with cargo-zigbuild
[cargo-zigbuild-version-features], reproducible with an embedded SBOM via
cargo-auditable [cargo-auditable-version-reproducible]. Kernel-only at runtime
(the Simple gate, ADR-0017): no JVM, no .NET, no sidecar. TLS is in-process
(rustls, #105), and metrics are in-process (#86), so there is no exporter or
proxy process.

### Self-update

`ironcache upgrade` fetches the new artifact, verifies its minisign signature and
SBOM (ADR-0020), atomically swaps the binary, and rolls back if the new version
fails to come up (the full rollback contract is #83, the packaging/distribution
is #84).

## Open questions

- Whether `bench` and `check` ship in the same binary always or behind a build
  feature to trim size (interacts with the Simple-gate binary-size ceiling, #84).
- The exact default port and whether to auto-detect a free port in dev mode.

## Acceptance and test hooks

- `ironcache server` with no config boots and serves `GET`/`SET`/`PING` within
  the install-to-first-GET bound (ADR-0017), with eviction + ceiling on
  (ADR-0007).
- One static musl binary per arch, kernel-only at runtime, reproducible
  (byte-identical rebuild) with an embedded SBOM (#84).
- Each subcommand has a `--help` snapshot test; the `redis-cli` alias forwards to
  `cli` (ADR-0020).

## References

- ADR-0007, ADR-0017, ADR-0020; issues #85, #86, #83, #84, #22, #105, #8.
- Claims: [redis-no-config-file-default], [redis-separate-binaries-symlinks],
  [dragonfly-single-binary-gflags], [rust-musl-crt-static-default],
  [cargo-zigbuild-version-features], [cargo-auditable-version-reproducible].
