# Design: TOML config with CONFIG GET/SET/REWRITE parity and live reload

Issue: #85. Decisions: ADR-0020 (clap flags), ADR-0009 (compat). Related: #81
(binary), #86 (observability), #137 (admission knobs), #15 (CONFIG command).

## Goal and scope

Configuration must load from a file for reproducible deploys, be overridable at
the wire for compatibility with existing Redis tooling, and be reloadable without
a restart, an operational property Redis does not give (its CONFIG SET is not
persisted without CONFIG REWRITE, and there is no SIGHUP reload
[redis-config-set-rewrite-no-sighup]). This specifies the config system end to
end. (The #85 issue body carried an orchestrator "INPUT GAP" banner from the
old undefined-path bug; this design is grounded directly in the pinned claims.)

## Design

### Sources and precedence

The effective value of each key is resolved across ordered layers, highest
precedence first: runtime `CONFIG SET` > command-line flags (ADR-0020 root flags)
> environment variables > the TOML config file > built-in safe defaults
(ADR-0007 cache-mode posture). The layers are kept distinct rather than collapsed
into one mutable struct, so each source can change independently and the effective
value is always recomputed as the highest-precedence layer that set the key. A key
set by `CONFIG SET` wins until it is explicitly overwritten or cleared (a later
`CONFIG SET`, or a `CONFIG RESETSTAT`-style clear of the runtime layer); the
precedence is documented so an operator can reason about the effective value,
surfaced by `CONFIG GET`.

### Wire parity: CONFIG GET / SET / REWRITE

- `CONFIG GET <pattern>` returns the effective values with Redis-recognized
  parameter names (glob patterns supported), so existing tooling works
  (ADR-0009).
- `CONFIG SET` applies a runtime change to the live struct; whether it is durable
  depends on `CONFIG REWRITE`.
- `CONFIG REWRITE` writes the current effective config back to the TOML file,
  preserving comments and structure where possible. This matches Redis's
  set-then-rewrite-to-persist model [redis-config-set-rewrite-no-sighup].
- Parameters IronCache does not implement (or that are no-ops under its engine,
  for example `maxmemory-samples`, #50) are accepted and echoed for compatibility,
  documented as no-ops.

### Live reload (beyond Redis)

Unlike Redis, IronCache supports reloading the config file without a restart: a
`config reload` admin action (and the `ironcache config` subcommand) re-reads the
file and refreshes **only the file layer**, then recomputes each effective value
across the layers above. A live `CONFIG SET` override therefore survives a reload
(it sits in a higher-precedence layer) and is not silently clobbered by the new
file contents; an operator who wants the file value back clears the runtime layer
explicitly. Reload applies to the parameters that are safely hot-swappable (memory
ceiling, eviction policy via #50, log level, slowlog/latency thresholds, admission
limits #137, codec policy #53). Parameters that cannot change at runtime (bind
address, the shard/core count) are reported as requiring a restart rather than
silently ignored. Reload is atomic per parameter and goes through the same
validation as startup.

### Container awareness

The default memory ceiling (ADR-0007) is derived from the cgroup limit when
present, not just host RAM, so a containerized IronCache sizes itself correctly.

## Open questions

- The exact hot-swappable vs restart-required parameter partition (which knobs
  can change live without breaking an invariant), enumerated as each subsystem
  lands.
- Whether `CONFIG REWRITE` round-trips comments fully or only preserves a managed
  block.

## Acceptance and test hooks

- `CONFIG GET`/`SET` for the supported parameters match the pinned oracle
  (names, glob, reply shape) (#97); unsupported parameters are accepted+echoed and
  documented.
- `CONFIG SET` then `CONFIG REWRITE` persists to the TOML file and survives a
  restart.
- A `config reload` applies a file change to a hot-swappable parameter with no
  restart and reports a restart-required parameter rather than silently dropping
  it.
- A `CONFIG SET` override survives a subsequent `config reload` of a changed file
  (the runtime layer wins); clearing the runtime layer then lets the reloaded file
  value take effect (a precedence-composition test).

## References

- ADR-0007, ADR-0009, ADR-0020; issues #81, #86, #137, #50, #53, #15, #97.
- Claims: [redis-config-set-rewrite-no-sighup], [redis-no-config-file-default].
