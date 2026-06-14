# Design specifications

The subsystem design specs that gate implementation. Each builds on the
[Architecture Decision Records](../adr/INDEX.md) (the decisions) and the
[charter](../CHARTER.md) tenets, and is the spec the engine code will implement.
Claim citations in these docs are validated in CI by
[`check-prior-art-claims.sh`](../../scripts/ci/check-prior-art-claims.sh).

- [PROTOCOL.md](PROTOCOL.md): RESP parser, serializer, the HELLO-driven
  per-connection state machine, and the Tier 0 connection commands (#15).
- [ERRORS.md](ERRORS.md): the canonical Redis-compatible error-string catalog
  (#18).
- [HARDENING.md](HARDENING.md): RESP request-size and adversarial-input limits
  (#138).
- [RUNTIME.md](RUNTIME.md): shared-nothing thread-per-core runtime, io_uring
  fast path with portable fallback, and the Env seam (#25).
- [STORAGE_API.md](STORAGE_API.md): the four-primitive narrow-waist storage
  contract (Read/Upsert/Delete/RMW) with hooks (#34).

More design specs land here as the Implementation Readiness milestone progresses
(runtime, storage API, hash table, eviction, expiration, commands, CLI, config,
observability, testing, benchmarking).
