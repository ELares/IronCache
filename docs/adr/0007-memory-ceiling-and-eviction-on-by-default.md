# ADR-0007: Ship a memory ceiling and eviction ON by default

Status: Accepted
Issue: #45

## Context

Stock Redis ships `maxmemory 0` and `maxmemory-policy noeviction`
[redis-maxmemory-default] [redis-maxmemory-policy-default], so out of the box it
is an unbounded store that returns OOM errors on write rather than a cache that
evicts. For a product whose identity is "a cache", that default pushes
correctness onto the operator and is a documented footgun. This decides the
default posture.

## Decision

IronCache defaults to **cache mode**: a host-derived memory ceiling (a fraction
of detected RAM) with **eviction enabled** using the default policy (ADR-0008).
Strict no-evict **datastore mode** (`maxmemory-policy noeviction`) is an explicit
opt-in for users who want a bounded store that refuses writes rather than evicts.
The ceiling is enforced against allocator-attributed bytes (ADR-0006).

## Rejected Alternatives

- **Mirror Redis (`maxmemory 0`, `noeviction`).** Rejected on Simple: byte-for-
  byte default parity is the only upside, and it ships a "cache" that does not
  cache and OOMs on write unless the operator intervenes. Safe defaults that
  match the product's identity outrank surprise-minimizing parity, and the
  divergence is in the operationally safer direction.
- **Ceiling on but `noeviction` default (bounded store that errors on write).**
  This was issue #45's Option C. Rejected as the worst of both: it caps memory
  but then refuses writes when full instead of caching, blurring the
  cache-versus-datastore line precisely when an operator most expects cache
  behavior. Datastore semantics are available, but as the explicit opt-in above,
  not the default.

## Consequences

- Install-to-first-GET yields a working bounded cache with no tuning, serving the
  Simple tenet; this mirrors the spirit of Dragonfly's cache_mode
  [dragonfly-cache-mode-eviction] but with a policy chosen on evidence (ADR-0008).
- The default is a documented, deliberate divergence from Redis, recorded so the
  compatibility tiering (#16) can flag it as a default-behavior difference (not a
  command-semantics difference).
- The host-RAM fraction, its floor/ceiling, and container-awareness
  (cgroup limits) are config knobs decided in the config issue (#85).
