# Glossary

The canonical meaning of project-wide terms. When a design issue and a review
disagree about what a term means, this file wins. Each term names the issue that
owns its design. Companion to [INVARIANTS.md](INVARIANTS.md); both roll up to
issue #3.

- **shard**: the unit of keyspace partition inside one process. A key maps to a
  shard by `k = HASH(KEY) % N`, where N is at most the core count
  [dragonfly-shard-formula]. Distinct from a Redis-wire hash slot. Owner: #24.
- **slot**: a Redis Cluster hash slot (0 to 16383), a protocol-level concept for
  client routing and resharding. Many slots map onto one IronCache shard; the
  two are not interchangeable. Owner: #70.
- **hot path**: the per-request GET/SET execution path inside a shard's owning
  core. State touched here must be shard-local: no locks, no cross-core atomics
  [glommio-locks-never-necessary]. Owner: #24.
- **advisor**: an out-of-band component that observes traffic and proposes
  tuning (encoding thresholds, admission, knobs). Advisors never sit on the hot
  path and never make a decision the hot path must block on. Owner: #88.
- **codec**: the wire-level encoder/decoder for RESP2/RESP3 frames. Strictly
  protocol; carries no storage semantics. Owner: #15.
- **encoding**: the in-memory representation of a value (inline string,
  listpack-equivalent, hashtable, and so on). An implementation detail surfaced
  only through `OBJECT ENCODING` compatibility. Owner: #40.
- **tier**: a storage level distinguished by latency and cost (in-memory versus
  cold or compressed). Orthogonal to shard and encoding. Owner: #66.

This file is updated whenever a term's canonical meaning changes; it does not
re-litigate the owning issue's design.
