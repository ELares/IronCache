// SPDX-License-Identifier: MIT OR Apache-2.0
//! Layered configuration for IronCache (CONFIG.md, #85).
//!
//! The effective value of each key is resolved across ordered layers, highest
//! precedence first (CONFIG.md "sources and precedence"):
//!
//! ```text
//! runtime CONFIG SET  >  CLI flags  >  environment variables  >  TOML file  >  built-in defaults
//! ```
//!
//! The lower four startup layers fold into a [`Config`] at boot via
//! [`Config::resolve`] ([`ConfigOverlay`] of optional fields, defaults-first so a
//! higher layer that sets a key wins). The HIGHEST layer, the runtime `CONFIG SET`
//! overlay, is the separate [`RuntimeConfig`] cell (PR-4b): it sits ABOVE the
//! resolved [`Config`] and is the one a `CONFIG SET` mutates, so a future file
//! reload (which re-folds only the lower layers) can never clobber a runtime
//! override. The [`registry`] maps Redis parameter names to the getters/setters
//! `CONFIG GET`/`CONFIG SET` dispatch over. Human sizes ("512mb") are parsed by
//! [`parse_human_size`].

// No unsafe anywhere in the config crate (it folds layered config and hashes the
// requirepass password); the hand-rolled SHA-256 (#65) is pure safe Rust, so the
// whole crate forbids unsafe, matching every other workspace crate.
#![forbid(unsafe_code)]

pub mod notify;
pub mod persist_cpu;
pub mod registry;
pub mod runtime;
pub mod sha256;

pub use notify::{EventClass, KeyspaceEvent, NotifyFlags};
pub use persist_cpu::{PersistCpu, parse_persist_cpu, select_persist_cpus};
pub use registry::{
    ParamSpec, SetKind, SetOutcome, apply_set, effective_value, lookup, param_specs,
};
pub use runtime::RuntimeConfig;
pub use sha256::sha256_hex;

use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use thiserror::Error;

/// The default RESP port. Redis/Valkey use 6379; IronCache keeps it for drop-in
/// compatibility (CLI_BINARY.md leaves the exact port open but defaults to parity).
pub const DEFAULT_PORT: u16 = 6379;

/// The default list listpack byte budget (`list-max-listpack-size`), expressed as a
/// BYTE budget (PR-5, ENCODINGS.md / OBJECT_ENCODING_MAPPING.md #40, LIST_LARGE.md
/// "Node sizing ~8 KB"). Redis's default `-2` means "8 KB per listpack node"; we
/// store the resolved byte budget directly. A LIST whose total element bytes stay at
/// or below this is `OBJECT ENCODING` -> `listpack`; once it exceeds this it
/// transitions to `quicklist`. There is NO element-count cap for lists: the Redis
/// `-2` negative fill sizes the node by BYTES with the element count left unlimited
/// (`quicklistNodeLimit` sets `count = UINT_MAX`), so a 129-element list of small
/// values stays `listpack`. The store reads this default; `CONFIG GET
/// list-max-listpack-size` reports the Redis `-2` spelling.
pub const DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES: usize = 8 * 1024;

/// The default per-collection element-count cap for a HASH listpack
/// (`hash-max-listpack-entries`, default 512; verified against Redis 7.4 `config.c` /
/// `t_hash.c` and the pinned claim [redis-hash-max-listpack-entries-512]). A hash whose
/// entry count exceeds this transitions away from `listpack` to `hashtable` even when it
/// is under the per-element byte cap. This is the HASH cap SPECIFICALLY: it is NOT the
/// list cap (lists have no entry cap, only the byte budget above) and NOT the ZSET/SET
/// cap (those default to 128, see [`DEFAULT_ZSET_MAX_LISTPACK_ENTRIES`]; the older
/// "128 shared by hash and zset" reading conflated the per-type defaults). Wired into the
/// hash encoding logic in PR-6; kept here so the collection PRs share the pinned default.
pub const DEFAULT_HASH_MAX_LISTPACK_ENTRIES: usize = 512;

/// The default per-collection element-count cap for a ZSET listpack
/// (`zset-max-listpack-entries`, default 128). RESERVED for the PR-8 sorted-set encoding
/// logic; kept here so that PR shares the pinned default. This is DISTINCT from the HASH
/// cap ([`DEFAULT_HASH_MAX_LISTPACK_ENTRIES`] = 512): Redis's 128 entry default applies to
/// ZSETs and SETs (`set-max-listpack-entries`), NOT to hashes. SETs get their own constant
/// in PR-7 (they also have `set-max-intset-entries`, a separate 512 default), so this is
/// the ZSET constant only.
pub const DEFAULT_ZSET_MAX_LISTPACK_ENTRIES: usize = 128;

/// The default per-member BYTE cap for a ZSET `listpack` (`zset-max-listpack-value`,
/// default 64; [redis-zset-max-listpack-entries-128]). A zset whose ANY member byte
/// length exceeds this transitions away from `listpack` to `skiplist`, EVEN when it has
/// few entries (the byte cap is per-member, not a total). This is the ZSET companion to
/// [`DEFAULT_ZSET_MAX_LISTPACK_ENTRIES`] (the entry cap): a zset stays `listpack` while
/// `entries <= zset-max-listpack-entries` AND every member byte length `<=
/// zset-max-listpack-value`. SHARES the 64 value with the HASH/SET per-element byte caps
/// ([`DEFAULT_HASH_MAX_LISTPACK_VALUE`] / [`DEFAULT_SET_MAX_LISTPACK_VALUE`]) but is a
/// SEPARATE Redis parameter. Wired into the zset encoding logic in PR-8.
pub const DEFAULT_ZSET_MAX_LISTPACK_VALUE: usize = 64;

/// The default per-field/value BYTE cap for a HASH listpack (`hash-max-listpack-value`,
/// default 64). A hash whose ANY field-or-value byte length exceeds this transitions
/// away from `listpack` to `hashtable`, EVEN when it has few entries (the byte cap is
/// per-element, not a total). This is the HASH companion to
/// [`DEFAULT_HASH_MAX_LISTPACK_ENTRIES`] (the entry cap): a hash stays `listpack` while
/// `entries <= hash-max-listpack-entries` AND every field-and-value byte length `<=
/// hash-max-listpack-value`. Wired into the hash encoding logic in PR-6; kept here so
/// PR-6/7 share the pinned default.
pub const DEFAULT_HASH_MAX_LISTPACK_VALUE: usize = 64;

/// The default ALL-INTEGER set element-count cap for the `intset` encoding
/// (`set-max-intset-entries`, default 512; verified against Redis 7.4 `config.c` /
/// `t_set.c` and the pinned claims [redis-set-encoding-defaults] /
/// [redis-set-encodings-thresholds]). An all-integer set stays `intset` (a sorted
/// integer array, binary-search membership) while its member count is at or below this;
/// growth PAST it converts away from `intset`. Because 512 > the 128 listpack-entries
/// cap below, an integer set that exceeds 512 members goes STRAIGHT to `hashtable` (it
/// cannot fit the 128-member listpack). This is the SET-specific intset cap, DISTINCT
/// from the hash 512 entry cap ([`DEFAULT_HASH_MAX_LISTPACK_ENTRIES`], a different param
/// that happens to share the 512 value) and from the set listpack-entries cap below.
/// Wired into the set encoding logic in PR-7.
pub const DEFAULT_SET_MAX_INTSET_ENTRIES: usize = 512;

/// The default per-collection element-count cap for a SET `listpack`
/// (`set-max-listpack-entries`, default 128; [redis-set-encodings-thresholds]). Once an
/// all-integer set takes a NON-integer member (leaving `intset`) it becomes `listpack`
/// IFF the resulting member count is at or below this AND every member byte length is at
/// or below [`DEFAULT_SET_MAX_LISTPACK_VALUE`]; otherwise it becomes `hashtable`. A
/// listpack set that grows past this (or past the per-member byte cap) converts to
/// `hashtable`. This SHARES the 128 value with the ZSET listpack cap
/// ([`DEFAULT_ZSET_MAX_LISTPACK_ENTRIES`]) but is a SEPARATE Redis parameter.
pub const DEFAULT_SET_MAX_LISTPACK_ENTRIES: usize = 128;

/// The default per-member BYTE cap for a SET `listpack` (`set-max-listpack-value`,
/// default 64; [redis-set-encodings-thresholds]). A listpack set whose ANY member byte
/// length exceeds this converts to `hashtable`, EVEN when it has few members (the byte
/// cap is per-member, not a total). The SET companion to
/// [`DEFAULT_SET_MAX_LISTPACK_ENTRIES`] (the entry cap). Wired into the set encoding
/// logic in PR-7.
pub const DEFAULT_SET_MAX_LISTPACK_VALUE: usize = 64;

/// The Redis `list-max-listpack-size` default SPELLING (`-2` = "8 KB per node"). This
/// is what `CONFIG GET list-max-listpack-size` echoes BY DEFAULT (the configured Redis
/// form), while the store works in the resolved byte budget
/// ([`DEFAULT_LIST_MAX_LISTPACK_SIZE_BYTES`]). The default integer form is
/// [`DEFAULT_LIST_MAX_LISTPACK_SIZE`] (`-2`); a `CONFIG SET list-max-listpack-size`
/// overrides the live value (see [`runtime::RuntimeConfig::list_max_listpack_size`]).
pub const LIST_MAX_LISTPACK_SIZE_REDIS_DEFAULT: &str = "-2";

/// The default `list-max-listpack-size` in the RAW Redis integer form (`-2` = the 8 KB
/// negative size-tier). A NEGATIVE value `-1..-5` selects a fixed per-node BYTE budget
/// (Redis `quicklist` size tiers: `-1`=4 KB, `-2`=8 KB, `-3`=16 KB, `-4`=32 KB,
/// `-5`=64 KB); a POSITIVE value is a max element COUNT per listpack node. The store
/// resolves this to its transition predicate via
/// [`EncodingThresholds::list_budget`](ironcache_storage::EncodingThresholds::list_budget).
/// Stored signed so the negative form round-trips through `CONFIG GET`.
pub const DEFAULT_LIST_MAX_LISTPACK_SIZE: i64 = -2;

/// The default `proto-max-bulk-len` in bytes (Redis default 512 MB): the maximum size of
/// a single inbound bulk string the RESP decoder accepts, and the ceiling a string
/// value (APPEND/SETRANGE/SETBIT) may grow to. Runtime-settable via
/// `CONFIG SET proto-max-bulk-len` (see [`runtime::RuntimeConfig::proto_max_bulk_len`]).
pub const DEFAULT_PROTO_MAX_BULK_LEN: u64 = 512 * 1024 * 1024;

/// The default `tcp-keepalive` in SECONDS (Redis default 300): the SO_KEEPALIVE idle
/// interval applied to a newly-accepted client connection. `0` DISABLES keepalive.
/// Runtime-settable via `CONFIG SET tcp-keepalive` (affects newly-accepted connections;
/// see [`runtime::RuntimeConfig::tcp_keepalive_secs`]).
pub const DEFAULT_TCP_KEEPALIVE: u64 = 300;

/// The default shard count: the host's available parallelism via
/// [`std::thread::available_parallelism`] (CONFIG.md), which honors cgroup CPU
/// quotas (unlike the `num_cpus` crate). Never zero (a degenerate host reports at
/// least one).
#[must_use]
pub fn default_shards() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Errors from loading or resolving configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The TOML file could not be parsed.
    #[error("config file parse error: {0}")]
    Toml(#[from] toml::de::Error),
    /// An I/O error reading the config file.
    #[error("config file read error: {0}")]
    Io(String),
    /// A human-size string ("512mb") was malformed.
    #[error("invalid size value '{0}': {1}")]
    Size(String, String),
    /// A field held an out-of-range or otherwise invalid value.
    #[error("invalid config value for {field}: {reason}")]
    Invalid {
        /// The offending field name.
        field: &'static str,
        /// Why it was rejected.
        reason: String,
    },
}

/// One node in the static cluster topology (CLUSTER_CONTRACT.md #70, slice 2). TOML-only
/// (a nested array-of-tables); there is no env/CLI form for the structured topology.
///
/// `id` is the stable 40-lowercase-hex node id, `host`/`port` are the ADVERTISED endpoint
/// clients dial (a MOVED redirect points here, NOT at the bind address, which may be
/// `0.0.0.0`), and `slots` is the list of inclusive `[start, end]` slot ranges this node
/// owns. Validation (gap/overlap/dup-id/bad-range/bad-id) is delegated to
/// `ironcache_cluster::SlotMap::build` so there is one slot-assignment validator.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClusterNode {
    /// The stable 40-lowercase-hex node id.
    pub id: String,
    /// The advertised host clients dial (NOT the bind address).
    pub host: String,
    /// The advertised TCP port clients dial.
    pub port: u16,
    /// The inclusive `[start, end]` slot ranges this node owns.
    pub slots: Vec<[u16; 2]>,
}

/// The static cluster topology: the full set of nodes and their slot assignments
/// (CLUSTER_CONTRACT.md #70, slice 2). TOML-only (`[[cluster_topology.nodes]]`). When set
/// (and `cluster_enabled`), it flips IronCache from the slice-1 single-node-owns-all
/// behavior to real multi-node routing (MOVED / CROSSSLOT / multi-node projection).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct ClusterTopology {
    /// The nodes, in declaration order.
    pub nodes: Vec<ClusterNode>,
}

/// HOW the cluster's slot-ownership map is GOVERNED at runtime (HA-4c). Only meaningful when
/// `cluster_enabled`; the default ([`ClusterMode::Static`]) keeps the slice-2/slice-3 behavior
/// BYTE-UNCHANGED.
///
/// * [`Static`](ClusterMode::Static) (DEFAULT): the slot map is a STATIC config-driven map
///   (slice 2) OR is mutated DIRECTLY by the local `CLUSTER MEET / ADDSLOTS / SETSLOT / ...`
///   verbs (slice 3). This is the only path slice 1-3 ever exercised; a node compiled and run
///   in this mode pays ZERO new cost and behaves exactly as before HA-4c.
/// * [`Raft`](ClusterMode::Raft) (OPT-IN): the merged, DST-verified Raft control plane GOVERNS
///   the cluster. A `CLUSTER ADDSLOTS / SETSLOT / MEET / FORGET / SET-CONFIG-EPOCH` becomes a
///   PROPOSAL the leader commits through the replicated log; every node applies the SAME
///   committed sequence into its shared `SlotMap`, so all nodes converge to one identical
///   ownership view (no two owners per epoch). A follower redirects the mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterMode {
    /// The pre-HA-4c behavior: static topology / slice-3 local direct mutation. The DEFAULT.
    #[default]
    Static,
    /// Opt-in: the Raft control plane governs slot ownership via committed proposals (HA-4c).
    Raft,
    /// Opt-in (#517): a SINGLE node exposes its N INTERNAL shards as N hashslot owners, each on its
    /// own port (`shard_base_port + i`), so a cluster-aware client routes each key straight to the
    /// shard that owns it -- eliminating the internal cross-shard hop. The slot->owner partition is
    /// the same `slot_to_shard` the router uses, so the connection homes on the key's owner. Mutually
    /// exclusive with `Raft` (which governs ownership across PHYSICAL nodes). TOML
    /// `cluster_mode = "shard-owners"` / `IRONCACHE_CLUSTER_MODE=shard-owners`.
    #[serde(rename = "shard-owners", alias = "shardowners")]
    ShardOwners,
}

/// The embedded transport-TLS posture for the CLIENT listener (#105, docs/design/TLS.md).
/// The default ([`TlsMode::Off`]) keeps the listener PLAINTEXT and byte-unchanged.
///
/// * [`Off`](TlsMode::Off) (DEFAULT): the client port speaks plaintext RESP exactly as before
///   TLS existed. No cert/key is loaded; the rustls layer is never touched (the accept path
///   returns a plain `TcpStream`, the hot path has no per-byte TLS cost).
/// * [`On`](TlsMode::On) (OPT-IN): the client port is TLS-ONLY. Every accepted connection
///   performs a rustls handshake (TLS 1.2/1.3, server-auth) BEFORE the RESP loop; a plaintext
///   client to this port FAILS the handshake and is rejected (not hung). Requires
///   [`Config::tls_cert_path`] + [`Config::tls_key_path`] (validated readable at boot).
///
/// A mixed both-ports posture (a separate `tls-port` alongside the plaintext port, the shape
/// docs/design/TLS.md sketches) is a documented follow-up; v1 is off|on on the one client port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    /// Plaintext client listener (the default, byte-unchanged).
    #[default]
    Off,
    /// TLS-only client listener (opt-in): every connection is rustls-terminated.
    On,
}

/// WHICH async I/O backend the per-shard runtime uses (PROD-10 / #28,
/// docs/design/IOURING_DATAPATH.md). The default ([`RuntimeBackend::Tokio`]) is the portable
/// epoll/kqueue backend that runs on every platform and is byte-unchanged.
///
/// * [`Tokio`](RuntimeBackend::Tokio) (DEFAULT): the cross-platform tokio current-thread,
///   per-core-pinned backend (RUNTIME.md, ADR-0002). The only backend on macOS/Windows/BSD and
///   the only one in the default build (the `io_uring` Cargo feature is off by default).
/// * [`IoUring`](RuntimeBackend::IoUring) (OPT-IN, LINUX-ONLY): the io_uring datapath. It takes
///   effect ONLY when the binary was built `--features io_uring` AND is running on Linux AND TLS
///   is off; in EVERY other case the boot SILENTLY FALLS BACK to `Tokio` (with a one-line log),
///   so requesting it can never fail to start a node. The full registered-buffer / multishot
///   fast path is deferred to a Linux soak (no throughput claim is made at v1).
///
/// TOML (`runtime = "io_uring"`) + the `IRONCACHE_RUNTIME` env var + the `--runtime` CLI flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBackend {
    /// The portable tokio epoll/kqueue backend (the default; every platform).
    #[default]
    Tokio,
    /// The opt-in, Linux-only io_uring backend (falls back to tokio off-Linux / no-feature / TLS).
    IoUring,
}

/// The fully-resolved, effective configuration the server boots from.
// This is a flat Redis-style config record: each bool is an INDEPENDENT operator knob
// (`default_resp3`, `cluster_enabled`, `cluster_raft_joining`, `cluster_tls_insecure_skip_verify`),
// not a hidden state machine. Folding them into enums would obscure, not clarify, the 1:1 mapping to
// the documented TOML/env knobs, so the excessive-bools lint is allowed here by design.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Address to bind the RESP listener.
    pub bind: IpAddr,
    /// TCP port for the RESP listener.
    pub port: u16,
    /// Number of shards / per-core runtimes. Defaults to available parallelism.
    pub shards: usize,
    /// Number of logical databases (`SELECT 0..databases-1`). Redis default 16.
    pub databases: u32,
    /// The number of per-slot store tables each database is partitioned into (#570, the
    /// bounded-resize tail-latency lever). A MEMORY vs tail-latency tradeoff: more slots
    /// bound a `hashbrown` all-at-once resize to fewer entries (~N/slots) so the p99.9
    /// insert stall shrinks, at a small fixed per-touched-DB `Vec` cost. Default 256 (the
    /// store's [`DEFAULT_SLOTS_PER_DB`](../ironcache_store) safe default, which keeps the
    /// perf-gate bytes-per-key green). Structural, so restart-required (immutable at
    /// runtime like `databases`); the store rounds it UP to a power of two. `0` is rejected.
    pub slots_per_db: u32,
    /// The default protocol for a new connection before `HELLO 3` (always RESP2
    /// per PROTOCOL.md; exposed for completeness/testing).
    pub default_resp3: bool,
    /// Memory ceiling in bytes. `0` means unlimited (PR-1 does not enforce it;
    /// PR-3a enforces it at the dispatch layer via the per-shard budget). The value
    /// is parsed and surfaced for `config`/INFO.
    pub maxmemory: u64,
    /// The eviction policy: one of the eight Redis `maxmemory-policy` names
    /// (EVICTION.md #50). Defaults to an eviction-ON name (`allkeys-lru`) per
    /// ADR-0007 (cache mode), NOT Redis's `noeviction` default. Validated against the
    /// recognized names in [`Config::validate`].
    pub maxmemory_policy: String,
    /// Optional `requirepass` credential, stored as the SHA-256 HEX digest of the
    /// password AT REST (AUTH.md "Passwords are stored as SHA-256", #65), NOT the
    /// plaintext. `None` means auth is not required.
    ///
    /// The boot INPUT (TOML/env/CLI) is plaintext, so the plaintext is folded through
    /// the overlays and then hashed ONCE at the end of [`Config::resolve`] (via
    /// [`Config::finalize_requirepass`]); the long-lived `Config` therefore holds only
    /// the digest, and no plaintext password persists past config load. An
    /// empty/unset password stays `None`.
    pub requirepass: Option<String>,
    /// Idle timeout in seconds; `0` disables idle disconnection (Redis default 0,
    /// CONNECTION_LIFECYCLE.md).
    pub timeout_secs: u64,
    /// The maximum number of simultaneous client connections (Redis `maxclients`,
    /// default 10000). A new connection accepted while the process is AT this cap is
    /// rejected with `-ERR max number of clients reached` (the error is written, then
    /// the socket is closed), bounding the connection-exhaustion DoS (PROD-SAFETY #3).
    /// Tracked against the process-global connected-clients gauge. `0` DISABLES the cap
    /// (unlimited connections, the pre-fix behavior), but the default is the Redis
    /// 10000 ceiling so an unconfigured node is protected.
    pub maxclients: u64,
    /// The per-connection OUTPUT-BUFFER hard cap in bytes (the IronCache analog of Redis
    /// `client-output-buffer-limit`, PROD-SAFETY #5). A connection whose pending unflushed
    /// reply buffer would exceed this is CLOSED rather than allowed to grow unbounded (a
    /// slow consumer / a huge reply / a pub-sub flood would otherwise be a server-memory
    /// DoS). `0` DISABLES the cap (the pre-fix unbounded behavior); the default is a high
    /// ceiling ([`DEFAULT_OUTPUT_BUFFER_LIMIT`]) so a legitimate large pipeline/reply is
    /// unaffected while a pathological accumulation is bounded.
    pub output_buffer_limit: u64,
    /// The per-connection QUERY-BUFFER hard cap in bytes (the inbound analog of
    /// [`output_buffer_limit`](Self::output_buffer_limit); Redis `client-query-buffer-limit`,
    /// #528). A connection whose accumulated inbound read buffer would exceed this is CLOSED
    /// rather than allowed to grow unbounded: a client that announces a large multibulk
    /// (`*<huge>\r\n`) and then dribbles the elements slowly forces the server to buffer every
    /// partial byte while the frame never completes, a PRE-AUTH memory-amplification DoS. `0`
    /// DISABLES the cap (the pre-fix unbounded behavior); the default is a high ceiling
    /// ([`DEFAULT_QUERY_BUFFER_LIMIT`]) so a legitimate large request / deep pipeline is unaffected
    /// while a slow-dribble accumulation is bounded.
    pub query_buffer_limit: u64,
    /// The maximum size in bytes of a single inbound bulk string the RESP decoder accepts,
    /// AND the ceiling a string value may grow to (Redis `proto-max-bulk-len`, default
    /// [`DEFAULT_PROTO_MAX_BULK_LEN`] = 512 MB). A bulk string larger than this is a
    /// protocol error; an APPEND/SETRANGE/SETBIT/BITFIELD that would grow a value past it
    /// is rejected (Redis `checkStringLength`). Runtime-settable: the serve loop builds the
    /// decoder [`Limits`](crate) from the live overlay per connection, and the string /
    /// bitmap ceilings read the live value, so a `CONFIG SET proto-max-bulk-len` takes
    /// effect without a restart. `0` is rejected at set time (a zero ceiling would reject
    /// every value).
    pub proto_max_bulk_len: u64,
    /// The TCP keepalive idle interval in SECONDS applied to a newly-accepted client
    /// connection (Redis `tcp-keepalive`, default [`DEFAULT_TCP_KEEPALIVE`] = 300). `0`
    /// DISABLES keepalive. The accept path enables SO_KEEPALIVE with this idle time so a
    /// dead peer (a half-open connection behind a NAT/firewall that dropped state) is
    /// detected and the connection reaped. Runtime-settable: the accept path reads the live
    /// overlay, so a `CONFIG SET tcp-keepalive` applies to NEWLY-accepted connections
    /// (existing connections keep the option set at their own accept time, matching Redis).
    pub tcp_keepalive_secs: u64,
    /// The HASH listpack->hashtable entry-count cap (Redis `hash-max-listpack-entries`,
    /// default [`DEFAULT_HASH_MAX_LISTPACK_ENTRIES`]). The store reads the live runtime
    /// value at the hash encoding-transition decision; a `CONFIG SET` affects FUTURE
    /// inserts only (existing keys keep their encoding).
    pub hash_max_listpack_entries: usize,
    /// The HASH listpack->hashtable per-field/value byte cap (Redis `hash-max-listpack-value`,
    /// default [`DEFAULT_HASH_MAX_LISTPACK_VALUE`]).
    pub hash_max_listpack_value: usize,
    /// The LIST listpack->quicklist size in the SIGNED Redis form (`list-max-listpack-size`,
    /// default [`DEFAULT_LIST_MAX_LISTPACK_SIZE`] = `-2`). Negative selects a byte tier;
    /// positive is a max element count per node (see
    /// [`EncodingThresholds::list_budget`](ironcache_storage::EncodingThresholds::list_budget)).
    pub list_max_listpack_size: i64,
    /// The SET intset entry-count cap (Redis `set-max-intset-entries`, default
    /// [`DEFAULT_SET_MAX_INTSET_ENTRIES`]).
    pub set_max_intset_entries: usize,
    /// The SET listpack->hashtable entry-count cap (Redis `set-max-listpack-entries`, default
    /// [`DEFAULT_SET_MAX_LISTPACK_ENTRIES`]).
    pub set_max_listpack_entries: usize,
    /// The SET listpack per-member byte cap (Redis `set-max-listpack-value`, default
    /// [`DEFAULT_SET_MAX_LISTPACK_VALUE`]).
    pub set_max_listpack_value: usize,
    /// The ZSET listpack->skiplist entry-count cap (Redis `zset-max-listpack-entries`, default
    /// [`DEFAULT_ZSET_MAX_LISTPACK_ENTRIES`]).
    pub zset_max_listpack_entries: usize,
    /// The ZSET listpack per-member byte cap (Redis `zset-max-listpack-value`, default
    /// [`DEFAULT_ZSET_MAX_LISTPACK_VALUE`]).
    pub zset_max_listpack_value: usize,
    /// Whether the server runs in cluster mode (Redis `cluster-enabled`,
    /// CLUSTER_CONTRACT.md #70). BOOT-ONLY (immutable at runtime, like Redis): it is
    /// reported by `CLUSTER INFO` (`cluster_enabled:0/1`) and the INFO `# Cluster`
    /// section, and gates the mutating CLUSTER subcommands. Defaults to `false`
    /// (cluster-disabled), matching a standalone Redis. Slice 1 only ever observes
    /// `false`: it adds the read-only CLUSTER introspection surface without any routing
    /// change; turning this on is a later slice.
    pub cluster_enabled: bool,
    /// The static cluster topology (CLUSTER_CONTRACT.md #70, slice 2). TOML-only. `None`
    /// keeps the slice-1 single-node-owns-all behavior even when `cluster_enabled` (a
    /// topology is opt-in); `Some` flips on multi-node routing (MOVED / CROSSSLOT) and the
    /// multi-node CLUSTER projection. Validated in [`Config::validate`] via
    /// `ironcache_cluster::SlotMap::build` when `cluster_enabled` is set.
    pub cluster_topology: Option<ClusterTopology>,
    /// THIS node's announce id, used to find self in [`cluster_topology`](Self::cluster_topology).
    /// REQUIRED when a topology is set (the bind address may be `0.0.0.0`, so a node cannot
    /// be matched to its topology entry by address; the id is explicit). TOML + the
    /// `IRONCACHE_CLUSTER_ANNOUNCE_ID` env var (a single scalar, useful for per-pod identity
    /// injection). In cluster-map mode the node id IS this announce id (stable across boots),
    /// so `CLUSTER MYID` / `CLUSTER NODES` self-line / MOVED-target identity all agree.
    pub cluster_announce_id: Option<String>,
    /// HOW the slot-ownership map is governed (HA-4c). Only meaningful when `cluster_enabled`.
    /// Defaults to [`ClusterMode::Static`] (the pre-HA-4c behavior, byte-unchanged);
    /// [`ClusterMode::Raft`] opts the node into the Raft-governed control plane. TOML
    /// (`cluster_mode = "raft"`) + the `IRONCACHE_CLUSTER_MODE` env var.
    pub cluster_mode: ClusterMode,
    /// Whether this node is JOINING an already-formed Raft cluster at runtime (HA-prod-membership),
    /// rather than being one of the cluster's initial (boot-time) voters. When `true` (and
    /// `cluster_mode = raft`), the node boots as a NON-VOTER: it does not campaign and is not in the
    /// initial voter set; it learns it is a member only when the leader's committed `AddLearner`
    /// (then auto-promote `PromoteLearner`) entry replicates to it after an operator `CLUSTER MEET`.
    /// Defaults to `false` (a normal boot voter), so the established boot path is byte-unchanged.
    pub cluster_raft_joining: bool,
    /// The replication-lag bound, in LOGICAL WRITES, that gates BOTH (a) HA-8 promotion
    /// eligibility (a replica is promotable only when its link is up AND its lag is
    /// `<= replica_max_lag`, ADR-0026, so a stale replica is never promoted -> no data
    /// loss beyond the async-replication window) and (b) the HA-8 replica-read staleness
    /// bound (a READONLY read is served by a replica only while within this lag bound;
    /// past it the read returns MOVED to the owner). Meaningful ONLY in raft-governance
    /// mode (the default static path never reads it). Defaults to
    /// [`DEFAULT_REPLICA_MAX_LAG`].
    pub replica_max_lag: u64,
    /// How long (in SECONDS) a replica's link to its master must be CONTINUOUSLY DOWN
    /// before the replica PROPOSES its own promotion (HA-8 failover detection). A larger
    /// value tolerates longer transient master blips before failing over; a smaller one
    /// fails over faster (at the cost of more spurious-but-safe promotions). Meaningful
    /// ONLY in raft-governance mode. Defaults to [`DEFAULT_FAILOVER_TIMEOUT_SECS`].
    pub failover_timeout_secs: u64,
    /// The WRITE-SIDE replication guardrail (Redis `min-replicas-to-write`, ADR-0026): the
    /// minimum number of IN-SYNC replicas an owner must have before it ACCEPTS a write to a
    /// slot it owns. When the count of in-sync replicas drops below this, the owner REJECTS the
    /// write with `-NOREPLICAS Not enough good replicas to write.`, so an ACKNOWLEDGED write is
    /// known to be on at least this many replicas -- bounding how many writes a failover can lose
    /// to the async-replication window (the READ side is already bounded by [`replica_max_lag`]).
    ///
    /// DEFAULT 0 = DISABLED (the Redis default): the write hot path is BYTE-UNCHANGED -- the
    /// guardrail is a single `> 0` short-circuit that never reads the in-sync count. Meaningful
    /// ONLY in raft-governance mode (the default static path never reads it). TOML
    /// (`min_replicas_to_write = N`) + the `IRONCACHE_MIN_REPLICAS_TO_WRITE` env var.
    pub min_replicas_to_write: u32,
    /// The lag bound (in LOGICAL WRITES) for the [`min_replicas_to_write`] guardrail: a replica
    /// counts toward the in-sync quorum ONLY while its link is up AND its lag is `<= this` (the
    /// same offset-lag semantics as [`replica_max_lag`], which Redis expresses in seconds as
    /// `min-replicas-max-lag`). Meaningful ONLY when `min_replicas_to_write > 0` (and in
    /// raft-governance mode). Defaults to [`DEFAULT_MIN_REPLICAS_MAX_LAG`]. TOML
    /// (`min_replicas_max_lag = N`) + the `IRONCACHE_MIN_REPLICAS_MAX_LAG` env var.
    pub min_replicas_max_lag: u64,
    /// The HA-3c Raft-log compaction threshold: once the number of committed-and-applied
    /// log entries ABOVE the last snapshot exceeds this, the raft control plane snapshots
    /// its state machine and compacts the log to there (Raft section 7), bounding the
    /// on-disk log + the post-restart replay. Passed straight into
    /// `RaftConfig.snapshot_threshold`. Defaults to [`DEFAULT_RAFT_SNAPSHOT_THRESHOLD`]
    /// (1024, NON-ZERO) so a real deployment actually compacts; `0` DISABLES compaction
    /// (the log grows unbounded, the pre-3c behaviour). Meaningful ONLY in raft-governance
    /// mode (the default static path never builds a `RaftConfig`). TOML
    /// (`raft_snapshot_threshold = N`) + the `IRONCACHE_RAFT_SNAPSHOT_THRESHOLD` env var.
    pub raft_snapshot_threshold: u64,
    /// The PROD-9 chunked-InstallSnapshot chunk size (in BYTES): when a leader catches up a
    /// lagging follower by shipping its Raft snapshot, it sends the snapshot in bounded
    /// SEQUENTIAL chunks of at most this many bytes (Raft Figure 13) instead of one giant
    /// frame, so no install frame approaches the cluster-bus max-frame length and neither end
    /// spikes memory. Passed straight into `RaftConfig.snapshot_chunk_bytes`. Defaults to
    /// [`DEFAULT_RAFT_SNAPSHOT_CHUNK_BYTES`] (256 KiB), comfortably under the bus frame bound;
    /// it is a pure framing parameter, so any value installs a byte-identical snapshot. `0` is
    /// treated as "the whole snapshot in one chunk" (no zero-length-chunk loop). Meaningful
    /// ONLY in raft-governance mode. TOML (`raft_snapshot_chunk_bytes = N`) + the
    /// `IRONCACHE_RAFT_SNAPSHOT_CHUNK_BYTES` env var.
    pub raft_snapshot_chunk_bytes: usize,
    /// The HA-7e DISK-BACKED replication backlog size, in BYTES: the bound on the on-disk spill of
    /// the per-shard replication tail. When a replica falls behind the IN-MEMORY backlog ring, a
    /// non-zero value lets it catch up INCREMENTALLY from disk (a wider resume window) instead of
    /// forcing a full snapshot re-sync; the oldest on-disk segments are evicted past this bound.
    ///
    /// DEFAULT 0 = DISABLED: the replication path is BYTE-IDENTICAL to the in-memory-only backlog
    /// (nothing is spilled, the hot path is unchanged). REQUIRES a [`Self::data_dir`] (the spill
    /// lives under `<data_dir>/repl-backlog`); with no `data_dir` the knob is inert even if set.
    /// Meaningful ONLY in raft-governance mode (the default static path runs no replication tail).
    /// TOML (`repl_backlog_disk_bytes = 268435456`) + the `IRONCACHE_REPL_BACKLOG_DISK_BYTES` env var.
    pub repl_backlog_disk_bytes: u64,
    /// The durable data directory for the Raft log (and future on-disk state). When set, a
    /// raft-mode node persists its committed Raft log at `<data_dir>/ironcache-raft-<bus-port>.log`,
    /// so the control-plane state survives a reboot that clears the OS temp dir. When `None`
    /// (the default) the log lives under [`std::env::temp_dir`], the pre-existing behavior
    /// (byte-unchanged), which is fine for tests and ephemeral nodes but is NOT durable across a
    /// `/tmp`-clearing reboot. TOML (`data_dir = "/var/lib/ironcache"`) + the `IRONCACHE_DATA_DIR`
    /// env var.
    ///
    /// PERSISTENCE (#58): `data_dir` is ALSO the on-disk SNAPSHOT location and the SINGLE enable
    /// switch for durable persistence. When set, the serve layer LOADS any committed snapshot at
    /// boot and WRITES `<data_dir>/dump-shard-<n>.icss` + `<data_dir>/dump.manifest` on `SAVE` /
    /// `BGSAVE` (and on the periodic save policy below). When `None` (the default) persistence is
    /// OFF: no snapshot is loaded or written, boot starts empty, and the hot write path is
    /// byte-unchanged. (It is read by the PERSISTENCE path regardless of cluster mode; the Raft
    /// log use of it is the separate raft-governance concern above.)
    pub data_dir: Option<PathBuf>,
    /// The ACL FILE path (#106, Redis `aclfile`). When set, the server LOADS the `user <name>
    /// <rules>...` lines from it at boot (so ACL users survive a restart) and `ACL SAVE`
    /// writes the live registry back to it. `None` (the default) means NO aclfile: the ACL
    /// registry is the single all-permissive `default` user (plus any `requirepass`), so the
    /// default deployment is byte-identical and `ACL SAVE`/`LOAD` report "no aclfile
    /// configured". TOML (`aclfile = "/etc/ironcache/users.acl"`) + the `IRONCACHE_ACLFILE`
    /// env var. The file holds passwords ONLY as `#<sha256-hex>` digests, never plaintext.
    pub aclfile: Option<PathBuf>,
    /// The embedded transport-TLS posture for the CLIENT listener (#105, docs/design/TLS.md).
    /// Defaults to [`TlsMode::Off`] (plaintext, byte-unchanged). [`TlsMode::On`] makes the
    /// client port TLS-only and REQUIRES [`Self::tls_cert_path`] + [`Self::tls_key_path`]. TOML
    /// (`tls = "on"`) + the `IRONCACHE_TLS` env var.
    pub tls: TlsMode,
    /// Path to the PEM certificate CHAIN (leaf first, then any intermediates) the TLS listener
    /// presents (#105). REQUIRED + readable when [`Self::tls`] is [`TlsMode::On`]; ignored when
    /// off. TOML (`tls_cert_path = "..."`) + the `IRONCACHE_TLS_CERT_PATH` env var.
    pub tls_cert_path: Option<PathBuf>,
    /// Path to the PEM PRIVATE KEY (PKCS#8 / RSA / SEC1) matching [`Self::tls_cert_path`] (#105).
    /// REQUIRED + readable when [`Self::tls`] is [`TlsMode::On`]; ignored when off. TOML
    /// (`tls_key_path = "..."`) + the `IRONCACHE_TLS_KEY_PATH` env var.
    pub tls_key_path: Option<PathBuf>,
    /// WHICH async I/O backend the per-shard runtime uses (PROD-10 / #28). Defaults to
    /// [`RuntimeBackend::Tokio`] (the portable epoll/kqueue backend, byte-unchanged on every
    /// platform). [`RuntimeBackend::IoUring`] is honored ONLY on a Linux build with the
    /// `io_uring` Cargo feature AND TLS off; otherwise the boot falls back to `Tokio`. TOML
    /// (`runtime = "io_uring"`) + the `IRONCACHE_RUNTIME` env var + the `--runtime` CLI flag.
    pub runtime: RuntimeBackend,
    /// The PERIODIC SAVE INTERVAL in SECONDS (#58 persistence save policy): when non-zero AND a
    /// [`Self::data_dir`] is set, the server triggers a background save every `save_interval_secs`
    /// seconds PROVIDED at least [`Self::save_min_changes`] keyspace writes have happened since the
    /// last save (the Redis `save <seconds> <changes>` cadence, expressed as two scalars). `0`
    /// (the DEFAULT) DISABLES the periodic save: only an explicit `SAVE` / `BGSAVE` persists, so
    /// the default posture adds no background timer. TOML (`save_interval_secs = 900`) + the
    /// `IRONCACHE_SAVE_INTERVAL_SECS` env var.
    pub save_interval_secs: u64,
    /// The MINIMUM keyspace writes (since the last save) the periodic save policy requires before
    /// it fires (#58, the `changes` half of Redis `save <seconds> <changes>`). With the default
    /// `0` an enabled interval saves unconditionally on each tick; a non-zero value skips a tick
    /// when fewer than this many writes happened (avoiding a needless save of an idle keyspace).
    /// Meaningful only when [`Self::save_interval_secs`] is non-zero. TOML (`save_min_changes = 1`)
    /// + the `IRONCACHE_SAVE_MIN_CHANGES` env var.
    pub save_min_changes: u64,
    /// The DEDICATED PERSIST CORE knob (#589, the durable-snapshot tail residual lever). Selects which
    /// CPU core(s) the off-datapath `ic-persist-<shard>` thread (#588 per-slot Arc-COW) pins to, so its
    /// encode+fsync stops competing for a pinned datapath serving core during a save. Values (parsed
    /// by [`crate::parse_persist_cpu`], validated at boot): `""`/`off` (the DEFAULT: no pin, today's
    /// float behavior), `auto` (reserve the HIGHEST core of the process cpuset), or an explicit cpu
    /// list -- a single id (`"8"`), a range (`"6-7"`), or a mix (`"6-7,10"`). SCHEDULING-ONLY: it
    /// changes only which core a thread runs on, never a stored value or ordering, so ADR-0003
    /// determinism is untouched. A no-op on non-Linux (CPU affinity is a Linux primitive). TOML
    /// (`persist_cpu = "8"`) + the `IRONCACHE_PERSIST_CPU` env var + the `--persist-cpu` CLI flag.
    pub persist_cpu: String,
    /// The SLOWLOG threshold in MICROSECONDS (Redis `slowlog-log-slower-than`, PROD-7). `-1`
    /// DISABLES the SLOWLOG (the per-command hook short-circuits on a single atomic load); `0` logs
    /// every command. Default [`DEFAULT_SLOWLOG_LOG_SLOWER_THAN`] (10ms, the Redis default).
    /// Runtime-settable via `CONFIG SET slowlog-log-slower-than`.
    pub slowlog_log_slower_than: i64,
    /// The SLOWLOG max length (Redis `slowlog-max-len`, PROD-7): the maximum retained entries (the
    /// ring drops the oldest past it). Default [`DEFAULT_SLOWLOG_MAX_LEN`] (128). Runtime-settable
    /// via `CONFIG SET slowlog-max-len`.
    pub slowlog_max_len: u64,
    /// The `notify-keyspace-events` FLAG STRING (PROD-8, keyspace notifications). EMPTY by default
    /// (notifications DISABLED, the Redis default + a byte-identical hot path). When non-empty it
    /// selects the channels (`K` keyspace / `E` keyevent) + the event classes (`g$lshzxet...`, `A`
    /// = `g$lshzxet`) that publish to `__keyspace@db__:<key>` / `__keyevent@db__:<event>`. Parsed
    /// at boot (an invalid flag char fails validation) and runtime-settable via
    /// `CONFIG SET notify-keyspace-events`. TOML (`notify_keyspace_events = "KEA"`) + the
    /// `IRONCACHE_NOTIFY_KEYSPACE_EVENTS` env var.
    pub notify_keyspace_events: String,
    /// The INTRA-CLUSTER transport-TLS posture (PROD-3): whether the node-to-node links (the Raft
    /// cluster-bus `RAFTMSG` control plane AND the replication stream) are TLS-encrypted. Defaults
    /// to [`TlsMode::Off`] (PLAINTEXT, byte-unchanged: no handshake, no secret check, exactly the
    /// pre-PROD-3 wire). [`TlsMode::On`] makes BOTH the bus + repl listeners perform a rustls SERVER
    /// handshake on accept and the dials perform a rustls CLIENT handshake, REQUIRING
    /// [`Self::cluster_tls_cert_path`] + [`Self::cluster_tls_key_path`]. SEPARATE from [`Self::tls`]
    /// (the public CLIENT listener): a deployment can run a public TLS client port AND an internal
    /// TLS cluster bus independently. TOML (`cluster_tls = "on"`) + the `IRONCACHE_CLUSTER_TLS` env
    /// var. Meaningful only in raft-governance / replication deployments (a standalone node has no
    /// peer links), but the knob is general.
    pub cluster_tls: TlsMode,
    /// Path to the PEM certificate CHAIN the intra-cluster TLS listener (bus + repl) presents
    /// (PROD-3). REQUIRED + readable when [`Self::cluster_tls`] is [`TlsMode::On`]; ignored when off.
    /// May reuse the public [`Self::tls_cert_path`] cert or be a dedicated cluster cert. TOML
    /// (`cluster_tls_cert_path = "..."`) + the `IRONCACHE_CLUSTER_TLS_CERT_PATH` env var.
    pub cluster_tls_cert_path: Option<PathBuf>,
    /// Path to the PEM PRIVATE KEY (PKCS#8 / RSA / SEC1) matching [`Self::cluster_tls_cert_path`]
    /// (PROD-3). REQUIRED + readable when [`Self::cluster_tls`] is [`TlsMode::On`]; ignored when off.
    /// TOML (`cluster_tls_key_path = "..."`) + the `IRONCACHE_CLUSTER_TLS_KEY_PATH` env var.
    pub cluster_tls_key_path: Option<PathBuf>,
    /// OPTIONAL path to the PEM cluster CA the intra-cluster DIAL verifies the PEER cert against
    /// (PROD-3, the stronger posture). When set, a dialed peer presenting a cert NOT signed by this
    /// CA fails the TLS handshake (standard webpki verification). When `None` (the default), the
    /// dial uses an accept-the-cert verifier (the link is still ENCRYPTED) and peer AUTHENTICATION
    /// relies on the [`Self::cluster_secret`] shared-secret handshake -- the pragmatic v1 for a
    /// self-signed cluster cert. mTLS (client-cert verification) is the documented stronger
    /// follow-up. TOML (`cluster_ca_path = "..."`) + the `IRONCACHE_CLUSTER_CA_PATH` env var.
    pub cluster_ca_path: Option<PathBuf>,
    /// The SHARED CLUSTER SECRET (PROD-3): a token every node holds and presents in a constant-time
    /// handshake right after the TLS handshake (or right after the TCP connect when only a secret is
    /// configured), on BOTH the bus and the repl link. A peer that does not present the correct
    /// secret is DROPPED, so an attacker who reaches the port cannot join the bus, forge `RAFTMSG`,
    /// or pull the replication stream even if they complete a TLS handshake. REQUIRED when
    /// [`Self::cluster_tls`] is on; MAY also be set WITHOUT TLS to add peer authentication to a
    /// plaintext bus (the secret travels in cleartext then, so TLS+secret is the recommended pairing
    /// -- documented). `None` (the default) means NO secret check (the pre-PROD-3 behavior). TOML
    /// (`cluster_secret = "..."`) + the `IRONCACHE_CLUSTER_SECRET` env var. Held in cleartext in the
    /// long-lived `Config` (it is compared against the peer's, not stored hashed).
    pub cluster_secret: Option<String>,
    /// EXPLICIT, NOT-RECOMMENDED opt-in to run the intra-cluster TLS dial WITHOUT verifying the peer
    /// certificate (PROD-3 security fix). When `cluster_tls = on`, the dial verifies the peer cert
    /// against [`Self::cluster_ca_path`] by default, which DEFEATS an active man-in-the-middle: an
    /// attacker presenting its own cert is not signed by the cluster CA, so the TLS handshake fails
    /// BEFORE the [`Self::cluster_secret`] is ever sent. Without a CA the secret would be exposed to
    /// a MITM, so a CA is REQUIRED when TLS is on -- UNLESS this flag is `true`, which installs an
    /// accept-any-cert verifier (the link is encrypted but UNAUTHENTICATED at the TLS layer, so an
    /// active MITM can capture the `cluster_secret`). When `true` the server logs a LOUD boot-time
    /// warning. A shared self-signed cert pointed at by BOTH `cluster_tls_cert_path` AND
    /// `cluster_ca_path` verifies against itself, so this flag is NOT needed for the simple
    /// no-PKI-but-secure deployment -- prefer that. Defaults to `false`. TOML
    /// (`cluster_tls_insecure_skip_verify = true`) + the `IRONCACHE_CLUSTER_TLS_INSECURE_SKIP_VERIFY`
    /// env var.
    pub cluster_tls_insecure_skip_verify: bool,
    /// FAIL CLOSED when the on-disk snapshot ([`Self::data_dir`]) has a format version this binary
    /// CANNOT read (#530). By default (`false`) a version-mismatched dump -- almost always a NEWER
    /// dump an OLDER binary is asked to load (a downgrade / a failed-upgrade rollback) -- is NOT
    /// loaded and the node starts with an EMPTY keyspace, but the mismatch is now surfaced with a
    /// LOUD boot-time `tracing::error!` (it is never silent). With this `true` the node REFUSES TO
    /// BOOT on such a dump instead, so an operator who would rather halt than silently serve an empty
    /// keyspace (and risk overwriting the newer dump on the next save) can fail closed. Inert with no
    /// `data_dir` or when the dump is loadable / genuinely absent. TOML
    /// (`refuse_empty_start_on_version_mismatch = true`) + the
    /// `IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH` env var.
    pub refuse_empty_start_on_version_mismatch: bool,
}

impl Default for Config {
    fn default() -> Self {
        // Built-in safe defaults (the lowest-precedence layer; CONFIG.md /
        // ADR-0007 cache-mode posture). Bind to loopback by default so an
        // unconfigured boot is not exposed on all interfaces.
        Config {
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: DEFAULT_PORT,
            // Default to available parallelism via std (which honors cgroup CPU
            // quotas, unlike num_cpus). Never zero. Mirrors
            // `ironcache_runtime::available_shards` without taking a dep on the
            // runtime crate (and thus tokio) here.
            shards: default_shards(),
            databases: 16,
            // The store's per-slot partition default (#570): a modest count that bounds the
            // resize unit to ~N/256 while keeping the perf-gate bytes-per-key green. Mirrors
            // ironcache-store's DEFAULT_SLOTS_PER_DB.
            slots_per_db: 256,
            default_resp3: false,
            maxmemory: 0,
            // ADR-0007: cache mode default is eviction-ON with a Redis-recognized
            // name, NOT noeviction. allkeys-lru is the typical cache default; the
            // FIFO-class engine (ADR-0008) serves it.
            maxmemory_policy: "allkeys-lru".to_owned(),
            requirepass: None,
            timeout_secs: 0,
            // The Redis `maxclients` default (10000): an unconfigured node is protected
            // from connection exhaustion without an operator opting in. A huge value (or
            // 0) effectively disables the cap; the default still leaves vast headroom for
            // a normal workload. (PROD-SAFETY #3.)
            maxclients: DEFAULT_MAXCLIENTS,
            // A high per-connection output-buffer ceiling by default (PROD-SAFETY #5): a
            // legitimate large reply / deep pipeline is unaffected, but a pathological
            // unbounded accumulation (slow consumer / pub-sub flood) is bounded so it
            // cannot OOM the host. 0 disables it (unbounded, the pre-fix behavior).
            output_buffer_limit: DEFAULT_OUTPUT_BUFFER_LIMIT,
            // A high per-connection query-buffer ceiling by default (#528): a legitimate large
            // request / deep pipeline is unaffected, but a slow-dribble multibulk that never
            // completes cannot force unbounded inbound buffering (a pre-auth memory-amplification
            // DoS). 0 disables it (unbounded, the pre-fix behavior).
            query_buffer_limit: DEFAULT_QUERY_BUFFER_LIMIT,
            // The Redis `proto-max-bulk-len` default (512 MB): the inbound bulk-string ceiling and
            // the string-value growth ceiling. Runtime-settable; the default keeps the decoder
            // Limits + the string/bitmap ceilings byte-identical to the pre-fix compiled constant.
            proto_max_bulk_len: DEFAULT_PROTO_MAX_BULK_LEN,
            // The Redis `tcp-keepalive` default (300 s): SO_KEEPALIVE idle interval on accepted
            // connections so a dead peer is reaped. Runtime-settable; `0` disables keepalive.
            tcp_keepalive_secs: DEFAULT_TCP_KEEPALIVE,
            // The 8 collection-encoding thresholds, seeded from the compiled Redis defaults so the
            // default deployment's encoding transitions are byte-identical. Runtime-settable via
            // `CONFIG SET`; a change affects FUTURE inserts only (existing keys keep their encoding).
            hash_max_listpack_entries: DEFAULT_HASH_MAX_LISTPACK_ENTRIES,
            hash_max_listpack_value: DEFAULT_HASH_MAX_LISTPACK_VALUE,
            list_max_listpack_size: DEFAULT_LIST_MAX_LISTPACK_SIZE,
            set_max_intset_entries: DEFAULT_SET_MAX_INTSET_ENTRIES,
            set_max_listpack_entries: DEFAULT_SET_MAX_LISTPACK_ENTRIES,
            set_max_listpack_value: DEFAULT_SET_MAX_LISTPACK_VALUE,
            zset_max_listpack_entries: DEFAULT_ZSET_MAX_LISTPACK_ENTRIES,
            zset_max_listpack_value: DEFAULT_ZSET_MAX_LISTPACK_VALUE,
            // Standalone by default (Redis `cluster-enabled no`). Slice 1 is
            // cluster-disabled-but-introspectable (CLUSTER_CONTRACT.md #70).
            cluster_enabled: false,
            // No static topology by default: an unconfigured (or cluster-enabled but
            // topology-less) boot stays single-node-owns-all (slice-1 behavior). A topology
            // is opt-in (CLUSTER_CONTRACT.md #70, slice 2).
            cluster_topology: None,
            cluster_announce_id: None,
            cluster_raft_joining: false,
            // The slot map is STATICALLY governed by default (HA-4c): the pre-HA-4c
            // behavior, byte-unchanged. Raft governance is strictly opt-in.
            cluster_mode: ClusterMode::Static,
            // HA-8 defaults; meaningful only in raft-governance mode (the static path
            // never reads them, so they do not perturb the default posture).
            replica_max_lag: DEFAULT_REPLICA_MAX_LAG,
            failover_timeout_secs: DEFAULT_FAILOVER_TIMEOUT_SECS,
            // The WRITE-SIDE guardrail is DISABLED by default (min_replicas_to_write = 0, the
            // Redis default): the write hot path is byte-unchanged. The lag bound carries a sane
            // default but is only read when the guardrail is enabled. Meaningful only in raft-mode.
            min_replicas_to_write: 0,
            min_replicas_max_lag: DEFAULT_MIN_REPLICAS_MAX_LAG,
            // A NON-ZERO production compaction cadence (the engine's own default is 0 =
            // disabled, to keep the determinism sweep byte-identical); a real raft-mode
            // node compacts once its log grows this far past the last snapshot.
            raft_snapshot_threshold: DEFAULT_RAFT_SNAPSHOT_THRESHOLD,
            raft_snapshot_chunk_bytes: DEFAULT_RAFT_SNAPSHOT_CHUNK_BYTES,
            // HA-7e disk-backed replication backlog: DISABLED by default (0), so replication is
            // byte-identical to the in-memory-only backlog -- nothing spills to disk. A non-zero
            // value (with a data_dir) widens the incremental-resync window. Opt-in.
            repl_backlog_disk_bytes: 0,
            // No data directory by default: the Raft log lands under the OS temp dir
            // (byte-unchanged pre-existing behavior). Setting it makes the log durable
            // across a reboot that clears /tmp. Meaningful only in raft-governance mode.
            data_dir: None,
            // No aclfile by default (#106): the ACL registry is the single all-permissive
            // `default` user (plus any requirepass), so the default deployment is byte-identical.
            // Setting it loads users at boot and makes ACL SAVE persistent.
            aclfile: None,
            // TLS is OFF by default (#105): the client listener is plaintext and byte-unchanged.
            // No cert/key is loaded and the rustls layer is never touched. Turning it on is opt-in
            // and requires a cert + key.
            tls: TlsMode::Off,
            tls_cert_path: None,
            tls_key_path: None,
            // The runtime backend is the portable TOKIO epoll/kqueue path by default (PROD-10):
            // byte-unchanged on every platform, the only path in the default (no-feature) build.
            // `io_uring` is opt-in and Linux/feature-gated (and falls back here otherwise).
            runtime: RuntimeBackend::Tokio,
            // The PERIODIC SAVE policy is OFF by default (#58): 0 = no background save timer, so
            // the default posture is unchanged (only an explicit SAVE/BGSAVE persists, and only
            // when a data_dir is set). A non-zero interval + a data_dir enables the cadence.
            save_interval_secs: 0,
            save_min_changes: 0,
            // The dedicated persist core (#589) is OFF by default: the empty value means no CPU pin,
            // so the `ic-persist` thread floats exactly as it does today (byte-unchanged, the safe
            // default per the tunability tenet). An operator opts into `auto` or an explicit core.
            persist_cpu: String::new(),
            slowlog_log_slower_than: DEFAULT_SLOWLOG_LOG_SLOWER_THAN,
            slowlog_max_len: DEFAULT_SLOWLOG_MAX_LEN,
            // Keyspace notifications are OFF by default (PROD-8, the Redis default): the empty flag
            // string disables them and keeps the write hot path byte-identical.
            notify_keyspace_events: String::new(),
            // INTRA-CLUSTER transport security is OFF by default (PROD-3): the bus + repl links are
            // plaintext and byte-unchanged (no handshake, no secret check). Turning it on is opt-in
            // and requires a cert + key (+ secret). The CA + secret default to None.
            cluster_tls: TlsMode::Off,
            cluster_tls_cert_path: None,
            cluster_tls_key_path: None,
            cluster_ca_path: None,
            cluster_secret: None,
            // Peer-cert verification is ON by default when cluster TLS is on (a CA is required, so
            // the secret is never exposed to a MITM). This insecure escape hatch is opt-in only.
            cluster_tls_insecure_skip_verify: false,
            // A format-version-mismatched snapshot is NOT loaded (start empty) but is surfaced with a
            // LOUD boot error (#530); refusing to boot on it is the opt-in fail-closed posture, OFF by
            // default so a mismatch degrades to a loud empty start rather than a boot failure.
            refuse_empty_start_on_version_mismatch: false,
        }
    }
}

/// Default `maxclients` (PROD-SAFETY #3): the simultaneous-connection ceiling, matching
/// Redis's 10000 default. A new connection accepted at this cap is rejected with
/// `-ERR max number of clients reached`, bounding the connection-exhaustion DoS. The
/// default protects an unconfigured node while leaving ample headroom for a normal
/// workload; `0` disables the cap (unlimited, the pre-fix behavior).
pub const DEFAULT_MAXCLIENTS: u64 = 10_000;

/// Default per-connection output-buffer hard cap in bytes (PROD-SAFETY #5): 1 GiB. A
/// connection whose pending unflushed reply would exceed this is closed, bounding a
/// slow-consumer / huge-reply / pub-sub-flood server-memory DoS. The default is high
/// enough that a legitimate large reply or deep pipeline (a bulk-string value is itself
/// capped at the 512 MB `proto-max-bulk-len`) is never affected, while a pathological
/// unbounded accumulation is bounded; `0` disables the cap (unbounded, the pre-fix
/// behavior).
pub const DEFAULT_OUTPUT_BUFFER_LIMIT: u64 = 1024 * 1024 * 1024;

/// Default per-connection query-buffer hard cap in bytes (#528): 1 GiB. A connection whose
/// accumulated inbound read buffer would exceed this is closed, bounding the slow-dribble
/// multibulk memory-amplification DoS (a client announces a large `*<huge>\r\n` array and then
/// trickles the elements, forcing the server to buffer every partial byte pre-auth while the
/// frame never completes). The default is high enough that a legitimate large request or deep
/// pipeline (an individual bulk string is itself capped at the 512 MB `proto-max-bulk-len`) is
/// never affected, while a pathological accumulation is bounded; `0` disables the cap (unbounded,
/// the pre-fix behavior). Mirrors [`DEFAULT_OUTPUT_BUFFER_LIMIT`] on the inbound side.
pub const DEFAULT_QUERY_BUFFER_LIMIT: u64 = 1024 * 1024 * 1024;

/// Default `slowlog-log-slower-than` threshold in MICROSECONDS (Redis default 10000us = 10ms,
/// PROD-7). A command taking at least this long is recorded in the SLOWLOG. `-1` disables the
/// SLOWLOG entirely; `0` logs every command.
pub const DEFAULT_SLOWLOG_LOG_SLOWER_THAN: i64 = 10_000;

/// Default `slowlog-max-len` (Redis default 128, PROD-7): the maximum SLOWLOG entries retained.
pub const DEFAULT_SLOWLOG_MAX_LEN: u64 = 128;

/// Default `save-backpressure-percent` (#577, the concurrent-snapshot p99.9 stopgap). `100` means
/// NO throttle: a `SAVE`/`BGSAVE` dumps at full speed (byte-identical to the pre-#577 behavior), so
/// the default deployment is unchanged. A value in `1..=100` makes the per-shard save loop sleep
/// proportionally after each dump chunk so the save consumes only about that percent of the serving
/// core, keeping the datapath throughput above the offered load (the open-loop queue drains instead
/// of building). Runtime-settable via `CONFIG SET save-backpressure-percent`.
pub const DEFAULT_SAVE_BACKPRESSURE_PERCENT: u64 = 100;

/// Default HA-8 replication-lag bound (logical writes) for promotion eligibility + the
/// replica-read staleness gate. A modest window: a replica more than this many writes
/// behind is neither promotable nor allowed to serve a (stale) READONLY read.
pub const DEFAULT_REPLICA_MAX_LAG: u64 = 256;

/// Default HA-8 failover timeout (seconds): how long a replica's master link must be
/// continuously down before the replica proposes its own promotion. Comfortably above
/// the replication poll cadence so a single missed poll does not trigger a failover.
pub const DEFAULT_FAILOVER_TIMEOUT_SECS: u64 = 5;

/// Default lag bound (logical writes) for the `min-replicas-to-write` guardrail's in-sync
/// quorum: a replica counts toward the write-side quorum only while within this many writes of
/// the master. A modest window (10) consistent with Redis's small `min-replicas-max-lag` default
/// (10 seconds): a replica more than this far behind is not counted, so an accepted write is on
/// at least `min_replicas_to_write` replicas that were recently current. Only read when the
/// guardrail is enabled (`min_replicas_to_write > 0`), so the default path never touches it.
pub const DEFAULT_MIN_REPLICAS_MAX_LAG: u64 = 10;

/// Default HA-3c Raft-log compaction threshold (entries above the last snapshot). A
/// PRODUCTION default of 1024: a real raft-mode deployment compacts its log once it grows
/// this far past the last snapshot, bounding the on-disk log and the replay time after a
/// restart -- the whole point of HA-3c. NON-ZERO so a real node actually compacts, unlike
/// the pure-engine default ([`ironcache_raft::DEFAULT_SNAPSHOT_THRESHOLD`] = 0, which keeps
/// the determinism sweep + every direct-`RaftConfig` test byte-identical). The config
/// snapshot is tiny (the committed `SlotMap` state), so 1024 entries between snapshots is a
/// comfortable, cheap cadence. Only the raft-governance boot path reads this; the default
/// static path never does.
pub const DEFAULT_RAFT_SNAPSHOT_THRESHOLD: u64 = 1024;

/// The default PROD-9 chunked-InstallSnapshot chunk size in BYTES (256 KiB). Mirrors the
/// pure engine's [`ironcache_raft::DEFAULT_SNAPSHOT_CHUNK_BYTES`]: comfortably under the
/// cluster-bus max-frame length (`ironcache_runtime::MAX_CLUSTER_FRAME_LEN`, 512 MiB) so a
/// large snapshot is shipped in many small frames rather than one giant one, while large
/// enough that a typical config snapshot is one or two chunks. A pure framing knob -- the
/// installed snapshot is byte-identical at any value. Only the raft-governance boot path
/// reads it.
pub const DEFAULT_RAFT_SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;

/// The eight Redis `maxmemory-policy` names IronCache accepts (EVICTION.md #50).
/// Inlined here (rather than depending on `ironcache-eviction`) to keep the config
/// crate dependency-light; this list is a stable Redis fact and is mirrored by
/// `ironcache_eviction::REDIS_POLICY_NAMES` (kept in lockstep). Validation is
/// case-insensitive.
pub const MAXMEMORY_POLICY_NAMES: [&str; 8] = [
    "noeviction",
    "allkeys-lru",
    "allkeys-lfu",
    "allkeys-random",
    "volatile-lru",
    "volatile-lfu",
    "volatile-random",
    "volatile-ttl",
];

impl Config {
    /// Resolve the effective config by folding `overlays` over the defaults,
    /// lowest-precedence first. The caller passes overlays in precedence order:
    /// `[toml, env, cli]` (later overrides earlier).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Size`] if any overlay carries a malformed or
    /// out-of-range `maxmemory` (e.g. `garbage`, `99999999999gb`, `1.5gb`). A bad
    /// ceiling HARD-FAILS here rather than silently resolving to 0 = unlimited,
    /// which would violate the honest-ceiling invariant (#3).
    pub fn resolve(overlays: &[ConfigOverlay]) -> Result<Config, ConfigError> {
        let mut cfg = Config::default();
        for o in overlays {
            o.apply_to(&mut cfg)?;
        }
        // SECURITY (#65): the overlays carry the requirepass PLAINTEXT (TOML/env/CLI
        // input), and `apply_to` left the resolved plaintext in `cfg.requirepass`. Hash
        // it AT THE END, after the full TOML < env < CLI layering has resolved the final
        // plaintext, so the long-lived `Config` holds ONLY the SHA-256 digest and no
        // plaintext password survives config load. An empty/unset password stays `None`.
        cfg.finalize_requirepass();
        Ok(cfg)
    }

    /// Replace the resolved `requirepass` PLAINTEXT with its SHA-256 hex digest in place
    /// (#65). Called once at the end of [`Config::resolve`], after the overlay fold has
    /// produced the final plaintext, so the long-lived `Config` never retains the
    /// plaintext. An empty string clears it to `None` (auth disabled); a `None` stays
    /// `None`. Idempotency is NOT assumed: this must run exactly once on the resolved
    /// plaintext (a second call would hash the hash). `resolve` is the single caller.
    fn finalize_requirepass(&mut self) {
        self.requirepass = match self.requirepass.take() {
            Some(plaintext) if !plaintext.is_empty() => Some(sha256_hex(plaintext.as_bytes())),
            // An explicit empty password disables auth, matching the runtime
            // `set_requirepass("")` semantics; an unset password is already `None`.
            _ => None,
        };
    }

    /// Validate cross-field invariants after resolution.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.shards == 0 {
            return Err(ConfigError::Invalid {
                field: "shards",
                reason: "must be at least 1".to_owned(),
            });
        }
        // HARD CEILING on the shard count (COORDINATOR.md #107, FIX 2): the cross-shard
        // SCAN wire cursor is COMPOSITE -- its high
        // [`ironcache_storage::ScanCursor::SHARD_BITS`] bits carry the shard index, so it
        // can address at most [`ironcache_storage::ScanCursor::MAX_SHARDS`] (256) shards.
        // Beyond that the shard index overflows its field and silently CORRUPTS the cursor
        // (shard 256 packs to the all-zero "done" sentinel; shard 257 aliases shard 1).
        // This is reachable in release (`default_shards()` on a >256-core host, or an
        // explicit `--shards 512`), so it is a LOUD boot error, NOT a debug_assert. The
        // limit lives in the storage waist so the cursor math and this guard share ONE
        // source of truth.
        let max_shards = ironcache_storage::ScanCursor::MAX_SHARDS;
        if self.shards > max_shards {
            return Err(ConfigError::Invalid {
                field: "shards",
                reason: format!(
                    "{} exceeds the maximum of {max_shards} (the cross-shard SCAN cursor \
                     reserves {} high bits for the shard index; more shards would corrupt \
                     the composite cursor)",
                    self.shards,
                    ironcache_storage::ScanCursor::SHARD_BITS,
                ),
            });
        }
        if self.databases == 0 {
            return Err(ConfigError::Invalid {
                field: "databases",
                reason: "must be at least 1".to_owned(),
            });
        }
        if self.slots_per_db == 0 {
            return Err(ConfigError::Invalid {
                field: "slots_per_db",
                reason: "must be at least 1".to_owned(),
            });
        }
        // maxmemory-policy must be one of the eight Redis names (case-insensitive),
        // EVICTION.md #50. An unknown name hard-fails boot rather than silently
        // falling back to a default (an operator typo must be visible).
        let policy_lower = self.maxmemory_policy.to_ascii_lowercase();
        if !MAXMEMORY_POLICY_NAMES.contains(&policy_lower.as_str()) {
            return Err(ConfigError::Invalid {
                field: "maxmemory-policy",
                reason: format!(
                    "'{}' is not a recognized policy (expected one of: {})",
                    self.maxmemory_policy,
                    MAXMEMORY_POLICY_NAMES.join(", ")
                ),
            });
        }
        // KEYSPACE NOTIFICATIONS (PROD-8). The `notify-keyspace-events` flag string must parse to a
        // valid flag set: an unrecognized flag character hard-fails boot (an operator typo must be
        // visible, matching how Redis rejects a bad `notify-keyspace-events`), rather than silently
        // dropping the bad character. The empty default parses to the disabled set (a no-op).
        if let Err(bad) = NotifyFlags::parse(&self.notify_keyspace_events) {
            return Err(ConfigError::Invalid {
                field: "notify-keyspace-events",
                reason: format!(
                    "'{bad}' is not a valid notify-keyspace-events flag \
                     (expected a subset of KEg$lshzxetdmnA)"
                ),
            });
        }
        // DATA DIRECTORY (the durable Raft-log location). When set it must not be empty: an
        // empty path would resolve the log relative to the process CWD, which is almost
        // certainly an operator mistake (it defeats the durability intent), so it hard-fails
        // boot rather than silently writing the log somewhere surprising. `None` (the default)
        // is fine: it keeps the OS-temp-dir behavior.
        if let Some(dir) = &self.data_dir {
            if dir.as_os_str().is_empty() {
                return Err(ConfigError::Invalid {
                    field: "data_dir",
                    reason: "must not be empty when set".to_owned(),
                });
            }
        }
        // DEDICATED PERSIST CORE (#589): parse the knob once on the resolved value so a malformed
        // `persist_cpu` (a bad range / non-numeric id) hard-fails boot rather than silently running
        // unpinned. The empty default parses to `Off` (no pin), so an unconfigured node is unaffected.
        crate::parse_persist_cpu(&self.persist_cpu).map_err(|reason| ConfigError::Invalid {
            field: "persist-cpu",
            reason,
        })?;
        // TRANSPORT TLS (#105, docs/design/TLS.md). A no-op when `tls = off` (the default), so the
        // plaintext path is byte-unchanged; otherwise it requires a readable cert + key. Factored
        // into a helper so `validate` stays within the line budget and the TLS pre-flight lives in
        // one place.
        self.validate_tls()?;
        // INTRA-CLUSTER TRANSPORT SECURITY (PROD-3). A no-op when `cluster_tls = off` AND no secret
        // is set (the default), so the plaintext bus + repl path is byte-unchanged; otherwise it
        // requires a readable cert + key (+ a secret) for a TLS cluster, or just a secret for a
        // plaintext-but-authenticated cluster. Factored into a helper so `validate` stays within the
        // line budget.
        self.validate_cluster_transport()?;
        // CLUSTER TOPOLOGY (CLUSTER_CONTRACT.md #70, slice 2). When cluster mode is enabled
        // AND a static topology is configured, it must be a COMPLETE, non-overlapping,
        // well-formed map that includes THIS node (by announce id). A topology is opt-in: a
        // cluster-enabled node with no topology stays single-node-owns-all (slice-1), so this
        // block only runs when both are present. The validation is delegated to
        // `ironcache_cluster::SlotMap::build` (the single slot-assignment validator), so the
        // router, the projection, and config all agree on what a valid map is.
        if self.cluster_enabled {
            if let Some(topo) = &self.cluster_topology {
                // announce-id is REQUIRED when a topology is set (bind may be 0.0.0.0, so self
                // cannot be matched by address). `field` is &'static str, so the dynamic
                // context lives in `reason`.
                let announce =
                    self.cluster_announce_id
                        .as_deref()
                        .ok_or_else(|| ConfigError::Invalid {
                            field: "cluster-announce-id",
                            reason:
                                "is required when cluster_topology is set (the bind address may \
                                 be 0.0.0.0, so this node cannot be matched to a topology entry \
                                 by address)"
                                    .to_owned(),
                        })?;
                match self.cluster_mode {
                    // STATIC governance: the topology IS the slot map, so it must be a COMPLETE,
                    // non-overlapping, self-inclusive static assignment (slice 2 rule).
                    ClusterMode::Static => validate_topology(topo, announce)?,
                    // RAFT governance (HA-4c): the topology supplies ONLY the voter set + the
                    // peer cluster-bus addresses; slot ownership is established at runtime through
                    // committed proposals (each node boots `empty_self` owning zero slots). So the
                    // slot RANGES are ignored and the completeness/overlap rules do NOT apply;
                    // only the node-IDENTITY rules (40-hex ids, no duplicates, self present) are
                    // enforced here so the voter set is well-formed.
                    ClusterMode::Raft => validate_raft_topology(topo, announce)?,
                    // SHARD-OWNERS (#517): a standalone node derives its slot owners from its OWN
                    // shard count, so a config `cluster_topology` is meaningless here -- reject it
                    // loudly rather than silently ignore a topology the operator declared.
                    ClusterMode::ShardOwners => {
                        return Err(ConfigError::Invalid {
                            field: "cluster-mode",
                            reason: "shard-owners mode derives slot owners from the shard count; \
                                     it does not take a cluster_topology (remove the topology)"
                                .to_owned(),
                        });
                    }
                }
            }
        }
        self.validate_shard_owners()?;
        Ok(())
    }

    /// Validate the SHARD-OWNERS (#517) mode rules. A NO-OP for every other `cluster_mode`.
    ///
    /// The mode requires `cluster_enabled` (it serves CLUSTER SLOTS + MOVED); it is mutually
    /// exclusive with raft governance (the enum enforces that: one `cluster_mode`). It binds one
    /// listener PER shard (`port + i`), which the tokio bootstrap does but the io_uring bootstrap
    /// does not yet, so the io_uring combo is rejected loudly rather than silently binding a single
    /// port whose `CLUSTER SLOTS` projection would advertise per-shard ports no one listens on. And
    /// the whole port block `port .. port + shards - 1` must fit u16 (checked here so the operator
    /// gets ONE clean config error instead of a boot panic from the projection build or a mid-bind
    /// io::Error from the bootstrap racing to fire first).
    fn validate_shard_owners(&self) -> Result<(), ConfigError> {
        if self.cluster_mode != ClusterMode::ShardOwners {
            return Ok(());
        }
        if !self.cluster_enabled {
            return Err(ConfigError::Invalid {
                field: "cluster-mode",
                reason: "shard-owners mode requires cluster_enabled = true (it exposes the node's \
                         shards as hashslot owners over the CLUSTER protocol)"
                    .to_owned(),
            });
        }
        if self.runtime == RuntimeBackend::IoUring {
            return Err(ConfigError::Invalid {
                field: "cluster-mode",
                reason: "shard-owners mode is not yet supported with the io_uring runtime (its \
                         per-shard listeners are a follow-up); use the tokio runtime for shard-owners"
                    .to_owned(),
            });
        }
        let top_offset = u16::try_from(self.shards.max(1) - 1).unwrap_or(u16::MAX);
        if self.port.checked_add(top_offset).is_none() {
            return Err(ConfigError::Invalid {
                field: "port",
                reason: format!(
                    "shard-owners mode needs ports {}..{} (one per shard), which overflows the \
                     maximum port 65535; lower the base port or the shard count",
                    self.port,
                    u32::from(self.port) + u32::from(top_offset)
                ),
            });
        }
        Ok(())
    }

    /// Validate the transport-TLS pre-flight (#105, docs/design/TLS.md). A NO-OP when `tls = off`
    /// (the default), so the plaintext path is byte-unchanged. When `tls = on` (the TLS-only
    /// client listener), a cert AND a key path are REQUIRED and both must be READABLE at boot: a
    /// TLS-only listener with no usable cert/key would reject every handshake, so this is a LOUD
    /// boot error (the operator sees it immediately) rather than a listener that silently refuses
    /// all clients. The actual PEM parse + rustls acceptance happens at boot in the runtime layer;
    /// here we only assert the paths are set and the files OPEN (a cheap, clear pre-flight: a
    /// typo'd path or an unreadable key fails here with a precise field).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] (`field = "tls_cert_path"` / `"tls_key_path"`) when TLS is
    /// on and a path is missing or unreadable.
    fn validate_tls(&self) -> Result<(), ConfigError> {
        if self.tls != TlsMode::On {
            return Ok(());
        }
        let cert = self
            .tls_cert_path
            .as_ref()
            .ok_or_else(|| ConfigError::Invalid {
                field: "tls_cert_path",
                reason: "is required when tls = on (the TLS listener needs a certificate chain)"
                    .to_owned(),
            })?;
        let key = self
            .tls_key_path
            .as_ref()
            .ok_or_else(|| ConfigError::Invalid {
                field: "tls_key_path",
                reason: "is required when tls = on (the TLS listener needs a private key)"
                    .to_owned(),
            })?;
        // Pre-flight readability: open each file so a bad path / permission problem is a clear boot
        // error here, not an opaque handshake failure later. `field` is &'static str, so the
        // offending path goes in `reason`.
        if let Err(e) = std::fs::File::open(cert) {
            return Err(ConfigError::Invalid {
                field: "tls_cert_path",
                reason: format!("cannot read certificate at {}: {e}", cert.display()),
            });
        }
        if let Err(e) = std::fs::File::open(key) {
            return Err(ConfigError::Invalid {
                field: "tls_key_path",
                reason: format!("cannot read private key at {}: {e}", key.display()),
            });
        }
        Ok(())
    }

    /// Validate the intra-cluster transport-security pre-flight (PROD-3). A NO-OP when
    /// `cluster_tls = off` AND no `cluster_secret` is set (the default), so the plaintext bus + repl
    /// path is byte-unchanged. The rules when configured:
    ///
    /// * `cluster_tls = on` REQUIRES a `cluster_tls_cert_path` + `cluster_tls_key_path` (both
    ///   readable at boot, like the public TLS knobs) AND a `cluster_secret`: a TLS cluster bus with
    ///   no peer secret would encrypt the link but NOT authenticate the peer (any party that can
    ///   complete a TLS handshake to a self-signed cluster cert could still join), which defeats the
    ///   forge-consensus / siphon-keyspace defense, so the secret is mandatory under TLS. A TLS
    ///   cluster with no usable cert/key would reject every handshake, so a missing/unreadable
    ///   cert/key is a LOUD boot error.
    /// * `cluster_tls = on` ALSO REQUIRES a `cluster_ca_path` (the CA/cert that signs peer certs) so
    ///   the dial VERIFIES the peer cert -- without it the dial would accept any cert and the
    ///   `cluster_secret` would be sent to an active man-in-the-middle (the MITM secret-capture this
    ///   fix closes). The one exception is the explicit `cluster_tls_insecure_skip_verify = true`
    ///   opt-out (NOT recommended), which runs encrypted-but-unverified. A single shared self-signed
    ///   cert may serve as its own CA (point `cluster_ca_path` at it).
    /// * The optional `cluster_ca_path`, when set, must be readable (it is loaded into the dial's
    ///   peer-cert verifier). Setting it WITHOUT `cluster_tls = on` is rejected (a CA is only used by
    ///   the TLS dial, so it would silently do nothing).
    /// * A `cluster_secret` MAY be set WITHOUT TLS (plaintext-but-authenticated bus). This is
    ///   allowed but the secret then travels in cleartext, so TLS+secret is the recommended pairing
    ///   (documented on [`Config::cluster_secret`]).
    /// * An EMPTY `cluster_secret` is rejected (an operator who set the knob to "" almost certainly
    ///   meant to disable it -- unset it instead -- and an empty shared secret is no authentication).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] naming the offending field when a required value is missing,
    /// empty, or unreadable.
    fn validate_cluster_transport(&self) -> Result<(), ConfigError> {
        let tls_on = self.cluster_tls == TlsMode::On;
        // An empty secret is a misconfiguration regardless of TLS: it is no authentication and is
        // almost certainly a typo for "unset". Reject it loudly.
        if let Some(secret) = &self.cluster_secret {
            if secret.is_empty() {
                return Err(ConfigError::Invalid {
                    field: "cluster_secret",
                    reason: "must not be empty when set (unset it to disable peer authentication)"
                        .to_owned(),
                });
            }
        }
        // A CA without TLS would silently do nothing (the CA is only consulted by the TLS dial's
        // peer-cert verifier); reject it so the operator's intent is not silently dropped.
        if self.cluster_ca_path.is_some() && !tls_on {
            return Err(ConfigError::Invalid {
                field: "cluster_ca_path",
                reason: "is only used by the TLS cluster dial; set cluster_tls = on to use it"
                    .to_owned(),
            });
        }
        if !tls_on {
            // Plaintext cluster: nothing else to pre-flight (a bare secret is allowed and validated
            // above; the default no-secret path is fully byte-unchanged).
            return Ok(());
        }
        // TLS on: a cert + key + secret are required, and the cert/key/CA must be readable.
        let cert = self
            .cluster_tls_cert_path
            .as_ref()
            .ok_or_else(|| ConfigError::Invalid {
                field: "cluster_tls_cert_path",
                reason: "is required when cluster_tls = on (the cluster TLS listener needs a \
                         certificate chain)"
                    .to_owned(),
            })?;
        let key = self
            .cluster_tls_key_path
            .as_ref()
            .ok_or_else(|| ConfigError::Invalid {
                field: "cluster_tls_key_path",
                reason: "is required when cluster_tls = on (the cluster TLS listener needs a \
                         private key)"
                    .to_owned(),
            })?;
        if self.cluster_secret.is_none() {
            return Err(ConfigError::Invalid {
                field: "cluster_secret",
                reason: "is required when cluster_tls = on (TLS encrypts the link but the shared \
                         secret authenticates the peer; without it any party reaching the port \
                         could join the bus / pull the replication stream)"
                    .to_owned(),
            });
        }
        // SECURITY (PROD-3 MITM fix): a CA is REQUIRED when cluster_tls = on so the dial VERIFIES the
        // peer cert. Without it the dial would accept ANY cert (an accept-any verifier) and then send
        // the cluster_secret to whatever it connected to -- an active man-in-the-middle could present
        // its own cert, pass the (non-)verification, and CAPTURE the secret (then forge RAFTMSG /
        // hijack consensus). The ONLY way to run TLS-on without a CA is the explicit, loudly-warned
        // cluster_tls_insecure_skip_verify opt-out.
        if self.cluster_ca_path.is_none() && !self.cluster_tls_insecure_skip_verify {
            return Err(ConfigError::Invalid {
                field: "cluster_ca_path",
                reason: "cluster_tls requires cluster_ca_path (the CA/cert that signs peer certs) \
                         so peer certs are verified; without it the cluster_secret is exposed to an \
                         active MITM. A single shared self-signed cert may serve as its own CA \
                         (point cluster_ca_path at it). To run encrypted-but-unverified anyway set \
                         cluster_tls_insecure_skip_verify=true (NOT recommended)."
                    .to_owned(),
            });
        }
        if let Err(e) = std::fs::File::open(cert) {
            return Err(ConfigError::Invalid {
                field: "cluster_tls_cert_path",
                reason: format!("cannot read certificate at {}: {e}", cert.display()),
            });
        }
        if let Err(e) = std::fs::File::open(key) {
            return Err(ConfigError::Invalid {
                field: "cluster_tls_key_path",
                reason: format!("cannot read private key at {}: {e}", key.display()),
            });
        }
        if let Some(ca) = &self.cluster_ca_path {
            if let Err(e) = std::fs::File::open(ca) {
                return Err(ConfigError::Invalid {
                    field: "cluster_ca_path",
                    reason: format!("cannot read cluster CA at {}: {e}", ca.display()),
                });
            }
        }
        Ok(())
    }

    /// True when a CLUSTERED mode is configured but the inter-node bus / replication link runs
    /// PLAINTEXT and UNAUTHENTICATED, i.e. it has NEITHER a shared `cluster_secret` NOR
    /// `cluster_tls = on`. In that posture any party that can reach the cluster-bus port could join
    /// consensus (forge `RAFTMSG`) or siphon the full keyspace off the replication stream, so the
    /// binary emits a loud boot warning (see `cluster_bus::warn_if_unauthenticated`). Pure: a
    /// posture read, no I/O, so it is unit-tested directly without a subscriber.
    ///
    /// "Clustered" is the broad reading the audit wants: `cluster_enabled` is set (a static
    /// multi-node bus + replication) OR `cluster_mode` is anything other than `Static` (raft
    /// governance or shard-owners), either of which stands up an inter-node link. The DEFAULT
    /// standalone node (`cluster_enabled = false`, `cluster_mode = Static`) is NOT clustered, so
    /// this returns `false` and the default boot logs nothing new.
    ///
    /// `cluster_tls = on` already REQUIRES a `cluster_secret` (enforced by
    /// `validate_cluster_transport`), so a TLS bus is always authenticated; a bare `cluster_secret`
    /// with TLS off is plaintext-but-authenticated (allowed, no warning). Only the no-secret,
    /// no-TLS clustered case is the exposure.
    #[must_use]
    pub fn cluster_bus_unauthenticated(&self) -> bool {
        let clustered = self.cluster_enabled || self.cluster_mode != ClusterMode::Static;
        clustered && self.cluster_secret.is_none() && self.cluster_tls != TlsMode::On
    }
}

/// Validate a [`ClusterTopology`] against `announce_id` (THIS node's id) by building a
/// throwaway [`ironcache_cluster::SlotMap`] and mapping any [`ironcache_cluster::SlotMapError`]
/// onto [`ConfigError::Invalid`]. This is the bridge that lets `Config::validate` reuse the
/// ONE slot-assignment validator (gap / overlap / duplicate id / bad range / bad id / empty /
/// self-not-present) the router and projection also build from, so the rules never drift.
///
/// `field` is `&'static str`, so the dynamic context (which slot, which ids) goes in `reason`
/// via the [`ironcache_cluster::SlotMapError`] `Display`.
///
/// # Errors
///
/// Returns [`ConfigError::Invalid`] with `field = "cluster_topology"` and a `reason` derived
/// from the [`ironcache_cluster::SlotMapError`] when the topology is not a complete, valid,
/// self-inclusive static map.
pub fn validate_topology(topo: &ClusterTopology, announce_id: &str) -> Result<(), ConfigError> {
    let nodes: Vec<(ironcache_cluster::NodeEntry, Vec<[u16; 2]>)> = topo
        .nodes
        .iter()
        .map(|n| {
            (
                ironcache_cluster::NodeEntry {
                    id: n.id.clone().into_boxed_str(),
                    host: n.host.clone().into_boxed_str(),
                    port: n.port,
                },
                n.slots.clone(),
            )
        })
        .collect();
    ironcache_cluster::SlotMap::build(nodes, announce_id)
        .map(|_| ())
        .map_err(|e| ConfigError::Invalid {
            field: "cluster_topology",
            reason: e.to_string(),
        })
}

/// Validate a [`ClusterTopology`] for RAFT governance (HA-4c): the topology supplies only the
/// voter set + peer cluster-bus addresses, NOT a static slot map, so this enforces ONLY the
/// node-IDENTITY rules and IGNORES the slot ranges (which are established at runtime through
/// committed proposals). The rules:
///   * at least one node;
///   * every id is exactly 40 lowercase hex characters (the Redis node-id shape the SlotMap
///     and the Raft id mapping require);
///   * no duplicate ids;
///   * `announce_id` names one of the nodes (this node is in its own voter set).
///
/// This deliberately does NOT call `SlotMap::build` (which requires complete, non-overlapping
/// slot coverage) because a raft-mode topology legitimately carries EMPTY slot ranges.
///
/// HOST FORMAT (k8s StatefulSet support): a node's `host` may be a DNS HOSTNAME (e.g. a per-pod
/// StatefulSet name `ironcache-0.ironcache.default.svc.cluster.local`) OR an IP literal. This
/// validation deliberately does NOT require the host to be an IP, and does NOT resolve it (name
/// resolution is real I/O that belongs in the boot / dial adapter, not in pure config validation):
/// the peer address is RESOLVED LAZILY at dial time (`ironcache::raft_boot` holds host + port and
/// the cluster-bus re-resolves per connect), so a peer whose DNS is not yet resolvable at config
/// time does not fail validation. Only the node-IDENTITY rules below are enforced here.
///
/// DERIVED-NODE-ID UNIQUENESS (F2): the Raft engine's `NodeId` is derived from only the TOP 64
/// BITS of the 40-hex announce id (the first 16 hex digits; see `raft_boot::node_id_from_announce`).
/// Two DISTINCT announce ids that share their first 16 hex digits would pass the full-id-uniqueness
/// check yet COLLIDE to ONE `NodeId` -- two physical nodes mapping to a single Raft identity, which
/// is catastrophic (one would silently shadow the other in every quorum). So this ALSO rejects a
/// topology in which any two announce ids derive the same `NodeId`. The derivation is inlined (the
/// config crate cannot depend on the `ironcache` crate where `node_id_from_announce` lives) but is
/// the SAME top-64-bit parse; the ids are already proven 40-lowercase-hex first, so the parse is
/// infallible here.
///
/// # Errors
///
/// Returns [`ConfigError::Invalid`] with `field = "cluster_topology"` (or `"cluster-announce-id"`
/// when self is absent) describing the first identity problem found.
pub fn validate_raft_topology(
    topo: &ClusterTopology,
    announce_id: &str,
) -> Result<(), ConfigError> {
    let invalid = |reason: String| ConfigError::Invalid {
        field: "cluster_topology",
        reason,
    };
    if topo.nodes.is_empty() {
        return Err(invalid("cluster topology has no nodes".to_owned()));
    }
    let mut seen: Vec<&str> = Vec::with_capacity(topo.nodes.len());
    // F2: track the derived NodeId (top 64 bits) -> the FIRST announce id that produced it, so a
    // collision error can name BOTH offending ids.
    let mut derived: Vec<(u64, &str)> = Vec::with_capacity(topo.nodes.len());
    for n in &topo.nodes {
        let id = n.id.as_str();
        // 40 lowercase hex (the Redis node-id shape).
        let well_formed = id.len() == 40
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !well_formed {
            return Err(invalid(format!(
                "node id '{id}' is not 40 lowercase hex characters"
            )));
        }
        if seen.contains(&id) {
            return Err(invalid(format!("duplicate node id '{id}'")));
        }
        // F2: the derived NodeId is the top 64 bits (first 16 hex digits). The id is proven 40-hex
        // above, so this parse cannot fail; it is the SAME derivation
        // `raft_boot::node_id_from_announce` uses, so config and the engine agree on a collision.
        let nid = u64::from_str_radix(&id[..16], 16)
            .expect("a validated 40-hex id parses its first 16 hex digits as u64");
        if let Some((_, other)) = derived.iter().find(|(d, _)| *d == nid) {
            return Err(invalid(format!(
                "node ids '{other}' and '{id}' derive the same raft NodeId (the engine keys nodes \
                 by the top 64 bits / first 16 hex digits of the announce id, which collide); give \
                 the nodes ids that differ within their first 16 hex digits"
            )));
        }
        seen.push(id);
        derived.push((nid, id));
    }
    if !seen.contains(&announce_id) {
        return Err(ConfigError::Invalid {
            field: "cluster-announce-id",
            reason: format!(
                "this node's announce id '{announce_id}' is not present in cluster_topology"
            ),
        });
    }
    Ok(())
}

/// A single layer of optional overrides. The TOML file deserializes directly into
/// this; the env and CLI layers construct it field by field. A `None` field means
/// "this layer does not set this key" and the lower layer shows through.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverlay {
    /// Bind address (string form, parsed on apply).
    pub bind: Option<IpAddr>,
    /// Port.
    pub port: Option<u16>,
    /// Shard count.
    pub shards: Option<usize>,
    /// Database count.
    pub databases: Option<u32>,
    /// Per-DB store slot count (#570, `store-slots-per-db`): the bounded-resize partition.
    pub slots_per_db: Option<u32>,
    /// Whether new connections default to RESP3.
    pub default_resp3: Option<bool>,
    /// Memory ceiling as a human size string ("512mb", "1gb", "0").
    pub maxmemory: Option<String>,
    /// Eviction policy name (one of the eight Redis `maxmemory-policy` names).
    pub maxmemory_policy: Option<String>,
    /// `requirepass` password.
    pub requirepass: Option<String>,
    /// Idle timeout in seconds.
    pub timeout: Option<u64>,
    /// The maximum simultaneous client connections (Redis `maxclients`, PROD-SAFETY #3).
    /// TOML (`maxclients = 10000`) + the `IRONCACHE_MAXCLIENTS` env var. `None` leaves the
    /// lower layer (default [`DEFAULT_MAXCLIENTS`] = 10000); `0` disables the cap.
    pub maxclients: Option<u64>,
    /// The per-connection output-buffer hard cap in bytes (PROD-SAFETY #5). TOML
    /// (`output_buffer_limit = 1073741824`) + the `IRONCACHE_OUTPUT_BUFFER_LIMIT` env var.
    /// `None` leaves the lower layer (default [`DEFAULT_OUTPUT_BUFFER_LIMIT`]); `0` disables it.
    pub output_buffer_limit: Option<u64>,
    /// The per-connection query-buffer hard cap in bytes (#528, the inbound analog of
    /// `output_buffer_limit`). TOML (`query_buffer_limit = 1073741824`) + the
    /// `IRONCACHE_QUERY_BUFFER_LIMIT` env var. `None` leaves the lower layer (default
    /// [`DEFAULT_QUERY_BUFFER_LIMIT`]); `0` disables it.
    pub query_buffer_limit: Option<u64>,
    /// The inbound bulk-string + string-value-growth ceiling in bytes (Redis
    /// `proto-max-bulk-len`). TOML (`proto_max_bulk_len = 536870912`) + the
    /// `IRONCACHE_PROTO_MAX_BULK_LEN` env var. `None` leaves the lower layer (default
    /// [`DEFAULT_PROTO_MAX_BULK_LEN`] = 512 MB).
    pub proto_max_bulk_len: Option<u64>,
    /// The TCP keepalive idle interval in seconds applied at accept (Redis `tcp-keepalive`).
    /// TOML (`tcp_keepalive_secs = 300`) + the `IRONCACHE_TCP_KEEPALIVE` env var. `None`
    /// leaves the lower layer (default [`DEFAULT_TCP_KEEPALIVE`] = 300); `0` disables it.
    pub tcp_keepalive_secs: Option<u64>,
    /// The 8 collection-encoding listpack/intset thresholds (`hash-max-listpack-entries`,
    /// `hash-max-listpack-value`, `list-max-listpack-size`, `set-max-intset-entries`,
    /// `set-max-listpack-entries`, `set-max-listpack-value`, `zset-max-listpack-entries`,
    /// `zset-max-listpack-value`). TOML (`hash_max_listpack_entries = 128`, ...) + the
    /// matching `IRONCACHE_*` env vars. `None` leaves the compiled default; the store reads
    /// the resolved value at the encoding-transition decision. `list_max_listpack_size` is
    /// the SIGNED Redis form (`-2` etc.); the rest are positive counts/byte caps.
    pub hash_max_listpack_entries: Option<usize>,
    /// See [`Self::hash_max_listpack_entries`].
    pub hash_max_listpack_value: Option<usize>,
    /// See [`Self::hash_max_listpack_entries`]. SIGNED Redis size form (`-2` etc.).
    pub list_max_listpack_size: Option<i64>,
    /// See [`Self::hash_max_listpack_entries`].
    pub set_max_intset_entries: Option<usize>,
    /// See [`Self::hash_max_listpack_entries`].
    pub set_max_listpack_entries: Option<usize>,
    /// See [`Self::hash_max_listpack_entries`].
    pub set_max_listpack_value: Option<usize>,
    /// See [`Self::hash_max_listpack_entries`].
    pub zset_max_listpack_entries: Option<usize>,
    /// See [`Self::hash_max_listpack_entries`].
    pub zset_max_listpack_value: Option<usize>,
    /// Whether to run in cluster mode (Redis `cluster-enabled`, CLUSTER_CONTRACT.md #70).
    /// Boot-only; `None` leaves the lower layer (default `false`) showing through.
    pub cluster_enabled: Option<bool>,
    /// The static cluster topology (CLUSTER_CONTRACT.md #70, slice 2). TOML-only (deserialized
    /// from `[[cluster_topology.nodes]]`); there is no env/CLI form for the structured map.
    pub cluster_topology: Option<ClusterTopology>,
    /// THIS node's announce id. TOML + the `IRONCACHE_CLUSTER_ANNOUNCE_ID` env var.
    pub cluster_announce_id: Option<String>,
    /// How the slot map is governed (HA-4c). TOML (`cluster_mode = "raft"`) + the
    /// `IRONCACHE_CLUSTER_MODE` env var. `None` leaves the lower layer (default `Static`).
    pub cluster_mode: Option<ClusterMode>,
    /// HA-8 replication-lag bound (logical writes) for promotion + replica-read staleness.
    /// TOML (`replica_max_lag = N`) + the `IRONCACHE_REPLICA_MAX_LAG` env var. `None` leaves
    /// the lower layer (default [`DEFAULT_REPLICA_MAX_LAG`]).
    pub replica_max_lag: Option<u64>,
    /// HA-8 failover timeout (seconds): link-down duration before a replica self-proposes
    /// promotion. TOML (`failover_timeout_secs = N`) + the `IRONCACHE_FAILOVER_TIMEOUT_SECS`
    /// env var. `None` leaves the lower layer (default [`DEFAULT_FAILOVER_TIMEOUT_SECS`]).
    pub failover_timeout_secs: Option<u64>,
    /// The WRITE-SIDE replication guardrail `min-replicas-to-write` (ADR-0026): minimum in-sync
    /// replicas before an owner accepts a write. TOML (`min_replicas_to_write = N`) + the
    /// `IRONCACHE_MIN_REPLICAS_TO_WRITE` env var. `None` leaves the lower layer (default 0 =
    /// DISABLED, the write hot path byte-unchanged).
    pub min_replicas_to_write: Option<u32>,
    /// The lag bound (logical writes) for the `min-replicas-to-write` quorum. TOML
    /// (`min_replicas_max_lag = N`) + the `IRONCACHE_MIN_REPLICAS_MAX_LAG` env var. `None` leaves
    /// the lower layer (default [`DEFAULT_MIN_REPLICAS_MAX_LAG`]).
    pub min_replicas_max_lag: Option<u64>,
    /// HA-3c Raft-log compaction threshold (entries above the last snapshot). TOML
    /// (`raft_snapshot_threshold = N`) + the `IRONCACHE_RAFT_SNAPSHOT_THRESHOLD` env var.
    /// `None` leaves the lower layer (default [`DEFAULT_RAFT_SNAPSHOT_THRESHOLD`]); `0`
    /// disables compaction.
    pub raft_snapshot_threshold: Option<u64>,
    /// PROD-9 chunked-InstallSnapshot chunk size (bytes). TOML (`raft_snapshot_chunk_bytes = N`)
    /// plus the `IRONCACHE_RAFT_SNAPSHOT_CHUNK_BYTES` env var. `None` leaves the lower layer
    /// (default [`DEFAULT_RAFT_SNAPSHOT_CHUNK_BYTES`]); `0` sends the whole snapshot in one chunk.
    pub raft_snapshot_chunk_bytes: Option<usize>,
    /// HA-7e disk-backed replication backlog size (bytes). TOML (`repl_backlog_disk_bytes = N`)
    /// plus the `IRONCACHE_REPL_BACKLOG_DISK_BYTES` env var. `None` leaves the lower layer (default
    /// `0` = DISABLED, byte-identical in-memory-only backlog).
    pub repl_backlog_disk_bytes: Option<u64>,
    /// The durable data directory for the Raft log (and future on-disk state). TOML
    /// (`data_dir = "/var/lib/ironcache"`, a string path) + the `IRONCACHE_DATA_DIR` env var.
    /// `None` leaves the lower layer (default `None` = the OS temp dir, byte-unchanged).
    pub data_dir: Option<PathBuf>,
    /// The ACL file path (#106). TOML (`aclfile = "..."`, a string path) + the
    /// `IRONCACHE_ACLFILE` env var. `None` leaves the lower layer (default `None` = no aclfile).
    pub aclfile: Option<PathBuf>,
    /// The transport-TLS posture for the client listener (#105). TOML (`tls = "on"`) + the
    /// `IRONCACHE_TLS` env var. `None` leaves the lower layer (default [`TlsMode::Off`],
    /// plaintext byte-unchanged).
    pub tls: Option<TlsMode>,
    /// Path to the PEM certificate chain for the TLS listener (#105). TOML (`tls_cert_path =
    /// "..."`, a string path) + the `IRONCACHE_TLS_CERT_PATH` env var. `None` leaves the lower
    /// layer (default `None`).
    pub tls_cert_path: Option<PathBuf>,
    /// Path to the PEM private key for the TLS listener (#105). TOML (`tls_key_path = "..."`) +
    /// the `IRONCACHE_TLS_KEY_PATH` env var. `None` leaves the lower layer (default `None`).
    pub tls_key_path: Option<PathBuf>,
    /// The per-shard runtime backend (PROD-10 / #28). TOML (`runtime = "io_uring"`) + the
    /// `IRONCACHE_RUNTIME` env var + the `--runtime` CLI flag. `None` leaves the lower layer
    /// (default [`RuntimeBackend::Tokio`]). `io_uring` is honored only on a Linux build with the
    /// `io_uring` feature + TLS off; otherwise the boot falls back to tokio.
    pub runtime: Option<RuntimeBackend>,
    /// The periodic save interval in seconds (#58 persistence save policy). TOML
    /// (`save_interval_secs = 900`) + the `IRONCACHE_SAVE_INTERVAL_SECS` env var. `None` leaves the
    /// lower layer (default `0` = no periodic save).
    pub save_interval_secs: Option<u64>,
    /// The minimum keyspace writes the periodic save policy requires before firing (#58). TOML
    /// (`save_min_changes = 1`) + the `IRONCACHE_SAVE_MIN_CHANGES` env var. `None` leaves the lower
    /// layer (default `0`).
    pub save_min_changes: Option<u64>,
    /// The dedicated persist core (#589): which CPU core(s) the `ic-persist` thread pins to. TOML
    /// (`persist_cpu = "8"`) + the `IRONCACHE_PERSIST_CPU` env var + the `--persist-cpu` CLI flag. A
    /// string (`off`/`auto`/a cpu list); `None` leaves the lower layer (default `""` = no pin).
    pub persist_cpu: Option<String>,
    /// The `notify-keyspace-events` flag string (PROD-8, keyspace notifications). TOML
    /// (`notify_keyspace_events = "KEA"`) + the `IRONCACHE_NOTIFY_KEYSPACE_EVENTS` env var. `None`
    /// leaves the lower layer (default `""` = DISABLED). The flag-string validity is checked in
    /// [`Config::validate`].
    pub notify_keyspace_events: Option<String>,
    /// The intra-cluster transport-TLS posture (PROD-3). TOML (`cluster_tls = "on"`) + the
    /// `IRONCACHE_CLUSTER_TLS` env var. `None` leaves the lower layer (default [`TlsMode::Off`],
    /// plaintext byte-unchanged).
    pub cluster_tls: Option<TlsMode>,
    /// Path to the PEM cert chain the intra-cluster TLS listener presents (PROD-3). TOML
    /// (`cluster_tls_cert_path = "..."`) + the `IRONCACHE_CLUSTER_TLS_CERT_PATH` env var.
    pub cluster_tls_cert_path: Option<PathBuf>,
    /// Path to the PEM private key for the intra-cluster TLS listener (PROD-3). TOML
    /// (`cluster_tls_key_path = "..."`) + the `IRONCACHE_CLUSTER_TLS_KEY_PATH` env var.
    pub cluster_tls_key_path: Option<PathBuf>,
    /// Path to the PEM cluster CA the dial verifies the peer cert against (PROD-3, optional). TOML
    /// (`cluster_ca_path = "..."`) + the `IRONCACHE_CLUSTER_CA_PATH` env var.
    pub cluster_ca_path: Option<PathBuf>,
    /// The shared cluster secret for peer authentication (PROD-3). TOML (`cluster_secret = "..."`)
    /// and the `IRONCACHE_CLUSTER_SECRET` env var. `None` leaves the lower layer (default `None`,
    /// no secret check).
    pub cluster_secret: Option<String>,
    /// EXPLICIT, NOT-RECOMMENDED opt-in to skip intra-cluster peer-cert verification (PROD-3). TOML
    /// (`cluster_tls_insecure_skip_verify = true`) + the `IRONCACHE_CLUSTER_TLS_INSECURE_SKIP_VERIFY`
    /// env var. `None` leaves the lower layer (default `false`, peer cert is verified against the CA).
    pub cluster_tls_insecure_skip_verify: Option<bool>,
    /// Fail closed on a format-version-mismatched on-disk snapshot (#530). TOML
    /// (`refuse_empty_start_on_version_mismatch = true`) + the
    /// `IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH` env var. `None` leaves the lower layer
    /// (default `false`, log loudly + start empty rather than refusing to boot).
    pub refuse_empty_start_on_version_mismatch: Option<bool>,
}

impl ConfigOverlay {
    /// Parse a TOML document into an overlay.
    pub fn from_toml_str(s: &str) -> Result<ConfigOverlay, ConfigError> {
        Ok(toml::from_str(s)?)
    }

    /// Load an overlay from a TOML file path. A missing file yields an empty
    /// overlay (an absent config file is allowed, CONFIG.md / Redis parity).
    pub fn from_toml_file(path: &std::path::Path) -> Result<ConfigOverlay, ConfigError> {
        match std::fs::read_to_string(path) {
            Ok(s) => ConfigOverlay::from_toml_str(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ConfigOverlay::default()),
            Err(e) => Err(ConfigError::Io(e.to_string())),
        }
    }

    /// Build an overlay from `IRONCACHE_*` environment variables. Unset variables
    /// leave their field `None`. Size/number parse errors are returned.
    ///
    /// `too_many_lines` is allowed: this is a FLAT one-block-per-knob env reader (bind, port,
    /// shards, maxmemory, policy, requirepass, cluster, the HA-8 knobs, data_dir, and the TLS
    /// knobs). Each block is a self-contained `if let Ok(v) = env::var(..)`; splitting them into
    /// helpers would scatter the single env-mapping surface for no readability gain. The same
    /// precedent as the `validate` / `run_server` wiring points.
    #[allow(clippy::too_many_lines)]
    pub fn from_env() -> Result<ConfigOverlay, ConfigError> {
        let mut o = ConfigOverlay::default();
        // SINGLE SOURCE OF TRUTH for the known `IRONCACHE_*` keys: `env_var` appends every key this
        // reader probes into `known` as it reads it, so the strict "unknown key" guard at the end
        // (below) cannot drift from the set of keys we actually honor. Adding a new `env_var(..)`
        // read below automatically extends the known set; there is no second hand-maintained list.
        let mut known: Vec<&'static str> = Vec::new();
        let mut env_var = |key: &'static str| {
            known.push(key);
            std::env::var(key)
        };
        if let Ok(v) = env_var("IRONCACHE_BIND") {
            o.bind = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "bind",
                reason: format!("not an IP address: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_PORT") {
            o.port = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "port",
                reason: format!("not a port: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_SHARDS") {
            o.shards = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "shards",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_MAXMEMORY") {
            o.maxmemory = Some(v);
        }
        if let Ok(v) = env_var("IRONCACHE_MAXMEMORY_POLICY") {
            o.maxmemory_policy = Some(v);
        }
        if let Ok(v) = env_var("IRONCACHE_REQUIREPASS") {
            o.requirepass = Some(v);
        }
        // Idle timeout (PROD-SAFETY #4): seconds a connection may sit idle before it is
        // closed; 0 disables it (Redis default). Env-readable for parity with the other
        // connection knobs (previously TOML-only).
        if let Ok(v) = env_var("IRONCACHE_TIMEOUT") {
            o.timeout = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "timeout",
                reason: format!("not a number of seconds: {v}"),
            })?);
        }
        // The simultaneous-connection ceiling (PROD-SAFETY #3, Redis `maxclients`); 0
        // disables the cap.
        if let Ok(v) = env_var("IRONCACHE_MAXCLIENTS") {
            o.maxclients = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "maxclients",
                reason: format!("not a number: {v}"),
            })?);
        }
        // The per-connection output-buffer hard cap in bytes (PROD-SAFETY #5); 0 disables it.
        if let Ok(v) = env_var("IRONCACHE_OUTPUT_BUFFER_LIMIT") {
            o.output_buffer_limit = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "output_buffer_limit",
                reason: format!("not a number of bytes: {v}"),
            })?);
        }
        // The per-connection query-buffer hard cap in bytes (#528, inbound analog); 0 disables it.
        if let Ok(v) = env_var("IRONCACHE_QUERY_BUFFER_LIMIT") {
            o.query_buffer_limit = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "query_buffer_limit",
                reason: format!("not a number of bytes: {v}"),
            })?);
        }
        // The inbound bulk-string + string-value-growth ceiling (Redis `proto-max-bulk-len`).
        // Accepts a human size ("512mb") OR a plain byte count for env parity with maxmemory.
        if let Ok(v) = env_var("IRONCACHE_PROTO_MAX_BULK_LEN") {
            o.proto_max_bulk_len =
                Some(parse_human_size(&v).map_err(|_| ConfigError::Invalid {
                    field: "proto_max_bulk_len",
                    reason: format!("not a number of bytes: {v}"),
                })?);
        }
        // The TCP keepalive idle interval in seconds applied at accept (Redis `tcp-keepalive`); 0
        // disables keepalive.
        if let Ok(v) = env_var("IRONCACHE_TCP_KEEPALIVE") {
            o.tcp_keepalive_secs = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "tcp_keepalive_secs",
                reason: format!("not a number of seconds: {v}"),
            })?);
        }
        // The 8 collection-encoding thresholds (Redis `*-max-listpack-*` / `set-max-intset-entries`).
        // Single scalars, so env-encodable; `list-max-listpack-size` is the signed Redis form.
        if let Ok(v) = env_var("IRONCACHE_HASH_MAX_LISTPACK_ENTRIES") {
            o.hash_max_listpack_entries = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "hash_max_listpack_entries",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_HASH_MAX_LISTPACK_VALUE") {
            o.hash_max_listpack_value = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "hash_max_listpack_value",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_LIST_MAX_LISTPACK_SIZE") {
            o.list_max_listpack_size = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "list_max_listpack_size",
                reason: format!("not a signed number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_SET_MAX_INTSET_ENTRIES") {
            o.set_max_intset_entries = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "set_max_intset_entries",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_SET_MAX_LISTPACK_ENTRIES") {
            o.set_max_listpack_entries = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "set_max_listpack_entries",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_SET_MAX_LISTPACK_VALUE") {
            o.set_max_listpack_value = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "set_max_listpack_value",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_ZSET_MAX_LISTPACK_ENTRIES") {
            o.zset_max_listpack_entries = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "zset_max_listpack_entries",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_ZSET_MAX_LISTPACK_VALUE") {
            o.zset_max_listpack_value = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "zset_max_listpack_value",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_ENABLED") {
            o.cluster_enabled = Some(parse_bool(&v).ok_or_else(|| ConfigError::Invalid {
                field: "cluster-enabled",
                reason: format!("not a boolean (expected yes/no/true/false/1/0): {v}"),
            })?);
        }
        // The announce id is a single scalar, so it is env-encodable (useful for per-pod
        // identity injection in a stateful set). The structured topology is TOML-only. The id
        // is validated (40-hex + present in topology) by Config::validate via the slot map.
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_ANNOUNCE_ID") {
            o.cluster_announce_id = Some(v);
        }
        // The cluster-governance mode (HA-4c): `static` (default) or `raft`. A single scalar,
        // so it is env-encodable (per-pod injection alongside the announce id). Mirrors the
        // `cluster_enabled` env handling: an unrecognized token hard-fails rather than
        // silently picking a mode.
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_MODE") {
            o.cluster_mode = Some(parse_cluster_mode(&v).ok_or_else(|| ConfigError::Invalid {
                field: "cluster-mode",
                reason: format!("not a cluster mode (expected static/raft/shard-owners): {v}"),
            })?);
        }
        // HA-8 knobs (single scalars, so env-encodable for per-pod injection). Both are
        // meaningful only in raft-mode; a malformed value hard-fails boot rather than
        // silently picking a default.
        if let Ok(v) = env_var("IRONCACHE_REPLICA_MAX_LAG") {
            o.replica_max_lag = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "replica-max-lag",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_FAILOVER_TIMEOUT_SECS") {
            o.failover_timeout_secs = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "failover-timeout-secs",
                reason: format!("not a number: {v}"),
            })?);
        }
        // The write-side replication guardrail knobs (single scalars, env-encodable for per-pod
        // injection). Both are meaningful only in raft-mode AND only when the guardrail is
        // enabled; a malformed value hard-fails boot rather than silently picking a default.
        if let Ok(v) = env_var("IRONCACHE_MIN_REPLICAS_TO_WRITE") {
            o.min_replicas_to_write = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "min-replicas-to-write",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_MIN_REPLICAS_MAX_LAG") {
            o.min_replicas_max_lag = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "min-replicas-max-lag",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_RAFT_SNAPSHOT_THRESHOLD") {
            o.raft_snapshot_threshold = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "raft-snapshot-threshold",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_RAFT_SNAPSHOT_CHUNK_BYTES") {
            o.raft_snapshot_chunk_bytes = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "raft-snapshot-chunk-bytes",
                reason: format!("not a number: {v}"),
            })?);
        }
        // HA-7e disk-backed replication backlog size (bytes); a single scalar, env-encodable.
        if let Ok(v) = env_var("IRONCACHE_REPL_BACKLOG_DISK_BYTES") {
            o.repl_backlog_disk_bytes = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "repl-backlog-disk-bytes",
                reason: format!("not a number: {v}"),
            })?);
        }
        // The durable data directory is a single scalar path, so it is env-encodable (useful
        // for per-pod injection in a stateful set alongside the announce id). A path is taken
        // verbatim (no parse can fail); an empty value is rejected by Config::validate.
        if let Ok(v) = env_var("IRONCACHE_DATA_DIR") {
            o.data_dir = Some(PathBuf::from(v));
        }
        // The ACL file path (#106) is a single scalar path, env-encodable for per-pod injection.
        // Taken verbatim (no parse can fail); a missing/unreadable file hard-fails at boot LOAD.
        if let Ok(v) = env_var("IRONCACHE_ACLFILE") {
            o.aclfile = Some(PathBuf::from(v));
        }
        // TRANSPORT TLS knobs (#105). The mode is a single scalar token (off/on, case-insensitive),
        // env-encodable for per-pod injection; an unrecognized token hard-fails boot rather than
        // silently picking a posture (mirrors `cluster_enabled` / `cluster_mode`). The cert/key are
        // scalar paths taken verbatim (no parse can fail); their readability is checked in
        // Config::validate when tls = on.
        if let Ok(v) = env_var("IRONCACHE_TLS") {
            o.tls = Some(parse_tls_mode(&v).ok_or_else(|| ConfigError::Invalid {
                field: "tls",
                reason: format!("not a TLS mode (expected off/on): {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_TLS_CERT_PATH") {
            o.tls_cert_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = env_var("IRONCACHE_TLS_KEY_PATH") {
            o.tls_key_path = Some(PathBuf::from(v));
        }
        // The per-shard RUNTIME backend (PROD-10 / #28): a single tokio/io_uring token
        // (case-insensitive), env-encodable for per-pod injection; an unrecognized token hard-fails
        // boot (mirrors the `tls` / `cluster_mode` knobs). Requesting `io_uring` on a non-Linux /
        // no-feature / TLS build is NOT an error here -- it falls back to tokio at boot.
        if let Ok(v) = env_var("IRONCACHE_RUNTIME") {
            o.runtime = Some(
                parse_runtime_backend(&v).ok_or_else(|| ConfigError::Invalid {
                    field: "runtime",
                    reason: format!("not a runtime backend (expected tokio/io_uring): {v}"),
                })?,
            );
        }
        // INTRA-CLUSTER transport-security knobs (PROD-3). The mode is a single off/on token
        // (case-insensitive), env-encodable for per-pod injection; an unrecognized token hard-fails
        // boot (mirrors the public `tls` knob). The cert/key/CA are scalar paths taken verbatim
        // (readability checked in Config::validate when cluster_tls = on); the secret is a scalar
        // token taken verbatim.
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_TLS") {
            o.cluster_tls = Some(parse_tls_mode(&v).ok_or_else(|| ConfigError::Invalid {
                field: "cluster-tls",
                reason: format!("not a TLS mode (expected off/on): {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_TLS_CERT_PATH") {
            o.cluster_tls_cert_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_TLS_KEY_PATH") {
            o.cluster_tls_key_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_CA_PATH") {
            o.cluster_ca_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_SECRET") {
            o.cluster_secret = Some(v);
        }
        // The insecure peer-cert-skip opt-out is a boolean (yes/no/true/false/1/0). An unrecognized
        // token hard-fails boot rather than silently leaving verification on/off.
        if let Ok(v) = env_var("IRONCACHE_CLUSTER_TLS_INSECURE_SKIP_VERIFY") {
            o.cluster_tls_insecure_skip_verify =
                Some(parse_bool(&v).ok_or_else(|| ConfigError::Invalid {
                    field: "cluster-tls-insecure-skip-verify",
                    reason: format!("not a boolean (expected yes/no/true/false/1/0): {v}"),
                })?);
        }
        // FAIL CLOSED on a version-mismatched snapshot is a boolean (#530). An unrecognized token
        // hard-fails boot rather than silently leaving the posture off.
        if let Ok(v) = env_var("IRONCACHE_REFUSE_EMPTY_START_ON_VERSION_MISMATCH") {
            o.refuse_empty_start_on_version_mismatch =
                Some(parse_bool(&v).ok_or_else(|| ConfigError::Invalid {
                    field: "refuse-empty-start-on-version-mismatch",
                    reason: format!("not a boolean (expected yes/no/true/false/1/0): {v}"),
                })?);
        }
        // PERSISTENCE save-policy knobs (#58, single scalars, env-encodable for per-pod injection).
        // Both are meaningful only when a data_dir is set; a malformed value hard-fails boot rather
        // than silently picking a default (mirrors the other numeric knobs above).
        if let Ok(v) = env_var("IRONCACHE_SAVE_INTERVAL_SECS") {
            o.save_interval_secs = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "save-interval-secs",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = env_var("IRONCACHE_SAVE_MIN_CHANGES") {
            o.save_min_changes = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "save-min-changes",
                reason: format!("not a number: {v}"),
            })?);
        }
        // The dedicated persist core (#589): a string knob (`off`/`auto`/a cpu list). Validity is
        // checked once on the RESOLVED value in Config::validate (like notify-keyspace-events), so an
        // env override folds through and is validated together with any TOML/CLI layer.
        if let Ok(v) = env_var("IRONCACHE_PERSIST_CPU") {
            o.persist_cpu = Some(v);
        }
        if let Ok(v) = env_var("IRONCACHE_NOTIFY_KEYSPACE_EVENTS") {
            // The flag-string validity is checked once on the resolved value in Config::validate.
            o.notify_keyspace_events = Some(v);
        }
        // UNKNOWN-KEY GUARD. Every key we honor was recorded in `known` above, so ANY `IRONCACHE_*`
        // variable present in the environment that is NOT there is either a typo (e.g.
        // `IRONCACHE_MAXCLIENT` for `IRONCACHE_MAXCLIENTS`) whose intended setting would otherwise be
        // silently dropped, or an orchestrator/tooling variable in the shared `IRONCACHE_*`
        // namespace. WARN loudly (do not fail boot) naming the key with a nearest-known suggestion:
        // the environment is shared, so a hard failure here is user-hostile (unlike the config FILE,
        // which stays strict). `env_var` is not called past this point, so its `&mut known` borrow
        // has ended and the guard can read `known`.
        warn_unknown_env(std::env::vars().map(|(k, _)| k), &known);
        Ok(o)
    }

    /// Apply this overlay's set fields onto `cfg`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Size`] if this overlay's `maxmemory` is malformed or
    /// out of range. A bad ceiling propagates so the binary hard-fails at boot
    /// rather than silently going unlimited (honest-ceiling invariant #3).
    ///
    /// `too_many_lines` is allowed: this is a FLAT one-`if let` per overlay field (the
    /// mechanical fold of every settable knob), the same shape + precedent as `from_env`.
    #[allow(clippy::too_many_lines)]
    fn apply_to(&self, cfg: &mut Config) -> Result<(), ConfigError> {
        if let Some(v) = self.bind {
            cfg.bind = v;
        }
        if let Some(v) = self.port {
            cfg.port = v;
        }
        if let Some(v) = self.shards {
            cfg.shards = v;
        }
        if let Some(v) = self.databases {
            cfg.databases = v;
        }
        if let Some(v) = self.slots_per_db {
            cfg.slots_per_db = v;
        }
        if let Some(v) = self.default_resp3 {
            cfg.default_resp3 = v;
        }
        if let Some(ref v) = self.maxmemory {
            // Parse the human size here and PROPAGATE any error: a malformed or
            // overflowing maxmemory must hard-fail boot, not silently become 0
            // (unlimited). Integer math, overflow-checked (see parse_human_size).
            cfg.maxmemory = parse_human_size(v)?;
        }
        if let Some(ref v) = self.maxmemory_policy {
            // Name validity is checked in Config::validate (after all overlays fold),
            // so an env/CLI override is validated once on the resolved value.
            cfg.maxmemory_policy.clone_from(v);
        }
        if let Some(ref v) = self.requirepass {
            cfg.requirepass = Some(v.clone());
        }
        if let Some(v) = self.timeout {
            cfg.timeout_secs = v;
        }
        if let Some(v) = self.maxclients {
            cfg.maxclients = v;
        }
        if let Some(v) = self.output_buffer_limit {
            cfg.output_buffer_limit = v;
        }
        if let Some(v) = self.query_buffer_limit {
            cfg.query_buffer_limit = v;
        }
        if let Some(v) = self.proto_max_bulk_len {
            cfg.proto_max_bulk_len = v;
        }
        if let Some(v) = self.tcp_keepalive_secs {
            cfg.tcp_keepalive_secs = v;
        }
        if let Some(v) = self.hash_max_listpack_entries {
            cfg.hash_max_listpack_entries = v;
        }
        if let Some(v) = self.hash_max_listpack_value {
            cfg.hash_max_listpack_value = v;
        }
        if let Some(v) = self.list_max_listpack_size {
            cfg.list_max_listpack_size = v;
        }
        if let Some(v) = self.set_max_intset_entries {
            cfg.set_max_intset_entries = v;
        }
        if let Some(v) = self.set_max_listpack_entries {
            cfg.set_max_listpack_entries = v;
        }
        if let Some(v) = self.set_max_listpack_value {
            cfg.set_max_listpack_value = v;
        }
        if let Some(v) = self.zset_max_listpack_entries {
            cfg.zset_max_listpack_entries = v;
        }
        if let Some(v) = self.zset_max_listpack_value {
            cfg.zset_max_listpack_value = v;
        }
        if let Some(v) = self.cluster_enabled {
            cfg.cluster_enabled = v;
        }
        if let Some(ref v) = self.cluster_topology {
            cfg.cluster_topology = Some(v.clone());
        }
        if let Some(ref v) = self.cluster_announce_id {
            cfg.cluster_announce_id = Some(v.clone());
        }
        if let Some(v) = self.cluster_mode {
            cfg.cluster_mode = v;
        }
        if let Some(v) = self.replica_max_lag {
            cfg.replica_max_lag = v;
        }
        if let Some(v) = self.failover_timeout_secs {
            cfg.failover_timeout_secs = v;
        }
        if let Some(v) = self.min_replicas_to_write {
            cfg.min_replicas_to_write = v;
        }
        if let Some(v) = self.min_replicas_max_lag {
            cfg.min_replicas_max_lag = v;
        }
        if let Some(v) = self.raft_snapshot_threshold {
            cfg.raft_snapshot_threshold = v;
        }
        if let Some(v) = self.raft_snapshot_chunk_bytes {
            cfg.raft_snapshot_chunk_bytes = v;
        }
        if let Some(v) = self.repl_backlog_disk_bytes {
            cfg.repl_backlog_disk_bytes = v;
        }
        if let Some(ref v) = self.data_dir {
            cfg.data_dir = Some(v.clone());
        }
        if let Some(ref v) = self.aclfile {
            cfg.aclfile = Some(v.clone());
        }
        if let Some(v) = self.tls {
            cfg.tls = v;
        }
        if let Some(ref v) = self.tls_cert_path {
            cfg.tls_cert_path = Some(v.clone());
        }
        if let Some(ref v) = self.tls_key_path {
            cfg.tls_key_path = Some(v.clone());
        }
        if let Some(v) = self.runtime {
            cfg.runtime = v;
        }
        if let Some(v) = self.save_interval_secs {
            cfg.save_interval_secs = v;
        }
        if let Some(v) = self.save_min_changes {
            cfg.save_min_changes = v;
        }
        if let Some(ref v) = self.persist_cpu {
            // Raw string folds through; validity is checked in Config::validate on the resolved value.
            cfg.persist_cpu.clone_from(v);
        }
        if let Some(ref v) = self.notify_keyspace_events {
            // Flag-string validity is checked in Config::validate (after all overlays fold), so an
            // env/CLI override is validated once on the resolved value.
            cfg.notify_keyspace_events.clone_from(v);
        }
        if let Some(v) = self.cluster_tls {
            cfg.cluster_tls = v;
        }
        if let Some(ref v) = self.cluster_tls_cert_path {
            cfg.cluster_tls_cert_path = Some(v.clone());
        }
        if let Some(ref v) = self.cluster_tls_key_path {
            cfg.cluster_tls_key_path = Some(v.clone());
        }
        if let Some(ref v) = self.cluster_ca_path {
            cfg.cluster_ca_path = Some(v.clone());
        }
        if let Some(ref v) = self.cluster_secret {
            cfg.cluster_secret = Some(v.clone());
        }
        if let Some(v) = self.cluster_tls_insecure_skip_verify {
            cfg.cluster_tls_insecure_skip_verify = v;
        }
        if let Some(v) = self.refuse_empty_start_on_version_mismatch {
            cfg.refuse_empty_start_on_version_mismatch = v;
        }
        Ok(())
    }
}

/// Parse a transport-TLS mode token (#105), accepting `off` / `on` case-insensitively with
/// surrounding whitespace trimmed (and the Redis-style `no`/`yes` spellings, mirroring
/// [`parse_bool`], so an operator can write either). Returns `None` on any other token (the
/// caller maps it to a config error). Used by the `IRONCACHE_TLS` env var; TOML deserializes
/// [`TlsMode`] directly (lowercase-renamed serde).
#[must_use]
pub fn parse_tls_mode(s: &str) -> Option<TlsMode> {
    match s.trim().to_ascii_lowercase().as_str() {
        "off" | "no" | "false" | "0" => Some(TlsMode::Off),
        "on" | "yes" | "true" | "1" => Some(TlsMode::On),
        _ => None,
    }
}

/// Parse a cluster-governance mode token (HA-4c), accepting `static` / `raft`
/// case-insensitively with surrounding whitespace trimmed. Returns `None` on any other
/// token (the caller maps it to a config error). Used by the `IRONCACHE_CLUSTER_MODE` env
/// var; TOML deserializes [`ClusterMode`] directly (lowercase-renamed serde).
#[must_use]
pub fn parse_cluster_mode(s: &str) -> Option<ClusterMode> {
    match s.trim().to_ascii_lowercase().as_str() {
        "static" => Some(ClusterMode::Static),
        "raft" => Some(ClusterMode::Raft),
        // #517: `shard-owners` / `shardowners` (friendly alias) select the standalone shard-slot-owner
        // mode where the node's internal shards are exposed as per-port hashslot owners.
        "shard-owners" | "shardowners" => Some(ClusterMode::ShardOwners),
        _ => None,
    }
}

/// Parse a runtime-backend token (PROD-10 / #28), accepting `tokio` and `io_uring` (also
/// `io-uring` / `iouring` / `uring` as friendly aliases) case-insensitively with surrounding
/// whitespace trimmed. Returns `None` on any other token (the caller maps it to a config
/// error). Used by the `IRONCACHE_RUNTIME` env var and the `--runtime` CLI flag; TOML
/// deserializes [`RuntimeBackend`] directly (snake_case-renamed serde, so `runtime =
/// "io_uring"`).
#[must_use]
pub fn parse_runtime_backend(s: &str) -> Option<RuntimeBackend> {
    match s.trim().to_ascii_lowercase().as_str() {
        "tokio" => Some(RuntimeBackend::Tokio),
        "io_uring" | "io-uring" | "iouring" | "uring" => Some(RuntimeBackend::IoUring),
        _ => None,
    }
}

/// Parse a human-readable size into bytes, accepting Redis-style suffixes:
/// `b`, `k`/`kb`, `m`/`mb`, `g`/`gb` (and uppercase). Bare numbers are bytes.
/// `k`/`m`/`g` are 1000-based and `kb`/`mb`/`gb` are 1024-based, matching Redis's
/// `memtoull` convention. `0` parses to `0` (unlimited).
///
/// The numeric prefix is parsed as a `u64` and multiplied by the unit with
/// `checked_mul`, so all arithmetic is integer and overflow is a hard error
/// (never a silent wrap). FRACTIONAL inputs are REJECTED: `1.5gb` returns a
/// [`ConfigError::Size`] rather than truncating, because a cache ceiling must be
/// an exact byte count and silent truncation hides operator intent. A leading `+`
/// is rejected too (Redis sizes are plain non-negative integers).
pub fn parse_human_size(s: &str) -> Result<u64, ConfigError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(ConfigError::Size(s.to_owned(), "empty".to_owned()));
    }
    // Numeric prefix is ASCII digits only (no '.', '+', '-'): any of those makes
    // the input either fractional, signed, or malformed, all of which we reject.
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num_part, unit_part) = t.split_at(split);
    if num_part.is_empty() {
        return Err(ConfigError::Size(
            s.to_owned(),
            "missing numeric value".to_owned(),
        ));
    }
    let unit = unit_part.trim().to_ascii_lowercase();
    let mult: u64 = match unit.as_str() {
        "" | "b" => 1,
        "k" => 1_000,
        "kb" => 1_024,
        "m" => 1_000_000,
        "mb" => 1_024 * 1_024,
        "g" => 1_000_000_000,
        "gb" => 1_024 * 1_024 * 1_024,
        other => {
            return Err(ConfigError::Size(
                s.to_owned(),
                format!("unknown unit '{other}'"),
            ));
        }
    };
    // Integer parse of the numeric prefix. A '.', '+', or '-' would have ended the
    // digit run above and shown up in `unit`, producing an "unknown unit" error;
    // an explicit check keeps the message precise for the common fractional case.
    if num_part.len() != t.len() && unit_part.starts_with('.') {
        return Err(ConfigError::Size(
            s.to_owned(),
            "fractional sizes are not allowed".to_owned(),
        ));
    }
    let value: u64 = num_part
        .parse()
        .map_err(|_| ConfigError::Size(s.to_owned(), "not an integer".to_owned()))?;
    value.checked_mul(mult).ok_or_else(|| {
        ConfigError::Size(s.to_owned(), "too large (overflows u64 bytes)".to_owned())
    })
}

/// Parse a boolean config value, accepting both the Redis spellings (`yes`/`no`) and the
/// common `true`/`false`/`1`/`0` forms, case-insensitively with surrounding whitespace
/// trimmed. Returns `None` on any other token (the caller maps it to a config error).
/// Used by the `IRONCACHE_CLUSTER_ENABLED` env var.
#[must_use]
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "yes" | "true" | "1" | "on" => Some(true),
        "no" | "false" | "0" | "off" => Some(false),
        _ => None,
    }
}

/// Parse a Redis `save` directive (`"<seconds> <changes> [<seconds> <changes> ...]"`) into the
/// `(interval_secs, min_changes)` the periodic saver uses (#58 durability footgun fix). This makes
/// `CONFIG SET save` ACTUALLY update the save policy instead of silently no-op'ing.
///
/// IronCache collapses Redis's multiple save points to ONE periodic cadence, so when several points
/// are given this returns the SHORTEST-interval (most aggressive) one -- the point that fires first
/// and bounds the data-loss window (RPO) tightest. An EMPTY / whitespace-only string returns
/// `Ok(None)` (DISABLE the periodic save: only an explicit SAVE/BGSAVE persists, the Redis
/// `save ""` semantics). A malformed directive (an odd token count, a non-integer, or a zero
/// interval) returns `Err(reason)` the caller surfaces as a `CONFIG SET failed` error -- it never
/// silently accepts a value it cannot honor.
///
/// # Errors
///
/// Returns an error string when the token count is odd, a token is not a non-negative integer, or a
/// save point has a zero `seconds` (a 0-second interval is not a valid cadence; use `""` to disable).
pub fn parse_save_points(s: &str) -> Result<Option<(u64, u64)>, String> {
    let tokens: Vec<&str> = s.split_whitespace().collect();
    if tokens.is_empty() {
        // `save ""` (or all-whitespace): disable the periodic save.
        return Ok(None);
    }
    if tokens.len() % 2 != 0 {
        return Err(format!(
            "invalid save directive '{s}': expected '<seconds> <changes> [<seconds> <changes> ...]'"
        ));
    }
    let mut best: Option<(u64, u64)> = None;
    for pair in tokens.chunks_exact(2) {
        let seconds: u64 = pair[0].parse().map_err(|_| {
            format!(
                "invalid save directive '{s}': '{}' is not an integer",
                pair[0]
            )
        })?;
        let changes: u64 = pair[1].parse().map_err(|_| {
            format!(
                "invalid save directive '{s}': '{}' is not an integer",
                pair[1]
            )
        })?;
        if seconds == 0 {
            return Err(format!(
                "invalid save directive '{s}': a save point's seconds must be > 0 (use \"\" to disable)"
            ));
        }
        // Keep the SHORTEST interval (the most aggressive / tightest-RPO point).
        best = match best {
            Some((bs, _)) if bs <= seconds => best,
            _ => Some((seconds, changes)),
        };
    }
    Ok(best)
}

/// Render the runtime save policy `(interval_secs, min_changes)` back to the Redis `save` directive
/// spelling `CONFIG GET save` reports (#58 durability footgun fix). A non-zero interval renders as
/// `"<interval_secs> <min_changes>"`; a zero interval (the periodic save disabled) renders as the
/// EMPTY string, exactly how Redis reports `save` when no save points are configured. This makes
/// `CONFIG GET save` report the REAL policy instead of a fixed empty stub, so an operator can see
/// whether durability is actually on.
#[must_use]
pub fn render_save_points(interval_secs: u64, min_changes: u64) -> String {
    if interval_secs == 0 {
        String::new()
    } else {
        format!("{interval_secs} {min_changes}")
    }
}

/// WARN (never fail boot) on every `IRONCACHE_*` environment variable in `present` that `from_env`
/// does not honor (its keys are `known`), each with a "did you mean" suggestion when a close known
/// key exists. Returns the warning messages (for testability) AND emits each through
/// `tracing::warn!`. A typo'd var (e.g. `IRONCACHE_MAXCLIENT` for `IRONCACHE_MAXCLIENTS`) would
/// otherwise be SILENTLY ignored and the operator's intended setting lost; this surfaces it loudly.
/// `known` is the exact set of keys `from_env` probed (its single source of truth), so the guard can
/// never drift from what the reader accepts.
///
/// WHY warn, not hard-fail (unlike the config FILE's `deny_unknown_fields`): the environment is a
/// namespace SHARED with the OS and the orchestrator, which legitimately set `IRONCACHE_*` variables
/// that are not server config knobs (this repo's own driver-matrix harness exports `IRONCACHE_BIN`,
/// the binary path, per tests/drivers/run.sh; a systemd unit or k8s pod may export others). Aborting
/// boot on those is user-hostile and a real footgun. A config FILE is author-controlled, so an
/// unknown key there IS a mistake and still hard-fails; an env var is not, so a mistyped knob is
/// surfaced LOUDLY but never silently swallowed and never aborts an otherwise-valid boot.
///
/// Two `IRONCACHE_*` namespaces are additionally skipped entirely (not even warned):
///
/// * `IRONCACHE_CONSOLE_*` belongs to the SEPARATE `ironcache-console` binary (with its own strict
///   env validation); a shared deployment env may legitimately carry both.
/// * `IRONCACHE_BUILD_VERSION` is the compile-time release stamp (read via `option_env!` in
///   `build.rs`); a build/deploy environment that exports it at runtime is harmless.
///
/// This is the PURE core (it takes the environment key names, not the process env) so it is
/// unit-tested without mutating `std::env`; `from_env` wires in `std::env::vars()`.
fn warn_unknown_env<I, S>(present: I, known: &[&str]) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut warnings = Vec::new();
    for key in present {
        let key = key.as_ref();
        if !key.starts_with("IRONCACHE_") {
            continue;
        }
        if key.starts_with("IRONCACHE_CONSOLE_") || key == "IRONCACHE_BUILD_VERSION" {
            continue;
        }
        if known.contains(&key) {
            continue;
        }
        let hint = nearest_env_key(key, known)
            .map(|s| format!(" (did you mean {s}?)"))
            .unwrap_or_default();
        let msg = format!(
            "ignoring unknown IRONCACHE_* environment variable {key}{hint}; it is not a server \
             config knob. If it is a typo, fix or unset it so your intended setting is not lost; if \
             your tooling sets it intentionally, this warning is harmless"
        );
        tracing::warn!(env_var = key, "{msg}");
        warnings.push(msg);
    }
    warnings
}

/// The nearest key in `known` to `key` by Levenshtein edit distance, returned ONLY when it is a
/// plausible typo (distance <= 3 AND strictly less than the key length, so an unrelated short key
/// is not "suggested" for a long typo). Used to add a "did you mean" hint to the unknown-env error.
/// Pure and small (the known set is tiny), so the quadratic DP cost is irrelevant at boot.
fn nearest_env_key<'a>(key: &str, known: &[&'a str]) -> Option<&'a str> {
    let mut best: Option<(&'a str, usize)> = None;
    for &cand in known {
        let d = levenshtein(key, cand);
        if best.is_none_or(|(_, bd)| d < bd) {
            best = Some((cand, d));
        }
    }
    best.and_then(|(cand, d)| (d <= 3 && d < key.len()).then_some(cand))
}

/// Levenshtein edit distance between two byte strings (ASCII env-var names), a standard two-row
/// dynamic program. Pure helper for [`nearest_env_key`]'s "did you mean" suggestion.
fn levenshtein(a: &str, b: &str) -> usize {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur: Vec<usize> = vec![0; b.len() + 1];
    for (i, &ac) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, &bc) in b.iter().enumerate() {
            let cost = usize::from(ac != bc);
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = Config::default();
        assert_eq!(c.port, 6379);
        assert!(c.shards >= 1);
        assert_eq!(c.databases, 16);
        assert_eq!(c.maxmemory, 0);
        // ADR-0007: the cache-mode default is eviction-ON, not noeviction.
        assert_eq!(c.maxmemory_policy, "allkeys-lru");
        assert_ne!(c.maxmemory_policy, "noeviction");
        assert!(c.requirepass.is_none());
        // Cluster mode is OFF by default (standalone, Redis `cluster-enabled no`).
        assert!(!c.cluster_enabled);
        // HA-8 knobs default to the documented constants (meaningful only in raft-mode).
        assert_eq!(c.replica_max_lag, DEFAULT_REPLICA_MAX_LAG);
        assert_eq!(c.failover_timeout_secs, DEFAULT_FAILOVER_TIMEOUT_SECS);
        // The WRITE-SIDE guardrail is DISABLED by default (0 = the Redis default): the write hot
        // path is byte-unchanged. The lag bound carries a sane default (only read when enabled).
        assert_eq!(c.min_replicas_to_write, 0);
        assert_eq!(c.min_replicas_max_lag, DEFAULT_MIN_REPLICAS_MAX_LAG);
        // HA-3c compaction threshold defaults to the NON-ZERO production cadence so a real
        // raft-mode node actually compacts (the engine's own default is 0 = disabled).
        assert_eq!(c.raft_snapshot_threshold, DEFAULT_RAFT_SNAPSHOT_THRESHOLD);
        assert_ne!(c.raft_snapshot_threshold, 0);
        // PROD-9 chunked InstallSnapshot: the chunk size defaults to 256 KiB, well under the bus
        // frame bound (a pure framing knob; any value installs a byte-identical snapshot).
        assert_eq!(
            c.raft_snapshot_chunk_bytes,
            DEFAULT_RAFT_SNAPSHOT_CHUNK_BYTES
        );
        assert_eq!(c.raft_snapshot_chunk_bytes, 256 * 1024);
        // CONNECTION SAFETY ceilings (PROD-SAFETY #3/#4/#5): the idle timeout is OFF by default
        // (Redis default 0, byte-unchanged), `maxclients` defaults to the Redis 10000 ceiling (an
        // unconfigured node is protected from connection exhaustion), and the output-buffer cap
        // defaults to the high 1 GiB ceiling (a legitimate large reply is unaffected; a pathological
        // accumulation is bounded). All are non-restrictive enough to leave the default hot path /
        // legitimate workload unchanged while closing the DoS gaps.
        assert_eq!(c.timeout_secs, 0);
        assert_eq!(c.maxclients, DEFAULT_MAXCLIENTS);
        assert_eq!(c.maxclients, 10_000);
        assert_eq!(c.output_buffer_limit, DEFAULT_OUTPUT_BUFFER_LIMIT);
        assert_eq!(c.output_buffer_limit, 1024 * 1024 * 1024);
        // The query-buffer cap (#528) mirrors the output cap: a high 1 GiB ceiling by default so a
        // legitimate large request / deep pipeline is unaffected while a slow-dribble multibulk is
        // bounded.
        assert_eq!(c.query_buffer_limit, DEFAULT_QUERY_BUFFER_LIMIT);
        assert_eq!(c.query_buffer_limit, 1024 * 1024 * 1024);
        // No data directory by default: the Raft log lands under the OS temp dir (unchanged).
        assert!(c.data_dir.is_none());
        // PERSISTENCE save policy is OFF by default (#58): no periodic save timer in the default
        // posture (only an explicit SAVE/BGSAVE, and only when a data_dir is set).
        assert_eq!(c.save_interval_secs, 0);
        assert_eq!(c.save_min_changes, 0);
        // INTRA-CLUSTER transport security is OFF by default (PROD-3): plaintext bus + repl,
        // byte-unchanged. No cert/key/CA, no secret.
        assert_eq!(c.cluster_tls, TlsMode::Off);
        assert!(c.cluster_tls_cert_path.is_none());
        assert!(c.cluster_tls_key_path.is_none());
        assert!(c.cluster_ca_path.is_none());
        assert!(c.cluster_secret.is_none());
        c.validate().unwrap();
    }

    #[test]
    fn cluster_transport_knobs_parse_and_validate() {
        // cluster_tls = on REQUIRES a readable cert + key + a secret: a missing secret is a clear
        // error (TLS encrypts but does not authenticate the peer without the shared secret).
        let mut overlay = ConfigOverlay {
            cluster_tls: Some(TlsMode::On),
            cluster_tls_cert_path: Some(PathBuf::from("/nonexistent/cert.pem")),
            cluster_tls_key_path: Some(PathBuf::from("/nonexistent/key.pem")),
            ..Default::default()
        };
        // No secret -> error naming cluster_secret.
        let err = Config::resolve(&[overlay.clone()])
            .unwrap()
            .validate()
            .expect_err("cluster_tls = on without a secret must error");
        assert!(format!("{err}").contains("cluster_secret"), "got {err}");

        // With a secret but an UNREADABLE cert -> error naming the cert path (the readability
        // pre-flight). The insecure flag is set so the CA-required check (PROD-3 MITM fix) does not
        // pre-empt the cert-readability check we are exercising here.
        overlay.cluster_secret = Some("supersecret".to_owned());
        overlay.cluster_tls_insecure_skip_verify = Some(true);
        let err = Config::resolve(&[overlay.clone()])
            .unwrap()
            .validate()
            .expect_err("an unreadable cluster cert must error");
        assert!(
            format!("{err}").contains("cluster_tls_cert_path"),
            "got {err}"
        );

        // An EMPTY secret is rejected regardless of TLS (no authentication; almost certainly a typo
        // for unset).
        let err = Config::resolve(&[ConfigOverlay {
            cluster_secret: Some(String::new()),
            ..Default::default()
        }])
        .unwrap()
        .validate()
        .expect_err("an empty cluster_secret must error");
        assert!(format!("{err}").contains("cluster_secret"), "got {err}");

        // A CA without TLS is rejected (it would silently do nothing).
        let err = Config::resolve(&[ConfigOverlay {
            cluster_ca_path: Some(PathBuf::from("/some/ca.pem")),
            ..Default::default()
        }])
        .unwrap()
        .validate()
        .expect_err("a cluster_ca_path without cluster_tls = on must error");
        assert!(format!("{err}").contains("cluster_ca_path"), "got {err}");

        // A bare secret WITHOUT TLS is allowed (plaintext-but-authenticated bus): validate passes.
        let c = Config::resolve(&[ConfigOverlay {
            cluster_secret: Some("token".to_owned()),
            ..Default::default()
        }])
        .unwrap();
        c.validate()
            .expect("a secret-only (no TLS) cluster is allowed");
        assert_eq!(c.cluster_secret.as_deref(), Some("token"));
        assert_eq!(c.cluster_tls, TlsMode::Off);
    }

    /// Write `contents` to a uniquely-named temp file (deterministic name from pid + counter, no
    /// rand) and return the path, for the cluster-TLS validate tests (which only check that the
    /// configured cert/key/CA paths are READABLE, not their PEM contents).
    fn temp_file(tag: &str, contents: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ironcache-cfg-test-{tag}-{}-{n}.pem",
            std::process::id()
        ));
        std::fs::write(&path, contents).expect("write temp file");
        path
    }

    #[test]
    fn cluster_tls_requires_a_ca_unless_insecure_flag_is_set() {
        // SECURITY (PROD-3 MITM fix): cluster_tls = on must REQUIRE a cluster_ca_path so the dial
        // VERIFIES the peer cert (otherwise the cluster_secret leaks to an active MITM). The ONLY
        // escape is the explicit cluster_tls_insecure_skip_verify = true.
        let cert = temp_file("cert", "test-cert");
        let key = temp_file("key", "test-key");
        let ca = temp_file("ca", "test-ca");

        let base = ConfigOverlay {
            cluster_tls: Some(TlsMode::On),
            cluster_tls_cert_path: Some(cert.clone()),
            cluster_tls_key_path: Some(key.clone()),
            cluster_secret: Some("supersecret".to_owned()),
            ..Default::default()
        };

        // (i) tls-on, secret set, readable cert+key, but NO CA and NO insecure flag -> REJECTED,
        // naming cluster_ca_path, with the MITM rationale in the message.
        let err = Config::resolve(std::slice::from_ref(&base))
            .unwrap()
            .validate()
            .expect_err("cluster_tls = on without a CA and without the insecure flag must error");
        let msg = format!("{err}");
        assert!(msg.contains("cluster_ca_path"), "got {msg}");
        assert!(
            msg.contains("MITM"),
            "the error must explain the MITM risk: {msg}"
        );

        // With a readable CA -> validate PASSES (the verifying posture is satisfied).
        let with_ca = ConfigOverlay {
            cluster_ca_path: Some(ca.clone()),
            ..base.clone()
        };
        Config::resolve(&[with_ca])
            .unwrap()
            .validate()
            .expect("cluster_tls = on WITH a readable CA must pass (the verifying posture)");

        // (ii) the EXPLICIT insecure opt-out lets tls-on-without-CA pass (encrypted-but-unverified).
        let insecure = ConfigOverlay {
            cluster_tls_insecure_skip_verify: Some(true),
            ..base.clone()
        };
        let c = Config::resolve(&[insecure]).unwrap();
        c.validate()
            .expect("the explicit insecure opt-out must allow tls-on without a CA");
        assert!(c.cluster_tls_insecure_skip_verify);

        let _ = std::fs::remove_file(&cert);
        let _ = std::fs::remove_file(&key);
        let _ = std::fs::remove_file(&ca);
    }

    #[test]
    fn cluster_tls_parses_from_env_token() {
        // The IRONCACHE_CLUSTER_TLS env token is parsed off/on case-insensitively (mirrors the
        // public `tls` knob); an unrecognized token hard-fails. Exercised through parse_tls_mode
        // directly (env reads share global state across tests, so assert the parser, not getenv).
        assert_eq!(parse_tls_mode("on"), Some(TlsMode::On));
        assert_eq!(parse_tls_mode("OFF"), Some(TlsMode::Off));
        assert_eq!(parse_tls_mode("garbage"), None);
    }

    #[test]
    fn runtime_backend_parses_tokens_and_defaults() {
        // The `--runtime` / IRONCACHE_RUNTIME token (PROD-10 / #28): tokio + io_uring (with the
        // friendly aliases), case-insensitive + trimmed; an unrecognized token is None (the caller
        // maps it to a boot error). Exercised through the parser directly (env reads share global
        // state across tests, so assert the parser, not getenv -- mirrors the cluster-tls test).
        assert_eq!(parse_runtime_backend("tokio"), Some(RuntimeBackend::Tokio));
        assert_eq!(
            parse_runtime_backend("  TOKIO "),
            Some(RuntimeBackend::Tokio)
        );
        for t in ["io_uring", "io-uring", "iouring", "URING"] {
            assert_eq!(
                parse_runtime_backend(t),
                Some(RuntimeBackend::IoUring),
                "{t:?} -> io_uring"
            );
        }
        assert_eq!(parse_runtime_backend("epoll"), None);
        assert_eq!(parse_runtime_backend(""), None);
        // The default backend is tokio (the portable, byte-unchanged path).
        assert_eq!(Config::default().runtime, RuntimeBackend::Tokio);
    }

    #[test]
    fn runtime_backend_resolves_from_overlay_and_toml() {
        // An overlay that sets `runtime` folds into the resolved Config; an unset overlay leaves
        // the tokio default showing through (so the default boot is byte-unchanged).
        let c = Config::resolve(&[ConfigOverlay {
            runtime: Some(RuntimeBackend::IoUring),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(c.runtime, RuntimeBackend::IoUring);
        let d = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert_eq!(d.runtime, RuntimeBackend::Tokio);
        // TOML deserializes `runtime = "io_uring"` directly (snake_case serde on RuntimeBackend).
        let o = ConfigOverlay::from_toml_str("runtime = \"io_uring\"").unwrap();
        assert_eq!(o.runtime, Some(RuntimeBackend::IoUring));
        let o2 = ConfigOverlay::from_toml_str("runtime = \"tokio\"").unwrap();
        assert_eq!(o2.runtime, Some(RuntimeBackend::Tokio));
    }

    #[test]
    fn save_policy_parses_from_overlay_toml_and_env() {
        // The overlay sets the periodic save policy; an unset overlay leaves the defaults (0/0).
        let c = Config::resolve(&[ConfigOverlay {
            save_interval_secs: Some(900),
            save_min_changes: Some(1),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(c.save_interval_secs, 900);
        assert_eq!(c.save_min_changes, 1);

        // Unset -> the disabled defaults (byte-unchanged posture).
        let unset = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert_eq!(unset.save_interval_secs, 0);
        assert_eq!(unset.save_min_changes, 0);

        // TOML form: two scalars deserialize into the Option<u64> overlay fields.
        let o = ConfigOverlay::from_toml_str("save_interval_secs = 300\nsave_min_changes = 10\n")
            .unwrap();
        assert_eq!(o.save_interval_secs, Some(300));
        assert_eq!(o.save_min_changes, Some(10));
    }

    #[test]
    fn refuse_empty_start_on_version_mismatch_parses_and_defaults_off() {
        // The fail-closed snapshot-version knob (#530) folds in from an overlay; an unset overlay
        // leaves the default `false` (log loudly + start empty, byte-unchanged boot posture).
        let on = Config::resolve(&[ConfigOverlay {
            refuse_empty_start_on_version_mismatch: Some(true),
            ..Default::default()
        }])
        .unwrap();
        assert!(on.refuse_empty_start_on_version_mismatch);
        let off = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert!(!off.refuse_empty_start_on_version_mismatch);
        // TOML deserializes the boolean directly into the Option<bool> overlay field.
        let o = ConfigOverlay::from_toml_str("refuse_empty_start_on_version_mismatch = true\n")
            .unwrap();
        assert_eq!(o.refuse_empty_start_on_version_mismatch, Some(true));
    }

    #[test]
    fn data_dir_parses_from_overlay_and_toml_and_defaults_none() {
        // The overlay sets the durable data directory; an unset overlay leaves the default None.
        let c = Config::resolve(&[ConfigOverlay {
            data_dir: Some(PathBuf::from("/var/lib/ironcache")),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(
            c.data_dir.as_deref(),
            Some(std::path::Path::new("/var/lib/ironcache"))
        );
        c.validate().expect("a non-empty data_dir validates");

        // Unset -> None (the byte-unchanged temp-dir default).
        let unset = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert!(unset.data_dir.is_none());

        // TOML form: a string path deserializes into the Option<PathBuf>.
        let o = ConfigOverlay::from_toml_str("data_dir = \"/srv/ironcache/data\"\n").unwrap();
        assert_eq!(
            o.data_dir.as_deref(),
            Some(std::path::Path::new("/srv/ironcache/data"))
        );

        // An empty data_dir is rejected by validate (a likely operator mistake, not durable).
        let empty = Config {
            data_dir: Some(PathBuf::new()),
            ..Config::default()
        };
        assert!(matches!(
            empty.validate().unwrap_err(),
            ConfigError::Invalid {
                field: "data_dir",
                ..
            }
        ));
    }

    #[test]
    fn ha8_knobs_parse_from_overlay_and_toml() {
        // The overlay sets the HA-8 knobs; an unset overlay leaves the defaults.
        let c = Config::resolve(&[ConfigOverlay {
            replica_max_lag: Some(1024),
            failover_timeout_secs: Some(12),
            raft_snapshot_threshold: Some(256),
            raft_snapshot_chunk_bytes: Some(64 * 1024),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(c.replica_max_lag, 1024);
        assert_eq!(c.failover_timeout_secs, 12);
        assert_eq!(c.raft_snapshot_threshold, 256);
        assert_eq!(c.raft_snapshot_chunk_bytes, 64 * 1024);
        let unset = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert_eq!(unset.replica_max_lag, DEFAULT_REPLICA_MAX_LAG);
        assert_eq!(unset.failover_timeout_secs, DEFAULT_FAILOVER_TIMEOUT_SECS);
        assert_eq!(
            unset.raft_snapshot_threshold,
            DEFAULT_RAFT_SNAPSHOT_THRESHOLD
        );
        assert_eq!(
            unset.raft_snapshot_chunk_bytes,
            DEFAULT_RAFT_SNAPSHOT_CHUNK_BYTES
        );
        // An explicit 0 disables compaction (the pre-3c unbounded-log behaviour).
        let disabled = Config::resolve(&[ConfigOverlay {
            raft_snapshot_threshold: Some(0),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(disabled.raft_snapshot_threshold, 0);
        // TOML form.
        let o = ConfigOverlay::from_toml_str(
            "replica_max_lag = 64\nfailover_timeout_secs = 3\nraft_snapshot_threshold = 2048\n\
             raft_snapshot_chunk_bytes = 131072\n",
        )
        .unwrap();
        assert_eq!(o.replica_max_lag, Some(64));
        assert_eq!(o.failover_timeout_secs, Some(3));
        assert_eq!(o.raft_snapshot_threshold, Some(2048));
        assert_eq!(o.raft_snapshot_chunk_bytes, Some(131_072));
    }

    #[test]
    fn min_replicas_to_write_knobs_parse_and_default_disabled() {
        // DEFAULT: the write-side guardrail is DISABLED (0); the lag bound carries the sane
        // default. An unset overlay leaves both at their defaults (write hot path byte-unchanged).
        let unset = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert_eq!(unset.min_replicas_to_write, 0);
        assert_eq!(unset.min_replicas_max_lag, DEFAULT_MIN_REPLICAS_MAX_LAG);

        // The overlay enables + tunes the guardrail.
        let c = Config::resolve(&[ConfigOverlay {
            min_replicas_to_write: Some(2),
            min_replicas_max_lag: Some(32),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(c.min_replicas_to_write, 2);
        assert_eq!(c.min_replicas_max_lag, 32);
        c.validate().expect("an enabled guardrail validates");

        // TOML form.
        let o =
            ConfigOverlay::from_toml_str("min_replicas_to_write = 1\nmin_replicas_max_lag = 5\n")
                .unwrap();
        assert_eq!(o.min_replicas_to_write, Some(1));
        assert_eq!(o.min_replicas_max_lag, Some(5));
    }

    #[test]
    fn tls_defaults_off_and_parses_off_or_on() {
        // DEFAULT is Off (plaintext, byte-unchanged): no cert/key required, validate passes.
        let c = Config::default();
        assert_eq!(c.tls, TlsMode::Off);
        assert_eq!(TlsMode::default(), TlsMode::Off);
        assert!(c.tls_cert_path.is_none());
        assert!(c.tls_key_path.is_none());
        c.validate().expect("tls off needs no cert/key");

        // The overlay sets the mode + paths; an unset overlay leaves the default Off.
        let on = Config::resolve(&[ConfigOverlay {
            tls: Some(TlsMode::On),
            tls_cert_path: Some(PathBuf::from("/etc/ssl/cert.pem")),
            tls_key_path: Some(PathBuf::from("/etc/ssl/key.pem")),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(on.tls, TlsMode::On);
        assert_eq!(
            on.tls_cert_path.as_deref(),
            Some(std::path::Path::new("/etc/ssl/cert.pem"))
        );
        let unset = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert_eq!(unset.tls, TlsMode::Off);

        // parse_tls_mode accepts off/on (+ no/yes/true/false/0/1), trimmed + case-insensitive.
        for t in ["off", "OFF", " no ", "false", "0"] {
            assert_eq!(parse_tls_mode(t), Some(TlsMode::Off), "{t:?} -> off");
        }
        for t in ["on", "ON", " yes ", "true", "1"] {
            assert_eq!(parse_tls_mode(t), Some(TlsMode::On), "{t:?} -> on");
        }
        assert_eq!(parse_tls_mode("maybe"), None);
        assert_eq!(parse_tls_mode(""), None);

        // TOML deserializes the lowercase-renamed enum directly.
        let o = ConfigOverlay::from_toml_str(
            "tls = \"on\"\ntls_cert_path = \"/c.pem\"\ntls_key_path = \"/k.pem\"\n",
        )
        .unwrap();
        assert_eq!(o.tls, Some(TlsMode::On));
        assert_eq!(
            o.tls_cert_path.as_deref(),
            Some(std::path::Path::new("/c.pem"))
        );
        assert_eq!(
            o.tls_key_path.as_deref(),
            Some(std::path::Path::new("/k.pem"))
        );
    }

    #[test]
    fn tls_on_without_cert_or_key_fails_validate() {
        // tls = on with NO cert path -> a precise field error.
        let no_cert = Config {
            tls: TlsMode::On,
            ..Config::default()
        };
        assert!(matches!(
            no_cert.validate().unwrap_err(),
            ConfigError::Invalid {
                field: "tls_cert_path",
                ..
            }
        ));

        // A guaranteed-readable file (this crate's own Cargo.toml), absolute via the manifest dir
        // so it resolves regardless of the test's CWD. Used where the readability pre-flight must
        // PASS so we reach the next check.
        let readable_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");

        // tls = on with a cert but NO key -> the key field error (use a path that exists so the
        // cert readability check passes and we reach the key check).
        let with_cert_no_key = Config {
            tls: TlsMode::On,
            tls_cert_path: Some(readable_path.clone()),
            ..Config::default()
        };
        assert!(matches!(
            with_cert_no_key.validate().unwrap_err(),
            ConfigError::Invalid {
                field: "tls_key_path",
                ..
            }
        ));

        // tls = on with cert + key paths that do NOT exist -> a readability error on the cert.
        let unreadable = Config {
            tls: TlsMode::On,
            tls_cert_path: Some(PathBuf::from("/nonexistent/cert.pem")),
            tls_key_path: Some(PathBuf::from("/nonexistent/key.pem")),
            ..Config::default()
        };
        let err = unreadable.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Invalid {
                field: "tls_cert_path",
                ..
            }
        ));
        assert!(err.to_string().contains("cannot read"), "reason: {err}");

        // tls = on with cert + key that DO exist (use a real file for both) validates: the
        // readability pre-flight only opens the files, it does not parse the PEM (that is the
        // runtime layer's job at boot).
        let readable = Config {
            tls: TlsMode::On,
            tls_cert_path: Some(readable_path.clone()),
            tls_key_path: Some(readable_path),
            ..Config::default()
        };
        readable
            .validate()
            .expect("tls on with readable cert+key paths passes the pre-flight");
    }

    #[test]
    fn cluster_mode_defaults_static_and_parses_static_or_raft() {
        // DEFAULT is Static (the pre-HA-4c, byte-unchanged behavior).
        assert_eq!(Config::default().cluster_mode, ClusterMode::Static);
        assert_eq!(ClusterMode::default(), ClusterMode::Static);

        // The overlay sets the mode; an unset overlay leaves the default Static.
        let raft = Config::resolve(&[ConfigOverlay {
            cluster_mode: Some(ClusterMode::Raft),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(raft.cluster_mode, ClusterMode::Raft);
        let unset = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert_eq!(unset.cluster_mode, ClusterMode::Static);

        // parse_cluster_mode accepts static/raft (case-insensitive, trimmed) and rejects else.
        assert_eq!(parse_cluster_mode("static"), Some(ClusterMode::Static));
        assert_eq!(parse_cluster_mode(" RAFT "), Some(ClusterMode::Raft));
        assert_eq!(parse_cluster_mode("gossip"), None);
        assert_eq!(parse_cluster_mode(""), None);

        // TOML deserializes the lowercase-renamed enum directly.
        let o = ConfigOverlay::from_toml_str("cluster_mode = \"raft\"").unwrap();
        assert_eq!(o.cluster_mode, Some(ClusterMode::Raft));

        // #517: shard-owners mode parses from both spellings via `parse_cluster_mode` (env/CLI)...
        assert_eq!(
            parse_cluster_mode("shard-owners"),
            Some(ClusterMode::ShardOwners)
        );
        assert_eq!(
            parse_cluster_mode(" ShardOwners "),
            Some(ClusterMode::ShardOwners)
        );
        // ...AND both spellings via TOML/serde (the `rename` + `alias` cover the hyphen + no-hyphen).
        for toml in [
            "cluster_mode = \"shard-owners\"",
            "cluster_mode = \"shardowners\"",
        ] {
            let o = ConfigOverlay::from_toml_str(toml).unwrap();
            assert_eq!(o.cluster_mode, Some(ClusterMode::ShardOwners), "{toml}");
        }
    }

    #[test]
    fn shard_owners_mode_requires_cluster_enabled_and_rejects_a_topology() {
        // #517: ShardOwners requires cluster_enabled (it serves CLUSTER SLOTS + MOVED).
        // `validate` (not `resolve`, which does not validate) enforces the cross-field rules.
        let no_enable = Config {
            cluster_mode: ClusterMode::ShardOwners,
            cluster_enabled: false,
            ..Config::default()
        };
        assert!(
            no_enable.validate().is_err(),
            "shard-owners without cluster_enabled must be rejected"
        );

        // With cluster_enabled and NO topology, it validates (owners derive from the shard count).
        let ok = Config {
            cluster_mode: ClusterMode::ShardOwners,
            cluster_enabled: true,
            shards: 4,
            ..Config::default()
        };
        assert!(
            ok.validate().is_ok(),
            "shard-owners + cluster_enabled + no topology must validate"
        );

        // A cluster_topology alongside ShardOwners is a config conflict (owners derive from shards).
        let with_topo = Config {
            cluster_mode: ClusterMode::ShardOwners,
            cluster_enabled: true,
            cluster_announce_id: Some("1".repeat(40)),
            cluster_topology: Some(ClusterTopology {
                nodes: vec![ClusterNode {
                    id: "1".repeat(40),
                    host: "127.0.0.1".to_owned(),
                    port: 6379,
                    slots: vec![[0, 16383]],
                }],
            }),
            ..Config::default()
        };
        assert!(
            with_topo.validate().is_err(),
            "shard-owners + a cluster_topology must be rejected"
        );

        // #517 PR3: shard-owners is rejected with the io_uring runtime (per-shard listeners are a
        // tokio-only follow-up there); the tokio runtime (the default) is accepted.
        let with_uring = Config {
            cluster_mode: ClusterMode::ShardOwners,
            cluster_enabled: true,
            runtime: RuntimeBackend::IoUring,
            ..Config::default()
        };
        assert!(
            with_uring.validate().is_err(),
            "shard-owners + io_uring must be rejected"
        );
        let with_tokio = Config {
            cluster_mode: ClusterMode::ShardOwners,
            cluster_enabled: true,
            runtime: RuntimeBackend::Tokio,
            ..Config::default()
        };
        assert!(
            with_tokio.validate().is_ok(),
            "shard-owners + tokio must be accepted"
        );

        // #517 PR4 review: the per-shard port block `port .. port + shards - 1` must fit u16 -- a
        // clean config error, not a boot panic. 65500 + 100 shards overflows; 65500 + 4 fits (top
        // port 65503); the exact boundary base 65535 - (shards-1) also fits.
        let overflow = Config {
            cluster_mode: ClusterMode::ShardOwners,
            cluster_enabled: true,
            port: 65500,
            shards: 100,
            ..Config::default()
        };
        assert!(
            overflow.validate().is_err(),
            "shard-owners port block overflowing u16 must be rejected at validate"
        );
        let fits = Config {
            cluster_mode: ClusterMode::ShardOwners,
            cluster_enabled: true,
            port: 65532,
            shards: 4,
            ..Config::default()
        };
        assert!(
            fits.validate().is_ok(),
            "shard-owners port block ending exactly at 65535 must be accepted"
        );
    }

    #[test]
    fn raft_mode_topology_ignores_empty_slot_ranges_but_enforces_identity() {
        // In RAFT mode the topology supplies only the voter set, so EMPTY slot ranges validate
        // (the static-completeness rule does NOT apply): the would-be "gap" is ignored.
        let cfg = Config {
            cluster_mode: ClusterMode::Raft,
            ..topology_config(
                true,
                Some("0000000000000000000000000000000000000000"),
                &[
                    ("0000000000000000000000000000000000000000", &[]),
                    ("1111111111111111111111111111111111111111", &[]),
                ],
            )
        };
        cfg.validate()
            .expect("raft-mode topology with empty slot ranges should validate");

        // But identity rules still hold: a missing self announce id is rejected.
        let bad_self = Config {
            cluster_mode: ClusterMode::Raft,
            ..topology_config(
                true,
                Some("9999999999999999999999999999999999999999"),
                &[("0000000000000000000000000000000000000000", &[])],
            )
        };
        assert!(matches!(
            bad_self.validate().unwrap_err(),
            ConfigError::Invalid {
                field: "cluster-announce-id",
                ..
            }
        ));

        // The SAME topology under STATIC mode is rejected (empty ranges == a gap).
        let static_cfg = Config {
            cluster_mode: ClusterMode::Static,
            ..topology_config(
                true,
                Some("0000000000000000000000000000000000000000"),
                &[("0000000000000000000000000000000000000000", &[])],
            )
        };
        assert!(matches!(
            static_cfg.validate().unwrap_err(),
            ConfigError::Invalid {
                field: "cluster_topology",
                ..
            }
        ));
    }

    /// F2: a raft-mode topology with two DISTINCT announce ids that share their FIRST 16 hex digits
    /// (so they pass the full-40-hex uniqueness check) is REJECTED, because the engine derives the
    /// `NodeId` from only the top 64 bits / first 16 hex digits -- the two ids would COLLIDE to one
    /// Raft identity. A topology whose ids differ within their first 16 hex digits validates.
    #[test]
    fn raft_topology_rejects_derived_node_id_collision() {
        // Two ids identical in their first 16 hex ("aaaaaaaaaaaaaaaa", 16 chars) but different
        // afterwards: the full 40-hex ids are distinct, yet both derive NodeId(0xaaaa_..._aaaa).
        let empty_slots: &[[u16; 2]] = &[];
        let collide_a = "aaaaaaaaaaaaaaaa000000000000000000000000"; // 16 a + 24 zeros = 40
        let collide_b = "aaaaaaaaaaaaaaaa111111111111111111111111"; // 16 a + 24 ones = 40
        let colliding = Config {
            cluster_mode: ClusterMode::Raft,
            ..topology_config(
                true,
                Some(collide_a),
                &[(collide_a, empty_slots), (collide_b, empty_slots)],
            )
        };
        let err = colliding
            .validate()
            .expect_err("two ids sharing their first 16 hex must be rejected");
        assert!(
            matches!(&err, ConfigError::Invalid { field: "cluster_topology", reason }
                if reason.contains("same raft NodeId")),
            "got {err:?}"
        );

        // Ids that DIFFER within their first 16 hex digits validate (no collision): a shared SUFFIX
        // is fine. Here the 16th hex char differs (0 vs 1).
        let ok_a = "aaaaaaaaaaaaaaa0ffffffffffffffffffffffff"; // 15 a + 0 + 24 f = 40
        let ok_b = "aaaaaaaaaaaaaaa1ffffffffffffffffffffffff"; // 15 a + 1 + 24 f = 40
        let ok = Config {
            cluster_mode: ClusterMode::Raft,
            ..topology_config(
                true,
                Some(ok_a),
                &[(ok_a, empty_slots), (ok_b, empty_slots)],
            )
        };
        ok.validate()
            .expect("ids differing in their first 16 hex digits should validate");
    }

    /// k8s StatefulSet support: a raft-mode topology whose node `host`s are DNS HOSTNAMES (per-pod
    /// StatefulSet names) validates -- `validate_raft_topology` enforces only node identity, never
    /// that the host is an IP literal (resolution is deferred to the dial path). This is the
    /// prerequisite for a hostname-addressed cluster to even pass config; the old peer-parsing then
    /// silently dropped such hosts at boot, which the dial-path change fixes.
    #[test]
    fn raft_topology_accepts_dns_hostname_hosts() {
        let topo = ClusterTopology {
            nodes: vec![
                ClusterNode {
                    id: "0000000000000000000000000000000000000000".to_owned(),
                    host: "ironcache-0.ironcache.default.svc.cluster.local".to_owned(),
                    port: 6379,
                    slots: vec![],
                },
                ClusterNode {
                    id: "1111111111111111111111111111111111111111".to_owned(),
                    host: "ironcache-1.ironcache.default.svc.cluster.local".to_owned(),
                    port: 6379,
                    slots: vec![],
                },
            ],
        };
        validate_raft_topology(&topo, "0000000000000000000000000000000000000000")
            .expect("a raft-mode topology addressed by per-pod DNS hostnames must validate");
    }

    #[test]
    fn cluster_enabled_overlay_and_parse_bool() {
        // The overlay sets the boot-only cluster flag; default is false.
        let on = Config::resolve(&[ConfigOverlay {
            cluster_enabled: Some(true),
            ..Default::default()
        }])
        .unwrap();
        assert!(on.cluster_enabled);
        let off = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert!(!off.cluster_enabled);
        // parse_bool accepts the Redis yes/no spellings plus true/false/1/0/on/off
        // (case-insensitive, trimmed) and rejects anything else.
        for t in ["yes", "TRUE", " 1 ", "on"] {
            assert_eq!(parse_bool(t), Some(true), "{t:?} should be true");
        }
        for f in ["no", "False", "0", "OFF"] {
            assert_eq!(parse_bool(f), Some(false), "{f:?} should be false");
        }
        assert_eq!(parse_bool("maybe"), None);
        assert_eq!(parse_bool(""), None);
    }

    #[test]
    fn maxmemory_policy_overlay_and_validation() {
        // A valid Redis name resolves and validates (case-insensitive).
        for good in [
            "noeviction",
            "allkeys-lfu",
            "VOLATILE-TTL",
            "volatile-random",
        ] {
            let o = ConfigOverlay {
                maxmemory_policy: Some(good.to_owned()),
                ..Default::default()
            };
            let cfg = Config::resolve(&[o]).unwrap();
            assert_eq!(cfg.maxmemory_policy, good);
            cfg.validate()
                .expect("valid policy name should pass validate");
        }
        // An unknown name resolves (the layer just sets a string) but FAILS validate.
        let o = ConfigOverlay {
            maxmemory_policy: Some("allkeys-ttl".to_owned()),
            ..Default::default()
        };
        let cfg = Config::resolve(&[o]).unwrap();
        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::Invalid {
                field: "maxmemory-policy",
                ..
            })
        ));
    }

    #[test]
    fn precedence_cli_over_env_over_file() {
        let file = ConfigOverlay {
            port: Some(1111),
            shards: Some(2),
            ..Default::default()
        };
        let env = ConfigOverlay {
            port: Some(2222),
            ..Default::default()
        };
        let cli = ConfigOverlay {
            port: Some(3333),
            ..Default::default()
        };
        let cfg = Config::resolve(&[file, env, cli]).unwrap();
        // CLI wins on port.
        assert_eq!(cfg.port, 3333);
        // shards only set by file -> shows through.
        assert_eq!(cfg.shards, 2);
    }

    #[test]
    fn toml_parse_roundtrip() {
        let toml_src = r#"
            port = 7000
            shards = 4
            maxmemory = "256mb"
            requirepass = "secret"
        "#;
        let o = ConfigOverlay::from_toml_str(toml_src).unwrap();
        let cfg = Config::resolve(&[o]).unwrap();
        assert_eq!(cfg.port, 7000);
        assert_eq!(cfg.shards, 4);
        assert_eq!(cfg.maxmemory, 256 * 1024 * 1024);
        // SECURITY (#65): requirepass is stored as the SHA-256 HEX of the plaintext AT
        // REST, never the plaintext "secret" itself.
        assert_eq!(
            cfg.requirepass.as_deref(),
            Some(sha256_hex(b"secret").as_str())
        );
        assert_ne!(cfg.requirepass.as_deref(), Some("secret"));
    }

    #[test]
    fn requirepass_is_hashed_at_rest_not_plaintext() {
        // SECURITY (#65): the boot input is plaintext (TOML/env/CLI), but after resolve
        // the long-lived Config holds ONLY the SHA-256 hex digest, never the plaintext.
        let o = ConfigOverlay {
            requirepass: Some("hunter2".to_owned()),
            ..Default::default()
        };
        let cfg = Config::resolve(&[o]).unwrap();
        let stored = cfg
            .requirepass
            .as_deref()
            .expect("requirepass should be set");
        assert_eq!(stored, sha256_hex(b"hunter2"));
        assert_eq!(stored.len(), 64);
        // The plaintext must NOT be retained anywhere in the stored credential.
        assert_ne!(stored, "hunter2");
        assert!(!stored.contains("hunter2"));
    }

    #[test]
    fn empty_requirepass_resolves_to_none() {
        // An explicit empty requirepass disables auth (None), and is never hashed into a
        // bogus credential.
        let o = ConfigOverlay {
            requirepass: Some(String::new()),
            ..Default::default()
        };
        let cfg = Config::resolve(&[o]).unwrap();
        assert!(cfg.requirepass.is_none());
        // An unset requirepass stays None too.
        let cfg2 = Config::resolve(&[ConfigOverlay::default()]).unwrap();
        assert!(cfg2.requirepass.is_none());
    }

    #[test]
    fn maxmemory_bad_value_hard_fails_resolution() {
        // A malformed/overflowing/fractional maxmemory must error out of resolve,
        // NOT silently resolve to 0 (unlimited).
        for bad in ["garbage", "99999999999gb", "1.7b", "1.5gb", "12xb", "-5mb"] {
            let o = ConfigOverlay {
                maxmemory: Some(bad.to_owned()),
                ..Default::default()
            };
            let res = Config::resolve(&[o]);
            assert!(
                matches!(res, Err(ConfigError::Size(_, _))),
                "expected Size error for maxmemory {bad:?}, got {res:?}"
            );
        }
    }

    #[test]
    fn maxmemory_good_value_resolves() {
        let o = ConfigOverlay {
            maxmemory: Some("512mb".to_owned()),
            ..Default::default()
        };
        let cfg = Config::resolve(&[o]).unwrap();
        assert_eq!(cfg.maxmemory, 512 * 1024 * 1024);
    }

    #[test]
    fn toml_rejects_unknown_field() {
        let res = ConfigOverlay::from_toml_str("nonsense = 1");
        assert!(res.is_err());
    }

    #[test]
    fn human_size_units() {
        assert_eq!(parse_human_size("0").unwrap(), 0);
        assert_eq!(parse_human_size("512").unwrap(), 512);
        assert_eq!(parse_human_size("512b").unwrap(), 512);
        assert_eq!(parse_human_size("1k").unwrap(), 1_000);
        assert_eq!(parse_human_size("1kb").unwrap(), 1_024);
        assert_eq!(parse_human_size("512mb").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_human_size("2gb").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_human_size("1G").unwrap(), 1_000_000_000);
        // whitespace tolerated.
        assert_eq!(parse_human_size("  64mb ").unwrap(), 64 * 1024 * 1024);
    }

    #[test]
    fn human_size_rejects_garbage() {
        assert!(parse_human_size("").is_err());
        assert!(parse_human_size("abc").is_err());
        assert!(parse_human_size("12xb").is_err());
        assert!(parse_human_size("-5mb").is_err());
        // Fractional inputs are rejected (no silent truncation).
        assert!(parse_human_size("1.5gb").is_err());
        assert!(parse_human_size("1.7b").is_err());
        // Overflow is a hard error, not a silent wrap.
        assert!(parse_human_size("99999999999gb").is_err());
        assert!(parse_human_size("18446744073709551616").is_err()); // u64::MAX + 1
    }

    #[test]
    fn validate_rejects_zero_shards() {
        let c = Config {
            shards: 0,
            ..Config::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_too_many_shards() {
        // FIX 2: a shard count above the composite-cursor ceiling (MAX_SHARDS == 256) is
        // a LOUD boot error (reachable via `--shards 512` or `default_shards()` on a
        // >256-core host), not a silent SCAN cursor corruption. Exactly MAX_SHARDS is OK;
        // one beyond is rejected with the `shards` field error.
        let max = ironcache_storage::ScanCursor::MAX_SHARDS;
        let ok = Config {
            shards: max,
            ..Config::default()
        };
        ok.validate().expect("exactly MAX_SHARDS shards is allowed");

        let too_many = Config {
            shards: max + 1,
            ..Config::default()
        };
        assert!(matches!(
            too_many.validate(),
            Err(ConfigError::Invalid {
                field: "shards",
                ..
            })
        ));
    }

    /// The example multi-node topology TOML (`[[cluster_topology.nodes]]` array-of-tables)
    /// deserializes into the expected `ClusterTopology`, and a full-coverage 3-way split plus
    /// a matching announce id passes `validate`.
    #[test]
    fn cluster_topology_full_coverage_validates_and_round_trips() {
        let toml_src = r#"
            cluster_enabled = true
            cluster_announce_id = "1111111111111111111111111111111111111111"

            [[cluster_topology.nodes]]
            id = "0000000000000000000000000000000000000000"
            host = "10.0.0.10"
            port = 6379
            slots = [[0, 5460]]

            [[cluster_topology.nodes]]
            id = "1111111111111111111111111111111111111111"
            host = "10.0.0.11"
            port = 6379
            slots = [[5461, 10922]]

            [[cluster_topology.nodes]]
            id = "2222222222222222222222222222222222222222"
            host = "10.0.0.12"
            port = 6379
            slots = [[10923, 16383]]
        "#;
        let o = ConfigOverlay::from_toml_str(toml_src).unwrap();
        // The structured topology round-trips into the expected shape.
        let topo = o.cluster_topology.clone().expect("topology parsed");
        assert_eq!(topo.nodes.len(), 3);
        assert_eq!(topo.nodes[0].id, "0000000000000000000000000000000000000000");
        assert_eq!(topo.nodes[1].host, "10.0.0.11");
        assert_eq!(topo.nodes[2].slots, vec![[10923u16, 16383u16]]);
        assert_eq!(
            o.cluster_announce_id.as_deref(),
            Some("1111111111111111111111111111111111111111")
        );
        // And a complete, self-inclusive map validates.
        let cfg = Config::resolve(&[o]).unwrap();
        cfg.validate()
            .expect("a full 3-way split with a matching announce id is valid");
    }

    #[test]
    fn cluster_topology_gap_is_rejected() {
        // ID0 owns 0-8190, ID1 owns 8192-16383: slot 8191 is unassigned.
        let cfg = topology_config(
            true,
            Some("0000000000000000000000000000000000000000"),
            &[
                ("0000000000000000000000000000000000000000", &[[0, 8190]]),
                ("1111111111111111111111111111111111111111", &[[8192, 16383]]),
            ],
        );
        let err = cfg.validate().unwrap_err();
        assert!(
            matches!(
                err,
                ConfigError::Invalid {
                    field: "cluster_topology",
                    ..
                }
            ),
            "got {err:?}"
        );
        assert!(
            err.to_string().contains("8191"),
            "reason names the gap: {err}"
        );
    }

    #[test]
    fn cluster_topology_overlap_is_rejected() {
        // ID0 owns 0-8191, ID1 owns 8000-16383: slot 8000 is the first overlap.
        let cfg = topology_config(
            true,
            Some("0000000000000000000000000000000000000000"),
            &[
                ("0000000000000000000000000000000000000000", &[[0, 8191]]),
                ("1111111111111111111111111111111111111111", &[[8000, 16383]]),
            ],
        );
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::Invalid {
                field: "cluster_topology",
                ..
            }
        ));
    }

    #[test]
    fn cluster_topology_self_missing_is_rejected() {
        // announce id is well-formed but names no node in the topology.
        let cfg = topology_config(
            true,
            Some("9999999999999999999999999999999999999999"),
            &[
                ("0000000000000000000000000000000000000000", &[[0, 8191]]),
                ("1111111111111111111111111111111111111111", &[[8192, 16383]]),
            ],
        );
        let err = cfg.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Invalid {
                field: "cluster_topology",
                ..
            }
        ));
        assert!(
            err.to_string().contains("9999"),
            "reason names the announce id: {err}"
        );
    }

    #[test]
    fn cluster_announce_id_required_when_topology_set() {
        // A topology with NO announce id is rejected on the dedicated field.
        let cfg = topology_config(
            true,
            None,
            &[
                ("0000000000000000000000000000000000000000", &[[0, 8191]]),
                ("1111111111111111111111111111111111111111", &[[8192, 16383]]),
            ],
        );
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::Invalid {
                field: "cluster-announce-id",
                ..
            }
        ));
    }

    #[test]
    fn cluster_topology_ignored_when_cluster_disabled() {
        // A topology supplied with cluster_enabled = false is NOT validated (and never used):
        // the topology block only engages when cluster mode is on. Even a deliberately broken
        // (gappy) topology passes validate when disabled.
        let cfg = topology_config(
            false,
            None,
            &[("0000000000000000000000000000000000000000", &[[0, 100]])],
        );
        cfg.validate()
            .expect("topology is inert when cluster is disabled");
    }

    /// Build a resolved `Config` with a cluster topology directly (bypassing TOML) so the
    /// validation tests can assemble exactly the (possibly invalid) maps they need.
    fn topology_config(
        enabled: bool,
        announce: Option<&str>,
        nodes: &[(&str, &[[u16; 2]])],
    ) -> Config {
        Config {
            cluster_enabled: enabled,
            cluster_announce_id: announce.map(str::to_owned),
            cluster_topology: Some(ClusterTopology {
                nodes: nodes
                    .iter()
                    .enumerate()
                    .map(|(i, (id, slots))| ClusterNode {
                        id: (*id).to_owned(),
                        host: format!("10.0.0.{i}"),
                        port: 6379,
                        slots: slots.to_vec(),
                    })
                    .collect(),
            }),
            ..Config::default()
        }
    }

    #[test]
    fn runtime_overlay_outranks_the_file_layer() {
        // The reload-clobber correctness test (CONFIG.md): the runtime overlay is the
        // HIGHEST-precedence layer, so a CONFIG SET out-ranks the value resolved from
        // the file/CLI/env/default layers, and a subsequent file reload (re-folding the
        // lower layers) does NOT clobber the runtime override.
        use crate::registry::{apply_set, effective_value};
        use crate::runtime::RuntimeConfig;

        // The boot config as resolved from a TOML file layer (maxmemory 256mb).
        let file = ConfigOverlay {
            maxmemory: Some("256mb".to_owned()),
            maxmemory_policy: Some("allkeys-lru".to_owned()),
            ..Default::default()
        };
        let boot = Config::resolve(&[file]).unwrap();
        assert_eq!(boot.maxmemory, 256 * 1024 * 1024);

        // The runtime overlay seeds from the boot config, then a CONFIG SET overrides.
        let runtime = RuntimeConfig::from_config(&boot);
        assert_eq!(
            effective_value("maxmemory", &runtime, &boot).as_deref(),
            Some((256 * 1024 * 1024).to_string().as_str())
        );
        apply_set("maxmemory", "512mb", &runtime);
        apply_set("maxmemory-policy", "allkeys-lfu", &runtime);

        // The runtime override wins over the boot (file) value.
        assert_eq!(
            effective_value("maxmemory", &runtime, &boot).as_deref(),
            Some((512 * 1024 * 1024).to_string().as_str())
        );
        assert_eq!(
            effective_value("maxmemory-policy", &runtime, &boot).as_deref(),
            Some("allkeys-lfu")
        );

        // Simulate a file reload: the file layer changes (maxmemory 128mb) and the boot
        // config is re-folded from the lower layers ONLY. The runtime overlay is NOT
        // touched by the reload, so the CONFIG SET override survives (no clobber).
        let reloaded_file = ConfigOverlay {
            maxmemory: Some("128mb".to_owned()),
            ..Default::default()
        };
        let reloaded_boot = Config::resolve(&[reloaded_file]).unwrap();
        assert_eq!(reloaded_boot.maxmemory, 128 * 1024 * 1024);
        // The effective value is STILL the runtime override (512mb), not the reloaded
        // file value (128mb): the overlay out-ranks the file layer.
        assert_eq!(
            effective_value("maxmemory", &runtime, &reloaded_boot).as_deref(),
            Some((512 * 1024 * 1024).to_string().as_str())
        );
    }

    /// PROD-SAFETY #3/#4/#5 + #528: the connection-safety knobs (`maxclients`, `timeout`,
    /// `output_buffer_limit`, `query_buffer_limit`) parse from TOML + the overlay, override the
    /// defaults, and are runtime-settable + readable via the CONFIG registry (`maxclients` /
    /// `output-buffer-limit` / `query-buffer-limit`).
    #[test]
    fn connection_safety_knobs_parse_and_are_runtime_settable() {
        use crate::registry::{SetOutcome, apply_set, effective_value, lookup};
        use crate::runtime::RuntimeConfig;

        // TOML overlay sets all four; the resolved Config carries them (overriding the defaults).
        let toml = "maxclients = 250\ntimeout = 45\noutput_buffer_limit = 65536\nquery_buffer_limit = 32768\n";
        let overlay = ConfigOverlay::from_toml_str(toml).unwrap();
        assert_eq!(overlay.maxclients, Some(250));
        assert_eq!(overlay.timeout, Some(45));
        assert_eq!(overlay.output_buffer_limit, Some(65536));
        assert_eq!(overlay.query_buffer_limit, Some(32768));
        let cfg = Config::resolve(&[overlay]).unwrap();
        assert_eq!(cfg.maxclients, 250);
        assert_eq!(cfg.timeout_secs, 45);
        assert_eq!(cfg.output_buffer_limit, 65536);
        assert_eq!(cfg.query_buffer_limit, 32768);
        cfg.validate().unwrap();

        // The CONFIG registry knows the runtime-settable names and reports the boot values.
        assert!(lookup("maxclients").is_some());
        assert!(lookup("output-buffer-limit").is_some());
        assert!(lookup("query-buffer-limit").is_some());
        let runtime = RuntimeConfig::from_config(&cfg);
        assert_eq!(
            effective_value("maxclients", &runtime, &cfg).as_deref(),
            Some("250")
        );
        assert_eq!(
            effective_value("output-buffer-limit", &runtime, &cfg).as_deref(),
            Some("65536")
        );
        assert_eq!(
            effective_value("query-buffer-limit", &runtime, &cfg).as_deref(),
            Some("32768")
        );
        assert_eq!(runtime.maxclients(), 250);
        assert_eq!(runtime.output_buffer_limit(), 65536);
        assert_eq!(runtime.query_buffer_limit(), 32768);

        // `CONFIG SET maxclients` updates the live ceiling the accept path reads; `0` disables it.
        assert_eq!(apply_set("maxclients", "9", &runtime), SetOutcome::Applied);
        assert_eq!(runtime.maxclients(), 9);
        assert_eq!(apply_set("maxclients", "0", &runtime), SetOutcome::Applied);
        assert_eq!(runtime.maxclients(), 0);
        // A malformed value is rejected (never a silent accept).
        assert!(matches!(
            apply_set("maxclients", "lots", &runtime),
            SetOutcome::InvalidValue(_)
        ));

        // `CONFIG SET output-buffer-limit` accepts a human size; `0` disables it.
        assert_eq!(
            apply_set("output-buffer-limit", "256mb", &runtime),
            SetOutcome::Applied
        );
        assert_eq!(runtime.output_buffer_limit(), 256 * 1024 * 1024);
        assert_eq!(
            apply_set("output-buffer-limit", "0", &runtime),
            SetOutcome::Applied
        );
        assert_eq!(runtime.output_buffer_limit(), 0);
        assert!(matches!(
            apply_set("output-buffer-limit", "1.5gb", &runtime),
            SetOutcome::InvalidValue(_)
        ));

        // `CONFIG SET query-buffer-limit` (#528) accepts a human size; `0` disables it; garbage is
        // rejected. Mirrors the output-buffer-limit setter on the inbound side.
        assert_eq!(
            apply_set("query-buffer-limit", "128mb", &runtime),
            SetOutcome::Applied
        );
        assert_eq!(runtime.query_buffer_limit(), 128 * 1024 * 1024);
        assert_eq!(
            apply_set("query-buffer-limit", "0", &runtime),
            SetOutcome::Applied
        );
        assert_eq!(runtime.query_buffer_limit(), 0);
        assert!(matches!(
            apply_set("query-buffer-limit", "1.5gb", &runtime),
            SetOutcome::InvalidValue(_)
        ));
    }

    #[test]
    fn cluster_bus_unauthenticated_flags_only_the_exposed_posture() {
        // The DEFAULT standalone node is NOT clustered -> no warning (byte-unchanged boot).
        assert!(!Config::default().cluster_bus_unauthenticated());

        // cluster_enabled with NEITHER a secret NOR TLS is the exposure the audit names.
        let exposed = Config {
            cluster_enabled: true,
            ..Config::default()
        };
        assert!(exposed.cluster_bus_unauthenticated());

        // A non-static cluster_mode (raft / shard-owners) is likewise clustered.
        let raft = Config {
            cluster_mode: ClusterMode::Raft,
            ..Config::default()
        };
        assert!(raft.cluster_bus_unauthenticated());

        // A shared secret (even plaintext) AUTHENTICATES the peer -> no warning.
        let secret = Config {
            cluster_enabled: true,
            cluster_secret: Some("s3cret".to_owned()),
            ..Config::default()
        };
        assert!(!secret.cluster_bus_unauthenticated());

        // cluster_tls = on always carries a secret (validate enforces it) -> authenticated.
        let tls = Config {
            cluster_enabled: true,
            cluster_tls: TlsMode::On,
            ..Config::default()
        };
        assert!(!tls.cluster_bus_unauthenticated());
    }

    #[test]
    fn warn_unknown_env_names_the_typo_and_suggests() {
        // The exact key set from_env probes is the single source of truth; a small representative
        // slice is enough to exercise the guard. A typo'd key is WARNED (not rejected), naming it +
        // a hint, so the operator sees a mistyped knob without a hard boot failure.
        let known: &[&'static str] = &["IRONCACHE_MAXCLIENTS", "IRONCACHE_PORT", "IRONCACHE_BIND"];
        let warnings = warn_unknown_env(["IRONCACHE_MAXCLIENT"], known);
        assert_eq!(warnings.len(), 1, "one unknown key warned: {warnings:?}");
        let msg = &warnings[0];
        assert!(msg.contains("IRONCACHE_MAXCLIENT"), "names the key: {msg}");
        assert!(
            msg.contains("did you mean IRONCACHE_MAXCLIENTS"),
            "suggests the nearest key: {msg}"
        );
    }

    #[test]
    fn warn_unknown_env_stays_silent_for_known_foreign_and_non_prefixed_keys() {
        let known: &[&'static str] = &["IRONCACHE_PORT", "IRONCACHE_BIND"];
        // A known key, a non-IRONCACHE key, the console namespace, and the build stamp all warn
        // nothing; only a genuinely-unknown server-namespaced var (IRONCACHE_BIN here) does.
        let warnings = warn_unknown_env(
            [
                "IRONCACHE_PORT",
                "PATH",
                "RUST_BACKTRACE",
                "LISTEN_FDS",
                "IRONCACHE_CONSOLE_HTTP_ADDR",
                "IRONCACHE_BUILD_VERSION",
            ],
            known,
        );
        assert!(
            warnings.is_empty(),
            "known + foreign-namespaced + non-prefixed vars warn nothing: {warnings:?}"
        );
    }

    #[test]
    fn nearest_env_key_only_suggests_plausible_typos() {
        let known: &[&'static str] = &["IRONCACHE_MAXCLIENTS", "IRONCACHE_TIMEOUT"];
        // A one-char deletion is a plausible typo.
        assert_eq!(
            nearest_env_key("IRONCACHE_MAXCLIENT", known),
            Some("IRONCACHE_MAXCLIENTS")
        );
        // A wildly different key gets no (misleading) suggestion.
        assert_eq!(
            nearest_env_key("IRONCACHE_TOTALLY_DIFFERENT_KNOB", known),
            None
        );
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("same", "same"), 0);
    }
}
