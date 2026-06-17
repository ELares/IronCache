// SPDX-License-Identifier: MIT OR Apache-2.0
//! The `CLUSTER` command family (CLUSTER_CONTRACT.md #70, slice 1).
//!
//! This is the client-contract FOUNDATION: it gives IronCache the `CLUSTER` command surface
//! a real Redis presents, byte-for-byte, plus the pure CRC16/XMODEM `CLUSTER KEYSLOT`
//! projection. It performs NO routing change, NO MOVED/ASK redirection, NO slot map
//! mutation, and NO gossip/Raft (those are later slices). The Compatible tenet governs:
//! every reply here matches a real Redis exactly, with one documented divergence (the
//! single-node auto-slots simplification, below).
//!
//! Two modes, gated on `ctx.info.cluster_enabled` exactly like Redis's `clusterCommand`:
//!
//! * cluster-DISABLED (`cluster-enabled no`, slice-1 default): a real Redis rejects EVERY
//!   `CLUSTER` subcommand at the top of `clusterCommand` with
//!   `-ERR This instance has cluster support disabled` (src/cluster.c, the
//!   `server.cluster_enabled == 0` gate). There is NO per-subcommand carve-out; even
//!   KEYSLOT/INFO/SLOTS are rejected. We do the same: after the arity check, a disabled
//!   instance returns that error for ALL subcommands.
//!
//! * cluster-ENABLED (`cluster-enabled yes`): the introspection subcommands (KEYSLOT /
//!   MYID / INFO / SLOTS / SHARDS / NODES / COUNTKEYSINSLOT / GETKEYSINSLOT / HELP) reply,
//!   and the topology-mutation subcommands (MEET / ADDSLOTS / SETSLOT / ...) return the
//!   single-node "not supported" error (runtime topology mutation is a later slice).
//!
//! ## Slice 2: map-driven multi-node projection
//!
//! When a STATIC topology is configured (`ctx.cluster.is_some()`), the introspection
//! subcommands project the REAL multi-node map: SLOTS / SHARDS / NODES render every node's
//! coalesced slot ranges (from `SlotMap::ranges()`), and INFO reports the map's
//! `cluster_known_nodes` / `cluster_size` / `cluster_slots_assigned`. When NO topology is
//! configured (`ctx.cluster.is_none()`, even if cluster mode is enabled), the slice-1
//! single-node-owns-all bodies (the `*_singlenode` helpers) render exactly as before. A
//! topology is opt-in, so every slice-1 test stays green via the fallback.
//!
//! The node id and the cluster-enabled flag come from [`ServerContext::info`] (the
//! boot-stable [`ironcache_observe::ServerInfo`]).
//!
//! ## Slice-1 single-node divergence (documented)
//!
//! A cluster-ENABLED IronCache node is treated as a single-node cluster that AUTO-OWNS all
//! 16384 slots, so CLUSTER INFO reports `cluster_slots_assigned:16384` / `cluster_size:1`
//! and SLOTS/SHARDS/NODES render one `0-16383` range owned by self. A fresh real-Redis
//! cluster-enabled node owns NO slots until `CLUSTER ADDSLOTS`; multi-node slot assignment,
//! `CLUSTER ADDSLOTS`, MOVED/ASK redirection, and CROSSSLOT enforcement arrive in slice 2.
//! This is the ONE deliberate divergence from Redis; everything else matches.

use crate::cmd_util::{ascii_upper, parse_i64};
use crate::dispatch::ServerContext;
use ironcache_protocol::{CLUSTER_SLOTS, ErrorReply, Request, Value, key_slot};

/// `CLUSTER <subcommand> [args]` (CLUSTER_CONTRACT.md #70, slice 1). Matches on the
/// UPPERCASED subcommand.
///
/// `CLUSTER` is never key-routed (`AlwaysHome`): it runs on the home shard and reads only
/// the immutable server facts in `ctx.info` (the node id, the listen addr, the
/// cluster-enabled flag), so it takes neither the store nor any connection state.
///
/// The dispatch order matches Redis's `clusterCommand`:
///   1. wrong arity (`CLUSTER` with no subcommand) -> the wrong-arity error;
///   2. cluster DISABLED -> `-ERR This instance has cluster support disabled` for EVERY
///      subcommand (the `server.cluster_enabled == 0` gate, NO per-subcommand carve-out);
///   3. cluster ENABLED -> the introspection subcommands reply with single-node values and
///      the topology-mutation subcommands return the single-node "not supported" error.
///
/// Slice-1 single-node simplification (documented in the module header and
/// CLUSTER_CONTRACT.md): an enabled node auto-owns all 16384 slots, whereas a fresh real
/// cluster-enabled Redis owns none until `CLUSTER ADDSLOTS`.
#[must_use]
pub fn cmd_cluster(ctx: &ServerContext, req: &Request) -> Value {
    // 1. `CLUSTER` with no subcommand is the wrong-arity error (the registry arity is
    // Min(2): the token plus a subcommand). Matches Redis's container-command arity.
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("cluster"));
    }

    // 2. Cluster-disabled gate (Redis parity). A real Redis with `cluster-enabled no`
    // rejects EVERY CLUSTER subcommand at the top of clusterCommand with
    // `-ERR This instance has cluster support disabled` (src/cluster.c, the
    // `server.cluster_enabled == 0` gate). There is NO introspection carve-out, so KEYSLOT,
    // INFO, SLOTS, MYID, etc. are all rejected too. This must run BEFORE the subcommand
    // match.
    if !ctx.info.cluster_enabled {
        return Value::error(ErrorReply::cluster_disabled());
    }

    // 3. Cluster-ENABLED: the introspection subcommands reply with single-node values.
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"KEYSLOT" => cluster_keyslot(req),
        b"MYID" => cluster_myid(ctx, req),
        b"INFO" => cluster_info(ctx, req),
        b"SLOTS" => cluster_slots(ctx, req),
        b"SHARDS" => cluster_shards(ctx, req),
        b"NODES" => cluster_nodes(ctx, req),
        b"COUNTKEYSINSLOT" => cluster_countkeysinslot(req),
        b"GETKEYSINSLOT" => cluster_getkeysinslot(req),
        b"HELP" => cluster_help(),
        // Topology-mutation / cluster-only subcommands. On a single-node cluster IronCache
        // cannot reshard or change membership in slice 1, so each returns
        // `-ERR <SUBCOMMAND> is not supported on a single-node cluster`. Real topology
        // (ADDSLOTS / SETSLOT / MEET / ...) plus MOVED/ASK/CROSSSLOT arrives in slice 2.
        // (When cluster mode is DISABLED these never reach here; the gate above already
        // returned the cluster-disabled error.)
        b"MEET" | b"ADDSLOTS" | b"ADDSLOTSRANGE" | b"DELSLOTS" | b"DELSLOTSRANGE" | b"SETSLOT"
        | b"FORGET" | b"REPLICATE" | b"FAILOVER" | b"RESET" | b"BUMPEPOCH" | b"FLUSHSLOTS"
        | b"SET-CONFIG-EPOCH" => Value::error(ErrorReply::err(format!(
            "{} is not supported on a single-node cluster",
            String::from_utf8_lossy(&sub)
        ))),
        // An unrecognized subcommand is the same unknown-subcommand error CONFIG/CLIENT
        // use (byte-exact to Redis addReplySubcommandSyntaxError).
        _ => Value::error(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&req.args[1]),
        )),
    }
}

/// `CLUSTER KEYSLOT <key>` -> the integer slot `CRC16(hash_tag(key)) % 16384`. Works
/// regardless of cluster mode (it is a pure projection, CLUSTER_CONTRACT.md #70). Arity is
/// exactly 3 (`CLUSTER KEYSLOT <key>`); anything else is the wrong-arity error.
fn cluster_keyslot(req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("cluster|keyslot"));
    }
    Value::Integer(i64::from(key_slot(&req.args[2])))
}

/// `CLUSTER MYID` -> the 40-hex node id (a bulk string). Arity exactly 2.
///
/// In cluster-map mode the id comes from the map (`map.me().id`), which equals
/// `ctx.info.cluster_node_id` after the boot-time node-id reconciliation (the announce id);
/// without a map it is `ctx.info.cluster_node_id` (the slice-1 random/boot id). Both agree, so
/// this is belt-and-suspenders.
fn cluster_myid(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|myid"));
    }
    match &ctx.cluster {
        Some(map) => Value::bulk(map.me().id.as_bytes().to_vec()),
        None => Value::bulk_str(ctx.info.cluster_node_id),
    }
}

/// `CLUSTER INFO` -> the cluster status as a RESP3 verbatim string (txt) with the exact
/// `field:value` lines a real Redis emits (each `\r\n`-terminated). Arity exactly 2.
///
/// Reachable only when cluster mode is ENABLED (the disabled gate in [`cmd_cluster`] runs
/// first), so this reports the single-node-cluster picture: `cluster_enabled:1`,
/// `cluster_state:ok`, all 16384 slots assigned and OK, one known node, `cluster_size:1`.
/// Epochs and message counters are zero (no gossip yet). Redis replies CLUSTER INFO via
/// `addReplyVerbatim(..., "txt")`, so this is a `VerbatimString` (it degrades to a bulk
/// string under RESP2 automatically).
fn cluster_info(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|info"));
    }
    // Map-driven counts when a topology is configured; else the single-node-owns-all picture.
    let (slots_assigned, known_nodes, cluster_size) = match &ctx.cluster {
        Some(map) => (map.slots_assigned(), map.known_nodes(), map.cluster_size()),
        None => (u32::from(CLUSTER_SLOTS), 1, 1),
    };
    verbatim_txt(cluster_info_body(slots_assigned, known_nodes, cluster_size).as_bytes())
}

/// Build the `CLUSTER INFO` `field:value` body (each line `\r\n`-terminated) from the slot
/// counts, shared by the map-driven and single-node paths. `cluster_state` stays `ok` because
/// a slice-2 map is always fully assigned (validation rejects a gap); epochs and message
/// counters are zero (no gossip yet).
fn cluster_info_body(slots_assigned: u32, known_nodes: usize, cluster_size: usize) -> String {
    format!(
        "cluster_enabled:1\r\n\
         cluster_state:ok\r\n\
         cluster_slots_assigned:{slots_assigned}\r\n\
         cluster_slots_ok:{slots_assigned}\r\n\
         cluster_slots_pfail:0\r\n\
         cluster_slots_fail:0\r\n\
         cluster_known_nodes:{known_nodes}\r\n\
         cluster_size:{cluster_size}\r\n\
         cluster_current_epoch:0\r\n\
         cluster_my_epoch:0\r\n\
         cluster_stats_messages_sent:0\r\n\
         cluster_stats_messages_received:0\r\n\
         total_cluster_links_buffer_limit_exceeded:0\r\n"
    )
}

/// `CLUSTER SLOTS` -> an array with ONE slot range, `[0, 16383, [ip, port, node_id]]`,
/// because the single-node cluster owns the entire slot space (slice-1 simplification).
/// Arity exactly 2.
///
/// Each range is `[start, end, [host, port, node_id], ...]` per Redis; we emit exactly one
/// served-by triple (self). The host/port come from the boot config and the node id from
/// `ctx.info`.
fn cluster_slots(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|slots"));
    }
    match &ctx.cluster {
        Some(map) => {
            // Project each coalesced (start, end, owner) range as [start, end, [host, port, id]].
            let ranges = map
                .ranges()
                .into_iter()
                .map(|(start, end, idx)| {
                    let n = &map.nodes()[idx];
                    let served = served_by_triple(&n.host, n.port, &n.id);
                    Value::Array(Some(vec![
                        Value::Integer(i64::from(start)),
                        Value::Integer(i64::from(end)),
                        served,
                    ]))
                })
                .collect();
            Value::Array(Some(ranges))
        }
        None => cluster_slots_singlenode(ctx),
    }
}

/// The slice-1 single-node `CLUSTER SLOTS`: one `[0, 16383, [bind, port, node_id]]` range.
fn cluster_slots_singlenode(ctx: &ServerContext) -> Value {
    let served = served_by_triple_str(
        &ctx.boot.bind.to_string(),
        ctx.info.tcp_port,
        ctx.info.cluster_node_id,
    );
    let range = Value::Array(Some(vec![
        Value::Integer(0),
        Value::Integer(i64::from(CLUSTER_SLOTS) - 1),
        served,
    ]));
    Value::Array(Some(vec![range]))
}

/// A `CLUSTER SLOTS` served-by triple `[host, port, id]` from owned-byte node fields.
fn served_by_triple(host: &str, port: u16, id: &str) -> Value {
    Value::Array(Some(vec![
        Value::bulk(host.as_bytes().to_vec()),
        Value::Integer(i64::from(port)),
        Value::bulk(id.as_bytes().to_vec()),
    ]))
}

/// A `CLUSTER SLOTS` served-by triple `[host, port, id]` from `&str` fields (the single-node
/// path, whose id is a `'static str`).
fn served_by_triple_str(host: &str, port: u16, id: &str) -> Value {
    Value::Array(Some(vec![
        Value::bulk_str(host),
        Value::Integer(i64::from(port)),
        Value::bulk_str(id),
    ]))
}

/// `CLUSTER SHARDS` -> an array with ONE shard owning the whole slot space (single-node
/// cluster, slice-1 simplification). Arity exactly 2.
///
/// Each shard is a map `{slots => [start, end], nodes => [<node-map>]}`; the one node is
/// self, a master at `0` replication offset reporting `health: online`, exactly the fields
/// Redis populates in `clusterReplyShards`.
fn cluster_shards(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|shards"));
    }
    match &ctx.cluster {
        Some(map) => {
            // One shard map per node. Collect each node's coalesced ranges from `map.ranges()`,
            // emit `{slots => [s0,e0, s1,e1, ...], nodes => [<master node map>]}`. Slice 2 has
            // no replicas, so exactly one (master) node per shard.
            let ranges = map.ranges();
            let shards = map
                .nodes()
                .iter()
                .enumerate()
                .map(|(idx, n)| {
                    // The flat [start, end] integer pairs for THIS node, in slot order.
                    let mut slots = Vec::new();
                    for &(start, end, owner) in &ranges {
                        if owner == idx {
                            slots.push(Value::Integer(i64::from(start)));
                            slots.push(Value::Integer(i64::from(end)));
                        }
                    }
                    let node = shard_node_map(&n.host, n.port, &n.id);
                    Value::Map(vec![
                        (Value::bulk_str("slots"), Value::Array(Some(slots))),
                        (Value::bulk_str("nodes"), Value::Array(Some(vec![node]))),
                    ])
                })
                .collect();
            Value::Array(Some(shards))
        }
        None => cluster_shards_singlenode(ctx),
    }
}

/// The slice-1 single-node `CLUSTER SHARDS`: one shard owning `[0, 16383]` served by self.
fn cluster_shards_singlenode(ctx: &ServerContext) -> Value {
    let node = shard_node_map(
        &ctx.boot.bind.to_string(),
        ctx.info.tcp_port,
        ctx.info.cluster_node_id,
    );
    let shard = Value::Map(vec![
        (
            Value::bulk_str("slots"),
            Value::Array(Some(vec![
                Value::Integer(0),
                Value::Integer(i64::from(CLUSTER_SLOTS) - 1),
            ])),
        ),
        (Value::bulk_str("nodes"), Value::Array(Some(vec![node]))),
    ]);
    Value::Array(Some(vec![shard]))
}

/// A `CLUSTER SHARDS` node map: a master at `0` replication offset reporting `health: online`,
/// exactly the fields Redis populates in `clusterReplyShards` (no replicas in slice 2). The
/// `ip` and `endpoint` are both the advertised host.
fn shard_node_map(host: &str, port: u16, id: &str) -> Value {
    Value::Map(vec![
        (Value::bulk_str("id"), Value::bulk(id.as_bytes().to_vec())),
        (Value::bulk_str("port"), Value::Integer(i64::from(port))),
        (Value::bulk_str("ip"), Value::bulk(host.as_bytes().to_vec())),
        (
            Value::bulk_str("endpoint"),
            Value::bulk(host.as_bytes().to_vec()),
        ),
        (Value::bulk_str("role"), Value::bulk_str("master")),
        (Value::bulk_str("replication-offset"), Value::Integer(0)),
        (Value::bulk_str("health"), Value::bulk_str("online")),
    ])
}

/// `CLUSTER NODES` -> a RESP3 verbatim string (txt) with ONE line for self, owning the full
/// `0-16383` slot range, in the Redis gossip text format
/// `<id> <ip>:<port>@<cport> myself,master - 0 0 0 connected 0-16383\n` where
/// `cport = port + 10000`. Arity exactly 2.
///
/// The listen `ip:port` comes from the boot config (`ctx.boot.bind`/`ctx.info.tcp_port`);
/// self is `myself,master`, `connected`, owning the whole slot space (single-node cluster,
/// slice-1 simplification). Redis replies CLUSTER NODES via `addReplyVerbatim(..., "txt")`,
/// so this is a `VerbatimString` (it degrades to a bulk string under RESP2 automatically).
fn cluster_nodes(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|nodes"));
    }
    let Some(map) = &ctx.cluster else {
        return cluster_nodes_singlenode(ctx);
    };
    // One gossip line per node, in declaration order; THIS node is flagged `myself,master`,
    // the others `master`. Each node's served slot ranges (its coalesced runs from
    // `map.ranges()`) are space-joined as the trailing field.
    let ranges = map.ranges();
    let self_id = &map.me().id;
    let mut text = String::new();
    for n in map.nodes() {
        let flags = if &n.id == self_id {
            "myself,master"
        } else {
            "master"
        };
        let owned: Vec<String> = ranges
            .iter()
            .filter(|&&(_, _, idx)| map.nodes()[idx].id == n.id)
            .map(|&(start, end, _)| {
                if start == end {
                    start.to_string()
                } else {
                    format!("{start}-{end}")
                }
            })
            .collect();
        text.push_str(&node_line(&n.host, n.port, &n.id, flags, &owned.join(" ")));
    }
    verbatim_txt(text.as_bytes())
}

/// The slice-1 single-node `CLUSTER NODES` line: self owns the whole `0-16383` space, flagged
/// `myself,master`, `connected`. Used when no static topology is configured.
fn cluster_nodes_singlenode(ctx: &ServerContext) -> Value {
    let last_slot = CLUSTER_SLOTS - 1;
    let line = node_line(
        &ctx.boot.bind.to_string(),
        ctx.info.tcp_port,
        ctx.info.cluster_node_id,
        "myself,master",
        &format!("0-{last_slot}"),
    );
    verbatim_txt(line.as_bytes())
}

/// Build one Redis `CLUSTER NODES` gossip text line:
/// `<id> <host>:<port>@<cport> <flags> <master> 0 0 0 connected <ranges>\n`.
///
/// The cluster bus port is the listen port + 10000 (Redis's fixed offset). The `<master>`
/// field is `-` (no replicas in slice 2). The trailing `\n` (NOT `\r\n`) terminates each line
/// in the Redis NODES text format. `ranges` is the already-formatted served-slot field (e.g.
/// `0-5460` or `0-100 200-300`).
fn node_line(host: &str, port: u16, id: &str, flags: &str, ranges: &str) -> String {
    let cport = u32::from(port) + 10_000;
    format!("{id} {host}:{port}@{cport} {flags} - 0 0 0 connected {ranges}\n")
}

/// A RESP3 verbatim string with the `txt` format (degrades to a bulk string under RESP2).
/// Redis replies CLUSTER INFO / CLUSTER NODES with `addReplyVerbatim(..., "txt")`.
fn verbatim_txt(data: &[u8]) -> Value {
    Value::VerbatimString {
        format: *b"txt",
        data: bytes::Bytes::copy_from_slice(data),
    }
}

/// `CLUSTER COUNTKEYSINSLOT <slot>` -> the number of keys in `<slot>` as an integer. Arity
/// exactly 3.
///
/// The slot arg is parsed as an integer FIRST (a non-integer is Redis's default
/// `-ERR value is not an integer or out of range`), THEN range-checked to `[0, 16384)`
/// (out of range is `-ERR Invalid slot`, matching `getSlotOrReply`).
///
/// DOCUMENTED PLACEHOLDER: the count is always `0`. An accurate per-slot count needs a
/// cross-shard slot index (a slot -> key-count map maintained as keys are written), which
/// is a later slice (slice 2, alongside real slot ownership). Slice 1 keeps no such index,
/// so it returns 0 rather than silently pretending an accurate count.
fn cluster_countkeysinslot(req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("cluster|countkeysinslot"));
    }
    if let Err(e) = parse_slot(&req.args[2]) {
        return Value::error(e);
    }
    // Placeholder: no per-slot index in slice 1 (see the doc comment above).
    Value::Integer(0)
}

/// `CLUSTER GETKEYSINSLOT <slot> <count>` -> the (up to `<count>`) keys in `<slot>` as an
/// array. Arity exactly 4.
///
/// Both args are parsed as integers FIRST (a non-integer is Redis's default
/// `-ERR value is not an integer or out of range`). Then a bad slot OR a negative count is
/// the SINGLE Redis error `-ERR Invalid slot or number of keys` (src/cluster.c
/// `clusterCommand` GETKEYSINSLOT path validates both with one message; there is no separate
/// "Invalid number of keys" string in Redis).
///
/// DOCUMENTED PLACEHOLDER: the result is always EMPTY for the same reason as
/// COUNTKEYSINSLOT (no cross-shard slot index in slice 1).
fn cluster_getkeysinslot(req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("cluster|getkeysinslot"));
    }
    // Parse both as integers first (non-integer -> the default not-an-integer error).
    let Some(slot) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    let Some(count) = parse_i64(&req.args[3]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    // A bad slot OR a negative count is the one Redis error (no separate count string).
    if !(0..i64::from(CLUSTER_SLOTS)).contains(&slot) || count < 0 {
        return Value::error(ErrorReply::err("Invalid slot or number of keys"));
    }
    // Placeholder: no per-slot index in slice 1 (see the doc comment above).
    Value::Array(Some(Vec::new()))
}

/// `CLUSTER HELP` -> an array of bulk-string help lines summarizing the supported
/// subcommands (like Redis `addReplyHelp`).
fn cluster_help() -> Value {
    let lines: &[&str] = &[
        "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "INFO",
        "    Return information about the cluster.",
        "MYID",
        "    Return the node id.",
        "KEYSLOT <key>",
        "    Return the hash slot for <key>.",
        "SLOTS",
        "    Return information about slots range mappings.",
        "SHARDS",
        "    Return information about slot range mappings and the nodes serving them.",
        "NODES",
        "    Return cluster configuration seen by node. Output format:",
        "    <id> <ip:port@cport> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
        "COUNTKEYSINSLOT <slot>",
        "    Return the number of keys in <slot>.",
        "GETKEYSINSLOT <slot> <count>",
        "    Return key names stored by current node in a slot.",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(Some(lines.iter().map(|l| Value::bulk_str(l)).collect()))
}

/// Parse and bounds-check a slot argument the way Redis's `getSlotOrReply` does: parse the
/// integer FIRST, then range-check `[0, 16384)`.
///
/// * a NON-integer arg -> `Err(not_an_integer())` (Redis's default
///   `-ERR value is not an integer or out of range`, emitted before any range check);
/// * an out-of-range integer (negative or `>= 16384`) -> `Err(-ERR Invalid slot)`;
/// * otherwise `Ok(slot)` (the cast is exact, 0..16383 fits a u16).
fn parse_slot(arg: &[u8]) -> Result<u16, ErrorReply> {
    let n = parse_i64(arg).ok_or_else(ErrorReply::not_an_integer)?;
    if (0..i64::from(CLUSTER_SLOTS)).contains(&n) {
        // In range, so the cast is exact (0..16383 fits a u16).
        Ok(n as u16)
    } else {
        Err(ErrorReply::err("Invalid slot"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ironcache_config::{Config, RuntimeConfig};
    use ironcache_env::Monotonic;
    use ironcache_observe::ServerInfo;

    const TEST_NODE_ID: &str = "abcdef0123456789abcdef0123456789abcdef01";

    fn ctx_with(boot: Config) -> ServerContext {
        let runtime = RuntimeConfig::from_config(&boot);
        ServerContext {
            runtime,
            databases: boot.databases,
            shards: boot.shards,
            info: ServerInfo {
                tcp_port: boot.port,
                shards: boot.shards,
                pid: 1,
                started_at: Monotonic::ZERO,
                maxmemory: boot.maxmemory,
                maxmemory_policy: "allkeys-lru",
                mem_allocator: "jemalloc",
                cluster_node_id: TEST_NODE_ID,
                cluster_enabled: boot.cluster_enabled,
            },
            // No slot map: the single-node-owns-all fallback (slice-1 behavior). The
            // projection tests below use `ctx_with_map` to supply a real multi-node map.
            cluster: None,
            boot,
        }
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    fn run(ctx: &ServerContext, parts: &[&[u8]]) -> Value {
        cmd_cluster(ctx, &req(parts))
    }

    /// A cluster-DISABLED ctx (the slice-1 default boot).
    fn disabled() -> ServerContext {
        ctx_with(Config::default())
    }

    /// A cluster-ENABLED ctx on a given port (single-node cluster owning all slots).
    fn enabled(port: u16) -> ServerContext {
        ctx_with(Config {
            port,
            cluster_enabled: true,
            ..Config::default()
        })
    }

    const MAP_ID0: &str = "0000000000000000000000000000000000000000";
    const MAP_ID1: &str = "1111111111111111111111111111111111111111";
    const MAP_ID2: &str = "2222222222222222222222222222222222222222";

    /// A cluster-ENABLED ctx carrying a real 3-node slot map (ID0=0-5460, ID1=5461-10922,
    /// ID2=10923-16383), with `self` = ID1. Used by the multi-node projection tests.
    fn ctx_with_map() -> ServerContext {
        let map = ironcache_cluster::SlotMap::build(
            vec![
                (
                    ironcache_cluster::NodeEntry {
                        id: MAP_ID0.into(),
                        host: "10.0.0.10".into(),
                        port: 7000,
                    },
                    vec![[0, 5460]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: MAP_ID1.into(),
                        host: "10.0.0.11".into(),
                        port: 7001,
                    },
                    vec![[5461, 10922]],
                ),
                (
                    ironcache_cluster::NodeEntry {
                        id: MAP_ID2.into(),
                        host: "10.0.0.12".into(),
                        port: 7002,
                    },
                    vec![[10923, 16383]],
                ),
            ],
            MAP_ID1,
        )
        .expect("a full 3-way split is valid");
        let mut ctx = ctx_with(Config {
            cluster_enabled: true,
            ..Config::default()
        });
        ctx.cluster = Some(std::sync::Arc::new(map));
        ctx
    }

    /// The textual payload of a CLUSTER INFO / NODES reply. These are RESP3 verbatim
    /// strings (Redis `addReplyVerbatim(..., "txt")`); the test reads the `txt`-format
    /// body. (Bulk is accepted too, in case a future change degrades it.)
    fn text_body(v: &Value) -> String {
        match v {
            Value::VerbatimString { format, data } => {
                assert_eq!(format, b"txt", "expected a txt verbatim string");
                String::from_utf8_lossy(data).into_owned()
            }
            Value::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("expected a verbatim/bulk text reply, got {other:?}"),
        }
    }

    fn bulk_string(v: &Value) -> String {
        match v {
            Value::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    // ----- DISABLED mode (Redis `cluster-enabled no` parity) -----

    /// With cluster mode DISABLED, a real Redis rejects EVERY CLUSTER subcommand at the top
    /// of clusterCommand (the `server.cluster_enabled == 0` gate) with the same error. There
    /// is NO introspection carve-out, so KEYSLOT/INFO/SLOTS/MYID and an unknown subcommand
    /// all return `-ERR This instance has cluster support disabled`.
    #[test]
    fn disabled_rejects_every_subcommand() {
        let c = disabled();
        for sub in [
            b"KEYSLOT".as_slice(),
            b"INFO",
            b"SLOTS",
            b"SHARDS",
            b"NODES",
            b"MYID",
            b"COUNTKEYSINSLOT",
            b"GETKEYSINSLOT",
            b"HELP",
            b"MEET",
            b"ADDSLOTS",
            b"SETSLOT",
            b"FORGET",
            b"BOGUS",
        ] {
            // Include a key/arg so a would-be introspection reply is well-formed; the gate
            // must fire regardless of args.
            match cmd_cluster(&c, &req(&[b"CLUSTER", sub, b"foo"])) {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-ERR This instance has cluster support disabled",
                    "subcommand {:?}",
                    String::from_utf8_lossy(sub)
                ),
                other => panic!("expected cluster-disabled for {sub:?}, got {other:?}"),
            }
        }
    }

    /// `CLUSTER` with no subcommand is the wrong-arity error EVEN when disabled (arity is
    /// checked before the cluster-disabled gate, matching Redis's argument-count path).
    #[test]
    fn cluster_alone_is_wrong_arity_in_both_modes() {
        match cmd_cluster(&disabled(), &req(&[b"CLUSTER"])) {
            Value::Error(e) => assert!(
                e.line().contains("wrong number of arguments"),
                "got {}",
                e.line()
            ),
            other => panic!("expected wrong-arity, got {other:?}"),
        }
        match cmd_cluster(&enabled(6390), &req(&[b"CLUSTER"])) {
            Value::Error(e) => assert!(
                e.line().contains("wrong number of arguments"),
                "got {}",
                e.line()
            ),
            other => panic!("expected wrong-arity, got {other:?}"),
        }
    }

    // ----- ENABLED mode (single-node cluster owning all 16384 slots) -----

    #[test]
    fn enabled_keyslot_matches_crc16_and_co_locates_hash_tags() {
        let c = enabled(6390);
        // The reference vectors (verified against a real Redis Cluster).
        assert_eq!(
            run(&c, &[b"CLUSTER", b"KEYSLOT", b"foo"]),
            Value::Integer(12182)
        );
        assert_eq!(
            run(&c, &[b"CLUSTER", b"KEYSLOT", b"bar"]),
            Value::Integer(5061)
        );
        // Hash-tag co-location: `{user1000}.following` and `.followers` share a slot.
        let a = run(&c, &[b"CLUSTER", b"KEYSLOT", b"{user1000}.following"]);
        let b = run(&c, &[b"CLUSTER", b"KEYSLOT", b"{user1000}.followers"]);
        assert_eq!(a, b);
        assert_eq!(a, Value::Integer(3443));
        // Wrong arity (no key, or extra arg).
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"KEYSLOT"]),
            Value::Error(_)
        ));
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"KEYSLOT", b"a", b"b"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn enabled_myid_is_the_40_hex_node_id() {
        let c = enabled(6390);
        let v = run(&c, &[b"CLUSTER", b"MYID"]);
        let id = bulk_string(&v);
        assert_eq!(id, TEST_NODE_ID);
        assert_eq!(id.len(), 40);
        assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
        // Arity: MYID takes no args.
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"MYID", b"x"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn enabled_info_reports_single_node_owning_all_slots() {
        let c = enabled(6390);
        let body = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        for line in [
            "cluster_enabled:1\r\n",
            "cluster_state:ok\r\n",
            "cluster_slots_assigned:16384\r\n",
            "cluster_slots_ok:16384\r\n",
            "cluster_slots_pfail:0\r\n",
            "cluster_slots_fail:0\r\n",
            "cluster_known_nodes:1\r\n",
            "cluster_size:1\r\n",
            "cluster_current_epoch:0\r\n",
            "cluster_my_epoch:0\r\n",
            "cluster_stats_messages_sent:0\r\n",
            "cluster_stats_messages_received:0\r\n",
            "total_cluster_links_buffer_limit_exceeded:0\r\n",
        ] {
            assert!(body.contains(line), "INFO missing {line:?} in {body:?}");
        }
        // The first line is cluster_enabled, exactly as Redis orders it.
        assert!(body.starts_with("cluster_enabled:1\r\n"));
    }

    #[test]
    fn enabled_info_and_nodes_are_verbatim_txt() {
        let c = enabled(6390);
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"INFO"]),
            Value::VerbatimString { format, .. } if &format == b"txt"
        ));
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"NODES"]),
            Value::VerbatimString { format, .. } if &format == b"txt"
        ));
    }

    #[test]
    fn enabled_slots_has_one_full_range() {
        let c = enabled(6390);
        let v = run(&c, &[b"CLUSTER", b"SLOTS"]);
        // [ [0, 16383, [ip, port, node_id]] ]
        let Value::Array(Some(ranges)) = v else {
            panic!("expected an array, got {v:?}");
        };
        assert_eq!(ranges.len(), 1, "single-node cluster has one slot range");
        let Value::Array(Some(range)) = &ranges[0] else {
            panic!("expected a range array, got {:?}", ranges[0]);
        };
        assert_eq!(range[0], Value::Integer(0));
        assert_eq!(range[1], Value::Integer(16383));
        let Value::Array(Some(node)) = &range[2] else {
            panic!("expected a served-by triple, got {:?}", range[2]);
        };
        assert_eq!(node[0], Value::bulk_str("127.0.0.1"));
        assert_eq!(node[1], Value::Integer(6390));
        assert_eq!(node[2], Value::bulk_str(TEST_NODE_ID));
    }

    #[test]
    fn enabled_shards_has_one_shard() {
        let c = enabled(6390);
        let v = run(&c, &[b"CLUSTER", b"SHARDS"]);
        let Value::Array(Some(shards)) = v else {
            panic!("expected an array, got {v:?}");
        };
        assert_eq!(shards.len(), 1, "single-node cluster has one shard");
        let Value::Map(shard) = &shards[0] else {
            panic!("expected a shard map, got {:?}", shards[0]);
        };
        // slots => [0, 16383]
        let slots = &shard
            .iter()
            .find(|(k, _)| *k == Value::bulk_str("slots"))
            .expect("shard has slots")
            .1;
        assert_eq!(
            *slots,
            Value::Array(Some(vec![Value::Integer(0), Value::Integer(16383)]))
        );
        // nodes => [ { role => master, health => online, ... } ]
        let nodes = &shard
            .iter()
            .find(|(k, _)| *k == Value::bulk_str("nodes"))
            .expect("shard has nodes")
            .1;
        let Value::Array(Some(node_list)) = nodes else {
            panic!("expected a nodes array, got {nodes:?}");
        };
        assert_eq!(node_list.len(), 1);
        let Value::Map(node) = &node_list[0] else {
            panic!("expected a node map, got {:?}", node_list[0]);
        };
        let field = |name: &str| {
            node.iter()
                .find(|(k, _)| *k == Value::bulk_str(name))
                .map(|(_, val)| val.clone())
        };
        assert_eq!(field("id"), Some(Value::bulk_str(TEST_NODE_ID)));
        assert_eq!(field("port"), Some(Value::Integer(6390)));
        assert_eq!(field("role"), Some(Value::bulk_str("master")));
        assert_eq!(field("health"), Some(Value::bulk_str("online")));
    }

    #[test]
    fn enabled_nodes_renders_self_owning_all_slots() {
        let c = enabled(6390);
        let line = text_body(&run(&c, &[b"CLUSTER", b"NODES"]));
        // <id> <ip>:<port>@<cport> myself,master - 0 0 0 connected 0-16383\n
        assert!(line.starts_with(TEST_NODE_ID), "got {line:?}");
        // cport = port + 10000 = 16390; the default bind is loopback.
        assert!(line.contains("127.0.0.1:6390@16390"), "got {line:?}");
        assert!(line.contains("myself,master"), "got {line:?}");
        // The served slot range owns the whole space.
        assert!(line.contains("connected 0-16383"), "got {line:?}");
        assert!(line.ends_with("0-16383\n"), "got {line:?}");
    }

    #[test]
    fn enabled_countkeysinslot_validates_bounds_and_returns_zero() {
        let c = enabled(6390);
        // In-range slots -> :0 (documented placeholder; no per-slot index in slice 1).
        assert_eq!(
            run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"0"]),
            Value::Integer(0)
        );
        assert_eq!(
            run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"16383"]),
            Value::Integer(0)
        );
        // Out of range (>= 16384) and negative are -ERR Invalid slot.
        for bad in [b"16384".as_slice(), b"99999", b"-1"] {
            match run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", bad]) {
                Value::Error(e) => assert_eq!(e.line(), "-ERR Invalid slot", "slot {bad:?}"),
                other => panic!("expected Invalid slot for {bad:?}, got {other:?}"),
            }
        }
        // A non-integer slot is the default not-an-integer error (parsed before range check).
        match run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"abc"]) {
            Value::Error(e) => {
                assert_eq!(e.line(), "-ERR value is not an integer or out of range");
            }
            other => panic!("expected not-an-integer, got {other:?}"),
        }
    }

    #[test]
    fn enabled_getkeysinslot_validates_and_returns_empty() {
        let c = enabled(6390);
        assert_eq!(
            run(&c, &[b"CLUSTER", b"GETKEYSINSLOT", b"0", b"10"]),
            Value::Array(Some(Vec::new()))
        );
        // Bad slot -> the single Redis error `Invalid slot or number of keys`.
        match run(&c, &[b"CLUSTER", b"GETKEYSINSLOT", b"16384", b"10"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR Invalid slot or number of keys"),
            other => panic!("expected Invalid slot or number of keys, got {other:?}"),
        }
        // Negative count -> the SAME single error (not a separate "Invalid number of keys").
        match run(&c, &[b"CLUSTER", b"GETKEYSINSLOT", b"0", b"-1"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR Invalid slot or number of keys"),
            other => panic!("expected Invalid slot or number of keys, got {other:?}"),
        }
        // A non-integer slot or count is the default not-an-integer error (parsed first).
        for args in [[b"abc".as_slice(), b"10"], [b"0".as_slice(), b"xyz"]] {
            match run(&c, &[b"CLUSTER", b"GETKEYSINSLOT", args[0], args[1]]) {
                Value::Error(e) => {
                    assert_eq!(e.line(), "-ERR value is not an integer or out of range");
                }
                other => panic!("expected not-an-integer for {args:?}, got {other:?}"),
            }
        }
    }

    /// On a single-node cluster the topology-mutation subcommands return the documented
    /// `-ERR <SUBCOMMAND> is not supported on a single-node cluster` (slice 2 adds the real
    /// topology). They are reachable only because cluster mode is ENABLED here; when
    /// DISABLED they hit the cluster-disabled gate instead.
    #[test]
    fn enabled_topology_mutation_subcommands_are_not_supported() {
        let c = enabled(6390);
        for sub in [
            b"MEET".as_slice(),
            b"ADDSLOTS",
            b"ADDSLOTSRANGE",
            b"DELSLOTS",
            b"DELSLOTSRANGE",
            b"SETSLOT",
            b"FORGET",
            b"REPLICATE",
            b"FAILOVER",
            b"RESET",
            b"BUMPEPOCH",
            b"FLUSHSLOTS",
            b"SET-CONFIG-EPOCH",
        ] {
            match cmd_cluster(&c, &req(&[b"CLUSTER", sub])) {
                Value::Error(e) => {
                    let want = format!(
                        "-ERR {} is not supported on a single-node cluster",
                        String::from_utf8_lossy(sub)
                    );
                    assert_eq!(
                        e.line(),
                        want,
                        "subcommand {:?}",
                        String::from_utf8_lossy(sub)
                    );
                }
                other => panic!("expected not-supported for {sub:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn enabled_help_is_an_array_and_unknown_sub_errors() {
        let c = enabled(6390);
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"HELP"]),
            Value::Array(Some(_))
        ));
        match run(&c, &[b"CLUSTER", b"BOGUS"]) {
            Value::Error(e) => assert!(e.line().contains("unknown subcommand"), "got {}", e.line()),
            other => panic!("expected unknown subcommand, got {other:?}"),
        }
    }

    // ----- MAP-DRIVEN multi-node projection (slice 2) -----

    #[test]
    fn map_slots_reflects_three_ranges() {
        let c = ctx_with_map();
        let Value::Array(Some(ranges)) = run(&c, &[b"CLUSTER", b"SLOTS"]) else {
            panic!("expected an array");
        };
        assert_eq!(ranges.len(), 3, "three nodes -> three slot ranges");
        // Each range is [start, end, [host, port, id]]. Check the boundaries and served-by.
        let expect = [
            (0i64, 5460i64, "10.0.0.10", 7000i64, MAP_ID0),
            (5461, 10922, "10.0.0.11", 7001, MAP_ID1),
            (10923, 16383, "10.0.0.12", 7002, MAP_ID2),
        ];
        for (range, (start, end, host, port, id)) in ranges.iter().zip(expect) {
            let Value::Array(Some(r)) = range else {
                panic!("expected a range array");
            };
            assert_eq!(r[0], Value::Integer(start));
            assert_eq!(r[1], Value::Integer(end));
            let Value::Array(Some(served)) = &r[2] else {
                panic!("expected a served-by triple");
            };
            assert_eq!(served[0], Value::bulk_str(host));
            assert_eq!(served[1], Value::Integer(port));
            assert_eq!(served[2], Value::bulk_str(id));
        }
    }

    #[test]
    fn map_shards_reflects_three_nodes() {
        let c = ctx_with_map();
        let Value::Array(Some(shards)) = run(&c, &[b"CLUSTER", b"SHARDS"]) else {
            panic!("expected an array");
        };
        assert_eq!(shards.len(), 3, "three nodes -> three shards");
        // The middle shard (ID1) owns [5461, 10922] and is a master, online.
        let Value::Map(shard) = &shards[1] else {
            panic!("expected a shard map");
        };
        let slots = &shard
            .iter()
            .find(|(k, _)| *k == Value::bulk_str("slots"))
            .expect("shard has slots")
            .1;
        assert_eq!(
            *slots,
            Value::Array(Some(vec![Value::Integer(5461), Value::Integer(10922)]))
        );
        let Value::Array(Some(nodes)) = &shard
            .iter()
            .find(|(k, _)| *k == Value::bulk_str("nodes"))
            .expect("shard has nodes")
            .1
        else {
            panic!("expected a nodes array");
        };
        assert_eq!(nodes.len(), 1, "no replicas in slice 2");
        let Value::Map(node) = &nodes[0] else {
            panic!("expected a node map");
        };
        let field = |name: &str| {
            node.iter()
                .find(|(k, _)| *k == Value::bulk_str(name))
                .map(|(_, v)| v.clone())
        };
        assert_eq!(field("id"), Some(Value::bulk_str(MAP_ID1)));
        assert_eq!(field("port"), Some(Value::Integer(7001)));
        assert_eq!(field("ip"), Some(Value::bulk_str("10.0.0.11")));
        assert_eq!(field("role"), Some(Value::bulk_str("master")));
        assert_eq!(field("health"), Some(Value::bulk_str("online")));
    }

    #[test]
    fn map_nodes_renders_one_line_per_node_with_self_flagged() {
        let c = ctx_with_map();
        let body = text_body(&run(&c, &[b"CLUSTER", b"NODES"]));
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 3, "one gossip line per node");
        // Self (ID1) is `myself,master` with its bus port = 7001 + 10000; owns 5461-10922.
        let self_line = lines
            .iter()
            .find(|l| l.starts_with(MAP_ID1))
            .expect("a self line");
        assert!(
            self_line.contains("10.0.0.11:7001@17001"),
            "got {self_line}"
        );
        assert!(self_line.contains("myself,master"), "got {self_line}");
        assert!(
            self_line.ends_with("connected 5461-10922"),
            "got {self_line}"
        );
        // The other two are plain `master` (not myself), with their own ranges.
        let other = lines
            .iter()
            .find(|l| l.starts_with(MAP_ID0))
            .expect("the ID0 line");
        assert!(other.contains(" master - "), "got {other}");
        assert!(!other.contains("myself"), "got {other}");
        assert!(other.ends_with("connected 0-5460"), "got {other}");
    }

    #[test]
    fn map_info_counts_from_map() {
        let c = ctx_with_map();
        let body = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(body.contains("cluster_known_nodes:3\r\n"), "got {body:?}");
        assert!(body.contains("cluster_size:3\r\n"), "got {body:?}");
        assert!(
            body.contains("cluster_slots_assigned:16384\r\n"),
            "got {body:?}"
        );
        assert!(body.contains("cluster_state:ok\r\n"), "got {body:?}");
    }

    #[test]
    fn map_myid_is_the_self_node_id() {
        let c = ctx_with_map();
        // In map mode MYID is the map's self id (the announce id), NOT the ServerInfo id.
        assert_eq!(bulk_string(&run(&c, &[b"CLUSTER", b"MYID"])), MAP_ID1);
    }
}
