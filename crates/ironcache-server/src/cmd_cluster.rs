// SPDX-License-Identifier: MIT OR Apache-2.0
//! The `CLUSTER` command family in CLUSTER-DISABLED-but-introspectable mode
//! (CLUSTER_CONTRACT.md #70, slice 1).
//!
//! This is the client-contract FOUNDATION: it gives IronCache the read-only `CLUSTER`
//! command surface a real Redis presents with `cluster-enabled no`, byte-for-byte, plus
//! the pure CRC16/XMODEM `CLUSTER KEYSLOT` projection. It performs NO routing change, NO
//! MOVED/ASK redirection, NO slot map, and NO gossip/Raft (those are later slices). The
//! Compatible tenet governs: every reply here matches a standalone Redis exactly, and the
//! mutating/cluster-only subcommands are rejected with the same
//! `-ERR This instance has cluster support disabled` Redis emits.
//!
//! The introspection subcommands (KEYSLOT / MYID / INFO / SLOTS / SHARDS / NODES /
//! COUNTKEYSINSLOT / GETKEYSINSLOT / HELP) reply normally; the cluster-only subcommands
//! (MEET / ADDSLOTS / SETSLOT / FORGET / REPLICATE / FAILOVER / RESET / ...) reply the
//! cluster-disabled error. The node id and the cluster-enabled flag come from
//! [`ServerContext::info`] (the boot-stable [`ironcache_observe::ServerInfo`]).

use crate::cmd_util::{ascii_upper, parse_i64};
use crate::dispatch::ServerContext;
use ironcache_protocol::{CLUSTER_SLOTS, ErrorReply, Request, Value, key_slot};

/// `CLUSTER <subcommand> [args]` (CLUSTER_CONTRACT.md #70, slice 1). Matches on the
/// UPPERCASED subcommand and replies as a real Redis with `cluster-enabled no` does.
///
/// `CLUSTER` is never key-routed (`AlwaysHome`): it runs on the home shard and reads only
/// the immutable server facts in `ctx.info` (the node id, the listen addr, the
/// cluster-enabled flag), so it takes neither the store nor any connection state. KEYSLOT
/// is a pure CRC16/XMODEM computation and works regardless of cluster mode.
#[must_use]
pub fn cmd_cluster(ctx: &ServerContext, req: &Request) -> Value {
    // `CLUSTER` with no subcommand is the wrong-arity error (the registry arity is Min(2):
    // the token plus a subcommand). Matches Redis's container-command arity.
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("cluster"));
    }
    let sub = ascii_upper(&req.args[1]);
    match sub.as_slice() {
        b"KEYSLOT" => cluster_keyslot(req),
        b"MYID" => cluster_myid(ctx, req),
        b"INFO" => cluster_info(ctx, req),
        b"SLOTS" => cluster_slots(req),
        b"SHARDS" => cluster_shards(req),
        b"NODES" => cluster_nodes(ctx, req),
        b"COUNTKEYSINSLOT" => cluster_countkeysinslot(req),
        b"GETKEYSINSLOT" => cluster_getkeysinslot(req),
        b"HELP" => cluster_help(),
        // Mutating / cluster-only subcommands: a real Redis with cluster mode disabled
        // rejects every one of these with `-ERR This instance has cluster support
        // disabled` (src/cluster.c clusterCommand, the cluster_enabled == 0 gate). We do
        // the same; turning them on is a later slice (NO MOVED/ASK/CROSSSLOT codes here).
        b"MEET" | b"ADDSLOTS" | b"ADDSLOTSRANGE" | b"DELSLOTS" | b"DELSLOTSRANGE" | b"SETSLOT"
        | b"FORGET" | b"REPLICATE" | b"FAILOVER" | b"RESET" | b"BUMPEPOCH" | b"FLUSHSLOTS"
        | b"SET-CONFIG-EPOCH" | b"SLAVES" | b"REPLICAS" | b"LINKS" => {
            Value::error(ErrorReply::cluster_disabled())
        }
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
fn cluster_myid(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|myid"));
    }
    Value::bulk_str(ctx.info.cluster_node_id)
}

/// `CLUSTER INFO` -> the cluster status as a bulk string with the exact `field:value`
/// lines a real Redis emits in NON-cluster mode (each `\r\n`-terminated). Arity exactly 2.
///
/// Every counter is zero and `cluster_enabled:0` because slice 1 is cluster-disabled with
/// no slots assigned; `cluster_state:ok` is what a standalone Redis reports. The
/// `cluster_enabled` line is sourced from `ctx.info` so it stays consistent with the INFO
/// `# Cluster` section.
fn cluster_info(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|info"));
    }
    let enabled = u8::from(ctx.info.cluster_enabled);
    let body = format!(
        "cluster_enabled:{enabled}\r\n\
         cluster_state:ok\r\n\
         cluster_slots_assigned:0\r\n\
         cluster_slots_ok:0\r\n\
         cluster_slots_pfail:0\r\n\
         cluster_slots_fail:0\r\n\
         cluster_known_nodes:1\r\n\
         cluster_size:0\r\n\
         cluster_current_epoch:0\r\n\
         cluster_my_epoch:0\r\n\
         cluster_stats_messages_sent:0\r\n\
         cluster_stats_messages_received:0\r\n\
         total_cluster_links_buffer_limit_exceeded:0\r\n"
    );
    Value::bulk(body.into_bytes())
}

/// `CLUSTER SLOTS` -> an EMPTY array. Cluster disabled means no slots are assigned, so the
/// slot-range projection is empty, exactly like a real Redis non-cluster instance. Arity
/// exactly 2.
fn cluster_slots(req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|slots"));
    }
    Value::Array(Some(Vec::new()))
}

/// `CLUSTER SHARDS` -> an EMPTY array (no shards in the slot projection when cluster mode
/// is off), matching a real Redis non-cluster instance. Arity exactly 2.
fn cluster_shards(req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|shards"));
    }
    Value::Array(Some(Vec::new()))
}

/// `CLUSTER NODES` -> a bulk string with ONE line for self (no slot range), in the Redis
/// gossip text format `<id> <ip>:<port>@<cport> myself,master - 0 0 0 connected\n` where
/// `cport = port + 10000`. Arity exactly 2.
///
/// The listen `ip:port` comes from the boot config (`ctx.boot.bind`/`ctx.info.tcp_port`).
/// A standalone Redis reports itself as `myself,master` with a `connected` link state and
/// no assigned slots, which is what we render.
fn cluster_nodes(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|nodes"));
    }
    let id = ctx.info.cluster_node_id;
    let ip = ctx.boot.bind;
    let port = ctx.info.tcp_port;
    // The cluster bus port is the listen port + 10000 (Redis's fixed offset). The
    // trailing `\n` (NOT `\r\n`) terminates each node line in the Redis NODES text format.
    let cport = u32::from(port) + 10_000;
    let line = format!("{id} {ip}:{port}@{cport} myself,master - 0 0 0 connected\n");
    Value::bulk(line.into_bytes())
}

/// `CLUSTER COUNTKEYSINSLOT <slot>` -> the number of keys in `<slot>` as an integer. The
/// slot is validated to be in `[0, 16384)` (else `-ERR Invalid slot`); the count is always
/// `0` because slice 1 keeps NO per-slot index (matching a non-cluster Redis, which has no
/// slot ownership). Arity exactly 3.
fn cluster_countkeysinslot(req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("cluster|countkeysinslot"));
    }
    if parse_slot(&req.args[2]).is_none() {
        return Value::error(ErrorReply::err("Invalid slot"));
    }
    Value::Integer(0)
}

/// `CLUSTER GETKEYSINSLOT <slot> <count>` -> the (up to `<count>`) keys in `<slot>` as an
/// array. The slot is validated to be in `[0, 16384)` and `<count>` to be a non-negative
/// integer (else `-ERR Invalid slot`); the result is always EMPTY because slice 1 keeps no
/// per-slot index. Arity exactly 4.
fn cluster_getkeysinslot(req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("cluster|getkeysinslot"));
    }
    if parse_slot(&req.args[2]).is_none() {
        return Value::error(ErrorReply::err("Invalid slot"));
    }
    // `count` must be a non-negative integer. Redis parses it as a long and replies
    // `-ERR Invalid number of keys` (NOT `Invalid slot`) for a non-integer or negative
    // count, so that is the wording used here.
    match parse_i64(&req.args[3]) {
        Some(n) if n >= 0 => Value::Array(Some(Vec::new())),
        _ => Value::error(ErrorReply::err("Invalid number of keys")),
    }
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

/// Parse and bounds-check a slot argument: a base-10 integer in `[0, 16384)`. Returns the
/// slot on success or `None` (the caller maps `None` to the `-ERR Invalid slot` reply). A
/// negative, non-integer, or out-of-range value is rejected, matching Redis's slot bounds.
fn parse_slot(arg: &[u8]) -> Option<u16> {
    let n = parse_i64(arg)?;
    if n >= 0 && n < i64::from(CLUSTER_SLOTS) {
        // In range, so the cast is exact (0..16383 fits a u16).
        Some(n as u16)
    } else {
        None
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

    fn bulk_string(v: &Value) -> String {
        match v {
            Value::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    #[test]
    fn keyslot_matches_crc16_and_co_locates_hash_tags() {
        let c = ctx_with(Config::default());
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
    fn myid_is_the_40_hex_node_id() {
        let c = ctx_with(Config::default());
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
    fn info_has_the_exact_disabled_lines() {
        let c = ctx_with(Config::default());
        let body = bulk_string(&run(&c, &[b"CLUSTER", b"INFO"]));
        for line in [
            "cluster_enabled:0\r\n",
            "cluster_state:ok\r\n",
            "cluster_slots_assigned:0\r\n",
            "cluster_slots_ok:0\r\n",
            "cluster_slots_pfail:0\r\n",
            "cluster_slots_fail:0\r\n",
            "cluster_known_nodes:1\r\n",
            "cluster_size:0\r\n",
            "cluster_current_epoch:0\r\n",
            "cluster_my_epoch:0\r\n",
            "cluster_stats_messages_sent:0\r\n",
            "cluster_stats_messages_received:0\r\n",
            "total_cluster_links_buffer_limit_exceeded:0\r\n",
        ] {
            assert!(body.contains(line), "INFO missing {line:?} in {body:?}");
        }
        // The first line is cluster_enabled, exactly as Redis orders it.
        assert!(body.starts_with("cluster_enabled:0\r\n"));
    }

    #[test]
    fn slots_and_shards_are_empty_arrays() {
        let c = ctx_with(Config::default());
        assert_eq!(
            run(&c, &[b"CLUSTER", b"SLOTS"]),
            Value::Array(Some(Vec::new()))
        );
        assert_eq!(
            run(&c, &[b"CLUSTER", b"SHARDS"]),
            Value::Array(Some(Vec::new()))
        );
    }

    #[test]
    fn nodes_renders_one_self_line() {
        let c = ctx_with(Config {
            port: 6390,
            ..Config::default()
        });
        let line = bulk_string(&run(&c, &[b"CLUSTER", b"NODES"]));
        // <id> <ip>:<port>@<cport> myself,master - 0 0 0 connected\n
        assert!(line.starts_with(TEST_NODE_ID), "got {line:?}");
        // cport = port + 10000 = 16390; the default bind is loopback.
        assert!(line.contains("127.0.0.1:6390@16390"), "got {line:?}");
        assert!(
            line.contains("myself,master - 0 0 0 connected"),
            "got {line:?}"
        );
        assert!(line.ends_with("connected\n"), "got {line:?}");
    }

    #[test]
    fn countkeysinslot_validates_bounds_and_returns_zero() {
        let c = ctx_with(Config::default());
        assert_eq!(
            run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"0"]),
            Value::Integer(0)
        );
        assert_eq!(
            run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"16383"]),
            Value::Integer(0)
        );
        // Out of range (>= 16384) and negative are -ERR Invalid slot.
        match run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"16384"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR Invalid slot"),
            other => panic!("expected Invalid slot, got {other:?}"),
        }
        match run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"99999"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR Invalid slot"),
            other => panic!("expected Invalid slot, got {other:?}"),
        }
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"-1"]),
            Value::Error(_)
        ));
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"COUNTKEYSINSLOT", b"abc"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn getkeysinslot_validates_and_returns_empty() {
        let c = ctx_with(Config::default());
        assert_eq!(
            run(&c, &[b"CLUSTER", b"GETKEYSINSLOT", b"0", b"10"]),
            Value::Array(Some(Vec::new()))
        );
        // Bad slot.
        match run(&c, &[b"CLUSTER", b"GETKEYSINSLOT", b"16384", b"10"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR Invalid slot"),
            other => panic!("expected Invalid slot, got {other:?}"),
        }
        // Negative count.
        match run(&c, &[b"CLUSTER", b"GETKEYSINSLOT", b"0", b"-1"]) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR Invalid number of keys"),
            other => panic!("expected Invalid number of keys, got {other:?}"),
        }
    }

    #[test]
    fn cluster_only_subcommands_are_disabled() {
        let c = ctx_with(Config::default());
        for sub in [
            b"MEET".as_slice(),
            b"ADDSLOTS",
            b"DELSLOTS",
            b"SETSLOT",
            b"FORGET",
            b"REPLICATE",
            b"FAILOVER",
            b"RESET",
            b"BUMPEPOCH",
            b"FLUSHSLOTS",
            b"SET-CONFIG-EPOCH",
            b"SLAVES",
            b"REPLICAS",
            b"LINKS",
        ] {
            match cmd_cluster(&c, &req(&[b"CLUSTER", sub])) {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-ERR This instance has cluster support disabled",
                    "subcommand {:?}",
                    String::from_utf8_lossy(sub)
                ),
                other => panic!("expected cluster-disabled error for {sub:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn help_is_an_array_and_unknown_sub_errors() {
        let c = ctx_with(Config::default());
        assert!(matches!(
            run(&c, &[b"CLUSTER", b"HELP"]),
            Value::Array(Some(_))
        ));
        match run(&c, &[b"CLUSTER", b"BOGUS"]) {
            Value::Error(e) => assert!(e.line().contains("unknown subcommand"), "got {}", e.line()),
            other => panic!("expected unknown subcommand, got {other:?}"),
        }
        // No subcommand -> wrong arity.
        assert!(matches!(run(&c, &[b"CLUSTER"]), Value::Error(_)));
    }

    #[test]
    fn info_reflects_enabled_flag_from_ctx() {
        // Sanity: if a future slice flips cluster_enabled, CLUSTER INFO reports :1. (Slice
        // 1 always boots disabled; this only checks the field is sourced from ctx.info.)
        let c = ctx_with(Config {
            cluster_enabled: true,
            ..Config::default()
        });
        let body = bulk_string(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(body.starts_with("cluster_enabled:1\r\n"), "got {body:?}");
    }
}
