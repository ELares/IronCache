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
        // Slice-3 topology-mutation subcommands: each drives a `SlotMap` mutator (the real
        // local self-formation surface). Inter-node SYNC of these is slice 3b; here a node
        // mutates only its OWN view. Cluster mode is enabled (the gate above), so `ctx.cluster`
        // is `Some` per the slice-3 boot wiring; the `None` arm is a defensive fallback.
        b"ADDSLOTS" => cluster_addslots(ctx, req),
        b"DELSLOTS" => cluster_delslots(ctx, req),
        b"ADDSLOTSRANGE" => cluster_addslotsrange(ctx, req),
        b"DELSLOTSRANGE" => cluster_delslotsrange(ctx, req),
        b"SETSLOT" => cluster_setslot(ctx, req),
        b"FLUSHSLOTS" => cluster_flushslots(ctx, req),
        b"MEET" => cluster_meet(ctx, req),
        b"FORGET" => cluster_forget(ctx, req),
        b"BUMPEPOCH" => cluster_bumpepoch(ctx, req),
        b"SET-CONFIG-EPOCH" => cluster_set_config_epoch(ctx, req),
        // Replication / failover / reset are deferred to a later slice (slice 4): IronCache has
        // no replicas yet. They keep the documented not-supported reply.
        b"REPLICATE" | b"FAILOVER" | b"RESET" => Value::error(ErrorReply::err(format!(
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

/// The `Some(map)` slot map for a mutator subcommand, or the documented not-supported error
/// when there is no map (a defensive fallback; the slice-3 boot wiring always supplies a map
/// for a cluster-enabled node).
fn cluster_map<'a>(
    ctx: &'a ServerContext,
    sub: &str,
) -> Result<&'a std::sync::Arc<ironcache_cluster::SlotMap>, Value> {
    ctx.cluster.as_ref().ok_or_else(|| {
        Value::error(ErrorReply::err(format!(
            "{sub} is not supported on a single-node cluster"
        )))
    })
}

/// Wrap a [`SlotMutError`](ironcache_cluster::SlotMutError) into the `-ERR <message>` reply.
/// `SlotMutError::Display` is byte-exact to Redis's `clusterCommand` message (cluster_legacy.c),
/// so the reply matches Redis on the parity cases (busy / unassigned / unknown-node / forget-self
/// / the epoch errors).
fn mut_err(e: &ironcache_cluster::SlotMutError) -> Value {
    Value::error(ErrorReply::err(e.to_string()))
}

/// The Redis `addReplySubcommandSyntaxError` reply for a CLUSTER subcommand whose token matched but
/// whose argument count is too small (Redis returns this class, NOT a bare wrong-arity, for an
/// under-arity mutator subcommand). Uses the raw subcommand token from `req.args[1]`, matching the
/// `_ =>` unknown-subcommand arm's call style.
fn cluster_subcommand_syntax_error(req: &Request) -> Value {
    Value::error(ErrorReply::unknown_subcommand(
        "CLUSTER",
        &String::from_utf8_lossy(&req.args[1]),
    ))
}

/// `CLUSTER ADDSLOTS <slot> [<slot> ...]` -> claim each slot for self. Arity Min(3).
fn cluster_addslots(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() < 3 {
        // Under-arity: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return cluster_subcommand_syntax_error(req);
    }
    let slots = match parse_slot_list(&req.args[2..]) {
        Ok(s) => s,
        Err(e) => return Value::error(e),
    };
    let map = match cluster_map(ctx, "ADDSLOTS") {
        Ok(m) => m,
        Err(v) => return v,
    };
    match map.add_slots(&slots) {
        Ok(()) => Value::ok(),
        Err(e) => mut_err(&e),
    }
}

/// `CLUSTER DELSLOTS <slot> [<slot> ...]` -> release each slot. Arity Min(3).
fn cluster_delslots(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() < 3 {
        // Under-arity: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return cluster_subcommand_syntax_error(req);
    }
    let slots = match parse_slot_list(&req.args[2..]) {
        Ok(s) => s,
        Err(e) => return Value::error(e),
    };
    let map = match cluster_map(ctx, "DELSLOTS") {
        Ok(m) => m,
        Err(v) => return v,
    };
    match map.del_slots(&slots) {
        Ok(()) => Value::ok(),
        Err(e) => mut_err(&e),
    }
}

/// `CLUSTER ADDSLOTSRANGE <start> <end> [<start> <end> ...]` -> claim each inclusive range for
/// self. The args after the subcommand must come in pairs (even count, >= 2). Each `start`/`end`
/// is a valid slot and `start <= end`; the full expanded slot set is added all-or-nothing.
fn cluster_addslotsrange(ctx: &ServerContext, req: &Request) -> Value {
    let slots = match parse_slot_ranges(&req.args, "cluster|addslotsrange") {
        Ok(s) => s,
        Err(e) => return Value::error(e),
    };
    let map = match cluster_map(ctx, "ADDSLOTSRANGE") {
        Ok(m) => m,
        Err(v) => return v,
    };
    match map.add_slots(&slots) {
        Ok(()) => Value::ok(),
        Err(e) => mut_err(&e),
    }
}

/// `CLUSTER DELSLOTSRANGE <start> <end> [<start> <end> ...]` -> release each inclusive range.
fn cluster_delslotsrange(ctx: &ServerContext, req: &Request) -> Value {
    let slots = match parse_slot_ranges(&req.args, "cluster|delslotsrange") {
        Ok(s) => s,
        Err(e) => return Value::error(e),
    };
    let map = match cluster_map(ctx, "DELSLOTSRANGE") {
        Ok(m) => m,
        Err(v) => return v,
    };
    match map.del_slots(&slots) {
        Ok(()) => Value::ok(),
        Err(e) => mut_err(&e),
    }
}

/// `CLUSTER SETSLOT <slot> <NODE|MIGRATING|IMPORTING> <node-id>` / `<slot> STABLE` -> flip the
/// slot's owner (NODE) or drive the HA-6 online-migration state machine (MIGRATING / IMPORTING /
/// STABLE) on the LOCAL static slot map. NODE/MIGRATING/IMPORTING take a node id (argc == 5); STABLE
/// takes none (argc == 4). In RAFT mode these become committed proposals (handled in the serve
/// router, not here); this static-path arm mutates the node-local view directly (slice 3 semantics),
/// now extended with the migration verbs since the `SlotMap` carries the (additive, owns()-inert)
/// migration state.
fn cluster_setslot(ctx: &ServerContext, req: &Request) -> Value {
    // SETSLOT <slot> <subcmd> ... : the shortest form (STABLE) is 4 args.
    if req.args.len() < 4 {
        // Under-arity base guard: the addReplySubcommandSyntaxError class (Redis parity).
        return cluster_subcommand_syntax_error(req);
    }
    let slot = match parse_slot_strict(&req.args[2]) {
        Ok(s) => s,
        Err(e) => return Value::error(e),
    };
    let setsub = ascii_upper(&req.args[3]);
    // Any unmatched action OR a matched action at the WRONG argc is the single Redis message
    // (cluster_legacy.c SETSLOT: `Invalid CLUSTER SETSLOT action or number of arguments.`).
    let setslot_err = || {
        Value::error(ErrorReply::err(
            "Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP",
        ))
    };
    let map = match cluster_map(ctx, "SETSLOT") {
        Ok(m) => m,
        Err(v) => return v,
    };
    match setsub.as_slice() {
        b"NODE" if req.args.len() == 5 => {
            // The committed FLIP (also clears any in-flight migration on the slot, in the SlotMap).
            let node_id = String::from_utf8_lossy(&req.args[4]);
            match map.set_slot_node(slot, &node_id) {
                Ok(()) => Value::ok(),
                Err(e) => mut_err(&e),
            }
        }
        b"MIGRATING" if req.args.len() == 5 => {
            // HA-6 source-side: tag the slot MIGRATING toward the named dest (additive state, never
            // touches owns()). The dest must be a known node.
            let dest = String::from_utf8_lossy(&req.args[4]);
            match map.set_migrating(slot, &dest) {
                Ok(()) => Value::ok(),
                Err(e) => mut_err(&e),
            }
        }
        b"IMPORTING" if req.args.len() == 5 => {
            // HA-6 destination-side: tag the slot IMPORTING from the named src. The src must be
            // known.
            let src = String::from_utf8_lossy(&req.args[4]);
            match map.set_importing(slot, &src) {
                Ok(()) => Value::ok(),
                Err(e) => mut_err(&e),
            }
        }
        b"STABLE" if req.args.len() == 4 => {
            // HA-6: clear the slot's migration state (the abort path; always succeeds, idempotent).
            map.clear_migration(slot);
            Value::ok()
        }
        // Unknown action, or a known action at the wrong argc (incl. NODE with argc != 5).
        _ => setslot_err(),
    }
}

/// `CLUSTER FLUSHSLOTS` -> release every slot THIS node owns. Arity exactly 2.
///
/// DOCUMENTED DIVERGENCE from Redis 7.4: Redis errors `DB must be empty to perform CLUSTER
/// FLUSHSLOTS.` when the keyspace is non-empty. IronCache has no per-slot / per-DB key-count index
/// yet (the same gap COUNTKEYSINSLOT documents), so it cannot test DB-emptiness and always returns
/// `+OK`. The emptiness gate lands with the cross-shard slot index in a later slice.
fn cluster_flushslots(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        // Wrong argc: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return cluster_subcommand_syntax_error(req);
    }
    let map = match cluster_map(ctx, "FLUSHSLOTS") {
        Ok(m) => m,
        Err(v) => return v,
    };
    map.flush_slots();
    Value::ok()
}

/// `CLUSTER MEET <ip> <port> [<bus-port>]` -> add the named node to this node's table. Slice 3
/// records the node locally (no handshake / gossip yet, slice 3b). Arity 4 or 5 (the optional
/// cluster-bus port is accepted but unused). The new node's id is unknown until gossip, so a
/// deterministic placeholder id is synthesized from the endpoint so the entry is addressable by
/// `SETSLOT` / `FORGET` in this slice (documented divergence).
fn cluster_meet(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 4 && req.args.len() != 5 {
        // Wrong argc: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return cluster_subcommand_syntax_error(req);
    }
    let host = String::from_utf8_lossy(&req.args[2]).into_owned();
    let port_arg = String::from_utf8_lossy(&req.args[3]).into_owned();
    // Non-parseable base port -> Redis `Invalid base port specified: %s` (uses the raw arg).
    let Some(port) = parse_i64(&req.args[3]) else {
        return Value::error(ErrorReply::err(format!(
            "Invalid base port specified: {port_arg}"
        )));
    };
    // The optional cluster-bus port (arg 4) is parsed for validity but unused (no bus yet).
    // Non-parseable bus port -> Redis `Invalid bus port specified: %s` (uses the raw arg).
    if req.args.len() == 5 && parse_i64(&req.args[4]).is_none() {
        let bus_arg = String::from_utf8_lossy(&req.args[4]).into_owned();
        return Value::error(ErrorReply::err(format!(
            "Invalid bus port specified: {bus_arg}"
        )));
    }
    // Out-of-range base port (or any otherwise-invalid address/port) -> Redis
    // `Invalid node address specified: %s:%s` (the ip:port).
    if !(1..=65535).contains(&port) {
        return Value::error(ErrorReply::err(format!(
            "Invalid node address specified: {host}:{port_arg}"
        )));
    }
    let map = match cluster_map(ctx, "MEET") {
        Ok(m) => m,
        Err(v) => return v,
    };
    let id = synth_node_id(&host, port as u16);
    map.meet(ironcache_cluster::NodeEntry {
        id: id.into_boxed_str(),
        host: host.into_boxed_str(),
        port: port as u16,
    });
    Value::ok()
}

/// `CLUSTER FORGET <node-id>` -> remove the node from this node's table (reindexing ownership).
/// Arity exactly 3.
fn cluster_forget(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 3 {
        // Wrong argc: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return cluster_subcommand_syntax_error(req);
    }
    let node_id = String::from_utf8_lossy(&req.args[2]);
    let map = match cluster_map(ctx, "FORGET") {
        Ok(m) => m,
        Err(v) => return v,
    };
    match map.forget(&node_id) {
        Ok(()) => Value::ok(),
        Err(e) => mut_err(&e),
    }
}

/// `CLUSTER BUMPEPOCH` -> conditionally advance the config epoch (Redis's
/// `clusterBumpConfigEpochWithoutConsensus`): `+BUMPED <epoch>` on a real bump, `+STILL <epoch>`
/// when the epoch was already the cluster max. Arity exactly 2.
fn cluster_bumpepoch(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        // Under-arity: the addReplySubcommandSyntaxError class, not a bare wrong-arity (Redis
        // parity for a matched-but-malformed CLUSTER subcommand).
        return Value::error(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&req.args[1]),
        ));
    }
    let map = match cluster_map(ctx, "BUMPEPOCH") {
        Ok(m) => m,
        Err(v) => return v,
    };
    // Redis replies the status `+BUMPED <epoch>` on a real bump and `+STILL <epoch>` otherwise.
    match map.bump_epoch() {
        ironcache_cluster::BumpEpoch::Bumped(epoch) => Value::simple(&format!("BUMPED {epoch}")),
        ironcache_cluster::BumpEpoch::Still(epoch) => Value::simple(&format!("STILL {epoch}")),
    }
}

/// `CLUSTER SET-CONFIG-EPOCH <epoch>` -> set this node's config epoch (only when fresh and
/// alone). Arity exactly 3. A negative epoch is the Redis `Invalid config epoch specified:`
/// error.
fn cluster_set_config_epoch(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 3 {
        // Wrong argc: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return cluster_subcommand_syntax_error(req);
    }
    let Some(epoch) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if epoch < 0 {
        // Redis: `addReplyErrorFormat(c,"Invalid config epoch specified: %lld",epoch)`.
        return Value::error(ErrorReply::err(format!(
            "Invalid config epoch specified: {epoch}"
        )));
    }
    let map = match cluster_map(ctx, "SET-CONFIG-EPOCH") {
        Ok(m) => m,
        Err(v) => return v,
    };
    match map.set_config_epoch(epoch as u64) {
        Ok(()) => Value::ok(),
        Err(e) => mut_err(&e),
    }
}

/// Parse a list of slot arguments for the MUTATOR paths (ADDSLOTS / DELSLOTS), each
/// `parse_slot_strict`-validated (the single `Invalid or out of range slot` message Redis's
/// `getSlotOrReply` emits for both a non-integer and an out-of-range value). Returns the first
/// error encountered, matching Redis's left-to-right validation.
fn parse_slot_list(args: &[bytes::Bytes]) -> Result<Vec<u16>, ErrorReply> {
    args.iter().map(|a| parse_slot_strict(a)).collect()
}

/// Parse the `<start> <end> [<start> <end> ...]` pairs of ADDSLOTSRANGE / DELSLOTSRANGE into the
/// expanded, deduplicated-by-position slot list. The args after the subcommand must be a non-empty
/// EVEN count (>= 2). Each `start`/`end` is a valid slot (strict getSlotOrReply parse) and
/// `start <= end`. `cmd` is the wrong-arity command label (e.g. `cluster|addslotsrange`).
///
/// Two distinct argc errors, matching Redis (cluster_legacy.c ADDSLOTSRANGE/DELSLOTSRANGE):
/// * UNDER-arity (fewer than the 4-arg minimum `CLUSTER <sub> <start> <end>`, i.e. no pair at all)
///   -> the `addReplySubcommandSyntaxError` class (a matched-but-malformed subcommand), like the
///   other under-arity mutator guards;
/// * an ODD number of range args while >= 4 (`c->argc % 2 == 1`) -> Redis calls `addReplyErrorArity`
///   (a real wrong-arity), so this case keeps `wrong_arity(cmd)`.
fn parse_slot_ranges(args: &[bytes::Bytes], cmd: &str) -> Result<Vec<u16>, ErrorReply> {
    // args[0]=CLUSTER, args[1]=subcommand; the range pairs start at args[2].
    let pairs = &args[2..];
    if pairs.is_empty() {
        // Under-arity (no <start> <end> pair): the subcommand-syntax-error class.
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&args[1]),
        ));
    }
    if pairs.len() % 2 != 0 {
        // >= 4 args but an odd count: Redis's addReplyErrorArity (a real wrong-arity).
        return Err(ErrorReply::wrong_arity(cmd));
    }
    let mut out = Vec::new();
    for pair in pairs.chunks_exact(2) {
        let start = parse_slot_strict(&pair[0])?;
        let end = parse_slot_strict(&pair[1])?;
        if start > end {
            // Redis: `start slot number %d is greater than end slot number %d`.
            return Err(ErrorReply::err(format!(
                "start slot number {start} is greater than end slot number {end}"
            )));
        }
        out.extend(start..=end);
    }
    Ok(out)
}

/// Synthesize a deterministic 40-lowercase-hex placeholder node id from a MEET endpoint, so the
/// MEET'd node is addressable by `SETSLOT` / `FORGET` before gossip learns its real id (slice 3b).
/// DOCUMENTED DIVERGENCE: real Redis learns the peer's actual id over the cluster bus; with no bus
/// in slice 3 we derive a stable id from `host:port` (FNV-1a over the endpoint, hex-padded to 40).
fn synth_node_id(host: &str, port: u16) -> String {
    // FNV-1a 64-bit over "host:port" (deterministic, no rand/time, ADR-0003), rendered as hex and
    // repeated to fill the 40-hex node-id width.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let endpoint = format!("{host}:{port}");
    for b in endpoint.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let hex16 = format!("{h:016x}");
    // 16 hex chars per hash; concatenate three copies and truncate to exactly 40.
    let mut id = String::with_capacity(40);
    while id.len() < 40 {
        id.push_str(&hex16);
    }
    id.truncate(40);
    id
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

// NOTE (slice 3): `CLUSTER MYID` and the SLOTS/SHARDS/NODES projection now read OWNED snapshots
// (`map.me()` clones the self entry, `map.nodes()` clones the table) because slice 3 made the node
// table mutable behind a lock; each projection takes ONE `nodes()` snapshot and pairs it with ONE
// `ranges()` read so the two stay self-consistent.

/// `CLUSTER INFO` -> the cluster status as a RESP3 verbatim string (txt) with the exact
/// `field:value` lines a real Redis emits (each `\r\n`-terminated). Arity exactly 2.
///
/// Reachable only when cluster mode is ENABLED (the disabled gate in [`cmd_cluster`] runs first).
/// The counts/epochs/state come from the live map (slice 3: always present when cluster mode is on):
/// `cluster_slots_assigned` / `cluster_known_nodes` / `cluster_size` from the map, `cluster_state`
/// is `ok` iff every slot is owned (a fresh / mid-formation node is `fail`), and the epochs from
/// `current_epoch()` / `my_epoch()`. Message counters are zero (no gossip yet). Redis replies
/// CLUSTER INFO via `addReplyVerbatim(..., "txt")`, so this is a `VerbatimString` (it degrades to a
/// bulk string under RESP2 automatically).
fn cluster_info(ctx: &ServerContext, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("cluster|info"));
    }
    // Map-driven counts/epochs/state when a map is present (slice 3: always present when cluster
    // mode is enabled); else the slice-1 single-node-owns-all picture with `None` epochs.
    let info = match &ctx.cluster {
        Some(map) => ClusterInfoFields {
            slots_assigned: map.slots_assigned(),
            known_nodes: map.known_nodes(),
            cluster_size: map.cluster_size(),
            // A node with every slot owned is `ok`; a fresh / mid-formation node is `fail`
            // (matches real Redis, which is `fail` until all 16384 slots are assigned).
            state_ok: map.is_fully_assigned(),
            current_epoch: map.current_epoch(),
            my_epoch: map.my_epoch(),
        },
        None => ClusterInfoFields {
            slots_assigned: u32::from(CLUSTER_SLOTS),
            known_nodes: 1,
            cluster_size: 1,
            state_ok: true,
            current_epoch: 0,
            my_epoch: 0,
        },
    };
    verbatim_txt(cluster_info_body(&info).as_bytes())
}

/// The `CLUSTER INFO` fields that vary between the map-driven and single-node paths.
struct ClusterInfoFields {
    slots_assigned: u32,
    known_nodes: usize,
    cluster_size: usize,
    state_ok: bool,
    current_epoch: u64,
    my_epoch: u64,
}

/// Build the `CLUSTER INFO` `field:value` body (each line `\r\n`-terminated) from the resolved
/// fields, shared by the map-driven and single-node paths. The message counters are zero (no
/// gossip yet); `cluster_state` is `ok` iff every slot is assigned.
fn cluster_info_body(f: &ClusterInfoFields) -> String {
    let state = if f.state_ok { "ok" } else { "fail" };
    let slots_assigned = f.slots_assigned;
    let known_nodes = f.known_nodes;
    let cluster_size = f.cluster_size;
    let current_epoch = f.current_epoch;
    let my_epoch = f.my_epoch;
    format!(
        "cluster_enabled:1\r\n\
         cluster_state:{state}\r\n\
         cluster_slots_assigned:{slots_assigned}\r\n\
         cluster_slots_ok:{slots_assigned}\r\n\
         cluster_slots_pfail:0\r\n\
         cluster_slots_fail:0\r\n\
         cluster_known_nodes:{known_nodes}\r\n\
         cluster_size:{cluster_size}\r\n\
         cluster_current_epoch:{current_epoch}\r\n\
         cluster_my_epoch:{my_epoch}\r\n\
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
            // ONE owned snapshot of the node table, paired with ONE ranges() read; the
            // `owner` index from ranges() indexes this snapshot.
            let nodes = map.nodes();
            let ranges = map
                .ranges()
                .into_iter()
                .filter_map(|(start, end, idx)| {
                    let n = nodes.get(idx)?;
                    let served = served_by_triple(&n.host, n.port, &n.id);
                    Some(Value::Array(Some(vec![
                        Value::Integer(i64::from(start)),
                        Value::Integer(i64::from(end)),
                        served,
                    ])))
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
            // One shard map per slot-OWNING node. Collect each node's coalesced ranges from
            // `map.ranges()`, emit `{slots => [s0,e0, s1,e1, ...], nodes => [<master>, <replica>*]}`.
            // The master node entry reports its REAL replication-offset + health (HA-7e); each
            // node that REPLICATES one of the shard's slots is appended as a `role => replica`
            // entry. In static mode (no replicas assigned, no repl status cell) this reduces to
            // exactly the slice-2/3 projection: one master per shard at offset 0, health online.
            let ranges = map.ranges();
            let nodes = map.nodes();
            // THIS node's live replication status, used to fill the real offset/health/role for
            // the self entry (a node has live repl state only for ITSELF; peers keep the
            // projection default). `None` -> the byte-compatible master/0/online default. Self is
            // identified by the MAP's own `me()` id (the authority on which table entry is this
            // node), so the status is applied to the right entry even if the same physical node
            // appears under more than one id.
            let self_status = ctx.repl_status.as_ref().map(|s| s.snapshot());
            let self_id = map.me().id;
            let shards = nodes
                .iter()
                .enumerate()
                .filter_map(|(idx, n)| {
                    // The flat [start, end] integer pairs for THIS owner node, in slot order.
                    let mut slots = Vec::new();
                    let mut owned_slots: Vec<u16> = Vec::new();
                    for &(start, end, owner) in &ranges {
                        if owner == idx {
                            slots.push(Value::Integer(i64::from(start)));
                            slots.push(Value::Integer(i64::from(end)));
                            owned_slots.push(start);
                        }
                    }
                    // A node that owns no slots is not a shard MASTER (it appears, if at all, as a
                    // replica under the shard it mirrors); skip it here.
                    if slots.is_empty() {
                        return None;
                    }
                    // The master node entry: real status iff it is self, else the projection default.
                    let mut shard_nodes = vec![shard_node_entry(
                        n,
                        ShardRole::Master,
                        node_status_for(n, &self_id, self_status.as_ref()),
                    )];
                    // Append each node that REPLICATES one of this shard's slots (HA-7d). The MVP
                    // single-replica-per-slot means at most one replica per slot; dedup so a node
                    // replicating several of the shard's slots appears once.
                    let mut seen_replicas: Vec<usize> = Vec::new();
                    let rep_slot = owned_slots.first().copied();
                    if let Some(slot) = rep_slot {
                        for rep_idx in map.replicas_of(slot) {
                            let rep_idx = rep_idx as usize;
                            if seen_replicas.contains(&rep_idx) {
                                continue;
                            }
                            seen_replicas.push(rep_idx);
                            if let Some(rep) = nodes.get(rep_idx) {
                                shard_nodes.push(shard_node_entry(
                                    rep,
                                    ShardRole::Replica,
                                    node_status_for(rep, &self_id, self_status.as_ref()),
                                ));
                            }
                        }
                    }
                    Some(Value::Map(vec![
                        (Value::bulk_str("slots"), Value::Array(Some(slots))),
                        (Value::bulk_str("nodes"), Value::Array(Some(shard_nodes))),
                    ]))
                })
                .collect();
            Value::Array(Some(shards))
        }
        None => cluster_shards_singlenode(ctx),
    }
}

/// The role a node plays WITHIN a CLUSTER SHARDS shard map (HA-7e): the shard's master, or one of
/// its replicas. Distinct from the node's globally-observed [`ironcache_repl::ReplRole`]; here it
/// is the structural position in the projection (the owner is the master, an assigned replica is a
/// replica).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShardRole {
    Master,
    Replica,
}

/// The REAL replication offset + health a node reports in CLUSTER SHARDS (HA-7e). For THIS node it
/// comes from the live repl status; for a peer (no live data) it is the projection default
/// (offset 0, online).
#[derive(Debug, Clone, Copy)]
struct NodeShardStatus {
    /// The node's replication offset (CLUSTER SHARDS `replication-offset`).
    offset: i64,
    /// The node's health (`online` | `loading` | `fail`).
    health: &'static str,
}

impl NodeShardStatus {
    /// The projection DEFAULT for a node this server has no live repl data for (a peer, or any
    /// node in static mode): offset 0, online. This is the byte-compatible slice-2/3 value.
    fn default_online() -> Self {
        NodeShardStatus {
            offset: 0,
            health: "online",
        }
    }
}

/// Resolve a node's CLUSTER SHARDS replication status (HA-7e): the node's REAL offset + health
/// when it is THIS node (from the live repl snapshot), else the projection default (offset 0,
/// online), since a node has live replication state only for itself.
///
/// For self as a MASTER: offset = its head, health online. For self as a REPLICA: offset = its
/// applied offset; health is `online` while the master link is up, `loading` while it is down (it
/// is re-syncing / catching up), matching Redis's `loading` health for a replica that is not
/// caught up. (`fail` is reserved for a node believed down by the cluster, which the single-node
/// repl status cannot determine alone; a master-believed-down judgment is HA-8 territory.)
fn node_status_for(
    node: &ironcache_cluster::NodeEntry,
    self_id: &str,
    self_status: Option<&ironcache_repl::ReplStatusSnapshot>,
) -> NodeShardStatus {
    // Only THIS node's entry gets real status; match by node id.
    if node.id.as_ref() != self_id {
        return NodeShardStatus::default_online();
    }
    let Some(snap) = self_status else {
        return NodeShardStatus::default_online();
    };
    match snap.role {
        ironcache_repl::ReplRole::Master => NodeShardStatus {
            offset: offset_to_i64(snap.node_offset.0),
            health: "online",
        },
        ironcache_repl::ReplRole::Replica => NodeShardStatus {
            offset: offset_to_i64(snap.node_offset.0),
            // A replica is `online` once its link is up + applying; `loading` while the link is
            // down (re-dialing / re-syncing, not yet caught up).
            health: if snap.master_link.is_up() {
                "online"
            } else {
                "loading"
            },
        },
    }
}

/// Clamp a `u64` replication offset to the `i64` the RESP Integer carries (saturating at
/// `i64::MAX`, an offset never reached in practice). CLUSTER SHARDS `replication-offset` is a
/// signed integer in the Redis reply.
fn offset_to_i64(offset: u64) -> i64 {
    i64::try_from(offset).unwrap_or(i64::MAX)
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

/// A `CLUSTER SHARDS` node map for a node ENTRY (HA-7e): the `role` (master | replica), the REAL
/// `replication-offset`, and the `health` (online | loading | fail) supplied in `status`. The
/// field set AND order are byte-faithful to Redis's `addNodeDetailsToShardReply`: id, port,
/// tls-port, ip, endpoint, role, replication-offset, health. `tls-port` is `0` (TLS off); `ip`
/// and `endpoint` are both the advertised host.
///
/// A master with `status = NodeShardStatus::default_online()` renders EXACTLY the slice-2/3 bytes
/// (role master, replication-offset 0, health online), so the static-mode projection is unchanged.
fn shard_node_entry(
    node: &ironcache_cluster::NodeEntry,
    role: ShardRole,
    status: NodeShardStatus,
) -> Value {
    let role_str = match role {
        ShardRole::Master => "master",
        ShardRole::Replica => "replica",
    };
    Value::Map(vec![
        (
            Value::bulk_str("id"),
            Value::bulk(node.id.as_bytes().to_vec()),
        ),
        (
            Value::bulk_str("port"),
            Value::Integer(i64::from(node.port)),
        ),
        // tls-port: 0 (TLS off). Real Redis emits this right after `port`.
        (Value::bulk_str("tls-port"), Value::Integer(0)),
        (
            Value::bulk_str("ip"),
            Value::bulk(node.host.as_bytes().to_vec()),
        ),
        (
            Value::bulk_str("endpoint"),
            Value::bulk(node.host.as_bytes().to_vec()),
        ),
        (Value::bulk_str("role"), Value::bulk_str(role_str)),
        (
            Value::bulk_str("replication-offset"),
            Value::Integer(status.offset),
        ),
        (Value::bulk_str("health"), Value::bulk_str(status.health)),
    ])
}

/// The single-node `CLUSTER SHARDS` node map: a master at `0` replication offset, `health:
/// online`. Builds an owned [`NodeEntry`] from the `(host, port, id)` triple and delegates to
/// [`shard_node_entry`] with the default-online status, so the single-node bytes match Redis (and
/// the multi-node master-default path) exactly.
fn shard_node_map(host: &str, port: u16, id: &str) -> Value {
    let node = ironcache_cluster::NodeEntry {
        id: id.into(),
        host: host.into(),
        port,
    };
    shard_node_entry(&node, ShardRole::Master, NodeShardStatus::default_online())
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
    // One gossip line per node, in table order; THIS node is flagged `myself,master`, the
    // others `master`. Each node's served slot ranges (its coalesced runs from `map.ranges()`)
    // are space-joined as the trailing field. ONE owned `nodes()` snapshot + ONE `ranges()`
    // read, paired so the `owner` index from ranges() indexes this snapshot.
    let ranges = map.ranges();
    let nodes = map.nodes();
    let self_id = map.me().id;
    let mut text = String::new();
    for (node_idx, n) in nodes.iter().enumerate() {
        let flags = if n.id == self_id {
            "myself,master"
        } else {
            "master"
        };
        let owned: Vec<String> = ranges
            .iter()
            .filter(|&&(_, _, idx)| idx == node_idx)
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

/// Parse and bounds-check a slot the way Redis's `getSlotOrReply` does for the MUTATOR paths
/// (ADDSLOTS / DELSLOTS / SETSLOT / ADDSLOTSRANGE / DELSLOTSRANGE). Unlike [`parse_slot`] (the
/// COUNTKEYSINSLOT / GETKEYSINSLOT path), Redis's `getSlotOrReply` returns the SINGLE message
/// `Invalid or out of range slot` for BOTH a non-integer arg AND an out-of-range value (it does
/// `getLongLongFromObject ... || slot < 0 || slot >= CLUSTER_SLOTS` and replies once).
fn parse_slot_strict(arg: &[u8]) -> Result<u16, ErrorReply> {
    match parse_i64(arg) {
        Some(n) if (0..i64::from(CLUSTER_SLOTS)).contains(&n) => Ok(n as u16),
        // Non-integer OR out of range -> the one getSlotOrReply error.
        _ => Err(ErrorReply::err("Invalid or out of range slot")),
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
            // No raft handle: the cmd_cluster unit tests exercise the STATIC path only (the
            // raft-mode proposal interception lives in serve.rs, tested over real sockets).
            raft: None,
            repl_status: None,
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
        // tls-port: 0 (TLS off), emitted right after port (Redis field order/fidelity).
        assert_eq!(field("tls-port"), Some(Value::Integer(0)));
        assert_eq!(field("role"), Some(Value::bulk_str("master")));
        assert_eq!(field("health"), Some(Value::bulk_str("online")));
        // Field set AND order match Redis `addNodeDetailsToShardReply`.
        let field_names: Vec<Vec<u8>> = node
            .iter()
            .map(|(k, _)| match k {
                Value::BulkString(Some(b)) => b.to_vec(),
                other => panic!("non-bulk field key: {other:?}"),
            })
            .collect();
        assert_eq!(
            field_names,
            vec![
                b"id".to_vec(),
                b"port".to_vec(),
                b"tls-port".to_vec(),
                b"ip".to_vec(),
                b"endpoint".to_vec(),
                b"role".to_vec(),
                b"replication-offset".to_vec(),
                b"health".to_vec(),
            ]
        );
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

    /// REPLICATE / FAILOVER / RESET (replication / failover, deferred to slice 4) still return
    /// the documented `-ERR <SUBCOMMAND> is not supported on a single-node cluster`. They are
    /// reachable only because cluster mode is ENABLED here; when DISABLED they hit the
    /// cluster-disabled gate instead. (The slot/membership/epoch mutators are now SUPPORTED in
    /// slice 3 and tested in the empty-self section below.)
    #[test]
    fn enabled_replication_subcommands_are_not_supported() {
        let c = enabled(6390);
        for sub in [b"REPLICATE".as_slice(), b"FAILOVER", b"RESET"] {
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
        assert_eq!(field("tls-port"), Some(Value::Integer(0)));
        assert_eq!(field("ip"), Some(Value::bulk_str("10.0.0.11")));
        assert_eq!(field("role"), Some(Value::bulk_str("master")));
        assert_eq!(field("health"), Some(Value::bulk_str("online")));
    }

    /// A small helper to read a node map's field by name.
    fn node_field(node: &Value, name: &str) -> Option<Value> {
        let Value::Map(fields) = node else {
            panic!("expected a node map, got {node:?}");
        };
        fields
            .iter()
            .find(|(k, _)| *k == Value::bulk_str(name))
            .map(|(_, v)| v.clone())
    }

    /// HA-7e: with a node-level repl status cell present and THIS node (ID1) advertising a real
    /// MASTER head, its CLUSTER SHARDS node entry reports that REAL replication-offset (not the
    /// hard-coded 0) + health online. The OTHER nodes (no live data) keep the offset-0 default.
    #[test]
    fn shards_reports_real_master_offset_for_self() {
        let mut c = ctx_with_map(); // self == ID1, the middle shard
        let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
        status.set_master_head(ironcache_repl::ReplOffset(4242));
        c.repl_status = Some(status);

        let Value::Array(Some(shards)) = run(&c, &[b"CLUSTER", b"SHARDS"]) else {
            panic!("expected an array");
        };
        // ID1 (self) reports the real offset 4242 + online; ID0/ID2 stay at the default 0.
        let nodes_of = |shard: &Value| -> Vec<Value> {
            let Value::Map(s) = shard else { panic!() };
            let Value::Array(Some(ns)) = &s
                .iter()
                .find(|(k, _)| *k == Value::bulk_str("nodes"))
                .unwrap()
                .1
            else {
                panic!()
            };
            ns.clone()
        };
        let self_node = &nodes_of(&shards[1])[0];
        assert_eq!(node_field(self_node, "id"), Some(Value::bulk_str(MAP_ID1)));
        assert_eq!(
            node_field(self_node, "role"),
            Some(Value::bulk_str("master"))
        );
        assert_eq!(
            node_field(self_node, "replication-offset"),
            Some(Value::Integer(4242))
        );
        assert_eq!(
            node_field(self_node, "health"),
            Some(Value::bulk_str("online"))
        );
        // A PEER (ID0) keeps the offset-0 default (this node has no live data for it).
        let peer = &nodes_of(&shards[0])[0];
        assert_eq!(
            node_field(peer, "replication-offset"),
            Some(Value::Integer(0))
        );
    }

    /// HA-7e: when THIS node (ID1) is a committed REPLICA of another node's slot AND its repl
    /// status says role=replica, the replica appears as a `role => replica` node entry under the
    /// owning master's shard, reporting its applied offset + health (online while the link is up,
    /// loading while down). The master entry stays a master. This exercises the
    /// raft-mode-with-a-replica projection (the static-mode byte shape is unchanged: no replicas
    /// assigned -> one node per shard).
    #[test]
    fn shards_reports_replica_node_entry_with_offset_and_health() {
        let c = ctx_with_map(); // self == ID1
        // Commit "self (ID1) replicates a slot OWNED by ID0 (the 0-5460 shard)", exactly as the
        // ConfigSm apply does. ID0 owns slot 0.
        let map = c.cluster.as_ref().unwrap();
        map.set_slot_replica(0, MAP_ID1).expect("known node");

        // Publish this node's REPLICA status: attached to ID0's endpoint, link up, applied 77.
        let mut c = c;
        let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
        status.set_replica_attached("10.0.0.10", 7000, ironcache_repl::ReplOffset(70));
        status.set_observed_master_head(ironcache_repl::ReplOffset(80));
        status.set_replica_applied(ironcache_repl::ReplOffset(77));
        c.repl_status = Some(status);

        let Value::Array(Some(shards)) = run(&c, &[b"CLUSTER", b"SHARDS"]) else {
            panic!("expected an array");
        };
        // Find the shard owned by ID0 (slots [0, 5460]); it now has a master (ID0) + a replica
        // (ID1, self).
        let id0_shard = shards
            .iter()
            .find(|sh| {
                let Value::Map(s) = sh else { return false };
                let Value::Array(Some(ns)) = &s
                    .iter()
                    .find(|(k, _)| *k == Value::bulk_str("nodes"))
                    .unwrap()
                    .1
                else {
                    return false;
                };
                node_field(&ns[0], "id") == Some(Value::bulk_str(MAP_ID0))
            })
            .expect("ID0's shard");
        let Value::Map(s) = id0_shard else { panic!() };
        let Value::Array(Some(nodes)) = &s
            .iter()
            .find(|(k, _)| *k == Value::bulk_str("nodes"))
            .unwrap()
            .1
        else {
            panic!()
        };
        assert_eq!(nodes.len(), 2, "the master + its one replica");
        // The master entry: ID0, role master.
        assert_eq!(node_field(&nodes[0], "id"), Some(Value::bulk_str(MAP_ID0)));
        assert_eq!(
            node_field(&nodes[0], "role"),
            Some(Value::bulk_str("master"))
        );
        // The replica entry: ID1 (self), role replica, applied offset 77, health online (link up).
        let rep = &nodes[1];
        assert_eq!(node_field(rep, "id"), Some(Value::bulk_str(MAP_ID1)));
        assert_eq!(node_field(rep, "role"), Some(Value::bulk_str("replica")));
        assert_eq!(
            node_field(rep, "replication-offset"),
            Some(Value::Integer(77))
        );
        assert_eq!(node_field(rep, "health"), Some(Value::bulk_str("online")));

        // When the link drops, the replica reports `loading` (re-syncing, not caught up).
        let mut c2 = c;
        let status2 = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
        status2.set_replica_attached("10.0.0.10", 7000, ironcache_repl::ReplOffset(70));
        status2.set_replica_applied(ironcache_repl::ReplOffset(77));
        status2.set_master_link_down();
        c2.repl_status = Some(status2);
        let Value::Array(Some(shards2)) = run(&c2, &[b"CLUSTER", b"SHARDS"]) else {
            panic!();
        };
        let id0_shard2 = shards2
            .iter()
            .find_map(|sh| {
                let Value::Map(s) = sh else { return None };
                let Value::Array(Some(ns)) = &s
                    .iter()
                    .find(|(k, _)| *k == Value::bulk_str("nodes"))
                    .unwrap()
                    .1
                else {
                    return None;
                };
                (node_field(&ns[0], "id") == Some(Value::bulk_str(MAP_ID0))).then(|| ns.clone())
            })
            .expect("ID0's shard");
        assert_eq!(
            node_field(&id0_shard2[1], "health"),
            Some(Value::bulk_str("loading")),
            "a replica with a down link reports loading"
        );
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

    // ----- SLICE 3: runtime self-formation (empty-self map + mutator dispatch) -----

    /// A cluster-ENABLED ctx carrying a fresh EMPTY-SELF map (self owns ZERO slots), the slice-3
    /// boot state of a cluster-enabled node with no static topology. Returns the ctx AND the
    /// shared `Arc<SlotMap>` so a test can build a SECOND ctx clone over the SAME map (the
    /// cross-shard-visibility test).
    fn ctx_empty_self() -> (ServerContext, std::sync::Arc<ironcache_cluster::SlotMap>) {
        let map = std::sync::Arc::new(ironcache_cluster::SlotMap::empty_self(
            TEST_NODE_ID,
            "127.0.0.1",
            6390,
        ));
        let mut ctx = ctx_with(Config {
            cluster_enabled: true,
            ..Config::default()
        });
        ctx.cluster = Some(map.clone());
        (ctx, map)
    }

    /// Assert a `Value` is `+OK`.
    fn assert_ok(v: &Value, ctx: &str) {
        assert_eq!(*v, Value::ok(), "{ctx}: expected +OK, got {v:?}");
    }

    /// The `-ERR <message>` line of an error reply, or panic.
    fn err_line(v: &Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    /// Drive the documented single-node create sequence on ONE empty-self node: MEET peers, claim
    /// the slot space in two ADDSLOTSRANGE halves, then assert SLOTS / INFO converge.
    #[test]
    fn empty_self_create_sequence_converges_to_full_ownership() {
        let (c, _map) = ctx_empty_self();

        // Fresh node: owns zero slots, state:fail, my_epoch:0.
        let info0 = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(
            info0.contains("cluster_slots_assigned:0\r\n"),
            "fresh node owns zero slots: {info0:?}"
        );
        assert!(
            info0.contains("cluster_state:fail\r\n"),
            "fresh node state is fail: {info0:?}"
        );
        assert!(info0.contains("cluster_known_nodes:1\r\n"), "{info0:?}");

        // MEET two peers (the node set grows; ids are synthesized from the endpoints).
        assert_ok(
            &run(&c, &[b"CLUSTER", b"MEET", b"10.0.0.2", b"7002"]),
            "MEET peer 2",
        );
        assert_ok(
            &run(&c, &[b"CLUSTER", b"MEET", b"10.0.0.3", b"7003"]),
            "MEET peer 3",
        );
        let info_after_meet = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(
            info_after_meet.contains("cluster_known_nodes:3\r\n"),
            "two MEETs grow the node set to 3: {info_after_meet:?}"
        );

        // ADDSLOTSRANGE the first half -> +OK; INFO shows 5461 assigned, still fail.
        assert_ok(
            &run(&c, &[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"5460"]),
            "ADDSLOTSRANGE 0 5460",
        );
        let info1 = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(
            info1.contains("cluster_slots_assigned:5461\r\n"),
            "first half assigned: {info1:?}"
        );
        assert!(
            info1.contains("cluster_state:fail\r\n"),
            "partial map is fail: {info1:?}"
        );

        // CLUSTER SLOTS shows the single [0, 5460, self] range.
        let Value::Array(Some(ranges)) = run(&c, &[b"CLUSTER", b"SLOTS"]) else {
            panic!("expected a SLOTS array");
        };
        assert_eq!(ranges.len(), 1, "one owned range so far");
        let Value::Array(Some(r)) = &ranges[0] else {
            panic!("expected a range array");
        };
        assert_eq!(r[0], Value::Integer(0));
        assert_eq!(r[1], Value::Integer(5460));
        let Value::Array(Some(served)) = &r[2] else {
            panic!("expected a served-by triple");
        };
        assert_eq!(served[2], Value::bulk_str(TEST_NODE_ID), "self serves it");

        // ADDSLOTSRANGE the rest -> all 16384 assigned, state:ok.
        assert_ok(
            &run(&c, &[b"CLUSTER", b"ADDSLOTSRANGE", b"5461", b"16383"]),
            "ADDSLOTSRANGE 5461 16383",
        );
        let info2 = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(
            info2.contains("cluster_slots_assigned:16384\r\n"),
            "fully assigned: {info2:?}"
        );
        assert!(
            info2.contains("cluster_state:ok\r\n"),
            "full map is ok: {info2:?}"
        );

        // SET-CONFIG-EPOCH would now be rejected (knows other nodes), so test it on a fresh node;
        // here just exercise BUMPEPOCH, which works regardless of node count.
        let bumped = run(&c, &[b"CLUSTER", b"BUMPEPOCH"]);
        assert_eq!(
            bumped,
            Value::simple("BUMPED 1"),
            "BUMPEPOCH replies +BUMPED <epoch>"
        );
        let info3 = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(
            info3.contains("cluster_current_epoch:1\r\n"),
            "BUMPEPOCH advanced current_epoch: {info3:?}"
        );
        assert!(info3.contains("cluster_my_epoch:1\r\n"), "{info3:?}");
    }

    /// SET-CONFIG-EPOCH on a fresh, alone node sets my_epoch; an immediate BUMPEPOCH is then STILL
    /// (my_epoch already == the cluster max, Redis's clusterBumpConfigEpochWithoutConsensus).
    #[test]
    fn empty_self_set_config_epoch_then_bumpepoch() {
        let (c, _map) = ctx_empty_self();
        assert_ok(
            &run(&c, &[b"CLUSTER", b"SET-CONFIG-EPOCH", b"5"]),
            "SET-CONFIG-EPOCH 5",
        );
        let info = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(info.contains("cluster_my_epoch:5\r\n"), "{info:?}");
        assert!(info.contains("cluster_current_epoch:5\r\n"), "{info:?}");
        // my_epoch (5) is already the cluster max, so BUMPEPOCH replies STILL 5 (no change).
        assert_eq!(
            run(&c, &[b"CLUSTER", b"BUMPEPOCH"]),
            Value::simple("STILL 5")
        );
        let info2 = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(
            info2.contains("cluster_my_epoch:5\r\n"),
            "STILL leaves my_epoch at 5: {info2:?}"
        );
        assert!(
            info2.contains("cluster_current_epoch:5\r\n"),
            "STILL leaves current_epoch at 5: {info2:?}"
        );
    }

    /// Byte-exact Redis error parity for the mutator rejections.
    #[test]
    fn mutator_errors_are_byte_exact_to_redis() {
        let (c, _map) = ctx_empty_self();
        // ADDSLOTS then re-ADD the same slot -> `Slot N is already busy`.
        assert_ok(&run(&c, &[b"CLUSTER", b"ADDSLOTS", b"100"]), "ADDSLOTS 100");
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"ADDSLOTS", b"100"])),
            "-ERR Slot 100 is already busy"
        );
        // DELSLOTS an unassigned slot -> `Slot N is already unassigned`.
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"DELSLOTS", b"200"])),
            "-ERR Slot 200 is already unassigned"
        );
        // FORGET self -> the exact "can't forget myself" line.
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"FORGET", TEST_NODE_ID.as_bytes()])),
            "-ERR I tried hard but I can't forget myself..."
        );
        // FORGET unknown node -> `Unknown node <id>`.
        let unknown = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"FORGET", unknown.as_bytes()])),
            format!("-ERR Unknown node {unknown}")
        );
        // SETSLOT <slot> NODE <unknown> -> `Unknown node <id>`.
        assert_eq!(
            err_line(&run(
                &c,
                &[b"CLUSTER", b"SETSLOT", b"0", b"NODE", unknown.as_bytes()]
            )),
            format!("-ERR Unknown node {unknown}")
        );
        // Out-of-range slot in a MUTATOR (ADDSLOTS) -> the single getSlotOrReply message
        // `Invalid or out of range slot` (NOT the COUNTKEYSINSLOT "Invalid slot").
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"ADDSLOTS", b"99999"])),
            "-ERR Invalid or out of range slot"
        );
        // A NON-integer slot in a mutator gets the SAME single message (getSlotOrReply: one error
        // for both a non-integer and an out-of-range value).
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"ADDSLOTS", b"abc"])),
            "-ERR Invalid or out of range slot"
        );
        // The same single message on a DELSLOTS mutator (non-integer) and SETSLOT (out of range).
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"DELSLOTS", b"xyz"])),
            "-ERR Invalid or out of range slot"
        );
        assert_eq!(
            err_line(&run(
                &c,
                &[
                    b"CLUSTER",
                    b"SETSLOT",
                    b"99999",
                    b"NODE",
                    TEST_NODE_ID.as_bytes()
                ]
            )),
            "-ERR Invalid or out of range slot"
        );
        // A slot named TWICE in one ADD/DEL batch -> `Slot N specified multiple times`.
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"ADDSLOTS", b"5", b"5"])),
            "-ERR Slot 5 specified multiple times"
        );
        // ADDSLOTSRANGE with start > end -> the Redis range error.
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"ADDSLOTSRANGE", b"50", b"10"])),
            "-ERR start slot number 50 is greater than end slot number 10"
        );
    }

    /// FINDING 3: an under-arity mutator subcommand (token matches, argc too small) is the
    /// addReplySubcommandSyntaxError class, NOT a bare wrong-arity. Redis:
    /// `unknown subcommand or wrong number of arguments for '<sub>'. Try CLUSTER HELP.`
    #[test]
    fn under_arity_mutator_subcommands_are_subcommand_syntax_errors() {
        let (c, _map) = ctx_empty_self();
        // Each of these matches its token but is one arg short of the minimum.
        let cases: &[&[&[u8]]] = &[
            &[b"CLUSTER", b"ADDSLOTS"],
            &[b"CLUSTER", b"DELSLOTS"],
            &[b"CLUSTER", b"SETSLOT", b"0"], // base guard: argc < 4
            &[b"CLUSTER", b"FLUSHSLOTS", b"extra"], // argc != 2
            &[b"CLUSTER", b"MEET", b"127.0.0.1"], // argc not 4/5
            &[b"CLUSTER", b"FORGET"],        // argc != 3
            &[b"CLUSTER", b"BUMPEPOCH", b"extra"], // argc != 2
            &[b"CLUSTER", b"SET-CONFIG-EPOCH"], // argc != 3
            &[b"CLUSTER", b"ADDSLOTSRANGE"], // no pair at all
            &[b"CLUSTER", b"DELSLOTSRANGE"], // no pair at all
        ];
        for parts in cases {
            let line = err_line(&run(&c, parts));
            assert!(
                line.contains("unknown subcommand or wrong number of arguments")
                    && line.contains("Try CLUSTER HELP."),
                "{parts:?} should be a subcommand-syntax error, got {line:?}"
            );
        }
    }

    /// FINDING 3 EXCEPTION: ADDSLOTSRANGE / DELSLOTSRANGE with >= 4 args but an ODD count is a
    /// REAL wrong-arity (Redis addReplyErrorArity), not the subcommand-syntax error.
    #[test]
    fn odd_range_args_are_real_wrong_arity() {
        let (c, _map) = ctx_empty_self();
        for sub in [b"ADDSLOTSRANGE".as_slice(), b"DELSLOTSRANGE"] {
            // Three range args (one pair + a dangling one): odd while >= 4 total.
            let line = err_line(&run(&c, &[b"CLUSTER", sub, b"0", b"5", b"7"]));
            assert!(
                line.contains("wrong number of arguments") && !line.contains("unknown subcommand"),
                "{:?} odd args should be wrong-arity, got {line:?}",
                String::from_utf8_lossy(sub)
            );
        }
    }

    /// FINDING 4: CLUSTER MEET port / address error strings (Redis 7.4 exact).
    #[test]
    fn meet_port_and_address_errors_are_byte_exact() {
        let (c, _map) = ctx_empty_self();
        // Non-integer base port -> `Invalid base port specified: <arg>`.
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"MEET", b"127.0.0.1", b"notaport"])),
            "-ERR Invalid base port specified: notaport"
        );
        // Non-integer bus port -> `Invalid bus port specified: <arg>`.
        assert_eq!(
            err_line(&run(
                &c,
                &[b"CLUSTER", b"MEET", b"127.0.0.1", b"7000", b"busbad"]
            )),
            "-ERR Invalid bus port specified: busbad"
        );
        // Out-of-range base port -> `Invalid node address specified: <ip>:<port>`.
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"MEET", b"10.0.0.9", b"99999"])),
            "-ERR Invalid node address specified: 10.0.0.9:99999"
        );
        // A valid MEET still succeeds.
        assert_ok(
            &run(&c, &[b"CLUSTER", b"MEET", b"10.0.0.9", b"7000"]),
            "valid MEET",
        );
    }

    /// FINDING 5: SETSLOT bad-action / wrong-argc inner error is the SINGLE Redis message.
    #[test]
    fn setslot_bad_action_or_wrong_argc_is_one_message() {
        const SETSLOT_ERR: &str =
            "-ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP";
        let (c, _map) = ctx_empty_self();
        // Unrecognized action.
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"SETSLOT", b"0", b"BOGUS"])),
            SETSLOT_ERR
        );
        // NODE form at the wrong argc (missing the node id) -> the SAME message (was wrong-arity).
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"SETSLOT", b"0", b"NODE"])),
            SETSLOT_ERR
        );
        // STABLE at the wrong argc (an extra arg) -> the SAME message (STABLE is argc==4).
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"SETSLOT", b"0", b"STABLE", b"x"])),
            SETSLOT_ERR
        );
        // MIGRATING at the wrong argc (missing node id) -> the SAME message (MIGRATING is argc==5).
        assert_eq!(
            err_line(&run(&c, &[b"CLUSTER", b"SETSLOT", b"0", b"MIGRATING"])),
            SETSLOT_ERR
        );
    }

    /// HA-6: SETSLOT MIGRATING / IMPORTING / STABLE drive the (additive, owns()-inert) migration
    /// state on the static slot map. MIGRATING/IMPORTING to a KNOWN node succeed; an UNKNOWN node is
    /// the standard cluster mutation error; STABLE always succeeds. The node MIGRATING/IMPORTING is
    /// named here as TEST_NODE_ID (self -- always a known node), proving the verb wires through.
    #[test]
    fn setslot_migrating_importing_stable_drive_migration_state() {
        let (c, map) = ctx_empty_self();
        // MIGRATING to a KNOWN node (self) -> +OK; the slot's migration state is now set.
        assert_ok(
            &run(
                &c,
                &[
                    b"CLUSTER",
                    b"SETSLOT",
                    b"0",
                    b"MIGRATING",
                    TEST_NODE_ID.as_bytes(),
                ],
            ),
            "SETSLOT 0 MIGRATING <self>",
        );
        assert_eq!(
            map.migration_state(0),
            ironcache_cluster::MigrationState::Migrating
        );
        // STABLE clears it (always succeeds).
        assert_ok(
            &run(&c, &[b"CLUSTER", b"SETSLOT", b"0", b"STABLE"]),
            "SETSLOT 0 STABLE",
        );
        assert_eq!(
            map.migration_state(0),
            ironcache_cluster::MigrationState::None
        );
        // IMPORTING to a KNOWN node -> +OK; the slot's migration state is now IMPORTING.
        assert_ok(
            &run(
                &c,
                &[
                    b"CLUSTER",
                    b"SETSLOT",
                    b"1",
                    b"IMPORTING",
                    TEST_NODE_ID.as_bytes(),
                ],
            ),
            "SETSLOT 1 IMPORTING <self>",
        );
        assert_eq!(
            map.migration_state(1),
            ironcache_cluster::MigrationState::Importing
        );
        // MIGRATING / IMPORTING to an UNKNOWN node -> the standard `Unknown node` mutation error.
        let unknown = "0000000000000000000000000000000000000000";
        assert_eq!(
            err_line(&run(
                &c,
                &[
                    b"CLUSTER",
                    b"SETSLOT",
                    b"2",
                    b"MIGRATING",
                    unknown.as_bytes()
                ]
            )),
            format!("-ERR Unknown node {unknown}")
        );
    }

    /// FLUSHSLOTS releases every self-owned slot.
    #[test]
    fn flushslots_releases_all_self_slots() {
        let (c, _map) = ctx_empty_self();
        assert_ok(
            &run(&c, &[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"99"]),
            "claim 0-99",
        );
        assert_ok(&run(&c, &[b"CLUSTER", b"FLUSHSLOTS"]), "FLUSHSLOTS");
        let info = text_body(&run(&c, &[b"CLUSTER", b"INFO"]));
        assert!(
            info.contains("cluster_slots_assigned:0\r\n"),
            "FLUSHSLOTS cleared all slots: {info:?}"
        );
    }

    /// Cross-shard visibility: two ServerContext clones share ONE `Arc<SlotMap>`. A mutation via
    /// one ctx is visible through the other (the Arc is the cross-shard mutable state).
    #[test]
    fn mutation_via_one_ctx_is_visible_via_a_clone_sharing_the_arc() {
        let (c1, map) = ctx_empty_self();
        // c2 shares the SAME Arc<SlotMap> (as every shard's ctx clone does at runtime).
        let mut c2 = ctx_with(Config {
            cluster_enabled: true,
            ..Config::default()
        });
        c2.cluster = Some(map.clone());

        // Claim a range via c1; assert it is visible via c2's projection AND owns().
        assert_ok(
            &run(&c1, &[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"10"]),
            "claim via c1",
        );
        assert!(
            map.owns(0),
            "shared map owns slot 0 after c1's ADDSLOTSRANGE"
        );
        let info2 = text_body(&run(&c2, &[b"CLUSTER", b"INFO"]));
        assert!(
            info2.contains("cluster_slots_assigned:11\r\n"),
            "c2 sees c1's mutation through the shared Arc: {info2:?}"
        );
    }

    /// SLICE-3 3a-GAP NEGATIVE test: a node mutates only its OWN local view; with no inter-node
    /// sync (slice 3b), node1 does NOT learn node2's slot assignments. We model two INDEPENDENT
    /// empty-self nodes (separate Arcs), drive each to claim a half, and assert each node's local
    /// view is correct AND that node1 does NOT see node2's range.
    #[test]
    fn three_a_gap_node_does_not_see_a_peers_local_assignments() {
        // Two independent nodes (their own maps), NOT sharing an Arc (the 3a reality: no sync).
        let map1 = std::sync::Arc::new(ironcache_cluster::SlotMap::empty_self(
            MAP_ID0, "10.0.0.1", 7001,
        ));
        let map2 = std::sync::Arc::new(ironcache_cluster::SlotMap::empty_self(
            MAP_ID1, "10.0.0.2", 7002,
        ));
        let mut c1 = ctx_with(Config {
            cluster_enabled: true,
            ..Config::default()
        });
        c1.cluster = Some(map1.clone());
        let mut c2 = ctx_with(Config {
            cluster_enabled: true,
            ..Config::default()
        });
        c2.cluster = Some(map2.clone());

        // Each node claims its OWN half locally.
        assert_ok(
            &run(&c1, &[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"8191"]),
            "node1 claims low half",
        );
        assert_ok(
            &run(&c2, &[b"CLUSTER", b"ADDSLOTSRANGE", b"8192", b"16383"]),
            "node2 claims high half",
        );

        // Each node's LOCAL view is correct.
        assert!(map1.owns(0) && !map1.owns(8192), "node1 owns only its half");
        assert!(map2.owns(8192) && !map2.owns(0), "node2 owns only its half");

        // 3a GAP: node1 does NOT see node2's range. Its INFO shows only its own 8192 slots
        // (NOT 16384) and state:fail, and its SLOTS has exactly one range (its own).
        let info1 = text_body(&run(&c1, &[b"CLUSTER", b"INFO"]));
        assert!(
            info1.contains("cluster_slots_assigned:8192\r\n"),
            "node1 sees only its own half, NOT node2's: {info1:?}"
        );
        assert!(
            info1.contains("cluster_state:fail\r\n"),
            "node1's map is incomplete without node2's range: {info1:?}"
        );
        let Value::Array(Some(ranges1)) = run(&c1, &[b"CLUSTER", b"SLOTS"]) else {
            panic!("expected a SLOTS array");
        };
        assert_eq!(
            ranges1.len(),
            1,
            "node1 sees ONLY its own range, not node2's (no sync in slice 3a)"
        );
        // SLICE 3b will flip this to a positive assertion (both ranges visible on each node).
    }
}
