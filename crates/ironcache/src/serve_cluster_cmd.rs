// SPDX-License-Identifier: MIT OR Apache-2.0
//! Raft-mode CLUSTER command handling split out of `serve.rs` (#625, CONTROL_PLANE.md): the
//! `CLUSTER` mutator -> `ConfigCmd` proposal path (ADDSLOTS/DELSLOTS/SETSLOT/SET-CONFIG-EPOCH/MEET/
//! FORGET/FAILOVER/REPLICATE/FLUSHSLOTS/RESET), the membership-intent apply, and the slot/MEET-id
//! parse + build helpers. Behavior-preserving relocation: the bodies are byte-identical.

use super::{ShardState, ascii_upper, encode_into, replica_read_in_sync};
use ironcache_runtime::{Runtime, TokioRuntime};
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, Request};
use std::cell::RefCell;
use std::rc::Rc;

/// Handle a raft-mode `CLUSTER` MUTATOR by proposing the matching [`ConfigCmd`](ironcache_raft::ConfigCmd)
/// through the control plane (HA-4c). Returns `Some(close)` (always `Some(false)`) when the
/// subcommand WAS a mutator (a reply has been written to `out`), or `None` for a non-mutator
/// subcommand (the caller falls through to the unchanged home dispatch, which reads the committed
/// `ctx.cluster` map for the introspection projections).
///
/// The caller has already established `cluster_mode == Raft` and `ctx.raft.is_some()`. The
/// mutator -> ConfigCmd mapping (CONTROL_PLANE.md / the HA-3e `ConfigCmd` taxonomy):
///   * `ADDSLOTS` / `ADDSLOTSRANGE`  -> `AssignSlots { node: self_id, slots }`
///   * `SETSLOT <slot> NODE <id>`    -> `SetSlotOwner { slot, node: id }`
///   * `MEET <ip> <port> [bus]`      -> `AddNode { id, host, port }`
///   * `FORGET <id>`                 -> `RemoveNode { id }`
///   * `SET-CONFIG-EPOCH <epoch>`    -> `SetConfigEpoch(epoch)`
///   * `DELSLOTS` / `DELSLOTSRANGE`  -> `UnassignSlots { slots }` (the parsed / range-expanded list)
///   * `FLUSHSLOTS`                  -> `UnassignSlots { slots }` (every slot THIS node owns in the
///     committed map; Redis FLUSHSLOTS clears the node's own slots)
///
/// On commit -> `+OK`; when this node is NOT the leader -> `-CLUSTERDOWN ...` (the client retries
/// against the leader). The slot/argument validation mirrors the Redis error shapes the static
/// `cmd_cluster` mutators use for the common cases. `commands_processed` is bumped exactly once
/// (matching every other route), regardless of outcome.
pub(crate) async fn try_raft_cluster_mutator(
    ctx: &ServerContext,
    conn: &ConnState,
    state_rc: &Rc<RefCell<ShardState>>,
    request: &Request,
    out: &mut Vec<u8>,
) -> Option<bool> {
    use ironcache_protocol::ErrorReply;
    use ironcache_server::Value;

    // A bare `CLUSTER` (no subcommand) is not a mutator: let the home dispatch emit the arity
    // error (byte-identical to the static path).
    if request.args.len() < 2 {
        return None;
    }
    let sub = ascii_upper(&request.args[1]);

    // Build the ConfigCmd SEQUENCE for a recognized mutator, or return the appropriate immediate
    // error / None (non-mutator). `Err(reply)` is a validation error to send WITHOUT proposing;
    // `Ok(cmds)` is the ordered batch to propose+commit; `None` falls through to home dispatch.
    //
    // ADDSLOTS / ADDSLOTSRANGE prepend a self-`AddNode` (build_self_assign): they assign slots to
    // THIS node, but a FOLLOWER's committed map does not yet know the leader's id (each node boots
    // `empty_self` knowing only itself; MEET is leader -> peer). Committing `AddNode{self}` FIRST
    // teaches every node the leader's id+endpoint, so the following `AssignSlots{self}` applies
    // (and MOVED resolves) on every node. AddNode is idempotent on the leader's own table.
    let built: Option<Result<Vec<ironcache_raft::ConfigCmd>, ErrorReply>> = match sub.as_slice() {
        b"ADDSLOTS" => Some(build_self_assign(ctx, request, parse_addslots_slots)),
        b"ADDSLOTSRANGE" => Some(build_self_assign(ctx, request, parse_addslotsrange_slots)),
        b"SETSLOT" => Some(build_setslot(ctx, request).map(|c| vec![c])),
        // MEET LEARNS the peer's REAL announce id over the cluster bus (item-7 id-reconciliation),
        // which is real I/O (a bounded `CLUSTER MYID` fetch through the Runtime seam), so it is the
        // one builder that is `async`. On a reachable peer the committed `AddNode { id: real_id }`
        // COINCIDES with the peer's self-added announce entry (meet is idempotent on a duplicate
        // id), so no synth/announce duplicate inflates `cluster_known_nodes`; on an unreachable peer
        // it falls back to the synth id so the cluster still forms. See `build_meet`.
        b"MEET" => Some(build_meet(request).await.map(|c| vec![c])),
        b"FORGET" => Some(build_forget(request).map(|c| vec![c])),
        b"SET-CONFIG-EPOCH" => Some(build_set_config_epoch(request).map(|c| vec![c])),
        // HA-7d: `CLUSTER REPLICATE <node-id> <slot> [slot ...]` assigns `<node-id>` as a REPLICA
        // of the listed slots (drives `AssignReplica`). The named node must already be known (a
        // prior MEET / AddNode); the committed log order guarantees that, and the replica node
        // then attaches to each slot OWNER's primary (full-sync + tail) and serves READONLY reads.
        b"REPLICATE" => Some(build_replicate(request).map(|c| vec![c])),
        // HA-8 / #371: `CLUSTER FAILOVER` promotes THIS in-sync replica to owner of the slots it
        // replicates via a committed `PromoteReplica` (the operator entry point to the same path the
        // automatic failover uses). The in-sync gate (the data-safety crux) lives in `build_failover`.
        b"FAILOVER" => Some(build_failover(ctx, request)),
        // DELSLOTS / DELSLOTSRANGE UN-assign the parsed / range-expanded slots (the inverse of
        // ADDSLOTS / ADDSLOTSRANGE; the SAME slot-parse helpers). FLUSHSLOTS UN-assigns every slot
        // THIS node owns in the committed map. Each commits an `UnassignSlots` ConfigCmd, so the
        // slots become owned by nobody on every node (cluster_slots_assigned drops by that many).
        b"DELSLOTS" => Some(build_unassign(request, parse_addslots_slots)),
        b"DELSLOTSRANGE" => Some(build_unassign(request, parse_addslotsrange_slots)),
        b"FLUSHSLOTS" => Some(build_flushslots(ctx, request)),
        // #371: `CLUSTER REBALANCE APPLY` ARMS the planned slot migrations (a committed
        // MIGRATING + IMPORTING per move, driving HA-6's auto-copy); the DRYRUN / default form is a
        // read-only plan handled by the home dispatch (falls through the `_ => None` arm below).
        b"REBALANCE" if request.args.len() >= 3 && ascii_upper(&request.args[2]) == b"APPLY" => {
            Some(build_rebalance_apply(ctx))
        }
        // Any other subcommand (the introspection set, BUMPEPOCH, HELP, unknown, ...) is NOT a
        // mutator: fall through to the unchanged home dispatch.
        _ => None,
    };

    let cmds = match built? {
        Ok(cmds) => cmds,
        Err(reply) => {
            // A validation error (bad slot / arity / node id): reply now, do not propose.
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &Value::error(reply), conn.proto);
            return Some(false);
        }
    };

    // HA-prod-membership: a raft-mode MEET / FORGET ALSO drives the Raft VOTER / LEARNER set, not
    // just the node TABLE. Capture the membership intent from the built ConfigCmd batch BEFORE it is
    // consumed by the propose loop: MEET's `AddNode { id, host, port }` -> stage the node in as a
    // non-voting LEARNER (it catches up, then the leader's auto-promote driver promotes it to a
    // voter); FORGET's `RemoveNode { id }` -> drop it from the voter / learner set. The node-table
    // change and the membership change are SEPARATE committed entries (the table commits first
    // below; the membership change is proposed after), each correct on its own.
    let membership_intent = match sub.as_slice() {
        b"MEET" => cmds.iter().find_map(|c| match c {
            ironcache_raft::ConfigCmd::AddNode { id, host, port } => Some(MembershipIntent::Add {
                id: id.clone(),
                host: host.clone(),
                client_port: *port,
            }),
            _ => None,
        }),
        b"FORGET" => cmds.iter().find_map(|c| match c {
            ironcache_raft::ConfigCmd::RemoveNode { id } => {
                Some(MembershipIntent::Remove { id: id.clone() })
            }
            _ => None,
        }),
        _ => None,
    };

    // Count the command once (matching the home / remote / fan-out paths), then propose each
    // ConfigCmd in order and await its commit. The whole mutator replies `+OK` only if EVERY
    // entry commits; the FIRST NotLeader short-circuits to the `-CLUSTERDOWN` redirect (a
    // follower never commits anything, so a partial batch cannot land).
    state_rc.borrow_mut().counters.on_command();
    let handle = ctx
        .raft
        .as_ref()
        .expect("caller checked ctx.raft.is_some() before dispatching a raft mutator");
    for cmd in cmds {
        if matches!(
            handle.propose(cmd).await,
            ironcache_server::ProposeOutcome::NotLeader
        ) {
            // No leader reachable (no leader recognized, or a forward to the leader timed out;
            // with HA-9 forwarding a follower normally COMMITS transparently, so this is the
            // genuine no-leader / timeout case). PROD-9: resolve the leader's ADVERTISED CLIENT
            // endpoint (the SAME host:port `CLUSTER SHARDS` reports, dial-able by an operator) from
            // the raft `leader_id` via the committed slot map, so the redirect NAMES where to reissue
            // -- not the cluster-bus port (which is not a client target). Distinct messages for the
            // resolvable-client, unresolvable-but-known-id (degrade), and no-leader-elected cases.
            let msg = match ironcache_server::resolve_leader_hint(ctx) {
                // SelfIsLeader is unreachable here (a self-leader commits rather than redirecting),
                // but fold it into the no-leader retry text rather than panicking on an impossible
                // state: if we somehow got NotLeader while believing we are the leader, a retry is
                // the safe answer.
                ironcache_server::LeaderHint::SelfIsLeader
                | ironcache_server::LeaderHint::NoLeader => {
                    "NOTLEADER no leader is currently elected; retry the CLUSTER write once a leader is elected"
                        .to_owned()
                }
                ironcache_server::LeaderHint::Client(addr) => format!(
                    "NOTLEADER the current raft leader is {addr}; reissue the CLUSTER write there"
                ),
                ironcache_server::LeaderHint::NodeId(id) => format!(
                    "NOTLEADER this node is not the raft leader; the leader is raft node {id} (its client address is not yet known here); retry the CLUSTER write against the leader"
                ),
            };
            encode_into(out, &Value::error(ErrorReply::clusterdown(msg)), conn.proto);
            return Some(false);
        }
    }

    // HA-prod-membership: the node-table change committed; now drive the Raft config. A failure here
    // (not leader, in flight, refused) does NOT fail the whole CLUSTER command -- the table change is
    // already committed and the membership change is idempotent / retryable -- so it is surfaced as a
    // NOTE appended to the reply rather than a hard error, keeping the byte-compatible `+OK` for the
    // success path while telling the operator when the membership step needs a retry.
    if let Some(intent) = membership_intent {
        // F2: the EXISTING node table's announce ids (from the committed `ctx.cluster` map), so
        // `apply_membership_intent` can REJECT a MEET whose derived NodeId collides with an existing
        // node that has a DIFFERENT announce id (two physical nodes -> one Raft identity) rather than
        // silently swallowing it. The list is the source of announce-id -> derived-NodeId truth that
        // the raft config's `BTreeSet<NodeId>` alone cannot recover.
        let known_announce_ids: Vec<String> = ctx
            .cluster
            .as_deref()
            .map(|m| m.nodes().into_iter().map(|n| n.id.to_string()).collect())
            .unwrap_or_default();
        if let Some(note) = apply_membership_intent(handle, intent, &known_announce_ids).await {
            // A non-empty note means the membership step did not (yet) take effect; reply with a
            // -CLUSTERDOWN-style error carrying the reason so the operator retries the membership.
            encode_into(
                out,
                &Value::error(ErrorReply::clusterdown(note)),
                conn.proto,
            );
            return Some(false);
        }
    }

    encode_into(out, &Value::ok(), conn.proto);
    // The connection stays open in every case (mirrors the static CLUSTER path).
    Some(false)
}

/// The Raft-membership side of a raft-mode `CLUSTER MEET` / `FORGET` (HA-prod-membership), captured
/// from the built [`ConfigCmd`](ironcache_raft::ConfigCmd) batch so the voter / learner set is
/// driven ALONGSIDE the node table.
enum MembershipIntent {
    /// MEET: stage the node in as a non-voting LEARNER (it catches up, then auto-promotes to voter).
    /// Carries the 40-hex id (to derive the `NodeId`) plus the advertised host + client port (to
    /// derive the cluster-bus `SocketAddr` the leader replicates to).
    Add {
        id: String,
        host: String,
        client_port: u16,
    },
    /// FORGET: drop the node from the voter / learner set.
    Remove { id: String },
}

/// Apply the [`MembershipIntent`] of a committed raft-mode MEET / FORGET to the Raft config
/// (HA-prod-membership). Returns `None` on success (the membership change committed, or the FORGET
/// found nothing to remove), or `Some(note)` with an operator-facing reason when the membership step
/// did not take effect (not leader, a change already in flight, a quorum-safety refusal, or -- F2 --
/// a derived-NodeId collision with an existing different-announce-id node) so the operator can retry
/// or fix the id. The node-table commit has already happened; this only governs consensus
/// membership, which is idempotent and safely retryable.
///
/// `known_announce_ids` is the committed node table's announce ids (F2): a MEET whose derived NodeId
/// collides with an EXISTING node that has a DIFFERENT announce id is REJECTED (rather than silently
/// swallowed as an idempotent no-op), because two physical nodes mapping to one Raft identity is
/// catastrophic.
///
/// MEET stages the node as a LEARNER ([`MembershipChange::AddLearner`]): a non-voting member that
/// receives the log and catches up but is counted in NO quorum, so adding it can never stall
/// consensus. The leader's auto-promote driver later promotes it to a voter once it has caught up.
/// The new node's cluster-bus endpoint (`host` + `bus_port(client_port)`, reconstructed from the
/// MEET args) is passed as a [`PeerEndpoint`] so the leader can replicate to a runtime-joined node
/// that is NOT in the static topology peer map. The endpoint holds the HOST + PORT (a DNS hostname
/// OR an IP literal), resolved fresh on each dial -- so a hostname-addressed joiner (a k8s
/// StatefulSet pod) is reachable and a restarted pod's new IP is picked up, instead of the old code
/// which dropped a DNS-named joiner's address (`None`) because it only accepted an IP literal.
async fn apply_membership_intent(
    handle: &ironcache_server::RaftHandle,
    intent: MembershipIntent,
    known_announce_ids: &[String],
) -> Option<String> {
    use ironcache_raft::MembershipChange;
    match intent {
        MembershipIntent::Add {
            id,
            host,
            client_port,
        } => {
            let node = crate::raft_boot::node_id_from_announce(&id);
            // F2 COLLISION REJECT: the engine keys nodes by the derived NodeId (the announce id's top
            // 64 bits). If an EXISTING node with a DIFFERENT announce id derives the SAME NodeId, this
            // MEET would map two physical nodes to ONE Raft identity (catastrophic). The previous
            // guard below would SILENTLY swallow that (the colliding NodeId is already a voter/learner,
            // so `cfg.*.contains(&node)` is true and it returns `None` == success). Detect it
            // explicitly against the committed node table's announce ids and REJECT with a clear error
            // so the operator fixes the id, rather than a confusing silent no-op (or a shadowed node).
            if let Some(other) = known_announce_ids.iter().find(|known| {
                known.as_str() != id && crate::raft_boot::node_id_from_announce(known) == node
            }) {
                return Some(format!(
                    "MEET rejected: node id '{id}' derives the same raft NodeId as the existing node \
                     '{other}' (the engine keys nodes by the top 64 bits / first 16 hex digits of the \
                     announce id, which collide); use an id that differs within its first 16 hex digits"
                ));
            }
            // CRITICAL SAFETY: do NOT AddLearner a node that is ALREADY a voter (or this node
            // itself). The boot topology's voters MEET each other during formation, and a MEET is
            // idempotent on the node table; but `AddLearner` of an existing voter would DEMOTE it
            // out of the voter set (apply_membership_delta moves it voters -> learners), shrinking
            // quorum. So skip the learner-add when the named node is already a voter or is self --
            // the node table still records the MEET, the raft config is left correct. A node already
            // a LEARNER is also skipped (idempotent; it is already staged and catching up). (The F2
            // reject above already excluded a DIFFERENT-announce-id collision, so reaching this guard
            // with `contains(&node)` true means the SAME announce id is re-MEET'd -- a true no-op.)
            let cfg = handle.config();
            if node == handle.node_id()
                || cfg.voters.contains(&node)
                || cfg.learners.contains(&node)
            {
                return None;
            }
            // The new node's cluster-bus endpoint: host + (client_port + BUS_PORT_OFFSET). Held as a
            // PeerEndpoint (host + port, a DNS name OR an IP literal) so the leader can dial +
            // replicate to this runtime-joined node, re-resolving the host per dial -- a
            // hostname-addressed joiner is reachable (the old IP-only parse dropped it).
            let bus = crate::raft_boot::bus_port(client_port);
            let addr = Some(ironcache_clusterbus::PeerEndpoint::new(host.clone(), bus));
            match handle
                .propose_membership(MembershipChange::AddLearner(node), addr)
                .await
            {
                ironcache_server::MembershipOutcome::Committed(_) => None,
                ironcache_server::MembershipOutcome::NotLeader => {
                    Some("MEET committed the node table but this node is not the raft leader; retry to add it to the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::InFlight => {
                    Some("MEET committed the node table but a raft membership change is in flight; retry to add it to the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::Refused(why) => Some(why),
            }
        }
        MembershipIntent::Remove { id } => {
            let node = crate::raft_boot::node_id_from_announce(&id);
            let cfg = handle.config();
            // Nothing to do if the node is neither a voter nor a learner (a FORGET of an unknown id,
            // or one only ever in the table): the table removal already handled it.
            if !cfg.voters.contains(&node) && !cfg.learners.contains(&node) {
                return None;
            }
            let change = if cfg.voters.contains(&node) {
                MembershipChange::RemoveVoter(node)
            } else {
                MembershipChange::RemoveLearner(node)
            };
            match handle.propose_membership(change, None).await {
                ironcache_server::MembershipOutcome::Committed(_) => None,
                ironcache_server::MembershipOutcome::NotLeader => {
                    Some("FORGET committed the node table but this node is not the raft leader; retry to remove it from the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::InFlight => {
                    Some("FORGET committed the node table but a raft membership change is in flight; retry to remove it from the raft config".to_owned())
                }
                ironcache_server::MembershipOutcome::Refused(why) => Some(why),
            }
        }
    }
}

/// Build the `AssignSlots { node: self, slots }` ConfigCmd for raft-mode `CLUSTER ADDSLOTS`
/// (HA-4c). Build the SELF-ASSIGN batch for `CLUSTER ADDSLOTS` / `ADDSLOTSRANGE`: a self-`AddNode`
/// (so every node learns this node's id+endpoint before the assignment references it; idempotent
/// on self) FOLLOWED by `AssignSlots { node: self, slots }`. `parse_slots` extracts the slot list
/// from the request (the per-verb arity + slot validation, mirroring the static `cmd_cluster`
/// Redis error shapes).
fn build_self_assign(
    ctx: &ServerContext,
    request: &Request,
    parse_slots: impl Fn(&Request) -> Result<Vec<u16>, ironcache_protocol::ErrorReply>,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    let slots = parse_slots(request)?;
    let (id, host, port) = self_node_endpoint(ctx);
    Ok(vec![
        ironcache_raft::ConfigCmd::AddNode {
            id: id.clone(),
            host,
            port,
        },
        ironcache_raft::ConfigCmd::AssignSlots { node: id, slots },
    ])
}

/// Build the committed `ConfigCmd` for a manual `CLUSTER FAILOVER` (#371): promote THIS node from
/// replica to OWNER of the slots it replicates, proposed + committed through the leader (the same
/// raft path every other `ConfigCmd` mutator uses).
///
/// DATA-SAFETY (the crux): refuse unless this node is an IN-SYNC replica, reusing the EXACT gate the
/// AUTOMATIC promotion and the replica-read path use ([`replica_read_in_sync`]: `is_in_sync` within
/// `replica_max_lag`, ADR-0026). So a manual failover can NEVER promote a node the automatic path
/// would not, and a stale replica is never promoted (which would lose committed writes). The
/// committed `PromoteReplica` then atomically transfers ownership + bumps the config epoch (the
/// split-brain fence: at most one owner per slot per epoch), and the OLD owner steps down on apply.
/// There is a small check-to-commit window (the replica could fall behind before the entry commits),
/// identical to the automatic path's; the epoch fence still guarantees no two committed owners.
///
/// `FORCE` / `TAKEOVER` (which in Redis bypass the in-sync and committed-consensus safety) are
/// REFUSED: the only supported form is the safe, gated, committed failover.
pub(crate) fn build_failover(
    ctx: &ServerContext,
    request: &Request,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    // FORCE / TAKEOVER would bypass the safety gates; not supported (do not bypass, per #371).
    if request.args.len() > 2 {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER FORCE/TAKEOVER is not supported (it would bypass the in-sync + \
             committed-consensus safety gates); use a bare CLUSTER FAILOVER",
        ));
    }
    // THE DATA-SAFETY GATE: only an in-sync replica may take over (the SAME gate the automatic
    // promotion uses). A non-replica / link-down / lagging node is refused here, never promoted.
    if !replica_read_in_sync(ctx) {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER refused: this node is not an in-sync replica (not a replica, link \
             down, or lagging past replica_max_lag); promoting it would risk losing committed writes",
        ));
    }
    let Some(map) = ctx.cluster.as_ref() else {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER requires cluster mode with a slot map",
        ));
    };
    // The slots this node currently replicates are exactly the slots it would take ownership of.
    let slots: Vec<u16> = (0..ironcache_cluster::CLUSTER_SLOTS)
        .filter(|&s| map.is_replica_of_self(s))
        .collect();
    if slots.is_empty() {
        return Err(ironcache_protocol::ErrorReply::err(
            "CLUSTER FAILOVER refused: this node replicates no slots to take over",
        ));
    }
    let (id, _host, _port) = self_node_endpoint(ctx);
    Ok(vec![ironcache_raft::ConfigCmd::PromoteReplica {
        slots,
        new_primary: id,
    }])
}

/// Build the `UnassignSlots { slots }` ConfigCmd for raft-mode `CLUSTER DELSLOTS` / `DELSLOTSRANGE`
/// (the inverse of [`build_self_assign`]). `parse_slots` is the SAME per-verb slot parser ADDSLOTS /
/// ADDSLOTSRANGE use (`parse_addslots_slots` / `parse_addslotsrange_slots`), so the arity + slot +
/// range validation (and the Redis error shapes) match the add path exactly. UN-assign needs no
/// `AddNode` prefix (it references no node) and clears the slots on EVERY node (the committed map is
/// shared), so a single `UnassignSlots` entry is the whole proposal.
pub(crate) fn build_unassign(
    request: &Request,
    parse_slots: impl Fn(&Request) -> Result<Vec<u16>, ironcache_protocol::ErrorReply>,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    let slots = parse_slots(request)?;
    Ok(vec![ironcache_raft::ConfigCmd::UnassignSlots { slots }])
}

/// Build the `UnassignSlots { slots }` ConfigCmd for raft-mode `CLUSTER FLUSHSLOTS` (Redis clears
/// the node's OWN slots). Arity is exactly 2 (the Redis FLUSHSLOTS form; a wrong argc is the
/// addReplySubcommandSyntaxError class, mirroring the static path). The slot set is every slot THIS
/// node currently owns in the committed map (read via `owns()`), so the proposal UN-assigns exactly
/// the running node's slots; an empty set (the node owns nothing) is a valid, degenerate batch.
///
/// DOCUMENTED DIVERGENCE (same as the static `cluster_flushslots`): Redis errors `DB must be empty
/// to perform CLUSTER FLUSHSLOTS.` when the keyspace is non-empty; IronCache has no per-slot key
/// count index yet, so it cannot test DB-emptiness and proposes unconditionally.
pub(crate) fn build_flushslots(
    ctx: &ServerContext,
    request: &Request,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::{CLUSTER_SLOTS, ErrorReply};
    if request.args.len() != 2 {
        // Wrong argc: the addReplySubcommandSyntaxError class (Redis parity), not wrong-arity.
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    // The committed map is always installed as ctx.cluster in raft-mode (the caller established
    // cluster_mode == Raft); collect the slots this node owns. If, defensively, no map is present,
    // there are no owned slots to clear, so the batch is empty (a harmless no-op proposal).
    let slots: Vec<u16> = match ctx.cluster.as_deref() {
        Some(map) => (0..CLUSTER_SLOTS).filter(|&s| map.owns(s)).collect(),
        None => Vec::new(),
    };
    Ok(vec![ironcache_raft::ConfigCmd::UnassignSlots { slots }])
}

/// Parse the slot list of `CLUSTER ADDSLOTS <slot>...` (arity Min(3); each slot strictly
/// validated, mirroring the static path's Redis error shapes).
pub(crate) fn parse_addslots_slots(
    request: &Request,
) -> Result<Vec<u16>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() < 3 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let mut slots = Vec::with_capacity(request.args.len() - 2);
    for a in &request.args[2..] {
        slots.push(parse_slot_strict(a)?);
    }
    Ok(slots)
}

/// Parse + expand the `<start> <end>` pairs of `CLUSTER ADDSLOTSRANGE` (even, non-empty arg count;
/// each slot strictly validated, `start <= end`).
pub(crate) fn parse_addslotsrange_slots(
    request: &Request,
) -> Result<Vec<u16>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    let pairs = &request.args[2..];
    if pairs.is_empty() {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    if pairs.len() % 2 != 0 {
        return Err(ErrorReply::wrong_arity("cluster|addslotsrange"));
    }
    let mut slots = Vec::new();
    for pair in pairs.chunks_exact(2) {
        let start = parse_slot_strict(&pair[0])?;
        let end = parse_slot_strict(&pair[1])?;
        if start > end {
            return Err(ErrorReply::err(format!(
                "start slot number {start} is greater than end slot number {end}"
            )));
        }
        slots.extend(start..=end);
    }
    Ok(slots)
}

/// Build the SETSLOT ConfigCmd for raft-mode `CLUSTER SETSLOT` (HA-4c + HA-6). Four forms:
/// - `<slot> NODE <id>`      -> [`ConfigCmd::SetSlotOwner`]   (the committed FLIP, HA-4c).
/// - `<slot> MIGRATING <id>` -> [`ConfigCmd::SetSlotMigrating`] (source-side handshake, HA-6).
/// - `<slot> IMPORTING <id>` -> [`ConfigCmd::SetSlotImporting`] (destination-side handshake, HA-6).
/// - `<slot> STABLE`         -> [`ConfigCmd::SetSlotStable`]    (clear/abort, HA-6).
///
/// NODE/MIGRATING/IMPORTING take a node id (argc == 5); STABLE takes none (argc == 4). Any other
/// action or a known action at the wrong argc is the single Redis SETSLOT error.
///
/// HA-6 (Finding 2): the `IMPORTING <src>` proposal carries an explicit `dest` so apply tags
/// IMPORTING on EXACTLY the destination node (via `SlotMap::is_self`), never on a bystander
/// non-owner. The wire command stays `SETSLOT <slot> IMPORTING <src>` (the operator names only the
/// source). In IronCache's raft model every CLUSTER mutator is proposed by the LEADER (a follower
/// replies `-CLUSTERDOWN`), so "the node running the command" is the leader, NOT the importer --
/// using the local node id would tag the leader, which is wrong. The slot is already MIGRATING
/// toward a known DEST (the MIGRATING step of the handshake committed first), so the builder reads
/// the recorded migration peer (`migration_peer_id`) as the dest. If the slot is not yet migrating
/// on the leader (a malformed handshake with no prior MIGRATING, or a single node issuing IMPORTING
/// against itself), it falls back to the local node id -- the conservative choice that tags the
/// running node, matching the standalone Redis-style case.
fn build_setslot(
    ctx: &ServerContext,
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    let setslot_err = || {
        ErrorReply::err("Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP")
    };
    // The shortest form (STABLE) is 4 args; a node-id form is 5.
    if request.args.len() < 4 {
        return Err(setslot_err());
    }
    let slot = parse_slot_strict(&request.args[2])?;
    let action = ascii_upper(&request.args[3]);
    let node = |request: &Request| String::from_utf8_lossy(&request.args[4]).into_owned();
    match action.as_slice() {
        b"NODE" if request.args.len() == 5 => Ok(ironcache_raft::ConfigCmd::SetSlotOwner {
            slot,
            node: node(request),
        }),
        b"MIGRATING" if request.args.len() == 5 => {
            Ok(ironcache_raft::ConfigCmd::SetSlotMigrating {
                slot,
                dest: node(request),
            })
        }
        b"IMPORTING" if request.args.len() == 5 => {
            // The dest is the node this slot is MIGRATING toward (the recorded migration peer the
            // prior committed MIGRATING step set), so apply tags IMPORTING on EXACTLY that node --
            // never on the leader (which proposes every mutator) or a bystander non-owner. Fall back
            // to the local node id when the slot is not yet migrating on the leader (a handshake with
            // no prior MIGRATING, or a node issuing IMPORTING against itself).
            let dest = ctx
                .cluster
                .as_deref()
                .and_then(|m| m.migration_peer_id(slot))
                .unwrap_or_else(|| self_node_endpoint(ctx).0);
            Ok(ironcache_raft::ConfigCmd::SetSlotImporting {
                slot,
                src: node(request),
                dest,
            })
        }
        b"STABLE" if request.args.len() == 4 => {
            Ok(ironcache_raft::ConfigCmd::SetSlotStable { slot })
        }
        // Unknown action, or a known action at the wrong argc.
        _ => Err(setslot_err()),
    }
}

/// The MAX slot moves one `CLUSTER REBALANCE APPLY` ARMS per call. The command proposes + awaits each
/// `ConfigCmd` synchronously, so this bounds the command's latency; a large rebalance is armed over
/// several calls (re-running arms the next batch of not-yet-migrating moves). `* 2` because each move
/// is a MIGRATING + an IMPORTING proposal.
const MAX_REBALANCE_APPLY_MOVES: usize = 128;

/// Build the committed `ConfigCmd` batch for `CLUSTER REBALANCE APPLY` (#371, REBALANCE_APPLY.md).
///
/// For each planned move ([`SlotMap::rebalance_moves`]) whose slot is NOT already migrating (up to the
/// per-call cap), it tags the SOURCE `MIGRATING <dest>` and the DESTINATION `IMPORTING <src>` -- which
/// ARMS HA-6's `run_import_control` to auto-copy the slot's keys + tail to the destination. It does
/// NOT propose the ownership FLIP (`SETSLOT NODE`): the operator finalizes each slot with
/// `CLUSTER SETSLOT <slot> NODE <dest>` once `CLUSTER COUNTKEYSINSLOT` shows the destination caught up
/// (a background auto-flip controller is a tracked follow-up). Leaving the flip out is the SAFE choice
/// -- APPLY never races a last-moment source write against the flip.
///
/// Idempotent + resumable: every `SetSlot*` apply is idempotent, and re-running APPLY skips slots
/// already migrating and arms the NEXT batch, so a big rebalance is driven over repeated calls (the
/// operator flips caught-up slots in between, which lets `rebalance_moves` recompute). An empty batch
/// (already balanced, or every move already in flight) commits nothing and replies `+OK`.
fn build_rebalance_apply(
    ctx: &ServerContext,
) -> Result<Vec<ironcache_raft::ConfigCmd>, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    let map = ctx
        .cluster
        .as_deref()
        .ok_or_else(|| ErrorReply::err("This instance has cluster support disabled"))?;
    Ok(rebalance_apply_cmds(map, MAX_REBALANCE_APPLY_MOVES))
}

/// The PURE core of [`build_rebalance_apply`] (#371): the `MIGRATING` + `IMPORTING` `ConfigCmd`s for
/// up to `max_moves` of `map`'s planned moves whose slot is not already migrating. Pure over the slot
/// map, so the batch is unit-tested without a raft quorum. Deterministic (it walks
/// [`SlotMap::rebalance_moves`]'s deterministic order).
pub(crate) fn rebalance_apply_cmds(
    map: &ironcache_cluster::SlotMap,
    max_moves: usize,
) -> Vec<ironcache_raft::ConfigCmd> {
    let mut cmds = Vec::new();
    for mv in map.rebalance_moves() {
        if cmds.len() >= max_moves * 2 {
            break;
        }
        // Skip slots already migrating (armed by a prior APPLY): re-running arms the NEXT batch.
        if map.migration_state(mv.slot) != ironcache_cluster::MigrationState::None {
            continue;
        }
        cmds.push(ironcache_raft::ConfigCmd::SetSlotMigrating {
            slot: mv.slot,
            dest: mv.dst_node_id.clone(),
        });
        cmds.push(ironcache_raft::ConfigCmd::SetSlotImporting {
            slot: mv.slot,
            src: mv.src_node_id,
            dest: mv.dst_node_id,
        });
    }
    cmds
}

/// The bound on the MEET id-learning fetch: how long a raft-mode `CLUSTER MEET` will wait for the
/// peer's `CLUSTER MYID` before falling back to the synth id. Generous enough for a one-round-trip
/// loopback / LAN fetch, short enough that a MEET to a not-yet-up peer does not hang the serve
/// path (it falls back and the cluster still forms). Read through the Runtime timer seam.
const MEET_ID_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Build the `AddNode { id, host, port }` ConfigCmd for raft-mode `CLUSTER MEET <ip> <port> [bus]`
/// (HA-4c + item-7 id-reconciliation).
///
/// HISTORY / THE BUG THIS FIXES: the original raft-mode MEET SYNTHESIZED the peer's id from
/// `host:port` (`synth_meet_node_id`) because there is no gossip to learn the real id. But every
/// node ALSO self-adds under its REAL announce id (`empty_self`) and is declared under that id, so
/// a MEET'd peer ended up in the committed node table under BOTH a synth id AND its announce id ->
/// `cluster_known_nodes` / `CLUSTER NODES` were INFLATED with a duplicate per MEET'd peer (routing
/// stayed correct -- it matches by ENDPOINT -- but the operator-visible node count was wrong).
///
/// THE FIX: on a raft-mode MEET we LEARN the peer's REAL announce id by dialing the peer's RESP
/// CLIENT port (`host:port`, the same endpoint a client / a MOVED redirect uses -- NOT the
/// `+10000` cluster-bus port, which speaks only RAFTMSG) and reading `CLUSTER MYID` over the
/// cluster-bus `peer_node_id` helper. The fetch is BOUNDED by [`MEET_ID_FETCH_TIMEOUT`] through
/// the Runtime timer seam so it can never hang the serve path. We then propose
/// `AddNode { id: real_id, host, port }`; because the peer self-added that SAME announce id and the
/// committed `meet` apply is idempotent on a duplicate id, the table holds ONE entry per node and
/// `cluster_known_nodes` equals the real node count (no inflation).
///
/// FALLBACK (peer unreachable): if the fetch fails or times out (the peer is not yet up, refuses,
/// or returns a non-id reply), we FALL BACK to the deterministic `synth_meet_node_id` so a MEET to
/// a transiently-down peer STILL makes progress and the cluster forms (the synth entry later
/// reconciles when the peer comes up and is re-MEET'd, or via the cluster crate's defensive
/// `SlotMap::dedup_nodes_by_endpoint`). This is the documented fallback the slice-3 static MEET
/// also uses.
///
/// SCOPING (no SWIM): this is a LIGHTWEIGHT id-reconciliation, deliberately NOT a SWIM/Lifeguard
/// failure detector. Raft already provides the cluster's liveness + failover signal (heartbeats,
/// elections, committed `PromoteReplica`), so a separate gossip failure detector would be
/// redundant. The only gap raft-mode MEET had was learning a peer's stable IDENTITY at join time,
/// which one bounded `CLUSTER MYID` fetch closes; ongoing liveness stays Raft's job.
///
/// The `request` is the validated `CLUSTER MEET` frame; the runtime is a fresh zero-sized
/// [`TokioRuntime`] (the dial is one short-lived outbound connection over the seam, like the
/// expire task's runtime; no shard state is touched and no hot-path lock is taken).
async fn build_meet(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() != 4 && request.args.len() != 5 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let host = String::from_utf8_lossy(&request.args[2]).into_owned();
    let port_arg = String::from_utf8_lossy(&request.args[3]).into_owned();
    let Some(port) = ironcache_server::cmd_util::parse_i64(&request.args[3]) else {
        return Err(ErrorReply::err(format!(
            "Invalid base port specified: {port_arg}"
        )));
    };
    if !(1..=65535).contains(&port) {
        return Err(ErrorReply::err(format!(
            "Invalid node address specified: {host}:{port_arg}"
        )));
    }
    let port = port as u16;
    // Learn the peer's REAL announce id over the bus (bounded); fall back to the synth id when the
    // peer is unreachable so the cluster still forms.
    let id = learn_or_synth_meet_id(&host, port).await;
    Ok(ironcache_raft::ConfigCmd::AddNode { id, host, port })
}

/// Resolve the node id to commit for a raft-mode `CLUSTER MEET <host> <port>`: the peer's REAL
/// announce id when it can be fetched within [`MEET_ID_FETCH_TIMEOUT`], else the deterministic
/// `synth_meet_node_id` fallback (item-7). Dials the peer's RESP CLIENT port (`host:port`) and
/// reads `CLUSTER MYID` via [`ironcache_clusterbus::peer_node_id`], BOUNDED by the Runtime timer
/// seam (`select!` of the fetch vs the timer) so a not-yet-up peer never hangs the serve path.
///
/// The fetched id is accepted ONLY when it is a syntactically valid node id (40 lowercase hex);
/// any other reply (an empty / malformed id, an error, a wrong reply kind) is treated as a failed
/// fetch and falls back to the synth id, so a peer that is up but not yet cluster-identity-ready
/// can never poison the committed table with a junk id.
pub(crate) async fn learn_or_synth_meet_id(host: &str, port: u16) -> String {
    let synth = || synth_meet_node_id(host, port);
    let rt = TokioRuntime::new();
    // The advertised CLIENT endpoint (what a MOVED redirect / a client dials). RESOLVE it accepting
    // a DNS hostname OR an IP literal (k8s): a hostname-addressed peer can now be dialed to learn its
    // real id, where the old IP-only parse fell straight back to the synth id for any DNS name. A
    // host that does not resolve (a peer not yet up) still falls back to the synth id so the cluster
    // forms; the id is reconciled later via the auto-promote / status path.
    //
    // H1: `resolve` is now ASYNC (getaddrinfo on tokio's blocking pool, bounded by RESOLVE_TIMEOUT
    // via the Runtime timer seam), so a wedged resolver can never freeze THIS serve task; it is
    // awaited with the same `rt` that bounds the id fetch below.
    let Ok(addr) = ironcache_clusterbus::PeerEndpoint::new(host, port)
        .resolve(&rt)
        .await
    else {
        return synth();
    };
    // Bound the fetch: whichever of the fetch or the timer completes first wins. The timer is the
    // sanctioned time seam (no `std::time` / `tokio::time` directly), matching the adapter's
    // FORWARD_TIMEOUT shape.
    let learned = tokio::select! {
        r = ironcache_clusterbus::peer_node_id(&rt, addr) => r.ok(),
        () = rt.timer(MEET_ID_FETCH_TIMEOUT) => None,
    };
    match learned {
        Some(id) if is_valid_node_id(&id) => id,
        // Unreachable / timed out / a non-id reply: fall back to the synth id so the cluster forms.
        _ => synth(),
    }
}

/// Whether `id` is a syntactically valid IronCache node id: exactly 40 lowercase-hex characters
/// (the shape `CLUSTER MYID` / the announce id / `synth_meet_node_id` all produce). Used to gate a
/// fetched MEET id so a peer that answers with a malformed / empty id falls back to the synth id
/// rather than committing junk into the node table.
pub(crate) fn is_valid_node_id(id: &str) -> bool {
    id.len() == 40
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Build the `RemoveNode { id }` ConfigCmd for raft-mode `CLUSTER FORGET <id>` (HA-4c).
fn build_forget(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() != 3 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    Ok(ironcache_raft::ConfigCmd::RemoveNode {
        id: String::from_utf8_lossy(&request.args[2]).into_owned(),
    })
}

/// Build the `SetConfigEpoch(epoch)` ConfigCmd for raft-mode `CLUSTER SET-CONFIG-EPOCH <epoch>`
/// (HA-4c). A negative epoch is the Redis invalid-epoch error.
fn build_set_config_epoch(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() != 3 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let Some(epoch) = ironcache_server::cmd_util::parse_i64(&request.args[2]) else {
        return Err(ErrorReply::not_an_integer());
    };
    if epoch < 0 {
        return Err(ErrorReply::err(format!(
            "Invalid config epoch specified: {epoch}"
        )));
    }
    Ok(ironcache_raft::ConfigCmd::SetConfigEpoch(epoch as u64))
}

/// Build the `AssignReplica { node, slots }` ConfigCmd for raft-mode `CLUSTER REPLICATE <node-id>
/// <slot> [slot ...]` (HA-7d). The first arg after the subcommand is the node id that should
/// REPLICATE the listed slots; the rest are strictly-validated slots. Arity is Min(4) (the verb,
/// REPLICATE, the node id, and at least one slot). The committed entry records `node` as the
/// replica of each slot in the shared map; the named node then attaches to each slot owner's
/// primary and serves READONLY reads.
fn build_replicate(
    request: &Request,
) -> Result<ironcache_raft::ConfigCmd, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::ErrorReply;
    if request.args.len() < 4 {
        return Err(ErrorReply::unknown_subcommand(
            "CLUSTER",
            &String::from_utf8_lossy(&request.args[1]),
        ));
    }
    let node = String::from_utf8_lossy(&request.args[2]).into_owned();
    let mut slots = Vec::with_capacity(request.args.len() - 3);
    for a in &request.args[3..] {
        slots.push(parse_slot_strict(a)?);
    }
    Ok(ironcache_raft::ConfigCmd::AssignReplica { node, slots })
}

/// THIS node's committed identity `(id, host, port)` for a self-`AddNode` / `AssignSlots`,
/// read from the shared map's self entry (the advertised endpoint a MOVED redirect points at).
/// Falls back to the boot node id + bind/port if the map is somehow absent (unreachable in
/// raft-mode, which always installs the shared map as `ctx.cluster`).
fn self_node_endpoint(ctx: &ServerContext) -> (String, String, u16) {
    match ctx.cluster.as_deref() {
        Some(m) => {
            let me = m.me();
            (me.id.to_string(), me.host.to_string(), me.port)
        }
        None => (
            ctx.info.cluster_node_id.to_owned(),
            ctx.boot.bind.to_string(),
            ctx.info.tcp_port,
        ),
    }
}

/// Parse + bounds-check a slot the way Redis's `getSlotOrReply` does for the mutator paths: a
/// non-integer OR an out-of-range value is the single `Invalid or out of range slot` error.
fn parse_slot_strict(arg: &[u8]) -> Result<u16, ironcache_protocol::ErrorReply> {
    use ironcache_protocol::{CLUSTER_SLOTS, ErrorReply};
    match ironcache_server::cmd_util::parse_i64(arg) {
        Some(n) if (0..i64::from(CLUSTER_SLOTS)).contains(&n) => Ok(n as u16),
        _ => Err(ErrorReply::err("Invalid or out of range slot")),
    }
}

/// Synthesize a deterministic 40-lowercase-hex placeholder node id from a MEET endpoint (FNV-1a
/// over `host:port`, hex-padded to 40), so the MEET'd peer is addressable before gossip learns
/// its real id. The SAME derivation the static slice-3 `cmd_cluster` MEET uses, so a node MEET'd
/// in either mode gets the identical id.
pub(crate) fn synth_meet_node_id(host: &str, port: u16) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let endpoint = format!("{host}:{port}");
    for b in endpoint.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let hex16 = format!("{h:016x}");
    let mut id = String::with_capacity(40);
    while id.len() < 40 {
        id.push_str(&hex16);
    }
    id.truncate(40);
    id
}
