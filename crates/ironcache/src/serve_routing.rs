// SPDX-License-Identifier: MIT OR Apache-2.0
//! The serve-layer ROUTER split out of `serve.rs` (#625): `route_and_dispatch` (the single per-command
//! router that classifies home vs cross-shard-hop vs fan-out and dispatches accordingly), the cluster
//! MOVED/ASKING/CROSSSLOT redirect gate, the write guardrail + migration-decision helpers, the
//! CLIENT-PAUSE stall, the one-shot ASKING/UNPAUSE consumers, and the blocking-wake + keyspace-event
//! publish post-dispatch hooks. Behavior-preserving relocation: the bodies are byte-identical.

use super::{
    BlockPark, ShardState, ShardStoreImpl, ascii_upper, deregister_all_subscriptions,
    dispatch_spanning_combine, encode_into, handle_acl_command, handle_blocking_live,
    handle_persist_command, handle_request, handle_shutdown_command, info_reply_includes_keyspace,
    is_fan_out_multikey, is_fan_out_spanning_combine, is_fan_out_spanning_move,
    is_serve_pubsub_command, is_serving, is_shard_loading, is_spanning_move_reject,
    reject_internal_verb, reject_spanning_move, route_in_multi, shard_blocking,
    subscriber_gate_blocks, try_handle_pubsub, try_raft_cluster_mutator,
};
use crate::coordinator;
use ironcache_env::{Clock, SystemEnv};
use ironcache_runtime::Runtime;
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, Request, TimingWheel, UnixMillis, route};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// The cluster slot-ownership decision for one command (CLUSTER_CONTRACT.md #70, slice 2):
/// returns `Some(error)` when the command must be REDIRECTED (`-MOVED`) or REJECTED
/// (`-CROSSSLOT`) because its key(s) are not served by THIS node, else `None` (proceed to the
/// normal local / internal-shard routing). It is a PURE function of `(map, route, cmd, request)`
/// and is the SINGLE source of the cluster redirect rule, reused by both the live command path
/// ([`route_and_dispatch`]) and the MULTI queue-time hook ([`route_in_multi`]).
///
/// The rules (matching Redis `getNodeByQuery` in src/cluster.c):
/// - KEYLESS / ADMIN commands carry no slot, so [`route::CommandClass::AlwaysHome`] and
///   [`route::CommandClass::WholeKeyspace`] are NEVER redirected (`-> None`). Only `KeyedSingle`
///   / `KeyedMulti` reach the slot logic.
/// - The CLIENT-VISIBLE slot is [`ironcache_protocol::key_slot`] (CRC16/XMODEM + the hash-tag
///   rule), NOT `route::hash64` (the internal FNV-1a shard hash): they answer different
///   questions (which NODE owns the wire slot vs which of MY shards owns the key).
/// - For a multi-key command, ALL keys must hash to ONE slot; if they SPAN slots the reply is
///   `-CROSSSLOT` and this is checked BEFORE ownership (a cross-slot command is CROSSSLOT even
///   when none of its slots is local, matching Redis), so cluster mode never scatters a
///   cross-slot multi-key command.
/// - A single resolved slot NOT owned by this node yields `-MOVED <slot> <owner host:port>`;
///   an owned (or co-located + owned) slot yields `None` and falls through unchanged.
///
/// A malformed / short request that yields no key ([`route::KeySpec::None`]) returns `None`, so
/// the home handler emits the proper wrong-arity error rather than a redirect.
/// Whether THIS node's replica link is currently IN SYNC within the configured
/// `replica_max_lag` (HA-8 replica-read staleness bound): the HA-7e `is_in_sync` signal off
/// `ctx.repl_status` (link up AND lag <= max_lag). Returns `false` when there is no repl-status
/// cell (the default static / non-raft path), which combined with the `readonly` gate keeps that
/// path's routing byte-unchanged (it never reaches the replica-serve leg anyway). Cold: reads a
/// handful of atomics off the node-level status cell, never per stored key.
pub(crate) fn replica_read_in_sync(ctx: &ServerContext) -> bool {
    ctx.repl_status
        .as_ref()
        .is_some_and(|s| s.is_in_sync(ctx.boot.replica_max_lag))
}

/// The HA-6 migration context the redirect needs that the static/WATCH paths do NOT: whether the
/// connection set the one-shot `ASKING` flag, and a resolver telling whether a given CLIENT-VISIBLE
/// key is PRESENT (and live) on the shard that OWNS it. The resolver is the only store-touching part
/// of the redirect; the serve path supplies it, so [`cluster_redirect`] / [`redirect_for_keys`]
/// stay pure functions over `(map, keys, ...)` plus this borrowed context. `None` (the static path,
/// raft-without-migration, and the WATCH guard) makes the redirect byte-identical to pre-HA-6: the
/// migration arms are reached ONLY when a slot is actually tagged MIGRATING/IMPORTING.
///
/// MULTI-SHARD EXACTNESS (COORDINATOR.md #107): the resolver is now EXACT on a multi-shard node. A
/// migrating-slot key lives on the shard it FNV-hashes to ([`route::owner_shard`]), which on a
/// multi-shard node may be a SIBLING of the accept shard. The serve path pre-resolves any such
/// non-home key on its owner shard via the coordinator presence hop ([`coordinator::presence_via`])
/// and feeds the EXACT owner-shard answer here, so the source ASKs only for genuinely absent keys
/// (no more spurious `-ASK` for a present sibling-shard key). With `shards == 1` every key is home,
/// so the resolver is a pure local `contains_live` read -- byte-identical to before this fix.
pub(crate) struct MigrationCtx<'a> {
    /// Whether the connection's one-shot `ASKING` flag is set for THIS command (consumed by the
    /// caller after dispatch). Gates serving an IMPORTING slot locally. `pub(crate)` so the in-MULTI
    /// queue-time redirect (`serve_txn_block::route_in_multi`) constructs the same ctx (#625).
    pub(crate) asking: bool,
    /// Resolve whether a CLIENT-VISIBLE key is present-and-live on the shard that OWNS it (the home
    /// shard for a home key; a sibling shard's pre-resolved answer for a non-home key on a
    /// multi-shard node). Used to decide ASK (all keys gone) vs serve (all present) vs TRYAGAIN
    /// (mixed) on a MIGRATING slot.
    pub(crate) key_present: &'a dyn Fn(&[u8]) -> bool,
}

/// The host a shard-owner node ADVERTISES in its `CLUSTER SLOTS` projection (and thus in every
/// MOVED it emits). Clients must be able to DIAL it, so an unspecified bind (`0.0.0.0` / `::`) --
/// which is not a connectable address -- falls back to loopback (shard-owners is a single-box mode;
/// its clients/benches dial localhost). A concrete bind IP is advertised as-is. There is no
/// `cluster-announce-ip` knob today; adding one is a follow-up for the cross-host case.
pub(crate) fn shard_owner_announce_host(bind: std::net::IpAddr) -> String {
    if bind.is_unspecified() {
        match bind {
            std::net::IpAddr::V6(_) => "::1",
            std::net::IpAddr::V4(_) => "127.0.0.1",
        }
        .to_owned()
    } else {
        bind.to_string()
    }
}

/// The home `ShardId` to hand the cluster redirect, but ONLY in shard-owners mode (`Some(home)` ->
/// the per-shard ownership predicate in [`moved_if_unowned`]; `None` -> the default single-self-node
/// redirect used by Static/Raft, byte-unchanged). Centralizes the mode check so every redirect call
/// site agrees on when per-shard ownership applies.
pub(crate) fn shard_owner_home(ctx: &ServerContext, home: ShardId) -> Option<ShardId> {
    (ctx.cluster_mode() == ironcache_config::ClusterMode::ShardOwners).then_some(home)
}

// The cluster redirect predicate takes many orthogonal inputs (the map, the command's class + key
// spec, two read-gate flags, the migration context, and the shard-owner home). Bundling them would
// obscure more than it clarifies, so allow the extra parameter.
#[allow(clippy::too_many_arguments)]
pub(crate) fn cluster_redirect(
    map: &ironcache_cluster::SlotMap,
    route: route::CommandClass,
    cmd_upper: &[u8],
    request: &Request,
    readonly: bool,
    replica_in_sync: bool,
    migration: Option<&MigrationCtx<'_>>,
    home_owner: Option<ShardId>,
) -> Option<ironcache_protocol::ErrorReply> {
    // (a) keyless / admin exemption: only KEYED data commands carry slots.
    let spec = match route {
        route::CommandClass::KeyedSingle => match route::single_key(request) {
            Some(k) => route::KeySpec::One(k),
            None => return None, // malformed/short: home handler emits the arity error.
        },
        route::CommandClass::KeyedMulti => route::command_keys(cmd_upper, request),
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => return None,
    };

    // HA-7d replica-read gate (REPLICA_READ.md #147): a READ on a READONLY connection MAY be
    // served locally by a replica of the slot. A WRITE never is (it returns MOVED to the owner),
    // and a non-READONLY connection never is (the default strong-read behavior). The command's
    // write-ness comes from the #89 registry (`is_write`); an unknown command is treated as a
    // write (conservative), so a replica never serves an unrecognized command locally.
    //
    // HA-8 REPLICA-READ STALENESS BOUND (REPLICA_READ.md, finishing the 7d TODO): a replica may
    // serve the READONLY read ONLY while WITHIN the lag bound (link up AND lag <= max_lag, the
    // HA-7e `is_in_sync` signal, threaded in as `replica_in_sync`). Past the bound (or link down)
    // it is NOT in sync, so `replica_serves` is false and the slot it replicates-but-does-not-own
    // returns MOVED to the OWNER -- a stale replica never serves a stale read. In the default
    // static path there is no replication, so the caller passes `replica_in_sync = false` AND
    // `readonly` is the only other gate, keeping that path byte-unchanged (a static node owns its
    // slots, so it never reaches the replica leg regardless).
    let replica_serves =
        readonly && replica_in_sync && !ironcache_server::command_spec::is_write(cmd_upper);

    // (b) reduce the key(s) to a slot via the CLIENT-VISIBLE key_slot (CRC16 + hash-tag) and
    // apply the ONE shared redirect rule (CROSSSLOT-before-MOVED). The `route::KeySpec` is just
    // a borrowed view over the request bytes, so collapse it to an iterator of key slices and
    // hand it to `redirect_for_keys` (the SINGLE predicate WATCH also uses).
    match spec {
        // No routable key (malformed / short): fall through, the handler errors properly.
        route::KeySpec::None => None,
        route::KeySpec::One(k) => redirect_for_keys(
            map,
            std::iter::once(k),
            replica_serves,
            migration,
            home_owner,
        ),
        route::KeySpec::Many(keys) => redirect_for_keys(
            map,
            keys.iter().copied(),
            replica_serves,
            migration,
            home_owner,
        ),
    }
}

/// The SINGLE cluster redirect predicate over a sequence of CLIENT-VISIBLE keys: the one
/// place the CROSSSLOT-before-MOVED rule lives, shared by [`cluster_redirect`] (data commands)
/// and the WATCH cluster guard in [`route_and_dispatch`] (WATCH is `AlwaysHome` for
/// connection-state reasons but carries a key spec in Redis, so it must redirect like a keyed
/// command). Returns `Some(error)` when the keys must be REJECTED (`-CROSSSLOT`, they span
/// slots) or REDIRECTED (`-MOVED`, their single slot is foreign), else `None` (proceed local).
///
/// The rule, matching Redis `getNodeByQuery` (src/cluster.c):
/// - reduce each key to its slot via [`ironcache_protocol::key_slot`] (CRC16/XMODEM + hash-tag);
/// - if any key's slot differs from the first key's slot -> `-CROSSSLOT` (checked BEFORE
///   ownership: a cross-slot request is CROSSSLOT even when none of its slots is local);
/// - else the request resolves to ONE slot -> `-MOVED <slot> <owner host:port>` if this node
///   does not own it, else `None`.
///
/// An EMPTY key sequence yields `None` (no routable key: the home handler errors properly); it
/// cannot occur for a well-formed command but is handled defensively rather than indexing.
///
/// HA-6: when `migration` is `Some`, the single resolved slot's MIGRATING / IMPORTING state is
/// consulted AFTER CROSSSLOT but BEFORE the plain MOVED, producing `-ASK` / serve-locally /
/// `-TRYAGAIN` per [`migration_decision`]. When `None` (static / WATCH path) the migration arm is
/// skipped entirely and the result is byte-identical to pre-HA-6.
pub(crate) fn redirect_for_keys<'a, I>(
    map: &ironcache_cluster::SlotMap,
    keys: I,
    replica_serves: bool,
    migration: Option<&MigrationCtx<'_>>,
    home_owner: Option<ShardId>,
) -> Option<ironcache_protocol::ErrorReply>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    // Collect the keys into a slice so the migration arm can iterate them twice (presence per key);
    // for the common single-key command this is a one-element Vec and the CROSSSLOT loop is trivial.
    let key_vec: Vec<&[u8]> = keys.into_iter().collect();
    let first = *key_vec.first()?;
    let first_slot = ironcache_protocol::key_slot(first);
    // CROSSSLOT (keys span slots) takes precedence over MOVED/ASK, regardless of ownership: a
    // cross-slot request is rejected, never scattered.
    for &k in &key_vec[1..] {
        if ironcache_protocol::key_slot(k) != first_slot {
            return Some(ironcache_protocol::ErrorReply::crossslot());
        }
    }
    // HA-6: if the slot is mid-migration AND the caller supplied a migration context, the per-key
    // cutover decision (ASK / serve / TRYAGAIN / IMPORTING-ASKING) replaces the plain MOVED. The
    // function returns None to FALL THROUGH to the static decision below when the slot is not
    // migrating, so the default path is unchanged.
    if let Some(mig) = migration {
        if let Some(decision) = migration_decision(map, first_slot, &key_vec, mig) {
            return decision.into_reply();
        }
    }
    // All keys co-locate on one non-migrating slot: MOVED if this node neither owns nor (read-only)
    // replicates it. `replica_serves` carries the HA-7d READONLY-read gate (see `moved_if_unowned`).
    moved_if_unowned(map, first_slot, replica_serves, home_owner)
}

/// THE WRITE-SIDE replication guardrail decision (ADR-0026, Redis `min-replicas-to-write`). Returns
/// `Some(-NOREPLICAS)` when a WRITE to a slot THIS node owns must be REJECTED because fewer than
/// `min_replicas_to_write` replicas are currently in sync, else `None` (the write proceeds).
///
/// The CALLER has already established `ctx.boot.min_replicas_to_write > 0` (the byte-unchanged
/// short-circuit) and that the redirect returned `None` (so a keyed slot here is OWNED, not foreign
/// / read-replica-served). This function applies the remaining gates and is otherwise a PURE
/// decision over the context + the parsed request (it reads only the count atomic + the slot map +
/// the registry `is_write` bit; no store, no time, no rand):
///
/// 1. ONLY WRITES: a read command is never blocked (`is_write` from the #89 registry). An unknown
///    command is conservatively a write, but the redirect already passed it, so it is keyless/admin
///    (gate 3 then exempts it).
/// 2. ONLY in raft-mode with the count cell present (`ctx.in_sync_replicas` is `Some` iff raft-mode,
///    the same gate the cell is created under). `None` -> no guardrail (defensive; the caller's
///    `> 0` gate plus a static node having no cell already excludes this).
/// 3. ONLY OWNED KEYED slots: a keyless / admin / whole-keyspace command carries no slot, so it is
///    EXEMPT (Redis gates `min-replicas-to-write` on the per-command `is-write` + a key; a keyless
///    admin write like FLUSHALL is not slot-replicated through this path). A keyed command's slot is
///    resolved via the CLIENT-VISIBLE `key_slot`; the redirect guarantees this node OWNS it.
/// 4. THE QUORUM: reject when the in-sync replica count (`InSyncReplicas::count`, ONE relaxed load)
///    is BELOW `min_replicas_to_write`.
pub(crate) fn write_guardrail(
    ctx: &ServerContext,
    route: route::CommandClass,
    cmd_upper: &[u8],
    request: &Request,
) -> Option<ironcache_protocol::ErrorReply> {
    // (1) ONLY WRITES. A read is never blocked.
    if !ironcache_server::command_spec::is_write(cmd_upper) {
        return None;
    }
    // (2) The count cell exists ONLY in raft-mode (the same gate it is created under). Without it
    // there is no replication to gate on, so the guardrail does not apply.
    let in_sync = ctx.in_sync_replicas.as_deref()?;

    // (3) ONLY a KEYED slot this node OWNS. A keyless / admin / whole-keyspace command carries no
    // routable slot, so it is exempt (mirrors `cluster_redirect`'s keyless exemption). For a keyed
    // command we resolve its CLIENT-VISIBLE slot; the redirect already ensured this node owns it
    // (a foreign slot returned MOVED above and never reaches here), so an owned keyed write is the
    // only case that proceeds to the quorum check.
    let has_owned_keyed_slot = match route {
        route::CommandClass::KeyedSingle => route::single_key(request).is_some(),
        route::CommandClass::KeyedMulti => {
            // A multi-key write co-locates on one owned slot (CROSSSLOT was rejected by the redirect
            // above), so the presence of any key means an owned keyed slot is being written.
            !matches!(
                route::command_keys(cmd_upper, request),
                route::KeySpec::None
            )
        }
        // No slot: keyless / admin / whole-keyspace writes are exempt (not slot-replicated here).
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => false,
    };
    if !has_owned_keyed_slot {
        return None;
    }

    // (4) THE QUORUM: reject when too few replicas are currently in sync. ONE relaxed atomic load,
    // delegated to the pure decision so the gate is unit-testable without a ServerContext.
    write_guardrail_decision(ctx.boot.min_replicas_to_write, in_sync.count())
}

/// THE PURE write-side quorum decision (ADR-0026), split out of [`write_guardrail`] so the
/// reject/allow rule is unit-testable over plain values (no `ServerContext`, no atomics, no I/O).
/// Returns `Some(-NOREPLICAS)` when the live `in_sync_count` is BELOW the required
/// `min_replicas_to_write`, else `None` (the write proceeds). The CALLER has already applied the
/// is-write / owned-keyed-slot / raft-mode gates; this is only the final count compare.
///
/// `min_required == 0` would never reach here (the hot-path caller short-circuits on `> 0` before
/// touching the count), but it is handled correctly anyway: `count >= 0` always holds, so it
/// returns `None` (allow), which is the byte-unchanged default.
#[must_use]
pub(crate) fn write_guardrail_decision(
    min_required: u32,
    in_sync_count: usize,
) -> Option<ironcache_protocol::ErrorReply> {
    if (in_sync_count as u64) < u64::from(min_required) {
        Some(ironcache_protocol::ErrorReply::no_replicas())
    } else {
        None
    }
}

/// The outcome of the HA-6 migration redirect decision for one slot's keys. Distinct from a bare
/// `Option<ErrorReply>` so the "serve locally" outcome (None reply) is explicit and cannot be
/// confused with "not migrating, fall through to the static decision".
enum MigrationDecision {
    /// Serve the command locally (the keys are present here on a MIGRATING slot, or this is an
    /// IMPORTING slot with ASKING set). The redirect returns `None`.
    Serve,
    /// `-ASK <slot> <dest:port>`: every key has already migrated to the destination.
    Ask(ironcache_protocol::ErrorReply),
    /// `-TRYAGAIN ...`: a multi-key command on a MIGRATING slot whose keys are split.
    TryAgain,
}

impl MigrationDecision {
    /// The redirect reply for this decision: `None` to serve locally, `Some(error)` to redirect.
    fn into_reply(self) -> Option<ironcache_protocol::ErrorReply> {
        match self {
            MigrationDecision::Serve => None,
            MigrationDecision::Ask(reply) => Some(reply),
            MigrationDecision::TryAgain => Some(ironcache_protocol::ErrorReply::tryagain()),
        }
    }
}

/// THE HA-6 per-slot migration redirect decision (the heart of online slot migration). Returns
/// `Some(decision)` when `slot` is mid-migration in a way that overrides the plain MOVED/serve, or
/// `None` when the slot is NOT migrating (the caller falls through to the static MOVED/owns/replica
/// decision, so the default path is byte-unchanged).
///
/// The decision table (real Redis Cluster semantics, adapted to the Raft-committed map):
///
/// - Slot is MIGRATING toward `dest` AND THIS node still OWNS it (the SOURCE side):
///   * EVERY key is present locally -> Serve (the key has not migrated yet; serve it here).
///   * EVERY key is absent locally -> `-ASK <slot> <dest>` (migrated already / never existed; the
///     destination is where it lives now -- a ONE-TIME hint, NOT MOVED, ownership unchanged).
///   * MIXED (some present, some absent; only possible for a multi-key command) -> `-TRYAGAIN`
///     (cannot serve atomically on either side; the client retries as the migration converges).
/// - Slot is IMPORTING from `src` AND THIS node does NOT yet own it (the DESTINATION side):
///   * the connection set `ASKING` -> Serve (the migrated key has arrived; this is the second leg
///     of the ASK redirect).
///   * `ASKING` NOT set -> `None` (fall through to the static MOVED-to-owner: a client that lands
///     here without ASKING is talking to the wrong node for a slot it does not own yet).
/// - Any other combination (not migrating, or MIGRATING but this node does not own it, or IMPORTING
///   but already owns it) -> `None` (fall through to the static decision).
///
/// SAFETY: this never grants ownership; it only decides WHERE a request is served DURING the
/// migration window. Ownership transfers solely through the committed FLIP, after which the slot is
/// no longer MIGRATING/IMPORTING (the FLIP clears it) and this returns `None` -> the source serves
/// MOVED and the destination owns. So there is never a state where two nodes both serve a key as
/// owner: the source serves only present keys (handing absent ones to dest via ASK), and the dest
/// serves only under ASKING (or after it owns).
/// The NON-HOME keys whose presence the HA-6 migration ASK decision must resolve on a SIBLING
/// shard (COORDINATOR.md #107, the multi-shard exactness fix), each paired with its OWNER shard
/// index, or `None` when no cross-shard presence resolution is needed (so the caller uses the
/// byte-identical LOCAL `contains_live` resolver). The fast `None` short-circuit keeps the
/// `shards == 1` / default / hot path untouched.
///
/// Returns `None` (use the local resolver) UNLESS ALL of:
/// - there is more than one shard (`home.total > 1`); with one shard every key is home-owned, so
///   the local read is already exact -- this is the FIRST gate, so the single-shard path never
///   even looks at the slot or the keys (byte-identical to pre-fix);
/// - the command is KEYED (only `KeyedSingle` / `KeyedMulti` carry a slot the migration arm reads);
/// - the command's keys resolve to ONE slot (a CROSSSLOT multi-key command is rejected before the
///   migration arm, so presence is never consulted for it) and THIS node is MIGRATING that slot
///   (`MigrationState::Migrating` AND `owns(slot)` -- the ONLY arm of `migration_decision` that
///   calls `key_present`; IMPORTING / non-migrating slots never consult presence);
/// - at least one key is NOT home-owned (a key on a SIBLING shard, where the accept-shard read
///   could be wrong). When EVERY key is home-owned, the local read is exact and we return `None`.
///
/// When it returns `Some`, the vec holds EVERY non-home key of the migrating slot (deduplicated)
/// paired with its FNV `owner_shard` -- the SAME hash the coordinator routes a single-key op with,
/// so the presence hop lands on the shard that actually stores the key. Home-owned keys are
/// deliberately OMITTED (the caller resolves them locally), so a co-located subset still uses the
/// zero-hop local read. The migrating slot's keys all share ONE client-visible slot (CROSSSLOT is
/// enforced upstream) but may map to DIFFERENT internal FNV shards, so the multi-key case can yield
/// several owners -- one presence hop each (mirroring the coordinator's per-owner multi-key gather).
pub(crate) fn xshard_presence_keys(
    map: &ironcache_cluster::SlotMap,
    route: route::CommandClass,
    cmd_upper: &[u8],
    request: &Request,
    home: ShardId,
) -> Option<Vec<(Vec<u8>, usize)>> {
    use ironcache_cluster::MigrationState;
    // FIRST gate: a single-shard node never needs a cross-shard hop (every key is home-owned).
    // This short-circuits BEFORE touching the slot map or extracting keys, so the shards == 1
    // path is byte-identical to before this fix.
    if home.total <= 1 {
        return None;
    }
    // Only KEYED commands carry a slot the migration arm consults; reduce to the key spec exactly
    // as `cluster_redirect` does (so "which bytes are keys" cannot drift from the redirect).
    let spec = match route {
        route::CommandClass::KeyedSingle => match route::single_key(request) {
            Some(k) => route::KeySpec::One(k),
            None => return None,
        },
        route::CommandClass::KeyedMulti => route::command_keys(cmd_upper, request),
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => return None,
    };
    let keys: Vec<&[u8]> = match spec {
        route::KeySpec::None => return None,
        route::KeySpec::One(k) => vec![k],
        route::KeySpec::Many(ks) => ks,
    };
    let first = *keys.first()?;
    let slot = ironcache_protocol::key_slot(first);
    // A multi-key command spanning client-visible slots is rejected (-CROSSSLOT) BEFORE the
    // migration arm, so presence is never consulted for it; only resolve when every key shares the
    // first key's slot (the single-slot case the migration arm actually reaches).
    if keys[1..]
        .iter()
        .any(|k| ironcache_protocol::key_slot(k) != slot)
    {
        return None;
    }
    // Presence is consulted ONLY on the migration SOURCE arm (MIGRATING + this node owns the slot);
    // IMPORTING / non-migrating slots never call `key_present`, so no hop is needed for them.
    if !(map.migration_state(slot) == MigrationState::Migrating && map.owns(slot)) {
        return None;
    }
    // Collect the NON-home keys (deduplicated) with their owner shard. A home-owned key is resolved
    // locally by the caller (zero hop), so omit it. If EVERY key is home-owned, return None so the
    // caller uses the pure local resolver (no cross-shard work at all).
    let mut remote: Vec<(Vec<u8>, usize)> = Vec::new();
    for &k in &keys {
        let owner = route::owner_shard(k, home.total);
        if owner != home.index && !remote.iter().any(|(existing, _)| existing.as_slice() == k) {
            remote.push((k.to_vec(), owner));
        }
    }
    if remote.is_empty() {
        None
    } else {
        Some(remote)
    }
}

fn migration_decision(
    map: &ironcache_cluster::SlotMap,
    slot: u16,
    keys: &[&[u8]],
    mig: &MigrationCtx<'_>,
) -> Option<MigrationDecision> {
    use ironcache_cluster::MigrationState;
    match map.migration_state(slot) {
        MigrationState::Migrating if map.owns(slot) => {
            // SOURCE side: decide by local key presence.
            let mut any_present = false;
            let mut any_absent = false;
            for &k in keys {
                if (mig.key_present)(k) {
                    any_present = true;
                } else {
                    any_absent = true;
                }
            }
            if any_present && any_absent {
                // Multi-key split across the cutover: cannot serve atomically -> TRYAGAIN.
                Some(MigrationDecision::TryAgain)
            } else if any_absent {
                // All keys gone (migrated / never existed): ASK to the destination. The dest
                // endpoint must resolve; if it somehow does not (peer forgotten mid-migration),
                // `map()` yields None and we fall through to the static decision rather than dial a
                // nonexistent node.
                map.migration_peer_endpoint(slot).map(|(host, port)| {
                    MigrationDecision::Ask(ironcache_protocol::ErrorReply::ask(
                        slot,
                        &format!("{host}:{port}"),
                    ))
                })
            } else {
                // All keys present: serve locally (not migrated yet).
                Some(MigrationDecision::Serve)
            }
        }
        MigrationState::Importing if !map.owns(slot) => {
            // DESTINATION side: serve locally ONLY under ASKING; otherwise fall through to MOVED.
            if mig.asking {
                Some(MigrationDecision::Serve)
            } else {
                None
            }
        }
        // Not migrating, or a migration tag that does not match this node's ownership (e.g. a stale
        // MIGRATING tag on a node that no longer owns the slot): fall through to the static rule.
        _ => None,
    }
}

/// `Some(-MOVED <slot> <owner host:port>)` when THIS node does not own `slot` (and does not serve
/// it as a read-only replica), else `None`.
///
/// `replica_serves` is the HA-7d replica-read gate: `true` when the request is a READ on a
/// READONLY connection (computed by the caller from `conn.readonly && !is_write`). When set, a
/// slot this node does NOT own but IS a committed replica of ([`SlotMap::is_replica_of_self`]) is
/// served LOCALLY (returns `None`), the replica-read leg of REPLICA_READ.md #147. A write, a
/// non-READONLY read, or a slot this node neither owns nor replicates still returns `-MOVED` to
/// the OWNER. `replica_serves` is `false` for every non-replica/non-readonly path, so the default
/// (owner-only) routing is byte-unchanged; the cold `is_replica_of_self` check runs ONLY when a
/// slot is already known foreign AND the connection opted into replica reads.
///
/// The redirect target is the OWNER node's advertised `host:port` (what the client should dial),
/// never the bind address. `moved_target` resolves the owner's advertised endpoint under the
/// node lock (the COLD redirect path); the `?` on its `None` (an unassigned slot) is defensive
/// (an empty-self / mid-formation node may not yet own the slot, so we simply do not redirect
/// rather than dial a nonexistent owner).
fn moved_if_unowned(
    map: &ironcache_cluster::SlotMap,
    slot: u16,
    replica_serves: bool,
    home_owner: Option<ShardId>,
) -> Option<ironcache_protocol::ErrorReply> {
    // SHARD-OWNERS (#517 PR4): the projection map has N nodes (one per shard), but every shard shares
    // ONE `ctx.cluster`, so `map.owns(slot)` (which asks the SINGLE self-node) cannot tell shard i
    // from shard j. Instead this shard owns `slot` iff the CONTIGUOUS partition maps it here --
    // `slot_to_shard(slot, N) == home.index` -- the SAME predicate the internal hop uses, so when a
    // client dialed the right owner port (homed here) it serves locally with neither MOVED nor hop.
    // A foreign slot is MOVED to its owner's advertised `host:base+owner` (resolved from the N-node
    // map). Replica reads do not apply in shard-owners mode (no replication), so that leg is skipped.
    if let Some(home) = home_owner {
        if route::slot_to_shard(slot, home.total) == home.index {
            return None;
        }
        let (host, port) = map.moved_target(slot)?;
        return Some(ironcache_protocol::ErrorReply::moved(
            slot,
            &format!("{host}:{port}"),
        ));
    }
    if map.owns(slot) {
        return None;
    }
    // HA-7d replica read: a READONLY read for a slot this node replicates is served locally.
    if replica_serves && map.is_replica_of_self(slot) {
        return None;
    }
    let (host, port) = map.moved_target(slot)?;
    Some(ironcache_protocol::ErrorReply::moved(
        slot,
        &format!("{host}:{port}"),
    ))
}

/// ROUTE + DISPATCH one decoded request (COORDINATOR.md #107, Stage 1), appending its
/// encoded reply to `out` and returning whether the connection should close (QUIT). Split
/// out of the serve loop so the connection loop stays small; the routing decision is:
///
/// - KEYED (single/multi) command whose key(s) ALL resolve to ONE shard -> that shard:
///   the LOCAL fast path (sync `handle_request`) when it is home, else a single remote HOP
///   ([`coordinator::dispatch_via`]). A key-SPANNING multi-key command stays HOME (the
///   documented Stage 2 fan-out gap).
/// - WHOLE-KEYSPACE (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY) -> SCATTER-GATHER across
///   ALL shards so it covers the WHOLE keyspace (not just the home shard's ~1/N): SCAN is a
///   single-shard-per-call COMPOSITE-cursor walk ([`crate::whole_keyspace::scan_cross_shard`]),
///   the rest broadcast + merge ([`crate::whole_keyspace::fan_out_and_merge`]).
/// - AlwaysHome (control/conn/txn, SWAPDB, unknown) -> HOME (sync `handle_request`).
///
/// With shards == 1 every key is home-owned and the fan-out degenerates to the single local
/// call, so the whole path is byte-identical (no channel) to before this layer.
///
/// The per-connection `commands_processed` is bumped here for the remote / fan-out paths
/// (matching the bump `handle_request` does on the home path), so every command is counted
/// exactly once regardless of route.
///
/// The router enforces a STRICT ORDER for the pub/sub-related gates (the root-cause fix for the
/// adversarial-review findings): the internal-verb gate (FIX F), then the in-MULTI pub/sub REJECT
/// (FIX C), then the RESP2 subscribe-mode gate (FIX B, MOVED to run BEFORE pub/sub interception so
/// a RESP2 subscriber's PUBLISH/PUBSUB is rejected), then RESET interception (FIX A, deregisters
/// subscriptions + swaps the push channel), then `try_handle_pubsub`. The previous order ran the
/// pub/sub interception BEFORE both the in-MULTI gate and the subscribe-mode gate, so pub/sub
/// commands BYPASSED both; the order below closes that hole.
///
/// `too_many_lines` is allowed: this is the connection's central ROUTING HUB (the internal-verb
/// gate, the in-MULTI pub/sub reject, the subscribe-mode gate, RESET interception, the pub/sub
/// interception, the in-MULTI/WATCH guards, then the keyed / multikey / spanning / whole-keyspace
/// / home branches), each a documented decision the router must make in one place; splitting it
/// further would scatter the routing contract. The same precedent as `dispatch_inner` /
/// `command_spec::spec_of`.
/// HA-6: consume the one-shot `ASKING` flag for THIS command and return whether it was set.
///
/// `ASKING` itself just SETS the flag (handled in the router) and must NOT consume the flag it is
/// about to set, so for the `ASKING` command this returns `false` WITHOUT touching `conn.asking`.
/// For EVERY other command it reads the flag, CLEARS it, and returns its prior value. Calling this
/// EXACTLY ONCE at the top of `route_and_dispatch` -- before any early return (pubsub / in_multi /
/// WATCH) -- is what guarantees a set flag can never LEAK into a later command (the adversarial-
/// review Finding 1 hole). It is a single bool read+write; a non-cluster / non-migrating connection
/// never sets `asking`, so the value is always `false` and the static path is unaffected.
pub(crate) fn consume_one_shot_asking(cmd_upper: &[u8], conn: &mut ConnState) -> bool {
    if cmd_upper == b"ASKING" {
        false
    } else {
        let a = conn.asking;
        conn.asking = false;
        a
    }
}

/// Whether `request` is `CLIENT UNPAUSE` (case-insensitive), the pause-RECOVERY command that
/// [`pause_stall`] must never hold. Cheap and short-circuiting: it checks the `CLIENT` token first
/// (so a non-CLIENT command bails after one compare) and only then the `UNPAUSE` subcommand. Reached
/// ONLY when a pause is armed, never on the default hot path.
pub(crate) fn request_is_client_unpause(request: &Request) -> bool {
    request.args.len() == 2
        && request.args[0].eq_ignore_ascii_case(b"CLIENT")
        && request.args[1].eq_ignore_ascii_case(b"UNPAUSE")
}

/// The PER-COMMAND `CLIENT PAUSE` gate (#388, write-aware). Called in the serve loop's decode loop
/// for EACH decoded command, right BEFORE it is dispatched, so the command is HELD while a pause
/// that applies to it is active and released the instant the window clears or `CLIENT UNPAUSE` runs.
/// This is the SINGLE point where both pause kinds are honored, which is why it is correct under
/// PIPELINING (in a batch of mixed reads + writes under a WRITE pause, each read passes here and
/// each write holds here) and why it holds the VERY NEXT command after a pause begins (the old
/// post-batch stall let the first command after a pause slip through, since it stalled only AFTER
/// replying to the current batch):
///
/// * an ALL pause holds EVERY command (reads included), matching the prior ALL behavior;
/// * a WRITE-only pause holds ONLY writes -- reads + admin (PING/INFO/SAVE/...) flow straight
///   through -- making `CLIENT PAUSE WRITE` genuinely write-only (Redis semantics). This is the fix
///   for the ironcache-upgrade write-freeze, where the upgrade issues `CLIENT PAUSE WRITE` then
///   `SAVE`: the old superset stall held the SAVE too, deadlocking the upgrade's own snapshot.
///
/// HOT PATH (no pause): a SINGLE relaxed atomic load via [`ClientRegistry::is_pause_armed`] returns
/// `false`, so this returns immediately -- NO clock read, NO command uppercasing, NO classification,
/// NO further work. The default (never-paused) connection therefore pays only that one load per
/// command, and the rest of this function is never entered (the byte-identical hot path).
///
/// When a pause IS armed it reads the kind once. For a WRITE-only pause it classifies the command
/// via [`request_is_write_for_pause`] (which also covers `EXEC` of a write-containing transaction
/// and does NOT hold a command merely being queued inside a `MULTI`); a non-write proceeds at once.
/// A held command stalls in a short poll loop (a ~50ms quantum + an Env-monotonic deadline + the
/// Runtime timer seam) until the relevant remaining-ms reaches `0` (window expiry or `CLIENT
/// UNPAUSE`) or the connection is `CLIENT KILL`ed. It does NOT itself close the connection: the
/// caller re-checks `client_handle.is_killed()` after dispatch.
pub(crate) async fn pause_stall<T: Runtime>(
    ctx: &ServerContext,
    conn: &ConnState,
    request: &Request,
    env: &Rc<RefCell<SystemEnv>>,
    timer_rt: &T,
    client_handle: &ironcache_observe::ClientHandle,
) {
    // The single cheap guard: nothing recorded -> return without touching the clock, UPPERCASING the
    // command token, or classifying it. This keeps the default (no pause) hot path to one relaxed
    // atomic load per command.
    if !ctx.clients.is_pause_armed() {
        return;
    }
    // RECOVERY EXEMPTION: `CLIENT UNPAUSE` is NEVER held by a pause -- it is the command that LIFTS
    // the pause, so an ALL pause (which otherwise holds every command) would be UNRECOVERABLE from
    // the very connection that set it if its own UNPAUSE were stalled. This is the un-wedge the
    // ironcache-upgrade safe-abort relies on. (Under a WRITE pause, CLIENT is already a non-write and
    // passes the classifier below; the exemption is only load-bearing for an ALL pause.) Cheap: only
    // reached when a pause is armed, and short-circuits on the `CLIENT` token before the subcommand
    // compare.
    if request_is_client_unpause(request) {
        return;
    }
    // A pause is armed. A WRITE-only pause holds ONLY writes; an ALL pause holds everything. For a
    // WRITE pause, classify the command (uppercasing is only paid on this cold, paused path) and let
    // a non-write through. For an ALL pause every command is held.
    if ctx.clients.pause_is_writes_only() {
        let cmd_upper = ascii_upper(request.command());
        if !ironcache_server::request_is_write_for_pause(&cmd_upper, conn.in_multi, &conn.queued) {
            return;
        }
    }
    // Hold this command until the applicable pause window clears (a write is blocked by BOTH kinds;
    // a non-write reaches here only under an ALL pause), an UNPAUSE clears it, or the connection is
    // killed. `pause_write_remaining_ms` is the raw window for either kind, so it is the correct
    // remaining for both the write-under-any-pause case and the any-command-under-ALL case.
    loop {
        let now_mono_ms = env.borrow().now().as_millis();
        let remaining = ctx.clients.pause_write_remaining_ms(now_mono_ms);
        if remaining == 0 {
            break;
        }
        if client_handle.is_killed() {
            break;
        }
        let wait = remaining.min(50);
        timer_rt
            .timer(core::time::Duration::from_millis(wait))
            .await;
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn route_and_dispatch(
    ctx: &ServerContext,
    conn: &mut ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    push_tx: &mut tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    push_rx: &mut tokio::sync::mpsc::Receiver<crate::pubsub::ServerPush>,
    shed_flag: &mut std::sync::Arc<crate::pubsub::ShedSignal>,
    env: &Rc<RefCell<SystemEnv>>,
    store_rc: &Rc<RefCell<ShardStoreImpl>>,
    wheel_rc: &Rc<RefCell<TimingWheel>>,
    state_rc: &Rc<RefCell<ShardState>>,
    persist: Option<&Arc<crate::persist::PersistState>>,
    request: &Request,
    out: &mut Vec<u8>,
    block_request: &mut Option<BlockPark>,
    // CROSS-SHARD HOP OVERLAP (#8): when `defer_hops` is true (the tokio serve loop opts in), a
    // single-target remote hop ENQUEUES its ShardWork and returns the reply receiver via
    // `deferred_hop` INSTEAD of awaiting it inline -- so a pipeline of hops runs concurrently (the
    // owner drains the run FIFO) rather than N serialized round-trips. The caller parks the receiver
    // and drains it in order. When `defer_hops` is false (io_uring loop, or any non-pipelined caller)
    // the hop is awaited inline exactly as before and `deferred_hop` stays `None` -- byte-identical.
    defer_hops: bool,
    deferred_hop: &mut coordinator::HopOutcome,
) -> bool {
    // -- THE GLOBAL SERVE GATE (#391 PR-5 streamed live-cutover). Until THIS process has COMMITTED the
    // cross-shard cutover, reject EVERY client command with `-LOADING` -- BEFORE the command name is
    // even classified -- so a client never reads a half-loaded or not-yet-committed store. `is_serving`
    // is a single process-global relaxed load that is `true` on every normal (non-handoff) boot, so the
    // default datapath pays one predictable-not-taken branch and is BYTE-UNCHANGED; it is `false` ONLY
    // on a streamed-handoff RECEIVER boot, and flips to `true` EXACTLY ONCE, atomically for all shards
    // (one global bool, no per-shard stagger), on the PR-4 `Committed` transition
    // (`upgrade::commit::begin_serving_on_commit`). That flip happens only AFTER the OLD released write
    // authority (permanent quiesce, PR-4), so no write is ever double-acked across the cutover. This
    // reuses PR-3's retryable `-LOADING` (`ErrorReply::loading`); the connection stays OPEN (returns
    // `false`) so the client retries against whichever process ends up authoritative. In production the
    // orchestrator (PR-6) keeps the NEW's acceptor closed until the flip, so this gate is
    // defense-in-depth that never fires on the normal path.
    if !is_serving() {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::loading()),
            conn.proto,
        );
        return false;
    }

    let cmd_upper = ascii_upper(request.command());
    let route = route::classify(&cmd_upper);

    // -- THE HOISTED NOAUTH CHOKEPOINT (production security fix). This is the SINGLE earliest
    // point after the command name is known but BEFORE any interception, cross-shard fan-out,
    // CLUSTER-mutator proposal, persistence/shutdown handling, MULTI queueing, or local dispatch.
    // EVERY client command reaches dispatch THROUGH this router, so gating here closes -- in one
    // place -- the whole class of auth-bypass holes that existed when the gate lived DOWNSTREAM in
    // `dispatch_inner` (which the router's early-returning forks never reach):
    //   * a GET/SET on a FOREIGN-shard key (the `coordinator::dispatch_via` remote hop below),
    //   * the whole-keyspace fan-outs (KEYS/SCAN/DBSIZE/FLUSHDB/FLUSHALL/RANDOMKEY),
    //   * the multi-key + spanning-combine scatter-gather fan-outs,
    //   * the CLUSTER topology mutators (MEET/FORGET/ADDSLOTS/SETSLOT/DELSLOTS/REPLICATE/
    //     SET-CONFIG-EPOCH, whether handled synchronously by `cmd_cluster` or proposed via Raft),
    //   * SAVE/BGSAVE/LASTSAVE + SHUTDOWN (previously point-fixed inline; now gated here too),
    //   * a command issued INSIDE a MULTI (the `route_in_multi` queue path is downstream of here,
    //     so a queued command from an unauth client is rejected, never staged -- Redis parity).
    // The pre-auth allow-list is the EXACT shared `command_allowed_pre_auth` predicate the
    // downstream `dispatch_with_cmd` gate uses (AUTH/HELLO/QUIT/RESET), so the two can never
    // diverge and AUTH / HELLO AUTH still work pre-auth unchanged.
    //
    // DEFAULT (no requirepass) is BYTE-UNCHANGED + adds no cost: `ctx.requires_auth()` reads the
    // runtime requirepass overlay (the same load the connection's `authenticated` init + the
    // dispatch gate already do) and short-circuits the `&&` immediately, so an authed or
    // no-auth-configured connection pays at most this single bool check before falling through to
    // the identical routing below. The reply is the IDENTICAL `-NOAUTH` the dispatch gate emits.
    if ctx.requires_auth()
        && !conn.authenticated
        && !ironcache_server::command_allowed_pre_auth(&cmd_upper)
    {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::noauth()),
            conn.proto,
        );
        return false;
    }

    // -- LIVE-REVOCATION RE-RESOLVE (#106, F1). Run ONCE per command right BEFORE the ACL
    // enforcement chokepoint, so a mid-session `ACL SETUSER` / `ACL DELUSER` / `ACL LOAD` reaches
    // this already-AUTHed connection on its VERY NEXT command (was fail-open until reconnect,
    // diverging from Redis which revokes live). HOT PATH: one relaxed atomic load + integer
    // compare of the registry generation against the connection's cached generation; on the no-ACL
    // path (and whenever no `ACL` admin verb has run since this connection cached its user) the
    // generations match and this returns immediately -- byte-unchanged. ONLY when the generation
    // MOVED (rare) does it take the registry lock to re-resolve the connection's user by name. A
    // `false` return means the connection's user was DELUSER'd: it is now deauthenticated, so we
    // reply NOAUTH and CLOSE it (Redis kills a deleted user's clients).
    if !ironcache_server::acl_resolve_if_stale(ctx, conn) {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::noauth()),
            conn.proto,
        );
        return true;
    }

    // -- THE HOISTED ACL ENFORCEMENT CHOKEPOINT (#106). Immediately AFTER the NOAUTH gate and
    // BEFORE any interception / cross-shard fan-out / CLUSTER-mutator / persistence / MULTI
    // queueing / local dispatch, so per-command + per-key + per-channel authorization covers
    // EVERY command path in ONE place (the same reason the NOAUTH gate is hoisted here). The
    // connection's authenticated ACL identity (`conn.acl_user`, `None` == the implicit all-
    // permissive default) was cached at AUTH time, so this check is LOCK-FREE: it reads the
    // cached `Arc<User>`, never the ACL registry.
    //
    // DEFAULT (no ACL config) is BYTE-UNCHANGED + adds at most ~two bool tests: `acl_user` is
    // `None` for every connection on the no-ACL path, so `acl_enforce` returns `None` after a
    // single match, and `ctx.acl.is_acl_active()` is one relaxed atomic load that is `false`.
    // Only an ACL-governed connection (a narrowed `Some(user)`) pays for the command/key/
    // channel checks. A DENY short-circuits with the `-NOPERM` reply, exactly like the NOAUTH
    // gate above, and never reaches routing / dispatch.
    if let Some(deny) = ironcache_server::acl_enforce(
        ctx.acl.is_acl_active(),
        conn.acl_user.as_deref(),
        &cmd_upper,
        request,
    ) {
        state_rc.borrow_mut().counters.on_command();
        encode_into(out, &ironcache_server::Value::error(deny), conn.proto);
        return false;
    }

    // -- THE `-LOADING` WRITE-QUIESCE GATE (#391 streamed live-cutover, Decision 2 Option C).
    // While THIS shard is quiescing for the final delta cut, reject every client MUTATOR with
    // `-LOADING` HERE -- BEFORE routing, MULTI queueing, cross-shard hop, or local dispatch, and so
    // BEFORE the store's write funnel assigns the write a ring offset. That is what makes "a client
    // write is acked only if its offset <= E" structural: the write never reaches the ring, so it
    // can never land above the latched cut offset. Reads (and admin like PING/INFO) flow straight
    // through. The write classifier is the SAME [`ironcache_server::request_is_write_for_pause`] the
    // CLIENT PAUSE gate uses, so the MULTI/EXEC convention matches exactly: a command merely being
    // QUEUED inside a MULTI is not a write here (it is held at its EXEC), and an EXEC whose staged
    // batch contains any write IS a write (so the whole transaction is rejected, never partially
    // applied above E). DEFAULT (not quiescing) is BYTE-UNCHANGED and near-free: `is_shard_loading`
    // is a single core-local `Cell<bool>` load that short-circuits the `&&`, so the classifier never
    // runs and this is one predictable-not-taken branch. This is DELIBERATELY not CLIENT PAUSE: a
    // paused write applies AFTER the window (at an offset > E, lost by the cut), whereas this REJECTS
    // it so the client retries against whichever process ends up authoritative.
    if is_shard_loading()
        && ironcache_server::request_is_write_for_pause(&cmd_upper, conn.in_multi, &conn.queued)
    {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::loading()),
            conn.proto,
        );
        return false;
    }

    // HA-6: consume the one-shot ASKING EXACTLY ONCE PER COMMAND, BEFORE any early return
    // (pubsub interception / in_multi / WATCH cluster-redirect / WATCH cross-shard / the internal-
    // verb gate), so a set flag can NEVER leak into a later command. Previously the flag was
    // captured + cleared only inside the `ctx.cluster` block below, but the early returns above it
    // left `conn.asking` still true: `ASKING` then `SUBSCRIBE ch` (pubsub early return) then `GET
    // <key in an IMPORTING slot>` would see asking == true and serve LOCALLY on a node that does
    // NOT own the slot (a following `SET` there writes an orphaned key -> divergence / lost write
    // on a migration abort). `ASKING` itself (the command, handled below) must NOT clear the flag
    // it is about to set, so it is excluded here. Capturing for every OTHER command -- including
    // the early-returning ones -- means the flag is consumed once and the leak is closed; the
    // captured `asking` local is read by the migration redirect in the cluster block. A non-cluster
    // / non-migrating connection never sets `asking`, so this is a single bool read+write on the
    // cold path and the default static path is byte-unchanged.
    let asking = consume_one_shot_asking(&cmd_upper, conn);

    // HA-6 ASKING-IN-MULTI: carry the PRE-MULTI one-shot ASKING into the transaction it opens. The
    // one-shot is consumed PER COMMAND above (so it cannot LEAK past a command), which would
    // otherwise drop a flag set by `ASKING` BEFORE `MULTI` before the transaction's commands are
    // QUEUED. Redis keeps the single `CLIENT_ASKING` flag live across the MULTI queueing phase (its
    // cluster redirect runs at QUEUE time), so `ASKING; MULTI; <cmd on an IMPORTING slot>; EXEC`
    // queues + serves on the importing destination. We mirror that by recording the consumed
    // `asking` into the transaction-scoped `conn.txn_asking` for the MULTI that OPENS a transaction
    // (a nested `MULTI` is `in_multi` already and routes through `route_in_multi`, so it never
    // reaches here). The queue-time redirect in `route_in_multi` consults `txn_asking`; `clear_txn`
    // / `reset` clear it on EXEC / DISCARD / RESET, so it can NEVER leak past the transaction. On a
    // non-cluster / non-migrating connection `asking` is always false, so this is a single cold
    // bool write and the default path is byte-unchanged.
    if cmd_upper == b"MULTI" && !conn.in_multi {
        conn.txn_asking = asking;
    }

    // -- INTERNAL-VERB CLIENT GATE (COORDINATOR.md #107, Stage 2b). `__ICSTORESET` /
    // `__ICSTOREZSET` / `__ICSTOREHLL` are the coordinator's INTERNAL cross-shard *STORE
    // dest-write verbs (set / zset / PFMERGE-HLL): each lives in the command registry + has a
    // real dispatch arm (so it routes / admits like any keyed write and the registry-vs-dispatch
    // cross-check stays exact) but must be UNREACHABLE from clients -- only the coordinator
    // issues them (via `dispatch_one_value` / `run_local_keyed`, which call
    // `dispatch_remote_keyed` DIRECTLY and never pass through this router). A CLIENT socket only
    // ever reaches dispatch THROUGH this router, so rejecting them here -- before any routing or
    // queueing -- makes a client `__ICSTORE*` (in or out of MULTI) get the standard
    // unknown-command error while the coordinator's internal path is untouched.
    if cmd_upper == ironcache_server::ICSTORESET
        || cmd_upper == ironcache_server::ICSTOREZSET
        || cmd_upper == ironcache_server::ICSTOREHLL
        // `__ICPUBLISH` is the INTERNAL cross-shard PUBLISH fan-out verb (SERVER_PUSH.md #20, PR
        // 91a): in the registry so the cross-check stays exact, but client-unreachable -- only
        // the coordinator issues it (via the inbox). Reject a CLIENT `__ICPUBLISH` here with the
        // same unknown-command reply as the *STORE verbs.
        || cmd_upper == ironcache_server::ICPUBLISH
        // `__ICSPUBLISH` is the INTERNAL cross-shard SHARDED-PUBLISH fan-out verb (#410): the same
        // gate as `__ICPUBLISH` -- registry-present (cross-check exact) but client-unreachable; only
        // the coordinator issues it.
        || cmd_upper == ironcache_server::ICSPUBLISH
        // `__ICPUBSUB` is the INTERNAL cross-shard PUBSUB-introspection gather verb (SERVER_PUSH.md
        // #20, PR 91b): the same gate -- registry-present (cross-check exact) but client-
        // unreachable; only the coordinator issues it (via the inbox per shard).
        || cmd_upper == ironcache_server::ICPUBSUB
        // `__ICEXISTS` is the INTERNAL cross-shard KEY-PRESENCE query (HA-6 multi-shard migration,
        // COORDINATOR.md #107): the same gate -- client-unreachable; only the coordinator issues it
        // (via `coordinator::presence_via` to the key's owner shard). It is NOT in the `spec_of`
        // registry (it is dispatched directly, never classified), so a client sending it would
        // already fall to the unknown-command home arm; rejecting it HERE keeps the contract
        // explicit and uniform with the other internal verbs.
        || cmd_upper == ironcache_server::ICEXISTS
        // `__ICSAVE` is the INTERNAL cross-shard SAVE fan-out verb (#58 persistence): the same gate
        // -- client-unreachable; only the home core issues it (via `do_save_all`'s `fan_out_save`
        // to each shard's drain loop, which dumps that shard's partition, yielding between chunks).
        // Like `__ICEXISTS` it is
        // NOT in the `spec_of` registry (dispatched directly by the coordinator), so a client
        // sending it would already get unknown-command; rejecting it HERE keeps the contract uniform.
        || cmd_upper == crate::persist::ICSAVE
        // `__ICCOUNTKEYSINSLOT` / `__ICGETKEYSINSLOT` are the INTERNAL #371 slot-scan whole-keyspace
        // verbs the serve loop rewrites a cluster-mode `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` into;
        // a client must never reach them directly. Like `__ICEXISTS`/`__ICSAVE` they are not in
        // `spec_of` (a client sending one already gets unknown-command via the home arm), but gating
        // them here keeps the contract explicit and uniform.
        || cmd_upper == ironcache_server::ICCOUNTKEYSINSLOT
        || cmd_upper == ironcache_server::ICGETKEYSINSLOT
    {
        // FIX F: when a client issues an internal verb INSIDE a MULTI, dirty the transaction in
        // addition to replying the unknown-command error, so EXEC returns -EXECABORT exactly as
        // a genuine unknown command would (the queue gate dirties an unknown command at queue
        // time; this router intercepts the internal verb BEFORE that gate, so it must dirty here).
        reject_internal_verb(conn, state_rc, request, out);
        if conn.in_multi {
            conn.dirty_exec = true;
        }
        return false;
    }

    // -- MONITOR HONESTY INTERCEPTION (#527). MONITOR (stream every executed command to the
    // subscribed client) is NOT implemented: a correct implementation needs a fan-out from the
    // command choke point to a set of monitor connections, which this build does not have. Rather
    // than let it fall through to the generic `unknown command` reply (which would suggest it is
    // merely unrecognized) OR silently mis-behave, reply a CLEAR, honest `-ERR MONITOR is not
    // supported`. It is intentionally NOT registered in the command spec, so `COMMAND` does not
    // advertise it (we do not claim a capability we lack -- the same honesty that removed the
    // MONITOR mention from the README secret-hygiene note). Gated `!conn.in_multi` like the other
    // serve-layer rejects: inside a MULTI it is an unregistered token, so the queue gate rejects it
    // with the standard unknown-command error and dirties the transaction. A non-MONITOR command
    // never enters this block (one byte-compare), so the hot path is byte-unchanged.
    if !conn.in_multi && cmd_upper == b"MONITOR" {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::err(
                "MONITOR is not supported",
            )),
            conn.proto,
        );
        return false;
    }

    // -- GRACEFUL SHUTDOWN INTERCEPTION (#139, SHUTDOWN.md): SHUTDOWN [NOSAVE|SAVE]. The process
    // exit + the save-on-exit live HERE in the serve layer (it owns the runtime, the per-shard
    // stores, the data_dir, and the env Clock for the save timestamp); the generic dispatch sees
    // only the storage waist and cannot exit the process, so it MUST be intercepted before it. This
    // runs REGARDLESS of whether persistence is configured (NOSAVE / a bare SHUTDOWN with no save
    // policy exits without saving even when `persist` is `None`), so it is OUTSIDE the persistence
    // `Some` block below. Gated `!conn.in_multi` exactly like SAVE: a SHUTDOWN inside a MULTI falls
    // through to the dispatch fallback at EXEC (a documented minor divergence). On a successful stop
    // this NEVER returns (the process exits 0); on a refused save (a SAVE/policy save that fails) it
    // replies an error and does NOT exit, so the connection keeps serving. A non-SHUTDOWN command
    // never enters this block, so the hot path is byte-unchanged.
    if !conn.in_multi && cmd_upper == b"SHUTDOWN" {
        handle_shutdown_command(persist, ctx, conn, home, inbox, request, out).await;
        return false;
    }

    // -- ACL COMMAND INTERCEPTION (#106). The `ACL` admin family (WHOAMI/LIST/USERS/GETUSER/
    // SETUSER/DELUSER/CAT/GENPASS/SAVE/LOAD) is handled HERE in the serve layer (like CONFIG /
    // persistence) because it mutates the shared `ctx.acl` registry and SAVE/LOAD do aclfile
    // I/O the server crate (no std::fs by policy on the data path) does not own. It is gated
    // `!conn.in_multi` exactly like SAVE/SHUTDOWN: an ACL inside a MULTI falls through to the
    // generic dispatch (which has no ACL arm -> the standard unknown-command path), a tolerable
    // minor divergence. The per-command ACL ENFORCEMENT above already ran, so a user without
    // `+acl` cannot reach this; `default` (and any `+acl` user) can. A non-ACL command never
    // enters this block, so the hot path is byte-unchanged.
    if !conn.in_multi && cmd_upper == b"ACL" {
        handle_acl_command(ctx, conn, env, request, out);
        return false;
    }

    // -- PERSISTENCE INTERCEPTION (#58): SAVE / BGSAVE / LASTSAVE. When persistence is ENABLED (a
    // data_dir is configured -> `persist.is_some()`) and the command is NOT inside a MULTI (a SAVE
    // in MULTI is rare; it falls through to the persistence-disabled dispatch fallback inside EXEC,
    // a documented minor divergence), this router runs the REAL cross-shard save / reports the real
    // LASTSAVE -- the generic dispatch sees only the storage waist, not the concrete stores to dump.
    // With persistence OFF (`None`) this whole block is skipped and the commands fall through to the
    // dispatch persistence-disabled fallback, so the default posture is byte-unchanged.
    if let Some(persist) = persist {
        if !conn.in_multi && matches!(cmd_upper.as_slice(), b"SAVE" | b"BGSAVE" | b"LASTSAVE") {
            handle_persist_command(persist, ctx, conn, home, inbox, &cmd_upper, request, out).await;
            return false;
        }
        // DIRTY-WRITE COUNTER (#58 save policy): bump the node-level dirty counter for a write
        // command so the periodic save policy can decide whether enough changed since the last save.
        // This is a SINGLE RELAXED ATOMIC increment, gated on persistence being ENABLED (so the
        // default persistence-off path never touches it) AND on the command being a write
        // (`is_write`, the registry flag; a read / admin command never bumps it). It is in the SERVE
        // layer, NOT the store hot path, so the store primitives are byte-unchanged. It is
        // intentionally approximate (a write that later errors still bumped it), exactly like Redis's
        // `server.dirty` heuristic that drives its own `save` points.
        if ironcache_server::is_write(&cmd_upper) {
            persist.note_write();
        }
    }

    // -- IN-MULTI PUB/SUB REJECT (SERVER_PUSH.md #20, FIX C). The pub/sub commands are handled in
    // THIS serve layer (`try_handle_pubsub`), NOT in `dispatch_inner`, so EXEC -- which replays
    // the queued batch through `dispatch_inner` -- cannot run them. Rather than execute them
    // EAGERLY inside MULTI (silently wrong + out of transaction order, the bug the interception
    // order caused) or queue-then-fail-at-EXEC, REJECT them loudly at queue time and dirty the
    // transaction (so EXEC returns -EXECABORT and applies nothing): the same "correct, or
    // explicitly aborted, never silently wrong" contract as the cross-shard in-MULTI guards.
    //
    // DOCUMENTED DIVERGENCE from current Redis: Redis QUEUES the pub/sub commands inside MULTI
    // and runs them at EXEC (they do NOT carry CMD_NO_MULTI; verified against redis/redis
    // src/commands/*.json). Serve-layer EXEC replay of pub/sub is the tracked follow-up that
    // removes this divergence; until then we reject (never silently mis-execute). The reject runs
    // BEFORE `try_handle_pubsub` so the command is neither executed nor queued.
    if conn.in_multi && is_serve_pubsub_command(&cmd_upper) {
        state_rc.borrow_mut().counters.on_command();
        conn.dirty_exec = true;
        let name = String::from_utf8_lossy(&cmd_upper).into_owned();
        encode_into(
            out,
            &ironcache_server::Value::error(
                ironcache_protocol::ErrorReply::not_allowed_in_transactions(&name),
            ),
            conn.proto,
        );
        return false;
    }

    // -- SUBSCRIBE-MODE GATE (SERVER_PUSH.md #20, FIX B). MOVED to run BEFORE the pub/sub
    // interception: a RESP2 subscriber may run ONLY the (P)SUBSCRIBE / (P)UNSUBSCRIBE control set
    // + PING/QUIT/RESET; PUBLISH and PUBSUB are NOT allowed. The previous order intercepted
    // PUBLISH/PUBSUB before this gate, so a RESP2 subscriber wrongly executed them. The gate's
    // allowlist still passes the subscribe-family + PING/QUIT/RESET through to interception (so
    // SUBSCRIBE while subscribed, the subscribed PING array, etc. still work); only PUBLISH/PUBSUB
    // (and any other non-pub/sub command) get the subscribe-mode error. RESP3 has NO restriction.
    // The check + reply live in `subscriber_gate_blocks` (kept out of this router so it stays
    // small); it returns true (and has written the error) when the command is blocked. See that
    // helper for WHY the gate is ALSO in `dispatch` (a remote keyed hop bypasses the dispatch gate).
    if subscriber_gate_blocks(conn, state_rc, &cmd_upper, out) {
        return false;
    }

    // -- RESET INTERCEPTION (SERVER_PUSH.md #20, FIX A). RESET goes through the home dispatch path
    // (`dispatch_inner`'s RESET arm), which clears `conn.sub_channels` / `sub_patterns` but CANNOT
    // reach the per-shard subscription table (the push senders live in this serve layer). Without
    // this interception a post-RESET connection would still appear subscribed in the shard table:
    // a PUBLISH would still count + deliver to it (a GHOST), and PUBSUB CHANNELS would still list
    // it. So when RESET arrives on a subscriber, we FIRST deregister all its subscriptions from
    // the table (driven off the PRE-reset conn sub sets), THEN replace the per-connection push
    // channel (drop the old sender/receiver + shed flag, install a fresh trio) so a post-RESET
    // SUBSCRIBE re-registers cleanly with a live channel, and only THEN let dispatch run RESET
    // (which clears the conn sub sets + the rest of the reset). A RESET on a non-subscriber skips
    // straight to dispatch (the deregister is a no-op), so the non-subscriber path is unchanged.
    if cmd_upper == b"RESET" && conn.is_subscriber() {
        deregister_all_subscriptions(conn);
        // Swap in a fresh push channel + shed flag: the old `push_tx`/`push_rx`/`shed_flag` are
        // dropped, so any in-flight ghost sender the publisher still holds is closed, and a fresh
        // SUBSCRIBE after RESET registers the NEW sender. The serve loop owns these by &mut, so
        // the swap is visible to the idle wait on the next iteration.
        let (new_tx, new_rx) = tokio::sync::mpsc::channel::<crate::pubsub::ServerPush>(
            crate::pubsub::PUSH_CHANNEL_BOUND,
        );
        *push_tx = new_tx;
        *push_rx = new_rx;
        *shed_flag = std::sync::Arc::new(crate::pubsub::ShedSignal::default());
        // Fall through to dispatch so the RESET arm clears the conn sub sets + the rest of reset
        // and replies "+RESET".
    }

    // -- PUB/SUB SERVE-LAYER INTERCEPTION (SERVER_PUSH.md #20, PR 91a). SUBSCRIBE / UNSUBSCRIBE /
    // PUBLISH (and PING-while-subscribed under RESP2) are handled HERE because registration needs
    // the per-connection push sender + the per-shard subscription table that live in this serve
    // layer (the server crate has no tokio dep). By the time we reach here the in-MULTI reject and
    // the RESP2 subscribe-mode gate have already run (FIX C / FIX B), so a pub/sub command that
    // arrives here is NOT in MULTI and (if a RESP2 subscriber) is in the allowed control set. When
    // `try_handle_pubsub` handled the command it returns `Some(close)`; every other command
    // (`None`) falls through to the normal routing + dispatch. Split out so this router stays small.
    if let Some(close) = try_handle_pubsub(
        conn, home, inbox, push_tx, shed_flag, state_rc, &cmd_upper, request, out,
    )
    .await
    {
        return close;
    }

    // -- BLOCKING-COMMAND SERVE-LAYER INTERCEPTION (PROD-9). BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/
    // BZPOPMIN/BZPOPMAX/BZMPOP/WAIT are handled HERE (not in `dispatch_inner`) on the LIVE path
    // because PARKING needs the per-connection waker + the runtime timer seam + the connection's
    // stream (to observe a peer close while parked), which the serve loop owns. It fires ONLY when
    // NOT in a MULTI: inside a transaction a blocking command must NOT block (Redis: it QUEUES and
    // runs NON-BLOCKING at EXEC, returning nil at once if empty), so an in-MULTI blocking command
    // FALLS THROUGH to `route_in_multi` below -> the dispatch queue gate stages it (+QUEUED), and
    // EXEC replays it through its NON-BLOCKING dispatch arm. On the live path:
    //
    //   * a parse error is replied immediately (no park);
    //   * a non-blocking ATTEMPT that finds data replies it immediately (the fast path: NO park);
    //   * an attempt that finds every key empty sets `block_request` and returns -- the OWNING
    //     serve loop then runs the park loop (register a FIFO waiter, `select!` on wake/timeout/
    //     close, re-attempt on wake). WAIT sets a `block_request` too (it parks on the replica-ack
    //     quorum), with NO keys (it touches no keyspace).
    //
    // A non-blocking command never enters this block (a single `is_blocking_command` predicate),
    // so the hot path is byte-unchanged.
    if !conn.in_multi && ironcache_server::is_blocking_command(&cmd_upper) {
        let close = handle_blocking_live(
            ctx,
            conn,
            env,
            store_rc,
            state_rc,
            &cmd_upper,
            request,
            out,
            block_request,
        );
        // A blocking pop that found data on the FAST path recorded keyspace event(s) (the same
        // lpop/rpop/zpopmin emit as the non-blocking pop); drain + publish them now, AFTER the
        // reply is encoded (per-connection FIFO), through the EXISTING Pub/Sub fan-out -- exactly
        // like the normal home dispatch path. On the PARK path the store was not mutated (no pop),
        // so the drain is a no-op; the re-attempt in the serve loop's park loop publishes its own
        // events on a successful wake. The drain short-circuits on an empty buffer, so this is a
        // single thread-local `is_empty` check when notifications are off.
        publish_pending_keyspace_events(inbox, home.index);
        return close;
    }

    // -- TRANSACTION CORRECTNESS UNDER PARTITIONING (COORDINATOR.md #107, the critical fix).
    //
    // The coordinator routes each command to its key's OWNER shard. But a command issued
    // INSIDE a `MULTI` must be QUEUED (reply `+QUEUED`), not executed: routing it remotely
    // (the dispatch_via / multikey / whole-keyspace branches below) would EXECUTE it eagerly
    // and out of transaction order. The queue gate lives in `dispatch` (the server crate) on
    // the HOME path only, so the remote/fan-out branches bypass it entirely. We close that
    // hole here, BEFORE the routing decision.
    //
    // The KEY INVARIANT we establish: a transaction reaches real (home-only) EXEC ONLY when
    // ALL its watched keys AND all its queued commands' keys are HOME-OWNED, so home
    // execution is always correct. Otherwise we reject it LOUDLY (a transaction is correct,
    // or explicitly aborted -- never silently wrong). True cross-shard transactions (txid +
    // ordered apply) are Stage 3, out of scope here.
    //
    // With `shards == 1` every key is home-owned, so the guards below NEVER fire and the
    // `in_multi -> home path` branch is exactly the pre-coordinator behavior (home dispatch
    // was always the path): byte-identical, and every existing transaction test stays green.

    // (1) QUEUE GATE + (2) CROSS-SHARD-IN-MULTI / WHOLE-KEYSPACE GUARDS. Inside a transaction
    // a command must be QUEUED (or a control verb handled), NEVER routed/executed remotely, and
    // a transaction may reach real (home-only) EXEC ONLY when all its keys are home-owned. That
    // transaction-correctness logic lives in `route_in_multi` (kept out of this router so it
    // stays small); it returns the close flag when it handled the in-MULTI case.
    if conn.in_multi {
        return route_in_multi(
            ctx, conn, home, env, store_rc, wheel_rc, state_rc, &cmd_upper, route, request, out,
        );
    }

    // (3a) CLUSTER WATCH SLOT GUARD (CLUSTER_CONTRACT.md #70, slice 2). WATCH is classified
    // `AlwaysHome` (it is a connection-state verb that bypasses MULTI queueing), so the data
    // `cluster_redirect` below EXEMPTS it; but in Redis WATCH carries a key spec and goes
    // through `getNodeByQuery`, so a `WATCH <foreign-slot key>` must reply `-MOVED` (and two
    // keys spanning slots `-CROSSSLOT`), NOT snapshot locally and reply +OK (a bogus optimistic
    // lock + a parity hole). We therefore run the SAME shared `redirect_for_keys` predicate the
    // keyed-data path uses, over WATCH's keys (args[1..], read DIRECTLY because `command_keys`
    // does not extract an AlwaysHome command's keys), and only when a cluster map is configured
    // (`ctx.cluster` Some). On a redirect we short-circuit exactly like the data redirect below:
    // bump the command counter, encode the error, do NOT run WATCH, do NOT close. A WATCH whose
    // keys are all home-slot (or a malformed/arity-wrong WATCH that yields no key) returns None
    // and falls through to the cross-shard WATCH guard, then the home dispatch, unchanged. This
    // runs BEFORE the internal cross-shard WATCH guard so cluster MOVED/CROSSSLOT (the
    // client-visible, retryable redirect) takes precedence over the internal-shard error.
    if cmd_upper == b"WATCH" && request.args.len() >= 2 {
        if let Some(map) = ctx.cluster.as_deref() {
            // WATCH snapshots for a transaction (a CAS that gates a WRITE), so it must NEVER be
            // served by a replica's stale state: pass `replica_serves = false` so a WATCH of a
            // foreign slot always MOVEDs to the owner, even on a READONLY replica connection.
            // WATCH never participates in the HA-6 migration ASK/IMPORTING handshake (it is a CAS
            // gate, not a data read/write that the client retries with ASKING), so pass `None`:
            // the migration arm is skipped and WATCH redirects exactly as before HA-6.
            if let Some(reply) = redirect_for_keys(
                map,
                request.args[1..].iter().map(AsRef::as_ref),
                false,
                None,
                shard_owner_home(ctx, home),
            ) {
                state_rc.borrow_mut().counters.on_command();
                encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
                return false;
            }
        }
    }

    // (3) CROSS-SHARD WATCH GUARD (only when NOT in_multi; WATCH inside MULTI already errors
    // via dispatch's watch_inside_multi path). A `WATCH` of a key owned by a remote shard
    // would snapshot the WRONG (home) store, making the dirty-CAS meaningless. `route::classify`
    // treats WATCH as AlwaysHome and `command_keys` does not extract its keys, so we read
    // WATCH's keys (args[1..]) DIRECTLY here. If any is not home-owned, reply the cross-shard
    // WATCH error and do NOT run WATCH (no snapshot, no conn.watch mutation); the connection is
    // left un-watched so a following MULTI/EXEC works. A WATCH of only home-owned keys (or a
    // malformed/arity-wrong WATCH) falls through to the home dispatch -> cmd_watch unchanged.
    if cmd_upper == b"WATCH"
        && request.args.len() >= 2
        && request.args[1..]
            .iter()
            .any(|k| route::owner_shard(k.as_ref(), home.total) != home.index)
    {
        state_rc.borrow_mut().counters.on_command();
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::watch_cross_shard()),
            conn.proto,
        );
        return false;
    }

    // CLUSTER SLOT OWNERSHIP (CLUSTER_CONTRACT.md #70, slice 2). BEFORE any internal shard
    // routing (the multikey / spanning / single-target fan-out below): in cluster-map mode a
    // KEYED data command whose key(s) are not served by THIS node is REDIRECTED (`-MOVED`) or
    // REJECTED (`-CROSSSLOT`). `ctx.cluster` is `Some` ONLY when cluster mode is enabled AND a
    // topology is configured, so a standalone (or topology-less) node skips this entirely and
    // is byte-identical to slice 1 (Redis parity: a non-cluster node never sends MOVED). Keyless
    // / admin / whole-keyspace commands are exempt (`cluster_redirect` returns None for them).
    // The in-MULTI path does NOT reach here (it returned to `route_in_multi` above); queued
    // commands are checked at QUEUE time there, reusing this SAME predicate.
    // HA-6 ASKING: the one-shot per-connection flag. `ASKING` itself just sets the flag and replies
    // +OK (it does NOT consume it -- the NEXT command does). Handled HERE (the router) so the flag
    // is in scope for the migration redirect below; `ASKING` is `AlwaysHome`, so it otherwise falls
    // through to the home dispatch, but intercepting it here keeps the one-shot lifetime tight.
    if cmd_upper == b"ASKING" {
        state_rc.borrow_mut().counters.on_command();
        if request.args.len() == 1 {
            conn.asking = true;
            encode_into(out, &ironcache_server::Value::ok(), conn.proto);
        } else {
            encode_into(
                out,
                &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                    "asking",
                )),
                conn.proto,
            );
        }
        return false;
    }

    if let Some(map) = ctx.cluster.as_deref() {
        let in_sync = replica_read_in_sync(ctx);
        // HA-6: use the one-shot ASKING captured + cleared at the TOP of this function (before any
        // early return), so a flag set by an earlier `ASKING` can never leak past a pubsub / in_multi
        // / WATCH early return into this decision. The migration redirect is consulted only in raft
        // cluster mode (a committed MIGRATING/IMPORTING tag); a non-migrating slot makes the resolver
        // irrelevant and the decision byte-identical to pre-HA-6.
        let now = UnixMillis(env.borrow().now_unix_millis());
        let db = conn.db;

        // HA-6 MULTI-SHARD EXACT PRESENCE (COORDINATOR.md #107): the migration source's ASK
        // decision classifies each key present/absent against the shard that OWNS it (the FNV
        // `owner_shard`). On a SINGLE-shard node every key is home, so the local `contains_live`
        // is already exact -- and `xshard_presence_keys` returns `None`, so this whole block is a
        // single cheap predicate and the resolver below is BYTE-IDENTICAL to pre-fix (the hot path
        // is untouched). On a MULTI-shard node a migrating-slot key may live on a SIBLING shard;
        // there the accept-shard `contains_live` could report a present key ABSENT and emit a
        // (safe but unnecessary) extra `-ASK`. So when the command's keys are on a slot this node
        // is MIGRATING (the only case `migration_decision` consults presence) AND some key is NOT
        // home-owned, we PRE-RESOLVE that key's presence on its owner shard via the coordinator
        // (`presence_via`, the cross-shard `contains_live`), making the decision EXACT. This is a
        // COLD path (a slot actually MIGRATING + a keyed command landing on this owner) and the
        // hop is the same deadlock-free single-key mechanism Stage 1 routing uses (see
        // `presence_via`); the borrow of `store_rc` for a home key is taken + dropped INSIDE the
        // closure, never across the awaits done here.
        let xshard_presence: Vec<(Vec<u8>, bool)> =
            match xshard_presence_keys(map, route, &cmd_upper, request, home) {
                None => Vec::new(),
                Some(remote_keys) => {
                    let mut resolved = Vec::with_capacity(remote_keys.len());
                    for (key, owner) in remote_keys {
                        // Each remote key is resolved on its OWNER shard (a cross-shard hop). No
                        // `RefCell` borrow is held across this await (the only borrows in this fn
                        // are the brief `env.borrow()` above, already dropped, and the per-call
                        // closure borrow below).
                        let present = coordinator::presence_via(inbox, owner, &key, db).await;
                        resolved.push((key, present));
                    }
                    resolved
                }
            };

        // The key-presence resolver. For a HOME-owned key (always true when shards == 1) it reads
        // THIS shard's store via the pure `contains_live` -- byte-identical to before. For a key
        // PRE-RESOLVED on a sibling shard above (multi-shard migration only) it returns the EXACT
        // owner-shard answer. A key that is neither (cannot occur: `xshard_presence_keys` returns
        // EVERY non-home key of the migrating slot) falls back to the local read, the safe default.
        let key_present = |k: &[u8]| {
            if let Some(&(_, present)) = xshard_presence.iter().find(|(key, _)| key.as_slice() == k)
            {
                present
            } else {
                store_rc.borrow().contains_live(db, k, now)
            }
        };
        let mig = MigrationCtx {
            asking,
            key_present: &key_present,
        };
        if let Some(reply) = cluster_redirect(
            map,
            route,
            &cmd_upper,
            request,
            conn.readonly,
            in_sync,
            Some(&mig),
            shard_owner_home(ctx, home),
        ) {
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
            // Short-circuit WITHOUT closing the connection (same as the WATCH guard above):
            // the client keeps the connection and retries at the redirect target.
            return false;
        }
    }

    // WRITE-SIDE REPLICATION GUARDRAIL (ADR-0026, Redis `min-replicas-to-write`). After the
    // redirect above returned `None` (so this node OWNS the keyed slot, or the command is
    // keyless/admin), an owned WRITE is REJECTED with `-NOREPLICAS Not enough good replicas to
    // write.` when too few replicas are currently in sync -- so an ACKNOWLEDGED write is known to
    // be on at least `min_replicas_to_write` replicas, bounding the failover loss window.
    //
    // BYTE-UNCHANGED at the default: the FIRST gate is `min_replicas_to_write > 0`. With the
    // guardrail at its default-disabled 0 this whole block short-circuits BEFORE touching the
    // count atomic, the map, or `is_write` -- the write hot path is byte-identical to before. The
    // check applies ONLY to WRITES (`is_write`), ONLY to slots this node OWNS (the redirect already
    // sent foreign / read-replica-served slots away), and ONLY in raft-mode (the count cell is
    // `Some` only there). Reads are never blocked.
    if ctx.boot.min_replicas_to_write > 0 {
        if let Some(reply) = write_guardrail(ctx, route, &cmd_upper, request) {
            state_rc.borrow_mut().counters.on_command();
            encode_into(out, &ironcache_server::Value::error(reply), conn.proto);
            // Short-circuit WITHOUT closing the connection: the client may retry once enough
            // replicas are back in sync (the same non-closing contract as the redirect above).
            return false;
        }
    }

    // RAFT-MODE CLUSTER MUTATOR -> PROPOSAL (HA-4c). A `CLUSTER ADDSLOTS / ADDSLOTSRANGE /
    // SETSLOT / MEET / FORGET / SET-CONFIG-EPOCH` is normally handled SYNCHRONOUSLY by
    // `cmd_cluster` (the slice-3 direct local mutation). In raft-governance mode the slot map is
    // owned by the committed log, so the mutator becomes a PROPOSAL: build a `ConfigCmd`, await
    // its commit through the leader, and reply `+OK` (committed) or `-CLUSTERDOWN` (this node is
    // not the leader). This branch fires ONLY when `cluster_mode == Raft` AND a handle is present
    // (`ctx.raft.is_some()`), so the DEFAULT static path never reaches it and is byte-unchanged.
    //
    // It is intercepted HERE (the async router), NOT in `cmd_cluster` (which is sync and cannot
    // await a commit): `CLUSTER` is `AlwaysHome`, so none of the keyed routing below applies to
    // it, and the await parks on the proposal's one-shot ack (the single control-plane task
    // fulfills it) WITHOUT blocking the shard executor. The introspection subcommands (SLOTS /
    // SHARDS / NODES / INFO / MYID / ...) are NOT mutators, so they fall through to the unchanged
    // home dispatch, which reads the committed `ctx.cluster` map.
    if cmd_upper == b"CLUSTER"
        && ctx.cluster_mode() == ironcache_config::ClusterMode::Raft
        && ctx.raft.is_some()
    {
        if let Some(close) = try_raft_cluster_mutator(ctx, conn, state_rc, request, out).await {
            return close;
        }
        // Not a mutator (an introspection subcommand or a malformed CLUSTER): fall through to the
        // unchanged home dispatch.
    }

    // A SHARD-SPANNING KeyedMulti command (its keys land on >1 shard, so `owner_shard_set`
    // is None) that is one of the SIX fan-out-supported commands routes to the multi-key
    // SCATTER-GATHER (COORDINATOR.md #107, Stage 2a). Co-located KeyedMulti (Some(shard))
    // routes via Stage 1 below; any OTHER spanning multi-key command stays on the home sync
    // fall-through (the documented Stage 2b/2c gap), unchanged. We compute this BEFORE the
    // single-target `target` so the two are mutually exclusive (a spanning command has no
    // single owner, so `target` would be None anyway).
    let multikey_fan_out =
        matches!(route, route::CommandClass::KeyedMulti) && is_fan_out_multikey(&cmd_upper) && {
            let spec = route::command_keys(&cmd_upper, request);
            // None from owner_shard_set means EITHER a malformed/short request (keep home,
            // the handler emits the proper error) OR a genuine spanning command. We only
            // fan out when the spec actually has MULTIPLE keys spanning shards; a malformed
            // command (KeySpec::None) must stay home. `command_keys` returns None/One for
            // the degenerate cases, so require Many AND a None owner set (truly spanning).
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // A SHARD-SPANNING gather-combine command -- set algebra (SINTER/SUNION/SDIFF/SINTERCARD
    // + the three *STORE), zset algebra (ZUNION/ZINTER/ZDIFF/ZINTERCARD + the three *STORE +
    // ZRANGESTORE), BITOP, or HyperLogLog (PFCOUNT/PFMERGE) -- routes to the GATHER + (shared)
    // COMBINE + STORE path (COORDINATOR.md #107, Stage 2b-1 + 2b-2 + 2b-3). The gate is the
    // SAME shape as `multikey_fan_out`: KeyedMulti, one of the supported tokens, and the keys
    // genuinely SPAN shards (`Many` AND a `None` owner set). Co-located invocations route via
    // Stage 1 below; a malformed/short request stays home (the handler emits the proper
    // error). The two predicates are mutually exclusive (their command sets are disjoint). The
    // remaining spanning multi-key commands (RENAME/COPY/MOVE/SMOVE/LMOVE/RPOPLPUSH moves)
    // stay on the home fall-through (deferred).
    let spanning_set_fan_out = matches!(route, route::CommandClass::KeyedMulti)
        && is_fan_out_spanning_combine(&cmd_upper)
        && {
            let spec = route::command_keys(&cmd_upper, request);
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // A SHARD-SPANNING element-MOVE command -- SMOVE (set member), LMOVE / RPOPLPUSH (list
    // element) -- whose two keys span shards routes to the ATOMIC cross-shard apply
    // (COORDINATOR.md #107, the PROD-9 cross-shard atomicity slice): the spanning_move module
    // gathers + validates the source (read-only), then COMMITS the dst write + the src
    // mutation in a deadlock-free deterministic order, ending the prior SILENT home-subset
    // partial-apply. Co-located invocations route via Stage 1 below (the single-shard
    // handler); a malformed/short request stays home (the handler emits the proper error). The
    // gate shape mirrors `multikey_fan_out` / `spanning_set_fan_out` (Many AND a None owner
    // set = truly spanning); the command sets are disjoint, so the branches are exclusive.
    let spanning_move_fan_out = matches!(route, route::CommandClass::KeyedMulti)
        && is_fan_out_spanning_move(&cmd_upper)
        && {
            let spec = route::command_keys(&cmd_upper, request);
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // A SHARD-SPANNING all-or-nothing MSETNX (COORDINATOR.md #107): EXISTS-scan every key on
    // its owner FIRST, then (iff none exist) fan a per-owner MSET out -- replacing the prior
    // home-subset existence check + home-subset write (which set ONLY the home keys and
    // MISREPORTED its 1/0). Co-located MSETNX routes via Stage 1; a malformed request stays
    // home. MSETNX is NOT in `is_fan_out_multikey` (the Stage 2a fan-out deliberately deferred
    // it), so this is its dedicated spanning gate.
    let spanning_msetnx = cmd_upper == b"MSETNX" && {
        let spec = route::command_keys(&cmd_upper, request);
        matches!(spec, route::KeySpec::Many(_))
            && route::owner_shard_set(&spec, home.total).is_none()
    };

    // A SHARD-SPANNING multi-key command this slice cannot apply atomically
    // (RENAME/RENAMENX/COPY/LMPOP/ZMPOP/SORT...STORE) is REJECTED LOUDLY (a clear error naming
    // the hash-tag remedy) rather than falling through to the home shard and SILENTLY
    // operating on only the home subset (COORDINATOR.md #107). The gate is the same
    // truly-spanning shape; co-located invocations (incl. a SORT without STORE -- one key)
    // route via Stage 1 / the home path, unchanged.
    let spanning_move_reject = matches!(route, route::CommandClass::KeyedMulti)
        && is_spanning_move_reject(&cmd_upper)
        && {
            let spec = route::command_keys(&cmd_upper, request);
            matches!(spec, route::KeySpec::Many(_))
                && route::owner_shard_set(&spec, home.total).is_none()
        };

    // CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT in cluster mode (#371): a slot's keys span EVERY
    // shard (the client CRC16 slot vs the FNV owner-shard are independent), so an honest count /
    // key list must aggregate cross-shard. This fires ONLY for a fully-valid slot-scan AND only
    // when cluster mode is on; a malformed one (or standalone) falls to the home `CLUSTER` path,
    // which returns the exact error (or `-ERR cluster support disabled`). The `args[1]` peek runs
    // only for the CLUSTER command, never on the GET/SET hot path.
    let cluster_slot_scan: Option<ironcache_server::SlotScan> = (cmd_upper == b"CLUSTER"
        && ctx.info.cluster_enabled)
        .then(|| ironcache_server::parse_slot_scan(request))
        .flatten();

    // The routing TARGET shard, if a KEYED command routes to exactly one NON-home shard
    // (else `None` -> the home path). The single-key case keeps the zero-alloc fast path
    // (one hash + compare); only the genuinely multi-key commands pay the `command_keys`
    // walk. WholeKeyspace is NOT a single-target hop (it fans out in its own branch).
    let target = match route {
        route::CommandClass::KeyedSingle => route::single_key(request).and_then(|key| {
            let owner = route::owner_shard(key, home.total);
            (owner != home.index).then_some(owner)
        }),
        route::CommandClass::KeyedMulti => {
            let spec = route::command_keys(&cmd_upper, request);
            route::owner_shard_set(&spec, home.total).filter(|&owner| owner != home.index)
        }
        route::CommandClass::AlwaysHome | route::CommandClass::WholeKeyspace => None,
    };

    let close = if matches!(route, route::CommandClass::WholeKeyspace) {
        // WHOLE-KEYSPACE dispatch. In Static/Raft the keyspace is ONE logical whole, so a
        // whole-keyspace command SCATTER-GATHERS across EVERY shard's partition (SCAN walks one
        // shard per call via the composite cursor; the rest broadcast + merge on the home core).
        //
        // In shard-owners mode (#526) the node advertises its N internal shards as N cluster
        // nodes (one per port) and each shard's store holds EXACTLY its slot range (#520). So a
        // whole-keyspace command issued to shard i's port must answer for shard i ONLY -- the
        // per-node Redis Cluster view -- NOT the global fan-out (which would make a per-node
        // aggregator over-count DBSIZE by N and return N copies from SCAN). Serve HOME-ONLY: the
        // connecting shard's local partial IS the whole per-node answer. Both paths run the SAME
        // per-shard partial; they differ only in whether it is fanned out or served alone.
        //
        // These were never on the single-key hot path, so awaiting here is fine.
        state_rc.borrow_mut().counters.on_command();
        let home_only = ctx.cluster_mode() == ironcache_config::ClusterMode::ShardOwners;
        if cmd_upper == b"SCAN" {
            // SCAN pins to the home shard when `home_only` (start there, finish when it is
            // exhausted rather than advancing to a sibling); else it walks all shards.
            crate::whole_keyspace::scan_cross_shard(
                inbox, ctx, request, conn.db, home.index, out, conn.proto, home_only,
            )
            .await;
        } else if home_only {
            // HOME-ONLY (no fan-out, no cross-shard RNG shard-pick): DBSIZE / KEYS / RANDOMKEY /
            // FLUSHDB / FLUSHALL served from the connecting shard's local partition alone.
            // RANDOMKEY draws from THIS shard's own Env RNG seam inside the partial (ADR-0003);
            // FLUSHDB / FLUSHALL clear only this shard's slice (each cluster node flushes its
            // own slots).
            crate::whole_keyspace::run_home_only(ctx, request, conn.db, out, conn.proto);
        } else {
            // RANDOMKEY draws its shard-pick from the home Env RNG seam ONCE (ADR-0003);
            // the other whole-keyspace merges (DBSIZE / KEYS / FLUSHDB / FLUSHALL) ignore
            // it. Gate the draw to RANDOMKEY (FIX 3): drawing unconditionally (for a bare
            // arity-1 DBSIZE / FLUSHALL / FLUSHDB) would PERTURB the per-shard SplitMix64
            // stream that RANDOMKEY / SPOP / *-random eviction read from, breaking ADR-0003
            // replay AND the shards == 1 byte-identical parity (the home path draws 0 for
            // these). Non-RANDOMKEY -> 0, no draw.
            let pick = if cmd_upper == b"RANDOMKEY" {
                crate::whole_keyspace::randomkey_pick(request)
            } else {
                0
            };
            crate::whole_keyspace::fan_out_and_merge(
                inbox, ctx, &cmd_upper, request, conn.db, home.index, pick, out, conn.proto,
            )
            .await;
        }
        false
    } else if multikey_fan_out {
        // SHARD-SPANNING multi-key SCATTER-GATHER (COORDINATOR.md #107, Stage 2a): one of
        // the six (MGET/MSET/DEL/EXISTS/UNLINK/TOUCH) whose keys span shards. The multikey
        // module groups the keys by owner, runs a per-shard sub-request (the home shard's
        // subset LOCALLY + sync, the rest via their drain loops), and reassembles the reply.
        // Bump commands_processed here (matching the home / remote / whole-keyspace paths);
        // the owning shards fold their own data counters.
        state_rc.borrow_mut().counters.on_command();
        crate::multikey::fan_out_multikey(
            inbox, ctx, &cmd_upper, request, conn.db, home, out, conn.proto,
        )
        .await;
        false
    } else if spanning_set_fan_out {
        // SHARD-SPANNING gather-combine (COORDINATOR.md #107, Stage 2b-1/2b-2/2b-3): set /
        // zset algebra, BITOP, or HyperLogLog (PFCOUNT/PFMERGE) whose keys span shards. The
        // spanning_combine module gathers each source from its owner (the home subset LOCALLY
        // + sync, the rest via their drain loops), combines with the PURE combiner shared with
        // the single-shard handler, and for the write forms writes the result to the dest
        // owner. Bump commands_processed here (matching the home / remote / whole-keyspace /
        // multikey paths); the owning shards fold their own data counters. The per-command
        // dispatch is split out so this router stays small.
        state_rc.borrow_mut().counters.on_command();
        dispatch_spanning_combine(ctx, conn, home, inbox, &cmd_upper, request, out).await;
        false
    } else if spanning_move_fan_out {
        // SHARD-SPANNING element MOVE (COORDINATOR.md #107, the PROD-9 cross-shard atomicity
        // slice): SMOVE / LMOVE / RPOPLPUSH whose two keys span shards. The spanning_move
        // module gathers + validates the source (read-only on its owner), then COMMITS the dst
        // write + the src mutation across the owner shards in a deadlock-free deterministic
        // order -- ENDING the prior SILENT home-subset partial-apply. Bump commands_processed
        // here (matching the home / remote / whole-keyspace / multikey / spanning-combine
        // paths); the owning shards fold their own data counters.
        state_rc.borrow_mut().counters.on_command();
        crate::spanning_move::fan_out_spanning_move(
            inbox, ctx, &cmd_upper, request, conn.db, home, out, conn.proto,
        )
        .await;
        false
    } else if spanning_msetnx {
        // SHARD-SPANNING all-or-nothing MSETNX (COORDINATOR.md #107): EXISTS-scan every key on
        // its owner FIRST, then (iff none exist) fan a per-owner MSET out. Replaces the prior
        // home-subset existence check + home-subset write (a SILENT partial that set only the
        // home keys and misreported 1/0). Bump commands_processed here; the owning shards fold
        // their own data counters.
        state_rc.borrow_mut().counters.on_command();
        crate::spanning_move::fan_out_spanning_msetnx(
            inbox, ctx, request, conn.db, home, out, conn.proto,
        )
        .await;
        false
    } else if spanning_move_reject {
        // SHARD-SPANNING RENAME/RENAMENX/COPY/LMPOP/ZMPOP/SORT...STORE: REJECT LOUDLY (a clear
        // error naming the hash-tag remedy) rather than fall through to the home shard and
        // SILENTLY operate on only the home subset (the cardinal safety bug). These need a
        // value-object cross-shard transfer / multi-key pop the engine does not expose yet;
        // the reject is the "correct, or explicitly aborted, never silently wrong" contract.
        reject_spanning_move(conn, state_rc, &cmd_upper, out);
        false
    } else if let Some(scan) = cluster_slot_scan {
        // CLUSTER COUNTKEYSINSLOT/GETKEYSINSLOT CROSS-SHARD FAN-OUT (#371): rewrite the validated
        // slot-scan into its internal whole-keyspace verb and broadcast + merge across EVERY shard,
        // exactly like DBSIZE (sum) / KEYS (concat). The home shard's partial runs locally + sync;
        // the rest via their drain loops. Attribute commands_processed like the other fan-out paths
        // (the per-shard slot-scan partials fold no data counters). `pick = 0`: only RANDOMKEY draws
        // from the Env RNG seam, so this never perturbs the per-shard SplitMix64 stream (ADR-0003).
        state_rc.borrow_mut().counters.on_command();
        let (verb, internal): (&'static [u8], Request) = match scan {
            ironcache_server::SlotScan::Count { slot } => (
                ironcache_server::ICCOUNTKEYSINSLOT,
                Request {
                    args: vec![
                        bytes::Bytes::from_static(ironcache_server::ICCOUNTKEYSINSLOT),
                        bytes::Bytes::from(slot.to_string()),
                    ],
                },
            ),
            ironcache_server::SlotScan::Get { slot, count } => (
                ironcache_server::ICGETKEYSINSLOT,
                Request {
                    args: vec![
                        bytes::Bytes::from_static(ironcache_server::ICGETKEYSINSLOT),
                        bytes::Bytes::from(slot.to_string()),
                        bytes::Bytes::from(count.to_string()),
                    ],
                },
            ),
        };
        crate::whole_keyspace::fan_out_and_merge(
            inbox, ctx, verb, &internal, conn.db, home.index, 0, out, conn.proto,
        )
        .await;
        false
    } else if let Some(target) = target {
        // REMOTE keyed hop: enqueue to the owning shard, encode its reply here. The owning shard
        // folded the data counters; here we only attribute commands_processed.
        // KEYSPACE NOTIFICATIONS (PROD-8): the MUTATION runs on the OWNER shard, so it records its
        // keyspace events into the OWNER shard's pending buffer; that shard's drain loop drains +
        // publishes them (see `run_remote`). The home path here records nothing for a remote write.
        state_rc.borrow_mut().counters.on_command();
        // COORDINATOR HOP OBSERVABILITY (#556, the #517 zero-hop measurement harness): THIS shard is
        // about to DISPATCH a single-target cross-shard keyed hop to `target` -- the hop it PAYS.
        // Count it here (covering BOTH the deferred #8-overlap enqueue and the fused `dispatch_via`),
        // ONE relaxed atomic on this already-taken remote branch. hop-rate = hops_sent / (hops_sent +
        // local_served); in shard-owners mode a client dialing owner ports never reaches this branch,
        // so `hops_sent` trends to ~0 -- the #517 property, now MEASURABLE instead of merely claimed.
        state_rc.borrow().counters.on_hop_sent();
        if defer_hops {
            // #8 OVERLAP + #674 COALESCING: RECORD the owning shard; do NOT send yet. The serve loop
            // parks this as a `DeferredHop` and `drain_deferred_hops` groups the whole run's hops per
            // shard, sending ONE coalesced `ShardWork::Batch` per shard with >= 2 hops (a `Single` for
            // a lone hop) and demuxing the replies in wire order. `out` is left UNTOUCHED (the reply is
            // encoded later, in order, at drain). Deferring the send to drain is what enables the
            // coalescing; the decode-overlap it trades away is negligible (decode is not the cost).
            *deferred_hop = coordinator::HopOutcome::Deferred(target);
            // No home post-processing for a deferred remote hop: the wake/keyspace-publish run on the
            // OWNER shard (via run_remote), and the home probes are no-ops for a remote key. Return
            // early so we do NOT run the shared post-dispatch (wake/publish) against `out`.
            return false;
        }
        coordinator::dispatch_via(inbox, target, request, conn.db, out, conn.proto).await;
        false
    } else {
        // HOME path: the SYNC fast path (zero await/channel). Covers the home-owned keyed
        // commands, AlwaysHome, and the key-SPANNING multi-key commands (Stage 2 gap).
        // COORDINATOR HOP OBSERVABILITY (#556, the #517 zero-hop measurement harness): a KEYED
        // request whose owner IS the home shard is served here with NO hop -- the ZERO-hop path, the
        // complement of `hops_sent` (so hop-rate = hops_sent / (hops_sent + local_served)). Count it
        // ONLY for the keyed classes (AlwaysHome control/conn commands are not keyed requests and
        // must not dilute the ratio); a co-located KeyedMulti reaches here too (the shard-spanning
        // forms took their fan-out branches above). ONE relaxed atomic on this existing home branch.
        if matches!(
            route,
            route::CommandClass::KeyedSingle | route::CommandClass::KeyedMulti
        ) {
            state_rc.borrow().counters.on_local_served();
        }
        // Pass the ALREADY-uppercased command (FIX 5): we computed `cmd_upper` above for
        // routing, so the home dispatch reuses it instead of re-uppercasing + re-allocating.
        //
        // #531: an INFO whose reply includes the `# Keyspace` section reports the NODE-WIDE per-db
        // key counts on a multi-shard node -- consistent with DBSIZE. Gather them FIRST via the SAME
        // whole-keyspace scatter-gather DBSIZE uses, then hand the summed lines to the sync INFO
        // render. This runs ONLY for INFO (a cold, rare command) on a >1-shard node; a single-shard
        // node (the serving shard IS the whole keyspace) passes `None`, so its local `db_len` render
        // stays byte-identical. Both serve loops (tokio + io_uring) route through here, so the fix
        // covers both. AlwaysHome, so INFO reaches this home branch; the await here is off the data
        // hot path.
        let node_keyspace: Option<Vec<ironcache_observe::KeyspaceDbLine>> =
            if home.total > 1 && cmd_upper == b"INFO" && info_reply_includes_keyspace(request) {
                Some(
                    crate::whole_keyspace::gather_node_keyspace(
                        inbox,
                        ctx,
                        ctx.databases,
                        conn.db,
                        home.index,
                    )
                    .await,
                )
            } else {
                None
            };
        handle_request(
            ctx,
            conn,
            env,
            store_rc,
            wheel_rc,
            state_rc,
            request,
            &cmd_upper,
            node_keyspace.as_deref(),
            out,
        )
    };

    // BLOCKING WAKE (PROD-9): a HOME-shard WRITE that may have ADDED an element to a list/zset
    // (a push / move-dest / zadd / store-into) WAKES the longest-waiting parked waiter on that
    // destination key, so a BLPOP/BZPOPMIN/... blocked on the key re-attempts its pop and gets the
    // pushed element (Redis "serve the longest-waiting blocked client first"). It runs on the
    // HOME (key-owner) shard, the same shard a co-located blocked client parked on, so the wake +
    // the park share the one per-shard registry with no cross-shard coordination -- the common
    // co-located/single-key case is fully covered. A REMOTE write (a cross-shard push to a sibling
    // shard) wakes a waiter parked on THAT shard via its own drain loop (`run_remote`); a blocking
    // command whose keys SPAN shards is documented as not awaited cross-shard this pass. The wake
    // is a single registry probe gated on the command being an element-adding write
    // (`wake_keys_for_write` returns empty for every read / non-adding command), so the hot path is
    // a single match + an empty-Vec check. An over-broad wake is SAFE: the woken waiter re-checks
    // and re-parks if the key is still empty.
    wake_blocking_waiters_home(conn.db, &cmd_upper, request);

    // KEYSPACE NOTIFICATIONS (PROD-8): any HOME-shard mutation in the branches above (the home
    // keyed path, the home SUBSET of a multikey / spanning fan-out, the active TTL drain) recorded
    // its keyspace event(s) into THIS shard's pending buffer DURING dispatch. Drain + PUBLISH them
    // now, AFTER the reply is encoded (per-connection FIFO, SERVER_PUSH.md), through the existing
    // Pub/Sub fan-out. The drain short-circuits on an EMPTY buffer (the common case: a read, or
    // notifications disabled), so on the default deployment this is a single thread-local
    // `is_empty` check and the path is byte-identical. Events recorded on a REMOTE owner shard
    // (a cross-shard write) are drained + published by THAT shard's drain loop (`run_remote`).
    publish_pending_keyspace_events(inbox, home.index);
    close
}

/// WAKE any blocking waiter parked on a key this HOME-shard WRITE may have made ready (PROD-9).
/// `wake_keys_for_write` returns the destination key(s) of an element-adding command (push / move
/// dest / zadd / store-into), or an EMPTY vec for every other command -- so on the hot path (reads,
/// non-adding writes) this is one match + an `is_empty` check and the registry is never touched.
/// For each ready key it wakes the FRONT (longest-waiting) waiter (Redis fairness); the woken
/// connection re-attempts its pop. The registry handle is taken + dropped here (cold path).
fn wake_blocking_waiters_home(db: u32, cmd_upper: &[u8], request: &Request) {
    let keys = ironcache_server::wake_keys_for_write(cmd_upper, request);
    if keys.is_empty() {
        return;
    }
    let registry = shard_blocking();
    let mut reg = registry.borrow_mut();
    for key in keys {
        reg.wake_one(db, &key);
    }
}

/// WAKE blocking waiters parked on THIS shard for a CROSS-SHARD write that ran here (PROD-9), called
/// from the coordinator drain loop's `run_remote` path. It uppercases the command itself (the
/// coordinator carries the raw request) and delegates to the same wake logic as the home path, so a
/// push that lands on this shard from a writer homed elsewhere still wakes a co-located blocked
/// client. `pub(crate)` so `crate::coordinator` reaches it on the owner shard thread.
pub(crate) fn wake_blocking_waiters_for_shard(db: u32, request: &Request) {
    let cmd_upper = ascii_upper(request.command());
    wake_blocking_waiters_home(db, &cmd_upper, request);
}

/// DRAIN this shard's pending keyspace events (PROD-8) and PUBLISH each through the EXISTING
/// Pub/Sub fan-out ([`coordinator::fan_out_publish`]), so subscribers of `__keyspace@db__:<key>` /
/// `__keyevent@db__:<event>` (and PSUBSCRIBE patterns + cross-shard subscribers) receive them
/// exactly like a client PUBLISH. Called AFTER the command's reply is encoded (per-connection FIFO,
/// SERVER_PUSH.md "a push arrives after that command's reply").
///
/// FAST PATH: when no event was recorded (a read, or `notify-keyspace-events` disabled -- the
/// common case) the drain returns an empty Vec and this returns immediately, so it costs a single
/// thread-local `is_empty` check and no fan-out. Only when an event was actually recorded does it
/// build the channel name(s) + fan out. Each recorded event publishes the `K` keyspace message
/// (channel `__keyspace@db__:<key>`, payload = the event name) and/or the `E` keyevent message
/// (channel `__keyevent@db__:<event>`, payload = the key), per the channel selectors resolved at
/// record time. The receiver COUNT each PUBLISH returns is ignored (a notification's value is the
/// delivery, not a reply).
pub(crate) fn publish_pending_keyspace_events(inbox: &coordinator::Inbox, home: usize) {
    let events = ironcache_config::notify::drain();
    if events.is_empty() {
        return;
    }
    for ev in events {
        // FIRE-AND-FORGET (#543): the delivery COUNT is ignored for a notification, so this enqueues
        // the fan-out and returns rather than awaiting every shard's reply. This keeps notifications
        // off the command's synchronous cross-shard path (the drain-loop analog in
        // `coordinator::publish_pending_keyspace_events` MUST be fire-and-forget to avoid a two-shard
        // drain-loop deadlock; the home path matches it for a consistent, deadlock-free model). FIFO
        // to any one subscriber is preserved (per source->target inbox ordering); a self-subscribed
        // connection still receives the push AFTER its command reply because the push rides the
        // separate per-connection channel drained only after this batch's reply is flushed.
        if ev.keyspace {
            let channel = ev.keyspace_channel();
            coordinator::fan_out_publish_notify(inbox, &channel, ev.event.as_bytes(), ev.db, home);
        }
        if ev.keyevent {
            let channel = ev.keyevent_channel();
            coordinator::fan_out_publish_notify(inbox, &channel, &ev.key, ev.db, home);
        }
    }
}
