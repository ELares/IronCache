// SPDX-License-Identifier: MIT OR Apache-2.0
//! Per-connection state (PROTOCOL.md "per-connection state machine",
//! CONNECTION_LIFECYCLE.md).
//!
//! A connection's state is shard-core-local; there is no cross-core sharing
//! (ADR-0002). It holds the negotiated protocol version, the selected DB, the
//! client name, the authenticated flag, and the per-connection client id.

use crate::acl::{DEFAULT_USER, User};
use bytes::Bytes;
use ironcache_protocol::{ProtoVersion, Request};
use ironcache_storage::WatchEntry;
use std::collections::HashSet;
use std::sync::Arc;

/// The mutable state of a single client connection.
///
/// It carries several independent boolean flags (authenticated / should_close /
/// in_multi / dirty_exec), each a distinct connection-lifecycle bit rather than a
/// bit-field to pack; the `struct_excessive_bools` lint is allowed for this reason.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct ConnState {
    /// Monotonic per-process client id (CLIENT ID / HELLO `id`).
    pub id: u64,
    /// The negotiated protocol version (RESP2 until `HELLO 3`).
    pub proto: ProtoVersion,
    /// The selected logical database index.
    pub db: u32,
    /// The client-set name (CLIENT SETNAME / HELLO SETNAME), empty if unset.
    pub name: String,
    /// Whether the connection has authenticated. When no password is configured
    /// the connection is authenticated from the start (Redis behavior).
    pub authenticated: bool,
    /// The ACL user this connection is AUTHENTICATED AS (#106), cached at AUTH time so the
    /// per-command authorization check reads it LOCK-FREE (no ACL-registry lock on the data
    /// path). `None` means "the implicit all-permissive default user": a connection that is
    /// authenticated without an explicit ACL user (the no-ACL / legacy-requirepass posture)
    /// carries `None`, and the enforcement layer treats `None` as full access -- so the
    /// default deployment is byte-identical (no per-command ACL cost). `AUTH <user> <pass>`
    /// (or the requirepass `AUTH <pass>` resolving the narrowed `default`) caches the
    /// resolved `Arc<User>` here; RESET / a re-AUTH replaces it.
    pub acl_user: Option<Arc<User>>,
    /// The NAME of the ACL user this connection is authenticated as (#106, F1 live revocation).
    /// `"default"` on a fresh connection (the implicit identity, even with `acl_user == None`),
    /// updated to the resolved user's name at AUTH time and reset to `"default"` by RESET. The
    /// per-command path needs the name to RE-RESOLVE the user by name when the registry
    /// generation moves (`acl_user` alone, being `None` for an all-permissive default, does not
    /// carry it). A mid-session `ACL SETUSER default ...` therefore reaches a never-AUTHed
    /// no-requirepass connection too (it is still the `"default"` identity).
    pub acl_user_name: String,
    /// The ACL-registry GENERATION the cached `acl_user` was resolved against (#106, F1). The
    /// per-command enforcement path does ONE relaxed load of the live registry generation and
    /// compares it to this; on a MISMATCH (a mutation happened) it re-resolves `acl_user` by
    /// `acl_user_name` and updates this. On the no-ACL path the generation never moves, so this
    /// stays equal and the hot path is a single integer compare.
    pub acl_user_gen: u64,
    /// Whether the connection asked to close (QUIT). The serve loop flushes the
    /// pending reply and then closes.
    pub should_close: bool,
    /// The peer address string, for CLIENT INFO.
    pub addr: String,
    /// The local (server) address string, for CLIENT INFO.
    pub laddr: String,
    /// Whether the connection is inside a transaction (`MULTI` opened, `EXEC`/
    /// `DISCARD`/`RESET` not yet seen). While `true`, every non-control command is
    /// QUEUED rather than executed (TRANSACTIONS.md "queue then apply", PR-10a).
    pub in_multi: bool,
    /// The staged commands awaiting `EXEC`, in arrival order. Each is the parsed
    /// [`Request`] cloned at queue time; `EXEC` replays them in order against the
    /// already-borrowed store (no re-borrow, no rollback). Empty unless `in_multi`.
    pub queued: Vec<Request>,
    /// Whether a command that failed validation at queue time (unknown command or a
    /// table-arity error) has dirtied the transaction. When `true`, `EXEC` refuses the
    /// whole batch with `-EXECABORT` and applies nothing, faithful to Redis
    /// (TRANSACTIONS.md "queue-time command errors abort the whole batch"). WATCH's
    /// dirty-CAS abort is a SEPARATE mechanism (the `watch` snapshot list below, PR-10b):
    /// it makes EXEC return a null array, NOT EXECABORT, and does not set this flag.
    pub dirty_exec: bool,
    /// The WATCHed-key snapshots for this connection (TRANSACTIONS.md per-key dirty-CAS,
    /// PR-10b). Each [`WatchEntry`] is the version + present/absent snapshot taken at
    /// `WATCH` time on the connection's accept shard. At `EXEC` the dispatcher
    /// revalidates every entry against the store: if any is dirty, EXEC returns a null
    /// array and applies nothing. The list is cleared (and the store deregistered via
    /// `Watch::unwatch`) by `EXEC` (every exit path), `UNWATCH`, `DISCARD`, `RESET`, and
    /// a connection close. Empty unless the connection has an active WATCH set.
    ///
    /// SINGLE-SHARD-PER-CONNECTION (PR-10b scope): every entry's `db`+`key` lives on this
    /// connection's accept shard, so revalidation + apply run on one owning core; a
    /// watched key on another shard (cross-shard EXEC) is out of scope (COORDINATOR.md).
    pub watch: Vec<WatchEntry>,
    /// The exact channels this connection is SUBSCRIBEd to (classic Pub/Sub, SERVER_PUSH.md
    /// #20, PR 91a). Holds only the channel NAMES (the per-shard subscription table in the
    /// serve layer holds the `Send` push sender); a connection is "in subscribe mode" when
    /// this or [`Self::sub_patterns`] is non-empty ([`Self::is_subscriber`]). It drives the
    /// disconnect-cleanup deregistration (the serve loop removes this connection from each
    /// named channel in the shard table). The push SENDER deliberately lives in the serve
    /// layer, NOT here: it is a tokio handle, and this crate carries no tokio dependency.
    pub sub_channels: HashSet<Bytes>,
    /// The PSUBSCRIBE patterns this connection is subscribed to (SERVER_PUSH.md). RESERVED
    /// for PR 91b; always empty this pass. Designed in now so [`Self::is_subscriber`] and the
    /// disconnect cleanup already account for patterns without reshaping the struct.
    pub sub_patterns: HashSet<Bytes>,
    /// The SHARD channels this connection is SSUBSCRIBEd to (sharded Pub/Sub, #410). A SEPARATE
    /// namespace from [`Self::sub_channels`]; the SSUBSCRIBE confirmation count is the size of
    /// THIS set only (Redis reports the shard-channel count, not the channels+patterns total).
    /// Drives the disconnect-cleanup deregistration from the serve layer's `shard_channels` table.
    /// A connection holding any shard subscription is "in subscribe mode" ([`Self::is_subscriber`]).
    pub sub_shard_channels: HashSet<Bytes>,
    /// CLIENT TRACKING: server-assisted client-side caching is ON for this connection (#409). When
    /// set, a READ registers the read keys in the per-shard tracking table, and a later change to
    /// one of them pushes an `invalidate` to this connection. Default `false`, so the non-tracking
    /// hot path is a single bool test. `CLIENT TRACKING OFF` and RESET clear it (which also purges
    /// this connection from every shard's tracking table). BCAST/OPTIN/OPTOUT/REDIRECT are later
    /// stages.
    pub tracking_on: bool,
    /// CLIENT TRACKING `NOLOOP` (#409): suppress invalidation pushes caused by THIS connection's
    /// OWN writes (a client that already updated its cache does not need the echo). Default `false`.
    pub tracking_noloop: bool,
    /// CLIENT TRACKING `BCAST` mode (#409 stage 2): broadcast tracking by key PREFIX instead of by
    /// individual read keys. When set, the connection does NOT register the keys it reads; instead
    /// its [`Self::tracking_prefixes`] are registered once, and EVERY changed key matching a prefix
    /// pushes an invalidation (sticky, not one-shot). Default `false` (the per-read default mode).
    pub tracking_bcast: bool,
    /// The BCAST key prefixes this connection tracks (#409 stage 2). Empty with `tracking_bcast`
    /// means the EMPTY prefix (track ALL keys). The serve layer registers these in the per-shard
    /// tracking table on the BCAST-enter transition and purges them on OFF/RESET/disconnect.
    pub tracking_prefixes: Vec<Bytes>,
    /// CLIENT TRACKING `OPTIN` mode (#409 stage 3): in default (per-read) tracking, register a
    /// read's keys ONLY when the connection ran `CLIENT CACHING YES` immediately before. Mutually
    /// exclusive with [`Self::tracking_optout`] and with BCAST. Default `false`.
    pub tracking_optin: bool,
    /// CLIENT TRACKING `OPTOUT` mode (#409 stage 3): register every read's keys EXCEPT when the
    /// connection ran `CLIENT CACHING NO` immediately before. Mutually exclusive with `OPTIN` and
    /// with BCAST. Default `false`.
    pub tracking_optout: bool,
    /// The ONE-SHOT `CLIENT CACHING YES|NO` flag (#409 stage 3): `Some(true)` after `CACHING YES`,
    /// `Some(false)` after `CACHING NO`, consumed by the NEXT command's track decision (then
    /// cleared). Only meaningful in OPTIN/OPTOUT mode. `None` otherwise.
    pub caching_next: Option<bool>,
    /// The per-connection CLUSTER read-only bit (REPLICA_READ.md #147, HA-7d). `false` (read
    /// -write) by default; `READONLY` sets it, `READWRITE` clears it. On a REPLICA node, a keyed
    /// READ for a slot this node replicates is served LOCALLY only when this bit is set; otherwise
    /// (or for any WRITE) the replica returns `-MOVED` to the slot owner. The bit is independent of
    /// node role, so on a non-replica / standalone node it is harmless (the routing only consults
    /// it on the cold replica-read path). RESET clears it back to read-write (Redis parity).
    pub readonly: bool,
    /// The per-connection ONE-SHOT `ASKING` flag (HA-6 online slot migration). `ASKING` sets it;
    /// the VERY NEXT command consumes it (whether or not it was a keyed command). When set, a keyed
    /// command on a slot THIS node is IMPORTING is served LOCALLY instead of being redirected with
    /// `-MOVED` to the (still-)owner -- this is the second leg of an `-ASK` redirect (the client
    /// sends `ASKING` then re-issues the command at the destination). It is independent of node role
    /// and consulted ONLY on the cold migration redirect path, so a non-importing / standalone node
    /// is unaffected. RESET clears it (Redis parity), and the router clears it after each command.
    pub asking: bool,
    /// The TRANSACTION-SCOPED `ASKING` state for the in-`MULTI` QUEUE-TIME cluster redirect (HA-6).
    /// The one-shot [`Self::asking`] is consumed PER COMMAND at the top of the router (so it can
    /// never LEAK past a command), which means the flag a client set with `ASKING` BEFORE `MULTI`
    /// would otherwise be gone by the time the transaction's commands are QUEUED. Redis keeps the
    /// single `CLIENT_ASKING` flag live across the MULTI queueing phase (its cluster redirect runs
    /// at queue time, BEFORE the flag is cleared per executed command), so an `ASKING; MULTI; <cmd
    /// on an IMPORTING slot>; EXEC` queues + serves on the importing destination. We mirror that by
    /// CARRYING the ASKING in effect when `MULTI` opens into this transaction-scoped field: the
    /// queue-time redirect in `route_in_multi` consults THIS (not the per-command one-shot) so every
    /// command queued inside the transaction honors the pre-MULTI `ASKING`. It is bounded to the
    /// transaction lifetime -- `enter_multi` initializes it from the pre-MULTI one-shot, and
    /// `clear_txn` / `reset` clear it on EXEC / DISCARD / RESET -- so it can NEVER leak past the
    /// transaction (the leak-fix invariant is preserved). On a non-cluster / non-migrating node it
    /// is always `false` and is consulted only on the cold migration redirect, so the default path
    /// is unaffected.
    pub txn_asking: bool,
}

impl ConnState {
    /// Construct the initial state for a new connection.
    ///
    /// `requires_auth` is whether a password is configured; if not, the connection
    /// starts authenticated (Redis: with no `requirepass`, every connection is
    /// effectively authenticated as the default user).
    #[must_use]
    pub fn new(
        id: u64,
        default_proto: ProtoVersion,
        requires_auth: bool,
        addr: String,
        laddr: String,
    ) -> Self {
        ConnState {
            id,
            proto: default_proto,
            db: 0,
            name: String::new(),
            authenticated: !requires_auth,
            // No explicit ACL user on a fresh connection: the implicit all-permissive default
            // (full access) until an `AUTH <user> <pass>` caches a narrowed identity. On the
            // no-ACL path this stays `None` for the connection's life, so enforcement is skipped
            // and the default path is byte-identical (#106).
            acl_user: None,
            // A fresh connection is the implicit `default` identity (F1): the name is set even
            // when `acl_user` is `None`, so a mid-session `ACL SETUSER default ...` re-resolves
            // for it. AUTH overwrites it with the resolved user's name.
            acl_user_name: DEFAULT_USER.to_owned(),
            // Cached generation starts at 0 (the boot generation); the first command compares it
            // against the live registry generation and re-resolves only if a mutation has moved it.
            acl_user_gen: 0,
            should_close: false,
            addr,
            laddr,
            in_multi: false,
            queued: Vec::new(),
            dirty_exec: false,
            watch: Vec::new(),
            sub_channels: HashSet::new(),
            sub_patterns: HashSet::new(),
            sub_shard_channels: HashSet::new(),
            tracking_on: false,
            tracking_noloop: false,
            tracking_bcast: false,
            tracking_prefixes: Vec::new(),
            tracking_optin: false,
            tracking_optout: false,
            caching_next: None,
            // Read-write by default (the strong-read behavior unmodified clients expect); a client
            // opts into replica reads with READONLY (REPLICA_READ.md #147).
            readonly: false,
            // No ASKING pending on a fresh connection (HA-6); set only by an explicit ASKING.
            asking: false,
            // No transaction in flight on a fresh connection, so no transaction-scoped ASKING.
            txn_asking: false,
        }
    }

    /// Reset the connection to a fresh post-handshake baseline (RESET command):
    /// proto back to RESP2, DB 0, name cleared, transaction state cleared (RESET
    /// inside a MULTI aborts it, matching Redis). Authentication is dropped if a
    /// password is configured (`requires_auth`).
    pub fn reset(&mut self, requires_auth: bool) {
        self.proto = ProtoVersion::Resp2;
        self.db = 0;
        self.name.clear();
        self.authenticated = !requires_auth;
        // RESET drops the authenticated ACL identity back to the implicit default (#106):
        // a post-RESET connection must re-AUTH to regain a narrowed user, matching Redis
        // (RESET re-authenticates as the default user when auth is required). The name goes
        // back to `"default"` (F1) so the post-RESET implicit-default identity re-resolves on a
        // mid-session `ACL SETUSER default ...`; `acl_user_gen` is left as-is (the next command
        // re-resolves the `default` user if the generation has since moved).
        self.acl_user = None;
        // Reuse the existing allocation rather than reassign (clippy::assigning_clones).
        self.acl_user_name.clear();
        self.acl_user_name.push_str(DEFAULT_USER);
        self.clear_txn();
        // RESET exits subscribe mode (Redis: RESET unsubscribes from all channels/patterns).
        // This clears the CONNECTION-side membership only; the serve layer deregisters the
        // matching shard-table entries (it owns the push senders) when it observes the
        // connection leave subscribe mode, the same split as the disconnect-cleanup path.
        self.sub_channels.clear();
        self.sub_patterns.clear();
        self.sub_shard_channels.clear();
        // RESET turns CLIENT TRACKING OFF (Redis parity); the serve layer observes the connection
        // leave tracking mode and purges it from every shard's tracking table (the same split as
        // the subscribe-mode cleanup).
        self.tracking_on = false;
        self.tracking_noloop = false;
        self.tracking_bcast = false;
        self.tracking_prefixes.clear();
        self.tracking_optin = false;
        self.tracking_optout = false;
        self.caching_next = None;
        // RESET clears the CLUSTER read-only bit back to read-write (Redis parity).
        self.readonly = false;
        // RESET clears any pending one-shot ASKING (HA-6): a fresh baseline never carries a stale
        // ASKING into the next command. `clear_txn` (called above) already cleared the
        // transaction-scoped `txn_asking`, so a RESET inside a MULTI cannot carry ASKING forward.
        self.asking = false;
        // should_close intentionally not touched by RESET.
    }

    /// Whether the connection is in SUBSCRIBE mode (SERVER_PUSH.md #20): it has at least one
    /// active channel or pattern subscription. In RESP2 this mode RESTRICTS the allowed
    /// commands (the subscribe-mode gate in `dispatch`); in RESP3 there is no restriction. It
    /// also selects the serve loop's `select!` idle-wait (drain pushes vs read commands) and
    /// the PING reply shape, so the non-subscriber hot path stays untouched.
    #[must_use]
    pub fn is_subscriber(&self) -> bool {
        !self.sub_channels.is_empty()
            || !self.sub_patterns.is_empty()
            || !self.sub_shard_channels.is_empty()
    }

    /// Enter the transaction (queueing) state for a fresh `MULTI`: mark `in_multi`
    /// and clear any stale queue/dirty flag from a prior transaction so the new
    /// transaction starts clean (TRANSACTIONS.md, PR-10a).
    ///
    /// `txn_asking` is deliberately NOT touched here: the ASKING in effect when `MULTI`
    /// opens is the PRE-MULTI one-shot, which the router has already consumed by the time
    /// dispatch reaches this arm (HA-6). The router therefore records it into `txn_asking`
    /// for the MULTI command BEFORE this runs, and a prior transaction's `txn_asking` was
    /// already cleared by `clear_txn` / `reset` -- so a fresh transaction's `txn_asking`
    /// reflects exactly the pre-MULTI ASKING with no stale carry.
    pub fn enter_multi(&mut self) {
        self.in_multi = true;
        self.queued.clear();
        self.dirty_exec = false;
    }

    /// Leave the transaction state, dropping the staged queue and the dirty flag.
    /// Called by `EXEC` (after running or aborting the batch), `DISCARD`, and
    /// `RESET`. All the MULTI fields are cleared together so no stale queue leaks into
    /// the next command (TRANSACTIONS.md "exiting MULTI clears the queue").
    ///
    /// `txn_asking` (the HA-6 transaction-scoped ASKING for the in-MULTI queue-time
    /// cluster redirect) is cleared here too, so the pre-MULTI ASKING applies ONLY to the
    /// transaction it opened and can NEVER leak past EXEC / DISCARD / RESET into a later
    /// command -- the one-shot leak-fix invariant, extended across the transaction.
    pub fn clear_txn(&mut self) {
        self.in_multi = false;
        self.queued.clear();
        self.dirty_exec = false;
        self.txn_asking = false;
    }

    /// Clear the WATCH snapshot list (TRANSACTIONS.md, PR-10b). This drops the
    /// connection-side snapshots ONLY; the dispatcher must deregister them from the store
    /// FIRST via `Watch::unwatch(&state.watch)` (the store holds the per-key watcher
    /// counts, which `ConnState` cannot reach), then call this. Invoked by `EXEC` (every
    /// exit path), `UNWATCH`, `DISCARD`, `RESET`, and a connection close, so a stale watch
    /// never lingers on either side.
    pub fn clear_watch(&mut self) {
        self.watch.clear();
    }
}
