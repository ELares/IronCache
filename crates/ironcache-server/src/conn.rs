// SPDX-License-Identifier: MIT OR Apache-2.0
//! Per-connection state (PROTOCOL.md "per-connection state machine",
//! CONNECTION_LIFECYCLE.md).
//!
//! A connection's state is shard-core-local; there is no cross-core sharing
//! (ADR-0002). It holds the negotiated protocol version, the selected DB, the
//! client name, the authenticated flag, and the per-connection client id.

use bytes::Bytes;
use ironcache_protocol::{ProtoVersion, Request};
use ironcache_storage::WatchEntry;
use std::collections::HashSet;

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
            should_close: false,
            addr,
            laddr,
            in_multi: false,
            queued: Vec::new(),
            dirty_exec: false,
            watch: Vec::new(),
            sub_channels: HashSet::new(),
            sub_patterns: HashSet::new(),
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
        self.clear_txn();
        // RESET exits subscribe mode (Redis: RESET unsubscribes from all channels/patterns).
        // This clears the CONNECTION-side membership only; the serve layer deregisters the
        // matching shard-table entries (it owns the push senders) when it observes the
        // connection leave subscribe mode, the same split as the disconnect-cleanup path.
        self.sub_channels.clear();
        self.sub_patterns.clear();
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
        !self.sub_channels.is_empty() || !self.sub_patterns.is_empty()
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
