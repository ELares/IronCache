// SPDX-License-Identifier: MIT OR Apache-2.0
//! Per-connection state (PROTOCOL.md "per-connection state machine",
//! CONNECTION_LIFECYCLE.md).
//!
//! A connection's state is shard-core-local; there is no cross-core sharing
//! (ADR-0002). It holds the negotiated protocol version, the selected DB, the
//! client name, the authenticated flag, and the per-connection client id.

use ironcache_protocol::{ProtoVersion, Request};

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
    /// dirty-CAS abort is a SEPARATE mechanism deferred to PR-10b.
    pub dirty_exec: bool,
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
        // should_close intentionally not touched by RESET.
    }

    /// Enter the transaction (queueing) state for a fresh `MULTI`: mark `in_multi`
    /// and clear any stale queue/dirty flag from a prior transaction so the new
    /// transaction starts clean (TRANSACTIONS.md, PR-10a).
    pub fn enter_multi(&mut self) {
        self.in_multi = true;
        self.queued.clear();
        self.dirty_exec = false;
    }

    /// Leave the transaction state, dropping the staged queue and the dirty flag.
    /// Called by `EXEC` (after running or aborting the batch), `DISCARD`, and
    /// `RESET`. All three of the MULTI fields are cleared together so no stale queue
    /// leaks into the next command (TRANSACTIONS.md "exiting MULTI clears the queue").
    pub fn clear_txn(&mut self) {
        self.in_multi = false;
        self.queued.clear();
        self.dirty_exec = false;
    }
}
