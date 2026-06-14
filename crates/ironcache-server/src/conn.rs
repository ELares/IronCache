// SPDX-License-Identifier: MIT OR Apache-2.0
//! Per-connection state (PROTOCOL.md "per-connection state machine",
//! CONNECTION_LIFECYCLE.md).
//!
//! A connection's state is shard-core-local; there is no cross-core sharing
//! (ADR-0002). It holds the negotiated protocol version, the selected DB, the
//! client name, the authenticated flag, and the per-connection client id.

use ironcache_protocol::ProtoVersion;

/// The mutable state of a single client connection.
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
        }
    }

    /// Reset the connection to a fresh post-handshake baseline (RESET command):
    /// proto back to RESP2, DB 0, name cleared, MULTI state cleared (none yet).
    /// Authentication is dropped if a password is configured (`requires_auth`).
    pub fn reset(&mut self, requires_auth: bool) {
        self.proto = ProtoVersion::Resp2;
        self.db = 0;
        self.name.clear();
        self.authenticated = !requires_auth;
        // should_close intentionally not touched by RESET.
    }
}
