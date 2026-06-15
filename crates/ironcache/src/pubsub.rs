// SPDX-License-Identifier: MIT OR Apache-2.0
//! Classic exact-channel Pub/Sub: the server-push substrate (SERVER_PUSH.md #20/#108,
//! PASS 1 = PR 91a).
//!
//! Four Redis features deliver data OUTSIDE the request/reply flow (classic Pub/Sub,
//! sharded Pub/Sub, keyspace notifications, CSC invalidations); on the wire they are ONE
//! shape, a RESP3 push frame (`>`) on RESP3 and a multi-bulk array (`*`) on RESP2
//! (SERVER_PUSH.md). This module implements the FIRST of those (classic SUBSCRIBE /
//! UNSUBSCRIBE / PUBLISH) and lays the substrate the other three slot into:
//!
//! - [`ServerPush`] + [`render`]: the one internal push value and its per-connection
//!   renderer. Framing is written ONCE (via [`ironcache_protocol::Value::Push`], which the
//!   encoder maps to `>` under RESP3 / `*` under RESP2), so the four features cannot drift.
//!   [`ServerPush::PMessage`] is reserved for PSUBSCRIBE (PR 91b) and unused this pass.
//! - [`ShardPubSub`]: the PER-SHARD subscription table (channel -> {conn id -> push sender}).
//!   Subscription state is per-shard, NOT a global registry: under shared-nothing
//!   thread-per-core (ADR-0002) a global subscriber table would be a cross-core hot
//!   structure on every PUBLISH. It is a core-local thread-local with NO lock; the only
//!   cross-core handle it stores is the `Send` [`mpsc::Sender<ServerPush>`] of each
//!   subscriber connection (the connection lives on its home shard, but a PUBLISH that
//!   fans out reaches this table on every shard, so the senders must cross cores). The
//!   `patterns` map is reserved for PSUBSCRIBE (PR 91b) and stays empty this pass.
//!
//! ## Back-pressure (SERVER_PUSH.md)
//!
//! Delivery uses [`mpsc::Sender::try_send`], NEVER `send().await`: a push must never block
//! the publishing shard. A subscriber whose bounded push channel ([`PUSH_CHANNEL_BOUND`])
//! is FULL (a slow consumer) is DEREGISTERED from the shard table on the spot (its sender
//! is dropped); its serve loop then sees `push_rx.recv()` return `None` and treats that as
//! a disconnect, so shard memory stays bounded.

use bytes::Bytes;
use ironcache_protocol::{ProtoVersion, Value};
use std::collections::HashMap;
use tokio::sync::mpsc;

/// The bounded depth of each subscriber connection's per-connection push channel
/// (SERVER_PUSH.md back-pressure). A push is `try_send`'d into this channel by the
/// publishing shard and drained by the subscriber's serve loop. The bound is the
/// back-pressure knob: once a slow consumer's undrained pushes reach it, the publisher's
/// `try_send` returns [`mpsc::error::TrySendError::Full`] and the subscriber is shed
/// (deregistered + disconnected) rather than allowed to grow shard memory unbounded. 1024
/// mirrors the cross-shard [`crate::coordinator::INBOX_DEPTH`] first cut: deep enough that
/// a burst coalesces, shallow enough to bound memory.
pub const PUSH_CHANNEL_BOUND: usize = 1024;

/// One internal server-push value (SERVER_PUSH.md). It carries every out-of-band frame so
/// framing is written ONCE by [`render`]; this pass uses only [`ServerPush::Message`]
/// (classic Pub/Sub), with [`ServerPush::PMessage`] reserved for PSUBSCRIBE (PR 91b).
///
/// All fields are [`Bytes`] (refcounted, `Send`), so a `ServerPush` crosses the
/// thread boundary from the publishing shard to a subscriber on another core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerPush {
    /// A classic Pub/Sub message: `channel` + `payload` (the SUBSCRIBE delivery shape).
    Message {
        /// The channel the message was published to.
        channel: Bytes,
        /// The published payload.
        payload: Bytes,
    },
    /// A pattern Pub/Sub message: `pattern` (the matched PSUBSCRIBE pattern) + `channel`
    /// (the concrete channel) + `payload`. RESERVED for PSUBSCRIBE (PR 91b); never
    /// constructed this pass.
    PMessage {
        /// The PSUBSCRIBE pattern that matched.
        pattern: Bytes,
        /// The concrete channel the message was published to.
        channel: Bytes,
        /// The published payload.
        payload: Bytes,
    },
}

impl ServerPush {
    /// Render this push into a [`Value::Push`] (SERVER_PUSH.md): the encoder writes the
    /// RESP3 push type `>` when `proto == Resp3` and the equivalent multi-bulk array `*`
    /// when `proto == Resp2` (ADR-0019), so the framing is chosen at the connection writer
    /// from the per-connection negotiated proto, not by the publisher. The element order is
    /// the canonical Redis shape: `["message", channel, payload]` for a classic message and
    /// `["pmessage", pattern, channel, payload]` for a pattern message.
    ///
    /// `proto` is accepted (not yet branched on) so this signature is stable when later
    /// push kinds need proto-specific shaping; `Value::Push` already carries the RESP2/RESP3
    /// split, so the renderer itself is proto-agnostic this pass.
    #[must_use]
    pub fn render(&self, _proto: ProtoVersion) -> Value {
        match self {
            ServerPush::Message { channel, payload } => Value::Push(vec![
                Value::bulk_str("message"),
                Value::bulk(channel.clone()),
                Value::bulk(payload.clone()),
            ]),
            ServerPush::PMessage {
                pattern,
                channel,
                payload,
            } => Value::Push(vec![
                Value::bulk_str("pmessage"),
                Value::bulk(pattern.clone()),
                Value::bulk(channel.clone()),
                Value::bulk(payload.clone()),
            ]),
        }
    }
}

/// The PER-SHARD subscription table (SERVER_PUSH.md routing tables). Maps a channel (and,
/// from PR 91b, a pattern) to the set of LOCAL subscriber connections by connection id,
/// each carrying that connection's `Send` push sender.
///
/// Core-local (a thread-local in [`crate::serve`]); NO lock (ADR-0002 shared-nothing). The
/// only cross-core handle stored is the `Send` [`mpsc::Sender<ServerPush>`]: a PUBLISH that
/// fans out to every shard reaches each shard's own `ShardPubSub` (via the cross-shard
/// coordinator), so each shard renders to ITS connections from ITS table with no shared
/// lock. The `patterns` map is reserved for PSUBSCRIBE (PR 91b) and stays empty this pass.
#[derive(Debug, Default)]
pub struct ShardPubSub {
    /// channel -> {conn id -> push sender}. A channel with no subscribers is absent (the
    /// last UNSUBSCRIBE / disconnect removes the empty inner map).
    pub channels: HashMap<Bytes, HashMap<u64, mpsc::Sender<ServerPush>>>,
    /// pattern -> {conn id -> push sender}. RESERVED for PSUBSCRIBE (PR 91b); empty this
    /// pass. Designed in now so the pattern fan-out slots in without reshaping this table.
    pub patterns: HashMap<Bytes, HashMap<u64, mpsc::Sender<ServerPush>>>,
}

impl ShardPubSub {
    /// Register `conn_id`'s push `sender` as a subscriber of `channel` on THIS shard. A
    /// re-subscribe (same conn id, same channel) overwrites the stale sender with the
    /// current one (idempotent on the table; the caller decides whether the running count
    /// bumps, matching Redis's "already subscribed does not bump" rule).
    pub fn subscribe(&mut self, channel: Bytes, conn_id: u64, sender: mpsc::Sender<ServerPush>) {
        self.channels
            .entry(channel)
            .or_default()
            .insert(conn_id, sender);
    }

    /// Deregister `conn_id` from `channel` on THIS shard, pruning the channel entry when it
    /// has no remaining subscribers (so an idle channel holds no memory).
    pub fn unsubscribe(&mut self, channel: &[u8], conn_id: u64) {
        if let Some(subs) = self.channels.get_mut(channel) {
            subs.remove(&conn_id);
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }
    }

    /// Deregister `conn_id` from `pattern` on THIS shard (the PSUBSCRIBE analog of
    /// [`Self::unsubscribe`]), pruning the empty pattern entry. RESERVED for PR 91b; the
    /// disconnect cleanup already drives it so pattern cleanup needs no reshaping when
    /// PSUBSCRIBE lands. A no-op this pass (the `patterns` map is always empty).
    pub fn unsubscribe_pattern(&mut self, pattern: &[u8], conn_id: u64) {
        if let Some(subs) = self.patterns.get_mut(pattern) {
            subs.remove(&conn_id);
            if subs.is_empty() {
                self.patterns.remove(pattern);
            }
        }
    }

    /// Deliver `push` to every LOCAL subscriber of `channel` on THIS shard, returning the
    /// number actually delivered (SERVER_PUSH.md fan-out). Delivery is NON-BLOCKING
    /// [`mpsc::Sender::try_send`] (a push must never block the publishing shard); a
    /// subscriber whose channel is FULL (a slow consumer past [`PUSH_CHANNEL_BOUND`]) is
    /// SHED here -- removed from the table so its `push_rx.recv()` returns `None` and its
    /// serve loop disconnects it, keeping shard memory bounded. A shed subscriber is NOT
    /// counted as a receiver (it did not receive this message).
    pub fn deliver(&mut self, channel: &[u8], push: &ServerPush) -> i64 {
        let Some(subs) = self.channels.get_mut(channel) else {
            return 0;
        };
        let mut delivered: i64 = 0;
        // Collect the conn ids to shed; we cannot remove while iterating the same map.
        let mut shed: Vec<u64> = Vec::new();
        for (&conn_id, sender) in subs.iter() {
            match sender.try_send(push.clone()) {
                Ok(()) => delivered += 1,
                // Full: a slow consumer past the bound -> SHED (drop its sender so its serve
                // loop sees push_rx closed and disconnects). Closed: the receiver is already
                // gone (the connection dropped push_rx but its disconnect cleanup has not pruned
                // the table yet) -> drop it here too. Both outcomes shed the subscriber.
                Err(mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_)) => {
                    shed.push(conn_id);
                }
            }
        }
        for conn_id in shed {
            subs.remove(&conn_id);
        }
        if subs.is_empty() {
            self.channels.remove(channel);
        }
        delivered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(ch: &[u8], p: &[u8]) -> ServerPush {
        ServerPush::Message {
            channel: Bytes::copy_from_slice(ch),
            payload: Bytes::copy_from_slice(p),
        }
    }

    #[test]
    fn render_message_is_the_canonical_three_element_shape() {
        let v = msg(b"ch", b"hi").render(ProtoVersion::Resp3);
        assert_eq!(
            v,
            Value::Push(vec![
                Value::bulk_str("message"),
                Value::bulk(Bytes::from_static(b"ch")),
                Value::bulk(Bytes::from_static(b"hi")),
            ])
        );
    }

    #[test]
    fn render_is_value_push_so_proto_selects_the_frame() {
        // The renderer returns Value::Push regardless of proto; the ENCODER picks `>`
        // (RESP3) vs `*` (RESP2) from the per-connection proto (ADR-0019). Both protos
        // therefore render the SAME Value::Push.
        let r2 = msg(b"c", b"p").render(ProtoVersion::Resp2);
        let r3 = msg(b"c", b"p").render(ProtoVersion::Resp3);
        assert_eq!(r2, r3);
        assert!(matches!(r2, Value::Push(_)));
    }

    #[test]
    fn deliver_counts_and_drops_full_subscribers() {
        let mut t = ShardPubSub::default();
        // A subscriber with a tiny channel: the second push fills it, the third sheds it.
        let (tx, mut rx) = mpsc::channel::<ServerPush>(1);
        t.subscribe(Bytes::from_static(b"ch"), 7, tx);
        // First deliver fits (count 1).
        assert_eq!(t.deliver(b"ch", &msg(b"ch", b"a")), 1);
        // Channel now holds 1 (the bound); the next deliver finds it FULL and sheds conn 7,
        // returning 0 delivered.
        assert_eq!(t.deliver(b"ch", &msg(b"ch", b"b")), 0);
        // Shed: the channel entry is pruned (no subscribers left).
        assert!(!t.channels.contains_key(b"ch".as_slice()));
        // The one queued message is still readable (shedding only drops the SENDER).
        assert_eq!(rx.try_recv().unwrap(), msg(b"ch", b"a"));
    }

    #[test]
    fn unsubscribe_prunes_empty_channels() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(4);
        t.subscribe(Bytes::from_static(b"ch"), 1, tx);
        assert!(t.channels.contains_key(b"ch".as_slice()));
        t.unsubscribe(b"ch", 1);
        assert!(!t.channels.contains_key(b"ch".as_slice()));
    }

    #[test]
    fn deliver_to_no_subscribers_is_zero() {
        let mut t = ShardPubSub::default();
        assert_eq!(t.deliver(b"absent", &msg(b"absent", b"x")), 0);
    }
}
