// SPDX-License-Identifier: MIT OR Apache-2.0
//! Pub/Sub: the server-push substrate (SERVER_PUSH.md #20/#108, PR 91a exact channels +
//! PR 91b glob patterns).
//!
//! Four Redis features deliver data OUTSIDE the request/reply flow (classic Pub/Sub,
//! sharded Pub/Sub, keyspace notifications, CSC invalidations); on the wire they are ONE
//! shape, a RESP3 push frame (`>`) on RESP3 and a multi-bulk array (`*`) on RESP2
//! (SERVER_PUSH.md). This module implements the FIRST of those (classic + pattern SUBSCRIBE /
//! PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE / PUBLISH) and lays the substrate the other three
//! slot into:
//!
//! - [`ServerPush`] + [`render`]: the one internal push value and its per-connection
//!   renderer. Framing is written ONCE (via [`ironcache_protocol::Value::Push`], which the
//!   encoder maps to `>` under RESP3 / `*` under RESP2), so the four features cannot drift.
//!   [`ServerPush::PMessage`] carries a PSUBSCRIBE pattern delivery (PR 91b).
//! - [`ShardPubSub`]: the PER-SHARD subscription table (channel/pattern -> {conn id -> push
//!   sender}). Subscription state is per-shard, NOT a global registry: under shared-nothing
//!   thread-per-core (ADR-0002) a global subscriber table would be a cross-core hot
//!   structure on every PUBLISH. It is a core-local thread-local with NO lock; the only
//!   cross-core handle it stores is the `Send` [`mpsc::Sender<ServerPush>`] of each
//!   subscriber connection (the connection lives on its home shard, but a PUBLISH that
//!   fans out reaches this table on every shard, so the senders must cross cores). The
//!   `patterns` map holds PSUBSCRIBE glob-pattern subscriptions (PR 91b); a PUBLISH delivers
//!   to BOTH the exact `channels` map ([`ShardPubSub::deliver`]) AND every matching pattern
//!   ([`ShardPubSub::deliver_patterns`]).
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
/// framing is written ONCE by [`render`]: [`ServerPush::Message`] (classic Pub/Sub) and
/// [`ServerPush::PMessage`] (PSUBSCRIBE pattern Pub/Sub, PR 91b).
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
    /// (the concrete channel) + `payload` (PSUBSCRIBE delivery, PR 91b).
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

/// The PER-SHARD subscription table (SERVER_PUSH.md routing tables). Maps a channel (and a
/// PSUBSCRIBE glob pattern, PR 91b) to the set of LOCAL subscriber connections by connection
/// id, each carrying that connection's `Send` push sender.
///
/// Core-local (a thread-local in [`crate::serve`]); NO lock (ADR-0002 shared-nothing). The
/// only cross-core handle stored is the `Send` [`mpsc::Sender<ServerPush>`]: a PUBLISH that
/// fans out to every shard reaches each shard's own `ShardPubSub` (via the cross-shard
/// coordinator), so each shard renders to ITS connections from ITS table with no shared
/// lock. The `patterns` map holds PSUBSCRIBE subscriptions; one PUBLISH delivers to BOTH the
/// exact `channels` entry AND every matching pattern (no dedup, Redis semantics).
#[derive(Debug, Default)]
pub struct ShardPubSub {
    /// channel -> {conn id -> push sender}. A channel with no subscribers is absent (the
    /// last UNSUBSCRIBE / disconnect removes the empty inner map).
    pub channels: HashMap<Bytes, HashMap<u64, mpsc::Sender<ServerPush>>>,
    /// pattern -> {conn id -> push sender} (PSUBSCRIBE, PR 91b). A pattern with no subscribers
    /// is absent (the last PUNSUBSCRIBE / disconnect removes the empty inner map). A PUBLISH
    /// iterates these and `glob_match`es each pattern against the published channel.
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

    /// Register `conn_id`'s push `sender` as a subscriber of `pattern` on THIS shard (the
    /// PSUBSCRIBE analog of [`Self::subscribe`], PR 91b). A re-subscribe (same conn id, same
    /// pattern) overwrites the stale sender with the current one (idempotent on the table; the
    /// caller decides whether the running count bumps, matching Redis's "already subscribed does
    /// not bump" rule).
    pub fn subscribe_pattern(
        &mut self,
        pattern: Bytes,
        conn_id: u64,
        sender: mpsc::Sender<ServerPush>,
    ) {
        self.patterns
            .entry(pattern)
            .or_default()
            .insert(conn_id, sender);
    }

    /// Deregister `conn_id` from `pattern` on THIS shard (the PSUBSCRIBE analog of
    /// [`Self::unsubscribe`]), pruning the empty pattern entry. The disconnect cleanup
    /// (`deregister_all_subscriptions`) drives it off the connection's `sub_patterns` set, and
    /// PUNSUBSCRIBE deregisters each named (or all) pattern through it.
    pub fn unsubscribe_pattern(&mut self, pattern: &[u8], conn_id: u64) {
        if let Some(subs) = self.patterns.get_mut(pattern) {
            subs.remove(&conn_id);
            if subs.is_empty() {
                self.patterns.remove(pattern);
            }
        }
    }

    /// Deliver `channel` + `payload` to every LOCAL subscriber whose PATTERN matches `channel`
    /// under Redis glob rules (PSUBSCRIBE fan-out, SERVER_PUSH.md, PR 91b), returning the number
    /// actually delivered. A connection is delivered ONCE PER MATCHING PATTERN it holds (Redis
    /// semantics: a connection subscribed to two patterns that both match one channel gets one
    /// `pmessage` per pattern), and the pattern delivery is INDEPENDENT of (and in addition to)
    /// the exact-channel [`Self::deliver`] fan-out -- a connection subscribed to both an exact
    /// channel AND a matching pattern receives BOTH a `message` AND a `pmessage` for one PUBLISH.
    ///
    /// Each matched subscriber is rendered a [`ServerPush::PMessage`] carrying the matched
    /// `pattern` so the client can tell which PSUBSCRIBE produced the delivery. Delivery is the
    /// SAME non-blocking [`mpsc::Sender::try_send`] + slow-consumer SHED as [`Self::deliver`] (a
    /// push must never block the publishing shard); a shed subscriber is NOT counted as a
    /// receiver. `glob` is the binary-safe Redis `stringmatchlen` matcher passed in by the caller
    /// (the `ironcache-server` crate owns it; this crate stays glob-engine-agnostic).
    pub fn deliver_patterns(
        &mut self,
        channel: &[u8],
        payload: &Bytes,
        glob: impl Fn(&[u8], &[u8]) -> bool,
    ) -> i64 {
        let mut delivered: i64 = 0;
        // Patterns whose inner map went empty after shedding (pruned after the walk; we cannot
        // mutate `self.patterns` while iterating it).
        let mut prune: Vec<Bytes> = Vec::new();
        for (pattern, subs) in &mut self.patterns {
            if !glob(pattern.as_ref(), channel) {
                continue;
            }
            let push = ServerPush::PMessage {
                pattern: pattern.clone(),
                channel: Bytes::copy_from_slice(channel),
                payload: payload.clone(),
            };
            let mut shed: Vec<u64> = Vec::new();
            for (&conn_id, sender) in subs.iter() {
                match sender.try_send(push.clone()) {
                    Ok(()) => delivered += 1,
                    Err(
                        mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_),
                    ) => shed.push(conn_id),
                }
            }
            for conn_id in shed {
                subs.remove(&conn_id);
            }
            if subs.is_empty() {
                prune.push(pattern.clone());
            }
        }
        for pattern in prune {
            self.patterns.remove(&pattern);
        }
        delivered
    }

    /// The LOCAL channel names that have at least one subscriber on THIS shard (PUBSUB CHANNELS,
    /// PR 91b), optionally filtered to those matching `pat` under Redis glob rules. The home
    /// core UNIONS + dedups these across shards (a channel may have subscribers on more than one
    /// shard). `glob` is the binary-safe matcher passed in by the caller; `pat == None` returns
    /// every locally-subscribed channel.
    #[must_use]
    pub fn local_channels(
        &self,
        pat: Option<&[u8]>,
        glob: impl Fn(&[u8], &[u8]) -> bool,
    ) -> Vec<Bytes> {
        self.channels
            .keys()
            .filter(|ch| pat.is_none_or(|p| glob(p, ch.as_ref())))
            .cloned()
            .collect()
    }

    /// The LOCAL subscriber count of `channel` on THIS shard (PUBSUB NUMSUB, PR 91b): the size
    /// of the channel's local subscriber map, or 0 when the channel has no local subscriber. The
    /// home core SUMS these per channel across shards.
    #[must_use]
    pub fn local_numsub(&self, channel: &[u8]) -> i64 {
        self.channels
            .get(channel)
            .map_or(0, |subs| i64::try_from(subs.len()).unwrap_or(i64::MAX))
    }

    /// The LOCAL pattern names that have at least one subscriber on THIS shard (PUBSUB NUMPAT,
    /// PR 91b). The home core UNIONS these across shards and COUNTS the DISTINCT patterns (the
    /// same pattern subscribed on two shards is ONE pattern, NOT two).
    #[must_use]
    pub fn local_patterns(&self) -> Vec<Bytes> {
        self.patterns.keys().cloned().collect()
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

    // A tiny test-only glob (prefix `news.` matches `news.*`) so the pattern tests do not pull
    // the real `ironcache_server::glob` into this crate's unit tests; the integration tests
    // exercise the real matcher end to end.
    fn star_suffix(pattern: &[u8], string: &[u8]) -> bool {
        pattern.last() == Some(&b'*') && string.starts_with(&pattern[..pattern.len() - 1])
    }

    #[test]
    fn deliver_patterns_matches_and_renders_pmessage() {
        let mut t = ShardPubSub::default();
        let (tx, mut rx) = mpsc::channel::<ServerPush>(4);
        t.subscribe_pattern(Bytes::from_static(b"news.*"), 3, tx);
        let payload = Bytes::from_static(b"hello");
        // A matching channel delivers exactly one pmessage; a non-matching one delivers zero.
        assert_eq!(t.deliver_patterns(b"news.tech", &payload, star_suffix), 1);
        assert_eq!(t.deliver_patterns(b"weather", &payload, star_suffix), 0);
        let got = rx.try_recv().unwrap();
        assert_eq!(
            got,
            ServerPush::PMessage {
                pattern: Bytes::from_static(b"news.*"),
                channel: Bytes::from_static(b"news.tech"),
                payload: Bytes::from_static(b"hello"),
            }
        );
    }

    #[test]
    fn deliver_patterns_one_per_matching_pattern() {
        // A connection subscribed to TWO patterns that both match one channel gets a pmessage
        // PER pattern (Redis: no dedup across patterns).
        let mut t = ShardPubSub::default();
        let (tx, mut rx) = mpsc::channel::<ServerPush>(8);
        t.subscribe_pattern(Bytes::from_static(b"news.*"), 3, tx.clone());
        t.subscribe_pattern(Bytes::from_static(b"n*"), 3, tx);
        let payload = Bytes::from_static(b"x");
        assert_eq!(t.deliver_patterns(b"news.tech", &payload, star_suffix), 2);
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn deliver_patterns_sheds_full_consumer_and_prunes() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(1);
        t.subscribe_pattern(Bytes::from_static(b"p*"), 9, tx);
        let payload = Bytes::from_static(b"a");
        // First delivery fits the bound.
        assert_eq!(t.deliver_patterns(b"px", &payload, star_suffix), 1);
        // Channel is full now -> the next delivery sheds conn 9 and prunes the empty pattern.
        assert_eq!(t.deliver_patterns(b"px", &payload, star_suffix), 0);
        assert!(!t.patterns.contains_key(b"p*".as_slice()));
    }

    #[test]
    fn unsubscribe_pattern_prunes_empty_patterns() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(4);
        t.subscribe_pattern(Bytes::from_static(b"p*"), 1, tx);
        assert!(t.patterns.contains_key(b"p*".as_slice()));
        t.unsubscribe_pattern(b"p*", 1);
        assert!(!t.patterns.contains_key(b"p*".as_slice()));
    }

    #[test]
    fn local_introspection_channels_numsub_patterns() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(4);
        t.subscribe(Bytes::from_static(b"news.tech"), 1, tx.clone());
        t.subscribe(Bytes::from_static(b"news.tech"), 2, tx.clone());
        t.subscribe(Bytes::from_static(b"weather"), 1, tx.clone());
        t.subscribe_pattern(Bytes::from_static(b"news.*"), 1, tx);

        // CHANNELS unfiltered: both channels (order-independent).
        let mut chans = t.local_channels(None, star_suffix);
        chans.sort();
        assert_eq!(
            chans,
            vec![
                Bytes::from_static(b"news.tech"),
                Bytes::from_static(b"weather"),
            ]
        );
        // CHANNELS filtered by `news.*`: only the matching channel.
        let filtered = t.local_channels(Some(b"news.*"), star_suffix);
        assert_eq!(filtered, vec![Bytes::from_static(b"news.tech")]);
        // NUMSUB: 2 for news.tech, 0 for an unsubscribed channel.
        assert_eq!(t.local_numsub(b"news.tech"), 2);
        assert_eq!(t.local_numsub(b"absent"), 0);
        // PATTERNS: the single pattern with a subscriber.
        assert_eq!(t.local_patterns(), vec![Bytes::from_static(b"news.*")]);
    }
}
