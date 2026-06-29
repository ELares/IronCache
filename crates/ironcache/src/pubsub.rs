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
//! is dropped) AND its per-connection shed signal ([`Subscriber::shed`]) is flipped, so its
//! serve loop's idle wait observes the kill flag and CLOSES the connection (it does not rely
//! on `push_rx` returning `None`: the serve loop holds its OWN `push_tx` clone, which would
//! keep the channel open). Disconnecting the slow consumer keeps shard memory bounded
//! (SERVER_PUSH.md "a slow pubsub consumer over its hard limit is disconnected").

use bytes::Bytes;
use ironcache_protocol::{ProtoVersion, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Notify, mpsc};

/// The per-connection SHED/kill signal (SERVER_PUSH.md #20, FIX D): a one-shot latch the
/// publisher trips when it sheds a slow consumer (its bounded push channel overflowed past
/// [`PUSH_CHANNEL_BOUND`]). The consumer's serve-loop idle wait holds a clone (registered into
/// the shard table via [`Subscriber::shed`]) and CLOSES the connection once tripped. This is
/// necessary because the serve loop holds its OWN `push_tx` clone, so `push_rx.recv()` would NOT
/// return `None` on a shed alone -- the latch is the disconnect trigger.
///
/// It pairs an [`AtomicBool`] (the observable, idempotent latch state, so the table side / a
/// pre-check / a test can read whether the connection was shed) with a [`Notify`] (a SPIN-FREE
/// wake, so the idle-wait `select!` arm `await`s rather than busy-polling). [`Self::trip`] sets
/// the flag and wakes the waiter; [`Self::wait`] resolves once tripped (immediately if already
/// tripped, via the permit `Notify::notify_one` leaves). Shared cross-core (the publisher runs on
/// any shard), so it lives behind an [`Arc`].
#[derive(Debug, Default)]
pub struct ShedSignal {
    flagged: AtomicBool,
    wake: Notify,
}

impl ShedSignal {
    /// Trip the latch: mark it shed and WAKE any waiter (the consumer's idle-wait shed arm).
    /// Idempotent -- a second trip is a no-op latch-wise and just re-wakes (harmless). Called by
    /// the publisher inside [`ShardPubSub::deliver`] / [`ShardPubSub::deliver_patterns`] on a
    /// `Full`/`Closed` send.
    pub fn trip(&self) {
        self.flagged.store(true, Ordering::Relaxed);
        // Wake the (single) waiter; `notify_one` leaves a permit if no waiter is parked yet, so a
        // `wait()` that starts AFTER the trip still returns immediately (no lost wakeup).
        self.wake.notify_one();
    }

    /// Whether the latch is tripped (the connection was shed). A relaxed load: the latch is a
    /// one-shot monotonic signal; no ordering with other state is needed to decide to disconnect.
    #[must_use]
    pub fn is_tripped(&self) -> bool {
        self.flagged.load(Ordering::Relaxed)
    }

    /// Resolve once the latch is tripped, WITHOUT busy-spinning (the idle-wait `select!` arm).
    /// Returns immediately if already tripped (re-checking the flag after registering interest,
    /// so a trip that races the registration is not missed); otherwise parks on the [`Notify`]
    /// until [`Self::trip`] wakes it.
    pub async fn wait(&self) {
        loop {
            if self.flagged.load(Ordering::Relaxed) {
                return;
            }
            // Register interest BEFORE the re-check so a trip between the check above and the
            // await below leaves a permit that makes `notified()` return at once (no lost wakeup).
            let notified = self.wake.notified();
            if self.flagged.load(Ordering::Relaxed) {
                return;
            }
            notified.await;
        }
    }
}

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
    /// A SHARDED Pub/Sub message (#410): `channel` + `payload`, delivered to SSUBSCRIBE
    /// subscribers of a SHARD channel. Distinct from [`ServerPush::Message`] so a client can
    /// tell a sharded delivery (`smessage`) apart from a regular one (`message`); the shard
    /// channel namespace is separate from the regular channel namespace (an SPUBLISH never
    /// reaches a SUBSCRIBE subscriber and vice versa).
    SMessage {
        /// The shard channel the message was published to.
        channel: Bytes,
        /// The published payload.
        payload: Bytes,
    },
    /// A CLIENT TRACKING invalidation (#409, server-assisted client-side caching): the
    /// `["invalidate", [key ...]]` RESP3 push that tells a tracking client to drop its local
    /// cache of those keys. `keys = None` is the FLUSH form (`["invalidate", nil]`), telling the
    /// client to drop EVERYTHING (FLUSHALL/FLUSHDB). Delivered on the same per-connection push
    /// channel Pub/Sub uses; for a RESP2 `REDIRECT` target it rides the `__redis__:invalidate`
    /// channel (a later stage).
    Invalidate {
        /// The invalidated keys, or `None` for a flush-everything invalidation.
        keys: Option<Vec<Bytes>>,
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
            // Sharded delivery (#410): the canonical Redis shape `["smessage", channel, payload]`.
            ServerPush::SMessage { channel, payload } => Value::Push(vec![
                Value::bulk_str("smessage"),
                Value::bulk(channel.clone()),
                Value::bulk(payload.clone()),
            ]),
            // Tracking invalidation (#409): `["invalidate", [key ...]]`, or `["invalidate", nil]`
            // for a flush (drop everything). The second element is an ARRAY of keys (even one),
            // matching Redis's `trackingSendInvalidationMessages`.
            ServerPush::Invalidate { keys } => Value::Push(vec![
                Value::bulk_str("invalidate"),
                match keys {
                    Some(ks) => Value::Array(Some(ks.iter().cloned().map(Value::bulk).collect())),
                    None => Value::Null,
                },
            ]),
        }
    }
}

/// A single LOCAL subscriber's delivery handles in the per-shard table (SERVER_PUSH.md #20).
/// Bundles the `Send` push `sender` with a per-connection `shed` kill-signal so the publisher,
/// when it SHEDS a slow consumer (its bounded push channel is `Full`), can both drop the table
/// entry AND trip the signal the consumer's serve loop awaits, so the shed connection is actively
/// DISCONNECTED rather than left as a zombie (FIX D). Both halves are `Send` so the subscriber
/// struct crosses the publishing-shard -> consumer-core boundary.
#[derive(Debug, Clone)]
pub struct Subscriber {
    /// The connection's bounded push channel sender. A push is `try_send`'d here (never
    /// blocking); a `Full`/`Closed` result sheds the subscriber.
    pub sender: mpsc::Sender<ServerPush>,
    /// The connection's shared shed/kill signal ([`ShedSignal`]). Tripped by
    /// [`ShardPubSub::deliver`] / [`ShardPubSub::deliver_patterns`] when this subscriber is shed
    /// for back-pressure (`Full`); the consumer's serve-loop idle wait also holds a clone and
    /// CLOSES the connection when it observes the trip (spin-free, via the signal's `Notify`).
    /// Shared cross-core (the connection lives on one core, but the publisher may run on another).
    pub shed: Arc<ShedSignal>,
}

/// The PER-SHARD subscription table (SERVER_PUSH.md routing tables). Maps a channel (and a
/// PSUBSCRIBE glob pattern, PR 91b) to the set of LOCAL subscriber connections by connection
/// id, each carrying that connection's delivery handles ([`Subscriber`]: the `Send` push
/// sender + the shared shed signal).
///
/// Core-local (a thread-local in [`crate::serve`]); NO lock (ADR-0002 shared-nothing). The
/// only cross-core handles stored are the `Send` [`mpsc::Sender<ServerPush>`] and the shed
/// `Arc<AtomicBool>`: a PUBLISH that fans out to every shard reaches each shard's own
/// `ShardPubSub` (via the cross-shard coordinator), so each shard renders to ITS connections
/// from ITS table with no shared lock. The `patterns` map holds PSUBSCRIBE subscriptions; one
/// PUBLISH delivers to BOTH the exact `channels` entry AND every matching pattern (no dedup,
/// Redis semantics).
#[derive(Debug, Default)]
pub struct ShardPubSub {
    /// channel -> {conn id -> subscriber}. A channel with no subscribers is absent (the
    /// last UNSUBSCRIBE / disconnect removes the empty inner map).
    pub channels: HashMap<Bytes, HashMap<u64, Subscriber>>,
    /// pattern -> {conn id -> subscriber} (PSUBSCRIBE, PR 91b). A pattern with no subscribers
    /// is absent (the last PUNSUBSCRIBE / disconnect removes the empty inner map). A PUBLISH
    /// iterates these and `glob_match`es each pattern against the published channel.
    pub patterns: HashMap<Bytes, HashMap<u64, Subscriber>>,
    /// shard channel -> {conn id -> subscriber} (SSUBSCRIBE, #410). A SEPARATE namespace from
    /// `channels`: an SPUBLISH delivers ONLY to this table, a PUBLISH only to `channels`, so the
    /// two never cross. Sharded Pub/Sub has NO pattern form (Redis: no PSSUBSCRIBE), so there is
    /// no shard analog of `patterns`. A shard channel with no subscribers is absent.
    pub shard_channels: HashMap<Bytes, HashMap<u64, Subscriber>>,
}

impl ShardPubSub {
    /// Register `conn_id`'s `subscriber` (push sender + shed signal) as a subscriber of
    /// `channel` on THIS shard. A re-subscribe (same conn id, same channel) overwrites the
    /// stale entry with the current one (idempotent on the table; the caller decides whether
    /// the running count bumps, matching Redis's "already subscribed does not bump" rule).
    pub fn subscribe(&mut self, channel: Bytes, conn_id: u64, subscriber: Subscriber) {
        self.channels
            .entry(channel)
            .or_default()
            .insert(conn_id, subscriber);
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

    /// Register `conn_id`'s `subscriber` (push sender + shed signal) as a subscriber of
    /// `pattern` on THIS shard (the PSUBSCRIBE analog of [`Self::subscribe`], PR 91b). A
    /// re-subscribe (same conn id, same pattern) overwrites the stale entry with the current
    /// one (idempotent on the table; the caller decides whether the running count bumps,
    /// matching Redis's "already subscribed does not bump" rule).
    pub fn subscribe_pattern(&mut self, pattern: Bytes, conn_id: u64, subscriber: Subscriber) {
        self.patterns
            .entry(pattern)
            .or_default()
            .insert(conn_id, subscriber);
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
            for (&conn_id, sub) in subs.iter() {
                match sub.sender.try_send(push.clone()) {
                    Ok(()) => delivered += 1,
                    Err(
                        mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_),
                    ) => {
                        // Trip the per-connection shed signal so the consumer's serve loop
                        // observes it and CLOSES the connection (FIX D), then drop the table
                        // entry. Tripping for a Closed sender is harmless (the serve loop is
                        // already tearing down).
                        sub.shed.trip();
                        shed.push(conn_id);
                    }
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
    /// SHED here -- removed from the table AND its [`Subscriber::shed`] signal is flipped so
    /// its serve loop's idle wait disconnects it (FIX D; the serve loop holds its own push
    /// sender clone, so dropping the table entry alone would NOT close `push_rx`), keeping
    /// shard memory bounded. A shed subscriber is NOT counted as a receiver (it did not
    /// receive this message).
    pub fn deliver(&mut self, channel: &[u8], push: &ServerPush) -> i64 {
        let Some(subs) = self.channels.get_mut(channel) else {
            return 0;
        };
        let mut delivered: i64 = 0;
        // Collect the conn ids to shed; we cannot remove while iterating the same map.
        let mut shed: Vec<u64> = Vec::new();
        for (&conn_id, sub) in subs.iter() {
            match sub.sender.try_send(push.clone()) {
                Ok(()) => delivered += 1,
                // Full: a slow consumer past the bound -> SHED. Flip its shed signal (FIX D) so
                // its serve loop's idle-wait CLOSES the connection, then drop the table entry.
                // Closed: the receiver is already gone (the connection dropped push_rx but its
                // disconnect cleanup has not pruned the table yet) -> drop it here too (setting
                // the flag is harmless, the serve loop is already tearing down). Both outcomes
                // shed the subscriber.
                Err(mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_)) => {
                    sub.shed.trip();
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

    // ------------------------------------------------------------------------
    // Sharded Pub/Sub (#410): the SHARD-channel namespace. Mirrors the regular-channel
    // methods above but over `shard_channels`, so SSUBSCRIBE/SPUBLISH never cross SUBSCRIBE/
    // PUBLISH. There is no pattern form (Redis has no PSSUBSCRIBE).
    // ------------------------------------------------------------------------

    /// Register `conn_id`'s `subscriber` as an SSUBSCRIBE subscriber of the SHARD `channel` on
    /// THIS shard (the sharded analog of [`Self::subscribe`]). Idempotent on the table.
    pub fn subscribe_shard(&mut self, channel: Bytes, conn_id: u64, subscriber: Subscriber) {
        self.shard_channels
            .entry(channel)
            .or_default()
            .insert(conn_id, subscriber);
    }

    /// Deregister `conn_id` from the SHARD `channel` on THIS shard, pruning the empty entry
    /// (the sharded analog of [`Self::unsubscribe`]).
    pub fn unsubscribe_shard(&mut self, channel: &[u8], conn_id: u64) {
        if let Some(subs) = self.shard_channels.get_mut(channel) {
            subs.remove(&conn_id);
            if subs.is_empty() {
                self.shard_channels.remove(channel);
            }
        }
    }

    /// Deliver `push` to every LOCAL SSUBSCRIBE subscriber of the SHARD `channel` on THIS shard,
    /// returning the count delivered (the sharded analog of [`Self::deliver`], same non-blocking
    /// `try_send` + slow-consumer SHED). Used by the SPUBLISH fan-out.
    pub fn deliver_shard(&mut self, channel: &[u8], push: &ServerPush) -> i64 {
        let Some(subs) = self.shard_channels.get_mut(channel) else {
            return 0;
        };
        let mut delivered: i64 = 0;
        let mut shed: Vec<u64> = Vec::new();
        for (&conn_id, sub) in subs.iter() {
            match sub.sender.try_send(push.clone()) {
                Ok(()) => delivered += 1,
                Err(mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_)) => {
                    sub.shed.trip();
                    shed.push(conn_id);
                }
            }
        }
        for conn_id in shed {
            subs.remove(&conn_id);
        }
        if subs.is_empty() {
            self.shard_channels.remove(channel);
        }
        delivered
    }

    /// The LOCAL shard-channel names with at least one subscriber on THIS shard (PUBSUB
    /// SHARDCHANNELS, #410), optionally filtered by `pat`. The home core UNIONS + dedups across
    /// shards. The sharded analog of [`Self::local_channels`].
    #[must_use]
    pub fn local_shard_channels(
        &self,
        pat: Option<&[u8]>,
        glob: impl Fn(&[u8], &[u8]) -> bool,
    ) -> Vec<Bytes> {
        self.shard_channels
            .keys()
            .filter(|ch| pat.is_none_or(|p| glob(p, ch.as_ref())))
            .cloned()
            .collect()
    }

    /// The LOCAL subscriber count of the SHARD `channel` on THIS shard (PUBSUB SHARDNUMSUB,
    /// #410). The home core SUMS these per channel across shards. The sharded analog of
    /// [`Self::local_numsub`].
    #[must_use]
    pub fn local_shard_numsub(&self, channel: &[u8]) -> i64 {
        self.shard_channels
            .get(channel)
            .map_or(0, |subs| i64::try_from(subs.len()).unwrap_or(i64::MAX))
    }
}

/// The per-shard CLIENT TRACKING invalidation table (#409, server-assisted client-side caching):
/// which connections READ each key and want an `invalidate` push when it changes. Core-local (a
/// serve thread-local), NO lock, exactly like [`ShardPubSub`]. The [`Subscriber`] is the SAME
/// `Send` push handle Pub/Sub stores, so a writer on the key's OWNER shard pushes the invalidation
/// to the tracking client's connection wherever that connection lives.
///
/// Both halves live on the key's OWNER shard: a tracking client's READ routes to the key's owner
/// and registers there ([`Self::track`]); a WRITE to that key runs on the SAME owner shard and
/// invalidates there ([`Self::invalidate`]). So no cross-shard table coordination is needed.
///
/// Default-mode this stage: `tracked` maps key -> {client_id -> Subscriber}. An invalidation is
/// ONE-SHOT (Redis `trackingInvalidateKey`): sending it DROPS the key's entries, so a client must
/// re-read to be re-tracked. BCAST (prefix) tracking is a later stage.
#[derive(Debug, Default)]
pub struct ShardTracking {
    /// DEFAULT mode (per-read keys): key -> {conn -> handle}. One-shot (an invalidation drops it).
    tracked: HashMap<Bytes, HashMap<u64, Subscriber>>,
    /// BCAST mode (#409 stage 2): prefix -> {conn -> handle}. STICKY (never dropped on invalidate);
    /// a changed key invalidates every prefix it starts with. The EMPTY prefix matches every key.
    prefixes: HashMap<Bytes, HashMap<u64, Subscriber>>,
}

/// Deliver an invalidation `push` to each subscriber in `subs` except `skip`, returning the count
/// delivered; a Full/Closed sender is shed (its kill signal tripped). Shared by the default and
/// BCAST invalidation paths so the back-pressure handling cannot drift.
fn deliver_invalidations(
    subs: &HashMap<u64, Subscriber>,
    push: &ServerPush,
    skip: Option<u64>,
) -> i64 {
    let mut delivered: i64 = 0;
    for (conn_id, sub) in subs {
        if Some(*conn_id) == skip {
            continue;
        }
        match sub.sender.try_send(push.clone()) {
            Ok(()) => delivered += 1,
            Err(mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_)) => {
                sub.shed.trip();
            }
        }
    }
    delivered
}

impl ShardTracking {
    /// Register `conn_id` (with its push handle) as a DEFAULT-mode tracker of `key` on THIS shard:
    /// it READ the key and wants an invalidation when the key changes. Idempotent. The table is
    /// empty until a tracking client reads, so the non-tracking hot path never touches it.
    pub fn track(&mut self, key: Bytes, conn_id: u64, subscriber: Subscriber) {
        self.tracked
            .entry(key)
            .or_default()
            .insert(conn_id, subscriber);
    }

    /// Register `conn_id` as a BCAST tracker of `prefix` (#409 stage 2): every changed key starting
    /// with `prefix` invalidates it (sticky). The EMPTY prefix tracks ALL keys. Idempotent.
    pub fn track_prefix(&mut self, prefix: Bytes, conn_id: u64, subscriber: Subscriber) {
        self.prefixes
            .entry(prefix)
            .or_default()
            .insert(conn_id, subscriber);
    }

    /// Whether NO trackers exist in EITHER table (the common case): the write path skips the
    /// invalidation pass on this single check when nobody is tracking.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tracked.is_empty() && self.prefixes.is_empty()
    }

    /// Invalidate `key`: push `["invalidate", [key]]` to every tracking connection except `skip`
    /// (NOLOOP). DEFAULT-mode trackers of `key` are notified and DROPPED (one-shot, Redis); BCAST
    /// trackers whose prefix is a prefix of `key` are notified but KEPT (sticky). Returns the count
    /// delivered. The BCAST scan is O(distinct prefixes) (a radix tree is the documented refinement).
    pub fn invalidate(&mut self, key: &[u8], skip: Option<u64>) -> i64 {
        let push = ServerPush::Invalidate {
            keys: Some(vec![Bytes::copy_from_slice(key)]),
        };
        let mut delivered: i64 = 0;
        // DEFAULT mode: one-shot, remove the key's set.
        if let Some(subs) = self.tracked.remove(key) {
            delivered += deliver_invalidations(&subs, &push, skip);
        }
        // BCAST mode: sticky, every prefix that `key` starts with (the empty prefix matches all).
        for (prefix, subs) in &self.prefixes {
            if key.starts_with(prefix.as_ref()) {
                delivered += deliver_invalidations(subs, &push, skip);
            }
        }
        delivered
    }

    /// Invalidate EVERYTHING (FLUSHALL/FLUSHDB): send the flush form `["invalidate", nil]` ONCE to
    /// every DISTINCT tracking connection (DEFAULT + BCAST) except `skip`, then clear the per-key
    /// table. BCAST prefix subscriptions are KEPT (sticky: the client stays subscribed for future
    /// keys), but its cache is flushed by the nil push.
    pub fn invalidate_all(&mut self, skip: Option<u64>) {
        let mut conns: HashMap<u64, Subscriber> = HashMap::new();
        for subs in self.tracked.values().chain(self.prefixes.values()) {
            for (id, sub) in subs {
                conns.entry(*id).or_insert_with(|| sub.clone());
            }
        }
        let push = ServerPush::Invalidate { keys: None };
        deliver_invalidations(&conns, &push, skip);
        self.tracked.clear();
    }

    /// Remove `conn_id` from EVERY default key set AND every BCAST prefix set (disconnect / `CLIENT
    /// TRACKING OFF` / RESET / leaving BCAST), pruning emptied entries. O(tracked keys + prefixes).
    pub fn forget_conn(&mut self, conn_id: u64) {
        self.tracked.retain(|_, subs| {
            subs.remove(&conn_id);
            !subs.is_empty()
        });
        self.prefixes.retain(|_, subs| {
            subs.remove(&conn_id);
            !subs.is_empty()
        });
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

    /// Wrap a bare push sender into a [`Subscriber`] with a fresh shed signal (the tests below
    /// that do not inspect the signal use this; the shed tests build the signal explicitly).
    fn sub(sender: mpsc::Sender<ServerPush>) -> Subscriber {
        Subscriber {
            sender,
            shed: Arc::new(ShedSignal::default()),
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
        let shed = Arc::new(ShedSignal::default());
        t.subscribe(
            Bytes::from_static(b"ch"),
            7,
            Subscriber {
                sender: tx,
                shed: Arc::clone(&shed),
            },
        );
        // First deliver fits (count 1).
        assert_eq!(t.deliver(b"ch", &msg(b"ch", b"a")), 1);
        // Channel now holds 1 (the bound); the next deliver finds it FULL and sheds conn 7,
        // returning 0 delivered.
        assert_eq!(t.deliver(b"ch", &msg(b"ch", b"b")), 0);
        // Shed: the channel entry is pruned (no subscribers left).
        assert!(!t.channels.contains_key(b"ch".as_slice()));
        // FIX D: the per-connection shed signal is tripped, so the consumer's serve loop closes.
        assert!(shed.is_tripped(), "shed signal is tripped on Full");
        // The one queued message is still readable (shedding only drops the SENDER).
        assert_eq!(rx.try_recv().unwrap(), msg(b"ch", b"a"));
    }

    #[test]
    fn unsubscribe_prunes_empty_channels() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(4);
        t.subscribe(Bytes::from_static(b"ch"), 1, sub(tx));
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
        t.subscribe_pattern(Bytes::from_static(b"news.*"), 3, sub(tx));
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
        t.subscribe_pattern(Bytes::from_static(b"news.*"), 3, sub(tx.clone()));
        t.subscribe_pattern(Bytes::from_static(b"n*"), 3, sub(tx));
        let payload = Bytes::from_static(b"x");
        assert_eq!(t.deliver_patterns(b"news.tech", &payload, star_suffix), 2);
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn deliver_patterns_sheds_full_consumer_and_prunes() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(1);
        let shed = Arc::new(ShedSignal::default());
        t.subscribe_pattern(
            Bytes::from_static(b"p*"),
            9,
            Subscriber {
                sender: tx,
                shed: Arc::clone(&shed),
            },
        );
        let payload = Bytes::from_static(b"a");
        // First delivery fits the bound.
        assert_eq!(t.deliver_patterns(b"px", &payload, star_suffix), 1);
        // Channel is full now -> the next delivery sheds conn 9 and prunes the empty pattern.
        assert_eq!(t.deliver_patterns(b"px", &payload, star_suffix), 0);
        assert!(!t.patterns.contains_key(b"p*".as_slice()));
        // FIX D: the per-connection shed signal is tripped on the pattern shed path too.
        assert!(shed.is_tripped(), "pattern shed signal is tripped on Full");
    }

    #[test]
    fn unsubscribe_pattern_prunes_empty_patterns() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(4);
        t.subscribe_pattern(Bytes::from_static(b"p*"), 1, sub(tx));
        assert!(t.patterns.contains_key(b"p*".as_slice()));
        t.unsubscribe_pattern(b"p*", 1);
        assert!(!t.patterns.contains_key(b"p*".as_slice()));
    }

    #[test]
    fn local_introspection_channels_numsub_patterns() {
        let mut t = ShardPubSub::default();
        let (tx, _rx) = mpsc::channel::<ServerPush>(4);
        t.subscribe(Bytes::from_static(b"news.tech"), 1, sub(tx.clone()));
        t.subscribe(Bytes::from_static(b"news.tech"), 2, sub(tx.clone()));
        t.subscribe(Bytes::from_static(b"weather"), 1, sub(tx.clone()));
        t.subscribe_pattern(Bytes::from_static(b"news.*"), 1, sub(tx));

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
