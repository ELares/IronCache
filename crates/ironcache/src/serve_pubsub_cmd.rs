// SPDX-License-Identifier: MIT OR Apache-2.0
//! Serve-layer Pub/Sub command handlers split out of `serve.rs` (#625, SERVER_PUSH.md #20). These
//! live in the SERVE layer (not `dispatch_inner`) because registration needs the per-connection push
//! SENDER + the per-shard subscription table (`shard_pubsub()`, a serve thread-local): SUBSCRIBE /
//! UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE / the #410 sharded trio / subscribed PING / PUBLISH /
//! PUBSUB, plus the close-path subscription deregistration. Behavior-preserving relocation: the
//! bodies are byte-identical to their former in-`serve.rs` definitions.

use super::{ShardState, ascii_upper, encode_into, purge_conn_tracking, shard_pubsub};
use crate::coordinator;
use ironcache_runtime::bootstrap::ShardId;
use ironcache_server::{ConnState, ProtoVersion, Request};
use std::cell::RefCell;
use std::rc::Rc;

// -- Pub/Sub serve-layer handlers (SERVER_PUSH.md #20, PR 91a). These live in the SERVE layer
// (not `dispatch_inner`) because registration needs the per-connection push SENDER (`push_tx`,
// a tokio handle the server crate has no dependency for) and the per-shard subscription table
// (`shard_pubsub()`, a serve thread-local). SUBSCRIBE/UNSUBSCRIBE are HOME-LOCAL (the
// connection's subscriptions live on its home shard); PUBLISH fans out via the coordinator.

/// Intercept and handle the SERVE-LAYER pub/sub commands (SERVER_PUSH.md #20, PR 91a/91b),
/// returning `Some(close)` when `cmd_upper` is one of them (always `false`: a pub/sub command
/// never closes the connection) and `None` when it is NOT a pub/sub command (the caller falls
/// through to the normal routing + dispatch). Split out of [`route_and_dispatch`] so the router
/// stays small.
///
/// `commands_processed` is bumped here for every handled command (matching every other reply
/// path's single count). SUBSCRIBE / PSUBSCRIBE / PUBLISH validate arity inline (the registry
/// arity, mirroring the dispatch arity path); UNSUBSCRIBE / PUNSUBSCRIBE accept zero args
/// (unsubscribe-all); PUBSUB validates its subcommand inline. PING is intercepted ONLY when the
/// connection is a RESP2 subscriber (the `["pong", ...]` array shape); a non-subscriber / RESP3
/// PING returns `None` so the normal `cmd_ping` arm handles it unchanged.
// `too_many_lines` allowed: this is the pub/sub command DISPATCH (one arm per SUBSCRIBE /
// UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE / PUBLISH / PUBSUB / the #410 sharded trio / subscribed
// PING), each a thin arity-check + handler call. Splitting it would scatter the single
// pub/sub interception point that mirrors the serve router's one entry.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn try_handle_pubsub(
    conn: &mut ConnState,
    home: ShardId,
    inbox: &coordinator::Inbox,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    state_rc: &Rc<RefCell<ShardState>>,
    cmd_upper: &[u8],
    request: &Request,
    out: &mut Vec<u8>,
) -> Option<bool> {
    match cmd_upper {
        b"SUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            // Arity (>= 2) is the registry's; a bare SUBSCRIBE with no channel is a wrong-arity
            // error, mirroring the dispatch arity path for the other serve-routed commands.
            if request.args.len() >= 2 {
                handle_subscribe(conn, push_tx, shed_flag, request, out);
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "subscribe",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"UNSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            handle_unsubscribe(conn, request, out);
            Some(false)
        }
        b"PSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            // Arity (>= 2) is the registry's; a bare PSUBSCRIBE with no pattern is a
            // wrong-arity error, mirroring SUBSCRIBE's inline arity path.
            if request.args.len() >= 2 {
                handle_psubscribe(conn, push_tx, shed_flag, request, out);
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "psubscribe",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"PUNSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            handle_punsubscribe(conn, request, out);
            Some(false)
        }
        b"PUBSUB" => {
            state_rc.borrow_mut().counters.on_command();
            // PUBSUB <subcommand> [args]: a cross-shard introspection GATHER (CHANNELS /
            // NUMSUB / NUMPAT). Like PUBLISH it lives in the serve layer (it reads the
            // per-shard subscription tables) and fans out via the coordinator's inbox.
            handle_pubsub(conn, inbox, home, request, out).await;
            Some(false)
        }
        b"PUBLISH" => {
            state_rc.borrow_mut().counters.on_command();
            if request.args.len() == 3 {
                handle_publish(conn, inbox, home, request, out).await;
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "publish",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        // -- Sharded Pub/Sub (#410): the SSUBSCRIBE / SUNSUBSCRIBE / SPUBLISH analogs of
        // SUBSCRIBE / UNSUBSCRIBE / PUBLISH, over the separate SHARD-channel namespace. --
        b"SSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            if request.args.len() >= 2 {
                handle_ssubscribe(conn, push_tx, shed_flag, request, out);
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "ssubscribe",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"SUNSUBSCRIBE" => {
            state_rc.borrow_mut().counters.on_command();
            handle_sunsubscribe(conn, request, out);
            Some(false)
        }
        b"SPUBLISH" => {
            state_rc.borrow_mut().counters.on_command();
            if request.args.len() == 3 {
                handle_spublish(conn, inbox, home, request, out).await;
            } else {
                encode_into(
                    out,
                    &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity(
                        "spublish",
                    )),
                    conn.proto,
                );
            }
            Some(false)
        }
        b"PING" if conn.is_subscriber() && conn.proto == ProtoVersion::Resp2 => {
            // PING while subscribed (RESP2): the `["pong", <arg>]` array shape, NOT `+PONG`. Bump
            // commands_processed like the dispatch path would, then encode the array. PING arity
            // is Min(1); a >2-arg PING is a wrong-arity error (Redis), matching `cmd_ping`.
            state_rc.borrow_mut().counters.on_command();
            let reply = if request.args.len() <= 2 {
                ping_subscribed_reply(request)
            } else {
                ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity("ping"))
            };
            encode_into(out, &reply, conn.proto);
            Some(false)
        }
        _ => None,
    }
}

/// Append a `["subscribe", channel, count]` confirmation (one per channel; SERVER_PUSH.md). It
/// is rendered through [`ironcache_protocol::Value::Push`], so the encoder writes RESP3 `>` /
/// RESP2 `*` from the connection proto (ADR-0019), matching Redis's subscribe-confirmation shape.
fn push_confirm(kind: &str, channel: &[u8], count: i64) -> ironcache_server::Value {
    ironcache_server::Value::Push(vec![
        ironcache_server::Value::bulk_str(kind),
        ironcache_server::Value::bulk(bytes::Bytes::copy_from_slice(channel)),
        ironcache_server::Value::Integer(count),
    ])
}

/// The running subscription count for a connection (`channels + patterns`), the integer in
/// each subscribe/unsubscribe confirmation (Redis reports the TOTAL of both, post-mutation).
fn running_count(conn: &ConnState) -> i64 {
    i64::try_from(conn.sub_channels.len() + conn.sub_patterns.len()).unwrap_or(i64::MAX)
}

/// `SUBSCRIBE channel [channel ...]` (SERVER_PUSH.md #20, PR 91a). For EACH channel: insert it
/// into `conn.sub_channels` and register `(channel, conn.id, push_tx.clone())` into THIS shard's
/// subscription table, then append a `["subscribe", channel, running_count]` confirmation. The
/// running count is `sub_channels.len() + sub_patterns.len()` AFTER the insert; a re-subscribe to
/// an already-subscribed channel does NOT bump the count (the `HashSet`/table inserts are
/// idempotent), matching Redis. One confirmation message per channel argument, in order.
fn handle_subscribe(
    conn: &mut ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let pubsub = shard_pubsub();
    for channel in &request.args[1..] {
        conn.sub_channels.insert(channel.clone());
        pubsub.borrow_mut().subscribe(
            channel.clone(),
            conn.id,
            crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
        );
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("subscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `UNSUBSCRIBE [channel ...]` (SERVER_PUSH.md #20, PR 91a). With channel args, unsubscribe each
/// named channel; with NO args, unsubscribe ALL currently-subscribed channels. Reply one
/// `["unsubscribe", channel, running_count]` per AFFECTED channel; the no-args-and-none-subscribed
/// edge replies a single `["unsubscribe", nil, 0]` (matching Redis). Deregister each from THIS
/// shard's subscription table (the connection's subscriptions are home-shard-local).
fn handle_unsubscribe(conn: &mut ConnState, request: &Request, out: &mut Vec<u8>) {
    let pubsub = shard_pubsub();
    // The channels to drop: the named args, or ALL currently-subscribed when none are named.
    let targets: Vec<bytes::Bytes> = if request.args.len() > 1 {
        request.args[1..].to_vec()
    } else {
        conn.sub_channels.iter().cloned().collect()
    };

    if targets.is_empty() {
        // No args AND nothing subscribed: Redis replies a single nil-channel confirmation.
        encode_into(
            out,
            &ironcache_server::Value::Push(vec![
                ironcache_server::Value::bulk_str("unsubscribe"),
                ironcache_server::Value::Null,
                ironcache_server::Value::Integer(0),
            ]),
            conn.proto,
        );
        return;
    }

    for channel in targets {
        conn.sub_channels.remove(&channel);
        pubsub.borrow_mut().unsubscribe(channel.as_ref(), conn.id);
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("unsubscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// The running SHARD-channel subscription count (#410), the integer in each
/// ssubscribe/sunsubscribe confirmation. Redis reports the SHARD-channel count ONLY here (NOT
/// the channels+patterns total `running_count` uses), so a client tracks its sharded
/// subscriptions independently of its regular ones.
fn running_shard_count(conn: &ConnState) -> i64 {
    i64::try_from(conn.sub_shard_channels.len()).unwrap_or(i64::MAX)
}

/// `SSUBSCRIBE shardchannel [shardchannel ...]` (#410, the sharded analog of SUBSCRIBE). For EACH
/// channel: insert it into `conn.sub_shard_channels` and register the subscriber into THIS shard's
/// `shard_channels` table, then append a `["ssubscribe", channel, running_shard_count]`
/// confirmation. Idempotent (a re-subscribe does not bump the count), matching Redis. The
/// SHARD-channel namespace is separate from SUBSCRIBE's, so an SPUBLISH (not a PUBLISH) delivers
/// here.
fn handle_ssubscribe(
    conn: &mut ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let pubsub = shard_pubsub();
    for channel in &request.args[1..] {
        conn.sub_shard_channels.insert(channel.clone());
        pubsub.borrow_mut().subscribe_shard(
            channel.clone(),
            conn.id,
            crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
        );
        let count = running_shard_count(conn);
        encode_into(
            out,
            &push_confirm("ssubscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `SUNSUBSCRIBE [shardchannel ...]` (#410, the sharded analog of UNSUBSCRIBE). With args,
/// unsubscribe each named shard channel; with NO args, unsubscribe ALL currently-held shard
/// channels. Reply one `["sunsubscribe", channel, running_shard_count]` per affected channel; the
/// no-args-and-none-subscribed edge replies a single `["sunsubscribe", nil, 0]` (matching Redis).
fn handle_sunsubscribe(conn: &mut ConnState, request: &Request, out: &mut Vec<u8>) {
    let pubsub = shard_pubsub();
    let targets: Vec<bytes::Bytes> = if request.args.len() > 1 {
        request.args[1..].to_vec()
    } else {
        conn.sub_shard_channels.iter().cloned().collect()
    };

    if targets.is_empty() {
        encode_into(
            out,
            &ironcache_server::Value::Push(vec![
                ironcache_server::Value::bulk_str("sunsubscribe"),
                ironcache_server::Value::Null,
                ironcache_server::Value::Integer(0),
            ]),
            conn.proto,
        );
        return;
    }

    for channel in targets {
        conn.sub_shard_channels.remove(&channel);
        pubsub
            .borrow_mut()
            .unsubscribe_shard(channel.as_ref(), conn.id);
        let count = running_shard_count(conn);
        encode_into(
            out,
            &push_confirm("sunsubscribe", channel.as_ref(), count),
            conn.proto,
        );
    }
}

/// `SPUBLISH shardchannel message` (#410, the sharded analog of PUBLISH). Fan the message out to
/// every shard's LOCAL `shard_channels` table via the coordinator (node-local; an SPUBLISH never
/// reaches a SUBSCRIBE subscriber), replying the integer total receiver count.
async fn handle_spublish(
    conn: &ConnState,
    inbox: &coordinator::Inbox,
    home: ShardId,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let channel = request.args[1].clone();
    let payload = request.args[2].clone();
    let total = coordinator::fan_out_spublish(
        inbox,
        channel.as_ref(),
        payload.as_ref(),
        conn.db,
        home.index,
    )
    .await;
    encode_into(out, &ironcache_server::Value::Integer(total), conn.proto);
}

/// `PSUBSCRIBE pattern [pattern ...]` (SERVER_PUSH.md #20, PR 91b). For EACH pattern: insert it
/// into `conn.sub_patterns` and register `(pattern, conn.id, push_tx.clone())` into THIS shard's
/// subscription `patterns` table, then append a `["psubscribe", pattern, running_count]`
/// confirmation. The running count is `sub_channels.len() + sub_patterns.len()` AFTER the insert
/// (the TOTAL of channels + patterns, exactly as SUBSCRIBE); a re-subscribe to an already-held
/// pattern does NOT bump the count (the `HashSet` / table inserts are idempotent), matching
/// Redis. One confirmation message per pattern argument, in order.
fn handle_psubscribe(
    conn: &mut ConnState,
    push_tx: &tokio::sync::mpsc::Sender<crate::pubsub::ServerPush>,
    shed_flag: &std::sync::Arc<crate::pubsub::ShedSignal>,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let pubsub = shard_pubsub();
    for pattern in &request.args[1..] {
        conn.sub_patterns.insert(pattern.clone());
        pubsub.borrow_mut().subscribe_pattern(
            pattern.clone(),
            conn.id,
            crate::pubsub::Subscriber {
                sender: push_tx.clone(),
                shed: std::sync::Arc::clone(shed_flag),
            },
        );
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("psubscribe", pattern.as_ref(), count),
            conn.proto,
        );
    }
}

/// `PUNSUBSCRIBE [pattern ...]` (SERVER_PUSH.md #20, PR 91b). With pattern args, unsubscribe each
/// named pattern; with NO args, unsubscribe ALL currently-subscribed patterns. Reply one
/// `["punsubscribe", pattern, running_count]` per AFFECTED pattern; the no-args-and-none-subscribed
/// edge replies a single `["punsubscribe", nil, 0]` (matching Redis). Deregister each from THIS
/// shard's subscription `patterns` table (the connection's subscriptions are home-shard-local).
fn handle_punsubscribe(conn: &mut ConnState, request: &Request, out: &mut Vec<u8>) {
    let pubsub = shard_pubsub();
    // The patterns to drop: the named args, or ALL currently-subscribed when none are named.
    let targets: Vec<bytes::Bytes> = if request.args.len() > 1 {
        request.args[1..].to_vec()
    } else {
        conn.sub_patterns.iter().cloned().collect()
    };

    if targets.is_empty() {
        // No args AND nothing subscribed: Redis replies a single nil-pattern confirmation.
        encode_into(
            out,
            &ironcache_server::Value::Push(vec![
                ironcache_server::Value::bulk_str("punsubscribe"),
                ironcache_server::Value::Null,
                ironcache_server::Value::Integer(0),
            ]),
            conn.proto,
        );
        return;
    }

    for pattern in targets {
        conn.sub_patterns.remove(&pattern);
        pubsub
            .borrow_mut()
            .unsubscribe_pattern(pattern.as_ref(), conn.id);
        let count = running_count(conn);
        encode_into(
            out,
            &push_confirm("punsubscribe", pattern.as_ref(), count),
            conn.proto,
        );
    }
}

/// `PUBSUB CHANNELS [pattern] | NUMSUB [ch ...] | NUMPAT` (SERVER_PUSH.md #20, PR 91b) -- the
/// cross-shard introspection GATHER. Subscription state is PER-SHARD (a channel may have
/// subscribers on several shards), so each subcommand fans the SAME internal `__ICPUBSUB <sub>
/// [args]` request out to EVERY shard via [`coordinator::fan_out_pubsub`] (the home shard runs
/// it locally, peers via their drain loops) and MERGES the per-shard partials per subcommand:
/// CHANNELS unions+dedups the channel names, NUMSUB sums the per-channel counts, NUMPAT unions
/// the pattern names and counts the DISTINCT total. `commands_processed` was already bumped by
/// the caller.
///
/// Per-subcommand ARITY is validated here, byte-exact to Redis `pubsubCommand` (verified against
/// redis/redis src/pubsub.c): CHANNELS accepts `argc == 2 || argc == 3` (at MOST one pattern arg,
/// FIX H), NUMPAT accepts EXACTLY `argc == 2` (NO args, FIX H), NUMSUB accepts `argc >= 2` (any
/// number of channels). A bare `PUBSUB` (no subcommand) is a WRONG-ARITY error (FIX G; the
/// registry arity is min-2, and Redis returns wrong-arity for a missing subcommand). Every other
/// invalid case -- an unknown subcommand, OR a known subcommand with the wrong arg count -- is the
/// Redis `addReplySubcommandSyntaxError` (our [`ErrorReply::unknown_subcommand`], byte-identical:
/// `ERR unknown subcommand or wrong number of arguments for '<sub>'. Try PUBSUB HELP.`).
async fn handle_pubsub(
    conn: &ConnState,
    inbox: &coordinator::Inbox,
    home: ShardId,
    request: &Request,
    out: &mut Vec<u8>,
) {
    // FIX G: a bare `PUBSUB` (no subcommand) is WRONG-ARITY (not unknown-subcommand). The registry
    // arity is min-2; Redis rejects a missing subcommand with the wrong-arity error.
    let Some(sub_raw) = request.args.get(1) else {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::wrong_arity("pubsub")),
            conn.proto,
        );
        return;
    };
    let sub_upper = ascii_upper(sub_raw.as_ref());
    let argc = request.args.len();
    // Each known subcommand carries its own arg-count rule (FIX H), byte-exact to Redis
    // pubsubCommand. A present-but-unrecognized subcommand OR a recognized subcommand with a bad
    // arg count both fall to the same subcommand-syntax error (Redis's addReplySubcommandSyntaxError).
    let valid = match sub_upper.as_slice() {
        // CHANNELS [pattern] / the sharded SHARDCHANNELS [pattern] (#410): at most one pattern
        // -> argc 2 or 3.
        b"CHANNELS" | b"SHARDCHANNELS" => argc == 2 || argc == 3,
        // NUMSUB [channel ...] / the sharded SHARDNUMSUB [channel ...] (#410): any number of
        // channels -> argc >= 2 (no upper bound).
        b"NUMSUB" | b"SHARDNUMSUB" => argc >= 2,
        // NUMPAT: takes NO args -> argc exactly 2.
        b"NUMPAT" => argc == 2,
        _ => false,
    };
    if !valid {
        encode_into(
            out,
            &ironcache_server::Value::error(ironcache_protocol::ErrorReply::unknown_subcommand(
                "pubsub",
                &String::from_utf8_lossy(sub_raw.as_ref()),
            )),
            conn.proto,
        );
        return;
    }
    let merged = coordinator::fan_out_pubsub(inbox, request, home.index).await;
    encode_into(out, &merged, conn.proto);
}

/// `PUBLISH channel payload` (SERVER_PUSH.md #20, PR 91a) -> the total number of receivers across
/// ALL shards. Classic Pub/Sub channels are not slotted, so delivery FANS OUT to every shard's
/// local subscriber table via [`coordinator::fan_out_publish`] (the home shard delivers locally,
/// peers via their drain loops), summing the per-shard counts. Encodes a [`Value::Integer`].
async fn handle_publish(
    conn: &ConnState,
    inbox: &coordinator::Inbox,
    home: ShardId,
    request: &Request,
    out: &mut Vec<u8>,
) {
    let channel = request.args[1].clone();
    let payload = request.args[2].clone();
    let total = coordinator::fan_out_publish(
        inbox,
        channel.as_ref(),
        payload.as_ref(),
        conn.db,
        home.index,
    )
    .await;
    encode_into(out, &ironcache_server::Value::Integer(total), conn.proto);
}

/// The PING reply for a connection in SUBSCRIBE mode under RESP2 (SERVER_PUSH.md #20, PR 91a).
/// Redis replies a 2-element ARRAY `["pong", ""]` (or `["pong", <arg>]`) rather than the usual
/// `+PONG` simple string while subscribed, so a client multiplexing pushes and replies can tell
/// the PONG apart from a pushed message. RESP3 and non-subscriber PING are unchanged (handled by
/// the normal `cmd_ping` dispatch arm). The reply is a plain `Array` (NOT a push frame): Redis
/// sends it as a normal multi-bulk reply.
fn ping_subscribed_reply(request: &Request) -> ironcache_server::Value {
    let second = request
        .args
        .get(1)
        .map_or_else(|| bytes::Bytes::from_static(b""), bytes::Bytes::clone);
    ironcache_server::Value::Array(Some(vec![
        ironcache_server::Value::bulk_str("pong"),
        ironcache_server::Value::bulk(second),
    ]))
}

/// Deregister EVERY subscription a connection holds from THIS shard's subscription table
/// (SERVER_PUSH.md #20, PR 91a), driven off `conn.sub_channels` / `conn.sub_patterns` /
/// `conn.sub_shard_channels` (O(subs)). Called on connection close (and could be reused on RESET):
/// the connection's subscriptions are home-shard-local, so this runs on the connection's home
/// shard. A no-op when not subscribed.
pub(crate) fn deregister_all_subscriptions(conn: &ConnState) {
    // CLIENT TRACKING (#409): purge this connection from the per-shard tracking table on close, so a
    // later write never pushes an invalidation to a gone connection. A no-op (no alloc) when no
    // tracking client ever used this shard; runs regardless of the pub/sub state below.
    purge_conn_tracking(conn.id);

    if conn.sub_channels.is_empty()
        && conn.sub_patterns.is_empty()
        && conn.sub_shard_channels.is_empty()
    {
        return;
    }
    let pubsub = shard_pubsub();
    let mut table = pubsub.borrow_mut();
    for channel in &conn.sub_channels {
        table.unsubscribe(channel.as_ref(), conn.id);
    }
    for pattern in &conn.sub_patterns {
        // PSUBSCRIBE pattern subscriptions (PR 91b): deregister each from this shard's
        // `patterns` table so a QUIT / error close / peer close leaves no pattern leak.
        table.unsubscribe_pattern(pattern.as_ref(), conn.id);
    }
    for channel in &conn.sub_shard_channels {
        // SSUBSCRIBE shard subscriptions (#410): deregister each from this shard's
        // `shard_channels` table so a close leaves no shard-channel leak.
        table.unsubscribe_shard(channel.as_ref(), conn.id);
    }
}
