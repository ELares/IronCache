// SPDX-License-Identifier: MIT OR Apache-2.0
//! pubsub: SUBSCRIBE on one connection, PUBLISH from another, receive the message.
//!
//! Pub/Sub needs two connections: a subscriber (which then only reads pushed messages) and
//! a separate publisher. IronCache fans a PUBLISH out across shards, so the two connections
//! do not need to land on the same shard.
//!
//! Start `ironcache server` on 127.0.0.1:6379 first, then run this example:
//!
//! ```sh
//! cargo run -p ironcache -- server        # terminal 1
//! cargo run -p ironcache --example pubsub  # terminal 2
//! ```
//!
//! Point `IRONCACHE_ADDR` at a different host:port to run it elsewhere.

#[path = "common/resp.rs"]
mod resp;

fn main() -> std::io::Result<()> {
    let addr = resp::server_addr();
    println!("connecting to {addr}");

    let channel = "pubsub:news";
    let payload = "hello subscribers";

    // Connection 1: the subscriber. SUBSCRIBE replies with a confirmation array
    // ["subscribe", <channel>, <subscription count>].
    let mut subscriber = resp::Conn::connect(&addr)?;
    let confirm = subscriber.command(&["SUBSCRIBE", channel])?.ok_or_panic();
    println!("SUBSCRIBE {channel} -> {confirm:?}");
    let confirm = confirm
        .into_array()
        .expect("subscribe confirmation is an array");
    assert_eq!(confirm[0].as_text().as_deref(), Some("subscribe"));
    assert_eq!(confirm[1].as_text().as_deref(), Some(channel));
    assert_eq!(confirm[2].as_int(), Some(1), "one channel subscribed");

    // Connection 2: the publisher. PUBLISH returns the number of clients that received it.
    let mut publisher = resp::Conn::connect(&addr)?;
    let received = publisher
        .command(&["PUBLISH", channel, payload])?
        .ok_or_panic();
    println!("PUBLISH {channel} \"{payload}\" -> {received:?}");
    assert_eq!(received.as_int(), Some(1), "our one subscriber received it");

    // Back on the subscriber: the next reply is the delivered message,
    // ["message", <channel>, <payload>].
    let message = subscriber.read_reply()?.ok_or_panic();
    println!("received          -> {message:?}");
    let message = message.into_array().expect("a message is an array");
    assert_eq!(message[0].as_text().as_deref(), Some("message"));
    assert_eq!(message[1].as_text().as_deref(), Some(channel));
    assert_eq!(message[2].as_text().as_deref(), Some(payload));

    // Tidy up: leave the channel.
    let _ = subscriber.command(&["UNSUBSCRIBE", channel])?;

    println!("pubsub: OK");
    Ok(())
}
