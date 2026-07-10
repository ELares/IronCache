// SPDX-License-Identifier: MIT OR Apache-2.0
//! pipeline: send a batch of commands without waiting between them, then read every reply.
//!
//! Pipelining amortizes the network round trip: N commands cost ~1 round trip instead of N.
//! This example sends the whole batch first, then reads the replies back in order.
//!
//! Start `ironcache server` on 127.0.0.1:6379 first, then run this example:
//!
//! ```sh
//! cargo run -p ironcache -- server          # terminal 1
//! cargo run -p ironcache --example pipeline  # terminal 2
//! ```
//!
//! Point `IRONCACHE_ADDR` at a different host:port to run it elsewhere.

#[path = "common/resp.rs"]
mod resp;

fn main() -> std::io::Result<()> {
    let addr = resp::server_addr();
    println!("connecting to {addr}");
    let mut conn = resp::Conn::connect(&addr)?;

    let counter = "pipeline:counter";

    // The batch: clear the counter, INCR it five times, then read it back.
    let batch: Vec<Vec<&str>> = vec![
        vec!["DEL", counter],
        vec!["INCR", counter],
        vec!["INCR", counter],
        vec!["INCR", counter],
        vec!["INCR", counter],
        vec!["INCR", counter],
        vec!["GET", counter],
    ];

    // Send every command FIRST (no reply read in between): that is the pipeline.
    for cmd in &batch {
        conn.send(cmd)?;
    }
    println!("pipelined {} commands in one batch", batch.len());

    // Now drain the replies, one per command, in the order they were sent.
    let mut replies = Vec::with_capacity(batch.len());
    for _ in 0..batch.len() {
        replies.push(conn.read_reply()?.ok_or_panic());
    }
    for (cmd, reply) in batch.iter().zip(&replies) {
        println!("{:<20} -> {reply:?}", cmd.join(" "));
    }

    // The five INCRs returned 1..=5, and the final GET reads back "5".
    let incrs: Vec<i64> = replies[1..=5]
        .iter()
        .filter_map(resp::Reply::as_int)
        .collect();
    assert_eq!(incrs, vec![1, 2, 3, 4, 5], "INCR replies should count up");
    assert_eq!(
        replies[6].as_text().as_deref(),
        Some("5"),
        "final GET should read back 5"
    );

    println!("pipeline: OK");
    Ok(())
}
