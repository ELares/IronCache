// SPDX-License-Identifier: MIT OR Apache-2.0
//! transactions: MULTI queues commands, EXEC runs them atomically and returns every reply.
//!
//! Between MULTI and EXEC each command is acknowledged with `+QUEUED`; EXEC then executes
//! the whole batch atomically and returns an array with one reply per queued command.
//!
//! IronCache is internally sharded, so a single-node transaction requires every queued key
//! to live on the connection's home shard. The reliable way to guarantee that is a shared
//! `{hash tag}`: keys that share the braced substring hash to the same slot (and shard).
//! This example tags every key with `{acct}` for that reason.
//!
//! Start `ironcache server` on 127.0.0.1:6379 first, then run this example:
//!
//! ```sh
//! cargo run -p ironcache -- server              # terminal 1
//! cargo run -p ironcache --example transactions  # terminal 2
//! ```
//!
//! Point `IRONCACHE_ADDR` at a different host:port to run it elsewhere.

#[path = "common/resp.rs"]
mod resp;

fn main() -> std::io::Result<()> {
    let addr = resp::server_addr();
    println!("connecting to {addr}");
    let mut conn = resp::Conn::connect(&addr)?;

    let balance = "{acct}:balance";

    // Open the transaction.
    let multi = conn.command(&["MULTI"])?.ok_or_panic();
    println!("MULTI                     -> {multi:?}");
    assert_eq!(multi.as_text().as_deref(), Some("OK"));

    // Queue the commands. Each is acknowledged with +QUEUED, not yet executed.
    for cmd in [
        &["SET", balance, "100"][..],
        &["INCRBY", balance, "50"][..],
        &["INCRBY", balance, "-30"][..],
    ] {
        let queued = conn.command(cmd)?.ok_or_panic();
        println!("{:<25} -> {queued:?}", cmd.join(" "));
        assert_eq!(queued.as_text().as_deref(), Some("QUEUED"));
    }

    // EXEC runs the batch atomically and returns one reply per queued command.
    let exec = conn.command(&["EXEC"])?.ok_or_panic();
    println!("EXEC                      -> {exec:?}");
    let results = exec.into_array().expect("EXEC returns an array of replies");
    assert_eq!(results.len(), 3, "one reply per queued command");
    assert_eq!(results[0].as_text().as_deref(), Some("OK")); // SET
    assert_eq!(results[1].as_int(), Some(150)); // 100 + 50
    assert_eq!(results[2].as_int(), Some(120)); // 150 - 30

    // Confirm the committed value outside the transaction.
    let final_balance = conn.command(&["GET", balance])?.ok_or_panic();
    println!("GET {balance}   -> {final_balance:?}");
    assert_eq!(final_balance.as_text().as_deref(), Some("120"));

    println!("transactions: OK");
    Ok(())
}
