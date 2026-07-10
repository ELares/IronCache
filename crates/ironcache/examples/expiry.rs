// SPDX-License-Identifier: MIT OR Apache-2.0
//! expiry: set a key with a TTL, observe the TTL, wait for it to lapse, and see it GONE.
//!
//! Start `ironcache server` on 127.0.0.1:6379 first, then run this example:
//!
//! ```sh
//! cargo run -p ironcache -- server        # terminal 1
//! cargo run -p ironcache --example expiry  # terminal 2
//! ```
//!
//! Point `IRONCACHE_ADDR` at a different host:port to run it elsewhere.

use std::thread::sleep;
use std::time::Duration;

#[path = "common/resp.rs"]
mod resp;

fn main() -> std::io::Result<()> {
    let addr = resp::server_addr();
    println!("connecting to {addr}");
    let mut conn = resp::Conn::connect(&addr)?;

    let key = "expiry:session";

    // SET the key with a 1-second expiry (the EX option).
    let set = conn
        .command(&["SET", key, "token", "EX", "1"])?
        .ok_or_panic();
    println!("SET {key} token EX 1 -> {set:?}");
    assert_eq!(set.as_text().as_deref(), Some("OK"));

    // TTL reports the remaining lifetime in whole seconds (1 right after the SET).
    let ttl = conn.command(&["TTL", key])?.ok_or_panic();
    println!("TTL {key}          -> {ttl:?}");
    assert_eq!(ttl.as_int(), Some(1));

    // The value is present while the TTL is live.
    let live = conn.command(&["GET", key])?.ok_or_panic();
    println!("GET {key}          -> {live:?}");
    assert_eq!(live.as_text().as_deref(), Some("token"));

    // Wait for the expiry to lapse.
    println!("waiting 1.2s for the key to expire...");
    sleep(Duration::from_millis(1200));

    // GET now returns nil, and TTL reports -2 (the key does not exist).
    let gone = conn.command(&["GET", key])?.ok_or_panic();
    println!("GET {key}          -> {gone:?}");
    assert!(gone.is_nil(), "key should have expired");

    let ttl_gone = conn.command(&["TTL", key])?.ok_or_panic();
    println!("TTL {key}          -> {ttl_gone:?}");
    assert_eq!(ttl_gone.as_int(), Some(-2), "TTL of a missing key is -2");

    println!("expiry: OK");
    Ok(())
}
