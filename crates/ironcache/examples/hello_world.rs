// SPDX-License-Identifier: MIT OR Apache-2.0
//! hello_world: the smallest round trip against IronCache (PING + SET/GET/DEL).
//!
//! Start `ironcache server` on 127.0.0.1:6379 first, then run this example:
//!
//! ```sh
//! cargo run -p ironcache -- server            # terminal 1
//! cargo run -p ironcache --example hello_world # terminal 2
//! ```
//!
//! Point `IRONCACHE_ADDR` at a different host:port to run it elsewhere.

#[path = "common/resp.rs"]
mod resp;

fn main() -> std::io::Result<()> {
    let addr = resp::server_addr();
    println!("connecting to {addr}");
    let mut conn = resp::Conn::connect(&addr)?;

    // PING: prove the server is up and speaking RESP.
    let pong = conn.command(&["PING"])?.ok_or_panic();
    println!("PING            -> {pong:?}");
    assert_eq!(pong.as_text().as_deref(), Some("PONG"));

    // SET a key, then read it back with GET.
    let set = conn.command(&["SET", "hello", "world"])?.ok_or_panic();
    println!("SET hello world -> {set:?}");
    assert_eq!(set.as_text().as_deref(), Some("OK"));

    let got = conn.command(&["GET", "hello"])?.ok_or_panic();
    println!("GET hello       -> {got:?}");
    assert_eq!(got.as_text().as_deref(), Some("world"));

    // DEL removes it; a second GET now returns nil.
    let deleted = conn.command(&["DEL", "hello"])?.ok_or_panic();
    println!("DEL hello       -> {deleted:?}");
    assert_eq!(deleted.as_int(), Some(1));

    let gone = conn.command(&["GET", "hello"])?.ok_or_panic();
    println!("GET hello       -> {gone:?}");
    assert!(gone.is_nil(), "key should be gone after DEL");

    println!("hello_world: OK");
    Ok(())
}
