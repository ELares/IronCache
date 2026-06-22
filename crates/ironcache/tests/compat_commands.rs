// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end tests for the drop-in compatibility commands (GETRANGE / SUBSTR / SETRANGE /
//! GETDEL / MSETNX, LMPOP / ZMPOP, SORT / SORT_RO) and the two CONFIG durability fixes
//! (`CONFIG SET appendonly no` -> +OK, `CONFIG GET save` -> empty when off).
//!
//! These boot the REAL server over a real socket and drive the wire, so they prove the whole
//! path (decode -> classify -> route -> dispatch -> encode), not just the unit level. A
//! single shard keeps every key home-owned so the reply bytes are clean and deterministic.

use ironcache::test_support::run_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Connect with a few short retries (the shards bind asynchronously after `run_server`).
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on port {port}");
}

/// Encode a RESP2 command array from string args.
fn encode_args(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// Send one command and read ONE socket read of the reply as a String. The replies here are
/// small (a status line, a short bulk, a few-element array), so a single read captures them.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    (rt, local)
}

/// The string commands: GETRANGE / SUBSTR / SETRANGE / GETDEL / MSETNX over the wire.
#[test]
fn string_compat_commands_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["SET", "k", "Hello World"]).await, "+OK\r\n");
        // GETRANGE signed-range substring.
        assert_eq!(
            cmd(&mut c, &["GETRANGE", "k", "0", "4"]).await,
            "$5\r\nHello\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["GETRANGE", "k", "-5", "-1"]).await,
            "$5\r\nWorld\r\n"
        );
        // A missing key -> the EMPTY bulk (not nil).
        assert_eq!(
            cmd(&mut c, &["GETRANGE", "miss", "0", "-1"]).await,
            "$0\r\n\r\n"
        );
        // SUBSTR is the alias.
        assert_eq!(
            cmd(&mut c, &["SUBSTR", "k", "6", "-1"]).await,
            "$5\r\nWorld\r\n"
        );
        // SETRANGE overwrites + returns the new length.
        assert_eq!(
            cmd(&mut c, &["SETRANGE", "k", "6", "Redis"]).await,
            ":11\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$11\r\nHello Redis\r\n");
        // GETDEL returns then removes.
        assert_eq!(
            cmd(&mut c, &["GETDEL", "k"]).await,
            "$11\r\nHello Redis\r\n"
        );
        assert_eq!(cmd(&mut c, &["EXISTS", "k"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["GETDEL", "k"]).await, "$-1\r\n");
        // MSETNX all-or-nothing.
        assert_eq!(cmd(&mut c, &["MSETNX", "a", "1", "b", "2"]).await, ":1\r\n");
        assert_eq!(cmd(&mut c, &["MSETNX", "b", "X", "z", "9"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["EXISTS", "z"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["GET", "b"]).await, "$1\r\n2\r\n");
    });
}

/// LMPOP / ZMPOP over the wire (the first-non-empty pick + COUNT).
#[test]
fn mpop_commands_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["RPUSH", "l2", "a", "b", "c"]).await, ":3\r\n");
        // LMPOP picks l2 (l1 missing), LEFT pops 'a': [l2, [a]].
        assert_eq!(
            cmd(&mut c, &["LMPOP", "2", "l1", "l2", "LEFT"]).await,
            "*2\r\n$2\r\nl2\r\n*1\r\n$1\r\na\r\n"
        );
        // All empty -> the null array (RESP2 `*-1`).
        cmd(&mut c, &["DEL", "l2"]).await;
        assert_eq!(
            cmd(&mut c, &["LMPOP", "2", "l1", "l2", "LEFT"]).await,
            "*-1\r\n"
        );
        // ZMPOP all-empty is also the null array.
        assert_eq!(cmd(&mut c, &["ZMPOP", "1", "nope", "MIN"]).await, "*-1\r\n");

        // ZMPOP MIN pops the lowest: [z, [[a, 1]]].
        cmd(&mut c, &["ZADD", "z", "1", "a", "2", "b"]).await;
        assert_eq!(
            cmd(&mut c, &["ZMPOP", "1", "z", "MIN"]).await,
            "*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n"
        );
    });
}

/// SORT / SORT_RO over the wire (numeric, ALPHA, LIMIT, BY/GET, STORE).
#[test]
fn sort_commands_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        cmd(&mut c, &["RPUSH", "nums", "3", "1", "2", "10"]).await;
        // Numeric ascending.
        assert_eq!(
            cmd(&mut c, &["SORT", "nums"]).await,
            "*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$2\r\n10\r\n"
        );
        // ALPHA ("10" < "2").
        assert_eq!(
            cmd(&mut c, &["SORT", "nums", "ALPHA"]).await,
            "*4\r\n$1\r\n1\r\n$2\r\n10\r\n$1\r\n2\r\n$1\r\n3\r\n"
        );
        // LIMIT after sort.
        assert_eq!(
            cmd(&mut c, &["SORT", "nums", "LIMIT", "0", "2"]).await,
            "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"
        );
        // BY external weights + STORE: weight_1=30, weight_2=10, weight_3=20.
        cmd(&mut c, &["RPUSH", "ids", "1", "2", "3"]).await;
        cmd(
            &mut c,
            &["MSET", "weight_1", "30", "weight_2", "10", "weight_3", "20"],
        )
        .await;
        assert_eq!(
            cmd(&mut c, &["SORT", "ids", "BY", "weight_*"]).await,
            "*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["SORT", "ids", "BY", "weight_*", "STORE", "dest"]).await,
            ":3\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["LRANGE", "dest", "0", "-1"]).await,
            "*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n"
        );
        // SORT_RO rejects STORE.
        assert_eq!(
            cmd(&mut c, &["SORT_RO", "ids", "STORE", "x"]).await,
            "-ERR syntax error\r\n"
        );
    });
}

/// The two CONFIG durability fixes over the wire.
#[test]
fn config_durability_fixes_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // `CONFIG SET appendonly no` -> +OK (the no-op-OK).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "appendonly", "no"]).await,
            "+OK\r\n"
        );
        // `CONFIG SET appendonly yes` is still REFUSED.
        let yes = cmd(&mut c, &["CONFIG", "SET", "appendonly", "yes"]).await;
        assert!(yes.starts_with("-ERR"), "expected refusal, got {yes}");
        // `CONFIG GET save` -> empty string when the periodic save is off (the default).
        // The reply is a 2-element array [save, ""] (RESP2 CONFIG GET map).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "save"]).await,
            "*2\r\n$4\r\nsave\r\n$0\r\n\r\n"
        );
    });
}

/// `CONFIG SET timeout` / `CONFIG GET timeout` over the wire (PROD-SAFETY #4: `timeout` is now
/// runtime-settable, was boot-only). Proves the registry plumbing + the wire encoding round-trip;
/// the LIVE serve-loop effect (idle disconnection honoring the runtime change) is covered by the
/// serve-loop self-review + the runtime/registry unit tests -- a timed idle-close behavioral test
/// would need a multi-second sleep, which we deliberately avoid (flaky).
#[test]
fn config_set_get_timeout_round_trips_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // The default boot timeout is 0 (Redis default: idle disconnection off).
        // The reply is a 2-element array [timeout, "0"] (RESP2 CONFIG GET map).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n"
        );
        // `CONFIG SET timeout 30` -> +OK.
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "timeout", "30"]).await,
            "+OK\r\n"
        );
        // `CONFIG GET timeout` now reflects the runtime change (the overlay wins over boot).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$2\r\n30\r\n"
        );
        // `CONFIG SET timeout 0` -> +OK (disables idle disconnection again).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "timeout", "0"]).await,
            "+OK\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n"
        );
        // A negative / non-numeric value is REJECTED with an error (not a panic, not a silent 0).
        let neg = cmd(&mut c, &["CONFIG", "SET", "timeout", "-1"]).await;
        assert!(neg.starts_with("-ERR"), "expected error, got {neg}");
        let bad = cmd(&mut c, &["CONFIG", "SET", "timeout", "abc"]).await;
        assert!(bad.starts_with("-ERR"), "expected error, got {bad}");
        // The rejected SETs left the value at the last accepted value (0).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n"
        );
    });
}
