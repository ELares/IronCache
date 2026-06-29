// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end tests for INFO COMMANDSTATS / ERRORSTATS (#413): boot the REAL server over a
//! real socket, drive a few commands (some succeeding, one erroring), and assert the per-command
//! and per-error tables INFO renders carry the Redis field shapes go-redis / redis-py parse.

use ironcache::test_support::run_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

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

fn encode_args(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// Send one command and read the full reply. INFO is a large bulk string, so keep reading until
/// the socket has the whole bulk (the `$<len>\r\n<body>\r\n` framing tells us when).
async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0u8; 8192];
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
        // A bulk reply `$<len>\r\n<body>\r\n` is complete once we have len + the two CRLFs.
        if buf.first() == Some(&b'$') {
            if let Some(hdr) = buf.windows(2).position(|w| w == b"\r\n") {
                let len: i64 = std::str::from_utf8(&buf[1..hdr]).unwrap().parse().unwrap();
                if len < 0 || buf.len() >= hdr + 2 + len as usize + 2 {
                    break;
                }
            }
        } else {
            break; // a small status/integer/error reply fits in one read
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    (rt, local)
}

#[test]
fn info_commandstats_and_errorstats_render_the_redis_shapes() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // A few successful commands + one that errors (LPUSH against a string -> WRONGTYPE).
        cmd(&mut c, &["SET", "str", "hello"]).await;
        cmd(&mut c, &["GET", "str"]).await;
        cmd(&mut c, &["GET", "str"]).await;
        let wt = cmd(&mut c, &["LPUSH", "str", "x"]).await;
        assert!(
            wt.starts_with("-WRONGTYPE"),
            "LPUSH on a string is WRONGTYPE, got {wt:?}"
        );

        // COMMANDSTATS: one cmdstat_ line per command with the Redis field shape.
        let cs = cmd(&mut c, &["INFO", "COMMANDSTATS"]).await;
        assert!(
            cs.contains("# Commandstats"),
            "missing section header: {cs:?}"
        );
        assert!(cs.contains("cmdstat_set:calls=1,"), "set calls: {cs:?}");
        assert!(
            cs.contains("cmdstat_get:calls=2,"),
            "get should show 2 calls: {cs:?}"
        );
        // The field shape: usec, usec_per_call (a float), rejected_calls, failed_calls all present.
        assert!(
            cs.contains("usec=")
                && cs.contains("usec_per_call=")
                && cs.contains("rejected_calls=")
                && cs.contains("failed_calls="),
            "cmdstat line must carry the full Redis field shape: {cs:?}"
        );
        // The erroring LPUSH is recorded with failed_calls=1.
        assert!(
            cs.contains("cmdstat_lpush:calls=1,usec=") && cs.contains(",failed_calls=1\r\n"),
            "lpush must show failed_calls=1: {cs:?}"
        );

        // ERRORSTATS: the WRONGTYPE code is counted.
        let es = cmd(&mut c, &["INFO", "ERRORSTATS"]).await;
        assert!(
            es.contains("# Errorstats"),
            "missing errorstats header: {es:?}"
        );
        assert!(
            es.contains("errorstat_WRONGTYPE:count=1"),
            "WRONGTYPE must be counted: {es:?}"
        );

        // The DEFAULT INFO does NOT include commandstats/errorstats (Redis keeps default small).
        let default_info = cmd(&mut c, &["INFO"]).await;
        assert!(
            !default_info.contains("cmdstat_") && !default_info.contains("# Commandstats"),
            "default INFO must NOT include commandstats"
        );

        // CONFIG RESETSTAT clears the tables.
        assert_eq!(cmd(&mut c, &["CONFIG", "RESETSTAT"]).await, "+OK\r\n");
        let after = cmd(&mut c, &["INFO", "COMMANDSTATS"]).await;
        // Only the INFO command itself (issued after the reset) may appear; the prior SET/GET/LPUSH
        // tallies are gone.
        assert!(
            !after.contains("cmdstat_set") && !after.contains("cmdstat_lpush"),
            "RESETSTAT must clear the prior command tallies: {after:?}"
        );

        // INFO everything includes the commandstats section too.
        let everything = cmd(&mut c, &["INFO", "everything"]).await;
        assert!(
            everything.contains("# Server") && everything.contains("# Commandstats"),
            "INFO everything must include both the standard sections and commandstats"
        );
    });
}
