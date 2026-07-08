// SPDX-License-Identifier: MIT OR Apache-2.0
//! #511 GET-BY-REFERENCE HOME FAST PATH. Boots the REAL server on ONE shard (so every key is
//! home-owned and every GET takes the by-ref home fast path, NOT the cross-shard hop) and asserts
//! the wire bytes are byte-identical to the copying `cmd_get` path for the tricky value shapes the
//! direct-to-`out` encode must get exactly right: the EMPTY value, a 1-byte value, a large multi-KB
//! value, and a BINARY value with embedded NUL / CR / LF bytes; plus NULL for a missing key and
//! WRONGTYPE for a non-string key. This drives the whole path (decode -> route -> home dispatch ->
//! direct bulk-ref encode -> socket), which is exactly where the allocation was dropped.

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

/// Encode a RESP2 command array from BYTE args (binary-safe: values may contain NUL/CR/LF).
fn encode_args(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Send one command and read the reply until AT LEAST `expect_len` bytes have arrived (a large
/// bulk may span several socket reads), then return exactly what was read. Binary-safe.
async fn cmd_expect(client: &mut TcpStream, args: &[&[u8]], expect_len: usize) -> Vec<u8> {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 8192];
    while got.len() < expect_len {
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed before full reply");
        got.extend_from_slice(&buf[..n]);
    }
    got
}

/// Build the expected `$<len>\r\n<bytes>\r\n` bulk frame for `data`.
fn bulk_frame(data: &[u8]) -> Vec<u8> {
    let mut f = format!("${}\r\n", data.len()).into_bytes();
    f.extend_from_slice(data);
    f.extend_from_slice(b"\r\n");
    f
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
fn get_by_ref_home_path_is_byte_identical() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // Value shapes the direct-to-out encode must frame exactly: empty, 1-byte, large multi-KB,
        // and binary with embedded NUL / CR / LF.
        let large = vec![b'Q'; 5000];
        let binary: Vec<u8> = vec![0, 1, 2, 255, b'\r', b'\n', 0, b'a', 7, 0];
        let cases: &[(&[u8], &[u8])] = &[
            (b"empty", b""),
            (b"one", b"x"),
            (b"large", &large),
            (b"bin", &binary),
        ];

        for (key, val) in cases {
            let set = cmd_expect(&mut c, &[b"SET", key, val], 5).await;
            assert_eq!(&set[..5], b"+OK\r\n", "SET {key:?} reply");
            let expected = bulk_frame(val);
            let got = cmd_expect(&mut c, &[b"GET", key], expected.len()).await;
            assert_eq!(got, expected, "GET {key:?} must be byte-identical");
        }

        // Missing key -> the RESP2 null bulk `$-1\r\n` (a keyspace MISS).
        let miss = cmd_expect(&mut c, &[b"GET", b"absent"], 5).await;
        assert_eq!(miss, b"$-1\r\n", "GET of a missing key -> null bulk");

        // A non-string key -> WRONGTYPE (the error code, not the exact human text).
        let _ = cmd_expect(&mut c, &[b"RPUSH", b"lst", b"a"], 4).await;
        let wt = cmd_expect(&mut c, &[b"GET", b"lst"], 10).await;
        assert!(
            wt.starts_with(b"-WRONGTYPE"),
            "GET of a non-string -> WRONGTYPE, got {:?}",
            String::from_utf8_lossy(&wt)
        );

        // Re-reading a present key after the WRONGTYPE still serves by-ref correctly (the fast path
        // is stateless across commands).
        let again = cmd_expect(&mut c, &[b"GET", b"one"], 6).await;
        assert_eq!(again, b"$1\r\nx\r\n");
    });
}
