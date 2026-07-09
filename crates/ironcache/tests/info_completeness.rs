// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end tests for the #527 INFO-completeness residuals: the net-io byte totals
//! (`total_net_input_bytes` / `total_net_output_bytes`, the fields `redis_exporter` reads as
//! `redis_net_input_bytes_total` / `redis_net_output_bytes_total`) and the cross-shard COMMANDSTATS
//! rollup. Both boot the REAL multi-shard server over a real socket and drive real traffic.

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

/// Send one command and read the full reply. INFO is a large bulk string, so keep reading until the
/// socket has the whole bulk (the `$<len>\r\n<body>\r\n` framing tells us when).
async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0u8; 8192];
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
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

/// Parse an integer INFO field (`name:VALUE`).
fn field(info: &str, name: &str) -> u64 {
    let prefix = format!("{name}:");
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            return rest
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("non-integer {name}: {line:?}"));
        }
    }
    panic!("missing INFO field {name} in:\n{info}");
}

/// Parse the `calls=` count of a `cmdstat_<lname>` COMMANDSTATS line, or 0 if the command is absent.
fn cmdstat_calls(cs: &str, lname: &str) -> u64 {
    let prefix = format!("cmdstat_{lname}:calls=");
    for line in cs.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            return rest.split(',').next().unwrap().parse().unwrap();
        }
    }
    0
}

/// #527: INFO `# Stats` exposes `total_net_input_bytes` / `total_net_output_bytes`, they start
/// non-negative and GROW with driven traffic, roughly reflecting the bytes moved (a 512-byte value
/// per SET inbound, the same bytes echoed by the GET reply outbound).
#[test]
fn net_io_byte_totals_grow_with_traffic() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 3);
        let mut c = connect_retry(port).await;

        // Baseline: the fields exist (redis_exporter reads them) and read as integers.
        let base = cmd(&mut c, &["INFO"]).await;
        assert!(
            base.contains("total_net_input_bytes:") && base.contains("total_net_output_bytes:"),
            "INFO Stats must carry the net-io byte fields: {base}"
        );
        let in0 = field(&base, "total_net_input_bytes");
        let out0 = field(&base, "total_net_output_bytes");

        // Drive a chunk of sizeable traffic: 200 SET+GET pairs of a 512-byte value.
        let val = "v".repeat(512);
        for i in 0..200u32 {
            let key = format!("k{i}");
            cmd(&mut c, &["SET", &key, &val]).await;
            cmd(&mut c, &["GET", &key]).await;
        }

        let after = cmd(&mut c, &["INFO"]).await;
        let in1 = field(&after, "total_net_input_bytes");
        let out1 = field(&after, "total_net_output_bytes");

        assert!(in1 > in0, "net input must grow: {in0} -> {in1}");
        assert!(out1 > out0, "net output must grow: {out0} -> {out1}");
        // Roughly reflects bytes moved: 200 SETs carrying a 512-byte value are >100 KiB inbound, and
        // 200 GET bulk replies of that value are >100 KiB outbound.
        assert!(
            in1 - in0 > 100_000,
            "input should reflect the SET payloads: grew {}",
            in1 - in0
        );
        assert!(
            out1 - out0 > 100_000,
            "output should reflect the GET replies: grew {}",
            out1 - out0
        );
    });
}

/// The GETs each connection issues in the cross-shard COMMANDSTATS test.
const GETS_PER_CONN: u64 = 4;

/// #527: COMMANDSTATS is now NODE-WIDE. Three connections round-robin onto three DISTINCT accept
/// shards; each issues N GETs (recorded on its own shard). INFO COMMANDSTATS served on one shard
/// reports the SUM (3*N), not just that shard's N -- the cross-shard rollup #545 gave the top-level
/// Stats, now for the per-command table. RESETSTAT then clears every shard's table node-wide.
#[test]
fn commandstats_get_calls_are_summed_across_shards() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 3);
        // The acceptor round-robins accepted connections across shards, so three fresh connections
        // land on three distinct accept shards.
        let mut c0 = connect_retry(port).await;
        let mut c1 = connect_retry(port).await;
        let mut c2 = connect_retry(port).await;

        for c in [&mut c0, &mut c1, &mut c2] {
            for _ in 0..GETS_PER_CONN {
                cmd(c, &["GET", "shared_key"]).await;
            }
        }

        // Served on c0's shard, but reports the WHOLE node's GET count (3*N). A per-shard table
        // would show only c0's own N here.
        let cs = cmd(&mut c0, &["INFO", "COMMANDSTATS"]).await;
        assert!(cs.contains("# Commandstats"), "missing header: {cs}");
        assert_eq!(
            cmdstat_calls(&cs, "get"),
            3 * GETS_PER_CONN,
            "GET calls must be the node-wide sum across shards: {cs}"
        );

        // CONFIG RESETSTAT fans across every shard's per-command table, so the node-wide rollup
        // zeroes (not just the serving shard's).
        assert_eq!(cmd(&mut c0, &["CONFIG", "RESETSTAT"]).await, "+OK\r\n");
        let after = cmd(&mut c0, &["INFO", "COMMANDSTATS"]).await;
        assert_eq!(
            cmdstat_calls(&after, "get"),
            0,
            "RESETSTAT must clear the GET tally on EVERY shard: {after}"
        );
    });
}
