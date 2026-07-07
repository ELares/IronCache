// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end test for #531: INFO's `# Stats` / `# Clients` / `# Keyspace` sections must aggregate
//! across ALL shards (node-wide), NOT report the serving shard's ~1/N slice, and must NOT vary by
//! which connection (hence which accept shard) reads them. It boots a REAL multi-shard node over
//! real sockets, drives traffic through several connections, and asserts:
//!
//!   * `keyspace_hits` / `keyspace_misses` / `total_connections_received` read from two DIFFERENT
//!     connections are EQUAL (node-wide invariance) and reflect the WHOLE node's traffic, not the
//!     ~1/N a single shard homed;
//!   * INFO `# Keyspace` `dbN:keys=...` equals `DBSIZE` (the whole-keyspace scatter-gather);
//!   * `CONFIG RESETSTAT` zeroes the node-wide stats (every shard's cell), so a subsequent INFO
//!     read from a different connection reports zeroed hits/misses.
//!
//! With the pre-#531 serving-shard-scoped rollup these node-wide totals VARIED per connection and
//! reported roughly 1/N of the node; this test pins the fix.

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
/// whole `$<len>\r\n<body>\r\n` frame has arrived; small status/integer/error replies fit one read.
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
            break;
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

/// Read the integer value of an INFO `field:value` line (e.g. `keyspace_hits`), panicking with the
/// body if the field is absent or non-numeric (a regression should fail loudly, not silently pass).
fn info_i64(info: &str, field: &str) -> i64 {
    let needle = format!("{field}:");
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(&needle) {
            return rest
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("field {field} not an integer in: {rest:?}"));
        }
    }
    panic!("field {field} missing from INFO body: {info:?}");
}

/// Read the `keys=` count of the INFO `# Keyspace` `dbN:keys=<n>,expires=...` line, or `None` when
/// the db has no line (Redis omits empty DBs).
fn keyspace_db_keys(info: &str, db: u32) -> Option<i64> {
    let prefix = format!("db{db}:");
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            for field in rest.split(',') {
                if let Some(v) = field.strip_prefix("keys=") {
                    return Some(v.trim().parse().expect("keys= is an integer"));
                }
            }
        }
    }
    None
}

/// Parse a RESP integer reply (`:<n>\r\n`), e.g. from DBSIZE.
fn integer_reply(reply: &str) -> i64 {
    reply
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap_or_else(|_| panic!("not an integer reply: {reply:?}"))
}

#[test]
fn info_stats_are_node_wide_not_per_shard() {
    let (r, local) = rt();
    local.block_on(&r, async {
        // A genuinely multi-shard node: keys + connections spread across shards, so a serving-shard-
        // scoped INFO would report only a fraction and vary per connection.
        const SHARDS: usize = 4;
        const KEYS: usize = 120;
        let port = free_port();
        let _server = run_server_for_test(port, SHARDS);

        // The WRITER drives all the traffic; two READERS (which the kernel SO_REUSEPORT-homes on
        // their own accept shards, likely different from the writer's) read INFO independently.
        let mut writer = connect_retry(port).await;
        let mut reader_a = connect_retry(port).await;
        let mut reader_b = connect_retry(port).await;

        // Populate KEYS distinct keys (they route to their owner shards -> spread across the node),
        // then GET each (KEYS hits) and GET KEYS absent keys (KEYS misses). Hits/misses accrue on
        // the OWNER shards, so only a node-wide rollup sees them all.
        for i in 0..KEYS {
            let k = format!("key:{i}");
            assert_eq!(cmd(&mut writer, &["SET", &k, "v"]).await, "+OK\r\n");
        }
        for i in 0..KEYS {
            let k = format!("key:{i}");
            let got = cmd(&mut writer, &["GET", &k]).await;
            assert!(got.starts_with('$'), "GET hit should be a bulk: {got:?}");
        }
        for i in 0..KEYS {
            let k = format!("absent:{i}");
            let got = cmd(&mut writer, &["GET", &k]).await;
            assert!(
                got.starts_with("$-1") || got.starts_with('_'),
                "GET miss should be nil: {got:?}"
            );
        }

        // # Keyspace matches DBSIZE (both the whole-keyspace scatter-gather): every key is in db0.
        let dbsize = integer_reply(&cmd(&mut reader_a, &["DBSIZE"]).await);
        assert_eq!(dbsize, KEYS as i64, "DBSIZE must cover the whole node");
        let info_a = cmd(&mut reader_a, &["INFO"]).await;
        assert_eq!(
            keyspace_db_keys(&info_a, 0),
            Some(dbsize),
            "INFO # Keyspace db0 keys must equal DBSIZE (node-wide), got:\n{info_a}"
        );

        // Node-wide hits/misses, read from reader_a: the WHOLE node's KEYS, not ~KEYS/SHARDS.
        let hits_a = info_i64(&info_a, "keyspace_hits");
        let misses_a = info_i64(&info_a, "keyspace_misses");
        assert_eq!(hits_a, KEYS as i64, "keyspace_hits must be node-wide");
        assert_eq!(misses_a, KEYS as i64, "keyspace_misses must be node-wide");

        // The SAME totals, read from a DIFFERENT connection (a different accept shard): they must NOT
        // vary by which shard homed the reader. keyspace_hits/misses are stable (INFO adds none), so
        // exact equality is the node-wide-invariance assertion.
        let info_b = cmd(&mut reader_b, &["INFO"]).await;
        assert_eq!(
            info_i64(&info_b, "keyspace_hits"),
            hits_a,
            "keyspace_hits must not vary by connection"
        );
        assert_eq!(
            info_i64(&info_b, "keyspace_misses"),
            misses_a,
            "keyspace_misses must not vary by connection"
        );
        // total_connections_received is a cumulative counter, stable while no new connection opens,
        // so both readers must agree on it too (and see all our connections, not one shard's share).
        assert_eq!(
            info_i64(&info_a, "total_connections_received"),
            info_i64(&info_b, "total_connections_received"),
            "total_connections_received must not vary by connection"
        );
        // total_commands_processed is the node-wide sum: it dwarfs any single shard's ~1/N. We drove
        // 3*KEYS data commands plus a handful of admin reads; assert it reflects the whole node.
        assert!(
            info_i64(&info_b, "total_commands_processed") >= (3 * KEYS) as i64,
            "total_commands_processed must be node-wide, got {}",
            info_i64(&info_b, "total_commands_processed")
        );

        // CONFIG RESETSTAT is node-wide (#531): it must zero EVERY shard's cell, so a reset issued on
        // the writer's shard clears the hits/misses that accrued on the OTHER shards too. Read the
        // result from reader_a (a different accept shard) to prove the reset crossed shards.
        assert_eq!(cmd(&mut writer, &["CONFIG", "RESETSTAT"]).await, "+OK\r\n");
        let after = cmd(&mut reader_a, &["INFO"]).await;
        assert_eq!(
            info_i64(&after, "keyspace_hits"),
            0,
            "RESETSTAT must zero keyspace_hits node-wide"
        );
        assert_eq!(
            info_i64(&after, "keyspace_misses"),
            0,
            "RESETSTAT must zero keyspace_misses node-wide"
        );

        // The keyspace section still reflects the live node after a stats reset (RESETSTAT clears
        // stat COUNTERS, not the keyspace).
        assert_eq!(keyspace_db_keys(&after, 0), Some(KEYS as i64));
    });
}
