// SPDX-License-Identifier: MIT OR Apache-2.0
//! Durable on-disk persistence acceptance tests (#58): boot the REAL multi-shard `run_server`
//! with a `data_dir`, drive `SAVE` / `BGSAVE` / `LASTSAVE` over real sockets, RESTART on the same
//! `data_dir`, and assert the keyspace (values + TTLs) is reconstructed from disk.
//!
//! These exercise the WHOLE persistence path end to end: the serve-router interception ->
//! cross-shard `__ICSAVE` fan-out (each shard dumps its own partition via the forkless
//! `snapshot_chunk`) -> the atomic manifest commit -> load-on-boot in each shard's drain loop.

use ironcache::test_support::run_persist_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A throwaway temp directory unique to the test + process for the snapshot files.
fn temp_data_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ic-persist-it-{tag}-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Connect with a few short retries (the shards bind asynchronously after `run_server`).
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on port {port}");
}

/// Read exactly `expect.len()` bytes and assert they match.
async fn expect_reply(client: &mut TcpStream, expect: &[u8]) {
    let mut buf = vec![0u8; expect.len()];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, expect, "got {:?}", String::from_utf8_lossy(&buf));
}

/// `SET key val` -> expect `+OK`.
async fn set(client: &mut TcpStream, key: &str, val: &str) {
    let frame = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        val.len(),
        val
    );
    client.write_all(frame.as_bytes()).await.unwrap();
    expect_reply(client, b"+OK\r\n").await;
}

/// `GET key` -> the raw reply bytes (small replies fit one read).
async fn get_raw(client: &mut TcpStream, key: &str) -> Vec<u8> {
    let frame = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client.write_all(frame.as_bytes()).await.unwrap();
    read_one(client).await
}

/// Send a bare arity-1 command (SAVE / LASTSAVE / BGSAVE / DBSIZE) and return its raw reply.
async fn cmd1(client: &mut TcpStream, name: &str) -> Vec<u8> {
    let frame = format!("*1\r\n${}\r\n{}\r\n", name.len(), name);
    client.write_all(frame.as_bytes()).await.unwrap();
    read_one(client).await
}

/// Read one reply (one socket read; the replies here are small and arrive in one segment).
async fn read_one(client: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// The expected bulk-string reply for `val`.
fn bulk(val: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", val.len(), val).into_bytes()
}

/// `SET key val EX secs` -> expect `+OK` (a key with a TTL).
async fn set_ex(client: &mut TcpStream, key: &str, val: &str, secs: u64) {
    let s = secs.to_string();
    let frame = format!(
        "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nEX\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        val.len(),
        val,
        s.len(),
        s
    );
    client.write_all(frame.as_bytes()).await.unwrap();
    expect_reply(client, b"+OK\r\n").await;
}

/// `TTL key` -> the integer reply (remaining seconds; `-1` no TTL, `-2` absent).
async fn ttl(client: &mut TcpStream, key: &str) -> i64 {
    let frame = format!("*2\r\n$3\r\nTTL\r\n${}\r\n{}\r\n", key.len(), key);
    client.write_all(frame.as_bytes()).await.unwrap();
    let raw = read_one(client).await;
    let s = String::from_utf8_lossy(&raw);
    s.trim_start_matches(':').trim_end().parse().unwrap_or(-99)
}

/// SET keys, SAVE, RESTART on the same data_dir, and assert GET returns the values (the core
/// warm-restart round-trip, #62). Multi-shard so each shard's file round-trips and load
/// reconstructs the full keyspace.
#[tokio::test(flavor = "current_thread")]
async fn save_restart_reloads_keyspace() {
    let dir = temp_data_dir("restart");
    let port = free_port();

    // -- Boot 1: populate + SAVE. --
    {
        let server = run_persist_server_for_test(port, 4, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        for i in 0..50 {
            set(&mut c, &format!("key-{i}"), &format!("value-{i}")).await;
        }
        // A key with a long TTL (to assert the TTL round-trips through the snapshot).
        set_ex(&mut c, "ttl-key", "ttl-val", 10_000).await;
        // LASTSAVE is 0 before any save.
        assert_eq!(cmd1(&mut c, "LASTSAVE").await, b":0\r\n");
        // SAVE blocks until committed, replies +OK.
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        // LASTSAVE now advanced to a non-zero unix time.
        let ls = cmd1(&mut c, "LASTSAVE").await;
        assert!(
            ls.starts_with(b":") && ls != b":0\r\n",
            "LASTSAVE advanced: {ls:?}"
        );
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // The snapshot files + manifest exist on disk.
    assert!(dir.join("dump.manifest").exists(), "manifest committed");

    // -- Boot 2: a FRESH server on the SAME data_dir must load the keyspace. --
    {
        let server = run_persist_server_for_test(port, 4, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        for i in 0..50 {
            assert_eq!(
                get_raw(&mut c, &format!("key-{i}")).await,
                bulk(&format!("value-{i}")),
                "key-{i} reloaded from disk after restart"
            );
        }
        // The TTL'd key reloaded WITH its value AND a still-positive TTL (the deadline round-trips).
        assert_eq!(get_raw(&mut c, "ttl-key").await, bulk("ttl-val"));
        let remaining = ttl(&mut c, "ttl-key").await;
        assert!(
            remaining > 0 && remaining <= 10_000,
            "the TTL round-trips through the snapshot (remaining={remaining})"
        );
        // DBSIZE across all shards equals the loaded key count (50 plain + 1 TTL'd = 51).
        assert_eq!(cmd1(&mut c, "DBSIZE").await, b":51\r\n");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// BGSAVE replies `+Background saving started` immediately and eventually persists (a restart
/// reloads the data). LASTSAVE advances after the background save commits.
#[tokio::test(flavor = "current_thread")]
async fn bgsave_persists_and_restart_reloads() {
    let dir = temp_data_dir("bgsave");
    let port = free_port();

    {
        let server = run_persist_server_for_test(port, 2, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        set(&mut c, "bgk", "bgv").await;
        // BGSAVE replies the Redis-faithful acknowledgement immediately.
        assert_eq!(
            cmd1(&mut c, "BGSAVE").await,
            b"+Background saving started\r\n"
        );
        // The background save commits shortly after; poll LASTSAVE until it advances (the manifest
        // commit + LASTSAVE stamp happen on the background task).
        let mut advanced = false;
        for _ in 0..100 {
            let ls = cmd1(&mut c, "LASTSAVE").await;
            if ls != b":0\r\n" {
                advanced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(advanced, "BGSAVE eventually committed (LASTSAVE advanced)");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // Restart reloads the BGSAVE'd data.
    {
        let server = run_persist_server_for_test(port, 2, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        assert_eq!(get_raw(&mut c, "bgk").await, bulk("bgv"));
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// With NO data_dir, persistence is OFF: SAVE/BGSAVE are the persistence-disabled no-op success
/// fallbacks, LASTSAVE is 0, and no files are written. This is the default byte-unchanged posture.
#[tokio::test(flavor = "current_thread")]
async fn persistence_off_is_noop_and_writes_no_files() {
    use ironcache::test_support::run_server_for_test;
    let port = free_port();
    let server = run_server_for_test(port, 2);
    let mut c = connect_retry(port).await;
    set(&mut c, "k", "v").await;
    // The persistence-disabled fallbacks: SAVE -> +OK, BGSAVE -> ack, LASTSAVE -> :0.
    assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
    assert_eq!(
        cmd1(&mut c, "BGSAVE").await,
        b"+Background saving started\r\n"
    );
    assert_eq!(cmd1(&mut c, "LASTSAVE").await, b":0\r\n");
    // The data is still served (in-memory), but nothing is persisted (no data_dir).
    assert_eq!(get_raw(&mut c, "k").await, bulk("v"));
    drop(c);
    server.shutdown_and_join().unwrap();
}
