// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end RESP smoke test: boot a real listener on an ephemeral port over the
//! tokio current-thread runtime, connect a client, and verify the Tier-0 wire
//! behavior (PROTOCOL.md acceptance: connect + PING + HELLO round trips).
//!
//! This exercises the actual decode -> dispatch -> encode path against a live
//! socket, which is the integration coverage the PR-1 gate asks for.

use ironcache_env::{Clock, SystemEnv};
use ironcache_eviction::Policy;
use ironcache_observe::{CounterSnapshot, MemoryInfo, ServerInfo};
use ironcache_protocol::{DecodeOutcome, Limits, ProtoVersion, decode, encode_to_vec};
use ironcache_runtime::tokio_rt::bind_reuseport;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, CounterDeltas, TimingWheel, UnixMillis, dispatch};
use ironcache_storage::CountingAccounting;
use ironcache_store::{ShardStore, process_memory};
use std::cell::RefCell;

// jemalloc as this test binary's global allocator so the INFO used_memory figure
// (process-global stats.allocated) is live, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn ctx(port: u16, pass: Option<&str>) -> ServerContext {
    let boot = ironcache_config::Config {
        port,
        databases: 16,
        shards: 1,
        requirepass: pass.map(str::to_owned),
        ..ironcache_config::Config::default()
    };
    let runtime = ironcache_config::RuntimeConfig::from_config(&boot);
    ServerContext {
        runtime,
        databases: 16,
        shards: 1,
        info: ServerInfo {
            tcp_port: port,
            shards: 1,
            pid: std::process::id(),
            started_at: ironcache_env::Monotonic::ZERO,
            maxmemory: 0,
            maxmemory_policy: "allkeys-lru",
            mem_allocator: "jemalloc",
        },
        boot,
    }
}

/// Serve a single connection: decode requests, dispatch, encode replies, until
/// the peer closes or QUIT.
async fn serve_one(mut stream: tokio::net::TcpStream, ctx: ServerContext) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut env = SystemEnv::new();
    // A real per-connection store for the test server, constructed exactly as the
    // binary's per-shard store is (the concrete ShardStore over the waist, wired with
    // the cache-mode eviction policy so it satisfies the Admit bound dispatch needs).
    let mut store = ShardStore::with_hooks(
        ctx.databases,
        Policy::cache_default(),
        CountingAccounting::new(),
    );
    // The per-shard timing wheel (#51), owned alongside the store as the binary does.
    let mut wheel = TimingWheel::new();
    let counters = RefCell::new(CounterSnapshot::default());
    let mut conn = ConnState::new(
        1,
        ProtoVersion::Resp2,
        ctx.requires_auth(),
        "test".to_owned(),
        "test".to_owned(),
    );
    let limits = Limits::default();
    let mut buf: Vec<u8> = Vec::new();
    // This connection's last-seen runtime-config generation (PR-4b): dispatch advances
    // it on a CONFIG SET maxmemory-policy and swaps this store's policy.
    let mut shard_gen = ctx.runtime.generation();
    loop {
        // Drain complete frames.
        loop {
            match decode(&buf, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    let rollup = || *counters.borrow();
                    let now = UnixMillis(env.now_unix_millis());
                    // Read the process-global allocator figures for INFO (ADR-0006),
                    // mirroring the server binary's once-per-INFO single-snapshot read
                    // (one epoch advance -> allocated + resident from the same snapshot).
                    let mem = if request.command().eq_ignore_ascii_case(b"INFO") {
                        let (used_memory, used_memory_rss) = process_memory();
                        MemoryInfo {
                            used_memory,
                            used_memory_rss,
                        }
                    } else {
                        MemoryInfo::default()
                    };
                    let mut deltas = CounterDeltas::default();
                    let reply = dispatch(
                        &ctx,
                        &mut conn,
                        &mut env,
                        &mut store,
                        &mut wheel,
                        now,
                        &mut shard_gen,
                        &rollup,
                        mem,
                        &mut deltas,
                        &request,
                    );
                    let bytes = encode_to_vec(&reply, conn.proto);
                    if stream.write_all(&bytes).await.is_err() {
                        return;
                    }
                    buf.drain(..consumed);
                    if conn.should_close {
                        return;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    let bytes = encode_to_vec(&ironcache_server::Value::Error(e), conn.proto);
                    let _ = stream.write_all(&bytes).await;
                    return;
                }
            }
        }
        let mut tmp = [0u8; 4096];
        match stream.read(&mut tmp).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
}

#[test]
fn ping_hello_select_over_real_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server_ctx = ctx(addr.port(), None);

        // Accept exactly one connection and serve it on the shard's LocalSet.
        let acceptor = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            serve_one(stream, server_ctx).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // 1. PING -> +PONG
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut r = [0u8; 7];
        client.read_exact(&mut r).await.unwrap();
        assert_eq!(&r, b"+PONG\r\n");

        // 2. Inline PING (netcat ergonomics).
        client.write_all(b"PING\r\n").await.unwrap();
        let mut r2 = [0u8; 7];
        client.read_exact(&mut r2).await.unwrap();
        assert_eq!(&r2, b"+PONG\r\n");

        // 3. HELLO 3 -> a RESP3 map (starts with '%').
        client
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .await
            .unwrap();
        let mut hbuf = [0u8; 256];
        let n = client.read(&mut hbuf).await.unwrap();
        assert!(hbuf[0] == b'%', "expected RESP3 map, got {:?}", &hbuf[..n]);

        // 4. SELECT 0 -> +OK
        client
            .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")
            .await
            .unwrap();
        let mut ok = [0u8; 5];
        client.read_exact(&mut ok).await.unwrap();
        assert_eq!(&ok, b"+OK\r\n");

        // 5. QUIT -> +OK then close.
        client.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
        let mut q = [0u8; 5];
        client.read_exact(&mut q).await.unwrap();
        assert_eq!(&q, b"+OK\r\n");
        // Peer should now be closed: a read returns 0.
        let mut tail = [0u8; 1];
        let n = client.read(&mut tail).await.unwrap_or(0);
        assert_eq!(n, 0, "server did not close after QUIT");

        acceptor.await.unwrap();
    });
}

/// Read exactly `expect.len()` bytes from `client` and assert they match `expect`.
async fn expect_reply(client: &mut tokio::net::TcpStream, expect: &[u8]) {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; expect.len()];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, expect, "got {:?}", String::from_utf8_lossy(&buf));
}

#[test]
fn data_commands_over_real_socket() {
    use tokio::io::AsyncWriteExt;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server_ctx = ctx(addr.port(), None);
        let acceptor = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            serve_one(stream, server_ctx).await;
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // SET foo bar -> +OK
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;

        // GET foo -> $3\r\nbar\r\n
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$3\r\nbar\r\n").await;

        // SET k v NX -> +OK ; SET k v2 NX -> $-1 (RESP2 null bulk)
        client
            .write_all(b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv2\r\n$2\r\nNX\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$-1\r\n").await;

        // SET k v2 XX GET -> old value "v"
        client
            .write_all(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv2\r\n$2\r\nXX\r\n$3\r\nGET\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$1\r\nv\r\n").await;

        // DEL foo k -> :2
        client
            .write_all(b"*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;

        // EXISTS foo -> :0
        client
            .write_all(b"*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":0\r\n").await;

        // TYPE on a missing key -> +none
        client
            .write_all(b"*2\r\n$4\r\nTYPE\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+none\r\n").await;

        // SET typed v, TYPE -> +string, STRLEN -> :1
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\ntyped\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*2\r\n$4\r\nTYPE\r\n$5\r\ntyped\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+string\r\n").await;
        client
            .write_all(b"*2\r\n$6\r\nSTRLEN\r\n$5\r\ntyped\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":1\r\n").await;

        drop(client);
        acceptor.await.unwrap();
    });
}

#[test]
fn numeric_append_and_info_over_real_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server_ctx = ctx(addr.port(), None);
        let acceptor = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            serve_one(stream, server_ctx).await;
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // SET n 10 ; INCR n -> :11 ; INCRBY n 5 -> :16 ; DECR n -> :15.
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nn\r\n$2\r\n10\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":11\r\n").await;
        client
            .write_all(b"*3\r\n$6\r\nINCRBY\r\n$1\r\nn\r\n$1\r\n5\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":16\r\n").await;
        client
            .write_all(b"*2\r\n$4\r\nDECR\r\n$1\r\nn\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":15\r\n").await;

        // APPEND s abc -> :3 ; APPEND s de -> :5 ; GET s -> $5 abcde.
        client
            .write_all(b"*3\r\n$6\r\nAPPEND\r\n$1\r\ns\r\n$3\r\nabc\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;
        client
            .write_all(b"*3\r\n$6\r\nAPPEND\r\n$1\r\ns\r\n$2\r\nde\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":5\r\n").await;
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$5\r\nabcde\r\n").await;

        // INCRBYFLOAT f 10.5 -> $4 10.5 (a bulk string in both RESP2 and RESP3).
        client
            .write_all(b"*3\r\n$11\r\nINCRBYFLOAT\r\n$1\r\nf\r\n$4\r\n10.5\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$4\r\n10.5\r\n").await;

        // INFO memory: used_memory must be > 0 (the process-global jemalloc figure).
        client
            .write_all(b"*2\r\n$4\r\nINFO\r\n$6\r\nmemory\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 2048];
        let n = client.read(&mut buf).await.unwrap();
        let body = String::from_utf8_lossy(&buf[..n]);
        let line = body
            .lines()
            .find(|l| l.starts_with("used_memory:"))
            .expect("INFO memory has a used_memory line");
        let val: u64 = line
            .trim_start_matches("used_memory:")
            .trim()
            .parse()
            .expect("used_memory is an integer");
        assert!(
            val > 0,
            "INFO used_memory should be > 0, got {val} ({body})"
        );

        // RESP3 leg: switch the connection to RESP3 (HELLO 3 -> a map, '%'), then
        // INCRBYFLOAT must STILL reply with a bulk string ($<len>\r\n<digits>\r\n),
        // NOT a RESP3 `,double` (ADR-0019: INCRBYFLOAT is bulk in both protocols).
        client
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .await
            .unwrap();
        let mut hbuf = [0u8; 256];
        let hn = client.read(&mut hbuf).await.unwrap();
        assert_eq!(hbuf[0], b'%', "expected RESP3 map, got {:?}", &hbuf[..hn]);
        // INCRBYFLOAT g 3.25 on an absent key -> "3.25" (non-integer, so it keeps a
        // dot and is unambiguously a bulk string, not a `,double`).
        client
            .write_all(b"*3\r\n$11\r\nINCRBYFLOAT\r\n$1\r\ng\r\n$4\r\n3.25\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$4\r\n3.25\r\n").await;

        drop(client);
        acceptor.await.unwrap();
    });
}

#[test]
fn keyspace_commands_over_real_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server_ctx = ctx(addr.port(), None);
        let acceptor = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            serve_one(stream, server_ctx).await;
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // SET three keys.
        for (k, klen) in [("k1", 2usize), ("k2", 2), ("k3", 2)] {
            let cmd = format!("*3\r\n$3\r\nSET\r\n${klen}\r\n{k}\r\n$1\r\nv\r\n");
            client.write_all(cmd.as_bytes()).await.unwrap();
            expect_reply(&mut client, b"+OK\r\n").await;
        }

        // DBSIZE -> :3
        client.write_all(b"*1\r\n$6\r\nDBSIZE\r\n").await.unwrap();
        expect_reply(&mut client, b":3\r\n").await;

        // SCAN 0 COUNT 100 -> a 2-element array; collect every key to completion.
        let mut collected: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cursor = String::from("0");
        loop {
            let curlen = cursor.len();
            let cmd =
                format!("*4\r\n$4\r\nSCAN\r\n${curlen}\r\n{cursor}\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n");
            client.write_all(cmd.as_bytes()).await.unwrap();
            // Read a chunk and parse the 2-array reply (small, fits one read here).
            let mut buf = [0u8; 1024];
            let n = client.read(&mut buf).await.unwrap();
            let (next, keys) = parse_scan_reply(&buf[..n]);
            for k in keys {
                collected.insert(k);
            }
            if next == "0" {
                break;
            }
            cursor = next;
        }
        assert_eq!(collected.len(), 3, "SCAN to completion collected all keys");
        assert!(collected.contains("k1") && collected.contains("k2") && collected.contains("k3"));

        // KEYS k* -> array containing all three (just assert the array header count).
        client
            .write_all(b"*2\r\n$4\r\nKEYS\r\n$2\r\nk*\r\n")
            .await
            .unwrap();
        let mut kbuf = [0u8; 256];
        let kn = client.read(&mut kbuf).await.unwrap();
        assert_eq!(kbuf[0], b'*', "KEYS reply is an array");
        assert!(
            String::from_utf8_lossy(&kbuf[..kn]).starts_with("*3\r\n"),
            "KEYS k* -> 3 keys"
        );

        // OBJECT ENCODING k1 -> embstr ($6\r\nembstr\r\n).
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$2\r\nk1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$6\r\nembstr\r\n").await;

        // RENAME k1 k1b -> +OK ; GET k1b -> v ; GET k1 -> null.
        client
            .write_all(b"*3\r\n$6\r\nRENAME\r\n$2\r\nk1\r\n$3\r\nk1b\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nk1b\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$1\r\nv\r\n").await;

        // FLUSHDB -> +OK ; DBSIZE -> :0.
        client.write_all(b"*1\r\n$7\r\nFLUSHDB\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client.write_all(b"*1\r\n$6\r\nDBSIZE\r\n").await.unwrap();
        expect_reply(&mut client, b":0\r\n").await;

        drop(client);
        acceptor.await.unwrap();
    });
}

/// Minimal RESP2 parser for a SCAN reply `*2\r\n$<n>\r\n<cursor>\r\n*<m>\r\n($<l>\r\n<key>\r\n)*`,
/// returning the next cursor string and the key strings. Sufficient for the small
/// keyspaces the e2e test uses (one read per SCAN call).
fn parse_scan_reply(buf: &[u8]) -> (String, Vec<String>) {
    let s = String::from_utf8_lossy(buf);
    let mut lines = s.split("\r\n");
    assert_eq!(lines.next(), Some("*2"), "SCAN reply is a 2-array");
    // The cursor: a bulk string ($len then the bytes).
    let cur_hdr = lines.next().unwrap_or("");
    assert!(
        cur_hdr.starts_with('$'),
        "cursor bulk header, got {cur_hdr}"
    );
    let cursor = lines.next().unwrap_or("").to_string();
    // The keys array header `*m`.
    let arr_hdr = lines.next().unwrap_or("");
    assert!(arr_hdr.starts_with('*'), "keys array header, got {arr_hdr}");
    let m: usize = arr_hdr[1..].parse().unwrap_or(0);
    let mut keys = Vec::new();
    for _ in 0..m {
        let _len = lines.next(); // $len
        if let Some(k) = lines.next() {
            keys.push(k.to_string());
        }
    }
    (cursor, keys)
}

#[test]
fn unknown_command_error_over_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server_ctx = ctx(addr.port(), None);
        let acceptor = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_one(stream, server_ctx).await;
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(b"*1\r\n$3\r\nFOO\r\n").await.unwrap();
        let mut buf = [0u8; 256];
        let n = client.read(&mut buf).await.unwrap();
        let s = String::from_utf8_lossy(&buf[..n]);
        assert!(s.starts_with("-ERR unknown command 'FOO'"), "got: {s}");
        drop(client);
        acceptor.await.unwrap();
    });
}
