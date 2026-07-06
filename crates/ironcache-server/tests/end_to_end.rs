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
use std::rc::Rc;

// jemalloc as this test binary's global allocator so the INFO used_memory figure
// (process-global stats.allocated) is live, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn ctx(port: u16, pass: Option<&str>) -> ServerContext {
    ctx_cfg(port, pass, false)
}

/// Build a server context on `port`, optionally requiring `pass`, with cluster mode set by
/// `cluster_enabled`. The cluster slice-1 e2e test boots BOTH a disabled node (the default,
/// `cluster-enabled no`) and an enabled single-node cluster (`cluster-enabled yes`) to
/// exercise the gate in `cmd_cluster` over a real socket (CLUSTER_CONTRACT.md #70).
fn ctx_cfg(port: u16, pass: Option<&str>, cluster_enabled: bool) -> ServerContext {
    let boot = ironcache_config::Config {
        port,
        databases: 16,
        shards: 1,
        requirepass: pass.map(str::to_owned),
        cluster_enabled,
        ..ironcache_config::Config::default()
    };
    let runtime = ironcache_config::RuntimeConfig::from_config(&boot);
    let acl = ironcache_server::AclState::from_requirepass(boot.requirepass.as_deref());
    ServerContext {
        runtime,
        acl,
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
            // A fixed 40-hex node id for the test harness (the real binary draws one at
            // boot from the SystemEnv RNG); cluster mode per `cluster_enabled`
            // (CLUSTER_CONTRACT.md #70).
            cluster_node_id: "abcdef0123456789abcdef0123456789abcdef01",
            cluster_enabled,
        },
        cluster: None,
        raft: None,
        repl_status: None,
        in_sync_replicas: None,
        repl_history_id: None,
        metrics_registry: None,
        persist_stats: None,
        process_memory: std::sync::Arc::new(ironcache_observe::ProcessMemoryGauge::new()),
        conn_gate: std::sync::Arc::new(ironcache_observe::ConnectionGate::new()),
        slowlog: std::sync::Arc::new(ironcache_observe::SlowLog::new()),
        latency: std::sync::Arc::new(ironcache_observe::LatencyMonitor::new()),
        clients: std::sync::Arc::new(ironcache_observe::ClientRegistry::new()),
        hotkeys: std::sync::Arc::new(ironcache_observe::Hotkeys::new()),
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
                        &|| (String::new(), String::new()),
                        &|| None,
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
fn list_commands_over_real_socket() {
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

        // RPUSH mylist a b c -> :3
        client
            .write_all(b"*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;

        // LLEN mylist -> :3
        client
            .write_all(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;

        // TYPE mylist -> +list
        client
            .write_all(b"*2\r\n$4\r\nTYPE\r\n$6\r\nmylist\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+list\r\n").await;

        // LRANGE mylist 0 -1 -> *3 a b c
        client
            .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await;

        // LPOP mylist -> $1 a
        client
            .write_all(b"*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$1\r\na\r\n").await;

        // OBJECT ENCODING mylist -> listpack (small).
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$6\r\nmylist\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$8\r\nlistpack\r\n").await;

        // Push a value over the 8 KB listpack budget -> quicklist. Build the RPUSH
        // frame with a 9000-byte element.
        let big = vec![b'q'; 9000];
        let mut frame =
            format!("*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n${}\r\n", big.len()).into_bytes();
        frame.extend_from_slice(&big);
        frame.extend_from_slice(b"\r\n");
        client.write_all(&frame).await.unwrap();
        // Reply :3 (b, c, + big).
        expect_reply(&mut client, b":3\r\n").await;

        // OBJECT ENCODING mylist -> quicklist now.
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$6\r\nmylist\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(
            &buf[..n],
            b"$9\r\nquicklist\r\n",
            "got {:?}",
            String::from_utf8_lossy(&buf[..n])
        );

        // WRONGTYPE: SET a string key, then a list command on it errors.
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nstr\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*3\r\n$5\r\nLPUSH\r\n$3\r\nstr\r\n$1\r\nx\r\n")
            .await
            .unwrap();
        let mut wbuf = [0u8; 128];
        let wn = client.read(&mut wbuf).await.unwrap();
        assert!(
            String::from_utf8_lossy(&wbuf[..wn]).starts_with("-WRONGTYPE"),
            "got {:?}",
            String::from_utf8_lossy(&wbuf[..wn])
        );

        drop(client);
        acceptor.await.unwrap();
    });
}

#[test]
fn hash_commands_over_real_socket() {
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

        // HSET h f1 v1 f2 v2 -> :2 (two new fields).
        client
            .write_all(
                b"*6\r\n$4\r\nHSET\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
            )
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;

        // TYPE h -> +hash.
        client
            .write_all(b"*2\r\n$4\r\nTYPE\r\n$1\r\nh\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+hash\r\n").await;

        // HGET h f1 -> $2 v1.
        client
            .write_all(b"*3\r\n$4\r\nHGET\r\n$1\r\nh\r\n$2\r\nf1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$2\r\nv1\r\n").await;

        // HGETALL h under RESP2 -> a flat 4-element array (field,value,field,value).
        client
            .write_all(b"*2\r\n$7\r\nHGETALL\r\n$1\r\nh\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let n = client.read(&mut buf).await.unwrap();
        let body = String::from_utf8_lossy(&buf[..n]);
        assert!(
            body.starts_with("*4\r\n"),
            "HGETALL under RESP2 is a flat 4-array, got {body:?}"
        );
        assert!(body.contains("v1") && body.contains("v2"), "got {body:?}");

        // OBJECT ENCODING h -> listpack (small).
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\nh\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$8\r\nlistpack\r\n").await;

        // HSET a value over the 64-byte cap -> hashtable encoding.
        let big = vec![b'q'; 100];
        let mut frame = format!(
            "*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$3\r\nbig\r\n${}\r\n",
            big.len()
        )
        .into_bytes();
        frame.extend_from_slice(&big);
        frame.extend_from_slice(b"\r\n");
        client.write_all(&frame).await.unwrap();
        expect_reply(&mut client, b":1\r\n").await;
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\nh\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$9\r\nhashtable\r\n").await;

        // HDEL h f1 f2 big -> :3 ; the hash is now empty so the key is gone.
        client
            .write_all(b"*5\r\n$4\r\nHDEL\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nf2\r\n$3\r\nbig\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;
        // EXISTS h -> :0 (empty hash deletes the key).
        client
            .write_all(b"*2\r\n$6\r\nEXISTS\r\n$1\r\nh\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":0\r\n").await;

        // WRONGTYPE: a hash command on a string key.
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nstr\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*4\r\n$4\r\nHSET\r\n$3\r\nstr\r\n$1\r\na\r\n$1\r\nx\r\n")
            .await
            .unwrap();
        let mut wbuf = [0u8; 128];
        let wn = client.read(&mut wbuf).await.unwrap();
        assert!(
            String::from_utf8_lossy(&wbuf[..wn]).starts_with("-WRONGTYPE"),
            "got {:?}",
            String::from_utf8_lossy(&wbuf[..wn])
        );

        drop(client);
        acceptor.await.unwrap();
    });
}

#[test]
fn set_commands_over_real_socket() {
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

        // SADD s 1 2 3 -> :3 (three new integer members; stays intset).
        client
            .write_all(b"*5\r\n$4\r\nSADD\r\n$1\r\ns\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;

        // TYPE s -> +set.
        client
            .write_all(b"*2\r\n$4\r\nTYPE\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+set\r\n").await;

        // SCARD s -> :3.
        client
            .write_all(b"*2\r\n$5\r\nSCARD\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;

        // OBJECT ENCODING s -> intset (all-integer, small).
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$6\r\nintset\r\n").await;

        // SMEMBERS s -> a 3-element set (RESP3 `~`), here degrading to a `*` array under the
        // RESP2 default of a fresh connection (intset is ascending: 1,2,3).
        client
            .write_all(b"*2\r\n$8\r\nSMEMBERS\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n").await;

        // A member over the 64-byte cap (after a non-integer forces listpack, then the big
        // member forces hashtable): OBJECT ENCODING s -> hashtable.
        client
            .write_all(b"*3\r\n$4\r\nSADD\r\n$1\r\ns\r\n$2\r\nxy\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":1\r\n").await;
        let big = vec![b'q'; 100];
        let mut frame = format!("*3\r\n$4\r\nSADD\r\n$1\r\ns\r\n${}\r\n", big.len()).into_bytes();
        frame.extend_from_slice(&big);
        frame.extend_from_slice(b"\r\n");
        client.write_all(&frame).await.unwrap();
        expect_reply(&mut client, b":1\r\n").await;
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$9\r\nhashtable\r\n").await;

        drop(client);
        acceptor.await.unwrap();
    });
}

#[test]
fn set_store_and_wrongtype_over_real_socket() {
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

        // SINTERSTORE over a socket: a={1,2,3}, b={2,3,4} -> dest={2,3} (cardinality 2).
        client
            .write_all(b"*5\r\n$4\r\nSADD\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;
        client
            .write_all(b"*5\r\n$4\r\nSADD\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;
        client
            .write_all(b"*4\r\n$11\r\nSINTERSTORE\r\n$4\r\ndest\r\n$1\r\na\r\n$1\r\nb\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;
        // SMEMBERS dest -> {2,3} (intset ascending; RESP3 set degrading to `*` under RESP2).
        client
            .write_all(b"*2\r\n$8\r\nSMEMBERS\r\n$4\r\ndest\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"*2\r\n$1\r\n2\r\n$1\r\n3\r\n").await;

        // SREM dest 2 3 empties dest -> the key is gone (EXISTS dest -> :0).
        client
            .write_all(b"*4\r\n$4\r\nSREM\r\n$4\r\ndest\r\n$1\r\n2\r\n$1\r\n3\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;
        client
            .write_all(b"*2\r\n$6\r\nEXISTS\r\n$4\r\ndest\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":0\r\n").await;

        // WRONGTYPE: a set command on a string key.
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nstr\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*3\r\n$4\r\nSADD\r\n$3\r\nstr\r\n$1\r\na\r\n")
            .await
            .unwrap();
        expect_reply(
            &mut client,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        )
        .await;

        drop(client);
        acceptor.await.unwrap();
    });
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

// A socket round-trip over the full zset surface (ZADD/ZSCORE/ZRANGE/WITHSCORES/
// ZRANGEBYSCORE/ZPOPMIN + the listpack->skiplist transition + RESP3 WITHSCORES nesting)
// is inherently long; the steps are linear write/expect pairs.
#[allow(clippy::too_many_lines)]
#[test]
fn zset_commands_over_real_socket() {
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

        // ZADD z 1 a 2 b 3 c -> :3.
        client
            .write_all(
                b"*8\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n",
            )
            .await
            .unwrap();
        expect_reply(&mut client, b":3\r\n").await;

        // TYPE z -> +zset.
        client
            .write_all(b"*2\r\n$4\r\nTYPE\r\n$1\r\nz\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+zset\r\n").await;

        // OBJECT ENCODING z -> listpack (small).
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\nz\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$8\r\nlistpack\r\n").await;

        // ZSCORE z b -> $1 2 (bulk).
        client
            .write_all(b"*3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\nb\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$1\r\n2\r\n").await;

        // ZRANGE z 0 -1 -> [a, b, c].
        client
            .write_all(b"*4\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n-1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await;

        // ZRANGE z 0 -1 WITHSCORES -> RESP2 flat [a,1,b,2,c,3].
        client
            .write_all(
                b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n",
            )
            .await
            .unwrap();
        expect_reply(
            &mut client,
            b"*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n",
        )
        .await;

        // ZRANGEBYSCORE z (1 +inf -> [b, c].
        client
            .write_all(
                b"*4\r\n$13\r\nZRANGEBYSCORE\r\n$1\r\nz\r\n$2\r\n(1\r\n$4\r\n+inf\r\n",
            )
            .await
            .unwrap();
        expect_reply(&mut client, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n").await;

        // ZPOPMIN z -> [a, 1] (member + score interleaved under RESP2).
        client
            .write_all(b"*2\r\n$7\r\nZPOPMIN\r\n$1\r\nz\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"*2\r\n$1\r\na\r\n$1\r\n1\r\n").await;

        // Drive the listpack->skiplist transition with a >64-byte member.
        let big = vec![b'q'; 100];
        let mut frame =
            format!("*4\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n9\r\n${}\r\n", big.len()).into_bytes();
        frame.extend_from_slice(&big);
        frame.extend_from_slice(b"\r\n");
        client.write_all(&frame).await.unwrap();
        expect_reply(&mut client, b":1\r\n").await;
        client
            .write_all(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\nz\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$8\r\nskiplist\r\n").await;

        // Switch to RESP3 (HELLO 3 -> a map) then verify WITHSCORES nests under RESP3.
        client
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .await
            .unwrap();
        let mut hbuf = [0u8; 512];
        let hn = client.read(&mut hbuf).await.unwrap();
        assert_eq!(hbuf[0], b'%', "expected RESP3 map, got {:?}", &hbuf[..hn]);
        // A fresh small zset for a clean WITHSCORES shape under RESP3.
        client
            .write_all(
                b"*6\r\n$4\r\nZADD\r\n$2\r\nz3\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n",
            )
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;
        client
            .write_all(
                b"*5\r\n$6\r\nZRANGE\r\n$2\r\nz3\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n",
            )
            .await
            .unwrap();
        // RESP3: an array of [member, ,double] 2-arrays.
        expect_reply(
            &mut client,
            b"*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2\r\n",
        )
        .await;

        drop(client);
        acceptor.await.unwrap();
    });
}

// A socket round-trip over the full transaction surface (MULTI/EXEC/DISCARD + the
// EXECABORT dirty path + the no-rollback runtime-error element + empty MULTI;EXEC + the
// control-verb-dirties case) is inherently long; the steps are linear write/expect pairs.
#[allow(clippy::too_many_lines)]
#[test]
fn transactions_over_real_socket() {
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

        // MULTI -> +OK.
        client.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        // SET k 1 -> +QUEUED (a simple string).
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+QUEUED\r\n").await;
        // INCR k -> +QUEUED.
        client
            .write_all(b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+QUEUED\r\n").await;
        // EXEC -> the per-command reply array: *2 then +OK then :2.
        client.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(&mut client, b"*2\r\n+OK\r\n:2\r\n").await;
        // The batch applied: GET k -> "2".
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$1\r\n2\r\n").await;

        // DISCARD path: MULTI, queue a write, DISCARD, confirm it did not apply.
        client.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nd\r\n$1\r\n9\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+QUEUED\r\n").await;
        client.write_all(b"*1\r\n$7\r\nDISCARD\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        // GET d -> null bulk (the discarded SET never applied).
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nd\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$-1\r\n").await;

        // EXEC without MULTI -> the control error.
        client.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(&mut client, b"-ERR EXEC without MULTI\r\n").await;

        // (a) EXECABORT dirty path: queue an UNKNOWN command inside MULTI (the queue-time
        // error is reported now + dirties the txn), then EXEC -> the byte-exact EXECABORT.
        client.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        // FROBNICATE a -> the unknown-command error reported now (txn dirtied).
        client
            .write_all(b"*2\r\n$10\r\nFROBNICATE\r\n$1\r\na\r\n")
            .await
            .unwrap();
        {
            use tokio::io::AsyncReadExt;
            let mut ebuf = [0u8; 128];
            let en = client.read(&mut ebuf).await.unwrap();
            assert!(
                String::from_utf8_lossy(&ebuf[..en])
                    .starts_with("-ERR unknown command 'FROBNICATE'"),
                "got {:?}",
                String::from_utf8_lossy(&ebuf[..en])
            );
        }
        client.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(
            &mut client,
            b"-EXECABORT Transaction discarded because of previous errors.\r\n",
        )
        .await;

        // (b) No-rollback runtime-error element: SET s hello; MULTI; INCR s (fails at run
        // time); SET s2 ok (must still apply); EXEC -> a *2 array whose first element is
        // the not-an-integer -ERR and whose second is +OK; then GET s2 -> "ok".
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\ns\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*2\r\n$4\r\nINCR\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+QUEUED\r\n").await;
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\ns2\r\n$2\r\nok\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+QUEUED\r\n").await;
        client.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(
            &mut client,
            b"*2\r\n-ERR value is not an integer or out of range\r\n+OK\r\n",
        )
        .await;
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$2\r\ns2\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$2\r\nok\r\n").await;

        // (c) Empty MULTI;EXEC -> the empty array *0.
        client.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(&mut client, b"*0\r\n").await;

        // (d) Control-verb-dirties (fix 1): MULTI; EXEC x (wrong arity dirties the open
        // txn); EXEC -> EXECABORT. The bad-arity EXEC replies its arity error first.
        client.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        client
            .write_all(b"*2\r\n$4\r\nEXEC\r\n$1\r\nx\r\n")
            .await
            .unwrap();
        expect_reply(
            &mut client,
            b"-ERR wrong number of arguments for 'exec' command\r\n",
        )
        .await;
        client.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(
            &mut client,
            b"-EXECABORT Transaction discarded because of previous errors.\r\n",
        )
        .await;

        drop(client);
        acceptor.await.unwrap();
    });
}

#[test]
fn bitmap_commands_over_real_socket() {
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

        // SETBIT bk 7 1 -> :0 (old bit), creates the key.
        client
            .write_all(b"*4\r\n$6\r\nSETBIT\r\n$2\r\nbk\r\n$1\r\n7\r\n$1\r\n1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":0\r\n").await;

        // GETBIT bk 7 -> :1 ; GETBIT bk 6 -> :0.
        client
            .write_all(b"*3\r\n$6\r\nGETBIT\r\n$2\r\nbk\r\n$1\r\n7\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":1\r\n").await;
        client
            .write_all(b"*3\r\n$6\r\nGETBIT\r\n$2\r\nbk\r\n$1\r\n6\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":0\r\n").await;

        // BITCOUNT bk -> :1 (one set bit).
        client
            .write_all(b"*2\r\n$8\r\nBITCOUNT\r\n$2\r\nbk\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":1\r\n").await;

        // GET bk -> the single byte 0x01 (a bitmap is the string type).
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$2\r\nbk\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$1\r\n\x01\r\n").await;

        // TYPE bk -> +string.
        client
            .write_all(b"*2\r\n$4\r\nTYPE\r\n$2\r\nbk\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"+string\r\n").await;

        // Two source bitmaps: SETBIT s1 0 1 (byte0 0x80) ; SETBIT s2 1 1 (byte0 0x40).
        client
            .write_all(b"*4\r\n$6\r\nSETBIT\r\n$2\r\ns1\r\n$1\r\n0\r\n$1\r\n1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":0\r\n").await;
        client
            .write_all(b"*4\r\n$6\r\nSETBIT\r\n$2\r\ns2\r\n$1\r\n1\r\n$1\r\n1\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":0\r\n").await;
        // BITOP OR dst s1 s2 -> :1 (1-byte result, byte0 = 0xC0).
        client
            .write_all(b"*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$3\r\ndst\r\n$2\r\ns1\r\n$2\r\ns2\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":1\r\n").await;
        // GET dst -> 0xC0 ; BITCOUNT dst -> :2.
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\ndst\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b"$1\r\n\xc0\r\n").await;
        client
            .write_all(b"*2\r\n$8\r\nBITCOUNT\r\n$3\r\ndst\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":2\r\n").await;

        drop(client);
        acceptor.await.unwrap();
    });
}

/// The concrete shared per-shard store the cross-connection WATCH e2e test drives: an
/// `Rc<RefCell<ShardStore>>` shared between connection tasks on ONE LocalSet, exactly as
/// the binary shares its per-shard store across the connections on a shard thread.
type SharedStore = Rc<RefCell<ShardStore<Policy, CountingAccounting>>>;

/// Serve a single connection against a SHARED store (PR-10b cross-connection WATCH).
/// Like [`serve_one`] but the store is supplied so several connections see the SAME
/// keyspace + WATCH version slots (single-shard-per-connection). On close it deregisters
/// this connection's WATCHes from the store, mirroring the serve-loop teardown in serve.rs.
async fn serve_one_shared(
    mut stream: tokio::net::TcpStream,
    ctx: ServerContext,
    store: SharedStore,
) {
    use ironcache_storage::Watch;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut env = SystemEnv::new();
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
    let mut shard_gen = ctx.runtime.generation();
    'outer: loop {
        loop {
            match decode(&buf, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    let rollup = || *counters.borrow();
                    let now = UnixMillis(env.now_unix_millis());
                    let mut deltas = CounterDeltas::default();
                    let reply = {
                        let mut st = store.borrow_mut();
                        dispatch(
                            &ctx,
                            &mut conn,
                            &mut env,
                            &mut *st,
                            &mut wheel,
                            now,
                            &mut shard_gen,
                            &rollup,
                            &|| (String::new(), String::new()),
                            &|| None,
                            MemoryInfo::default(),
                            &mut deltas,
                            &request,
                        )
                    };
                    let bytes = encode_to_vec(&reply, conn.proto);
                    if stream.write_all(&bytes).await.is_err() {
                        break 'outer;
                    }
                    buf.drain(..consumed);
                    if conn.should_close {
                        break 'outer;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    let bytes = encode_to_vec(&ironcache_server::Value::Error(e), conn.proto);
                    let _ = stream.write_all(&bytes).await;
                    break 'outer;
                }
            }
        }
        let mut tmp = [0u8; 4096];
        match stream.read(&mut tmp).await {
            Ok(0) | Err(_) => break 'outer,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
    // Connection-close watch deregistration (PR-10b), mirroring serve.rs teardown.
    if !conn.watch.is_empty() {
        store.borrow_mut().unwatch(&conn.watch);
        conn.clear_watch();
    }
}

// A socket round-trip over WATCH/MULTI/EXEC: the happy (CAS pass) path on one connection,
// then a genuine cross-connection CAS ABORT where a SECOND connection writes the watched
// key (on the SAME shared store) while the first holds an open WATCH+MULTI. Linear
// write/expect pairs, so the length is inherent.
#[allow(clippy::too_many_lines)]
#[test]
fn watch_multi_exec_over_real_socket() {
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
        // ONE shared store backs ALL connections (single-shard-per-connection).
        let store: SharedStore = Rc::new(RefCell::new(ShardStore::with_hooks(
            server_ctx.databases,
            Policy::cache_default(),
            CountingAccounting::new(),
        )));
        let store_for_acceptor = Rc::clone(&store);
        // Accept the watcher (c1) and the external writer (c2) concurrently on the
        // LocalSet, each served against the shared store. We drive them interleaved from
        // the test so c2's write lands between c1's WATCH and c1's EXEC.
        let acceptor = tokio::task::spawn_local(async move {
            let (s1, _) = listener.accept().await.unwrap();
            let _ = s1.set_nodelay(true);
            let store_a = Rc::clone(&store_for_acceptor);
            let ctx_a = server_ctx.clone();
            let h1 = tokio::task::spawn_local(async move {
                serve_one_shared(s1, ctx_a, store_a).await;
            });
            let (s2, _) = listener.accept().await.unwrap();
            let _ = s2.set_nodelay(true);
            let store_b = Rc::clone(&store_for_acceptor);
            let ctx_b = server_ctx.clone();
            let h2 = tokio::task::spawn_local(async move {
                serve_one_shared(s2, ctx_b, store_b).await;
            });
            h1.await.unwrap();
            h2.await.unwrap();
        });

        // -- Part 1: the WATCH/MULTI/EXEC happy path (CAS passes) on connection 1. --
        let mut c1 = tokio::net::TcpStream::connect(addr).await.unwrap();
        c1.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n")
            .await
            .unwrap();
        expect_reply(&mut c1, b"+OK\r\n").await;
        c1.write_all(b"*2\r\n$5\r\nWATCH\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut c1, b"+OK\r\n").await;
        c1.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut c1, b"+OK\r\n").await;
        c1.write_all(b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut c1, b"+QUEUED\r\n").await;
        c1.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(&mut c1, b"*1\r\n:2\r\n").await; // CAS passed, INCR ran
        c1.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut c1, b"$1\r\n2\r\n").await;

        // -- Part 2: cross-connection CAS ABORT. c1 WATCHes k again and opens MULTI; the
        // external writer c2 SETs k; c1's EXEC then aborts with a null array. --
        let mut c2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        c1.write_all(b"*2\r\n$5\r\nWATCH\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut c1, b"+OK\r\n").await;
        c1.write_all(b"*1\r\n$5\r\nMULTI\r\n").await.unwrap();
        expect_reply(&mut c1, b"+OK\r\n").await;
        c1.write_all(b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut c1, b"+QUEUED\r\n").await;
        // c2 (a different connection on the SAME shard) modifies the watched key.
        c2.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n99\r\n")
            .await
            .unwrap();
        expect_reply(&mut c2, b"+OK\r\n").await;
        // c1's EXEC now aborts: a null array (RESP2 *-1), nothing applied.
        c1.write_all(b"*1\r\n$4\r\nEXEC\r\n").await.unwrap();
        expect_reply(&mut c1, b"*-1\r\n").await;
        // k is c2's value, not an incremented one.
        c1.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        expect_reply(&mut c1, b"$2\r\n99\r\n").await;

        // Close both connections (c1's close path also exercises the watch teardown).
        c1.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
        expect_reply(&mut c1, b"+OK\r\n").await;
        c2.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
        expect_reply(&mut c2, b"+OK\r\n").await;
        drop(c1);
        drop(c2);
        acceptor.await.unwrap();
    });
}

/// CLUSTER command surface with cluster mode DISABLED (the slice-1 default,
/// `cluster-enabled no`, CLUSTER_CONTRACT.md #70) over a real socket. A real Redis rejects
/// EVERY CLUSTER subcommand with `-ERR This instance has cluster support disabled`; we
/// assert that for KEYSLOT and INFO, and that the INFO `# Cluster` section reports
/// `cluster_enabled:0`.
#[test]
fn cluster_disabled_over_real_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        // Default boot: cluster mode OFF.
        let server_ctx = ctx(addr.port(), None);
        let acceptor = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            serve_one(stream, server_ctx).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // CLUSTER KEYSLOT foo -> the cluster-disabled error (no introspection carve-out).
        client
            .write_all(b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        expect_reply(
            &mut client,
            b"-ERR This instance has cluster support disabled\r\n",
        )
        .await;

        // CLUSTER INFO -> the same cluster-disabled error.
        client
            .write_all(b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n")
            .await
            .unwrap();
        expect_reply(
            &mut client,
            b"-ERR This instance has cluster support disabled\r\n",
        )
        .await;

        // INFO cluster -> the `# Cluster` section reports `cluster_enabled:0`.
        client
            .write_all(b"*2\r\n$4\r\nINFO\r\n$7\r\ncluster\r\n")
            .await
            .unwrap();
        let mut cbuf = [0u8; 1024];
        let cn = client.read(&mut cbuf).await.unwrap();
        let cluster_info = String::from_utf8_lossy(&cbuf[..cn]);
        assert!(cluster_info.contains("# Cluster"), "got {cluster_info}");
        assert!(
            cluster_info.contains("cluster_enabled:0"),
            "got {cluster_info}"
        );

        client.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        acceptor.await.unwrap();
    });
}

/// CLUSTER command surface with cluster mode ENABLED (a single-node cluster owning all
/// 16384 slots, CLUSTER_CONTRACT.md #70 slice-1 simplification) over a real socket. The
/// introspection subcommands now reply: KEYSLOT computes the slot, INFO reports
/// `cluster_enabled:1` + `cluster_slots_assigned:16384`, and SLOTS renders one `0-16383`
/// range owned by self.
#[test]
fn cluster_enabled_over_real_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        // Boot in cluster-ENABLED mode (single-node cluster).
        let server_ctx = ctx_cfg(addr.port(), None, true);
        let acceptor = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            serve_one(stream, server_ctx).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // CLUSTER KEYSLOT foo -> :12182 (the reference CRC16/XMODEM vector).
        client
            .write_all(b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        expect_reply(&mut client, b":12182\r\n").await;

        // CLUSTER INFO -> cluster_enabled:1 + cluster_slots_assigned:16384 (the single-node
        // cluster owns all slots). The reply is a RESP3 verbatim string that degrades to a
        // bulk string under RESP2, so the body is in the wire payload either way.
        client
            .write_all(b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n")
            .await
            .unwrap();
        let mut ibuf = [0u8; 1024];
        let inn = client.read(&mut ibuf).await.unwrap();
        let info = String::from_utf8_lossy(&ibuf[..inn]);
        assert!(info.contains("cluster_enabled:1"), "got {info}");
        assert!(info.contains("cluster_state:ok"), "got {info}");
        assert!(info.contains("cluster_slots_assigned:16384"), "got {info}");
        assert!(info.contains("cluster_size:1"), "got {info}");

        // CLUSTER SLOTS -> one range owning 0-16383: *1 then a *3 of [0, 16383, [served-by]].
        client
            .write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n")
            .await
            .unwrap();
        let mut sbuf = [0u8; 512];
        let sn = client.read(&mut sbuf).await.unwrap();
        let slots = String::from_utf8_lossy(&sbuf[..sn]);
        // One range (outer *1), inner range starts `*3\r\n:0\r\n:16383\r\n` (start, end, ...).
        assert!(slots.starts_with("*1\r\n"), "got {slots:?}");
        assert!(slots.contains(":0\r\n"), "got {slots:?}");
        assert!(slots.contains(":16383\r\n"), "got {slots:?}");

        client.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
        expect_reply(&mut client, b"+OK\r\n").await;
        acceptor.await.unwrap();
    });
}
