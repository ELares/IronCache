// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end RESP smoke test: boot a real listener on an ephemeral port over the
//! tokio current-thread runtime, connect a client, and verify the Tier-0 wire
//! behavior (PROTOCOL.md acceptance: connect + PING + HELLO round trips).
//!
//! This exercises the actual decode -> dispatch -> encode path against a live
//! socket, which is the integration coverage the PR-1 gate asks for.

use ironcache_env::SystemEnv;
use ironcache_observe::{CounterSnapshot, ServerInfo};
use ironcache_protocol::{DecodeOutcome, Limits, ProtoVersion, decode, encode_to_vec};
use ironcache_runtime::tokio_rt::bind_reuseport;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, dispatch};
use std::cell::RefCell;

fn ctx(port: u16, pass: Option<&str>) -> ServerContext {
    ServerContext {
        requirepass: pass.map(str::to_owned),
        databases: 16,
        info: ServerInfo {
            tcp_port: port,
            shards: 1,
            pid: std::process::id(),
            started_at: ironcache_env::Monotonic::ZERO,
            maxmemory: 0,
            mem_allocator: "jemalloc",
        },
    }
}

/// Serve a single connection: decode requests, dispatch, encode replies, until
/// the peer closes or QUIT.
async fn serve_one(mut stream: tokio::net::TcpStream, ctx: ServerContext) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let env = SystemEnv::new();
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
    loop {
        // Drain complete frames.
        loop {
            match decode(&buf, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    let rollup = || *counters.borrow();
                    let reply = dispatch(&ctx, &mut conn, &env, &rollup, &request);
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
