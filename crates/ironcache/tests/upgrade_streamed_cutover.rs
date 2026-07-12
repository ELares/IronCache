// SPDX-License-Identifier: MIT OR Apache-2.0
//! #391 PR-6 ACCEPTANCE: a REAL two-process streamed live cutover old->new over a unix socket, with
//! the OLD's client listen socket INHERITED by the NEW sibling so no connection is reset across the
//! flip.
//!
//! The test process plays the OLD (sender) orchestrator: it seeds a live shard, binds a real client
//! TCP listener, queues a client connection in that listener's BACKLOG, then SPAWNS a real sibling
//! process (a re-exec of this test binary in receiver mode via
//! [`ironcache::upgrade::orchestrator::spawn_receiver_sibling`], which hands the sibling the OLD's
//! listen fd). The OLD drives [`run_sender_cutover`] to a committed handoff; the NEW sibling ADOPTS
//! the inherited listener (`adopt_listener_fd`), receives the keyspace via [`run_receiver_cutover`],
//! promotes it durably, and serves a tiny `GET` protocol on the SAME listen socket.
//!
//! It asserts:
//! - **NO RST (Decision 1):** the connection queued in the OLD's backlog BEFORE the flip is
//!   ACCEPTED + SERVED by the NEW after the OLD stops accepting -- the inherited listen socket was
//!   never closed, so the backlog was not orphaned/reset.
//! - **ZERO acknowledged-write loss:** every seeded (acked) key is GET-able with its exact value from
//!   the NEW after the cutover.
//! - **New connections are served:** a connection ARRIVING after the flip is served by the NEW too.
//!
//! Gated `#[ignore]` because it spawns a real sibling process (Linux/colima on-demand, or via the
//! scratchpad smoke `scratchpad/streamed-cutover-smoke.sh`); the deterministic in-process core (the
//! sub-second write-stall measurement, zero-loss, abort matrix, and post-commit `data_dir` recovery)
//! is the ALWAYS-ON lib test `upgrade::orchestrator::tests::*` + the no-RST primitive
//! `ironcache_runtime::tokio_rt::tests::inherited_listener_serves_backlog_without_rst`.

use ironcache_env::Clock;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::time::Duration;

use ironcache::upgrade::commit::Staging;
use ironcache::upgrade::drive::{accept_handoff, bind_handoff_listener, connect_handoff};
use ironcache::upgrade::orchestrator::{
    HANDOFF_LISTEN_FD_ENV, HANDOFF_ROLE_ENV, HANDOFF_SOCKET_ENV, ReceiverOutcome, SenderDecision,
    SenderShard, run_receiver_cutover, run_sender_cutover, spawn_receiver_sibling,
};
use ironcache_repl::{ReplId, ReplObserver, ReplOffset, ReplRing};
use ironcache_runtime::tokio_rt::adopt_listener_fd;
use ironcache_storage::{ExpireWrite, NewValue, Store, UnixMillis};
use ironcache_store::ShardStore;

const NOW: UnixMillis = UnixMillis(1_000);
const DBS: u32 = 4;
const KEYS: u32 = 2_000;

fn replid() -> ReplId {
    ReplId::from_bytes([0x77; 20])
}

/// The re-exec CHILD entry (the NEW-version sibling). A normal `cargo test` runs this as a trivial
/// no-op (the handoff env is absent); when SPAWNED by the parent (with `IRONCACHE_HANDOFF_ROLE=receiver`)
/// it becomes the receiver harness and `exit()`s -- it never returns to libtest.
#[test]
fn streamed_cutover_child_receiver() {
    if std::env::var(HANDOFF_ROLE_ENV).as_deref() != Ok("receiver") {
        return; // not spawned as the sibling: no-op.
    }
    child_receiver_main();
}

/// The receiver sibling: adopt the inherited client listener, receive the streamed handoff, promote
/// it, and serve `GET` on the inherited listener until told to shut down. Never returns.
fn child_receiver_main() -> ! {
    let fd: RawFd = std::env::var(HANDOFF_LISTEN_FD_ENV)
        .expect("child needs the inherited listener fd")
        .trim()
        .parse()
        .expect("listener fd is a number");
    let socket = PathBuf::from(std::env::var(HANDOFF_SOCKET_ENV).expect("child needs the socket"));
    // ADOPT the OLD's client listen socket (the never-closed-listener no-RST path).
    let std_listener = adopt_listener_fd(fd).expect("child adopts the inherited client listener");
    eprintln!(
        "child: adopted inherited listener fd {fd} -> local_addr {:?}",
        std_listener.local_addr()
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    let code: i32 = rt.block_on(async move {
        let stream = match connect_handoff(&socket, Duration::from_secs(15)).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("child: connect_handoff failed: {e}");
                return 11;
            }
        };
        let pid = std::process::id();
        let staging_dir = std::env::temp_dir().join(format!("ic-cutover-child-staging-{pid}"));
        let data_dir = std::env::temp_dir().join(format!("ic-cutover-child-data-{pid}"));
        let _ = std::fs::remove_dir_all(&staging_dir);
        let _ = std::fs::remove_dir_all(&data_dir);
        let staging = Staging::new(&staging_dir).expect("child staging dir");
        let mut streams = [stream];
        let outcome = match tokio::time::timeout(
            Duration::from_secs(30),
            run_receiver_cutover(
                &mut streams,
                || ShardStore::new(DBS),
                DBS,
                NOW,
                &staging,
                &data_dir,
                0,
            ),
        )
        .await
        {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                eprintln!("child: run_receiver_cutover error: {e}");
                return 12;
            }
            Err(_) => {
                eprintln!("child: run_receiver_cutover timed out");
                return 13;
            }
        };
        let mut store = match outcome {
            ReceiverOutcome::Committed(mut v) => v.remove(0).store,
            ReceiverOutcome::Aborted => {
                eprintln!("child: cutover aborted");
                return 14;
            }
        };

        // SERVE `GET` on the inherited listener until a client sends SHUTDOWN.
        let listener = tokio::net::TcpListener::from_std(std_listener).expect("tokio listener");
        loop {
            let Ok(Ok((conn, _peer))) =
                tokio::time::timeout(Duration::from_secs(30), listener.accept()).await
            else {
                return 15;
            };
            if serve_conn(conn, &mut store).await {
                return 0; // SHUTDOWN received: clean exit.
            }
        }
    });
    std::process::exit(code);
}

/// Serve one client connection: `GET <key>` -> value or `MISS`, `BYE` closes this connection, and
/// `SHUTDOWN` returns `true` so the child exits. Returns `false` on a plain connection close.
async fn serve_conn(mut conn: tokio::net::TcpStream, store: &mut ShardStore) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 512];
    loop {
        // Process every COMPLETE line already buffered.
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let mut line: Vec<u8> = buf.drain(..=pos).collect();
            line.pop(); // drop '\n'
            let line = String::from_utf8_lossy(&line).trim().to_string();
            if line == "SHUTDOWN" {
                return true;
            }
            if line == "BYE" {
                return false;
            }
            if let Some(key) = line.strip_prefix("GET ") {
                let resp = match store.read(0, key.as_bytes(), NOW) {
                    Some(v) => {
                        let mut s = String::from_utf8_lossy(v.as_bytes()).into_owned();
                        s.push('\n');
                        s
                    }
                    None => "MISS\n".to_string(),
                };
                if conn.write_all(resp.as_bytes()).await.is_err() {
                    return false;
                }
                let _ = conn.flush().await;
            }
        }
        match conn.read(&mut chunk).await {
            Ok(0) | Err(_) => return false, // EOF or a read error: close this connection.
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// Blocking client helper: send `GET <key>` on `conn`, read one line, return it (trimmed).
fn client_get(conn: &mut BufReader<std::net::TcpStream>, key: &str) -> String {
    conn.get_mut()
        .write_all(format!("GET {key}\n").as_bytes())
        .expect("client GET write");
    conn.get_mut().flush().expect("client GET flush");
    let mut line = String::new();
    conn.read_line(&mut line).expect("client GET read");
    line.trim().to_string()
}

/// Blocking client helper: send a bare `line` (e.g. `BYE` / `SHUTDOWN`) with no reply expected.
fn client_send(conn: &mut BufReader<std::net::TcpStream>, line: &str) {
    conn.get_mut()
        .write_all(format!("{line}\n").as_bytes())
        .expect("client control write");
    conn.get_mut().flush().expect("client control flush");
}

#[test]
#[ignore = "real two-process spawn: run on Linux/colima on demand (or scratchpad/streamed-cutover-smoke.sh)"]
#[allow(clippy::too_many_lines)]
fn streamed_cutover_real_two_process_no_rst_zero_loss() {
    // ---- OLD: seed a live shard (the acked keyspace that must survive the cutover). ----
    let ring = ReplRing::new(200_000, ReplOffset::ZERO);
    let store = std::rc::Rc::new(std::cell::RefCell::new(ShardStore::new(DBS)));
    store
        .borrow_mut()
        .set_write_observer(ReplObserver::boxed(std::rc::Rc::clone(&ring)));
    let mut ledger: Vec<(String, String)> = Vec::new();
    {
        let mut s = store.borrow_mut();
        for i in 0..KEYS {
            let key = format!("k-{i}");
            let val = format!("v-{i}");
            // All in db 0 (the child's GET protocol reads db 0).
            s.upsert(
                0,
                key.as_bytes(),
                NewValue::Bytes(val.as_bytes()),
                ExpireWrite::Clear,
                NOW,
            );
            ledger.push((key, val));
        }
    }

    // ---- OLD: bind the real CLIENT listener + queue a connection in its BACKLOG (never accepted). ----
    let client_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind the client listener");
    let client_addr = client_listener.local_addr().unwrap();
    // C_backlog: a client connected BEFORE the flip and left queued (the OLD never accepts it). If the
    // inherited listener were closed across the flip this connection would be RST; it must survive.
    let backlog_conn =
        std::net::TcpStream::connect(client_addr).expect("queue a backlog connection");
    backlog_conn
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();

    // ---- OLD: SPAWN the real sibling receiver (it retries connecting until the handoff socket is
    //      bound below, inside the runtime). ----
    let pid = std::process::id();
    let handoff_socket = std::env::temp_dir().join(format!("ic-cutover-handoff-{pid}.sock"));
    let _ = std::fs::remove_file(&handoff_socket);
    let exe = std::env::current_exe().expect("current exe");
    let child_args = [
        "--exact",
        "streamed_cutover_child_receiver",
        "--nocapture",
        "--test-threads=1",
    ];
    // Hand the sibling the OLD's client listen fd (Decision 1: the socket is inherited, never closed).
    let mut child = spawn_receiver_sibling(
        &exe,
        &child_args,
        &handoff_socket,
        Some(client_listener.as_raw_fd()),
    )
    .expect("spawn the receiver sibling");

    // ---- OLD: accept the sibling's handoff connection + drive the cutover to COMMIT. ----
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let t0 = ironcache_env::SystemEnv::new().now();
    let decision = rt.block_on(async {
        let handoff_listener =
            bind_handoff_listener(&handoff_socket).expect("bind the handoff socket");
        let stream = accept_handoff(&handoff_listener, Duration::from_secs(15))
            .await
            .expect("accept the sibling handoff connection");
        let mut shards = vec![SenderShard {
            stream,
            store: std::rc::Rc::clone(&store),
            ring: std::rc::Rc::clone(&ring),
            shard: 0,
        }];
        run_sender_cutover(&mut shards, replid(), NOW, 256)
            .await
            .expect("the OLD drove the cutover to a decision")
    });
    let cutover_ms = ironcache_env::SystemEnv::new()
        .now()
        .saturating_duration_since(t0)
        .as_secs_f64()
        * 1000.0;
    assert_eq!(
        decision,
        SenderDecision::Committed,
        "the real two-process cutover committed"
    );
    println!("REAL two-process cutover wall-time (accept -> committed+served): {cutover_ms:.1} ms");

    // The OLD stops serving on its client listener (drop its handle). The socket lives on via the
    // sibling's inherited dup, so the queued backlog connection is NOT orphaned/reset.
    drop(client_listener);

    // ---- NO-RST + ZERO-LOSS: the pre-flip BACKLOG connection is served by the NEW; every acked
    //      key GETs with its exact value. ----
    let mut backlog = BufReader::new(backlog_conn);
    for (key, val) in &ledger {
        let got = client_get(&mut backlog, key);
        assert_eq!(
            &got, val,
            "backlog connection served key {key} by the NEW with the acked value (no RST, no loss)"
        );
    }
    // Release the backlog connection so the sibling's sequential server can accept the next one.
    client_send(&mut backlog, "BYE");

    // ---- A NEW connection arriving AFTER the flip is also served by the NEW. ----
    let new_conn =
        std::net::TcpStream::connect(client_addr).expect("new connection after the flip");
    new_conn
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let mut newc = BufReader::new(new_conn);
    let (probe_key, probe_val) = &ledger[KEYS as usize / 2];
    assert_eq!(
        &client_get(&mut newc, probe_key),
        probe_val,
        "a connection arriving after the flip is served by the NEW"
    );

    // ---- Tell the sibling to shut down; reap it and assert a clean exit. ----
    client_send(&mut newc, "SHUTDOWN");
    let status = wait_child(&mut child, Duration::from_secs(15));
    let _ = std::fs::remove_file(&handoff_socket);
    assert_eq!(
        status.map(|s| s.success()),
        Some(true),
        "the sibling exited cleanly after serving the adopted keyspace"
    );
}

/// Reap `child` within `deadline`, killing it if it overruns, and return its exit status (or `None`
/// if it never exited before the kill+reap).
fn wait_child(
    child: &mut std::process::Child,
    deadline: Duration,
) -> Option<std::process::ExitStatus> {
    let start = ironcache_env::SystemEnv::new().now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {}
            Err(_) => return None,
        }
        if ironcache_env::SystemEnv::new()
            .now()
            .saturating_duration_since(start)
            >= deadline
        {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
