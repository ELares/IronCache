// SPDX-License-Identifier: MIT OR Apache-2.0
//! Graceful-shutdown acceptance tests (#139, SHUTDOWN.md): the `SHUTDOWN [NOSAVE|SAVE]` command and
//! the SIGTERM/SIGINT save-on-exit drain.
//!
//! SHUTDOWN exits the process, so the EXIT paths (SHUTDOWN NOSAVE / SHUTDOWN SAVE / signal save-on-
//! exit) are driven against the REAL compiled binary as a SUBPROCESS (so the test process survives):
//! we boot `ironcache`, drive RESP over a socket, trigger the stop, and assert the process exits 0
//! and the snapshot is (or is not) written, then RESTART on the same data_dir to prove a SAVE-on-
//! exit reloads. The NON-exiting refusal paths (the auth gate, a bad modifier, a forced SAVE with no
//! data_dir) are driven IN-PROCESS against `run_server` (the server must stay up), mirroring the
//! persistence integration tests.

use ironcache::test_support::{run_persist_server_with_auth_for_test, run_server_for_test};
use std::io::{Read as _, Write as _};
use std::net::TcpStream as StdTcpStream;
use std::process::{Child, Command, Stdio};
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
    let dir = std::env::temp_dir().join(format!("ic-shutdown-it-{tag}-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// IN-PROCESS refusal-path helpers (the server must STAY UP, so no exit here).
// ---------------------------------------------------------------------------

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

/// Encode + send an arbitrary command (each arg a bulk string) and return its raw reply.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    client.write_all(frame.as_bytes()).await.unwrap();
    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// SHUTDOWN is AUTH-GATED exactly like SAVE (#139 mirrors the persistence H2 gate): an
/// UNAUTHENTICATED client with `requirepass` set gets `-NOAUTH` and the server DOES NOT shut down --
/// it keeps serving (a following command still works), and no snapshot is written.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_is_auth_gated_and_server_stays_up() {
    let dir = temp_data_dir("authgate");
    let port = free_port();
    let server = run_persist_server_with_auth_for_test(port, 2, dir.clone(), "s3cr3t");
    let mut c = connect_retry(port).await;

    // UNAUTHENTICATED SHUTDOWN (every form) -> NOAUTH; the process is still alive.
    let noauth = b"-NOAUTH Authentication required.\r\n".to_vec();
    assert_eq!(
        cmd(&mut c, &["SHUTDOWN"]).await,
        noauth,
        "bare SHUTDOWN NOAUTH"
    );
    assert_eq!(
        cmd(&mut c, &["SHUTDOWN", "NOSAVE"]).await,
        noauth,
        "SHUTDOWN NOSAVE NOAUTH"
    );
    assert_eq!(
        cmd(&mut c, &["SHUTDOWN", "SAVE"]).await,
        noauth,
        "SHUTDOWN SAVE NOAUTH"
    );
    // The server is still up: AUTH then PING succeeds (it did not shut down).
    assert_eq!(cmd(&mut c, &["AUTH", "s3cr3t"]).await, b"+OK\r\n");
    assert_eq!(
        cmd(&mut c, &["PING"]).await,
        b"+PONG\r\n",
        "server still serving"
    );
    // And no snapshot was written by the rejected SHUTDOWNs.
    assert!(
        !dir.join("dump.manifest").exists(),
        "an unauthenticated SHUTDOWN writes no snapshot"
    );

    drop(c);
    server.shutdown_and_join().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// A bad SHUTDOWN modifier is `-ERR syntax error` and does NOT shut the server down; a forced
/// `SHUTDOWN SAVE` with NO data_dir (persistence off) is an error (it cannot honor the save) and
/// ALSO does not shut down. The server keeps serving in both cases.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_refusals_keep_the_server_up() {
    let port = free_port();
    let server = run_server_for_test(port, 2); // no data_dir -> persistence OFF.
    let mut c = connect_retry(port).await;

    // A bad modifier -> syntax error, server stays up.
    assert_eq!(
        cmd(&mut c, &["SHUTDOWN", "BOGUS"]).await,
        b"-ERR syntax error\r\n"
    );
    // A forced SAVE with no data_dir cannot be honored -> error (Redis errors rather than exit over
    // unwritten data), server stays up.
    let reply = cmd(&mut c, &["SHUTDOWN", "SAVE"]).await;
    assert!(
        reply.starts_with(b"-ERR Errors trying to SHUTDOWN"),
        "SHUTDOWN SAVE with no data_dir errors: {:?}",
        String::from_utf8_lossy(&reply)
    );
    // The server is still serving after both refusals.
    assert_eq!(cmd(&mut c, &["PING"]).await, b"+PONG\r\n");

    drop(c);
    server.shutdown_and_join().unwrap();
}

/// #543 REGRESSION: graceful shutdown must be BOUNDED even with active pub/sub subscribers. Each
/// SUBSCRIBE / PSUBSCRIBE connection leaves its per-connection serve loop PARKED on the subscribe-
/// mode idle wait -- the exact state that, before the fix, had no `select!` arm a graceful stop
/// woke, so `shutdown_and_join` blocked on the parked shard-thread join until the drain grace (an
/// ops hang on a real SIGTERM). The idle wait now races a short poll of the shared shutdown flag,
/// so a parked subscriber closes within one poll interval. We drive the join on a SEPARATE thread
/// behind a HARD `recv_timeout` so a regression FAILS FAST here instead of hanging the whole suite.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_is_bounded_with_active_subscribers() {
    let port = free_port();
    let server = run_server_for_test(port, 4); // multi-shard: subscribers spread across cores.

    // A dozen subscriber connections (plain SUBSCRIBE + pattern PSUBSCRIBE), round-robined across
    // the 4 shards, each parked in the subscribe-mode idle wait. Held open for the whole test so
    // the point is proven: shutdown must NOT wait on them.
    let mut subs = Vec::new();
    for i in 0..6 {
        let mut c = connect_retry(port).await;
        let reply = cmd(&mut c, &["SUBSCRIBE", &format!("chan-{i}")]).await;
        assert!(
            reply.starts_with(b"*") || reply.starts_with(b">"),
            "SUBSCRIBE confirmation: {:?}",
            String::from_utf8_lossy(&reply)
        );
        subs.push(c);
    }
    for i in 0..6 {
        let mut c = connect_retry(port).await;
        let reply = cmd(&mut c, &["PSUBSCRIBE", &format!("pat-{i}-*")]).await;
        assert!(
            reply.starts_with(b"*") || reply.starts_with(b">"),
            "PSUBSCRIBE confirmation: {:?}",
            String::from_utf8_lossy(&reply)
        );
        subs.push(c);
    }
    // Let the subscriptions register on their home shards and the serve loops settle into the wait.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Join on a dedicated thread (`ShardSet` is `Send`) so a hang trips a bounded `recv_timeout`
    // rather than wedging the suite. The `recv_timeout` returning `Ok` IS the assertion that
    // `shutdown_and_join` completed within the 2s bound (no wall-clock read is needed here, keeping
    // this test off the determinism seam / ADR-0003); a timeout means it hung on a parked subscriber.
    let (tx, rx) = std::sync::mpsc::channel();
    let joiner = std::thread::spawn(move || {
        server.shutdown_and_join().unwrap();
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(2)).expect(
        "#543: shutdown_and_join must return within 2s with active subscribers, not hang on a \
         parked subscriber serve loop",
    );
    joiner.join().unwrap();

    // The subscriber sockets stayed open across the whole shutdown (dropped only now): the fix
    // closed them from the server side promptly instead of the join waiting on them.
    drop(subs);
}

// ---------------------------------------------------------------------------
// SUBPROCESS exit-path helpers (the binary EXITS, so the test process survives).
// ---------------------------------------------------------------------------

/// Spawn the REAL compiled `ironcache` binary on `127.0.0.1:port`, optionally with persistence
/// (`data_dir`) and a save policy (`interval_secs`). Returns the child handle.
fn spawn_binary(port: u16, data_dir: Option<&std::path::Path>, interval_secs: u64) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ironcache"));
    cmd.arg("server")
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--shards")
        .arg("2")
        // Metrics endpoint is default-on (127.0.0.1:9091) since #555; this test does not exercise
        // it and several subprocesses run in parallel, so disable it explicitly to avoid a shared
        // ops-port bind conflict (a bind failure is a hard boot error).
        .arg("--metrics-addr")
        .arg("off")
        // Do not read the conventional /etc config path in CI.
        .env_remove("IRONCACHE_DATA_DIR")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(dir) = data_dir {
        cmd.env("IRONCACHE_DATA_DIR", dir);
        cmd.env("IRONCACHE_SAVE_INTERVAL_SECS", interval_secs.to_string());
        // Save on every tick / exit regardless of dirty count, so the policy is unambiguous.
        cmd.env("IRONCACHE_SAVE_MIN_CHANGES", "0");
    }
    cmd.spawn().expect("failed to spawn ironcache binary")
}

/// Connect to the subprocess with retries (it binds asynchronously after spawn).
fn connect_blocking(port: u16) -> StdTcpStream {
    for _ in 0..200 {
        if let Ok(s) = StdTcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_nodelay(true);
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            return s;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("subprocess never came up on port {port}");
}

/// Send one command (bulk-string args) and read one reply.
fn send_cmd(s: &mut StdTcpStream, args: &[&str]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    s.write_all(frame.as_bytes()).unwrap();
    let mut buf = [0u8; 512];
    let n = s.read(&mut buf).unwrap_or(0);
    buf[..n].to_vec()
}

/// Send a command that triggers a clean process exit (SHUTDOWN); the connection drops on exit, so we
/// ignore any read result. Then wait (bounded) for the child to exit and return its status.
fn shutdown_and_wait(
    mut child: Child,
    s: &mut StdTcpStream,
    args: &[&str],
) -> std::process::ExitStatus {
    // Best-effort: send the frame. The server exits and closes the socket, so the write may error.
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    let _ = s.write_all(frame.as_bytes());
    let _ = s.flush();
    // Poll for exit up to a generous bound (well inside the drain grace window).
    for _ in 0..200 {
        if let Ok(Some(status)) = child.try_wait() {
            return status;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    panic!("child did not exit after SHUTDOWN {args:?}");
}

/// `SHUTDOWN NOSAVE` exits the process CLEANLY (status 0) and, with persistence OFF, writes no file.
#[test]
fn shutdown_nosave_exits_clean_no_save() {
    let port = free_port();
    let child = spawn_binary(port, None, 0);
    let mut s = connect_blocking(port);
    // Sanity: it serves before the stop.
    assert_eq!(send_cmd(&mut s, &["PING"]), b"+PONG\r\n");
    let status = shutdown_and_wait(child, &mut s, &["SHUTDOWN", "NOSAVE"]);
    assert!(status.success(), "SHUTDOWN NOSAVE exits 0, got {status:?}");
}

/// `SHUTDOWN NOSAVE` exits clean EVEN with a save policy configured -- and writes NO snapshot
/// (NOSAVE suppresses the save even when one is configured).
#[test]
fn shutdown_nosave_suppresses_save_even_with_policy() {
    let dir = temp_data_dir("nosave-policy");
    let port = free_port();
    let child = spawn_binary(port, Some(&dir), 3600); // a save policy IS configured.
    let mut s = connect_blocking(port);
    assert_eq!(send_cmd(&mut s, &["SET", "k", "v"]), b"+OK\r\n");
    let status = shutdown_and_wait(child, &mut s, &["SHUTDOWN", "NOSAVE"]);
    assert!(status.success(), "exits 0, got {status:?}");
    assert!(
        !dir.join("dump.manifest").exists(),
        "SHUTDOWN NOSAVE wrote no snapshot despite a save policy"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// `SHUTDOWN SAVE` (persistence ON) writes a snapshot then exits 0; a RESTART on the same data_dir
/// reloads the saved keyspace (the durability round-trip the persistence story needs on exit).
#[test]
fn shutdown_save_writes_snapshot_then_restart_reloads() {
    let dir = temp_data_dir("save");
    let port = free_port();

    // Boot 1: populate, SHUTDOWN SAVE (no periodic policy -> SAVE forces the save anyway).
    let child = spawn_binary(port, Some(&dir), 0);
    let mut s = connect_blocking(port);
    for i in 0..20 {
        assert_eq!(
            send_cmd(&mut s, &["SET", &format!("k{i}"), &format!("v{i}")]),
            b"+OK\r\n"
        );
    }
    let status = shutdown_and_wait(child, &mut s, &["SHUTDOWN", "SAVE"]);
    assert!(status.success(), "SHUTDOWN SAVE exits 0, got {status:?}");
    assert!(
        dir.join("dump.manifest").exists(),
        "SHUTDOWN SAVE committed a snapshot"
    );

    // Boot 2: a fresh process on the same data_dir reloads the keyspace.
    let port2 = free_port();
    let child2 = spawn_binary(port2, Some(&dir), 0);
    let mut s2 = connect_blocking(port2);
    for i in 0..20 {
        let v = format!("v{i}");
        assert_eq!(
            send_cmd(&mut s2, &["GET", &format!("k{i}")]),
            format!("${}\r\n{v}\r\n", v.len()).into_bytes(),
            "k{i} reloaded after SHUTDOWN SAVE + restart"
        );
    }
    let status2 = shutdown_and_wait(child2, &mut s2, &["SHUTDOWN", "NOSAVE"]);
    assert!(status2.success());
    std::fs::remove_dir_all(&dir).ok();
}

/// Bare `SHUTDOWN` with a SAVE POLICY configured saves on exit (a save point is configured); a
/// restart reloads. This is the same default a SIGTERM-driven stop resolves.
#[test]
fn bare_shutdown_with_policy_saves_on_exit() {
    let dir = temp_data_dir("bare-policy");
    let port = free_port();

    let child = spawn_binary(port, Some(&dir), 3600); // a save policy IS configured.
    let mut s = connect_blocking(port);
    assert_eq!(send_cmd(&mut s, &["SET", "kept", "yes"]), b"+OK\r\n");
    let status = shutdown_and_wait(child, &mut s, &["SHUTDOWN"]);
    assert!(status.success(), "bare SHUTDOWN exits 0, got {status:?}");
    assert!(
        dir.join("dump.manifest").exists(),
        "bare SHUTDOWN with a save policy saved on exit"
    );

    let port2 = free_port();
    let child2 = spawn_binary(port2, Some(&dir), 3600);
    let mut s2 = connect_blocking(port2);
    assert_eq!(send_cmd(&mut s2, &["GET", "kept"]), b"$3\r\nyes\r\n");
    let status2 = shutdown_and_wait(child2, &mut s2, &["SHUTDOWN", "NOSAVE"]);
    assert!(status2.success());
    std::fs::remove_dir_all(&dir).ok();
}

/// SIGTERM drives the GRACEFUL stop: a configured save policy -> save-on-exit fires, the process
/// exits 0, and a restart reloads. (The signal handler turns SIGTERM into a controlled shutdown +
/// save, not an abrupt in-handler exit.)
#[cfg(unix)]
#[test]
fn sigterm_graceful_save_on_exit_then_restart_reloads() {
    let dir = temp_data_dir("sigterm");
    let port = free_port();

    let mut child = spawn_binary(port, Some(&dir), 3600); // a save policy IS configured.
    let mut s = connect_blocking(port);
    assert_eq!(send_cmd(&mut s, &["SET", "sig", "term"]), b"+OK\r\n");
    drop(s);

    // Send SIGTERM and wait (bounded) for the graceful stop to save + exit 0.
    let pid = child.id() as i32;
    // SAFETY: kill(2) with a valid pid + SIGTERM; the child is our own spawned process.
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
    let mut exited = None;
    for _ in 0..400 {
        if let Ok(Some(status)) = child.try_wait() {
            exited = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let status = exited.unwrap_or_else(|| {
        let _ = child.kill();
        panic!("child did not exit after SIGTERM");
    });
    assert!(
        status.success(),
        "SIGTERM graceful stop exits 0, got {status:?}"
    );
    assert!(
        dir.join("dump.manifest").exists(),
        "SIGTERM with a save policy saved on exit"
    );

    // Restart reloads the SIGTERM-saved data.
    let port2 = free_port();
    let child2 = spawn_binary(port2, Some(&dir), 3600);
    let mut s2 = connect_blocking(port2);
    assert_eq!(send_cmd(&mut s2, &["GET", "sig"]), b"$4\r\nterm\r\n");
    let status2 = shutdown_and_wait(child2, &mut s2, &["SHUTDOWN", "NOSAVE"]);
    assert!(status2.success());
    std::fs::remove_dir_all(&dir).ok();
}

/// #676 HOL-BLOCK-FIX REGRESSION GUARD: SIGTERM save-on-exit must NOT drop a REMOTE (sibling) shard's
/// partition. The fix runs `__ICSAVE` OFF the drain loop in STEADY STATE (so the loop keeps serving
/// cross-shard hops during a save), but INLINE on the graceful-shutdown window -- a detached spawn
/// there would be cancelled when the shutdown window returns at its ~120ms idle gap, silently failing
/// the final save for any per-shard dump over ~120ms and losing writes since the last snapshot. The
/// pre-fix single-key sigterm test can't see this (a 1-key dump is microseconds). This populates BOTH
/// of the (`--shards 2`) shards with a real ~10MB keyspace so shard 1's dump is non-trivial, SIGTERMs,
/// and asserts the restart reloads the FULL keyspace -- a dropped remote partition shows as DBSIZE < n.
#[test]
fn sigterm_save_on_exit_preserves_remote_shard_partition() {
    let dir = temp_data_dir("sigterm-multishard");
    let port = free_port();
    let mut child = spawn_binary(port, Some(&dir), 3600); // --shards 2 + save policy on.
    let n = 10_000usize;
    let val = "x".repeat(1024); // ~10MB total -> ~5MB per shard, a non-trivial sibling dump.
    {
        let mut s = connect_blocking(port);
        for k in 0..n {
            let key = format!("k:{k}");
            assert_eq!(
                send_cmd(&mut s, &["SET", &key, &val]),
                b"+OK\r\n",
                "populate SET {key}"
            );
        }
        assert_eq!(
            send_cmd(&mut s, &["DBSIZE"]),
            format!(":{n}\r\n").into_bytes(),
            "all {n} keys populated across both shards"
        );
        drop(s);
    }

    // SIGTERM -> graceful save-on-exit (fans `__ICSAVE` to the sibling shard) -> exit 0.
    let pid = child.id() as i32;
    // SAFETY: kill(2) with our own spawned child's pid + SIGTERM.
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
    let mut exited = None;
    for _ in 0..800 {
        if let Ok(Some(status)) = child.try_wait() {
            exited = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let status = exited.unwrap_or_else(|| {
        let _ = child.kill();
        panic!("child did not exit after SIGTERM");
    });
    assert!(
        status.success(),
        "SIGTERM graceful stop exits 0, got {status:?}"
    );
    assert!(
        dir.join("dump.manifest").exists(),
        "save-on-exit committed a manifest"
    );

    // Restart: the FULL keyspace must reload -- BOTH shards, not just the home shard. A remote
    // partition dropped by a cancelled save-on-exit (the regression) shows as DBSIZE < n here.
    let port2 = free_port();
    let child2 = spawn_binary(port2, Some(&dir), 3600);
    let mut s2 = connect_blocking(port2);
    assert_eq!(
        send_cmd(&mut s2, &["DBSIZE"]),
        format!(":{n}\r\n").into_bytes(),
        "restart reloaded the FULL keyspace (no remote-shard partition dropped on save-on-exit)"
    );
    // Spot-check keys spread across the keyspace (both shards) are present (EXISTS -> small reply).
    for k in (0..n).step_by(997) {
        let key = format!("k:{k}");
        assert_eq!(
            send_cmd(&mut s2, &["EXISTS", &key]),
            b":1\r\n",
            "sample key {key} reloaded"
        );
    }
    let status2 = shutdown_and_wait(child2, &mut s2, &["SHUTDOWN", "NOSAVE"]);
    assert!(status2.success());
    std::fs::remove_dir_all(&dir).ok();
}
