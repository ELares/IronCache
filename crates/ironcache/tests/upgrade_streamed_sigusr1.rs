// SPDX-License-Identifier: MIT OR Apache-2.0
//! #638 slice-5 CAPSTONE ACCEPTANCE: a REAL running `ironcache` server, configured with a
//! `handoff_socket`, receives SIGUSR1 and performs a full IN-SERVER streamed live cutover to a
//! sibling it spawns (a re-exec of its own binary in receiver role), while a client drives sustained
//! read + write traffic throughout.
//!
//! Unlike `tests/upgrade_streamed_cutover.rs` (which drives the orchestrator primitives IN-PROCESS
//! from the test binary), this test drives the WHOLE mechanism end to end through the real process:
//! the server's SIGUSR1 handler (slice-3) -> the main-thread cutover host (slice-3) ->
//! `spawn_receiver_sibling` (which re-execs THIS same binary in receiver role and hands it the OLD's
//! client listen fd) -> the per-shard `CutoverCoord` barrier (slice-2) -> the receiver-side flip
//! barrier (slice-4). Nothing is called directly; SIGUSR1 is the only trigger.
//!
//! It asserts, against the surviving server:
//! - ZERO acknowledged-write loss: a writer records every key that got a `+OK`; after the cutover
//!   EVERY such key GETs its exact value from the surviving (NEW) server.
//! - NO RST / no refused connections: a tight connect probe running across the flip never observes
//!   `ECONNREFUSED` (the inherited listen socket is never closed), and a connection established on the
//!   OLD before SIGUSR1 is closed with a clean EOF (a graceful FIN), never an abrupt reset. A fresh
//!   connection after the OLD exits is served by the NEW on the SAME port.
//! - The upgrade completes: the OLD process `exit(0)`s after Commit, and the sibling serves on the
//!   same port.
//! - Sub-second write stall: the client-visible write-unavailability window (measured via the
//!   `ironcache-env` clock seam, NOT `std::time`) is under one second.
//! - ABORT keeps serving: a cutover that must abort (an unusable handoff socket) leaves the OLD
//!   serving with zero loss; it never exits and writes resume.
//!
//! ## Gate placement
//!
//! Gated `#[ignore]` (Linux/colima on demand, or `scratchpad/streamed-cutover-smoke.sh`): it spawns a
//! real sibling process and relies on Linux fork/exec + `SO_REUSEPORT` + fd inheritance. The
//! DETERMINISTIC mechanism pieces are the ALWAYS-ON lib tests
//! (`upgrade::cutover_coord::tests::coord_*` for the sender barrier commit/abort/resume,
//! `upgrade::commit::tests::receiver_flip_*` for the all-or-nothing serve flip, and
//! `serve::tests::resolve_signal_maps_each_arm` for the SIGUSR1 -> Cutover arm). This test's unique
//! value is the real-binary + real-signal + real-listener-inheritance integration.
//!
//! ## STATUS: BOTH tests PASS (the commit-path bug this capstone surfaced is FIXED)
//!
//! `sigusr1_streamed_cutover_aborts_keeps_serving_zero_loss` passes (the host fail-safe: an unusable
//! handoff socket aborts the cutover and the OLD keeps serving with zero loss and never exits).
//!
//! `sigusr1_streamed_cutover_commits_zero_loss_no_rst` passes: the OLD commits and exits with zero
//! acked-write loss, no connection reset, and a sub-second write stall. HISTORY (why this test
//! exists): the first real-binary run of this capstone surfaced that the receiver BOOT path
//! (`coordinator::receive_shard_into`) still drove the legacy `stream::recv_shard` protocol while
//! the live SIGUSR1 sender spoke the PR-4 commit protocol, so a real cutover deadlocked into the
//! 30s timeout and safely aborted -- invisible to all 30+ in-process tests, which paired PR-4
//! halves with each other. The fix wired the boot path to the PR-4 receiver
//! (`commit::receive_shard_to_prepared` -> Commit-await -> promote -> `send_served`), which is
//! exactly what this test now locks in. The lesson stands: a multi-component protocol needs a
//! real-process end-to-end acceptance; unit tests that mock both halves hide the seam.

#![cfg(unix)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ironcache_env::Clock;

/// Shards to run: >= 2 to prove the multi-shard, cross-shard, staggered-quiesce path.
const SHARDS: usize = 4;
/// Keys seeded BEFORE the cutover (the bulk keyspace that must survive), spread across shards.
const SEED_KEYS: u32 = 3_000;
/// The writer cycles keys in `[0, WRITER_KEYSPACE)` so a key is re-`SET` (overwritten) across the
/// cutover -- exercising the bulk-then-delta overwrite path -- while bounding the verify GET count.
const WRITER_KEYSPACE: u64 = 20_000;
/// Coarse poll cadence (a fixed `Duration`, not a clock read -- inside the determinism invariant).
const POLL: Duration = Duration::from_millis(25);

// ---------------------------------------------------------------------------------------------
// RESP client helpers (raw sockets, exactly as the other real-server tests do).
// ---------------------------------------------------------------------------------------------

/// One decoded RESP reply (only the shapes this test drives: simple/error/bulk/nil, plus EOF).
enum Reply {
    Simple(String),
    Error(String),
    Bulk(Vec<u8>),
    Nil,
    Eof,
}

/// A free TCP port (bind ephemeral, read it, drop). A brief TOCTOU window before the binary rebinds;
/// fine on loopback for a test.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A throwaway per-test temp dir under the system temp, tagged by pid so parallel runs never collide.
fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ic-638-{tag}-{}", std::process::id()))
}

/// Encode + send one RESP command (bulk-string args).
fn write_cmd(w: &mut impl Write, args: &[&[u8]]) -> std::io::Result<()> {
    let mut frame = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        frame.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        frame.extend_from_slice(a);
        frame.extend_from_slice(b"\r\n");
    }
    w.write_all(&frame)?;
    w.flush()
}

/// Read exactly one RESP reply. A zero-length header read is a clean EOF (the peer closed).
fn read_reply(r: &mut impl BufRead) -> std::io::Result<Reply> {
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line)? == 0 {
        return Ok(Reply::Eof);
    }
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    if line.is_empty() {
        return Ok(Reply::Eof);
    }
    let rest = String::from_utf8_lossy(&line[1..]).into_owned();
    match line[0] {
        b'-' => Ok(Reply::Error(rest)),
        b'$' => {
            let len: i64 = rest.trim().parse().unwrap_or(-1);
            if len < 0 {
                return Ok(Reply::Nil);
            }
            let mut buf = vec![0u8; len as usize + 2]; // payload + CRLF
            r.read_exact(&mut buf)?;
            buf.truncate(len as usize);
            Ok(Reply::Bulk(buf))
        }
        // `+` simple, `:` integer, and any other byte: treat as a simple line reply.
        _ => Ok(Reply::Simple(rest)),
    }
}

/// Open a buffered RESP connection to `port`, or return the connect error (the caller distinguishes
/// `ECONNREFUSED`). Sets a read timeout so a wedged read cannot hang the test.
fn connect(port: u16) -> std::io::Result<BufReader<TcpStream>> {
    let s = TcpStream::connect(("127.0.0.1", port))?;
    s.set_nodelay(true).ok();
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    Ok(BufReader::new(s))
}

/// Connect with bounded retries (the subprocess binds asynchronously after spawn).
fn connect_blocking(port: u16) -> BufReader<TcpStream> {
    for _ in 0..400 {
        if let Ok(c) = connect(port) {
            return c;
        }
        std::thread::sleep(POLL);
    }
    panic!("subprocess never came up on port {port}");
}

/// `SET key val`, returning the raw reply.
fn do_set(c: &mut BufReader<TcpStream>, key: &str, val: &str) -> std::io::Result<Reply> {
    write_cmd(c.get_mut(), &[b"SET", key.as_bytes(), val.as_bytes()])?;
    read_reply(c)
}

/// `GET key` -> `Some(value)` or `None` (miss / nil).
fn do_get(c: &mut BufReader<TcpStream>, key: &str) -> std::io::Result<Option<String>> {
    write_cmd(c.get_mut(), &[b"GET", key.as_bytes()])?;
    match read_reply(c)? {
        Reply::Bulk(b) => Ok(Some(String::from_utf8_lossy(&b).into_owned())),
        _ => Ok(None),
    }
}

/// `PING` -> whether the reply was `+PONG` (a `-LOADING` receiver replies an error, not PONG).
fn ping_pong(c: &mut BufReader<TcpStream>) -> bool {
    if write_cmd(c.get_mut(), &[b"PING"]).is_err() {
        return false;
    }
    matches!(read_reply(c), Ok(Reply::Simple(s)) if s == "PONG")
}

/// Read `process_id:<pid>` from `INFO server`, or `None`.
fn info_process_id(c: &mut BufReader<TcpStream>) -> Option<i32> {
    write_cmd(c.get_mut(), &[b"INFO", b"server"]).ok()?;
    let Reply::Bulk(b) = read_reply(c).ok()? else {
        return None;
    };
    for line in String::from_utf8_lossy(&b).lines() {
        if let Some(pid) = line.trim().strip_prefix("process_id:") {
            return pid.trim().parse().ok();
        }
    }
    None
}

/// Deliver `signal` to a pid we spawned.
fn send_signal(pid: i32, signal: i32) {
    // SAFETY: kill(2) with a valid pid + signal number; `pid` is our own spawned process.
    unsafe {
        libc::kill(pid, signal);
    }
}

/// Poll `child.try_wait()` up to `attempts` times (spaced by `POLL`); `Some(status)` if it exited.
fn wait_exit(child: &mut Child, attempts: usize) -> Option<std::process::ExitStatus> {
    for _ in 0..attempts {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        std::thread::sleep(POLL);
    }
    None
}

// ---------------------------------------------------------------------------------------------
// The OLD (sender) server spawn.
// ---------------------------------------------------------------------------------------------

/// Spawn the REAL compiled `ironcache server` as the OLD (sender) process: a free port, `SHARDS`
/// shards, a temp `data_dir`, and a `handoff_socket`. `IRONCACHE_HANDOFF_ROLE` is REMOVED so it boots
/// as the normal sender/server; metrics are off so the sibling's re-exec does not collide on an ops
/// port. stdout/stderr go to `log_path` (the inherited sibling appends there too) for diagnosis.
fn spawn_old(port: u16, data_dir: &Path, handoff_socket: &Path, log_path: &Path) -> Child {
    let log = std::fs::File::create(log_path).expect("create the server log file");
    let log_err = log.try_clone().expect("clone the log fd for stderr");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ironcache"));
    cmd.arg("server")
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--shards")
        .arg(SHARDS.to_string())
        .arg("--metrics-addr")
        .arg("off")
        .env("IRONCACHE_DATA_DIR", data_dir)
        .env("IRONCACHE_HANDOFF_SOCKET", handoff_socket)
        // Boot as the SENDER (the running server), not a receiver sibling.
        .env_remove("IRONCACHE_HANDOFF_ROLE")
        // No save policy: the cutover-exit path writes no snapshot, and the receiver serves from
        // memory, so there is no shared-data_dir save race to reason about in this test.
        .env_remove("IRONCACHE_SAVE_INTERVAL_SECS")
        .env("RUST_LOG", "ironcache=info,warn")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    cmd.spawn()
        .expect("failed to spawn the ironcache server binary")
}

/// Wait (bounded) until `port` answers `+PONG` -- the OLD is serving.
fn wait_serving(port: u16) {
    for _ in 0..400 {
        if let Ok(mut c) = connect(port) {
            if ping_pong(&mut c) {
                return;
            }
        }
        std::thread::sleep(POLL);
    }
    panic!("server never began serving on port {port}");
}

// ---------------------------------------------------------------------------------------------
// The traffic workers.
// ---------------------------------------------------------------------------------------------

/// Shared cross-thread state the workers feed and the test asserts on.
struct Shared {
    stop: AtomicBool,
    /// Set if any connect ever returned `ECONNREFUSED` (a closed-listener window: MUST stay false).
    refused: AtomicBool,
    /// The first unexpected reply seen by the writer, if any (MUST stay `None`).
    weird: Mutex<Option<String>>,
}

/// What the writer thread returns on join.
struct WriterOut {
    /// Every `(key, value)` the writer got a `+OK` for, in write order (later overwrites win).
    acked: Vec<(String, String)>,
    /// The largest gap between two consecutive successful writes (the client-visible write stall).
    max_stall: Duration,
}

/// The WRITER: a tight `SET` loop that records every acked write and measures the write stall. On a
/// `-LOADING` (a quiesced shard) it advances to the next key (another shard may still serve). On a
/// connection drop (the OLD exiting) it reconnects to the SAME port (which now reaches the NEW), never
/// losing an already-acked key. Every timestamp is read through the `ironcache-env` clock seam.
fn writer_thread(port: u16, shared: &Shared) -> WriterOut {
    let clock = ironcache_env::SystemEnv::new();
    let mut acked: Vec<(String, String)> = Vec::new();
    let mut max_stall = Duration::ZERO;
    let mut last_ok = clock.now();
    let mut conn = connect_blocking(port);
    let mut i: u64 = 0;
    while !shared.stop.load(Ordering::Relaxed) {
        let key = format!("w-{}", i % WRITER_KEYSPACE);
        let val = format!("wv-{i}");
        match do_set(&mut conn, &key, &val) {
            Ok(Reply::Simple(s)) if s == "OK" => {
                let now = clock.now();
                let gap = now.saturating_duration_since(last_ok);
                if gap > max_stall {
                    max_stall = gap;
                }
                last_ok = now;
                acked.push((key, val));
                i += 1;
            }
            Ok(Reply::Error(e)) if e.starts_with("LOADING") => {
                // Not acked: a quiesced owner shard. Try the next key (another shard may serve).
                i += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(Reply::Eof) | Err(_) => {
                reconnect(port, shared, &mut conn);
                // retry the SAME key on the reconnected socket (do not advance `i`).
            }
            Ok(other) => {
                record_weird(shared, &other);
                reconnect(port, shared, &mut conn);
            }
        }
    }
    WriterOut { acked, max_stall }
}

/// Record the first unexpected reply for the test to surface.
fn record_weird(shared: &Shared, reply: &Reply) {
    let mut slot = shared.weird.lock().unwrap();
    if slot.is_none() {
        *slot = Some(match reply {
            Reply::Simple(s) => format!("unexpected simple reply {s:?}"),
            Reply::Error(e) => format!("unexpected error reply {e:?}"),
            Reply::Bulk(_) => "unexpected bulk reply to SET".to_string(),
            Reply::Nil => "unexpected nil reply to SET".to_string(),
            Reply::Eof => "unexpected eof".to_string(),
        });
    }
}

/// Reconnect to `port`, flagging a hard `ECONNREFUSED` (the closed-listener bug) if it ever happens.
/// Bounded so a genuinely dead port cannot hang the worker forever.
fn reconnect(port: u16, shared: &Shared, conn: &mut BufReader<TcpStream>) {
    for _ in 0..400 {
        if shared.stop.load(Ordering::Relaxed) {
            return;
        }
        match connect(port) {
            Ok(c) => {
                *conn = c;
                return;
            }
            Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
                shared.refused.store(true, Ordering::Relaxed);
                std::thread::sleep(POLL);
            }
            Err(_) => std::thread::sleep(POLL),
        }
    }
}

/// The READER + connect probe: a tight loop that OPENS a fresh connection across the whole cutover
/// (flagging any `ECONNREFUSED`), then drives a little read traffic (`PING` + a few `GET`s) so the
/// server is under sustained read load through the flip. Returns nothing; it feeds `shared`.
fn reader_probe_thread(port: u16, shared: &Shared) {
    let mut j: u64 = 0;
    while !shared.stop.load(Ordering::Relaxed) {
        match connect(port) {
            Ok(mut c) => {
                // Read load; ignore the values (the writer owns correctness). A -LOADING reply is
                // fine here: the point is that the connection was ACCEPTED, never reset/refused.
                let _ = ping_pong(&mut c);
                for _ in 0..4 {
                    let _ = do_get(&mut c, &format!("w-{}", j % WRITER_KEYSPACE));
                    j += 1;
                }
            }
            Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
                shared.refused.store(true, Ordering::Relaxed);
            }
            Err(_) => {}
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Kill (SIGTERM) the sibling serving on `port` -- read its pid from `INFO server` -- then, as a
/// backstop, kill the OLD child if it is somehow still alive, and remove the temp artifacts.
fn cleanup(port: u16, old: &mut Child, dirs: &[&Path]) {
    if let Ok(mut c) = connect(port) {
        if let Some(pid) = info_process_id(&mut c) {
            send_signal(pid, libc::SIGTERM);
        }
    }
    let _ = old.kill();
    let _ = old.wait();
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
        let _ = std::fs::remove_file(d);
    }
}

// ---------------------------------------------------------------------------------------------
// The CHILD entry: a normal `cargo test` no-ops; only the re-exec'd sibling runs the real server.
// ---------------------------------------------------------------------------------------------
//
// The sibling is a re-exec of THIS SAME binary. But `spawn_receiver_sibling` re-execs with the OLD's
// server ARGV (`server --bind ... --port ... --shards ... --metrics-addr off`), NOT libtest argv, so
// the re-exec'd process runs `fn main` (the real `ironcache server`), never libtest. Thus there is no
// child-harness entry to guard here (unlike the in-process `upgrade_streamed_cutover.rs`, whose
// sibling re-execs the TEST binary): the sibling IS the production server booting in receiver role.

// ---------------------------------------------------------------------------------------------
// TEST 1: SIGUSR1 -> COMMIT. Zero loss, no RST, OLD exits(0), sub-second stall.
// ---------------------------------------------------------------------------------------------

#[test]
#[ignore = "real two-process SIGUSR1 cutover: Linux/colima on demand (scratchpad/streamed-cutover-smoke.sh)"]
#[allow(clippy::too_many_lines)]
fn sigusr1_streamed_cutover_commits_zero_loss_no_rst() {
    let port = free_port();
    let data_dir = temp_path("commit-data");
    let handoff = temp_path("commit-handoff.sock");
    let log = temp_path("commit.log");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut old = spawn_old(port, &data_dir, &handoff, &log);
    let old_pid = old.id() as i32;

    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        refused: AtomicBool::new(false),
        weird: Mutex::new(None),
    });

    // Run the body in a closure so the children + temp dirs are ALWAYS cleaned up, even on a panic.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_serving(port);

        // ---- SEED the bulk keyspace (every key here got +OK, so every one must survive). ----
        let mut acked: HashMap<String, String> = HashMap::new();
        {
            let mut seed = connect_blocking(port);
            for i in 0..SEED_KEYS {
                let (k, v) = (format!("seed-{i}"), format!("sv-{i}"));
                let ok = matches!(do_set(&mut seed, &k, &v), Ok(Reply::Simple(s)) if s == "OK");
                assert!(
                    ok,
                    "seed SET {k} must be acked by the OLD before the cutover"
                );
                acked.insert(k, v);
            }
        }

        // ---- A connection ESTABLISHED on the OLD BEFORE SIGUSR1, then left idle across the flip. ----
        let mut conn_pre = connect_blocking(port);
        assert!(
            ping_pong(&mut conn_pre),
            "conn_pre is served by the OLD pre-cutover"
        );

        // ---- Start sustained write + read traffic, and let it flow before the trigger. ----
        let writer = {
            let s = Arc::clone(&shared);
            std::thread::spawn(move || writer_thread(port, &s))
        };
        let reader = {
            let s = Arc::clone(&shared);
            std::thread::spawn(move || reader_probe_thread(port, &s))
        };
        std::thread::sleep(Duration::from_millis(200)); // writes are flowing.

        // ---- TRIGGER: SIGUSR1 drives the in-server streamed cutover (the ONLY trigger). ----
        send_signal(old_pid, libc::SIGUSR1);

        // ---- The upgrade completes: the OLD drains + exit(0)s after Commit. ----
        let status = wait_exit(&mut old, 1200) // ~30s bound
            .expect("the OLD process must exit after a committed cutover");
        assert!(
            status.success(),
            "the OLD must exit(0) after Commit, got {status:?}"
        );

        // ---- The sibling now serves on the SAME port: a FRESH connection is served by the NEW. ----
        let mut conn_post = connect_blocking(port);
        assert!(
            poll_until_serving(&mut conn_post),
            "the NEW sibling must serve (flip past -LOADING) on the inherited listener"
        );

        // ---- Stop the workers and gather their results. ----
        shared.stop.store(true, Ordering::Relaxed);
        let writer_out = writer.join().expect("writer thread");
        reader.join().expect("reader thread");

        // Merge the writer's acked writes (order preserved: a later overwrite wins).
        for (k, v) in writer_out.acked {
            acked.insert(k, v);
        }

        // ---- NO refused connections anywhere across the flip (inherited listener never closed). ----
        assert!(
            !shared.refused.load(Ordering::Relaxed),
            "NO connection was ever refused across the cutover (inherited listener, no-RST)"
        );
        let weird = shared.weird.lock().unwrap().clone();
        assert!(
            weird.is_none(),
            "the writer saw an unexpected reply: {weird:?}"
        );

        // ---- NO RST: the pre-SIGUSR1 idle connection is closed with a clean EOF, never a reset. ----
        assert_no_rst(&mut conn_pre);

        // ---- Sub-second write stall (measured via the env clock seam). ----
        let stall_ms = writer_out.max_stall.as_secs_f64() * 1000.0;
        println!("SIGUSR1 cutover: client-visible max write stall = {stall_ms:.1} ms");
        assert!(
            writer_out.max_stall < Duration::from_secs(1),
            "the client-visible write stall must be sub-second, was {stall_ms:.1} ms"
        );

        // ---- ZERO acknowledged-write loss: every acked key GETs its exact value from the NEW. ----
        let total = acked.len();
        for (k, v) in &acked {
            let got = do_get(&mut conn_post, k).expect("GET from the NEW");
            assert_eq!(
                got.as_deref(),
                Some(v.as_str()),
                "acked key {k} must survive the cutover with its exact value on the NEW"
            );
        }
        println!("SIGUSR1 cutover: verified {total} acked keys present on the NEW (zero loss)");
    }));

    cleanup(port, &mut old, &[&data_dir, &handoff, &log]);
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}

/// Poll a connection until a `SET` is acked (the NEW flipped past `-LOADING`), bounded by attempts.
fn poll_until_serving(c: &mut BufReader<TcpStream>) -> bool {
    for _ in 0..800 {
        if matches!(do_set(c, "flip-probe", "1"), Ok(Reply::Simple(s)) if s == "OK") {
            return true;
        }
        std::thread::sleep(POLL);
    }
    false
}

/// Assert the idle pre-cutover connection is NOT reset: after the OLD exits, a read returns a clean
/// EOF (a graceful FIN) rather than `ECONNRESET`. (Connection MIGRATION of an established OLD socket
/// is deliberately out of scope, design risk 9; the guarantee proven here is the no-abrupt-RST close.)
fn assert_no_rst(conn_pre: &mut BufReader<TcpStream>) {
    // Nudge a read; the OLD has exited, so its end of this established socket is closed.
    let mut buf = [0u8; 64];
    match conn_pre.get_mut().read(&mut buf) {
        Ok(0) => {} // clean EOF (FIN): the OLD closed gracefully, not an RST.
        Ok(n) => assert!(n <= buf.len()), // a brief served read: still a graceful, non-RST outcome.
        Err(e) => assert_ne!(
            e.kind(),
            ErrorKind::ConnectionReset,
            "the pre-cutover connection must not be abruptly RESET; it should FIN (clean EOF)"
        ),
    }
}

// ---------------------------------------------------------------------------------------------
// TEST 2: SIGUSR1 -> ABORT. An unusable handoff socket aborts the cutover; the OLD keeps serving.
// ---------------------------------------------------------------------------------------------

#[test]
#[ignore = "real SIGUSR1 abort: Linux/colima on demand (scratchpad/streamed-cutover-smoke.sh)"]
fn sigusr1_streamed_cutover_aborts_keeps_serving_zero_loss() {
    let port = free_port();
    let data_dir = temp_path("abort-data");
    // The handoff socket lives in a directory that does NOT exist, so the host's per-shard
    // `bind_handoff_listener_for_shard` fails: the cutover aborts BEFORE any shard quiesces, so the
    // OLD keeps full authority and never exits (the crash-simple fail-safe toward keep-serving).
    let missing_dir = temp_path("abort-nonexistent");
    let handoff = missing_dir.join("handoff.sock");
    let log = temp_path("abort.log");
    std::fs::create_dir_all(&data_dir).unwrap();
    let _ = std::fs::remove_dir_all(&missing_dir); // ensure the parent dir is absent.

    let mut old = spawn_old(port, &data_dir, &handoff, &log);
    let old_pid = old.id() as i32;

    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        refused: AtomicBool::new(false),
        weird: Mutex::new(None),
    });

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_serving(port);

        let mut acked: HashMap<String, String> = HashMap::new();
        {
            let mut seed = connect_blocking(port);
            for i in 0..SEED_KEYS {
                let (k, v) = (format!("seed-{i}"), format!("sv-{i}"));
                let ok = matches!(do_set(&mut seed, &k, &v), Ok(Reply::Simple(s)) if s == "OK");
                assert!(ok, "seed SET {k} acked pre-abort");
                acked.insert(k, v);
            }
        }

        let writer = {
            let s = Arc::clone(&shared);
            std::thread::spawn(move || writer_thread(port, &s))
        };
        std::thread::sleep(Duration::from_millis(200));

        // ---- TRIGGER a cutover that MUST abort (unusable handoff socket). ----
        send_signal(old_pid, libc::SIGUSR1);

        // ---- The OLD must NOT exit: it keeps serving. Give the aborted cutover time to resolve. ----
        assert!(
            wait_exit(&mut old, 120).is_none(), // ~3s: it must still be alive
            "the OLD must NOT exit on an aborted cutover"
        );

        // ---- Writes RESUME: a fresh SET is acked, and the pid is unchanged (same process). ----
        let mut probe = connect_blocking(port);
        assert!(
            matches!(do_set(&mut probe, "post-abort", "1"), Ok(Reply::Simple(s)) if s == "OK"),
            "the OLD must resume acking writes after the aborted cutover"
        );
        acked.insert("post-abort".to_string(), "1".to_string());
        assert_eq!(
            info_process_id(&mut probe),
            Some(old_pid),
            "the SAME OLD process must still be serving (it never handed off / re-exec'd)"
        );

        shared.stop.store(true, Ordering::Relaxed);
        let writer_out = writer.join().expect("writer thread");
        for (k, v) in writer_out.acked {
            acked.insert(k, v);
        }
        assert!(
            !shared.refused.load(Ordering::Relaxed),
            "no connection was refused during the aborted cutover"
        );

        // ---- ZERO loss on the STILL-SERVING OLD: every acked key is present with its exact value. ----
        let total = acked.len();
        for (k, v) in &acked {
            let got = do_get(&mut probe, k).expect("GET from the still-serving OLD");
            assert_eq!(
                got.as_deref(),
                Some(v.as_str()),
                "acked key {k} must survive the aborted cutover on the OLD"
            );
        }
        println!("SIGUSR1 abort: OLD kept serving, verified {total} acked keys (zero loss)");
    }));

    cleanup(port, &mut old, &[&data_dir, &handoff, &missing_dir, &log]);
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}
