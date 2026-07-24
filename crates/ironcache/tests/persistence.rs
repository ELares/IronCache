// SPDX-License-Identifier: MIT OR Apache-2.0
//! Durable on-disk persistence acceptance tests (#58): boot the REAL multi-shard `run_server`
//! with a `data_dir`, drive `SAVE` / `BGSAVE` / `LASTSAVE` over real sockets, RESTART on the same
//! `data_dir`, and assert the keyspace (values + TTLs) is reconstructed from disk.
//!
//! These exercise the WHOLE persistence path end to end: the serve-router interception ->
//! cross-shard `__ICSAVE` fan-out (each shard dumps its own partition via the forkless
//! `snapshot_chunk`) -> the atomic manifest commit -> load-on-boot in each shard's drain loop.

use ironcache::test_support::{
    run_persist_server_for_test, run_persist_server_with_auth_for_test,
    run_persist_server_with_deltas_for_test,
};
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

/// Encode + send an arbitrary command (each arg a bulk string) and return its raw reply.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    client.write_all(frame.as_bytes()).await.unwrap();
    read_one(client).await
}

/// Read one reply (one socket read; the replies here are small and arrive in one segment).
async fn read_one(client: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// Send a command and read a COMPLETE single-line reply (`+...`/`-...`/`:...`), reading until the
/// terminating CRLF so a long error line that arrives across multiple TCP segments is fully read
/// (the fixed-size `read_one` could otherwise return a partial line and desync the stream).
async fn cmd_line(client: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    client.write_all(frame.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    loop {
        let mut b = [0u8; 256];
        let n = client.read(&mut b).await.unwrap();
        if n == 0 {
            break;
        }
        out.extend_from_slice(&b[..n]);
        if out.ends_with(b"\r\n") {
            break;
        }
    }
    out
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
        // Durability footgun fix #2: LASTSAVE is SEEDED on boot from the loaded snapshot's manifest,
        // so it is NON-ZERO immediately (before any in-process save) -- external "snapshot stale"
        // monitoring does not misfire against a 0. The INFO `# Persistence` section reports the same
        // seeded `rdb_last_save_time` (fix #5).
        let ls = cmd1(&mut c, "LASTSAVE").await;
        assert!(
            ls.starts_with(b":") && ls != b":0\r\n",
            "LASTSAVE seeded from the loaded manifest on boot (fix #2): {ls:?}"
        );
        let info_s =
            String::from_utf8_lossy(&cmd(&mut c, &["INFO", "persistence"]).await).into_owned();
        assert!(
            info_s.contains("# Persistence\r\n"),
            "INFO has a # Persistence section: {info_s}"
        );
        assert!(
            !info_s.contains("rdb_last_save_time:0\r\n"),
            "INFO rdb_last_save_time is seeded non-zero on boot (fix #2/#5): {info_s}"
        );
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

/// #571 YIELDING SAVE: a BGSAVE over a populated shard YIELDS between snapshot chunks, so concurrent
/// writes are SERVICED DURING the dump (not blocked until it ends) AND the dump still completes + is
/// loadable + captures every pre-existing key. Populate a shard, kick a BGSAVE, then fire a batch of
/// LIVE writes with a per-op timeout while the background dump runs (each must succeed promptly --
/// proof the shard is not monopolized by the dump), wait for the save to commit, then RESTART and
/// assert every pre-existing key (present for the whole dump) reloaded from the snapshot.
#[tokio::test(flavor = "current_thread")]
async fn bgsave_yields_so_concurrent_writes_are_serviced_and_snapshot_loads() {
    let dir = temp_data_dir("yield");
    let port = free_port();

    // One shard so the whole keyspace + the dump live on shard 0 and the concurrent writes below are
    // homed there too (they must interleave with the SAME shard's dump -- the yield is what lets them).
    let preload = 4000usize;
    {
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;

        // Populate a keyspace large enough that the dump spans MANY snapshot chunks (>> DUMP_CHUNK).
        for i in 0..preload {
            set(&mut c, &format!("pre-{i}"), &format!("v-{i}")).await;
        }

        // Kick the background save; it dumps on shard 0's executor and yields between chunks.
        assert_eq!(
            cmd1(&mut c, "BGSAVE").await,
            b"+Background saving started\r\n"
        );

        // While the dump runs, fire LIVE writes on the SAME connection. Each must be SERVICED
        // promptly (a generous timeout that a full-keyspace block would blow only under a huge
        // keyspace, but the real proof is that they all complete + are readable -- the shard is not
        // monopolized). If the save did NOT yield, these would queue behind the whole dump.
        let live = 200usize;
        for i in 0..live {
            let (key, val) = (format!("live-{i}"), format!("lv-{i}"));
            tokio::time::timeout(Duration::from_secs(10), set(&mut c, &key, &val))
                .await
                .expect("a concurrent write was serviced during BGSAVE (the shard yielded)");
        }
        // A live key written during the save window is served in-memory immediately.
        assert_eq!(get_raw(&mut c, "live-0").await, bulk("lv-0"));

        // The background save commits shortly after; poll LASTSAVE until it advances (dump done +
        // manifest committed).
        let mut advanced = false;
        for _ in 0..200 {
            if cmd1(&mut c, "LASTSAVE").await != b":0\r\n" {
                advanced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            advanced,
            "the yielding BGSAVE still committed (LASTSAVE advanced)"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // The committed manifest exists, and a fresh server on the same data_dir reloads EVERY
    // pre-existing key (each was present for the whole dump -> captured at least once, SCAN-stable).
    assert!(dir.join("dump.manifest").exists(), "manifest committed");
    {
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        for i in 0..preload {
            assert_eq!(
                get_raw(&mut c, &format!("pre-{i}")).await,
                bulk(&format!("v-{i}")),
                "pre-{i} (present for the whole dump) reloaded from the yielding snapshot"
            );
        }
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// #577 SAVE-BACKPRESSURE knob: `CONFIG SET save-backpressure-percent` is LIVE-settable + VALIDATED,
/// and a BGSAVE with the knob set still commits (manifest-last crash-safety unchanged), still services
/// concurrent writes DURING the dump, and reloads every pre-existing key on restart. NOTE (#576): the
/// freeze-based off-thread save does NO serving-side copy loop for the knob to throttle, so the knob is
/// now INERT for pacing (it stays settable for compatibility); this asserts the knob is still wired end
/// to end (GET/SET/validation) and that a save with it set preserves correctness.
#[tokio::test(flavor = "current_thread")]
async fn bgsave_backpressure_percent_is_live_settable_validated_and_still_commits() {
    let dir = temp_data_dir("throttle");
    let port = free_port();

    let preload = 4000usize;
    {
        // One shard so the whole keyspace + the dump live on shard 0 and the concurrent writes below
        // are homed there too (they must interleave with the SAME shard's throttled dump).
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;

        // The knob DEFAULTS to 100 (no throttle -- byte-identical saves).
        let got = cmd(&mut c, &["CONFIG", "GET", "save-backpressure-percent"]).await;
        assert!(
            got.windows(3).any(|w| w == b"100"),
            "default is 100 (no throttle), got {:?}",
            String::from_utf8_lossy(&got)
        );

        // Invalid values are REJECTED (never a silent clamp): 0 (would sleep forever) and 101.
        for bad in ["0", "101"] {
            let r = cmd(&mut c, &["CONFIG", "SET", "save-backpressure-percent", bad]).await;
            assert_eq!(
                r.first(),
                Some(&b'-'),
                "CONFIG SET save-backpressure-percent {bad} should error, got {:?}",
                String::from_utf8_lossy(&r)
            );
        }

        // Turn the throttle ON (10% of the core) LIVE -- +OK, and GET reflects it.
        assert_eq!(
            cmd(
                &mut c,
                &["CONFIG", "SET", "save-backpressure-percent", "10"]
            )
            .await,
            b"+OK\r\n"
        );
        let got = cmd(&mut c, &["CONFIG", "GET", "save-backpressure-percent"]).await;
        assert!(
            got.windows(2).any(|w| w == b"10"),
            "GET reflects the live throttle, got {:?}",
            String::from_utf8_lossy(&got)
        );

        // Populate a keyspace large enough that the throttled dump spans MANY chunks (>> DUMP_CHUNK).
        for i in 0..preload {
            set(&mut c, &format!("pre-{i}"), &format!("v-{i}")).await;
        }

        // Kick the THROTTLED background save; it sleeps proportionally between chunks on shard 0.
        assert_eq!(
            cmd1(&mut c, "BGSAVE").await,
            b"+Background saving started\r\n"
        );

        // Concurrent writes are STILL serviced during the throttled dump (the proportional sleep lets
        // the shard drain queued writes -- the whole point of the backpressure).
        let live = 100usize;
        for i in 0..live {
            let (key, val) = (format!("live-{i}"), format!("lv-{i}"));
            tokio::time::timeout(Duration::from_secs(20), set(&mut c, &key, &val))
                .await
                .expect("a concurrent write was serviced during the THROTTLED BGSAVE");
        }

        // The throttled save still COMMITS (manifest-last): poll LASTSAVE until it advances.
        let mut advanced = false;
        for _ in 0..600 {
            if cmd1(&mut c, "LASTSAVE").await != b":0\r\n" {
                advanced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            advanced,
            "the THROTTLED BGSAVE still committed (LASTSAVE advanced)"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // The committed manifest exists, and a restart reloads EVERY pre-existing key (present for the
    // whole throttled dump -> captured at least once, SCAN-stable) -- the throttle changed timing, not
    // correctness or crash-safety.
    assert!(dir.join("dump.manifest").exists(), "manifest committed");
    {
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        for i in 0..preload {
            assert_eq!(
                get_raw(&mut c, &format!("pre-{i}")).await,
                bulk(&format!("v-{i}")),
                "pre-{i} reloaded from the throttled snapshot"
            );
        }
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// #576 PR-B OFF-THREAD PERSIST: a BGSAVE encodes + fsyncs its keyspace on a DEDICATED persist thread
/// (`ic-persist-<n>`), so the serving shard stays UNCONTENDED during the dump. #571 (yield) and #578
/// (throttle) only REDUCED the contention because the encode was still on the serving core; #576
/// measured a 3.6s p99.9 STALL on c7g from that on-core encode+fsync. This drives LIVE writes on the
/// SAME shard DURING an UNTHROTTLED (pct==100, the p99.9-relevant config) BGSAVE over a populated
/// keyspace and asserts they are serviced with LOW latency THROUGHOUT -- NOT merely "eventually" like
/// the #571 test: every op completes well UNDER the multi-second stall the on-core encode produced (a
/// TIGHT per-op timeout, 10x tighter than the #571 test's), AND the whole batch of writes fired during
/// the dump completes PROMPTLY (a full-keyspace on-core encode would monopolize the shard for its whole
/// duration). The dump still commits (manifest-last crash-safety) and every pre-existing key
/// round-trips on restart (the copy is SCAN-stable, so a key present the whole dump is captured).
///
/// Latency is asserted via `tokio::time::timeout` (the determinism seam forbids ad-hoc `Instant` on
/// any path, ADR-0003; a socket-level test has no access to the shard Env clock the bench harness times
/// through). This is a unit-scale PROXY: a small keyspace on one box cannot reproduce the c7g
/// multi-second stall, so the ABSOLUTE p99.9 win is re-measured on c7g via scripts/bench/tail.sh. The
/// unit assertion guards against a gross regression and pins the off-thread path (the sole save path)
/// end to end, with GENEROUS bounds so single-box scheduling noise does not flake it.
#[tokio::test(flavor = "current_thread")]
async fn bgsave_off_thread_keeps_datapath_low_latency_during_dump() {
    let dir = temp_data_dir("offthread");
    let port = free_port();

    // One shard so the whole keyspace + the dump live on shard 0 and the ops below are homed there too
    // (they must contend with the SAME shard's dump -- the off-thread persist is what keeps them fast).
    // A keyspace large enough that the dump spans MANY chunks and stays in-flight while the ops run.
    let preload = 15000usize;
    {
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        for i in 0..preload {
            set(&mut c, &format!("pre-{i}"), &format!("v-{i}")).await;
        }

        // Kick the UNTHROTTLED background save; it FREEZES shard 0 (per-slot Arc-COW, #576) and
        // encodes+fsyncs the frozen slots on the persist thread OFF the serving core. The dump is now
        // in-flight for the ops below.
        assert_eq!(
            cmd1(&mut c, "BGSAVE").await,
            b"+Background saving started\r\n"
        );

        // While the dump runs, fire LIVE writes on the SAME shard. Two nested bounds assert the datapath
        // stays uncontended: (a) each op completes within a TIGHT per-op timeout (no multi-second stall
        // -- #576 measured 3.6s; a full-keyspace on-core encode+fsync would blow this), and (b) the
        // WHOLE batch completes within an aggregate timeout (the shard is not monopolized for the dump's
        // duration). We fire immediately after BGSAVE so the ops overlap the in-flight dump.
        let live = 300usize;
        tokio::time::timeout(Duration::from_secs(10), async {
            for i in 0..live {
                let key = format!("live-{i}");
                tokio::time::timeout(Duration::from_secs(1), set(&mut c, &key, "x"))
                    .await
                    .expect(
                        "a concurrent write was serviced with NO multi-second stall during BGSAVE \
                         (the encode + fsync run off the serving core)",
                    );
            }
        })
        .await
        .expect(
            "the whole batch of writes DURING the dump completed promptly -- the shard was not \
             monopolized by the encode + fsync (they run on the persist thread)",
        );

        // A live key written during the save window is served in-memory immediately.
        assert_eq!(get_raw(&mut c, "live-0").await, bulk("x"));

        // The off-thread save still COMMITS (manifest-last): poll LASTSAVE until it advances.
        let mut advanced = false;
        for _ in 0..600 {
            if cmd1(&mut c, "LASTSAVE").await != b":0\r\n" {
                advanced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            advanced,
            "the off-thread BGSAVE still committed (LASTSAVE advanced)"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // The committed manifest exists, and a restart reloads EVERY pre-existing key (present for the
    // whole dump -> captured at least once, SCAN-stable) -- the off-thread copy changed WHERE the
    // encode runs, not the file's correctness or crash-safety.
    assert!(dir.join("dump.manifest").exists(), "manifest committed");
    {
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        for i in 0..preload {
            assert_eq!(
                get_raw(&mut c, &format!("pre-{i}")).await,
                bulk(&format!("v-{i}")),
                "pre-{i} reloaded from the off-thread snapshot"
            );
        }
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// #576 PER-SLOT ARC-COW during a BGSAVE, through the REAL command path: APPEND / INCR / DEL / SET
/// on keys being dumped are serviced with LOW latency (the datapath is uncontended -- a write COWs
/// a still-frozen slot then mutates the fresh copy; the O(N) encode is off the serving core), AND
/// the reloaded snapshot is self-consistent (a key present at freeze reloads with a VALID, non-torn
/// value -- either its pre-freeze value or, if the write landed pre-freeze, its post-write value,
/// never garbage), AND the live in-place writes are in the live store afterward. This exercises the
/// in-place mutation (APPEND/INCR) + free (DEL/overwrite) COW hazards end to end. Latency is bounded
/// via `tokio::time::timeout` (the determinism seam forbids ad-hoc `Instant`, ADR-0003); GENEROUS
/// bounds absorb single-box noise, and the absolute p99.9 win is re-measured on c7g.
#[tokio::test(flavor = "current_thread")]
async fn bgsave_cow_serves_inplace_writes_and_reloads_consistently() {
    let dir = temp_data_dir("cow");
    let port = free_port();

    // One shard so the whole keyspace + the dump + the concurrent in-place writes are homed on
    // shard 0 (they must contend with the SAME shard's dump). Large enough that the dump stays
    // in-flight while the writes run.
    let preload = 20000usize;
    let ints = 2000usize;
    {
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        for i in 0..preload {
            set(&mut c, &format!("s-{i}"), &format!("v-{i}")).await;
        }
        for i in 0..ints {
            set(&mut c, &format!("n-{i}"), "1000").await;
        }

        // Kick the UNTHROTTLED background save; it FREEZES shard 0 and encodes+fsyncs the frozen
        // slots off the serving core, so the in-place writes below overlap the in-flight dump.
        assert_eq!(
            cmd1(&mut c, "BGSAVE").await,
            b"+Background saving started\r\n"
        );

        // Fire in-place + free + create writes DURING the dump; each must complete PROMPTLY (a
        // frozen-slot COW is a bounded one-time deep-clone, never a multi-second stall).
        let live = 600usize;
        tokio::time::timeout(Duration::from_secs(20), async {
            for i in 0..live {
                // APPEND to a pre-existing string (in-place mutation of a possibly-frozen pointee).
                tokio::time::timeout(
                    Duration::from_secs(1),
                    cmd(&mut c, &["APPEND", &format!("s-{i}"), "-x"]),
                )
                .await
                .expect("APPEND serviced promptly during BGSAVE (COW, no stall)");
                // INCR a pre-existing int (in-place mutation of a possibly-frozen pointee).
                tokio::time::timeout(
                    Duration::from_secs(1),
                    cmd(&mut c, &["INCR", &format!("n-{}", i % ints)]),
                )
                .await
                .expect("INCR serviced promptly during BGSAVE");
                // DELETE a pre-existing string (frees the pointee -- must not touch the frozen one).
                tokio::time::timeout(
                    Duration::from_secs(1),
                    cmd(&mut c, &["DEL", &format!("s-{}", preload - 1 - i)]),
                )
                .await
                .expect("DEL serviced promptly during BGSAVE");
                // A brand-new key created mid-save.
                tokio::time::timeout(
                    Duration::from_secs(1),
                    set(&mut c, &format!("fresh-{i}"), "new"),
                )
                .await
                .expect("SET serviced promptly during BGSAVE");
            }
        })
        .await
        .expect("the whole batch of in-place writes during the dump completed promptly");

        // The live store reflects the in-place writes (mutations landed on the fresh COW copies).
        assert_eq!(
            get_raw(&mut c, "s-0").await,
            bulk("v-0-x"),
            "APPEND applied to the live store"
        );
        assert_eq!(
            get_raw(&mut c, "n-0").await,
            bulk("1001"),
            "INCR applied to the live store"
        );
        assert_eq!(get_raw(&mut c, "fresh-0").await, bulk("new"));

        // The save commits (manifest-last).
        let mut advanced = false;
        for _ in 0..600 {
            if cmd1(&mut c, "LASTSAVE").await != b":0\r\n" {
                advanced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(advanced, "the COW BGSAVE committed (LASTSAVE advanced)");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // Restart: the reloaded snapshot is self-consistent.
    assert!(dir.join("dump.manifest").exists(), "manifest committed");
    {
        let server = run_persist_server_for_test(port, 1, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        // A string never touched during the save reloads EXACTLY (present the whole dump).
        let untouched = preload / 2; // 10000: outside the appended (0..600) + deleted (19400..) ranges.
        assert_eq!(
            get_raw(&mut c, &format!("s-{untouched}")).await,
            bulk(&format!("v-{untouched}")),
            "an untouched key reloads exactly"
        );
        // An APPENDed key reloads to a VALID value: its pre-freeze "v-0" (the COW isolated the
        // dump from a post-freeze APPEND) OR "v-0-x" (the APPEND landed pre-freeze) -- never a
        // torn/garbage value.
        let got = get_raw(&mut c, "s-0").await;
        assert!(
            got == bulk("v-0") || got == bulk("v-0-x"),
            "s-0 reloads self-consistent (pre- or post-freeze value, never torn), got {:?}",
            String::from_utf8_lossy(&got)
        );
        // An INCRed key reloads to a VALID value: "1000" (pre-freeze) or "1001" (INCR pre-freeze).
        let got_n = get_raw(&mut c, "n-0").await;
        assert!(
            got_n == bulk("1000") || got_n == bulk("1001"),
            "n-0 reloads self-consistent, got {:?}",
            String::from_utf8_lossy(&got_n)
        );
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

/// H2 REGRESSION: with `requirepass` set, the persistence-command interception is AUTH-GATED. An
/// UNAUTHENTICATED client gets `-NOAUTH` for SAVE / BGSAVE / LASTSAVE and NO snapshot is written;
/// after AUTH the same commands work and a snapshot is committed. (Before the fix the interception
/// returned before dispatch's auth gate, so an unauthenticated client could DoS via SAVE/BGSAVE and
/// read LASTSAVE.)
#[tokio::test(flavor = "current_thread")]
async fn persistence_commands_are_auth_gated() {
    let dir = temp_data_dir("auth");
    let port = free_port();
    let server = run_persist_server_with_auth_for_test(port, 2, dir.clone(), "s3cr3t");
    let mut c = connect_retry(port).await;

    // UNAUTHENTICATED: every persistence command is rejected with NOAUTH (and performs no save).
    let noauth = b"-NOAUTH Authentication required.\r\n";
    assert_eq!(cmd1(&mut c, "SAVE").await, noauth, "unauth SAVE is NOAUTH");
    assert_eq!(
        cmd1(&mut c, "BGSAVE").await,
        noauth,
        "unauth BGSAVE is NOAUTH"
    );
    assert_eq!(
        cmd1(&mut c, "LASTSAVE").await,
        noauth,
        "unauth LASTSAVE is NOAUTH"
    );
    // Give any (wrongly-spawned) background save a moment, then assert NOTHING was written.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !dir.join("dump.manifest").exists(),
        "no snapshot committed by an unauthenticated client"
    );

    // AUTH, then the same commands succeed.
    assert_eq!(cmd(&mut c, &["AUTH", "s3cr3t"]).await, b"+OK\r\n");
    assert_eq!(
        cmd1(&mut c, "LASTSAVE").await,
        b":0\r\n",
        "auth LASTSAVE ok"
    );
    set(&mut c, "k", "v").await;
    assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n", "auth SAVE persists");
    assert!(
        dir.join("dump.manifest").exists(),
        "an authenticated SAVE commits the snapshot"
    );
    drop(c);
    server.shutdown_and_join().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// Broadened round-trip (the task's test-gap fix): a key of EVERY core type -- string, list, hash,
/// set, zset, plus a bitmap (string-backed) and an HLL -- SAVE, restart, and assert each round-trips
/// intact through the snapshot codec.
#[tokio::test(flavor = "current_thread")]
async fn all_types_round_trip_through_save_restart() {
    let dir = temp_data_dir("alltypes");
    let port = free_port();

    // -- Boot 1: write one key of each type, SAVE. --
    {
        let server = run_persist_server_for_test(port, 4, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        set(&mut c, "str", "hello").await;
        assert_eq!(
            cmd(&mut c, &["RPUSH", "lst", "a", "b", "c"]).await,
            b":3\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["HSET", "hsh", "f1", "v1", "f2", "v2"]).await,
            b":2\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["SADD", "set", "x", "y", "z"]).await,
            b":3\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["ZADD", "zst", "1", "one", "2", "two"]).await,
            b":2\r\n"
        );
        // A bitmap (string-backed): SETBIT returns the prior bit (0).
        assert_eq!(cmd(&mut c, &["SETBIT", "bmp", "7", "1"]).await, b":0\r\n");
        // An HLL: PFADD of new elements returns 1 (the registers changed).
        assert_eq!(
            cmd(&mut c, &["PFADD", "hll", "e1", "e2", "e3"]).await,
            b":1\r\n"
        );
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // -- Boot 2: a fresh server reloads each type intact. --
    {
        let server = run_persist_server_for_test(port, 4, dir.clone(), 0, 0);
        let mut c = connect_retry(port).await;
        assert_eq!(get_raw(&mut c, "str").await, bulk("hello"), "string");
        // List: LRANGE 0 -1 -> [a b c].
        assert_eq!(
            cmd(&mut c, &["LRANGE", "lst", "0", "-1"]).await,
            b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
            "list order + members"
        );
        // Hash: HGET each field.
        assert_eq!(
            cmd(&mut c, &["HGET", "hsh", "f1"]).await,
            bulk("v1"),
            "hash f1"
        );
        assert_eq!(
            cmd(&mut c, &["HGET", "hsh", "f2"]).await,
            bulk("v2"),
            "hash f2"
        );
        // Set: SCARD == 3, and a member is present.
        assert_eq!(cmd(&mut c, &["SCARD", "set"]).await, b":3\r\n", "set card");
        assert_eq!(
            cmd(&mut c, &["SISMEMBER", "set", "y"]).await,
            b":1\r\n",
            "set member"
        );
        // Zset: ZSCORE round-trips the score.
        assert_eq!(
            cmd(&mut c, &["ZSCORE", "zst", "two"]).await,
            bulk("2"),
            "zset score"
        );
        // Bitmap: the set bit survives.
        assert_eq!(
            cmd(&mut c, &["GETBIT", "bmp", "7"]).await,
            b":1\r\n",
            "bitmap bit"
        );
        // HLL: the cardinality estimate is 3 for our 3 distinct elements.
        assert_eq!(
            cmd(&mut c, &["PFCOUNT", "hll"]).await,
            b":3\r\n",
            "hll count"
        );
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Durability footgun fixes #1 + #5 over the wire: `CONFIG SET save` ACTUALLY updates the runtime
/// save policy (and `CONFIG GET save` reports the real policy), `CONFIG SET appendonly yes` is
/// REFUSED with an error (no AOF), and the INFO `# Persistence` / `# Keyspace` sections render with
/// the live policy + per-db key counts. A booted-with-policy-OFF node is used to prove the policy is
/// no longer frozen at boot.
#[tokio::test(flavor = "current_thread")]
async fn config_save_appendonly_and_info_sections_over_the_wire() {
    let dir = temp_data_dir("config-save-info");
    let port = free_port();
    // Boot with the periodic save policy OFF (0/0).
    let server = run_persist_server_for_test(port, 2, dir.clone(), 0, 0);
    let mut c = connect_retry(port).await;

    // CONFIG GET save reports the REAL policy: empty when off (no longer a fixed empty stub lie).
    let g = cmd(&mut c, &["CONFIG", "GET", "save"]).await;
    let gs = String::from_utf8_lossy(&g);
    assert!(
        gs.contains("save"),
        "CONFIG GET save returns the param: {gs}"
    );

    // CONFIG SET save "900 1" ACTUALLY updates the policy; CONFIG GET save reports it back.
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "save", "900 1"]).await,
        b"+OK\r\n"
    );
    let g2 = cmd(&mut c, &["CONFIG", "GET", "save"]).await;
    assert!(
        String::from_utf8_lossy(&g2).contains("900 1"),
        "CONFIG GET save reports the configured policy (fix #1): {:?}",
        String::from_utf8_lossy(&g2)
    );

    // CONFIG SET appendonly yes is REFUSED with an explicit error (no AOF in this build, fix #1).
    // The error line is ~140 bytes and can arrive across more than one TCP segment, so read the
    // COMPLETE single-line reply (up to its terminating CRLF) rather than one socket read.
    let ao = cmd_line(&mut c, &["CONFIG", "SET", "appendonly", "yes"]).await;
    let aos = String::from_utf8_lossy(&ao);
    assert!(
        aos.starts_with("-ERR") && aos.contains("appendonly"),
        "CONFIG SET appendonly yes is refused (fix #1): {aos}"
    );
    // CONFIG GET appendonly is always `no`.
    assert!(
        String::from_utf8_lossy(&cmd(&mut c, &["CONFIG", "GET", "appendonly"]).await)
            .contains("no"),
        "CONFIG GET appendonly is no"
    );

    // Populate some keys, then the FILTERED INFO sections reflect the live policy + changes-since-
    // save (fix #5) and the per-db key counts. A filtered section reply is small (well under one
    // socket read), so the simple `read_one`-based `cmd` reads it whole.
    for i in 0..7 {
        set(&mut c, &format!("k{i}"), "v").await;
    }
    let persistence_section =
        String::from_utf8_lossy(&cmd(&mut c, &["INFO", "persistence"]).await).into_owned();
    assert!(
        persistence_section.contains("# Persistence\r\n"),
        "{persistence_section}"
    );
    assert!(
        persistence_section.contains("save:900 1\r\n"),
        "INFO save policy reflects the live CONFIG SET save (fix #1/#5): {persistence_section}"
    );
    assert!(
        persistence_section.contains("rdb_changes_since_last_save:"),
        "INFO changes-since-save: {persistence_section}"
    );
    let keyspace_section =
        String::from_utf8_lossy(&cmd(&mut c, &["INFO", "keyspace"]).await).into_owned();
    assert!(
        keyspace_section.contains("# Keyspace\r\n"),
        "INFO keyspace section (fix #5): {keyspace_section}"
    );
    assert!(
        keyspace_section.contains("db0:keys="),
        "INFO keyspace db0 line: {keyspace_section}"
    );

    // Disable the periodic policy again before teardown: with a save policy ACTIVE, the graceful
    // shutdown performs a save-on-exit and the save-host shard calls `std::process::exit(0)` (the
    // orchestrator contract), which would terminate the TEST process. Setting `save ""` returns the
    // node to the explicit-save-only posture so `shutdown_and_join` is a clean in-process stop. This
    // is itself a check that `CONFIG SET save ""` disables the runtime policy.
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "save", ""]).await,
        b"+OK\r\n"
    );

    drop(c);
    server.shutdown_and_join().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// Count the committed delta files (`dump-shard-<n>-delta-<epoch>.icsd`) present in `dir`.
fn count_delta_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".icsd"))
        .count()
}

/// #676 INCREMENTAL DELTA SNAPSHOTS end to end: with `snapshot_deltas` on, the first save is a full
/// BASE and subsequent saves write DELTAS of only the mutated keys; a restart reconstructs the
/// keyspace by folding the delta chain onto the base (later PUTs win, TOMBSTONEs remove, untouched
/// base keys survive). Also proves the post-boot RE-BASE (a fresh process owes a base before it may
/// delta again, so no write since load is skipped) and a delta appended onto that re-based generation.
#[tokio::test(flavor = "current_thread")]
async fn delta_snapshots_base_then_delta_reload_merges_chain() {
    let dir = temp_data_dir("delta-chain");
    let port = free_port();

    // -- Boot 1: base save, then two delta saves. --
    {
        let server = run_persist_server_with_deltas_for_test(port, 3, dir.clone());
        let mut c = connect_retry(port).await;
        // Ten keys at generation 0, then a BASE save (the first save is always a base).
        for i in 0..10 {
            set(&mut c, &format!("k{i}"), &format!("v{i}-g0")).await;
        }
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        assert!(
            dir.join("dump.manifest").exists(),
            "base manifest committed"
        );
        assert_eq!(
            count_delta_files(&dir),
            0,
            "the first save is a base -> no delta files yet"
        );
        let ls_base = cmd1(&mut c, "LASTSAVE").await;

        // Round A: overwrite k0, DELETE k1 (a tombstone), add a brand-new k10. SAVE -> delta 1.
        set(&mut c, "k0", "v0-g1").await;
        assert_eq!(cmd(&mut c, &["DEL", "k1"]).await, b":1\r\n");
        set(&mut c, "k10", "v10-new").await;
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        assert!(
            count_delta_files(&dir) >= 1,
            "a delta save wrote at least one .icsd delta file: found {}",
            count_delta_files(&dir)
        );
        let ls_delta = cmd1(&mut c, "LASTSAVE").await;
        assert!(
            ls_delta.starts_with(b":") && ls_delta != b":0\r\n",
            "LASTSAVE stamped on the delta save too: {ls_delta:?} (base was {ls_base:?})"
        );

        // Round B: overwrite k2 and k0 again (k0's latest value must win on reload). SAVE -> delta 2.
        set(&mut c, "k2", "v2-g2").await;
        set(&mut c, "k0", "v0-g2").await;
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // -- Boot 2: reload folds base + delta 1 + delta 2 into the merged keyspace. --
    {
        let server = run_persist_server_with_deltas_for_test(port, 3, dir.clone());
        let mut c = connect_retry(port).await;
        // k0: last write (v0-g2) wins across the chain.
        assert_eq!(get_raw(&mut c, "k0").await, bulk("v0-g2"), "k0 latest wins");
        // k1: tombstoned in delta 1 -> gone.
        assert_eq!(get_raw(&mut c, "k1").await, b"$-1\r\n", "k1 tombstoned");
        // k2: overwritten in delta 2.
        assert_eq!(get_raw(&mut c, "k2").await, bulk("v2-g2"), "k2 delta value");
        // k3..k9: untouched -> survive from the base.
        for i in 3..10 {
            assert_eq!(
                get_raw(&mut c, &format!("k{i}")).await,
                bulk(&format!("v{i}-g0")),
                "k{i} survives from the base"
            );
        }
        // k10: created only in delta 1.
        assert_eq!(
            get_raw(&mut c, "k10").await,
            bulk("v10-new"),
            "k10 from delta"
        );
        // 10 base keys - k1 deleted + k10 added = 10.
        assert_eq!(cmd1(&mut c, "DBSIZE").await, b":10\r\n", "merged key count");

        // The FIRST save after a fresh boot must RE-BASE (dirty tracking was not armed for writes
        // since load), so add a key and SAVE: this is a base, re-establishing the generation.
        set(&mut c, "k11", "v11-new").await;
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        // Round C: a delta ON the re-based generation (overwrite k3). SAVE -> delta.
        set(&mut c, "k3", "v3-g3").await;
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // -- Boot 3: the re-based base + its delta reload correctly. --
    {
        let server = run_persist_server_with_deltas_for_test(port, 3, dir.clone());
        let mut c = connect_retry(port).await;
        assert_eq!(
            get_raw(&mut c, "k0").await,
            bulk("v0-g2"),
            "k0 survives re-base"
        );
        assert_eq!(get_raw(&mut c, "k1").await, b"$-1\r\n", "k1 stays deleted");
        assert_eq!(
            get_raw(&mut c, "k3").await,
            bulk("v3-g3"),
            "k3 round-C delta"
        );
        assert_eq!(
            get_raw(&mut c, "k11").await,
            bulk("v11-new"),
            "k11 in re-base"
        );
        assert_eq!(
            cmd1(&mut c, "DBSIZE").await,
            b":11\r\n",
            "count after re-base + delta"
        );
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&dir).ok();
}
