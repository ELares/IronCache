// SPDX-License-Identifier: MIT OR Apache-2.0
//! Upgrade snapshot-to-tmpfs HANDOFF acceptance tests (#390, Phase 2b): boot the REAL multi-shard
//! `run_server` with a `data_dir` + an `upgrade_handoff_dir`, drive `SAVE HANDOFF` over real
//! sockets, and assert:
//!
//! 1. when tmpfs is available, the handoff is STAGED ON TMPFS (not `data_dir`), a fresh boot LOADS
//!    every key from it, and the tmpfs staging dir is CLEANED UP after load;
//! 2. when tmpfs is unavailable (a non-tmpfs `upgrade_handoff_dir`), the handoff FALLS BACK to the
//!    durable `data_dir` and still reloads;
//! 3. the DURABLE periodic/manual `SAVE` path is UNCHANGED (writes `data_dir`, never tmpfs);
//! 4. CRASH-SAFETY: after a simulated mid-upgrade failure (the ephemeral tmpfs handoff is lost), the
//!    last DURABLE `data_dir` snapshot still loads (the durable path is untouched by the handoff).
//!
//! These exercise the WHOLE handoff path end to end: the serve-router `SAVE HANDOFF` interception ->
//! the RAM-headroom guard + tmpfs target choice -> the cross-shard `__ICSAVE` fan-out into the
//! chosen dir -> the manifest commit -> load-on-boot resolving the handoff vs the durable snapshot
//! -> the post-load tmpfs cleanup.

use ironcache::test_support::run_persist_server_with_handoff_for_test;
use std::path::{Path, PathBuf};
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

/// A throwaway on-disk temp dir unique to the test + process for the DURABLE snapshot files.
fn temp_data_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ic-handoff-it-{tag}-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A UNIQUE tmpfs base under `/dev/shm` for this test + process, or `None` when tmpfs staging is
/// unavailable (non-Linux, no `/dev/shm`, `/dev/shm` not a tmpfs) so a tmpfs-only test can skip. The
/// uniqueness avoids the node-local `ironcache-handoff` name colliding across parallel tests.
fn unique_tmpfs_base(tag: &str) -> Option<PathBuf> {
    let base =
        PathBuf::from("/dev/shm").join(format!("ic-handoff-it-{tag}-{}", std::process::id()));
    // A lexical /proc/mounts tmpfs check (works even though `base` does not exist yet).
    ironcache::handoff::usable_tmpfs_base(Some(&base))
}

/// The staging dir the handoff snapshot files land in, under a tmpfs `base`.
fn staging(base: &Path) -> PathBuf {
    ironcache::handoff::handoff_staging_dir(base)
}

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

async fn expect_reply(client: &mut TcpStream, expect: &[u8]) {
    let mut buf = vec![0u8; expect.len()];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, expect, "got {:?}", String::from_utf8_lossy(&buf));
}

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

async fn get_raw(client: &mut TcpStream, key: &str) -> Vec<u8> {
    let frame = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client.write_all(frame.as_bytes()).await.unwrap();
    read_one(client).await
}

/// Send a bare arity-1 command (SAVE / LASTSAVE / DBSIZE) and return its raw reply.
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

async fn read_one(client: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

fn bulk(val: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", val.len(), val).into_bytes()
}

/// A directory (dump.manifest) that exists.
fn has_manifest(dir: &Path) -> bool {
    dir.join("dump.manifest").exists()
}

/// (1) The tmpfs handoff round-trip: `SAVE HANDOFF` STAGES ON TMPFS (not data_dir), a fresh boot
/// LOADS every key from the tmpfs handoff, and the staging dir is CLEANED UP after load-on-boot.
/// Skips cleanly where tmpfs is unavailable (non-Linux / no `/dev/shm`).
#[tokio::test(flavor = "current_thread")]
async fn handoff_stages_on_tmpfs_and_reloads_then_cleans_up() {
    let Some(base) = unique_tmpfs_base("roundtrip") else {
        eprintln!("skipping: tmpfs (/dev/shm) unavailable on this host");
        return;
    };
    std::fs::remove_dir_all(&base).ok();
    let dir = temp_data_dir("roundtrip");
    let stage = staging(&base);
    let port = free_port();

    // -- Boot 1: populate + SAVE HANDOFF (tmpfs). --
    {
        let server =
            run_persist_server_with_handoff_for_test(port, 4, dir.clone(), Some(base.clone()));
        let mut c = connect_retry(port).await;
        for i in 0..40 {
            set(&mut c, &format!("hk-{i}"), &format!("hv-{i}")).await;
        }
        // SAVE HANDOFF blocks until committed, replies +OK.
        assert_eq!(
            cmd(&mut c, &["SAVE", "HANDOFF"]).await,
            b"+OK\r\n",
            "SAVE HANDOFF commits"
        );
        // LASTSAVE advanced (the handoff stamps it so the upgrade SAVE-first confirmation works).
        let ls = cmd1(&mut c, "LASTSAVE").await;
        assert!(
            ls.starts_with(b":") && ls != b":0\r\n",
            "LASTSAVE advanced: {ls:?}"
        );
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // The handoff was staged ON TMPFS, and the DURABLE data_dir was left UNTOUCHED (no manifest).
    assert!(
        has_manifest(&stage),
        "the handoff manifest is on tmpfs at {}",
        stage.display()
    );
    assert!(
        !has_manifest(&dir),
        "the durable data_dir is UNTOUCHED by the tmpfs handoff (no manifest at {})",
        dir.display()
    );

    // -- Boot 2: a FRESH server LOADS the keyspace from the tmpfs handoff, then cleans it up. --
    {
        let server =
            run_persist_server_with_handoff_for_test(port, 4, dir.clone(), Some(base.clone()));
        let mut c = connect_retry(port).await;
        // LASTSAVE is seeded from the LOADED (tmpfs handoff) manifest on boot.
        let ls = cmd1(&mut c, "LASTSAVE").await;
        assert!(
            ls.starts_with(b":") && ls != b":0\r\n",
            "LASTSAVE seeded from the loaded handoff manifest: {ls:?}"
        );
        for i in 0..40 {
            assert_eq!(
                get_raw(&mut c, &format!("hk-{i}")).await,
                bulk(&format!("hv-{i}")),
                "hk-{i} reloaded from the tmpfs handoff after restart"
            );
        }
        assert_eq!(
            cmd1(&mut c, "DBSIZE").await,
            b":40\r\n",
            "every key reloaded"
        );
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // The ephemeral tmpfs handoff was CLEANED UP after the successful load (never leaked). Cleanup
    // runs on the drain loop right after load; poll briefly for it to disappear.
    let mut cleaned = false;
    for _ in 0..100 {
        if !stage.exists() {
            cleaned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        cleaned,
        "the tmpfs handoff staging dir was removed after load-on-boot: {}",
        stage.display()
    );

    std::fs::remove_dir_all(&base).ok();
    std::fs::remove_dir_all(&dir).ok();
}

/// (2) FALLBACK: when the configured `upgrade_handoff_dir` is NOT a tmpfs mount, `SAVE HANDOFF`
/// falls back to the durable `data_dir` (a warning, not a failure) and the keyspace still reloads.
/// Cross-platform (uses a non-tmpfs on-disk handoff dir, which the tmpfs guard rejects everywhere).
#[tokio::test(flavor = "current_thread")]
async fn handoff_falls_back_to_data_dir_when_tmpfs_unavailable() {
    let dir = temp_data_dir("fallback");
    // A regular on-disk directory: `usable_tmpfs_base` rejects it (not a tmpfs mount) -> data_dir.
    let disk_handoff = temp_data_dir("fallback-handoff");
    let port = free_port();

    {
        let server = run_persist_server_with_handoff_for_test(
            port,
            3,
            dir.clone(),
            Some(disk_handoff.clone()),
        );
        let mut c = connect_retry(port).await;
        for i in 0..30 {
            set(&mut c, &format!("fk-{i}"), &format!("fv-{i}")).await;
        }
        assert_eq!(
            cmd(&mut c, &["SAVE", "HANDOFF"]).await,
            b"+OK\r\n",
            "SAVE HANDOFF commits via the data_dir fallback"
        );
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // The handoff wrote to the DURABLE data_dir (the fallback), not the non-tmpfs handoff dir.
    assert!(
        has_manifest(&dir),
        "the fallback wrote a durable manifest to data_dir"
    );
    assert!(
        !has_manifest(&staging(&disk_handoff)),
        "nothing was staged under the non-tmpfs handoff dir"
    );

    // A fresh boot reloads from the durable data_dir.
    {
        let server = run_persist_server_with_handoff_for_test(
            port,
            3,
            dir.clone(),
            Some(disk_handoff.clone()),
        );
        let mut c = connect_retry(port).await;
        for i in 0..30 {
            assert_eq!(
                get_raw(&mut c, &format!("fk-{i}")).await,
                bulk(&format!("fv-{i}")),
                "fk-{i} reloaded from the data_dir fallback"
            );
        }
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&disk_handoff).ok();
    std::fs::remove_dir_all(&dir).ok();
}

/// (3) The DURABLE periodic/manual `SAVE` path is UNCHANGED: a plain `SAVE` writes the `data_dir`
/// snapshot and NEVER the tmpfs handoff dir, even when `upgrade_handoff_dir` points at a usable
/// tmpfs. (Only `SAVE HANDOFF` stages on tmpfs.)
#[tokio::test(flavor = "current_thread")]
async fn plain_save_is_durable_and_never_touches_tmpfs() {
    // Prefer a real tmpfs base to prove even THEN a plain SAVE stays on data_dir; else a disk dir
    // (the assertion holds either way).
    let base = unique_tmpfs_base("plain").unwrap_or_else(|| temp_data_dir("plain-handoff"));
    std::fs::remove_dir_all(&base).ok();
    let dir = temp_data_dir("plain");
    let port = free_port();

    {
        let server =
            run_persist_server_with_handoff_for_test(port, 2, dir.clone(), Some(base.clone()));
        let mut c = connect_retry(port).await;
        for i in 0..20 {
            set(&mut c, &format!("pk-{i}"), &format!("pv-{i}")).await;
        }
        // A PLAIN SAVE (the unchanged durable path).
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n", "plain SAVE commits");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    assert!(
        has_manifest(&dir),
        "plain SAVE wrote the durable data_dir snapshot"
    );
    assert!(
        !has_manifest(&staging(&base)),
        "plain SAVE did NOT stage anything on the tmpfs handoff dir"
    );

    std::fs::remove_dir_all(&base).ok();
    std::fs::remove_dir_all(&dir).ok();
}

/// (4) CRASH-SAFETY: the durable `data_dir` snapshot is the recovery source. After a durable `SAVE`
/// and a later `SAVE HANDOFF` (tmpfs), a simulated mid-upgrade failure that LOSES the ephemeral
/// tmpfs handoff (power loss clears tmpfs) still recovers the last DURABLE snapshot on the next
/// boot -- the handoff never touched `data_dir`. Skips where tmpfs is unavailable.
#[tokio::test(flavor = "current_thread")]
async fn crash_mid_upgrade_recovers_from_the_durable_data_dir() {
    let Some(base) = unique_tmpfs_base("crash") else {
        eprintln!("skipping: tmpfs (/dev/shm) unavailable on this host");
        return;
    };
    std::fs::remove_dir_all(&base).ok();
    let dir = temp_data_dir("crash");
    let stage = staging(&base);
    let port = free_port();

    // -- Boot 1: durable SAVE of the A-keys, then a HANDOFF save (tmpfs) that ALSO captured B-keys. --
    {
        let server =
            run_persist_server_with_handoff_for_test(port, 3, dir.clone(), Some(base.clone()));
        let mut c = connect_retry(port).await;
        for i in 0..15 {
            set(&mut c, &format!("ak-{i}"), &format!("av-{i}")).await;
        }
        // Durable snapshot to data_dir (the recovery floor).
        assert_eq!(cmd1(&mut c, "SAVE").await, b"+OK\r\n");
        // More writes, then an upgrade HANDOFF save to tmpfs (captures A + B, but ephemerally).
        for i in 0..15 {
            set(&mut c, &format!("bk-{i}"), &format!("bv-{i}")).await;
        }
        assert_eq!(cmd(&mut c, &["SAVE", "HANDOFF"]).await, b"+OK\r\n");
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    // Both snapshots exist: the durable data_dir (A) and the tmpfs handoff (A + B).
    assert!(has_manifest(&dir), "the durable data_dir snapshot exists");
    assert!(has_manifest(&stage), "the tmpfs handoff snapshot exists");

    // SIMULATE a mid-upgrade crash with power loss: the ephemeral tmpfs handoff is GONE, but the
    // durable data_dir snapshot is untouched.
    std::fs::remove_dir_all(&base).unwrap();
    assert!(
        has_manifest(&dir),
        "the durable snapshot survived the crash"
    );

    // -- Boot 2: recovery falls back to the DURABLE data_dir snapshot (the A-keys). --
    {
        let server =
            run_persist_server_with_handoff_for_test(port, 3, dir.clone(), Some(base.clone()));
        let mut c = connect_retry(port).await;
        for i in 0..15 {
            assert_eq!(
                get_raw(&mut c, &format!("ak-{i}")).await,
                bulk(&format!("av-{i}")),
                "ak-{i} recovered from the DURABLE data_dir snapshot after the crash"
            );
        }
        // The B-keys lived only in the lost tmpfs handoff -> absent (the accepted ephemeral tradeoff).
        assert_eq!(
            cmd1(&mut c, "DBSIZE").await,
            b":15\r\n",
            "only the durable A-keys recovered"
        );
        drop(c);
        server.shutdown_and_join().unwrap();
    }

    std::fs::remove_dir_all(&base).ok();
    std::fs::remove_dir_all(&dir).ok();
}
