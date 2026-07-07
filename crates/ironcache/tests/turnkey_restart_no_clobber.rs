// SPDX-License-Identifier: MIT OR Apache-2.0
//! PROD-turnkey REGRESSION acceptance: the turnkey auto-bootstrap driver must NOT re-bootstrap a
//! RESTARTED node and clobber runtime slot ownership (the no-snapshot-recovery race).
//!
//! ## The hazard this exercises (the bug the in-process tests miss)
//!
//! The turnkey driver auto-applies the shipped static `cluster_topology` on a FRESH cluster. Its
//! fresh-only guard originally read the shared `SlotMap` projection (`slots_assigned` /
//! `known_nodes` / `current_epoch`), which a node republishes ONLY when its `ConfigSm` applies /
//! restores. On the COMMON no-snapshot RESTART (the default `raft_snapshot_threshold` is 1024 and a
//! normally-sized cluster never reaches it), the engine recovers raft membership but does NOT replay
//! the committed `Config(ConfigCmd)` tail at construction, so the shared map is transiently PRISTINE
//! (epoch 0, slots 0, known_nodes 1) while `is_leader()` can already be true. The driver therefore
//! sampled the projection as FRESH and re-proposed the STATIC topology above the unapplied recovered
//! tail -- silently reverting every runtime migration / failover once the tail applied.
//!
//! Because that race needs a TRUE process restart (a freshly-constructed engine recovering a
//! persisted log with NO snapshot, with `commit_index`/`last_applied` reset to 0), it cannot be
//! reproduced against the in-process `run_raft_node_for_test` (whose detached raft thread keeps
//! running across a data-shard kill). So this test drives the REAL compiled binary as THREE
//! SUBPROCESSES, kills one with SIGKILL, and relaunches it on the SAME data_dir / port.
//!
//! ## The flow
//!
//!   1. Form a FRESH 3-node turnkey cluster (NO manual MEET/ADDSLOTS) -> cluster_state:ok, 16384
//!      slots, 3 known nodes.
//!   2. Runtime `CLUSTER SETSLOT 0 NODE <node2>` -- migrate slot 0 (in node0's DECLARED block
//!      [0,5460]) to node2, a NON-declared owner, committed through the log.
//!   3. COLD-RESTART the whole cluster: SIGKILL all three, then relaunch all three on the SAME
//!      data_dirs/ports WITHOUT a snapshot (only a handful of committed entries, well under
//!      `raft_snapshot_threshold`). Every node recovers with commit_index/last_applied reset to 0
//!      and a pristine shared map; whichever wins the re-election runs its turnkey driver in the
//!      pristine window -- the surest provocation of the race.
//!   4. ASSERT the runtime migration SURVIVES: slot 0 still routes to node2 from node0 (its DECLARED
//!      owner) AND node1 -- the turnkey driver did NOT re-bootstrap / clobber it back to the static
//!      layout.
//!
//! Pre-fix this FAILS (the re-elected leader runs its driver while its shared map is pristine,
//! samples the projection as fresh, and re-proposes the static split -> slot 0 reverts to node0).
//! Post-fix it PASSES (the driver consults the engine's recovered persisted-log fact: a non-empty
//! log means RESTARTED, so EVERY node stands down and never re-bootstraps).

use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const ID0: &str = "0000000000000000000000000000000000000000";
const ID1: &str = "1111111111111111111111111111111111111111";
const ID2: &str = "2222222222222222222222222222222222222222";

/// How many full-cluster cold-restart cycles to run. The re-bootstrap race fires only when the
/// re-elected leader's turnkey driver polls in the brief post-election, pre-commit window (its shared
/// map still pristine). On loopback that window is a single replication RTT, so ONE cold restart hits
/// it only ~1-in-3 of the time. 12 cycles make the PRE-FIX failure overwhelmingly likely
/// (1 - (2/3)^12 ~= 99%) while the POST-FIX run is always green (the persisted-log gate makes every
/// node stand down on every cycle).
const COLD_RESTART_CYCLES: usize = 12;

/// Grab a free TCP port (bind ephemeral, read it, drop). A brief TOCTOU window before the binary
/// rebinds; fine on loopback for a test.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A throwaway per-node data dir (durable Raft log lives here, so a restart recovers it).
fn node_data_dir(tag: &str, idx: usize) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ic-turnkey-restart-{tag}-{}-n{idx}",
        std::process::id()
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Write a per-node TOML config with the FULL shipped 3-node topology (declared slots), this node's
/// announce id + data_dir. The keyspace save policy is left off (we write no keys), and the default
/// `raft_snapshot_threshold` (1024) is kept so the handful of committed entries NEVER snapshots --
/// the exact no-snapshot recovery path the regression needs.
fn write_node_config(dir: &Path, self_id: &str, ports: [u16; 3]) -> PathBuf {
    let cfg = format!(
        r#"# SPDX-License-Identifier: MIT OR Apache-2.0
bind = "127.0.0.1"
port = {self_port}
shards = 1
cluster_enabled = true
cluster_mode = "raft"
cluster_announce_id = "{self_id}"
data_dir = "{data_dir}"
min_replicas_to_write = 0

[[cluster_topology.nodes]]
id = "{ID0}"
host = "127.0.0.1"
port = {p0}
slots = [[0, 5460]]

[[cluster_topology.nodes]]
id = "{ID1}"
host = "127.0.0.1"
port = {p1}
slots = [[5461, 10922]]

[[cluster_topology.nodes]]
id = "{ID2}"
host = "127.0.0.1"
port = {p2}
slots = [[10923, 16383]]
"#,
        self_port = port_for_id(self_id, ports),
        self_id = self_id,
        data_dir = dir.display(),
        ID0 = ID0,
        ID1 = ID1,
        ID2 = ID2,
        p0 = ports[0],
        p1 = ports[1],
        p2 = ports[2],
    );
    let path = dir.join("ironcache.toml");
    std::fs::write(&path, cfg).unwrap();
    path
}

/// The client port for a node's announce id (index 0/1/2 -> ports[0/1/2]).
fn port_for_id(id: &str, ports: [u16; 3]) -> u16 {
    match id {
        ID0 => ports[0],
        ID1 => ports[1],
        ID2 => ports[2],
        other => panic!("unknown id {other}"),
    }
}

/// Spawn the REAL compiled `ironcache` binary with the given TOML config. The config carries the
/// bind/port/topology, so only `--config` is passed. `IRONCACHE_*` env is cleared so a stray env
/// (or the conventional /etc path) cannot leak in.
fn spawn_node(config_path: &Path) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ironcache"));
    cmd.arg("server")
        .arg("--config")
        .arg(config_path)
        // Metrics endpoint is default-on (127.0.0.1:9091) since #555; several nodes run at once and
        // this test does not exercise it, so disable it to avoid a shared ops-port bind conflict.
        .arg("--metrics-addr")
        .arg("off")
        .env_remove("IRONCACHE_DATA_DIR")
        .env_remove("IRONCACHE_CLUSTER_MODE")
        .env_remove("IRONCACHE_CLUSTER_ENABLED")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn ironcache binary")
}

/// Connect to a subprocess with retries (it binds asynchronously after spawn).
fn connect_blocking(port: u16) -> TcpStream {
    for _ in 0..400 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_nodelay(true);
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            return s;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("subprocess never came up on port {port}");
}

/// Send one command (bulk-string args) and read one reply as a string.
fn send_cmd(s: &mut TcpStream, args: &[&str]) -> String {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    s.write_all(frame.as_bytes()).unwrap();
    let mut buf = [0u8; 4096];
    let n = s.read(&mut buf).unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// The polling cadence between attempts (a coarse fixed sleep; `Duration`/`thread::sleep` are not
/// clock reads, so this stays inside the determinism invariant -- the test bounds waits by an
/// ATTEMPT COUNT, not by reading a wall clock, mirroring `shutdown.rs`).
const POLL_SLEEP: Duration = Duration::from_millis(100);
/// Max attempts for a convergence poll (~40s at `POLL_SLEEP`): generous for real process spawn +
/// election + bootstrap proposals across three TCP nodes.
const CONVERGE_ATTEMPTS: usize = 400;
/// Max attempts for a routing poll (~30s at `POLL_SLEEP`).
const ROUTE_ATTEMPTS: usize = 300;

/// Poll every node's `CLUSTER INFO` until all three report ok + 16384 slots + 3 known nodes, or the
/// attempt budget is exhausted. Reconnects per poll so a node still coming up does not wedge the
/// loop. Bounds the wait by an ATTEMPT COUNT (not a wall-clock deadline) per the determinism
/// invariant (ADR-0003).
fn wait_for_turnkey_convergence(ports: [u16; 3]) -> bool {
    for _ in 0..CONVERGE_ATTEMPTS {
        let mut all_ok = true;
        for p in ports {
            let info = match TcpStream::connect(("127.0.0.1", p)) {
                Ok(mut s) => {
                    let _ = s.set_nodelay(true);
                    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
                    send_cmd(&mut s, &["CLUSTER", "INFO"])
                }
                Err(_) => String::new(),
            };
            if !(info.contains("cluster_state:ok")
                && info.contains("cluster_slots_assigned:16384")
                && info.contains("cluster_known_nodes:3"))
            {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            return true;
        }
        std::thread::sleep(POLL_SLEEP);
    }
    false
}

/// Poll `GET key` on `s` until the reply starts with `expect`, or the attempt budget is exhausted.
/// Returns whether the expected reply was observed. Bounds by ATTEMPT COUNT (determinism invariant).
fn poll_get_until(s: &mut TcpStream, key: &str, expect: &str, attempts: usize) -> bool {
    for _ in 0..attempts {
        if send_cmd(s, &["GET", key]).starts_with(expect) {
            return true;
        }
        std::thread::sleep(POLL_SLEEP);
    }
    false
}

/// Kill a child with SIGKILL and reap it (no graceful drain -- a hard crash, the worst case for the
/// no-snapshot recovery path).
fn kill_hard(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Clean the three node data dirs.
fn clean_dirs(dirs: &[PathBuf]) {
    for d in dirs {
        std::fs::remove_dir_all(d).ok();
    }
}

/// THE REGRESSION TEST. A runtime slot migration (SETSLOT to a non-declared owner) must SURVIVE a
/// no-snapshot restart of a node -- the turnkey driver must NOT re-bootstrap the restarted node and
/// clobber the migration back to the static layout.
//
// `too_many_lines` allowed: ONE end-to-end flow (form -> migrate -> kill -> restart -> assert
// survival), read in sequence over real subprocesses + sockets.
#[test]
#[allow(clippy::too_many_lines)]
fn runtime_slot_migration_survives_a_no_snapshot_restart_no_turnkey_clobber() {
    let ports = [free_port(), free_port(), free_port()];
    let dirs = [
        node_data_dir("dirs", 0),
        node_data_dir("dirs", 1),
        node_data_dir("dirs", 2),
    ];
    let ids = [ID0, ID1, ID2];
    let configs: Vec<PathBuf> = (0..3)
        .map(|i| write_node_config(&dirs[i], ids[i], ports))
        .collect();

    // Boot all three turnkey nodes. NOTHING is issued manually after this.
    let mut children: Vec<Option<Child>> = configs.iter().map(|c| Some(spawn_node(c))).collect();

    // Run the body in a closure so we ALWAYS tear down the children + dirs, even on a panic.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // ---- (1) TURNKEY CONVERGENCE.
        assert!(
            wait_for_turnkey_convergence(ports),
            "TURNKEY: a fresh cluster from the shipped static topology must reach cluster_state:ok + \
             16384 slots + 3 known nodes with NO manual MEET/ADDSLOTS"
        );

        // ---- (2) RUNTIME MIGRATION. Move slot 0 (in node0's DECLARED block [0,5460]) to node2,
        // a NON-declared owner, via a committed runtime SETSLOT issued to any node (forwarded to the
        // leader). It must commit (+OK) and then ROUTE to node2 from a node that does not own it.
        let mut c1 = connect_blocking(ports[1]); // drive from node1 (not the moved-from/-to owner)
        let mut setslot_ok = false;
        for _ in 0..ROUTE_ATTEMPTS {
            if send_cmd(&mut c1, &["CLUSTER", "SETSLOT", "0", "NODE", ID2]).starts_with("+OK") {
                setslot_ok = true;
                break;
            }
            std::thread::sleep(POLL_SLEEP);
        }
        assert!(
            setslot_ok,
            "a runtime CLUSTER SETSLOT 0 NODE <node2> must commit through the log post-turnkey"
        );

        // A key in slot 0, requested on node1, must MOVED to NODE2 (the migrated owner), not node0
        // (the declared owner). We just need ANY key in slot 0; brute-force it.
        let key0 = key_in_slot(0);
        let expect_node2 = format!("-MOVED 0 127.0.0.1:{}", ports[2]);
        assert!(
            poll_get_until(&mut c1, &key0, &expect_node2, ROUTE_ATTEMPTS),
            "after the runtime migration, slot 0 must route to node2 (expected {expect_node2:?})"
        );
        drop(c1);

        // ---- (3) + (4) REPEATED FULL-CLUSTER COLD RESTART + NO-CLOBBER ASSERTION.
        //
        // The re-bootstrap race is timing-dependent: it fires only when the re-elected leader's
        // turnkey driver polls in the brief window AFTER it wins leadership (status published as
        // leader) but BEFORE its election no-op commits and `apply_committed` replays the recovered
        // tail (so its shared map is still PRISTINE). On loopback that window is a single short
        // replication RTT, so ONE cold restart hits it only sometimes. We therefore run SEVERAL
        // cold-restart cycles: pre-fix, at least one cycle lands in the window and reverts slot 0 to
        // its declared owner (node0) -> the assertion FAILS; post-fix the persisted-log gate makes
        // EVERY node stand down on EVERY cycle, so slot 0 stays at node2 across all of them.
        for cycle in 0..COLD_RESTART_CYCLES {
            // SIGKILL all three (a hard crash), then relaunch all three on the SAME data_dirs / ports
            // WITHOUT a snapshot. The committed log (3 AddNode + 3 AssignSlots + 1 SETSLOT = 7
            // entries) is far under the default raft_snapshot_threshold (1024), so EVERY node takes
            // the NO-SNAPSHOT recovery path: a freshly-constructed engine with commit_index /
            // last_applied reset to 0 and a PRISTINE shared map (recompute_config_from_log recovers
            // raft membership, NOT the ConfigSm slots/epoch/nodes). Whichever node wins the
            // re-election runs its turnkey driver in that pristine window -- the exact catastrophic
            // scenario the bug describes.
            for child in &mut children {
                if let Some(c) = child.take() {
                    kill_hard(c);
                }
            }
            // A few beats so the OS releases the listener + bus sockets before the restarts rebind.
            std::thread::sleep(Duration::from_millis(700));
            for (i, child) in children.iter_mut().enumerate() {
                *child = Some(spawn_node(&configs[i]));
            }

            // The cold-restarted cluster must converge again to ok/16384/3.
            assert!(
                wait_for_turnkey_convergence(ports),
                "cycle {cycle}: the cold-restarted cluster must re-converge to ok + 16384 slots + 3 \
                 known nodes"
            );

            // NO-CLOBBER: the runtime migration of slot 0 to node2 must SURVIVE the cold restart,
            // observed from node0 (slot 0's DECLARED owner) AND node1. If the turnkey driver
            // re-bootstrapped, slot 0 reverts to node0 (route / serve there) -- the data-loss
            // regression. The recovered tail applies a moment after boot, so poll for the expected
            // route; if it never appears within the bound the migration was clobbered.

            // (a) From node0 (slot 0's DECLARED owner): GET of a slot-0 key must MOVED to node2, NOT
            // serve locally (a local serve = node0 re-claimed slot 0 = clobbered back to static).
            let mut c0 = connect_blocking(ports[0]);
            assert!(
                poll_get_until(&mut c0, &key0, &expect_node2, ROUTE_ATTEMPTS),
                "NO-CLOBBER (cycle {cycle}): from node0 (slot 0's declared owner), slot 0 must STILL \
                 route to node2 (the runtime migration), expected {expect_node2:?}; a revert to the \
                 declared owner (node0) is the re-bootstrap data-loss regression"
            );

            // (b) From node1: same -- confirming the cluster view (not just node0's) was not clobbered.
            let mut c1 = connect_blocking(ports[1]);
            assert!(
                poll_get_until(&mut c1, &key0, &expect_node2, ROUTE_ATTEMPTS),
                "NO-CLOBBER (cycle {cycle}): from node1, slot 0 must STILL route to node2 after the \
                 restart, expected {expect_node2:?}"
            );

            // (c) STABILITY: re-check after a short quiet window (a few poll sleeps) so a delayed
            // re-bootstrap (the driver polls every 200ms) cannot clobber it later in this cycle.
            for _ in 0..10 {
                std::thread::sleep(POLL_SLEEP);
            }
            let still = send_cmd(&mut c0, &["GET", &key0]);
            assert!(
                still.starts_with(&expect_node2),
                "NO-CLOBBER (cycle {cycle}, stability): slot 0 must remain at node2 after a quiet \
                 window, got {still:?}"
            );
        }
    }));

    // Tear down every surviving child + the data dirs, then resurface a panic.
    for child in children.into_iter().flatten() {
        kill_hard(child);
    }
    clean_dirs(&dirs);

    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

/// Brute-force a short key whose CRC16-derived slot equals `target`. Mirrors the cluster key-slot
/// hashing the server uses, computed here so the test stays free of the protocol crate's exact API
/// surface (it only needs SOME key that lands in the target slot).
fn key_in_slot(target: u16) -> String {
    for i in 0..1_000_000u32 {
        let k = format!("k{i}");
        if ironcache_protocol::key_slot(k.as_bytes()) == target {
            return k;
        }
    }
    panic!("no key found whose slot is {target}");
}
