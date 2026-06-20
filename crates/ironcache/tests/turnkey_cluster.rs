// SPDX-License-Identifier: MIT OR Apache-2.0
//! PROD-turnkey acceptance test: a FRESH raft cluster booted from the SHIPPED static
//! `cluster_topology` (each node declaring its `slots`) reaches `cluster_state:ok` with all 16384
//! slots assigned and `cluster_known_nodes:3`, over the real serve path, WITHOUT any operator
//! `CLUSTER MEET` / `CLUSTER ADDSLOTS`.
//!
//! This is the coverage gap that hid the non-turnkey behavior: the earlier `raft_cluster.rs`
//! acceptance flow declares EMPTY `slots` and DRIVES formation by manually issuing MEET + ADDSLOTS
//! over the wire, so it never exercised a from-the-shipped-topology turnkey boot. Here we declare the
//! real slot split (node0 [0,5460], node1 [5461,10922], node2 [10923,16383], exactly the shipped
//! `deploy/compose/config/nodeN.toml` layout) and assert the cluster forms + serves with NO manual
//! bootstrap.
//!
//! It also proves the IDEMPOTENT / NO-CLOBBER property two ways:
//!   * STABILITY: after formation, the committed assignment is STABLE over several seconds (the
//!     bootstrap driver stood down -- it does not re-propose and churn the committed config); and
//!   * NO-CLOBBER OF A RUNTIME CHANGE: a runtime `CLUSTER SETSLOT <slot> NODE <peer>` (committed
//!     through the SAME log) STICKS -- the turnkey driver, having already seen a non-fresh committed
//!     config, never reverts it back to the declared owner.
//!
//! This is the PRODUCTION path (not the deterministic DST suite), so it polls with generous
//! real-time timeouts and discovers behavior over real sockets rather than asserting timing.

use ironcache::raft_boot::bus_port;
use ironcache::test_support::run_raft_node_for_test;
use ironcache_config::{ClusterNode, ClusterTopology};
use ironcache_env::{Clock, SystemEnv};
use ironcache_protocol::key_slot;
use ironcache_runtime::bootstrap::ShardSet;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary (so the
// process-memory path used by INFO is live; harmless for these tests otherwise).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ID0: &str = "0000000000000000000000000000000000000000";
const ID1: &str = "1111111111111111111111111111111111111111";
const ID2: &str = "2222222222222222222222222222222222222222";

/// Grab a free TCP port (bind ephemeral, read it, drop). A brief TOCTOU window before the node
/// rebinds; fine on loopback for a test.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// The shipped 3-node split with DECLARED slots (the turnkey input): node0 [0,5460],
/// node1 [5461,10922], node2 [10923,16383] -- exactly the `deploy/compose` layout, covering all
/// 16384 slots. Unlike `raft_cluster.rs`'s empty-`slots` topology, these declared slots are what the
/// turnkey driver auto-applies on a fresh cluster.
fn shipped_topology(ports: [u16; 3]) -> ClusterTopology {
    ClusterTopology {
        nodes: vec![
            ClusterNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: ports[0],
                slots: vec![[0, 5460]],
            },
            ClusterNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: ports[1],
                slots: vec![[5461, 10_922]],
            },
            ClusterNode {
                id: ID2.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: ports[2],
                slots: vec![[10_923, 16_383]],
            },
        ],
    }
}

/// Remove any stale per-node FileStorage log (from an earlier run on the same ephemeral bus port) so
/// each node boots with a FRESH Raft log and cannot replay a prior committed config. The path matches
/// `raft_boot`'s `<temp>/ironcache-raft-<bus-port>.log` (plus its `.cfg` baseline sidecar).
fn clean_raft_logs(ports: [u16; 3]) {
    for p in ports {
        let bus = bus_port(p);
        let log = std::env::temp_dir().join(format!("ironcache-raft-{bus}.log"));
        let cfg = std::env::temp_dir().join(format!("ironcache-raft-{bus}.log.cfg"));
        let snap = std::env::temp_dir().join(format!("ironcache-raft-{bus}.log.snap"));
        let _ = std::fs::remove_file(log);
        let _ = std::fs::remove_file(cfg);
        let _ = std::fs::remove_file(snap);
    }
}

/// Connect with short retries: the shards + the raft control plane bind asynchronously on their own
/// threads after `run_raft_node_for_test` returns.
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("turnkey raft node never came up on port {port}");
}

/// Read until ONE complete RESP reply is buffered (handles split/slow reads under load, never
/// returning a partial that would desync the next command). Mirrors `raft_cluster.rs`'s reader.
async fn read_reply(client: &mut TcpStream) -> String {
    let mut acc = Vec::new();
    for _ in 0..120 {
        if let Some(len) = resp_reply_len(&acc, 0) {
            return String::from_utf8_lossy(&acc[..len]).into_owned();
        }
        let mut buf = [0u8; 8192];
        match tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => break, // EOF or read error: stop
            Ok(Ok(n)) => acc.extend_from_slice(&buf[..n]),
            Err(_) => {} // read timeout: a reply may still be in flight -> keep waiting
        }
    }
    String::from_utf8_lossy(&acc).into_owned()
}

/// Byte length of the FIRST complete RESP reply at `buf[start..]`, or `None` if incomplete. Handles
/// RESP2+RESP3 framing so a read returns EXACTLY one reply. (Same parser as `raft_cluster.rs`.)
fn resp_reply_len(buf: &[u8], start: usize) -> Option<usize> {
    if start >= buf.len() {
        return None;
    }
    let kind = buf[start];
    let mut i = start + 1;
    let crlf = loop {
        if i + 1 >= buf.len() {
            return None;
        }
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            break i;
        }
        i += 1;
    };
    let header = &buf[start + 1..crlf];
    let after = crlf + 2;
    match kind {
        b'$' | b'=' | b'!' => {
            let n: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if n < 0 {
                return Some(after);
            }
            let end = after + n as usize + 2;
            if end <= buf.len() { Some(end) } else { None }
        }
        b'*' | b'~' | b'>' | b'%' => {
            let mut n: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if n < 0 {
                return Some(after);
            }
            if kind == b'%' {
                n = n.checked_mul(2)?;
            }
            let mut p = after;
            for _ in 0..n {
                p = resp_reply_len(buf, p)?;
            }
            Some(p)
        }
        _ => Some(after),
    }
}

/// Send a RESP array command (each arg a bulk string) and read the reply as a string.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    client.write_all(frame.as_bytes()).await.unwrap();
    read_reply(client).await
}

/// Whether `timeout` has elapsed since `start`, measured through the env clock (ADR-0003).
fn deadline_passed(env: &SystemEnv, start: ironcache_env::Monotonic, timeout: Duration) -> bool {
    env.now().saturating_duration_since(start) >= timeout
}

/// Brute-force a short key whose `key_slot` falls in `[lo, hi]`.
fn key_in_range(lo: u16, hi: u16) -> String {
    for i in 0..200_000u32 {
        let k = format!("k{i}");
        let s = key_slot(k.as_bytes());
        if s >= lo && s <= hi {
            return k;
        }
    }
    panic!("no key found whose slot is in [{lo}, {hi}]");
}

// `too_many_lines` allowed: ONE end-to-end turnkey acceptance flow (fresh boot -> auto-converge ->
// serve -> stability -> no-clobber), read in sequence and indexing parallel `clients[i]`/`ports[i]`.
#[test]
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn turnkey_fresh_cluster_auto_forms_ok_full_slots_without_manual_meet_or_addslots() {
    // Generous bound: election base+jitter (150-300ms) + the leader's bootstrap proposals committing
    // across three real TCP-connected nodes, ample even on a loaded CI machine.
    let timeout = Duration::from_secs(25);

    let ports = [free_port(), free_port(), free_port()];
    clean_raft_logs(ports);
    let topo = shipped_topology(ports);

    // Boot all three raft-mode nodes from the SHIPPED static topology (declared slots). NOTHING is
    // issued manually after this: no MEET, no ADDSLOTS. The turnkey driver on each node's shard 0
    // auto-applies the declared assignment once a leader emerges.
    let ids = [ID0, ID1, ID2];
    let _nodes: Vec<ShardSet> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| run_raft_node_for_test(ports[i], topo.clone(), id))
        .collect();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut clients = Vec::new();
        for p in ports {
            clients.push(connect_retry(p).await);
        }
        let env = SystemEnv::new();

        // ---- (1) TURNKEY CONVERGENCE. With NO manual MEET/ADDSLOTS, every node must converge to
        // cluster_state:ok + cluster_slots_assigned:16384 + cluster_known_nodes:3, purely from the
        // declared static topology the leader auto-applied through the Raft log.
        let converged = {
            let start = env.now();
            loop {
                let mut all_ok = true;
                for i in 0..3 {
                    let info = cmd(&mut clients[i], &["CLUSTER", "INFO"]).await;
                    if !(info.contains("cluster_state:ok")
                        && info.contains("cluster_slots_assigned:16384")
                        && info.contains("cluster_known_nodes:3"))
                    {
                        all_ok = false;
                        break;
                    }
                }
                if all_ok {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(
            converged,
            "TURNKEY: a fresh cluster from the shipped static topology must reach cluster_state:ok + \
             16384 slots assigned + 3 known nodes on EVERY node with NO manual CLUSTER MEET/ADDSLOTS"
        );

        // ---- (1b) The declared split is the COMMITTED ownership: each node's declared slot block
        // routes to that node. A key in [0,5460] -> ID0, in [5461,10922] -> ID1, in [10923,16383] ->
        // ID2. We assert via routing: from ID0, a key in ID1's block must MOVED to ID1's port (and a
        // key in ID0's own block is served locally). This proves the auto-applied owners match the
        // DECLARED topology, not some arbitrary single-owner.
        let key_node1 = key_in_range(5461, 10_922);
        let expect_moved_to_1 = format!("-MOVED {} 127.0.0.1:{}", key_slot(key_node1.as_bytes()), ports[1]);
        let routed = {
            let start = env.now();
            loop {
                let r = cmd(&mut clients[0], &["GET", &key_node1]).await;
                if r.starts_with(&expect_moved_to_1) {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(
            routed,
            "a key in node1's DECLARED slot block must MOVED to node1 (the auto-applied owners match \
             the shipped topology), expected {expect_moved_to_1:?}"
        );

        // ---- (2) SERVE: a key in ID0's own declared block is served locally on ID0 (it owns it).
        let key_node0 = key_in_range(0, 5460);
        let set = cmd(&mut clients[0], &["SET", &key_node0, "v"]).await;
        assert!(
            set.starts_with("+OK"),
            "node0 owns its declared block, so it serves SET locally, got {set:?}"
        );
        let get = cmd(&mut clients[0], &["GET", &key_node0]).await;
        assert!(
            get.starts_with("$1\r\nv"),
            "node0 serves GET of its owned key locally, got {get:?}"
        );

        // ---- (3) STABILITY / NO RE-BOOTSTRAP CHURN. The committed config epoch must STOP advancing
        // once the bootstrap is done: read cluster_current_epoch on node0, wait several seconds, read
        // it again -- it must be IDENTICAL (the driver stood down; it is not re-proposing the
        // bootstrap on a loop, which would churn the epoch). A re-bootstrap-on-every-tick bug would
        // make the epoch climb here.
        let epoch_before = current_epoch(&mut clients[0]).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let epoch_after = current_epoch(&mut clients[0]).await;
        assert_eq!(
            epoch_before, epoch_after,
            "the committed config epoch must be STABLE after turnkey formation (the bootstrap driver \
             must NOT re-propose / churn the committed config)"
        );

        // ---- (4) NO-CLOBBER OF A RUNTIME CHANGE. Move a single slot (in ID0's declared block) to
        // node1 via a runtime committed SETSLOT, then confirm it STICKS and the turnkey driver does
        // NOT revert it to the declared owner (the driver saw a non-fresh committed config and stood
        // down permanently). The SETSLOT is issued to node0 and forwarded to / committed by the
        // leader (HA-9), so it succeeds from any node.
        let move_key = key_in_range(0, 0); // slot 0 is in node0's declared block
        let move_slot = key_slot(move_key.as_bytes());
        let start = env.now();
        let setslot_ok = loop {
            let r = cmd(
                &mut clients[0],
                &["CLUSTER", "SETSLOT", &move_slot.to_string(), "NODE", ID1],
            )
            .await;
            if r.starts_with("+OK") {
                break true;
            }
            if deadline_passed(&env, start, timeout) {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(
            setslot_ok,
            "a runtime CLUSTER SETSLOT (committed through the log) must succeed post-turnkey"
        );

        // The moved slot now routes to node1, and -- crucially -- STAYS there: poll for the MOVED,
        // then re-check after a few seconds to prove the turnkey driver never reverts the runtime
        // change back to the DECLARED owner (node0).
        let expect_moved = format!("-MOVED {move_slot} 127.0.0.1:{}", ports[1]);
        let moved = {
            let start = env.now();
            loop {
                let r = cmd(&mut clients[0], &["GET", &move_key]).await;
                if r.starts_with("-MOVED") {
                    break r;
                }
                if deadline_passed(&env, start, timeout) {
                    break String::new();
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(
            moved.starts_with(&expect_moved),
            "the runtime-moved slot must route to node1, expected {expect_moved:?}, got {moved:?}"
        );
        // Stays moved (no clobber): after a quiet window the runtime change is still in effect.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let still_moved = cmd(&mut clients[0], &["GET", &move_key]).await;
        assert!(
            still_moved.starts_with(&expect_moved),
            "NO-CLOBBER: the runtime SETSLOT must persist; the turnkey driver must NEVER revert it to \
             the declared owner. expected {expect_moved:?}, got {still_moved:?}"
        );
    });

    // The ShardSets drop here (each signals its shards to drain); the raft control-plane threads are
    // detached and exit with the process. Clean the logs we created.
    clean_raft_logs(ports);
}

/// Read `cluster_current_epoch:<n>` from CLUSTER INFO on `client`. Panics if the field is absent
/// (it is always present once cluster mode is enabled).
async fn current_epoch(client: &mut TcpStream) -> u64 {
    let info = cmd(client, &["CLUSTER", "INFO"]).await;
    for line in info.split("\r\n") {
        if let Some(v) = line.strip_prefix("cluster_current_epoch:") {
            return v.trim().parse().expect("epoch is an integer");
        }
    }
    panic!("CLUSTER INFO had no cluster_current_epoch line: {info:?}");
}
