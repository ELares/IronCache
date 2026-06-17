// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-4c acceptance test: the merged, DST-verified Raft control plane GOVERNS a live
//! IronCache cluster over the real serve path, strictly behind `cluster_mode = raft`.
//!
//! It boots THREE real `run_server` nodes in RAFT-GOVERNANCE mode on the loopback (each on its
//! own OS threads via the SO_REUSEPORT thread-per-core topology, each with its own Raft
//! control-plane thread + RAFTMSG listener, the same multi-node-on-loopback shape as
//! `cluster_slice2.rs` and the HA-4a `loopback_cluster.rs` proof). It then drives the cluster
//! OVER REAL CLIENT SOCKETS:
//!
//!   1. FORMATION: a unique leader emerges (discovered over the wire: a CLUSTER write returns
//!      `+OK` on the leader and `-CLUSTERDOWN` on a follower).
//!   2. PROPOSE -> COMMIT -> CONVERGE: the leader MEETs its peers and claims the whole slot
//!      space (`CLUSTER ADDSLOTSRANGE 0 16383`); every committed change converges, so ALL THREE
//!      nodes' `CLUSTER SLOTS` reflect 16384 assigned and `CLUSTER INFO` shows
//!      `cluster_state:ok` + `cluster_slots_assigned:16384`.
//!   3. SERVE + MOVED: a key in a leader-owned slot is SET/GET-served on the leader; after the
//!      leader SETSLOTs a specific slot to a peer (committed), a key in that slot returns
//!      `-MOVED <slot> <peer host:port>`.
//!   4. REDIRECT: proposing a CLUSTER write to a FOLLOWER returns `-CLUSTERDOWN`.
//!
//! This is the PRODUCTION path (not the deterministic DST suite), so it polls with generous
//! real-time timeouts and discovers the leader by behavior rather than asserting timing.

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

/// The shared 3-node raft topology: ids ID0/ID1/ID2 on the given ports, all advertised on
/// 127.0.0.1. In raft-mode the `slots` ranges are IGNORED (ownership is established at runtime
/// through committed proposals), so they are empty; the topology supplies only the voter set +
/// the peer cluster-bus addresses.
fn three_node_topology(ports: [u16; 3]) -> ClusterTopology {
    ClusterTopology {
        nodes: vec![
            ClusterNode {
                id: ID0.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: ports[0],
                slots: vec![],
            },
            ClusterNode {
                id: ID1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: ports[1],
                slots: vec![],
            },
            ClusterNode {
                id: ID2.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: ports[2],
                slots: vec![],
            },
        ],
    }
}

/// Remove any stale per-node FileStorage log (from an earlier run on the same ephemeral bus
/// port) so each node boots with a FRESH Raft log and cannot replay a prior term/vote. The path
/// matches `raft_boot`'s `<temp>/ironcache-raft-<bus-port>.log`.
fn clean_raft_logs(ports: [u16; 3]) {
    for p in ports {
        let bus = bus_port(p);
        let path = std::env::temp_dir().join(format!("ironcache-raft-{bus}.log"));
        let _ = std::fs::remove_file(path);
    }
}

/// Connect with short retries: the shards + the raft control plane bind asynchronously on their
/// own threads after `run_raft_node_for_test` returns.
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("raft node never came up on port {port}");
}

/// Read until quiet (a small reply may arrive in one or two segments).
async fn read_reply(client: &mut TcpStream) -> String {
    let mut acc = Vec::new();
    loop {
        let mut buf = [0u8; 8192];
        let n = tokio::time::timeout(Duration::from_millis(250), client.read(&mut buf))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(0);
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        if n < buf.len() {
            break;
        }
    }
    String::from_utf8_lossy(&acc).into_owned()
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

/// Whether `timeout` has elapsed since `start`, measured through the env clock (ADR-0003: even
/// tests read real time only through the env seam, never `std::time::Instant`). Polling loops
/// call this as their deadline check and `tokio::time::sleep` between tries.
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

// `too_many_lines` + `needless_range_loop` are allowed: this is ONE end-to-end acceptance flow
// (formation, leader discovery, propose/commit/converge, serve, MOVED, redirect) that must read
// in sequence, and its loops index PARALLEL arrays (`clients[i]` by `&mut` alongside `ports[i]`),
// which an iterator cannot express without splitting the mutable borrow.
#[test]
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn raft_mode_forms_assigns_converges_serves_moved_and_redirects() {
    // Generous bound: election base+jitter is 150-300ms and proposals must commit across three
    // real TCP-connected nodes, so several seconds is ample even on a loaded CI machine.
    let timeout = Duration::from_secs(20);

    let ports = [free_port(), free_port(), free_port()];
    clean_raft_logs(ports);
    let topo = three_node_topology(ports);

    // Boot all three raft-mode nodes (each spawns its serve shards + a raft control-plane thread).
    let ids = [ID0, ID1, ID2];
    let _nodes: Vec<ShardSet> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| run_raft_node_for_test(ports[i], topo.clone(), id))
        .collect();

    // One current-thread client runtime drives the whole test (the nodes run on their own OS
    // threads inside run_server / the control plane).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        // Open one client socket to each node.
        let mut clients = Vec::new();
        for p in ports {
            clients.push(connect_retry(p).await);
        }

        let env = SystemEnv::new();

        // ---- (1) FORMATION + leader discovery over the wire. A CLUSTER write returns +OK on the
        // leader and -CLUSTERDOWN on a follower; poll until exactly one node accepts. We probe
        // with a MEET of a peer (a real, idempotent mutator) so discovery also begins forming the
        // node table on the leader.
        let leader_idx = {
            let start = env.now();
            let mut found = None;
            'discover: loop {
                for i in 0..3 {
                    // MEET the "next" peer (idempotent on the leader; rejected on a follower).
                    let peer = (i + 1) % 3;
                    let reply = cmd(
                        &mut clients[i],
                        &["CLUSTER", "MEET", "127.0.0.1", &ports[peer].to_string()],
                    )
                    .await;
                    if reply.starts_with("+OK") {
                        found = Some(i);
                        break 'discover;
                    }
                }
                if deadline_passed(&env, start, timeout) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            found.expect("a unique leader must emerge and accept a CLUSTER write")
        };

        // Sanity: the OTHER two nodes reject a CLUSTER write with -CLUSTERDOWN (the redirect).
        for i in 0..3 {
            if i == leader_idx {
                continue;
            }
            let reply = cmd(
                &mut clients[i],
                &[
                    "CLUSTER",
                    "MEET",
                    "127.0.0.1",
                    &ports[leader_idx].to_string(),
                ],
            )
            .await;
            assert!(
                reply.starts_with("-CLUSTERDOWN"),
                "a follower must redirect a CLUSTER write with -CLUSTERDOWN, got {reply:?}"
            );
        }

        // ---- (2) PROPOSE -> COMMIT -> CONVERGE. The leader MEETs BOTH peers (so they enter the
        // committed node table) and claims the WHOLE slot space for itself. ADDSLOTS only claims
        // for self, so "across the nodes" is the leader-owns-all base + a per-slot SETSLOT below.
        for i in 0..3 {
            if i == leader_idx {
                continue;
            }
            let r = cmd(
                &mut clients[leader_idx],
                &["CLUSTER", "MEET", "127.0.0.1", &ports[i].to_string()],
            )
            .await;
            assert!(r.starts_with("+OK"), "leader MEET should commit, got {r:?}");
        }
        let r = cmd(
            &mut clients[leader_idx],
            &["CLUSTER", "ADDSLOTSRANGE", "0", "16383"],
        )
        .await;
        assert!(
            r.starts_with("+OK"),
            "leader ADDSLOTSRANGE 0 16383 should commit, got {r:?}"
        );

        // Every node converges: CLUSTER INFO shows state:ok + 16384 assigned on ALL THREE.
        let converged = {
            let start = env.now();
            loop {
                let mut all_ok = true;
                for i in 0..3 {
                    let info = cmd(&mut clients[i], &["CLUSTER", "INFO"]).await;
                    if !(info.contains("cluster_state:ok")
                        && info.contains("cluster_slots_assigned:16384"))
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
            "all three nodes must converge to cluster_state:ok + 16384 slots assigned"
        );

        // CLUSTER SLOTS on every node reflects the committed full-space assignment (one range
        // covering 0..16383: the reply contains the boundary integers 0 and 16383).
        for i in 0..3 {
            let slots = cmd(&mut clients[i], &["CLUSTER", "SLOTS"]).await;
            assert!(
                slots.contains(":0\r\n") && slots.contains(":16383\r\n"),
                "node {i} CLUSTER SLOTS should reflect the 0-16383 assignment, got {slots:?}"
            );
        }

        // ---- (3) SERVE: the leader owns every slot, so a SET/GET on it is served locally.
        let owned_key = key_in_range(0, 16383);
        let set = cmd(&mut clients[leader_idx], &["SET", &owned_key, "v"]).await;
        assert!(
            set.starts_with("+OK"),
            "the leader (owns all slots) should serve SET locally, got {set:?}"
        );
        let get = cmd(&mut clients[leader_idx], &["GET", &owned_key]).await;
        assert!(
            get.starts_with("$1\r\nv"),
            "the leader should serve GET locally, got {get:?}"
        );

        // ---- (3b) MOVED: the leader SETSLOTs a single slot to a peer (committed), then a key in
        // that slot returns -MOVED <slot> <peer host:port>. The peer's MEET'd entry carries the
        // peer's advertised endpoint, so MOVED resolves to the right host:port (endpoint-based).
        // Pick a peer + a target slot, and look up the synth id the leader's MEET assigned that
        // peer (host:port-derived, the same derivation the boot uses).
        let peer_idx = (leader_idx + 1) % 3;
        let peer_port = ports[peer_idx];
        let peer_synth_id = synth_meet_node_id("127.0.0.1", peer_port);
        let moved_key = key_in_range(100, 100); // slot 100 is arbitrary but fixed
        let target_slot = key_slot(moved_key.as_bytes());
        let r = cmd(
            &mut clients[leader_idx],
            &[
                "CLUSTER",
                "SETSLOT",
                &target_slot.to_string(),
                "NODE",
                &peer_synth_id,
            ],
        )
        .await;
        assert!(
            r.starts_with("+OK"),
            "leader SETSLOT <slot> NODE <peer> should commit, got {r:?}"
        );

        // Poll until the leader's committed map flips that slot to the peer (apply is async), then
        // a GET of the moved key returns -MOVED <slot> 127.0.0.1:<peer_port>.
        let expect_moved = format!("-MOVED {target_slot} 127.0.0.1:{peer_port}");
        let moved = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[leader_idx], &["GET", &moved_key]).await;
                if reply.starts_with("-MOVED") {
                    break Some(reply);
                }
                if deadline_passed(&env, start, timeout) {
                    break None;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        .expect("a key in the SETSLOT'd slot must MOVED to the peer after the commit applies");
        assert!(
            moved.starts_with(&expect_moved),
            "expected {expect_moved:?}, got {moved:?}"
        );

        // ---- (4) REDIRECT: a CLUSTER write to a FOLLOWER returns -CLUSTERDOWN (already shown in
        // step 1, re-asserted directly here against the chosen peer with a SETSLOT write).
        let follower_reply = cmd(
            &mut clients[peer_idx],
            &["CLUSTER", "SETSLOT", "5", "NODE", ID0],
        )
        .await;
        assert!(
            follower_reply.starts_with("-CLUSTERDOWN"),
            "a follower must redirect a CLUSTER write with -CLUSTERDOWN, got {follower_reply:?}"
        );
    });

    // The ShardSets drop here (each signals its shards to drain); the raft control-plane threads
    // are detached and exit with the process. Clean the logs we created.
    clean_raft_logs(ports);
}

/// The deterministic 40-hex placeholder id a raft-mode `CLUSTER MEET <host> <port>` synthesizes
/// for the MEET'd peer (FNV-1a over `host:port`, hex-padded to 40). MUST match `serve.rs`'s
/// `synth_meet_node_id` so the test can name the MEET'd peer in a SETSLOT.
fn synth_meet_node_id(host: &str, port: u16) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let endpoint = format!("{host}:{port}");
    for b in endpoint.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let hex16 = format!("{h:016x}");
    let mut id = String::with_capacity(40);
    while id.len() < 40 {
        id.push_str(&hex16);
    }
    id.truncate(40);
    id
}
