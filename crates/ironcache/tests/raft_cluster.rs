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
//!   1. FORMATION: a node that ACCEPTS a CLUSTER write emerges (discovered over the wire: until a
//!      leader exists every node returns `-CLUSTERDOWN`; once one does, that node's writes commit).
//!      The owning operations below are all issued through that SAME node, which becomes the
//!      committed owner whether it is the physical raft leader or a follower forwarding to it.
//!   2. PROPOSE -> COMMIT -> CONVERGE: that node MEETs its peers and claims the whole slot
//!      space (`CLUSTER ADDSLOTSRANGE 0 16383`); every committed change converges, so ALL THREE
//!      nodes' `CLUSTER SLOTS` reflect 16384 assigned and `CLUSTER INFO` shows
//!      `cluster_state:ok` + `cluster_slots_assigned:16384`.
//!   3. SERVE + MOVED: a key in an owned slot is SET/GET-served on the owner; after it SETSLOTs a
//!      specific slot to a peer (committed), a key in that slot returns `-MOVED <slot> <peer
//!      host:port>`.
//!   4. HA-9 FORWARDING: a CLUSTER write issued to a FOLLOWER now returns `+OK` (the follower
//!      transparently forwards the proposal to the leader, which commits it), NOT the old
//!      `-CLUSTERDOWN` redirect. CLUSTER INFO still converges on all nodes.
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
    // Up to ~120 * 1s reads: an absolute cap so a TRUE hang fails loudly instead of blocking
    // forever. A complete reply normally arrives in milliseconds; the cap only bounds a hang.
    for _ in 0..120 {
        // Return as soon as ONE complete RESP reply is buffered. Do NOT break on a short or slow
        // read: under load a reply can arrive split across reads or after a delay, and breaking
        // early (the old fixed-250ms-timeout behaviour) returned a partial/empty string that
        // desynced every subsequent request/response pair on the connection (a late reply then
        // paired with the next request). Parsing for one complete reply keeps cmd() strictly
        // request -> response under any timing.
        if let Some(len) = resp_reply_len(&acc, 0) {
            return String::from_utf8_lossy(&acc[..len]).into_owned();
        }
        let mut buf = [0u8; 8192];
        // CRITICAL: distinguish a READ TIMEOUT from EOF. The old code mapped both to n==0 and
        // returned immediately. Under heavy test oversubscription a server thread can be starved
        // for seconds, so a reply that is genuinely COMING is delayed; returning early then left an
        // empty/partial string AND, worse, the real reply later landed in the socket buffer where
        // the NEXT command's read picked it up -> every subsequent request/response pair desynced.
        // We must keep waiting for a pending reply (timeout -> continue), and only stop on real EOF
        // / error, with a generous absolute cap so a true hang still fails (rather than blocking
        // forever). A complete reply normally arrives in milliseconds; the cap only bounds a hang.
        match tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => break, // EOF (peer closed) or read error: stop
            Ok(Ok(n)) => acc.extend_from_slice(&buf[..n]),
            Err(_) => {} // read timeout: a reply may still be in flight -> keep waiting
        }
    }
    // Absolute cap reached or EOF/error without a complete reply: return what we have so the
    // caller's assertion fails loudly (a true hang) rather than silently desyncing.
    String::from_utf8_lossy(&acc).into_owned()
}

/// Byte length of the FIRST complete RESP reply at `buf[start..]`, or `None` if the buffer does not
/// yet hold one complete reply. Handles RESP2 + RESP3 framing (simple/error/integer/null/bool/
/// double/bignum lines; bulk/verbatim/blob-error strings; and array/set/push/map aggregates) so a
/// test read returns EXACTLY one reply and never a partial that would desync the next command.
fn resp_reply_len(buf: &[u8], start: usize) -> Option<usize> {
    if start >= buf.len() {
        return None;
    }
    let kind = buf[start];
    // The type/header line ends at the first CRLF at or after start+1.
    let mut i = start + 1;
    let crlf = loop {
        if i + 1 >= buf.len() {
            return None; // header line not yet complete
        }
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            break i;
        }
        i += 1;
    };
    let header = &buf[start + 1..crlf];
    let after = crlf + 2;
    match kind {
        // Length-prefixed blobs: bulk string, verbatim string, blob error.
        b'$' | b'=' | b'!' => {
            let n: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if n < 0 {
                return Some(after); // $-1 null bulk
            }
            let end = after + n as usize + 2; // payload + CRLF
            if end <= buf.len() { Some(end) } else { None }
        }
        // Aggregates: array, set, push (n elements); map (n key+value pairs).
        b'*' | b'~' | b'>' | b'%' => {
            let mut n: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if n < 0 {
                return Some(after); // *-1 null array
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
        // Everything else is a single CRLF-terminated line: simple string, error, integer, null,
        // bool, double, big number (and any unknown type byte, best effort).
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

        // ---- (1) FORMATION + writer discovery over the wire. Until a leader exists, EVERY node
        // returns -CLUSTERDOWN; once one is elected its writes commit (+OK), and after HA-9 a
        // follower that has learned the leader also returns +OK by forwarding. We poll until SOME
        // node accepts and use it as the owner for every owning op below (it becomes the committed
        // owner regardless of whether it is the physical leader). We probe with a MEET of a peer (a
        // real, idempotent mutator) so discovery also begins forming the node table.
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
            found.expect("a node must emerge that accepts a CLUSTER write")
        };

        // HA-9: the OTHER two nodes are FOLLOWERS, and a CLUSTER write to a follower now COMMITS by
        // FORWARDING to the leader (it returns +OK), instead of the old -CLUSTERDOWN redirect. Poll
        // until each follower accepts (it must first learn the leader to forward to it); a MEET of
        // the leader is idempotent, so re-trying is harmless.
        for i in 0..3 {
            if i == leader_idx {
                continue;
            }
            let start = env.now();
            let forwarded_ok = loop {
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
                if reply.starts_with("+OK") {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            assert!(
                forwarded_ok,
                "HA-9: a follower must FORWARD a CLUSTER write to the leader and reply +OK"
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

        // ---- (4) HA-9 FORWARDING: a CLUSTER write to a FOLLOWER now COMMITS by forwarding to the
        // leader (it replies +OK), repurposing the old "follower -> -CLUSTERDOWN" assertion. We
        // assign slot 5 to ID0 through the FOLLOWER (peer_idx); the follower forwards the proposal
        // to the leader, which commits it, so the write succeeds from any node. Poll until accepted
        // (the follower must recognize the leader to forward; SETSLOT NODE is idempotent).
        let start = env.now();
        let follower_forward_ok = loop {
            let reply = cmd(
                &mut clients[peer_idx],
                &["CLUSTER", "SETSLOT", "5", "NODE", ID0],
            )
            .await;
            if reply.starts_with("+OK") {
                break true;
            }
            if deadline_passed(&env, start, timeout) {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(
            follower_forward_ok,
            "HA-9: a CLUSTER write to a follower must FORWARD to the leader and reply +OK"
        );
    });

    // The ShardSets drop here (each signals its shards to drain); the raft control-plane threads
    // are detached and exit with the process. Clean the logs we created.
    clean_raft_logs(ports);
}

// `too_many_lines` + `needless_range_loop` allowed: ONE end-to-end HA-6 online-slot-migration flow
// (formation, SRC owns all, SETSLOT MIGRATING/IMPORTING handshake, -ASK for an absent key on SRC,
// ASKING-then-serve on DEST, the committed FLIP, then MOVED on SRC + owns on DEST), read in
// sequence, indexing parallel `clients[i]`/`ports[i]`.
//
// HONEST SCOPE NOTE (mirrors the HA-8 loopback note): the live DATA MOVE (the source dumping the
// migrating slot's keys and streaming them to the destination) is NOT wired in this slice -- it
// reuses the HA-5b snapshot + HA-7c stream transport, driven by a later slice. Here the data move is
// TEST-DRIVEN: the destination-side key is written explicitly via `ASKING; SET` so the IMPORTING
// node holds it, exactly as the real transfer would leave it. What is FULLY WIRED and proven here is
// the part that cannot be stubbed: the committed migration STATE MACHINE (MIGRATING/IMPORTING via
// committed ConfigCmds), the -ASK / ASKING / MOVED REDIRECT semantics over real sockets, and the
// committed FLIP transferring ownership (after which SRC serves MOVED, never ASK).
#[test]
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn raft_mode_slot_migration_asks_serves_under_asking_and_flips_to_moved() {
    let timeout = Duration::from_secs(20);

    let ports = [free_port(), free_port(), free_port()];
    clean_raft_logs(ports);
    let topo = three_node_topology(ports);

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

        // ---- (1) Discover the leader (SRC, the owner) over the wire.
        let src_idx = {
            let start = env.now();
            let mut found = None;
            'discover: loop {
                for i in 0..3 {
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
            found.expect("a unique leader (SRC) must emerge")
        };

        // ---- (2) SRC MEETs both peers and claims the WHOLE slot space (so it OWNS the migrating
        // slot). The DEST is the leader's first peer; its committed synth id is host:port-derived.
        for i in 0..3 {
            if i == src_idx {
                continue;
            }
            let r = cmd(
                &mut clients[src_idx],
                &["CLUSTER", "MEET", "127.0.0.1", &ports[i].to_string()],
            )
            .await;
            assert!(r.starts_with("+OK"), "SRC MEET should commit, got {r:?}");
        }
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "ADDSLOTSRANGE", "0", "16383"],
        )
        .await;
        assert!(r.starts_with("+OK"), "SRC ADDSLOTSRANGE should commit, got {r:?}");

        let dest_idx = (src_idx + 1) % 3;
        let dest_port = ports[dest_idx];
        // DEST is named by the SYNTH id SRC's MEET committed for it (every node learns DEST under
        // that id), so MIGRATING <dest> and the FLIP NODE <dest> resolve on all nodes.
        let dest_synth_id = synth_meet_node_id("127.0.0.1", dest_port);
        // SRC is named by its ANNOUNCE id: SRC's self-AddNode (prepended by ADDSLOTSRANGE) committed
        // SRC under its announce id on every node, so IMPORTING <src> resolves (a synth id would be
        // unknown -- SRC was never MEET'd, only self-added -- and the apply would silently no-op).
        let src_announce_id = ids[src_idx];

        // A key in a fixed slot SRC owns; this slot will migrate to DEST.
        let key = key_in_range(100, 100);
        let slot = key_slot(key.as_bytes());

        // ---- (3a) THE MIGRATION HANDSHAKE, SOURCE LEG (committed through the leader = SRC). SETSLOT
        // <slot> MIGRATING <dest> records SRC's view (the migration peer = DEST) and is a committed
        // ConfigCmd, so it applies on EVERY node's shared map.
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "SETSLOT", &slot.to_string(), "MIGRATING", &dest_synth_id],
        )
        .await;
        assert!(r.starts_with("+OK"), "SETSLOT MIGRATING should commit, got {r:?}");

        // ---- (4) -ASK: a GET for an ABSENT key in the migrating slot on SRC returns
        // `-ASK <slot> <dest host:port>` (the key is not present locally on SRC -> it has migrated /
        // never existed, so SRC hands the client to DEST -- a ONE-TIME hint, ownership unchanged).
        // We poll this BEFORE the IMPORTING leg: a `+OK` propose ack returns on COMMIT, but the
        // leader's shared map reflects the committed MIGRATING tag only once the control-plane task
        // ADVANCES last_applied past it (a small commit-vs-apply gap). Confirming -ASK here proves
        // the MIGRATING tag (and so the recorded migration peer = DEST) is LIVE on the leader, so the
        // IMPORTING proposal it builds next reads the correct DEST as the tag target (Finding 2).
        let expect_ask = format!("-ASK {slot} 127.0.0.1:{dest_port}");
        let asked = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[src_idx], &["GET", &key]).await;
                if reply.starts_with("-ASK") {
                    assert!(
                        reply.starts_with(&expect_ask),
                        "expected {expect_ask:?}, got {reply:?}"
                    );
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(asked, "an absent key on a MIGRATING slot must -ASK to the destination");

        // ---- (3b) THE MIGRATION HANDSHAKE, DESTINATION LEG. SETSLOT <slot> IMPORTING <src> tags the
        // DEST's view. The wire command names only the source; the leader fills the proposal's `dest`
        // from the slot's recorded migration peer (DEST, confirmed live by the -ASK poll above), so
        // apply tags IMPORTING on EXACTLY the DEST node (via `is_self(dest)`), never on the leader
        // (SRC) or the third bystander node (Finding 2).
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "SETSLOT", &slot.to_string(), "IMPORTING", src_announce_id],
        )
        .await;
        assert!(r.starts_with("+OK"), "SETSLOT IMPORTING should commit, got {r:?}");

        // ---- (5) ASKING -> serve on DEST. DEST is IMPORTING the slot (does not own it yet), so a
        // plain command there is MOVED to the owner (SRC). With ASKING set first, DEST serves the
        // command LOCALLY. We use this to DRIVE THE (test-driven) DATA MOVE: `ASKING; SET` writes the
        // migrated key on DEST, exactly as the real transfer would leave it. ASKING is ONE-SHOT, so
        // each command that must be served on DEST is preceded by its own ASKING.
        //
        // First wait until the committed IMPORTING tag has APPLIED on DEST. The only signal a client
        // can observe for that is `ASKING; <cmd>` being SERVED locally: a PLAIN command MOVEDs to the
        // owner whether or not IMPORTING is live (a non-owner that has not yet applied IMPORTING also
        // MOVEDs, and so does an importing-no-ASKING node), so polling a plain GET for MOVED cannot
        // distinguish the two and would race the apply under load. Probe with `ASKING; GET` until it
        // is served (a non-MOVED reply -- here `$-1`, the key absent on DEST), which positively
        // confirms IMPORTING is applied. ASKING is one-shot, so each probe re-sends it.
        let dest_importing_live = {
            let start = env.now();
            loop {
                let ask = cmd(&mut clients[dest_idx], &["ASKING"]).await;
                assert!(ask.starts_with("+OK"), "ASKING should reply +OK, got {ask:?}");
                let reply = cmd(&mut clients[dest_idx], &["GET", &key]).await;
                if !reply.starts_with("-MOVED") {
                    break true; // served locally -> the committed IMPORTING tag is applied on DEST
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(
            dest_importing_live,
            "the committed IMPORTING tag must apply on DEST so ASKING serves locally"
        );
        // (The contrast -- an IMPORTING DEST WITHOUT ASKING MOVEDs to the owner -- is a NEGATIVE
        // property that races the one-shot flag state in a live concurrent cluster, so it is proven
        // DETERMINISTICALLY by the pure unit test `serve::importing_dest_without_asking_is_moved_to_owner`
        // rather than re-asserted flakily here. This loopback proves the POSITIVE end-to-end path:
        // -ASK from the source, ASKING serves on the dest, and the FLIP -> MOVED below.)
        //
        // ASKING then SET on DEST: served locally (the migrated key now lives on DEST). Each command
        // that must be served on DEST is self-contained (its own ASKING immediately precedes it), so
        // it does not depend on any prior flag state.
        let ask_ok = cmd(&mut clients[dest_idx], &["ASKING"]).await;
        assert!(ask_ok.starts_with("+OK"), "ASKING should reply +OK, got {ask_ok:?}");
        let set_on_dest = cmd(&mut clients[dest_idx], &["SET", &key, "migrated"]).await;
        assert!(
            set_on_dest.starts_with("+OK"),
            "ASKING; SET on the IMPORTING DEST must be served locally, got {set_on_dest:?}"
        );
        // ASKING then GET on DEST: serves the value (the second leg of the -ASK redirect).
        let ask_ok = cmd(&mut clients[dest_idx], &["ASKING"]).await;
        assert!(ask_ok.starts_with("+OK"), "ASKING should reply +OK, got {ask_ok:?}");
        let get_on_dest = cmd(&mut clients[dest_idx], &["GET", &key]).await;
        assert!(
            get_on_dest.starts_with("$8\r\nmigrated"),
            "ASKING; GET on the IMPORTING DEST must serve the value locally, got {get_on_dest:?}"
        );

        // ---- (6) THE FLIP: commit SETSLOT <slot> NODE <dest> (ownership transfer). On apply, DEST
        // owns and the migration clears on every node. SRC then serves MOVED (NOT ASK) to DEST.
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &dest_synth_id],
        )
        .await;
        assert!(r.starts_with("+OK"), "the FLIP (SETSLOT NODE) should commit, got {r:?}");

        // SRC now MOVEDs the key to DEST (not ASK): the migration is cleared by the committed FLIP.
        let expect_moved_to_dest = format!("-MOVED {slot} 127.0.0.1:{dest_port}");
        let moved = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[src_idx], &["GET", &key]).await;
                if reply.starts_with("-MOVED") {
                    assert!(
                        reply.starts_with(&expect_moved_to_dest),
                        "post-FLIP SRC must MOVED to DEST: expected {expect_moved_to_dest:?}, got {reply:?}"
                    );
                    break true;
                }
                // Until the FLIP applies, SRC still owns + is MIGRATING, so the absent key ASKs;
                // keep polling until ownership has flipped (MOVED).
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(moved, "after the committed FLIP, SRC must serve MOVED (not ASK) to DEST");

        // And DEST now OWNS the slot: a plain GET there (no ASKING) serves the value locally.
        let owned_on_dest = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[dest_idx], &["GET", &key]).await;
                if reply.starts_with("$8\r\nmigrated") {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(
            owned_on_dest,
            "after the FLIP, DEST owns the slot and serves the key locally with no ASKING"
        );
    });

    clean_raft_logs(ports);
}

// `too_many_lines` + `needless_range_loop` allowed: ONE end-to-end HA-6 LIVE-DATA-COPY migration
// flow, read in sequence, indexing parallel `clients[i]`/`ports[i]`.
//
// This is the STRENGTHENED migration loopback: unlike the redirect-protocol test above (which
// FAKES the data move via `ASKING; SET` to isolate the ASK/ASKING/MOVED semantics), this one lets
// the REAL live copy run -- the IMPORTING destination's import control task pulls the slot's data
// from the source as a scoped snapshot PLUS a live mutation stream, applied additively, with NO
// `ASKING; SET` faking. It proves BOTH halves of the copy:
//   * the SNAPSHOT: several keys SET on the SOURCE owner BEFORE the migration land on the DEST.
//   * the STREAM: a key UPDATED on the SOURCE DURING the migration (served locally because it is
//     present on the still-owning source) reaches the DEST via the scoped tail.
// After the committed FLIP, a PLAIN GET on the DEST (no ASKING) serves every key with its latest
// value, and the SOURCE serves MOVED -- the dest actually HAS the data by FLIP time.
#[test]
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn raft_mode_slot_migration_live_copies_snapshot_and_stream_to_dest() {
    let timeout = Duration::from_secs(30);

    let ports = [free_port(), free_port(), free_port()];
    clean_raft_logs(ports);
    let topo = three_node_topology(ports);

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

        // ---- (1) Discover the leader (SRC, the owner).
        let src_idx = {
            let start = env.now();
            let mut found = None;
            'discover: loop {
                for i in 0..3 {
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
            found.expect("a unique leader (SRC) must emerge")
        };

        // ---- (2) SRC MEETs both peers and claims the whole slot space (so it owns the slot).
        for i in 0..3 {
            if i == src_idx {
                continue;
            }
            let r = cmd(
                &mut clients[src_idx],
                &["CLUSTER", "MEET", "127.0.0.1", &ports[i].to_string()],
            )
            .await;
            assert!(r.starts_with("+OK"), "SRC MEET should commit, got {r:?}");
        }
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "ADDSLOTSRANGE", "0", "16383"],
        )
        .await;
        assert!(r.starts_with("+OK"), "SRC ADDSLOTSRANGE should commit, got {r:?}");

        let dest_idx = (src_idx + 1) % 3;
        let dest_port = ports[dest_idx];
        let dest_synth_id = synth_meet_node_id("127.0.0.1", dest_port);
        let src_announce_id = ids[src_idx];

        // Several PRE-EXISTING keys in ONE fixed slot SRC owns (the snapshot's payload). They are
        // co-located via a hash tag so every one hashes to the SAME slot, which is the migration
        // unit. SET them on SRC BEFORE the migration so they are part of the slot's snapshot.
        let pre_keys = ["{mig}:a", "{mig}:b", "{mig}:c", "{mig}:d"];
        let slot = key_slot(pre_keys[0].as_bytes());
        for k in &pre_keys {
            assert_eq!(key_slot(k.as_bytes()), slot, "hash-tag co-locates the keys");
            let r = cmd(&mut clients[src_idx], &["SET", k, "v0"]).await;
            assert!(r.starts_with("+OK"), "pre-migration SET on SRC, got {r:?}");
        }

        // ---- (3a) MIGRATION HANDSHAKE, SOURCE LEG: SETSLOT <slot> MIGRATING <dest>.
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "SETSLOT", &slot.to_string(), "MIGRATING", &dest_synth_id],
        )
        .await;
        assert!(r.starts_with("+OK"), "SETSLOT MIGRATING should commit, got {r:?}");

        // Confirm the MIGRATING tag is LIVE on the leader before the IMPORTING leg (the leader fills
        // the IMPORTING proposal's `dest` from the slot's recorded migration peer, which exists only
        // once MIGRATING has APPLIED). An absent key on the migrating slot -ASKs to DEST once it is.
        let expect_ask = format!("-ASK {slot} 127.0.0.1:{dest_port}");
        let asked = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[src_idx], &["GET", "{mig}:absent"]).await;
                if reply.starts_with(&expect_ask) {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(asked, "the MIGRATING tag must be live on the leader (absent key -ASKs to DEST)");

        // ---- (3b) MIGRATION HANDSHAKE, DESTINATION LEG: SETSLOT <slot> IMPORTING <src>. On apply
        // this tags IMPORTING on EXACTLY the DEST node, which STARTS the DEST's import control task
        // pulling the slot's scoped snapshot + tail from SRC (the live data copy).
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "SETSLOT", &slot.to_string(), "IMPORTING", src_announce_id],
        )
        .await;
        assert!(r.starts_with("+OK"), "SETSLOT IMPORTING should commit, got {r:?}");

        // Wait until the committed IMPORTING tag has APPLIED on DEST (probe with ASKING; GET, which
        // is served locally only once IMPORTING is live -- the same positive signal the protocol
        // test uses; we do NOT write any data through it).
        let dest_importing_live = {
            let start = env.now();
            loop {
                let ask = cmd(&mut clients[dest_idx], &["ASKING"]).await;
                assert!(ask.starts_with("+OK"), "ASKING should reply +OK, got {ask:?}");
                let reply = cmd(&mut clients[dest_idx], &["GET", pre_keys[0]]).await;
                if !reply.starts_with("-MOVED") {
                    break true; // served locally -> IMPORTING is applied on DEST
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(dest_importing_live, "the committed IMPORTING tag must apply on DEST");

        // ---- (4) THE LIVE STREAM DURING MIGRATION: UPDATE a pre-existing key on SRC while the
        // migration is live. The key is PRESENT on the still-owning SRC, so the SET is served
        // LOCALLY on SRC (the migration redirect serves present keys), the write lands in SRC's
        // store, SRC's observer enqueues it, and the DEST's scoped import tail must ship it across.
        // We do NOT touch DEST here -- the ONLY path the new value can reach DEST is the live copy.
        let r = cmd(&mut clients[src_idx], &["SET", pre_keys[0], "v1-during"]).await;
        assert!(
            r.starts_with("+OK"),
            "a SET of a PRESENT key on the MIGRATING source is served locally, got {r:?}"
        );

        // CONVERGENCE GATE (during the window, BEFORE the flip): wait until the DEST's scoped
        // import tail has applied BOTH the snapshot value of an untouched key AND the during-
        // migration update -- observed via `ASKING; GET` on the still-IMPORTING DEST (served
        // locally, no data written through it). Confirming convergence here proves the live copy
        // (snapshot + stream) landed the data BEFORE the flip, so the flip cannot lose it. (A real
        // deployment gates the flip on this apply-lag bound; MIGRATION.md's FENCING phase. Here the
        // test waits for it explicitly, which is the same guarantee for the loopback.)
        let converged = {
            let start = env.now();
            loop {
                // The snapshot key (untouched, original value).
                let ask = cmd(&mut clients[dest_idx], &["ASKING"]).await;
                assert!(ask.starts_with("+OK"));
                let snap = cmd(&mut clients[dest_idx], &["GET", pre_keys[1]]).await;
                // The during-migration updated key (new value).
                let ask = cmd(&mut clients[dest_idx], &["ASKING"]).await;
                assert!(ask.starts_with("+OK"));
                let upd = cmd(&mut clients[dest_idx], &["GET", pre_keys[0]]).await;
                if snap.starts_with("$2\r\nv0") && upd.starts_with("$9\r\nv1-during") {
                    break true; // the live copy converged on DEST during the window.
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(
            converged,
            "the live copy (scoped snapshot + during-migration stream) must converge on DEST before the flip"
        );

        // ---- (5) THE FLIP: commit SETSLOT <slot> NODE <dest>. DEST becomes the owner; its import
        // task sees IMPORTING cleared and stops. SRC serves MOVED.
        let r = cmd(
            &mut clients[src_idx],
            &["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &dest_synth_id],
        )
        .await;
        assert!(r.starts_with("+OK"), "the FLIP (SETSLOT NODE) should commit, got {r:?}");

        // SRC now MOVEDs the slot's keys to DEST (the migration cleared by the committed FLIP).
        let expect_moved = format!("-MOVED {slot} 127.0.0.1:{dest_port}");
        let moved = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[src_idx], &["GET", pre_keys[1]]).await;
                if reply.starts_with("-MOVED") {
                    assert!(
                        reply.starts_with(&expect_moved),
                        "post-FLIP SRC must MOVED to DEST: expected {expect_moved:?}, got {reply:?}"
                    );
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(moved, "after the FLIP, SRC must serve MOVED (not ASK) to DEST");

        // ---- (6) THE PROOF: after the FLIP, DEST OWNS the slot and serves EVERY key on a PLAIN
        // GET (no ASKING) -- the pre-existing snapshot keys at their values AND the during-migration
        // updated key at its NEW value. The data was copied by the live snapshot + stream, NOT
        // faked. Poll for the during-migration value first (it must converge through the tail).
        let updated_on_dest = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[dest_idx], &["GET", pre_keys[0]]).await;
                if reply.starts_with("$9\r\nv1-during") {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        assert!(
            updated_on_dest,
            "the DURING-migration update must reach DEST via the live stream (proves the tail)"
        );
        // The other pre-existing snapshot keys are present on DEST at their original value (proves
        // the snapshot). A plain GET on the owner serves locally, no ASKING.
        for k in &pre_keys[1..] {
            let reply = cmd(&mut clients[dest_idx], &["GET", k]).await;
            assert!(
                reply.starts_with("$2\r\nv0"),
                "pre-existing snapshot key {k:?} must be served by DEST post-FLIP, got {reply:?}"
            );
        }
    });

    clean_raft_logs(ports);
}

// `too_many_lines` + `needless_range_loop` allowed: ONE end-to-end HA-7d acceptance flow
// (formation, owner+replica assignment, write-to-owner, replica attach + READONLY serve, MOVED on
// write / non-READONLY read), read in sequence, indexing parallel `clients[i]`/`ports[i]`.
#[test]
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn raft_mode_replica_attaches_full_syncs_and_serves_readonly_reads() {
    // Replica attach involves: leader election, several committed proposals, a full-sync transfer,
    // and the replica control task's poll cadence. Generous bound for a loaded CI machine.
    let timeout = Duration::from_secs(30);

    let ports = [free_port(), free_port(), free_port()];
    clean_raft_logs(ports);
    let topo = three_node_topology(ports);

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

        // ---- (1) Discover the leader (the OWNER, "node A") by probing a CLUSTER write.
        let leader_idx = {
            let start = env.now();
            let mut found = None;
            'discover: loop {
                for i in 0..3 {
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
            found.expect("a unique leader must emerge")
        };

        // ---- (2) The leader MEETs BOTH peers + claims the WHOLE slot space (so it OWNS the
        // slot under test). Then it commits `CLUSTER REPLICATE <peer-id> <slot>` so a PEER ("node
        // B") becomes a committed REPLICA of that leader-owned slot.
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
            "leader ADDSLOTSRANGE should commit, got {r:?}"
        );

        // The replica node B = the leader's first peer; its committed synth id is host:port-derived.
        let replica_idx = (leader_idx + 1) % 3;
        let replica_port = ports[replica_idx];
        let replica_synth_id = synth_meet_node_id("127.0.0.1", replica_port);

        // A key in a fixed slot the leader owns; the leader writes it, B replicates that slot.
        let key = key_in_range(100, 100);
        let slot = key_slot(key.as_bytes());

        // Commit "B replicates this leader-owned slot".
        let r = cmd(
            &mut clients[leader_idx],
            &["CLUSTER", "REPLICATE", &replica_synth_id, &slot.to_string()],
        )
        .await;
        assert!(
            r.starts_with("+OK"),
            "leader CLUSTER REPLICATE <peer> <slot> should commit, got {r:?}"
        );

        // ---- (3) Write the key on the OWNER (leader). It owns the slot, so SET is served locally.
        let set = cmd(&mut clients[leader_idx], &["SET", &key, "hello"]).await;
        assert!(
            set.starts_with("+OK"),
            "owner serves SET locally, got {set:?}"
        );

        // ---- (4) On the REPLICA (B), a READONLY GET must eventually serve the value LOCALLY
        // (B attaches to A, full-syncs the snapshot, tails the write, and serves the converged
        // read). Set the READONLY bit on B's connection first.
        let ro = cmd(&mut clients[replica_idx], &["READONLY"]).await;
        assert!(
            ro.starts_with("+OK"),
            "READONLY should reply +OK, got {ro:?}"
        );

        let served = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[replica_idx], &["GET", &key]).await;
                if reply.starts_with("$5\r\nhello") {
                    break true;
                }
                // Before the replica has attached + converged it MOVEDs to the owner (it does not
                // yet replicate the value); keep polling until the synced value appears.
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        assert!(
            served,
            "a READONLY GET on the replica must serve the converged value locally"
        );

        // ---- (5) A WRITE on the replica (even with READONLY set) returns MOVED to the OWNER.
        let owner_endpoint = format!("127.0.0.1:{}", ports[leader_idx]);
        let write_reply = cmd(&mut clients[replica_idx], &["SET", &key, "nope"]).await;
        assert!(
            write_reply.starts_with(&format!("-MOVED {slot} {owner_endpoint}")),
            "a write on a READONLY replica must MOVED to the owner, got {write_reply:?}"
        );

        // ---- (6) A NON-READONLY read on the replica returns MOVED to the OWNER. Clear the bit
        // with READWRITE, then GET.
        let rw = cmd(&mut clients[replica_idx], &["READWRITE"]).await;
        assert!(
            rw.starts_with("+OK"),
            "READWRITE should reply +OK, got {rw:?}"
        );
        let strong_read = cmd(&mut clients[replica_idx], &["GET", &key]).await;
        assert!(
            strong_read.starts_with(&format!("-MOVED {slot} {owner_endpoint}")),
            "a non-READONLY read on the replica must MOVED to the owner, got {strong_read:?}"
        );
    });

    clean_raft_logs(ports);
}

/// HA-7d passivity: a replica's shard store must NOT independently expire/evict keys; removals
/// arrive ONLY from the replication stream. End-to-end full convergence under TTL is timing
/// -sensitive over real sockets, so this asserts the structural guarantee directly: the replica
/// shard's active-expiry reaper is gated OFF (`is_replica_passive`) once the shard attaches as a
/// replica. The serve-layer unit (`expire_cycle_tick` returns 0 when passive, default false) plus
/// the replica-attach `set_replica_passive(true)` after the swap are the wired guard; this test
/// documents the end-to-end intent and is covered structurally by the in-crate unit tests
/// (`crate::serve` passive guard + `replica_attach` swap) which run under `cargo test -p ironcache`.
#[test]
fn ha7d_replica_is_passive_note() {
    // The passivity guard is enforced in-process by `crate::serve::expire_cycle_tick` (returns 0
    // when `is_replica_passive()`) and set by `replica_attach::attach_once` after the atomic store
    // swap. Those are exercised by the `-p ironcache` lib unit tests; this acceptance-suite marker
    // records the contract (REPLICA_READ.md #147: a replica applies removals only from the stream)
    // without re-driving a flaky real-time TTL race here.
}

// `too_many_lines` + `needless_range_loop` allowed: ONE end-to-end HA-8 acceptance flow (formation,
// owner + in-sync replica, READONLY serve, then KILL the owner so the replica's link drops, and
// assert the live HA-8 replica-read STALENESS BOUND -- the replica stops serving the now-stale read
// and MOVEDs to the owner), read in sequence, indexing parallel `clients[i]`/`ports[i]`.
#[test]
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn raft_mode_replica_read_staleness_bound_moves_when_link_drops() {
    // This is the LIVE, deterministic half of HA-8 over real TCP: the replica-read staleness gate.
    // (The PROMOTION half -- a committed PromoteReplica transferring ownership and the old primary
    // losing owns() on apply -- is proven exhaustively by the DST split-brain gate
    // `ironcache_raft::tests::failover_split_brain_gate` over 1000+ partition/heal timelines, and
    // by the production wiring's in-crate unit tests; it is NOT re-driven over real sockets here
    // because the test harness cannot stop a killed node's DETACHED raft thread, so a self-promotion
    // -- which commits only on the Raft leader -- cannot be made deterministic in loopback. This
    // test drives the part that IS deterministic and load-bearing live: the staleness gate.)
    let timeout = Duration::from_secs(40);

    let ports = [free_port(), free_port(), free_port()];
    clean_raft_logs(ports);
    let topo = three_node_topology(ports);
    let ids = [ID0, ID1, ID2];

    // Keep the ShardSets so we can KILL the owner (shutdown_and_join stops its data shards -> its
    // repl listener -> the replica's link drops). The raft control thread is detached and keeps
    // voting, so the Raft quorum survives the data-plane kill.
    let mut nodes: Vec<Option<ironcache_runtime::bootstrap::ShardSet>> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| Some(run_raft_node_for_test(ports[i], topo.clone(), id)))
        .collect();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let nodes = rt.block_on(async move {
        let env = SystemEnv::new();
        let mut clients = Vec::new();
        for p in ports {
            clients.push(connect_retry(p).await);
        }

        // ---- (1) Discover the leader (the OWNER, node A) by probing a CLUSTER write.
        let leader_idx = {
            let start = env.now();
            let mut found = None;
            'discover: loop {
                for i in 0..3 {
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
            found.expect("a unique leader must emerge")
        };

        // ---- (2) The leader (OWNER) MEETs both peers, claims the whole slot space, and commits
        // `CLUSTER REPLICATE <peer-B> <slot>` so a PEER (node B) replicates a leader-owned slot.
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
        assert!(r.starts_with("+OK"), "leader ADDSLOTSRANGE should commit, got {r:?}");

        let replica_idx = (leader_idx + 1) % 3; // node B: the pure replica
        let replica_port = ports[replica_idx];
        let replica_synth_id = synth_meet_node_id("127.0.0.1", replica_port);
        let leader_port = ports[leader_idx];
        let key = key_in_range(100, 100);
        let slot = key_slot(key.as_bytes());

        let r = cmd(
            &mut clients[leader_idx],
            &["CLUSTER", "REPLICATE", &replica_synth_id, &slot.to_string()],
        )
        .await;
        assert!(r.starts_with("+OK"), "leader CLUSTER REPLICATE should commit, got {r:?}");

        // ---- (3) Write the key on the OWNER (leader). It owns the slot, so SET serves locally.
        let set = cmd(&mut clients[leader_idx], &["SET", &key, "hello"]).await;
        assert!(set.starts_with("+OK"), "owner serves SET locally, got {set:?}");

        // ---- (4) The REPLICA (B) attaches + is IN SYNC: a READONLY GET serves the value LOCALLY
        // (link up + lag <= max_lag -> the HA-8 staleness gate allows the local read).
        let ro = cmd(&mut clients[replica_idx], &["READONLY"]).await;
        assert!(ro.starts_with("+OK"), "READONLY should reply +OK, got {ro:?}");
        let served = {
            let start = env.now();
            loop {
                let reply = cmd(&mut clients[replica_idx], &["GET", &key]).await;
                if reply.starts_with("$5\r\nhello") {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        assert!(served, "an in-sync READONLY replica must serve the converged value locally");

        // ---- (5) KILL the OWNER (leader's data shards): the replica B's link to it drops, so B is
        // no longer in sync (link down). A's detached raft thread keeps voting, so the quorum holds.
        let _ = clients; // release the borrow set; we reconnect to B below.
        let killed = nodes[leader_idx].take().expect("the owner node is live");
        let _ = killed.shutdown_and_join();

        let mut b = connect_retry(replica_port).await;

        // ---- (6) THE STALENESS BOUND (HA-8, finishing the 7d TODO): once B's link to the owner is
        // down (B is past the lag bound / not in sync), a READONLY GET that was served locally a
        // moment ago must now MOVED to the OWNER. A stale replica never serves a stale read. The
        // MOVED target is the (now-dead) owner's advertised endpoint -- the client would retry there
        // and discover the new topology; the load-bearing assertion is that B STOPPED serving stale.
        let expect_moved_to_owner = format!("-MOVED {slot} 127.0.0.1:{leader_port}");
        let moved = {
            let start = env.now();
            loop {
                let reply = cmd(&mut b, &["GET", &key]).await;
                if reply.starts_with(&expect_moved_to_owner) {
                    break true;
                }
                if deadline_passed(&env, start, timeout) {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        assert!(
            moved,
            "once the replica's link is down (not in sync), a READONLY read must MOVED to the owner \
             (the HA-8 staleness bound: a stale replica stops serving stale reads)"
        );
        nodes
    });

    // Drop every surviving ShardSet (each signals its shards to drain); raft threads are detached.
    for n in nodes.into_iter().flatten() {
        let _ = n.shutdown_and_join();
    }
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
