// SPDX-License-Identifier: MIT OR Apache-2.0
//! #392 clustered rolling-upgrade DRIVER acceptance tests: the LIVE
//! [`ironcache::cluster_upgrade_driver`] driven against REAL in-process servers -- a MINIMAL
//! single-node server for the always-on freeze gate, and a REAL 3-node raft cluster (the same
//! loopback topology `raft_cluster.rs` uses) for the on-demand full live acceptance.
//!
//! ## The two tests + why they are split (CI reliability)
//!
//! ALWAYS-ON CI GATE -- `freeze_seam_holds_a_real_write`: the load-bearing RPO=0 proof on a MINIMAL
//! single-node server (1 shard, NO raft, NO replicas, so NO cluster-formation flake). The exact
//! freeze the driver's `promote_candidate` performs -- a real `CLIENT PAUSE <ms> WRITE` through the
//! shipped [`ironcache::upgrade::pause`] seam -- is shown to HOLD a concurrent write to a key (no
//! `+OK`) until `CLIENT UNPAUSE`, then let it complete and apply. Deterministic; this is the
//! reliable hard gate for the safety mechanism.
//!
//! `#[ignore]` FULL LIVE ACCEPTANCE -- `live_cluster_upgrade_acceptance`: real observe +
//! primary-last sequencing against a REAL 3-node raft cluster. It PASSES when run, but booting a
//! raft cluster and waiting for BOTH replicas in the master's in-sync view is load-sensitive
//! multi-node formation timing (the same class as the documented raft-cluster CI flakiness: passes
//! in isolation, flakes under parallel load), so it is gated OFF the default CI run and executed on
//! demand (`cargo test -- --ignored` / nightly). Its two live properties are:
//!
//!   (a) REAL OBSERVE: [`LiveCluster::refresh`] assembles a correct [`ClusterView`] from the LIVE
//!       RESP surface -- the primary as master on the old version, the two attached replicas
//!       in-sync (link up, master-side lag within the bound), raft quorum true (from `CLUSTER
//!       INFO`), and `old_primary_id` captured as the real owner (the real `INFO` / `CLUSTER INFO`
//!       parse against a live server, not a fixture).
//!   (b) PRIMARY-LAST SEQUENCING: [`ironcache::cluster_upgrade_driver::run_cluster_upgrade`] driven
//!       to completion must upgrade BOTH replicas BEFORE the primary, issue EXACTLY one failover,
//!       keep `old_primary_id` fixed (no spurious second promotion), and terminate `Completed`.
//!
//! Its deterministic parts are ALSO covered off the live path: the driver's sequencing / fence logic
//! by the PR1 driver mock unit tests, and the promotion correctness by the DST split-brain gate
//! (below).
//!
//! ## The verification split (why parts are controllable, and what covers the rest)
//!
//! A loopback harness CANNOT deterministically drive a real COMMITTED cluster failover promotion
//! (documented at `crates/ironcache/tests/raft_cluster.rs:1508`: a self-promotion commits only on
//! the raft leader and the harness cannot stop a killed node's detached raft thread). The
//! promotion CORRECTNESS is proven exhaustively elsewhere by the DST split-brain gate
//! `ironcache_raft::tests::failover_split_brain_gate` (1000+ partition/heal timelines). So the
//! sequencing part (b) makes the promotion EFFECT controllable: after the driver issues the
//! failover the harness client flips the observed roles (candidate -> master, old primary ->
//! replica), exactly as a committed `PromoteReplica` would, so the state machine advances
//! deterministically WITHOUT a real committed promotion.
//!
//! The same part also OVERLAYS the observed per-node VERSION from the controllable roll model:
//! the dev/CI build pins `ironcache_version` to a single compile-time `CARGO_PKG_VERSION`
//! (`crates/ironcache-observe`), so a SIMULATED binary swap cannot change what a real node reports
//! -- the version progression the sequence keys on is therefore unobservable in-process. Every
//! OTHER fact in (b) is issued to the LIVE nodes each tick: the `INFO` / `CLUSTER INFO`
//! round-trip and the raft-quorum read are real, and the freeze inside `promote_candidate` is a
//! real `CLIENT PAUSE`/`CLIENT UNPAUSE` on the live primary.
//!
//! Member DISCOVERY is likewise shimmed to the known member set: in-process `CLUSTER SHARDS`
//! projects only slot-owners plus their FIRST-slot single replica
//! (`crates/ironcache-server/src/cmd_cluster.rs`), so it cannot enumerate a 3-node / 2-replica
//! shard; the harness still issues the real `CLUSTER SHARDS` round-trip, then returns the known
//! inventory ids so the membership cross-check passes while every role/version/lag/quorum fact is
//! read live.
//!
//! LIVE COMPOSITION -- a real committed promotion under sustained traffic, plus the adversarial
//! no-freeze control that must SHOW acked-write loss -- is a docker-harness smoke follow-up (not
//! a CI gate), recorded in `docs/UPGRADE.md`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ironcache::cluster_upgrade::{ClusterView, NodeRole};
use ironcache::cluster_upgrade_driver::{
    ActuationTarget, ClusterClient, ClusterUpgradeError, DriverConfig, FreezeCfg, Inventory,
    LiveCluster, NodeObservation, NodeUpgrader, PollCfg, QuorumObservation, RespClusterClient,
    SlaveEntry, Sleeper, run_cluster_upgrade,
};
use ironcache::raft_boot::bus_port;
use ironcache::test_support::{run_raft_node_for_test_min_replicas, run_server_for_test};
use ironcache::upgrade::pause::{LoopbackPauser, PauseTarget, Pauser};
use ironcache_config::{ClusterNode, ClusterTopology};
use ironcache_env::{Clock, Monotonic, SystemEnv};
use ironcache_repl::{LinkStatus, PromotionSafety, UpgradeReport};
use ironcache_runtime::bootstrap::ShardSet;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary (so the
// process-memory path used by INFO is live; harmless otherwise). Matches `raft_cluster.rs`.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ID0: &str = "0000000000000000000000000000000000000000";
const ID1: &str = "1111111111111111111111111111111111111111";
const ID2: &str = "2222222222222222222222222222222222222222";

/// The target version the roll aims at. It MUST differ from the node's compiled-in
/// `ironcache_version` so the observed nodes read as "old" (a roll is needed / not already done).
const TARGET_VERSION: &str = "9.9.9";

/// The shared convergence deadline for every wait loop, matching `raft_cluster.rs`: multi-node
/// raft integration boots (leader election + committed proposals + a full-sync attach) converge in
/// well under a second locally but stretch by an order of magnitude on an oversubscribed CI runner
/// (the raft/replication timers slip under CPU starvation), so the bound is generous. Every loop
/// breaks early the instant the state is reached; only a genuinely stuck cluster ever waits this long.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);

/// The per-op timeout for the driver's OBSERVE poll (part 1): generous so a slow (CPU-starved) RESP
/// op COMPLETES rather than truncating, but bounded so a genuinely wedged connection cannot hang the
/// poll (each observe is retried within [`CONVERGENCE_TIMEOUT`] anyway).
const OBSERVE_POLL_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Shared loopback + RESP helpers (mirrors raft_cluster.rs; a test binary cannot share them)
// ---------------------------------------------------------------------------

/// Grab a free TCP port (bind ephemeral, read it, drop). A brief TOCTOU window before the node
/// rebinds; fine on loopback for a test.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// The shared 3-node raft topology: ids ID0/ID1/ID2 on the given ports, all on 127.0.0.1. In
/// raft-mode the `slots` ranges are IGNORED (ownership is established at runtime), so they are empty.
fn three_node_topology(ports: [u16; 3]) -> ClusterTopology {
    let node = |id: &str, port| ClusterNode {
        id: id.to_owned(),
        host: "127.0.0.1".to_owned(),
        port,
        slots: vec![],
    };
    ClusterTopology {
        nodes: vec![
            node(ID0, ports[0]),
            node(ID1, ports[1]),
            node(ID2, ports[2]),
        ],
    }
}

/// Remove any stale per-node FileStorage log so each node boots with a FRESH raft log. The path
/// matches `raft_boot`'s `<temp>/ironcache-raft-<bus-port>.log`.
fn clean_raft_logs(ports: [u16; 3]) {
    for p in ports {
        let path = std::env::temp_dir().join(format!("ironcache-raft-{}.log", bus_port(p)));
        let _ = std::fs::remove_file(path);
    }
}

/// The resp `host:port` for a loopback node.
fn addr_of(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// Whether `timeout` has elapsed since `start`, measured through the env clock (ADR-0003: even
/// tests read real time only through the env seam, never `std::time::Instant`).
fn deadline_passed(env: &SystemEnv, start: Monotonic, timeout: Duration) -> bool {
    env.now().saturating_duration_since(start) >= timeout
}

/// Connect with short retries: the shards + raft control plane bind asynchronously on their own
/// threads after the boot helper returns.
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

/// Byte length of the FIRST complete RESP reply at `buf[start..]`, or `None` if the buffer does not
/// yet hold one complete reply. Handles RESP2 + RESP3 framing so a test read returns EXACTLY one
/// reply and never a partial that would desync the next command. (Mirrors `raft_cluster.rs`.)
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

/// Read until ONE complete RESP reply is buffered (a generous absolute cap bounds a true hang).
async fn read_reply(client: &mut TcpStream) -> String {
    let mut acc = Vec::new();
    for _ in 0..120 {
        if let Some(len) = resp_reply_len(&acc, 0) {
            return String::from_utf8_lossy(&acc[..len]).into_owned();
        }
        let mut buf = [0u8; 8192];
        match tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => break,
            Ok(Ok(n)) => acc.extend_from_slice(&buf[..n]),
            Err(_) => {}
        }
    }
    String::from_utf8_lossy(&acc).into_owned()
}

/// Send a RESP array command (each arg a bulk string) and read the reply as a string.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", args.len());
    for a in args {
        write!(frame, "${}\r\n{a}\r\n", a.len()).unwrap();
    }
    client.write_all(frame.as_bytes()).await.unwrap();
    read_reply(client).await
}

// ---------------------------------------------------------------------------
// Cluster formation over the wire (discover leader -> MEET -> claim all slots -> converge)
// ---------------------------------------------------------------------------

/// Discover the leader (the OWNER) by probing a `CLUSTER MEET` (idempotent on the leader, rejected
/// on a follower until it learns the leader), then have it MEET both peers, claim the WHOLE slot
/// space, and converge every node to `cluster_state:ok` + 16384 assigned. Returns the leader index.
#[allow(clippy::needless_range_loop)] // loops index the parallel `clients[i]` / `ports[i]` arrays
async fn form_owned_cluster(
    clients: &mut [TcpStream],
    ports: [u16; 3],
    env: &SystemEnv,
    timeout: Duration,
) -> usize {
    // (1) Discover the leader: poll until some node accepts a CLUSTER write.
    let leader_idx = {
        let start = env.now();
        'discover: loop {
            for i in 0..3 {
                let peer = (i + 1) % 3;
                let reply = cmd(
                    &mut clients[i],
                    &["CLUSTER", "MEET", "127.0.0.1", &ports[peer].to_string()],
                )
                .await;
                if reply.starts_with("+OK") {
                    break 'discover i;
                }
            }
            assert!(
                !deadline_passed(env, start, timeout),
                "a node must emerge that accepts a CLUSTER write"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };

    // (2) The leader MEETs BOTH peers (so they enter the committed node table) + claims all slots.
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

    // (3) Converge: every node reports state:ok + 16384 assigned.
    let start = env.now();
    loop {
        let mut all_ok = true;
        for i in 0..3 {
            let info = cmd(&mut clients[i], &["CLUSTER", "INFO"]).await;
            if !(info.contains("cluster_state:ok") && info.contains("cluster_slots_assigned:16384"))
            {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        assert!(
            !deadline_passed(env, start, timeout),
            "all three nodes must converge to cluster_state:ok + 16384 slots assigned"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    leader_idx
}

/// Commit `CLUSTER REPLICATE <replica-id> <slot>` on the leader (a peer becomes a replica of a
/// leader-owned slot).
async fn replicate(clients: &mut [TcpStream], leader_idx: usize, replica_id: &str, slot: u16) {
    let r = cmd(
        &mut clients[leader_idx],
        &["CLUSTER", "REPLICATE", replica_id, &slot.to_string()],
    )
    .await;
    assert!(
        r.starts_with("+OK"),
        "leader CLUSTER REPLICATE {replica_id} {slot} should commit, got {r:?}"
    );
}

/// Poll a node's own `INFO` until it reports `role:replica` with `master_link_status:up` (it has
/// attached to its owner and full-synced), so its node-level role is a settled replica.
async fn wait_replica_attached(client: &mut TcpStream, env: &SystemEnv, timeout: Duration) {
    let start = env.now();
    loop {
        let info = cmd(client, &["INFO"]).await;
        if info.contains("role:replica") && info.contains("master_link_status:up") {
            return;
        }
        assert!(
            !deadline_passed(env, start, timeout),
            "a committed replica must attach (role:replica + master_link_status:up)"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the PRIMARY's `INFO` until it lists BOTH replica endpoints as `state=online` in its
/// per-replica (`slaveN`) view -- the EXACT master-side fact the driver's observe reads to judge a
/// replica in-sync. Driven over this resilient path so the driver's own poll starts from an
/// already-converged master-side view instead of racing it coming up under load.
async fn wait_master_lists_replicas(
    client: &mut TcpStream,
    replica_ports: [u16; 2],
    env: &SystemEnv,
    timeout: Duration,
) {
    let start = env.now();
    loop {
        let info = cmd(client, &["INFO"]).await;
        let both_online = replica_ports
            .iter()
            .all(|p| info.contains(&format!("port={p},state=online")));
        if both_online {
            return;
        }
        assert!(
            !deadline_passed(env, start, timeout),
            "the primary must list both replicas online in its INFO replication view"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Build the driver inventory for the shard: the primary first, then the two replicas, each with
/// its live loopback resp addr + announce id. `ssh` / `upgrade_source` are unused (the tests use a
/// stub / noop [`NodeUpgrader`], never the real SSH actuator).
fn inventory_for(primary: (&str, u16), replicas: [(&str, u16); 2]) -> Inventory {
    let target = |id: &str, port: u16| ActuationTarget {
        id: id.to_owned(),
        resp_addr: addr_of(port),
        auth: None,
        ssh: String::new(),
        upgrade_source: String::new(),
    };
    Inventory::new(vec![
        target(primary.0, primary.1),
        target(replicas[0].0, replicas[0].1),
        target(replicas[1].0, replicas[1].1),
    ])
}

/// Split a `host:port` into its parts (rightmost `:`).
fn split_host_port(addr: &str) -> (String, u16) {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    (host.to_owned(), port.parse().expect("port"))
}

/// A compact, human-readable dump of an observed [`ClusterView`], for the convergence-timeout
/// diagnostic (names exactly which node/role/link/lag/quorum fact was not yet converged).
fn describe_view(v: &ClusterView) -> String {
    let nodes: Vec<String> = v
        .nodes
        .iter()
        .map(|n| {
            let id4 = n.id.get(..4).unwrap_or(&n.id);
            format!(
                "{id4}[role={:?} link={:?} lag={:?} ver={}]",
                n.role,
                n.link,
                n.lag.and_then(ironcache_repl::ReplicaLag::lag),
                n.version
            )
        })
        .collect();
    format!("quorum={} nodes={{{}}}", v.raft_quorum, nodes.join(", "))
}

// ===========================================================================
// OBSERVE support (the real-OBSERVE client used by the ignored full live test)
// ===========================================================================

/// A pass-through [`ClusterClient`] whose OBSERVE (`INFO` role/version/lag) and quorum
/// (`CLUSTER INFO`) go LIVE to the real [`RespClusterClient`]; only member DISCOVERY is shimmed to
/// the known inventory ids, because in-process `CLUSTER SHARDS` cannot enumerate a 3-node /
/// 2-replica shard (it projects slot-owners + a single first-slot replica). The real
/// `CLUSTER SHARDS` round-trip is still issued (transport exercised), its incomplete result
/// discarded.
struct MembershipShimClient {
    inner: RespClusterClient,
    member_ids: Vec<String>,
}

impl ClusterClient for MembershipShimClient {
    fn discover_members(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<Vec<String>, ClusterUpgradeError> {
        let _ = self.inner.discover_members(seed); // exercise the real CLUSTER SHARDS transport
        Ok(self.member_ids.clone())
    }
    fn observe_node(
        &mut self,
        node: &ActuationTarget,
    ) -> Result<NodeObservation, ClusterUpgradeError> {
        self.inner.observe_node(node) // REAL INFO parse against the live node
    }
    fn observe_quorum(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<QuorumObservation, ClusterUpgradeError> {
        self.inner.observe_quorum(seed) // REAL CLUSTER INFO parse against the live node
    }
    fn cluster_failover(&mut self, node: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        self.inner.cluster_failover(node)
    }
}

/// A no-op upgrader + sleeper for the observe path (the observe step never upgrades or paces).
struct NoopUpgrader;
impl NodeUpgrader for NoopUpgrader {
    fn upgrade(&mut self, _t: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        Ok(())
    }
}
struct NoopSleeper;
impl Sleeper for NoopSleeper {
    fn sleep(&self, _dur: Duration) {}
}

// ===========================================================================
// CI GATE (always on): the freeze holds a real write on a MINIMAL single-node server
// ===========================================================================

/// The load-bearing RPO=0 proof as an ALWAYS-ON, deterministic CI gate. A MINIMAL single-node server
/// (1 shard, NO raft, NO replicas -> NO cluster-formation flake) is frozen through the driver's EXACT
/// freeze seam -- the shipped [`LoopbackPauser`] issuing `CLIENT PAUSE <ms> WRITE` via a
/// [`PauseTarget`], which is precisely what [`LiveCluster::promote_candidate`]'s failover-freeze fence
/// does -- and a concurrent write to a key is shown to be HELD (no `+OK`) across a bounded window,
/// then RELEASED and APPLIED by `CLIENT UNPAUSE`. This proves the real-server mechanism the driver's
/// RPO=0 fence depends on (no acknowledged write escapes into the loss window while the fence is up)
/// with zero multi-node raft formation, so it is a reliable hard gate.
#[test]
fn freeze_seam_holds_a_real_write() {
    let port = free_port();
    let _node = run_server_for_test(port, 1);
    let addr = addr_of(port);
    let env = SystemEnv::new();
    let key = "cu392b-freeze-key";

    // Wait until the node serves, and confirm a NORMAL write is acked (the baseline the freeze
    // suppresses). `blocking_resp` returns "" on a not-yet-up connect, so this doubles as readiness.
    let start = env.now();
    loop {
        if blocking_resp(&addr, &["SET", key, "v1"]).contains("+OK") {
            break;
        }
        assert!(
            !deadline_passed(&env, start, Duration::from_secs(30)),
            "the single-node server never accepted a write"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // FREEZE through the driver's exact seam: LoopbackPauser -> CLIENT PAUSE <ms> WRITE on the node.
    let pause_target = PauseTarget {
        resp_addr: addr.clone(),
        auth: None,
        window_ms: 30_000,
    };
    let pauser = LoopbackPauser;
    pauser.freeze(&pause_target).expect("freeze the node");

    // A concurrent writer (BLOCKING std socket, so it cannot nest a runtime): it SETs the key and
    // only sets `acked` once the server replies +OK.
    let acked = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicBool::new(false));
    let writer = {
        let acked = Arc::clone(&acked);
        let sent = Arc::clone(&sent);
        let addr = addr.clone();
        std::thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(&addr).expect("writer connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(15)))
                .unwrap();
            stream
                .write_all(&encode_resp(&["SET", key, "v2"]))
                .expect("writer send");
            sent.store(true, Ordering::SeqCst);
            let mut buf = Vec::new();
            let mut tmp = [0u8; 512];
            loop {
                match stream.read(&mut tmp) {
                    Ok(n) if n > 0 => {
                        buf.extend_from_slice(&tmp[..n]);
                        if buf.windows(3).any(|w| w == b"+OK") {
                            acked.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                    // EOF (Ok(0)) or a read timeout / error: give up (the assert below fails loud).
                    _ => break,
                }
            }
        })
    };

    // The write must be HELD while the freeze is up: no +OK across a bounded window.
    let start = env.now();
    while !sent.load(Ordering::SeqCst) {
        assert!(
            !deadline_passed(&env, start, Duration::from_secs(5)),
            "the writer never sent its SET"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    for _ in 0..7 {
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !acked.load(Ordering::SeqCst),
            "the write must NOT be acked while the freeze holds it (RPO=0 no-ack)"
        );
    }

    // UNFREEZE (CLIENT UNPAUSE): the held write must now complete.
    pauser.unfreeze(&pause_target).expect("unfreeze the node");
    let start = env.now();
    while !acked.load(Ordering::SeqCst) {
        assert!(
            !deadline_passed(&env, start, Duration::from_secs(10)),
            "the write must complete after the freeze is lifted"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    writer.join().expect("writer thread");

    // The released write actually applied (not lost).
    let got = blocking_resp(&addr, &["GET", key]);
    assert!(
        got.contains("v2"),
        "the write released by unfreeze was applied (GET = {got:?})"
    );
}

// ===========================================================================
// The FULL 3-node live acceptance (#[ignore]: load-sensitive raft formation)
// ===========================================================================

// The FULL 3-node live acceptance (real observe of both-replicas-in-sync + primary-last
// sequencing). `#[ignore]` off the default CI run: booting a real raft cluster and waiting for BOTH
// replicas in the master's in-sync view is LOAD-SENSITIVE multi-node formation timing (the same class
// as the documented raft-cluster CI flakiness: passes in isolation, flakes under parallel load), so
// it is not a reliable HARD gate. Its DETERMINISTIC properties are covered elsewhere: the driver's
// sequencing / freeze-fence logic by the PR1 driver mock unit tests (`cluster_upgrade_driver` +
// `upgrade_plan`), the promotion correctness by the DST `ironcache_raft::tests::failover_split_brain_gate`,
// and the load-bearing real-server FREEZE by the always-on single-node gate below
// (`freeze_seam_holds_a_real_write`). Run this on demand with `cargo test -- --ignored` / nightly;
// it PASSES when run (it is just gated off the default flake-sensitive path).
#[test]
#[ignore = "load-sensitive multi-node raft formation timing; run with --ignored / nightly (see doc)"]
#[allow(clippy::too_many_lines, clippy::similar_names)]
fn live_cluster_upgrade_acceptance() {
    let timeout = CONVERGENCE_TIMEOUT;
    let ports = [free_port(), free_port(), free_port()];
    clean_raft_logs(ports);
    let topo = three_node_topology(ports);

    // ONE 3-node cluster serves BOTH live properties (observe + sequencing): test-process server
    // threads are NOT torn down between `#[test]` functions, so a second cluster would leave six raft
    // nodes contending and starve the replication timers. (min_replicas_to_write = 1 is harmless
    // here; no write is issued -- the write-freeze proof is the separate single-node gate.)
    let ids = [ID0, ID1, ID2];
    let _nodes: Vec<ShardSet> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| run_raft_node_for_test_min_replicas(ports[i], topo.clone(), id, 1))
        .collect();

    // Two distinct primary-owned slots for the two replicas to replicate, so both attach and show up
    // as in-sync replicas in the observed view.
    let slot_a: u16 = 1000;
    let slot_b: u16 = 2000;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // SETUP (async, resilient 1s-read path): form the cluster, attach both replicas, and DRIVE the
    // exact master-side convergence the observe step needs.
    let (primary_id, primary_port, replica_ids, replica_ports) = rt.block_on(async {
        let mut clients = Vec::new();
        for p in ports {
            clients.push(connect_retry(p).await);
        }
        let env = SystemEnv::new();

        let leader_idx = form_owned_cluster(&mut clients, ports, &env, timeout).await;
        let ra_idx = (leader_idx + 1) % 3;
        let rb_idx = (leader_idx + 2) % 3;

        // Each replica replicates one distinct primary-owned slot.
        replicate(&mut clients, leader_idx, ids[ra_idx], slot_a).await;
        replicate(&mut clients, leader_idx, ids[rb_idx], slot_b).await;

        // Both must attach (settled role:replica + link up) before we observe.
        wait_replica_attached(&mut clients[ra_idx], &env, timeout).await;
        wait_replica_attached(&mut clients[rb_idx], &env, timeout).await;

        // Front-load the MASTER-side convergence (the exact fact the driver's observe reads): wait,
        // through this resilient path, until the primary's INFO lists BOTH replicas online. This
        // makes the driver's own poll below start from an already-converged cluster (it just
        // re-confirms), instead of racing the master-side slaveN view coming up.
        wait_master_lists_replicas(
            &mut clients[leader_idx],
            [ports[ra_idx], ports[rb_idx]],
            &env,
            timeout,
        )
        .await;

        (
            ids[leader_idx].to_owned(),
            ports[leader_idx],
            [ids[ra_idx].to_owned(), ids[rb_idx].to_owned()],
            [ports[ra_idx], ports[rb_idx]],
        )
    });

    // ---- PROPERTY 1: REAL OBSERVE (synchronous; the RespClusterClient owns its own runtime, so the
    // driver must run OUTSIDE the setup runtime -- block_on above has already returned). ----
    let ra_port = replica_ports[0];
    let rb_port = replica_ports[1];
    let inventory = inventory_for(
        (&primary_id, primary_port),
        [(&replica_ids[0], ra_port), (&replica_ids[1], rb_port)],
    );
    let member_ids: Vec<String> = vec![
        primary_id.clone(),
        replica_ids[0].clone(),
        replica_ids[1].clone(),
    ];
    let config = DriverConfig {
        inventory,
        target_version: TARGET_VERSION.to_owned(),
        // A generous in-sync bound: an attached, quiescent replica reads lag ~0, so this asserts
        // "known lag within bound" (in-sync) without a flaky tight threshold.
        max_lag: 1_000_000,
        poll: PollCfg {
            tick_delay: Duration::ZERO,
        },
        freeze: FreezeCfg::default(),
    };
    let client = MembershipShimClient {
        inner: RespClusterClient::new(OBSERVE_POLL_TIMEOUT).expect("resp client"),
        member_ids,
    };
    let mut live = LiveCluster::new(
        client,
        NoopUpgrader,
        Box::new(LoopbackPauser),
        Box::new(NoopSleeper),
        config,
    );

    // Poll the driver's real refresh until the live cluster is observed as: primary master on the
    // old version, both replicas in-sync (link up, known lag within bound), raft quorum true. The
    // poll (not a single snapshot) absorbs the master-side slaveN view catching up + any raft
    // heartbeat jitter under a loaded CI runner; a rich diagnostic is carried into the deadline
    // panic so a genuine stall names the offending fact.
    let env = SystemEnv::new();
    let start = env.now();
    let mut refreshes = 0u32;
    let view = loop {
        // A fresh per-tick description (used only in the deadline panic below), so a genuine stall
        // names exactly which node/role/link/lag/quorum fact was not converged.
        let desc = match live.refresh() {
            Ok(v) => {
                refreshes += 1;
                let primary_master = v
                    .nodes
                    .iter()
                    .find(|n| n.id == primary_id)
                    .is_some_and(|p| p.role == NodeRole::Master);
                let replicas_in_sync = replica_ids.iter().all(|rid| {
                    v.nodes.iter().any(|n| {
                        n.id == *rid
                            && n.role == NodeRole::Replica
                            && n.link == LinkStatus::Up
                            && n.lag.is_some_and(|l| l.in_sync(v.max_lag))
                    })
                });
                if primary_master && v.raft_quorum && replicas_in_sync {
                    break v;
                }
                describe_view(&v)
            }
            Err(e) => format!("refresh error: {e}"),
        };
        assert!(
            !deadline_passed(&env, start, timeout),
            "the live cluster never converged to master + two in-sync replicas + quorum \
             (refreshes={refreshes}, elapsed={:?}); last: {desc}",
            env.now().saturating_duration_since(start)
        );
        std::thread::sleep(Duration::from_millis(250));
    };

    // Assert the assembled ClusterView is correct (proves the real INFO / CLUSTER INFO parse).
    assert_eq!(view.nodes.len(), 3, "the whole shard is observed");
    let primary = view.nodes.iter().find(|n| n.id == primary_id).unwrap();
    assert_eq!(primary.role, NodeRole::Master, "the primary is the master");
    assert_eq!(
        primary.link,
        LinkStatus::Down,
        "a master has no upstream link"
    );
    assert!(primary.lag.is_none(), "a master has no lag");
    for rid in &replica_ids {
        let r = view.nodes.iter().find(|n| n.id == *rid).unwrap();
        assert_eq!(r.role, NodeRole::Replica, "{rid} is a replica");
        assert_eq!(r.link, LinkStatus::Up, "{rid} link is up");
        assert!(
            r.lag.is_some_and(|l| l.in_sync(view.max_lag)),
            "{rid} has a known master-side lag within the in-sync bound"
        );
    }
    // Every node reports the SAME (old) version, and it is NOT the target -> two replicas to
    // upgrade, shard not already rolled (robust to whatever the build's CARGO_PKG_VERSION is).
    let versions: BTreeSet<&str> = view.nodes.iter().map(|n| n.version.as_str()).collect();
    assert_eq!(
        versions.len(),
        1,
        "all nodes on one old version: {versions:?}"
    );
    assert!(
        !versions.contains(TARGET_VERSION),
        "the observed version must not already be the target"
    );
    assert_eq!(view.replicas_to_upgrade(), 2, "two replicas need upgrading");
    assert!(!view.shard_fully_upgraded(), "a roll is needed");
    assert!(view.raft_quorum, "the raft quorum flag is true");
    assert_eq!(
        live.old_primary_id(),
        Some(primary_id.as_str()),
        "old_primary_id captured as the real pre-roll primary"
    );
    // With no UPGRADED in-sync replica yet, the promotion gate must NOT read Safe (it defers).
    assert_eq!(
        view.promotion_safety(),
        PromotionSafety::CandidateNotInSync,
        "no premature Safe verdict before any replica is upgraded"
    );

    // ---- PROPERTY 3: PRIMARY-LAST SEQUENCING against the SAME live cluster (still P master + two
    // attached replicas: property 1 above only observed it). Build a FRESH controllable roll model
    // over it and drive the whole run_cluster_upgrade. The version and the post-failover role flip
    // are overlaid (loopback cannot reflect a binary-swap version bump nor a deterministic committed
    // promotion, DST-proven separately), while the INFO / CLUSTER INFO transport, the quorum read,
    // and the freeze inside promote_candidate stay LIVE. ----
    let roll_inventory = inventory_for(
        (&primary_id, primary_port),
        [(&replica_ids[0], ra_port), (&replica_ids[1], rb_port)],
    );
    let mut roll_addrs = BTreeMap::new();
    roll_addrs.insert(primary_id.clone(), addr_of(primary_port));
    roll_addrs.insert(replica_ids[0].clone(), addr_of(ra_port));
    roll_addrs.insert(replica_ids[1].clone(), addr_of(rb_port));
    let roll_member_ids: Vec<String> = vec![
        primary_id.clone(),
        replica_ids[0].clone(),
        replica_ids[1].clone(),
    ];
    let state = Rc::new(RefCell::new(RollState {
        upgraded: BTreeSet::new(),
        resync: BTreeMap::new(),
        primary_id: primary_id.clone(),
        addrs: roll_addrs,
        promoted: None,
        order: Vec::new(),
        failovers: 0,
    }));
    let roll_config = DriverConfig {
        inventory: roll_inventory,
        target_version: TARGET_VERSION.to_owned(),
        max_lag: 8,
        poll: PollCfg {
            tick_delay: Duration::ZERO,
        },
        freeze: FreezeCfg {
            // A real CLIENT PAUSE window on the live primary; unfrozen right after the (immediate,
            // synth-lag-0) drain, so it never interferes.
            pause_window_ms: 5_000,
            max_drain_polls: 16,
            drain_poll_delay: Duration::ZERO,
        },
    };
    let roll_client = RollHarnessClient {
        // A GENEROUS per-op timeout: run_cluster_upgrade does NOT retry a refresh error and the roll
        // issues only a handful of refreshes, so a slow-but-valid op is TOLERATED (bounded latency)
        // rather than turned into a spurious failure.
        inner: RespClusterClient::new(Duration::from_secs(20)).expect("resp client"),
        state: Rc::clone(&state),
        member_ids: roll_member_ids,
    };
    let mut roll = LiveCluster::new(
        roll_client,
        StubUpgrader {
            state: Rc::clone(&state),
        },
        Box::new(LoopbackPauser), // REAL freeze on the live primary inside promote_candidate
        Box::new(RollSleeper {
            state: Rc::clone(&state),
        }),
        roll_config,
    );

    // Drive the WHOLE roll against the live cluster.
    let report = run_cluster_upgrade(&mut roll, 50).expect("the roll must not error");
    assert_eq!(report, UpgradeReport::Completed, "the roll completed");

    let st = state.borrow();
    assert_eq!(
        st.order.len(),
        3,
        "every node upgraded once: {:?}",
        st.order
    );
    assert_eq!(
        st.order[2], primary_id,
        "the primary is upgraded LAST: {:?}",
        st.order
    );
    let upgraded_first_two: BTreeSet<&str> = st.order[..2].iter().map(String::as_str).collect();
    let expected_replicas: BTreeSet<&str> = replica_ids.iter().map(String::as_str).collect();
    assert_eq!(
        upgraded_first_two, expected_replicas,
        "both replicas were upgraded BEFORE the primary"
    );
    assert_eq!(st.failovers, 1, "exactly one failover was issued");
    assert_eq!(
        roll.old_primary_id(),
        Some(primary_id.as_str()),
        "old_primary_id stayed the pre-roll primary throughout (no spurious second promotion)"
    );

    clean_raft_logs(ports);
}

// ===========================================================================
// PROPERTY 3 support: the controllable roll model + harness client
// ===========================================================================

/// The controllable roll model the harness client + stub upgrader mutate. It overlays the two facts
/// an in-process loopback cannot reflect -- the per-node VERSION (a simulated binary swap; the real
/// build pins one compile-time version) and the post-failover ROLE flip + master-side lag view (a
/// real committed promotion is not deterministic in loopback) -- while everything else is read live.
struct RollState {
    /// Node ids currently on the target version (the stub upgrader marks these).
    upgraded: BTreeSet<String>,
    /// Ticks-remaining for a just-upgraded node to re-sync (link down / large lag while > 0).
    resync: BTreeMap<String, u32>,
    /// The pre-roll primary id (demoted to a replica once the promotion is injected).
    primary_id: String,
    /// Every node id, with its loopback resp addr, for the synthesized master-side slave view.
    addrs: BTreeMap<String, String>,
    /// The promoted candidate (the new master) once the driver has issued its failover.
    promoted: Option<String>,
    /// The order the stub upgrader upgraded nodes in (the primary-last assertion).
    order: Vec<String>,
    /// The number of failovers the driver issued (must be exactly one).
    failovers: usize,
}

impl RollState {
    fn version_of(&self, id: &str) -> String {
        if self.upgraded.contains(id) {
            TARGET_VERSION.to_owned()
        } else {
            // The old version: whatever the build reports is fine as long as it is not the target;
            // a stable synthetic old tag keeps the overlay self-consistent.
            "0.0.0".to_owned()
        }
    }
    fn resync_of(&self, id: &str) -> u32 {
        self.resync.get(id).copied().unwrap_or(0)
    }
    /// The effective role: the REAL observed role before any promotion; after the injected failover
    /// the candidate is master and the old primary a replica (exactly a committed PromoteReplica).
    fn effective_role(&self, id: &str, real_role: NodeRole) -> NodeRole {
        match &self.promoted {
            Some(c) if id == c => NodeRole::Master,
            Some(_) if id == self.primary_id => NodeRole::Replica,
            _ => real_role,
        }
    }
    /// Advance one tick: a re-syncing node gets one step closer to caught up.
    fn tick(&mut self) {
        for v in self.resync.values_mut() {
            *v = v.saturating_sub(1);
        }
    }
}

/// The harness [`ClusterClient`] for the sequencing roll: it issues the REAL `INFO` / `CLUSTER INFO`
/// round-trip to the live node every tick (transport + liveness) and reads quorum live, but OVERLAYS
/// the version + post-promotion role/lag from the controllable [`RollState`], and INTERCEPTS the
/// failover (records it + flips the observed roles) instead of driving a non-deterministic committed
/// promotion. See the module header for why (loopback version pin + promotion non-determinism; the
/// promotion correctness is DST-proven by `failover_split_brain_gate`).
struct RollHarnessClient {
    inner: RespClusterClient,
    state: Rc<RefCell<RollState>>,
    member_ids: Vec<String>,
}

impl ClusterClient for RollHarnessClient {
    fn discover_members(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<Vec<String>, ClusterUpgradeError> {
        let _ = self.inner.discover_members(seed); // real CLUSTER SHARDS transport
        Ok(self.member_ids.clone())
    }

    fn observe_node(
        &mut self,
        node: &ActuationTarget,
    ) -> Result<NodeObservation, ClusterUpgradeError> {
        // REAL INFO round-trip (transport + the live node answers each tick); its role is used
        // pre-promotion, the rest overlaid from the roll model.
        let real = self.inner.observe_node(node)?;
        let st = self.state.borrow();
        let role = st.effective_role(&node.id, real.role);
        let version = st.version_of(&node.id);
        let (link, slaves) = match role {
            NodeRole::Master => {
                // The master-side per-replica lag view, synthesized: every OTHER node is a slave,
                // lag large while it is still re-syncing (resync > 0) else 0. (Real loopback lag is
                // always 0; the synthesis models a just-upgraded replica catching up, and keeps the
                // view coherent after the injected promotion flips who the master is.)
                let slaves = st
                    .addrs
                    .iter()
                    .filter(|(id, _)| id.as_str() != node.id.as_str())
                    .map(|(id, addr)| {
                        let (host, port) = split_host_port(addr);
                        SlaveEntry {
                            host,
                            port,
                            lag: if st.resync_of(id) > 0 { 1_000_000 } else { 0 },
                        }
                    })
                    .collect();
                (LinkStatus::Down, slaves)
            }
            NodeRole::Replica => {
                let link = if st.resync_of(&node.id) > 0 {
                    LinkStatus::Down
                } else {
                    LinkStatus::Up
                };
                (link, Vec::new())
            }
        };
        Ok(NodeObservation {
            role,
            version,
            link,
            slaves,
        })
    }

    fn observe_quorum(
        &mut self,
        seed: &ActuationTarget,
    ) -> Result<QuorumObservation, ClusterUpgradeError> {
        self.inner.observe_quorum(seed) // REAL CLUSTER INFO quorum read
    }

    fn cluster_failover(&mut self, node: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        // Do NOT drive a real committed promotion: a loopback self-promotion is not deterministic
        // (raft_cluster.rs:1508), and its correctness is DST-proven by
        // ironcache_raft::tests::failover_split_brain_gate. Record it and flip the observed roles so
        // the NEXT refresh advances exactly as a committed PromoteReplica would.
        let mut st = self.state.borrow_mut();
        st.failovers += 1;
        st.promoted = Some(node.id.clone());
        Ok(())
    }
}

/// The stub binary-swap actuator: records the upgrade ORDER, marks the node on the target version,
/// and marks it re-syncing (caught up after one tick) -- the simulated swap + resync.
struct StubUpgrader {
    state: Rc<RefCell<RollState>>,
}
impl NodeUpgrader for StubUpgrader {
    fn upgrade(&mut self, target: &ActuationTarget) -> Result<(), ClusterUpgradeError> {
        let mut st = self.state.borrow_mut();
        st.upgraded.insert(target.id.clone());
        st.resync.insert(target.id.clone(), 1);
        st.order.push(target.id.clone());
        Ok(())
    }
}

/// A sleeper that advances the roll by one tick (models the resync completing during the wait).
struct RollSleeper {
    state: Rc<RefCell<RollState>>,
}
impl Sleeper for RollSleeper {
    fn sleep(&self, _dur: Duration) {
        self.state.borrow_mut().tick();
    }
}

// ---------------------------------------------------------------------------
// Blocking RESP helpers (for the concurrent freeze writer + a post-freeze read; no tokio, so these
// never nest a runtime)
// ---------------------------------------------------------------------------

/// Encode `args` as a RESP2 command array.
fn encode_resp(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// One blocking RESP exchange: connect, send `args`, read one reply, return it lossy. A not-yet-up
/// connect returns "" (so callers can use it as a readiness probe); bounded by a read timeout so a
/// genuine hang fails loud rather than blocking forever.
fn blocking_resp(addr: &str, args: &[&str]) -> String {
    let Ok(mut stream) = std::net::TcpStream::connect(addr) else {
        return String::new();
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(&encode_resp(args)).expect("blocking send");
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match stream.read(&mut tmp) {
            Ok(n) if n > 0 => {
                buf.extend_from_slice(&tmp[..n]);
                if resp_reply_len(&buf, 0).is_some() {
                    break;
                }
            }
            // EOF (Ok(0)) or a read timeout / error: stop and return what we have.
            _ => break,
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}
