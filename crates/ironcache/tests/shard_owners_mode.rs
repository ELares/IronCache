// SPDX-License-Identifier: MIT OR Apache-2.0
//! #517 shard-owners acceptance.
//!
//! PR2: a node booted with `cluster_mode = shard-owners` must boot into a SERVING cluster (all 16384
//! slots assigned, `cluster_state:ok`, keys served immediately -- no `CLUSTER ADDSLOTS`). Guards the
//! "cluster-enabled but zero slots -> CLUSTERDOWN" trap: enabling the perf mode must NOT turn a
//! working cache into a non-serving node.
//!
//! PR3: the mode binds ONE listener PER shard at `base + i`, each homing its connections on shard
//! `i` (asserted via per-port liveness).
//!
//! PR4 (the hop-elimination milestone): the N shards are projected as N cluster nodes, node `i` at
//! `base + i` owning the contiguous slot range `slot_to_shard` assigns it. A cluster-aware client
//! that dials a key's owner port is served LOCALLY (no MOVED; internally owner == home so no hop); a
//! mis-routed key gets `-MOVED <slot> host:owner_port`, so clients converge to zero hops.

use ironcache::test_support::run_shard_owners_node_for_test;
use std::collections::BTreeSet;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            s.set_nodelay(true).unwrap();
            return s;
        }
        tokio::task::yield_now().await;
    }
    panic!("could not connect to the shard-owners test node on {port}");
}

fn cmd(args: &[&str]) -> String {
    use std::fmt::Write as _;
    let mut f = format!("*{}\r\n", args.len());
    for a in args {
        write!(f, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    f
}

/// Read one reply's bytes (a single read; the small replies here fit one packet).
async fn send_read(c: &mut TcpStream, frame: &str) -> Vec<u8> {
    c.write_all(frame.as_bytes()).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = c.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// The contiguous partition the router + projection share: slot -> owning shard.
fn owner_shard(key: &str, n: usize) -> usize {
    let slot = ironcache_protocol::key_slot(key.as_bytes()) as usize;
    (slot * n) / 16384
}

#[tokio::test(flavor = "current_thread")]
async fn shard_owners_mode_owns_all_slots_and_serves_keys() {
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);
    let mut c = connect_retry(base).await;

    // CLUSTER INFO must report a HEALTHY cluster with ALL 16384 slots assigned (across the N shard
    // owners), NOT the zero-slot `cluster_state:fail` a bare cluster-enabled node shows before
    // ADDSLOTS. This is the boot-serving guard against the CLUSTERDOWN trap.
    let info = send_read(&mut c, &cmd(&["CLUSTER", "INFO"])).await;
    let info_s = String::from_utf8_lossy(&info);
    assert!(
        info_s.contains("cluster_state:ok"),
        "shard-owners node must be cluster_state:ok, got:\n{info_s}"
    );
    assert!(
        info_s.contains("cluster_slots_assigned:16384"),
        "shard-owners node must have all 16384 slots assigned, got:\n{info_s}"
    );

    // And it SERVES keys immediately (no ADDSLOTS): a key OWNED by shard 0 (the `base` port) round
    // trips locally. (Shard 0 only owns its slot range; a foreign key would MOVED -- covered by the
    // dedicated MOVED test -- so pick a key `base` actually owns.)
    let key = (0..100_000)
        .map(|i| format!("k{i}"))
        .find(|k| owner_shard(k, N) == 0)
        .expect("some key in the first 100k hashes to shard 0");
    let set = send_read(&mut c, &cmd(&["SET", &key, "hello"])).await;
    assert_eq!(
        &set, b"+OK\r\n",
        "SET of a shard-0 key must serve locally on base"
    );
    let get = send_read(&mut c, &cmd(&["GET", &key])).await;
    assert_eq!(&get, b"$5\r\nhello\r\n", "GET must return the value");
}

#[tokio::test(flavor = "current_thread")]
async fn shard_owners_binds_one_serving_listener_per_shard() {
    // PR3: 4 shards -> the node binds 4 listeners (base .. base + 3). The helper reserves a
    // CONTIGUOUS free block and retries on a bind race, so this is robust under parallel test load.
    const SHARDS: u16 = 4;
    let (_node, base) = run_shard_owners_node_for_test(SHARDS as usize);

    // EVERY per-shard port must be independently BOUND and responsive (a connection to `base + i`
    // homes on shard `i`). Liveness via PING per port -- a missing listener fails to connect, a
    // broken one fails PING. (Each port only SERVES the keys it OWNS and MOVEDs the rest, so a blind
    // SET is not a valid liveness probe; the owns/MOVED semantics are covered below.)
    for i in 0..SHARDS {
        let port = base + i;
        let mut c = connect_retry(port).await;
        let pong = send_read(&mut c, &cmd(&["PING"])).await;
        assert_eq!(
            &pong, b"+PONG\r\n",
            "per-shard port {port} (shard {i}) must be bound and responsive"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owner_port_serves_locally_and_a_misrouted_key_gets_moved() {
    // PR4 (the milestone): a cluster-aware client that dials a key's OWNER port is served LOCALLY
    // (no MOVED, and internally no cross-shard hop -- owner_shard == home shard). A client that dials
    // the WRONG port gets a -MOVED to the owner's port, so it converges to zero hops.
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);

    for key in [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
    ] {
        let owner = owner_shard(key, N);
        let owner_port = base + owner as u16;

        // OWNER port -> served locally, no MOVED.
        let mut c = connect_retry(owner_port).await;
        let set = send_read(&mut c, &cmd(&["SET", key, "v"])).await;
        assert_eq!(
            &set, b"+OK\r\n",
            "the owner port {owner_port} must serve {key} locally (no MOVED)"
        );
        let get = send_read(&mut c, &cmd(&["GET", key])).await;
        assert_eq!(&get, b"$1\r\nv\r\n", "owner port must return {key}'s value");

        // A NON-owner port -> MOVED to the owner's port.
        let wrong_port = base + ((owner as u16 + 1) % N as u16);
        assert_ne!(wrong_port, owner_port, "test bug: wrong port equals owner");
        let mut c2 = connect_retry(wrong_port).await;
        let r = send_read(&mut c2, &cmd(&["GET", key])).await;
        let rs = String::from_utf8_lossy(&r);
        assert!(
            rs.starts_with("-MOVED"),
            "non-owner port {wrong_port} must MOVED {key}, got: {rs}"
        );
        assert!(
            rs.trim_end().ends_with(&format!(":{owner_port}")),
            "the MOVED for {key} must point to the owner port {owner_port}, got: {rs}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_info_reflects_the_n_shard_projection() {
    // The projection makes the node a healthy N-node cluster (was 1 node owning all slots pre-PR4).
    let (_node, base) = run_shard_owners_node_for_test(4);
    let mut c = connect_retry(base).await;
    let info =
        String::from_utf8_lossy(&send_read(&mut c, &cmd(&["CLUSTER", "INFO"])).await).into_owned();
    for want in [
        "cluster_state:ok",
        "cluster_slots_assigned:16384",
        "cluster_known_nodes:4",
        "cluster_size:4",
    ] {
        assert!(
            info.contains(want),
            "CLUSTER INFO must contain `{want}`, got:\n{info}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn n_equals_one_serves_every_key_locally_without_moved() {
    // N=1: a single owner of all 16384 slots -> every key is local, never MOVED (byte-identical to
    // the pre-projection single-node build).
    let (_node, base) = run_shard_owners_node_for_test(1);
    let mut c = connect_retry(base).await;
    for key in ["a", "b", "c", "zzz", "{tag}.x"] {
        let r = send_read(&mut c, &cmd(&["SET", key, "1"])).await;
        assert_eq!(
            &r,
            b"+OK\r\n",
            "N=1 must serve {key} locally with no MOVED, got: {}",
            String::from_utf8_lossy(&r)
        );
    }
}

// ---------------------------------------------------------------------------
// #526: whole-keyspace commands (KEYS / SCAN / DBSIZE / RANDOMKEY / FLUSHDB / FLUSHALL) are
// SCOPED to the connecting shard in shard-owners mode. Each per-shard port must report ONLY
// the keys IT owns (the per-node Redis Cluster view), never the global fan-out -- so a per-node
// aggregator sums the N ports to the true total instead of over-counting by N.
// ---------------------------------------------------------------------------

/// One parsed RESP reply (only the shapes these tests exercise).
#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

/// Parse ONE complete RESP value from the front of `buf`, returning `(value, consumed)` or
/// `None` if the buffer does not yet hold a whole value (the caller reads more and retries).
fn parse_resp(buf: &[u8]) -> Option<(Resp, usize)> {
    fn crlf(b: &[u8], from: usize) -> Option<usize> {
        let mut i = from;
        while i + 1 < b.len() {
            if b[i] == b'\r' && b[i + 1] == b'\n' {
                return Some(i);
            }
            i += 1;
        }
        None
    }
    let kind = *buf.first()?;
    let hdr_end = crlf(buf, 1)?;
    let hdr = std::str::from_utf8(&buf[1..hdr_end]).ok()?;
    let after = hdr_end + 2;
    match kind {
        b'+' => Some((Resp::Simple(hdr.to_owned()), after)),
        b'-' => Some((Resp::Error(hdr.to_owned()), after)),
        b':' => Some((Resp::Int(hdr.parse().ok()?), after)),
        b'$' => {
            let n: i64 = hdr.parse().ok()?;
            if n < 0 {
                return Some((Resp::Bulk(None), after));
            }
            let n = n as usize;
            let end = after + n + 2;
            if buf.len() < end {
                return None;
            }
            Some((Resp::Bulk(Some(buf[after..after + n].to_vec())), end))
        }
        b'*' => {
            let n: i64 = hdr.parse().ok()?;
            if n < 0 {
                return Some((Resp::Array(None), after));
            }
            let mut items = Vec::with_capacity(n as usize);
            let mut pos = after;
            for _ in 0..n {
                let (v, c) = parse_resp(&buf[pos..])?;
                items.push(v);
                pos += c;
            }
            Some((Resp::Array(Some(items)), pos))
        }
        _ => None,
    }
}

/// Send one command and read back one COMPLETE RESP reply (reads until it parses, so a large
/// KEYS array or a multi-packet reply is assembled correctly -- `send_read`'s single read is
/// not enough for the whole-keyspace replies here).
async fn request(c: &mut TcpStream, args: &[&str]) -> Resp {
    c.write_all(cmd(args).as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        if let Some((v, _)) = parse_resp(&buf) {
            return v;
        }
        let n = c.read(&mut tmp).await.unwrap();
        assert!(
            n > 0,
            "connection closed mid-reply (have {} bytes)",
            buf.len()
        );
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn resp_key(v: &Resp) -> String {
    match v {
        Resp::Bulk(Some(b)) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("expected a bulk-string key, got {other:?}"),
    }
}

/// SET each of `keys` on its OWNER port (a non-owner port would MOVED), returning the keys
/// grouped by owning shard. Panics if any shard would own zero keys, so the per-port
/// assertions below always exercise a non-empty shard.
async fn seed_by_owner(base: u16, n: usize, keys: &[String]) -> Vec<Vec<String>> {
    let mut by_shard: Vec<Vec<String>> = vec![Vec::new(); n];
    for k in keys {
        by_shard[owner_shard(k, n)].push(k.clone());
    }
    for (shard, ks) in by_shard.iter().enumerate() {
        assert!(
            !ks.is_empty(),
            "test setup needs shard {shard} to own at least one key"
        );
        let mut c = connect_retry(base + shard as u16).await;
        for k in ks {
            assert_eq!(
                request(&mut c, &["SET", k, "v"]).await,
                Resp::Simple("OK".to_owned()),
                "SET of owner-{shard} key {k} on its owner port must succeed"
            );
        }
    }
    by_shard
}

fn sample_keys(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("key:{i}")).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn dbsize_is_per_shard_and_the_per_port_sum_is_the_true_total() {
    // #526: DBSIZE on shard i's port returns ONLY shard i's key count; the sum over the N ports
    // equals the true total (each port distinct), so a per-node aggregator no longer multiplies
    // the count by N.
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);
    let keys = sample_keys(240);
    let by_shard = seed_by_owner(base, N, &keys).await;

    let mut sum = 0i64;
    for i in 0..N {
        let mut c = connect_retry(base + i as u16).await;
        let Resp::Int(count) = request(&mut c, &["DBSIZE"]).await else {
            panic!("DBSIZE must reply an integer");
        };
        assert_eq!(
            count,
            by_shard[i].len() as i64,
            "DBSIZE on shard {i}'s port must return only shard {i}'s count, not the global total"
        );
        sum += count;
    }
    assert_eq!(
        sum as usize,
        keys.len(),
        "the per-port DBSIZE sum must equal the true total (no over-count by N)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn keys_per_port_are_disjoint_and_their_union_is_the_keyspace() {
    // #526: KEYS * on shard i's port returns EXACTLY shard i's keys; the ports are disjoint and
    // their union is the whole keyspace with each key appearing exactly once (no N copies).
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);
    let keys = sample_keys(240);
    let by_shard = seed_by_owner(base, N, &keys).await;

    let mut union: BTreeSet<String> = BTreeSet::new();
    for i in 0..N {
        let mut c = connect_retry(base + i as u16).await;
        let Resp::Array(Some(items)) = request(&mut c, &["KEYS", "*"]).await else {
            panic!("KEYS must reply an array");
        };
        let got: BTreeSet<String> = items.iter().map(resp_key).collect();
        let want: BTreeSet<String> = by_shard[i].iter().cloned().collect();
        assert_eq!(
            got, want,
            "KEYS * on shard {i}'s port must return exactly shard {i}'s keys"
        );
        for k in &got {
            assert!(
                union.insert(k.clone()),
                "key {k} was reported by more than one port (a double-count)"
            );
        }
    }
    assert_eq!(
        union,
        keys.iter().cloned().collect::<BTreeSet<String>>(),
        "the union of the per-port KEYS must be the whole keyspace, each key exactly once"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scan_per_port_enumerates_only_that_ports_keys_and_terminates() {
    // #526: SCAN on shard i's port walks ONLY shard i's slice and terminates at cursor 0 (it does
    // not advance into a sibling shard), so the enumerated set equals shard i's keys.
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);
    let keys = sample_keys(240);
    let by_shard = seed_by_owner(base, N, &keys).await;

    for i in 0..N {
        let mut c = connect_retry(base + i as u16).await;
        let mut cursor = "0".to_owned();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut iters = 0;
        loop {
            let Resp::Array(Some(items)) = request(&mut c, &["SCAN", &cursor, "COUNT", "16"]).await
            else {
                panic!("SCAN must reply an array");
            };
            assert_eq!(items.len(), 2, "SCAN reply must be [cursor, keys]");
            cursor = resp_key(&items[0]);
            let Resp::Array(Some(batch)) = &items[1] else {
                panic!("SCAN's second element must be the key array");
            };
            for v in batch {
                seen.insert(resp_key(v));
            }
            iters += 1;
            assert!(iters < 10_000, "SCAN on shard {i}'s port did not terminate");
            if cursor == "0" {
                break;
            }
        }
        let want: BTreeSet<String> = by_shard[i].iter().cloned().collect();
        assert_eq!(
            seen, want,
            "SCAN on shard {i}'s port must enumerate exactly shard {i}'s keys and terminate"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn randomkey_per_port_returns_a_key_owned_by_that_shard() {
    // #526: RANDOMKEY on shard i's port samples ONLY shard i's slice (node-local), never a sibling
    // shard's key.
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);
    let keys = sample_keys(240);
    let _ = seed_by_owner(base, N, &keys).await;

    for i in 0..N {
        let mut c = connect_retry(base + i as u16).await;
        for _ in 0..8 {
            let key = match request(&mut c, &["RANDOMKEY"]).await {
                Resp::Bulk(Some(b)) => String::from_utf8(b).unwrap(),
                other => {
                    panic!("RANDOMKEY on a non-empty shard {i} must return a key, got {other:?}")
                }
            };
            assert_eq!(
                owner_shard(&key, N),
                i,
                "RANDOMKEY on shard {i}'s port returned {key}, which shard {} owns",
                owner_shard(&key, N)
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn flushdb_on_one_port_leaves_the_other_shards_intact() {
    // #526: FLUSHDB on shard 0's port clears ONLY shard 0's slice (each cluster node flushes its
    // own slots); the other shards' keys survive.
    const N: usize = 4;
    let (_node, base) = run_shard_owners_node_for_test(N);
    let keys = sample_keys(240);
    let by_shard = seed_by_owner(base, N, &keys).await;

    let mut c0 = connect_retry(base).await;
    assert_eq!(
        request(&mut c0, &["FLUSHDB"]).await,
        Resp::Simple("OK".to_owned()),
        "FLUSHDB must succeed"
    );
    assert_eq!(
        request(&mut c0, &["DBSIZE"]).await,
        Resp::Int(0),
        "FLUSHDB must clear the connecting shard's slice"
    );
    for i in 1..N {
        let mut c = connect_retry(base + i as u16).await;
        assert_eq!(
            request(&mut c, &["DBSIZE"]).await,
            Resp::Int(by_shard[i].len() as i64),
            "FLUSHDB on shard 0's port must NOT touch shard {i}"
        );
    }
}
