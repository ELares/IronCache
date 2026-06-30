// SPDX-License-Identifier: MIT OR Apache-2.0
//! #371 cross-shard `CLUSTER COUNTKEYSINSLOT` / `GETKEYSINSLOT` acceptance (SLOT_KEY_ENUMERATION.md,
//! slice 2): a single CLUSTER-ENABLED node with MULTIPLE internal shards, with keys placed (via a
//! shared `{hashtag}`) into ONE cluster slot but spread across the shards -- the client CRC16 slot
//! is the same for every `{t}i` (the hashtag rule), while the internal FNV `owner_shard` differs per
//! full key, so the slot's keys land on DIFFERENT shards. An honest count / key list must therefore
//! aggregate across shards: a home-shard-only answer would undercount. The server is driven over real
//! sockets, so the whole serve-loop rewrite -> whole-keyspace fan-out -> merge path is exercised.

use ironcache::test_support::run_cluster_node_for_test_shards;
use ironcache_config::{ClusterNode, ClusterTopology};
use ironcache_protocol::key_slot;
use ironcache_runtime::bootstrap::ShardSet;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ID: &str = "1111111111111111111111111111111111111111";

/// A single-node cluster that owns the whole slot space (so every slot is local; the test counts
/// keys this node physically holds, exactly what `COUNTKEYSINSLOT` reports).
fn one_node_topology(port: u16) -> ClusterTopology {
    ClusterTopology {
        nodes: vec![ClusterNode {
            id: ID.to_owned(),
            host: "127.0.0.1".to_owned(),
            port,
            slots: vec![[0, 16383]],
        }],
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("cluster node never came up on port {port}");
}

/// Send a RESP2 array command from string parts and read the reply (read until a short quiet window;
/// the replies here are small but an array can exceed one segment).
async fn cmd(c: &mut TcpStream, parts: &[&str]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut frame = format!("*{}\r\n", parts.len());
    for p in parts {
        let _ = write!(frame, "${}\r\n{}\r\n", p.len(), p);
    }
    c.write_all(frame.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        match tokio::time::timeout(Duration::from_millis(200), c.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                buf.extend_from_slice(&tmp[..n]);
                if n < tmp.len() {
                    break;
                }
            }
            _ => break,
        }
    }
    buf
}

/// Parse a RESP integer reply `:N\r\n`.
fn as_integer(reply: &[u8]) -> i64 {
    let s = std::str::from_utf8(reply).expect("utf8 reply");
    let line = s.lines().next().expect("a reply line");
    assert!(
        line.starts_with(':'),
        "expected an integer reply, got: {s:?}"
    );
    line[1..].trim().parse().expect("integer body")
}

/// The element count of a RESP array reply `*N\r\n...` (the header count).
fn array_len(reply: &[u8]) -> i64 {
    let s = std::str::from_utf8(reply).expect("utf8 reply");
    let line = s.lines().next().expect("a reply line");
    assert!(line.starts_with('*'), "expected an array reply, got: {s:?}");
    line[1..].trim().parse().expect("array length")
}

fn with_runtime<F: std::future::Future<Output = ()>>(body: F) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, body);
}

/// Seed `n` keys that all hash to ONE slot (shared `{tag}`) but spread across internal shards, plus
/// a few keys in a SECOND slot. Returns `(slot_a, slot_b)` (asserted distinct).
async fn seed(c: &mut TcpStream, tag_a: &str, n_a: usize, tag_b: &str, n_b: usize) -> (u16, u16) {
    let slot_a = key_slot(format!("{{{tag_a}}}0").as_bytes());
    let slot_b = key_slot(format!("{{{tag_b}}}0").as_bytes());
    assert_ne!(slot_a, slot_b, "the two tags must occupy distinct slots");
    for i in 0..n_a {
        let k = format!("{{{tag_a}}}{i}");
        let r = cmd(c, &["SET", &k, "v"]).await;
        assert_eq!(&r, b"+OK\r\n", "SET {k}");
    }
    for i in 0..n_b {
        let k = format!("{{{tag_b}}}{i}");
        cmd(c, &["SET", &k, "v"]).await;
    }
    (slot_a, slot_b)
}

/// An in-range slot guaranteed to hold no seeded key (neither `a` nor `b`).
fn empty_slot(a: u16, b: u16) -> u16 {
    (0u16..16384).find(|&s| s != a && s != b).unwrap()
}

#[test]
fn countkeysinslot_aggregates_the_count_across_internal_shards() {
    with_runtime(async {
        let port = free_port();
        // 4 shards so a single slot's keys genuinely span shards; a home-only count would undercount.
        let _node: ShardSet =
            run_cluster_node_for_test_shards(port, 4, one_node_topology(port), ID);
        let mut c = connect_retry(port).await;

        let (slot_a, slot_b) = seed(&mut c, "alpha", 12, "beta", 5).await;

        // The aggregate count is the FULL 12 (not the ~3 a single home shard would hold).
        let r = cmd(&mut c, &["CLUSTER", "COUNTKEYSINSLOT", &slot_a.to_string()]).await;
        assert_eq!(
            as_integer(&r),
            12,
            "slot_a count must aggregate across shards"
        );

        let r = cmd(&mut c, &["CLUSTER", "COUNTKEYSINSLOT", &slot_b.to_string()]).await;
        assert_eq!(as_integer(&r), 5, "slot_b count");

        // A slot with no keys is 0.
        let empty = empty_slot(slot_a, slot_b);
        let r = cmd(&mut c, &["CLUSTER", "COUNTKEYSINSLOT", &empty.to_string()]).await;
        assert_eq!(as_integer(&r), 0, "an empty slot counts 0");
    });
}

#[test]
fn getkeysinslot_returns_the_bounded_keys_across_internal_shards() {
    with_runtime(async {
        let port = free_port();
        let _node: ShardSet =
            run_cluster_node_for_test_shards(port, 4, one_node_topology(port), ID);
        let mut c = connect_retry(port).await;

        let (slot_a, slot_b) = seed(&mut c, "alpha", 12, "beta", 5).await;

        // A generous count returns ALL 12 keys in slot_a (aggregated across shards).
        let r = cmd(
            &mut c,
            &["CLUSTER", "GETKEYSINSLOT", &slot_a.to_string(), "100"],
        )
        .await;
        assert_eq!(array_len(&r), 12, "all of slot_a's keys, across shards");

        // The count BOUNDS the union (not 4x the per-shard cap): exactly 3.
        let r = cmd(
            &mut c,
            &["CLUSTER", "GETKEYSINSLOT", &slot_a.to_string(), "3"],
        )
        .await;
        assert_eq!(
            array_len(&r),
            3,
            "the count truncates the cross-shard union"
        );

        // slot_b independently returns its 5.
        let r = cmd(
            &mut c,
            &["CLUSTER", "GETKEYSINSLOT", &slot_b.to_string(), "100"],
        )
        .await;
        assert_eq!(array_len(&r), 5, "slot_b's keys");

        // An empty slot yields an empty array.
        let empty = empty_slot(slot_a, slot_b);
        let r = cmd(
            &mut c,
            &["CLUSTER", "GETKEYSINSLOT", &empty.to_string(), "100"],
        )
        .await;
        assert_eq!(array_len(&r), 0, "an empty slot yields no keys");
    });
}

#[test]
fn a_malformed_slot_scan_falls_through_to_the_exact_cluster_error() {
    // A malformed slot-scan must NOT be fanned out; it falls through to the normal CLUSTER home
    // path, which returns the EXACT Redis error (the serve-loop gate is Some only for valid ones).
    with_runtime(async {
        let port = free_port();
        let _node: ShardSet =
            run_cluster_node_for_test_shards(port, 4, one_node_topology(port), ID);
        let mut c = connect_retry(port).await;

        let err = |r: &[u8], what: &str| {
            assert!(
                r.starts_with(b"-"),
                "{what}: expected an error reply, got {:?}",
                String::from_utf8_lossy(r)
            );
        };
        // Non-integer slot, out-of-range slot, negative GET count, and missing slot (wrong arity)
        // each error rather than fan out.
        err(
            &cmd(&mut c, &["CLUSTER", "COUNTKEYSINSLOT", "notaslot"]).await,
            "non-integer slot",
        );
        err(
            &cmd(&mut c, &["CLUSTER", "COUNTKEYSINSLOT", "99999"]).await,
            "out-of-range slot",
        );
        err(
            &cmd(&mut c, &["CLUSTER", "GETKEYSINSLOT", "0", "-1"]).await,
            "negative count",
        );
        err(
            &cmd(&mut c, &["CLUSTER", "COUNTKEYSINSLOT"]).await,
            "missing slot (wrong arity)",
        );
    });
}
