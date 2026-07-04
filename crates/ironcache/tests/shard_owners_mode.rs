// SPDX-License-Identifier: MIT OR Apache-2.0
//! #517 PR2 acceptance: a node booted with `cluster_mode = shard-owners` (multiple internal shards,
//! no static topology) must boot into a SERVING single-node cluster -- it auto-owns all 16384 slots,
//! so `CLUSTER INFO` is `cluster_state:ok` and keys are served immediately (no `CLUSTER ADDSLOTS`).
//! This guards against the "cluster-enabled but zero slots -> CLUSTERDOWN" trap: enabling the perf
//! mode must NOT turn a working cache into a non-serving node. (The per-shard endpoints + the
//! N-shard projection that actually eliminate the internal hop are later PRs; here we only assert
//! the mode is a correct, serving single-node cluster.)

use ironcache::test_support::run_shard_owners_node_for_test;
use std::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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

#[tokio::test(flavor = "current_thread")]
async fn shard_owners_mode_owns_all_slots_and_serves_keys() {
    let port = free_port();
    let _node = run_shard_owners_node_for_test(port, 4);
    let mut c = connect_retry(port).await;

    // CLUSTER INFO must report a HEALTHY cluster (all 16384 slots owned by this single node), NOT
    // the zero-slot `cluster_state:fail` a bare cluster-enabled node would show before ADDSLOTS.
    let info = send_read(&mut c, &cmd(&["CLUSTER", "INFO"])).await;
    let info_s = String::from_utf8_lossy(&info);
    assert!(
        info_s.contains("cluster_state:ok"),
        "shard-owners node must own its slots and be cluster_state:ok, got:\n{info_s}"
    );
    assert!(
        info_s.contains("cluster_slots_assigned:16384"),
        "shard-owners node must own all 16384 slots, got:\n{info_s}"
    );

    // And it SERVES keys immediately (no ADDSLOTS): SET then GET round-trips.
    let set = send_read(&mut c, &cmd(&["SET", "k", "hello"])).await;
    assert_eq!(&set, b"+OK\r\n", "SET must succeed on the serving node");
    let get = send_read(&mut c, &cmd(&["GET", "k"])).await;
    assert_eq!(&get, b"$5\r\nhello\r\n", "GET must return the value");
}
