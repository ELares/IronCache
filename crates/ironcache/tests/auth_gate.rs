// SPDX-License-Identifier: MIT OR Apache-2.0
//! AUTH-gate regression tests for the production security fix (the hoisted NOAUTH chokepoint).
//!
//! A production-readiness audit found the NOAUTH gate lived DOWNSTREAM in `dispatch_inner`, so
//! every router path that intercepts a command and RETURNS before reaching it was BYPASSED. With
//! `requirepass` set and `shards > 1` (the default multi-shard deployment) an UNAUTHENTICATED
//! client could (a) read/write any FOREIGN-shard key (the cross-shard coordinator fan-out routes +
//! executes without re-checking auth), (b) run the whole-keyspace fan-outs (KEYS/SCAN/FLUSHALL),
//! and (c) run the CLUSTER topology mutators (MEET/FORGET/ADDSLOTS/SETSLOT/DELSLOTS/REPLICATE) to
//! take over or WIPE the cluster.
//!
//! The fix HOISTS the gate to the single chokepoint at the TOP of `route_and_dispatch`, so EVERY
//! path (cross-shard hop, whole-keyspace fan-out, CLUSTER mutator, persistence, SHUTDOWN, MULTI
//! queue) is gated once before any interception/fan-out/dispatch. These tests boot the REAL
//! multi-shard server over real sockets and assert each of those paths returns `-NOAUTH` while
//! unauthenticated, then works (or returns its normal non-auth error) after `AUTH <pass>`.

use ironcache::test_support::{run_cluster_node_with_auth_for_test, run_server_with_auth_for_test};
use ironcache_config::{ClusterNode, ClusterTopology};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const PASS: &str = "s3cr3t";
const NOAUTH: &[u8] = b"-NOAUTH Authentication required.\r\n";

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Connect with a few short retries (the shards bind asynchronously after `run_server`).
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on port {port}");
}

/// Encode a RESP2 command array from string args.
fn encode_args(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// Send one command (string args), read ONE socket read of reply, return the raw bytes. The
/// replies here are small (a NOAUTH line, +OK, an integer, a short error), so a single read
/// captures the whole reply.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

/// AUTH with the configured password; assert +OK.
async fn auth_ok(client: &mut TcpStream) {
    assert_eq!(cmd(client, &["AUTH", PASS]).await, b"+OK\r\n");
}

// Node ids must be 40 lowercase hex characters (the cluster-topology validator).
const NODE1: &str = "1111111111111111111111111111111111111111";
const NODE2: &str = "2222222222222222222222222222222222222222";

/// A two-node static cluster topology: NODE1 owns [0, 8191], NODE2 owns [8192, 16383].
fn two_node_topology(port1: u16, port2: u16) -> ClusterTopology {
    ClusterTopology {
        nodes: vec![
            ClusterNode {
                id: NODE1.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: port1,
                slots: vec![[0, 8191]],
            },
            ClusterNode {
                id: NODE2.to_owned(),
                host: "127.0.0.1".to_owned(),
                port: port2,
                slots: vec![[8192, 16383]],
            },
        ],
    }
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    (rt, local)
}

/// (a) CROSS-SHARD KEYED PATH: a GET/SET on a key that hashes to a FOREIGN shard must be NOAUTH
/// while unauthenticated. We do not know the connection's home shard (SO_REUSEPORT picks it), so we
/// sweep MANY keys spanning every shard: with 4 shards + FNV routing some are necessarily foreign,
/// and EVERY one must reply NOAUTH (the bypass would have served the foreign-shard key). After AUTH
/// the same SET/GET succeed end-to-end.
#[test]
fn cross_shard_keyed_is_noauth_until_authenticated() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_with_auth_for_test(port, 4, PASS);
        let mut c = connect_retry(port).await;

        // Unauthenticated: every keyed command (home OR foreign shard) -> NOAUTH.
        for i in 0..64 {
            let key = format!("xshard:{i}");
            assert_eq!(
                cmd(&mut c, &["SET", &key, "v"]).await,
                NOAUTH,
                "unauth SET {key} must be NOAUTH (cross-shard bypass)"
            );
            assert_eq!(
                cmd(&mut c, &["GET", &key]).await,
                NOAUTH,
                "unauth GET {key} must be NOAUTH (cross-shard bypass)"
            );
        }

        // After AUTH the foreign-shard round-trip works.
        auth_ok(&mut c).await;
        for i in 0..64 {
            let key = format!("xshard:{i}");
            let val = format!("v{i}");
            assert_eq!(cmd(&mut c, &["SET", &key, &val]).await, b"+OK\r\n");
            assert_eq!(
                cmd(&mut c, &["GET", &key]).await,
                format!("${}\r\n{val}\r\n", val.len()).into_bytes(),
                "authed foreign-shard GET {key} must round-trip"
            );
        }

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (b) WHOLE-KEYSPACE FAN-OUT: KEYS / SCAN / FLUSHALL must be NOAUTH while unauthenticated, then
/// work after AUTH. These broadcast to every shard on the home core; the bypass let an anonymous
/// client enumerate or WIPE the keyspace.
#[test]
fn whole_keyspace_fan_out_is_noauth_until_authenticated() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_with_auth_for_test(port, 4, PASS);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["KEYS", "*"]).await, NOAUTH, "unauth KEYS");
        assert_eq!(cmd(&mut c, &["SCAN", "0"]).await, NOAUTH, "unauth SCAN");
        assert_eq!(cmd(&mut c, &["FLUSHALL"]).await, NOAUTH, "unauth FLUSHALL");
        assert_eq!(cmd(&mut c, &["DBSIZE"]).await, NOAUTH, "unauth DBSIZE");

        // After AUTH these run (KEYS over an empty keyspace -> empty array; FLUSHALL -> +OK).
        auth_ok(&mut c).await;
        assert_eq!(cmd(&mut c, &["KEYS", "*"]).await, b"*0\r\n", "authed KEYS");
        assert_eq!(
            cmd(&mut c, &["FLUSHALL"]).await,
            b"+OK\r\n",
            "authed FLUSHALL"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (c) CLUSTER TOPOLOGY MUTATORS: each of MEET/FORGET/ADDSLOTS/SETSLOT/DELSLOTS/REPLICATE must be
/// NOAUTH while unauthenticated on a CLUSTER node with requirepass. The bypass let an anonymous
/// client take over or WIPE the slot map. After AUTH the same mutators reach `cmd_cluster` (their
/// normal, non-NOAUTH behavior), proving the gate -- not a blanket block -- was what changed.
#[test]
fn cluster_mutators_are_noauth_until_authenticated() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port1 = free_port();
        let port2 = free_port();
        let topo = two_node_topology(port1, port2);
        let server = run_cluster_node_with_auth_for_test(port1, 2, topo, NODE1, PASS);
        let mut c = connect_retry(port1).await;

        // Each mutator, unauthenticated, must be NOAUTH (never reaching the slot-map mutation).
        let mutators: Vec<Vec<&str>> = vec![
            vec!["CLUSTER", "MEET", "127.0.0.1", "7777"],
            vec!["CLUSTER", "FORGET", NODE2],
            vec!["CLUSTER", "ADDSLOTS", "100"],
            vec!["CLUSTER", "DELSLOTS", "100"],
            vec!["CLUSTER", "SETSLOT", "100", "NODE", NODE2],
            vec!["CLUSTER", "REPLICATE", NODE2],
            vec!["CLUSTER", "SET-CONFIG-EPOCH", "5"],
        ];
        for m in &mutators {
            assert_eq!(
                cmd(&mut c, m).await,
                NOAUTH,
                "unauth {m:?} must be NOAUTH (CLUSTER-mutator bypass)"
            );
        }

        // After AUTH a mutator reaches cmd_cluster: it no longer replies NOAUTH (its reply is
        // either +OK or a cluster-specific error, but NOT the auth error). We only assert it is
        // NOT NOAUTH, since the exact cluster reply is covered by the cluster test suites.
        auth_ok(&mut c).await;
        let reply = cmd(&mut c, &["CLUSTER", "ADDSLOTS", "200"]).await;
        assert_ne!(reply, NOAUTH, "authed CLUSTER ADDSLOTS must not be NOAUTH");

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (d) PERSISTENCE + SHUTDOWN: SAVE/BGSAVE/LASTSAVE and SHUTDOWN must be NOAUTH while
/// unauthenticated (the hoisted gate now covers them; the old inline point-fixes were removed). We
/// use a server WITHOUT a data_dir, so persistence is OFF: these commands used to reach dispatch
/// (gated there). The hoisted gate now blocks them in the router uniformly. SHUTDOWN must NOT exit
/// the process while unauthenticated.
#[test]
fn persistence_and_shutdown_are_noauth_until_authenticated() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_with_auth_for_test(port, 4, PASS);
        let mut c = connect_retry(port).await;

        for form in &[
            vec!["SAVE"],
            vec!["BGSAVE"],
            vec!["LASTSAVE"],
            vec!["SHUTDOWN"],
            vec!["SHUTDOWN", "NOSAVE"],
        ] {
            assert_eq!(
                cmd(&mut c, form).await,
                NOAUTH,
                "unauth {form:?} must be NOAUTH"
            );
        }

        // The server is still up (the unauthenticated SHUTDOWN did NOT exit it): AUTH then PING.
        auth_ok(&mut c).await;
        assert_eq!(
            cmd(&mut c, &["PING"]).await,
            b"+PONG\r\n",
            "server still up"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (e) MULTI: a command issued INSIDE a MULTI from an unauthenticated client must be rejected
/// (NOAUTH), not queued+executed. Since MULTI itself is not in the pre-auth allow-list, an unauth
/// MULTI is already NOAUTH; this asserts the whole transaction entry is gated (Redis parity: an
/// unauth client cannot open a transaction nor stage commands inside one).
#[test]
fn multi_from_unauth_client_is_noauth() {
    let (r, local) = rt();
    local.block_on(&r, async {
        // ONE shard so the post-auth queued SET is home-owned (every key is local) and yields a
        // clean +QUEUED; the unauth NOAUTH assertions below are the actual regression guard and
        // hold for any shard count.
        let port = free_port();
        let server = run_server_with_auth_for_test(port, 1, PASS);
        let mut c = connect_retry(port).await;

        // MULTI before auth -> NOAUTH (cannot open a transaction).
        assert_eq!(cmd(&mut c, &["MULTI"]).await, NOAUTH, "unauth MULTI");
        // A would-be queued command before auth -> NOAUTH (not +QUEUED).
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v"]).await,
            NOAUTH,
            "unauth command (would-be queued) must be NOAUTH, not +QUEUED"
        );

        // After AUTH the transaction path works: MULTI -> +OK, SET -> +QUEUED, EXEC -> array.
        auth_ok(&mut c).await;
        assert_eq!(cmd(&mut c, &["MULTI"]).await, b"+OK\r\n", "authed MULTI");
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v"]).await,
            b"+QUEUED\r\n",
            "authed SET inside MULTI must queue"
        );
        let exec = cmd(&mut c, &["EXEC"]).await;
        assert_eq!(exec, b"*1\r\n+OK\r\n", "authed EXEC applies the queued SET");

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (f) PRE-AUTH ALLOW-LIST UNCHANGED: AUTH/HELLO/RESET still work unauthenticated (the allow-list
/// is identical to before the hoist). PING is NOT in the allow-list, so it stays NOAUTH unauth --
/// exactly today's policy (this asserts we did not widen or narrow the allow-list).
#[test]
fn pre_auth_allow_list_is_unchanged() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_with_auth_for_test(port, 4, PASS);
        let mut c = connect_retry(port).await;

        // PING is NOT pre-auth allowed today -> NOAUTH (do not change this policy).
        assert_eq!(cmd(&mut c, &["PING"]).await, NOAUTH, "PING is not pre-auth");
        // RESET is pre-auth allowed -> +RESET.
        assert_eq!(
            cmd(&mut c, &["RESET"]).await,
            b"+RESET\r\n",
            "RESET pre-auth"
        );
        // AUTH succeeds pre-auth.
        auth_ok(&mut c).await;
        // After AUTH, PING works.
        assert_eq!(cmd(&mut c, &["PING"]).await, b"+PONG\r\n", "authed PING");

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (g) SINGLE-SHARD + requirepass: the gate must still fire on the shards == 1 path (a regression
/// guard for the single-shard router, where there is no cross-shard fork at all).
#[test]
fn single_shard_is_still_gated() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_with_auth_for_test(port, 1, PASS);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(&mut c, &["GET", "k"]).await,
            NOAUTH,
            "unauth GET (1 shard)"
        );
        assert_eq!(
            cmd(&mut c, &["KEYS", "*"]).await,
            NOAUTH,
            "unauth KEYS (1 shard)"
        );
        auth_ok(&mut c).await;
        assert_eq!(
            cmd(&mut c, &["GET", "k"]).await,
            b"$-1\r\n",
            "authed GET nil"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}
