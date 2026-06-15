// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transaction correctness UNDER PARTITIONING (COORDINATOR.md #107, the critical fix).
//!
//! These boot the REAL multi-shard `run_server` on an ephemeral port (the actual
//! SO_REUSEPORT thread-per-core topology + cross-shard coordinator) and drive it over real
//! sockets, exercising the whole serve path: classify -> the in-MULTI queue gate / guards ->
//! route -> dispatch.
//!
//! ## The bug these guard
//!
//! `route_and_dispatch` routes each command to its key's OWNER shard. Before this fix it did
//! NOT check `in_multi` before routing, so a command issued inside a `MULTI` whose key was
//! owned by a REMOTE shard took the remote-hop / fan-out branch and EXECUTED EAGERLY (the
//! client got the executed reply instead of `+QUEUED`, and the command ran immediately and
//! out of transaction order). The queue gate (`+QUEUED`) lived only on the home path.
//!
//! The invariant established: a transaction reaches real (home-only) EXEC ONLY when ALL its
//! watched keys AND all its queued commands' keys are HOME-OWNED (so home execution is always
//! correct); otherwise it is rejected LOUDLY (queue-time cross-shard error -> -EXECABORT, or a
//! cross-shard-WATCH error). With shards == 1 every key is home-owned, so the guards never
//! fire and the behavior is byte-identical to the single-shard transaction tests (PR-10).

use ironcache::test_support::run_server_for_test;
use ironcache_server::route::owner_shard;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Connect with a few short retries (shards bind asynchronously after `run_server` returns).
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

/// Boot a multi-shard server, returning (handle, port).
fn boot(shards: usize) -> (ironcache_runtime::bootstrap::ShardSet, u16) {
    let port = free_port();
    let set = run_server_for_test(port, shards);
    (set, port)
}

/// Send a command built from `parts` as a RESP2 array.
async fn send_cmd(client: &mut TcpStream, parts: &[&[u8]]) {
    let mut frame = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        frame.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        frame.extend_from_slice(p);
        frame.extend_from_slice(b"\r\n");
    }
    client.write_all(&frame).await.unwrap();
}

/// Read one short reply (the small replies here fit a single read) as raw bytes.
async fn read_raw(client: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 512];
    let n = client.read(&mut buf).await.unwrap();
    assert!(n > 0, "connection closed mid-reply");
    buf[..n].to_vec()
}

/// Send `parts` and read one short reply as raw bytes.
async fn cmd_raw(client: &mut TcpStream, parts: &[&[u8]]) -> Vec<u8> {
    send_cmd(client, parts).await;
    read_raw(client).await
}

/// Send `parts` and assert the reply matches `expect` exactly.
async fn expect(client: &mut TcpStream, parts: &[&[u8]], expect: &[u8]) {
    let got = cmd_raw(client, parts).await;
    assert_eq!(
        got,
        expect,
        "command {:?}: got {:?}",
        parts
            .iter()
            .map(|p| String::from_utf8_lossy(p))
            .collect::<Vec<_>>(),
        String::from_utf8_lossy(&got)
    );
}

/// A bulk-string reply `$<len>\r\n<bytes>\r\n`.
fn bulk(val: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", val.len(), val).into_bytes()
}

/// Find a connection's HOME shard by probing: SET many keys, GET them back; a key whose owner
/// is the home shard takes the local fast path. We cannot read the home directly, so we infer
/// it differently: the home shard is the one a key with `owner_shard(key) == h` belongs to.
/// Instead of inferring the (hidden) home, the tests below pick keys per owner shard and rely
/// on the guarantee that, over N distinct-owner keys, at most ONE is home-owned (so >= N-1 are
/// remote). This helper returns ONE key per distinct owner shard `[0, shards)`.
fn keys_per_owner(shards: usize) -> Vec<(usize, String)> {
    let mut found: Vec<Option<String>> = vec![None; shards];
    let mut remaining = shards;
    let mut i = 0u64;
    while remaining > 0 {
        let key = format!("txn:k:{i}");
        let owner = owner_shard(key.as_bytes(), shards);
        if found[owner].is_none() {
            found[owner] = Some(key);
            remaining -= 1;
        }
        i += 1;
        assert!(i < 1_000_000, "could not find a key for every owner shard");
    }
    found
        .into_iter()
        .enumerate()
        .map(|(s, k)| (s, k.unwrap()))
        .collect()
}

const CROSS_SHARD_CMD_ERR: &[u8] =
    b"-ERR a queued command references a key on another shard; cross-shard transactions are not supported yet\r\n";
const CROSS_SHARD_WATCH_ERR: &[u8] =
    b"-ERR WATCH of a key on another shard is not supported yet\r\n";
const WHOLE_KEYSPACE_TXN_ERR: &[u8] =
    b"-ERR a whole-keyspace command in a transaction is not supported across shards yet\r\n";
const EXECABORT: &[u8] = b"-EXECABORT Transaction discarded because of previous errors.\r\n";

#[test]
fn regression_in_multi_remote_command_never_executes_eagerly() {
    // THE REGRESSION (the critical part): inside a `MULTI`, a keyed command (`SET Kremote v`)
    // whose key is owned by a REMOTE shard must NOT EXECUTE EAGERLY. Before the fix it took the
    // dispatch_via (remote-hop) branch and ran immediately, returning the EXECUTED reply
    // (`+OK`) and WRITING the key right away (out of transaction order).
    //
    // After the fix, a remote keyed command in MULTI is rejected at queue time with the
    // cross-shard error (the loud rejection) -- crucially NEVER the executed `+OK` -- and the
    // key is NOT written. A HOME-owned keyed command in MULTI replies `+QUEUED` and is staged
    // (also never the executed `+OK`). Either way, the bug behavior (executed `+OK` + an eager
    // write visible before EXEC) is gone.
    //
    // We pick one key per owner shard; at most ONE is home-owned, so >= shards-1 are remote.
    // For each: the in-MULTI SET reply is either the cross-shard error (remote, >= shards-1
    // times) or +QUEUED (the single home-owned one) -- and in NO case the executed +OK. A
    // second connection confirms NO key is set (the GET routes to the true owner, so an eager
    // leaked write would show up).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let shards = 5usize;
        let (server, port) = boot(shards);
        let mut c = connect_retry(port).await;
        let mut probe = connect_retry(port).await;

        let keys = keys_per_owner(shards);

        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        let mut remote_rejected = 0usize;
        for (_, key) in &keys {
            let got = cmd_raw(&mut c, &[b"SET", key.as_bytes(), b"v"]).await;
            // The CRITICAL regression assertion: the reply is NEVER the executed `+OK`. Before
            // the fix a remote-owned SET in MULTI returned exactly this.
            assert_ne!(
                got, b"+OK\r\n",
                "in-MULTI SET must NOT return the executed +OK (the pre-fix eager-execution bug)"
            );
            if got == CROSS_SHARD_CMD_ERR {
                remote_rejected += 1;
            } else {
                assert_eq!(
                    got,
                    b"+QUEUED\r\n",
                    "in-MULTI SET must be the cross-shard error (remote) or +QUEUED (home); got {:?}",
                    String::from_utf8_lossy(&got)
                );
            }
        }
        assert!(
            remote_rejected >= shards - 1,
            "expected >= {} remote rejections (<=1 home-owned key); got {remote_rejected}",
            shards - 1
        );

        // No key was written eagerly: every key is still nil, checked via a 2nd connection so
        // each GET routes to the true owner shard.
        for (_, key) in &keys {
            expect(&mut probe, &[b"GET", key.as_bytes()], b"$-1\r\n").await;
        }

        drop(c);
        drop(probe);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn cross_shard_in_multi_aborts_with_execabort() {
    // (2) Cross-shard MULTI -> EXECABORT. We need a key that is DEFINITELY remote-owned for
    // THIS connection's home shard. We discover the home shard by elimination: SET a probe key
    // OUTSIDE a transaction and read it back is not enough (it routes regardless). Instead we
    // rely on the guard's observable signal: inside MULTI, a home-owned key returns +QUEUED and
    // a remote-owned key returns the cross-shard error. So we issue SET for one key PER owner
    // shard inside MULTI and assert: at least shards-1 of them return the cross-shard error
    // (only the single home-owned key, if any, queues), the txn is dirtied, and EXEC ->
    // EXECABORT applying nothing.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let shards = 5usize;
        let (server, port) = boot(shards);
        let mut c = connect_retry(port).await;
        let mut probe = connect_retry(port).await;

        let keys = keys_per_owner(shards);

        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        let mut cross_shard_hits = 0usize;
        let mut queued_hits = 0usize;
        for (_, key) in &keys {
            let got = cmd_raw(&mut c, &[b"SET", key.as_bytes(), b"v"]).await;
            if got == CROSS_SHARD_CMD_ERR {
                cross_shard_hits += 1;
            } else if got == b"+QUEUED\r\n" {
                queued_hits += 1;
            } else {
                panic!(
                    "in-MULTI SET must be either +QUEUED (home) or the cross-shard error; got {:?}",
                    String::from_utf8_lossy(&got)
                );
            }
        }
        // At most ONE key is home-owned for this connection, so we see >= shards-1 cross-shard
        // errors, and at most one +QUEUED. The cross-shard error MUST have fired (the whole
        // point), so the txn is now dirty.
        assert!(
            cross_shard_hits >= shards - 1,
            "expected >= {} cross-shard errors (one key per owner, <=1 home-owned); got {cross_shard_hits} cross-shard, {queued_hits} queued",
            shards - 1
        );
        assert!(
            cross_shard_hits >= 1,
            "the cross-shard guard must have fired at least once"
        );
        // EXEC -> EXECABORT (the dirtied batch), applying NOTHING.
        expect(&mut c, &[b"EXEC"], EXECABORT).await;
        // Nothing applied: every key (home- or remote-owned) is still nil, checked via a 2nd
        // connection so each GET routes to the true owner.
        for (_, key) in &keys {
            expect(&mut probe, &[b"GET", key.as_bytes()], b"$-1\r\n").await;
        }
        // The connection is usable again (clean post-EXECABORT state): a fresh MULTI/EXEC works.
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"PING"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"EXEC"], b"*1\r\n+PONG\r\n").await;

        drop(c);
        drop(probe);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn cross_shard_watch_errors_and_leaves_connection_usable() {
    // (3) Cross-shard WATCH -> error. WATCH of a remote-owned key replies the cross-shard-WATCH
    // error and does NOT leave the connection mid-watch (a following MULTI/EXEC works, and the
    // dirty-CAS does not abort because nothing was actually watched). We WATCH one key per owner
    // shard; >= shards-1 of them are remote, so the error fires; a home-owned WATCH replies +OK.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let shards = 5usize;
        let (server, port) = boot(shards);
        let mut c = connect_retry(port).await;

        let keys = keys_per_owner(shards);

        let mut watch_errors = 0usize;
        for (_, key) in &keys {
            let got = cmd_raw(&mut c, &[b"WATCH", key.as_bytes()]).await;
            if got == CROSS_SHARD_WATCH_ERR {
                watch_errors += 1;
            } else {
                assert_eq!(
                    got,
                    b"+OK\r\n",
                    "WATCH of a home-owned key must be +OK; got {:?}",
                    String::from_utf8_lossy(&got)
                );
            }
        }
        assert!(
            watch_errors >= shards - 1,
            "expected >= {} cross-shard WATCH errors; got {watch_errors}",
            shards - 1
        );

        // The connection is NOT left mid-watch in a broken way: UNWATCH clears any home-owned
        // watch cleanly, then a plain MULTI/EXEC commits (NOT a null-array dirty-CAS abort).
        // Pick the home-owned key (if any) to drive a real committing transaction; if every
        // key was remote, just run a non-keyed MULTI/EXEC.
        expect(&mut c, &[b"UNWATCH"], b"+OK\r\n").await;
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"PING"], b"+QUEUED\r\n").await;
        // A non-null-array reply proves the connection was not stuck in a failed-WATCH state.
        expect(&mut c, &[b"EXEC"], b"*1\r\n+PONG\r\n").await;

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn happy_path_all_home_transaction_commits_single_shard() {
    // (4) Happy path: a MULTI/EXEC whose keys are ALL home-owned commits correctly and returns
    // the per-command reply array. At shards == 1 EVERY key is home-owned (the deterministic
    // all-home construction), so this is the canonical "all keys home-owned -> home-only EXEC
    // is correct" path. SET k 1 ; INCR k ; GET k -> array [+OK, :2, $1 2], then the batch
    // applied (GET k -> "2"). Byte-identical to the single-shard PR-10 transaction test.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(1);
        let mut c = connect_retry(port).await;

        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"SET", b"k", b"1"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"INCR", b"k"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"GET", b"k"], b"+QUEUED\r\n").await;
        // EXEC -> the per-command reply array: *3 then +OK then :2 then $1 2.
        expect(&mut c, &[b"EXEC"], b"*3\r\n+OK\r\n:2\r\n$1\r\n2\r\n").await;
        // The batch applied.
        let got = cmd_raw(&mut c, &[b"GET", b"k"]).await;
        assert_eq!(got, bulk("2"), "the committed transaction wrote k=2");

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn happy_path_all_home_transaction_commits_multi_shard() {
    // (4b) Happy path under genuine partitioning: a MULTI/EXEC whose keys are all owned by ONE
    // shard must commit. We pick TWO keys that co-locate on the SAME owner shard, then drive the
    // transaction from EVERY shard-affinity by trying connections until the chosen keys are
    // home-owned for the connection. Since we cannot pin a connection's home shard, we instead
    // assert the WEAKER but still meaningful property: for keys that ARE home-owned (signaled by
    // +QUEUED rather than the cross-shard error), the transaction commits and applies. We probe
    // by trying each owner's key and only committing the home-owned one.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let shards = 4usize;
        let (server, port) = boot(shards);
        let mut c = connect_retry(port).await;

        let keys = keys_per_owner(shards);

        // Find a home-owned key for THIS connection: inside a throwaway MULTI, SET each key and
        // watch for the +QUEUED (home) vs cross-shard error (remote) signal. DISCARD after.
        let mut home_key: Option<String> = None;
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        for (_, key) in &keys {
            let got = cmd_raw(&mut c, &[b"SET", key.as_bytes(), b"probe"]).await;
            if got == b"+QUEUED\r\n" && home_key.is_none() {
                home_key = Some(key.clone());
            }
        }
        expect(&mut c, &[b"DISCARD"], b"+OK\r\n").await;

        let home_key = home_key.expect("exactly one key per owner -> one is home-owned");

        // Now run a real committing transaction on that home-owned key.
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(
            &mut c,
            &[b"SET", home_key.as_bytes(), b"10"],
            b"+QUEUED\r\n",
        )
        .await;
        expect(&mut c, &[b"INCR", home_key.as_bytes()], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"EXEC"], b"*2\r\n+OK\r\n:11\r\n").await;
        // The write is visible (routes to the owning = home shard).
        let got = cmd_raw(&mut c, &[b"GET", home_key.as_bytes()]).await;
        assert_eq!(
            got,
            bulk("11"),
            "the committed home-owned transaction applied"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn non_keyed_command_in_multi_queues_and_runs_at_exec() {
    // (6) A non-keyed command in MULTI (PING) still queues + returns +QUEUED and runs at EXEC,
    // unaffected by the cross-shard guard (the guard gates only KEYED data commands). Run under
    // genuine partitioning (shards > 1) to prove the guard does not over-fire on control verbs.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;

        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"PING"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"ECHO", b"hi"], b"+QUEUED\r\n").await;
        // EXEC -> *2 then +PONG then $2 hi.
        expect(&mut c, &[b"EXEC"], b"*2\r\n+PONG\r\n$2\r\nhi\r\n").await;

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn shards_one_parity_normal_multi_exec_byte_identical() {
    // (5) shards == 1 parity: a normal MULTI/SET/EXEC at one shard is byte-identical to the
    // pre-coordinator home path (every key home-owned -> the guards never fire; the in_multi
    // branch routes to the same home dispatch that always handled it). This is the same shape
    // the server-crate PR-10 `transactions_over_real_socket` test asserts, booted via the real
    // multi-shard run_server at shards == 1.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let (server, port) = boot(1);
        let mut c = connect_retry(port).await;

        // MULTI; SET k 1; INCR k; EXEC -> *2 +OK :2 ; GET k -> "2".
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"SET", b"k", b"1"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"INCR", b"k"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"EXEC"], b"*2\r\n+OK\r\n:2\r\n").await;
        let got = cmd_raw(&mut c, &[b"GET", b"k"]).await;
        assert_eq!(got, bulk("2"));

        // DISCARD path is byte-identical too.
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"SET", b"d", b"9"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"DISCARD"], b"+OK\r\n").await;
        expect(&mut c, &[b"GET", b"d"], b"$-1\r\n").await;

        // WATCH of a (home-owned, since shards == 1) key then MULTI/EXEC commits (no dirty-CAS
        // abort): WATCH never errors at shards == 1 (the cross-shard WATCH guard never fires).
        expect(&mut c, &[b"WATCH", b"w"], b"+OK\r\n").await;
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"SET", b"w", b"1"], b"+QUEUED\r\n").await;
        expect(&mut c, &[b"EXEC"], b"*1\r\n+OK\r\n").await;

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

#[test]
fn whole_keyspace_in_multi_aborts_at_many_shards_runs_at_one() {
    // A whole-keyspace command (DBSIZE/KEYS/FLUSHALL/...) inside MULTI cannot run correctly
    // home-only at EXEC when the keyspace is partitioned: EXEC replays synchronously on the
    // home store and would cover only ~1/N (a MULTI;FLUSHALL;EXEC would partially flush --
    // silent data RETENTION). At shards > 1 the guard rejects it at queue time (dirty ->
    // EXECABORT); at shards == 1 the home shard IS the whole keyspace, so it queues + runs.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        // shards > 1: DBSIZE/KEYS/FLUSHALL inside MULTI are rejected at queue time, EXEC aborts.
        let (server, port) = boot(4);
        let mut c = connect_retry(port).await;
        let mut probe = connect_retry(port).await;
        // Seed keys spanning shards so a partial FLUSHALL would be observable if it ran.
        for (_, key) in keys_per_owner(4) {
            expect(&mut probe, &[b"SET", key.as_bytes(), b"v"], b"+OK\r\n").await;
        }
        expect(&mut c, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c, &[b"DBSIZE"], WHOLE_KEYSPACE_TXN_ERR).await;
        expect(&mut c, &[b"KEYS", b"*"], WHOLE_KEYSPACE_TXN_ERR).await;
        expect(&mut c, &[b"FLUSHALL"], WHOLE_KEYSPACE_TXN_ERR).await;
        // The batch is dirty -> EXEC aborts, applying NOTHING (no partial flush).
        expect(&mut c, &[b"EXEC"], EXECABORT).await;
        // Every seeded key SURVIVED (the FLUSHALL never ran): DBSIZE outside MULTI fans out
        // and counts all of them.
        expect(&mut probe, &[b"DBSIZE"], b":4\r\n").await;
        drop(c);
        drop(probe);
        server.shutdown_and_join().unwrap();

        // shards == 1: the home shard is the whole keyspace, so a whole-keyspace command in
        // MULTI queues + runs (byte-identical to the pre-coordinator behavior).
        let (server1, port1) = boot(1);
        let mut c1 = connect_retry(port1).await;
        expect(&mut c1, &[b"SET", b"k", b"v"], b"+OK\r\n").await;
        expect(&mut c1, &[b"MULTI"], b"+OK\r\n").await;
        expect(&mut c1, &[b"DBSIZE"], b"+QUEUED\r\n").await;
        expect(&mut c1, &[b"EXEC"], b"*1\r\n:1\r\n").await;
        drop(c1);
        server1.shutdown_and_join().unwrap();
    });
}
