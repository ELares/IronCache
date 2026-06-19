// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration coverage for the `CONFIG` command surface and the runtime-config cell
//! (CONFIG.md, PR-4b): the `maxmemory-policy` hot-swap reaching ALL shards through the
//! shared [`RuntimeConfig`], the `maxmemory` hot-set changing the admission budget,
//! `requirepass` gating new connections, `CONFIG GET`/`SET` round trips, RESETSTAT, and
//! INFO reflecting a `CONFIG SET`.
//!
//! ## The two-shard harness (the cross-shard reach test)
//!
//! Two independent "shards" are modeled: each owns its OWN [`ShardStore`], timing wheel,
//! env, and per-shard last-seen generation (shared-nothing, ADR-0002), but they SHARE
//! one `Arc<RuntimeConfig>` cloned into each shard's [`ServerContext`] (the one new
//! cross-shard cell, like the shutdown flag). A `CONFIG SET maxmemory-policy` issued on
//! shard A bumps the shared generation; shard B notices on ITS next command (a relaxed
//! atomic load + compare at the top of dispatch) and rebuilds its own policy. This
//! proves the swap reaches a connection on a DIFFERENT shard.

use ironcache_config::{Config, RuntimeConfig};
use ironcache_env::SystemEnv;
use ironcache_eviction::{Policy, map_policy_name};
use ironcache_observe::{CounterDeltas, CounterSnapshot, MemoryInfo, ServerInfo, ShardCounters};
use ironcache_protocol::{DecodeOutcome, Limits, ProtoVersion, Value, decode, encode_to_vec};
use ironcache_runtime::tokio_rt::bind_reuseport;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{ConnState, TimingWheel, UnixMillis, dispatch};
use ironcache_storage::CountingAccounting;
use ironcache_store::{ShardStore, process_memory};
use std::cell::RefCell;
use std::sync::Arc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

type Store = ShardStore<Policy, CountingAccounting>;

/// Build a [`ServerContext`] sharing `runtime` with a boot config carrying the given
/// ceiling/policy. Used to give each modeled shard a context over the SAME runtime cell.
fn ctx(runtime: Arc<RuntimeConfig>, boot: Config) -> ServerContext {
    ServerContext {
        runtime,
        databases: boot.databases,
        shards: boot.shards,
        info: ServerInfo {
            tcp_port: boot.port,
            shards: boot.shards,
            pid: std::process::id(),
            started_at: ironcache_env::Monotonic::ZERO,
            maxmemory: boot.maxmemory,
            maxmemory_policy: "allkeys-lru",
            mem_allocator: "jemalloc",
            cluster_node_id: "0000000000000000000000000000000000000000",
            cluster_enabled: false,
        },
        cluster: None,
        raft: None,
        repl_status: None,
        in_sync_replicas: None,
        metrics_registry: None,
        persist_stats: None,
        boot,
    }
}

/// Serve a single connection against a caller-supplied store + per-shard generation
/// (so the cross-shard test can give two connections two separate stores over one
/// shared runtime cell). Mirrors the binary serve loop's dispatch wiring.
async fn serve_one(
    mut stream: tokio::net::TcpStream,
    ctx: ServerContext,
    mut store: Store,
    initial_policy: &str,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut env = SystemEnv::new();
    let mut wheel = TimingWheel::new();
    // A real per-shard counter set so INFO/RESETSTAT see accumulated stats (the binary
    // serve loop folds the per-command deltas into ShardCounters; we mirror that here).
    let counters = RefCell::new(ShardCounters::new());
    let mut conn = ConnState::new(
        1,
        ProtoVersion::Resp2,
        ctx.requires_auth(),
        "test".to_owned(),
        "test".to_owned(),
    );
    // This shard starts at generation 0 (the runtime cell also starts at 0); a CONFIG
    // SET maxmemory-policy on ANY shard bumps the shared generation, and this shard
    // catches up on its next command. `initial_policy` is the boot policy this shard's
    // store was built with (kept for documentation symmetry).
    let _ = initial_policy;
    let mut shard_gen = ctx.runtime.generation();
    let limits = Limits::default();
    let mut buf: Vec<u8> = Vec::new();
    loop {
        loop {
            match decode(&buf, &limits) {
                DecodeOutcome::Complete { request, consumed } => {
                    counters.borrow_mut().on_command();
                    let snapshot_fn = || counters.borrow().snapshot();
                    let rollup: &dyn Fn() -> CounterSnapshot = &snapshot_fn;
                    let now = UnixMillis(0);
                    let mem = if request.command().eq_ignore_ascii_case(b"INFO") {
                        let (used_memory, used_memory_rss) = process_memory();
                        MemoryInfo {
                            used_memory,
                            used_memory_rss,
                        }
                    } else {
                        MemoryInfo::default()
                    };
                    let mut deltas = CounterDeltas::default();
                    let reply = dispatch(
                        &ctx,
                        &mut conn,
                        &mut env,
                        &mut store,
                        &mut wheel,
                        now,
                        &mut shard_gen,
                        rollup,
                        mem,
                        &mut deltas,
                        &request,
                    );
                    // Fold this command's deltas into the shard counters (after dispatch
                    // returns, so the rollup borrow has ended), exactly as the binary
                    // serve loop does. RESETSTAT's reset_stats flag is honored in apply.
                    if deltas != CounterDeltas::default() {
                        counters.borrow_mut().apply(deltas);
                    }
                    let bytes = encode_to_vec(&reply, conn.proto);
                    if stream.write_all(&bytes).await.is_err() {
                        return;
                    }
                    buf.drain(..consumed);
                    if conn.should_close {
                        return;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Error(e) => {
                    let bytes = encode_to_vec(&Value::Error(e), conn.proto);
                    let _ = stream.write_all(&bytes).await;
                    return;
                }
            }
        }
        let mut tmp = [0u8; 4096];
        match stream.read(&mut tmp).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
}

/// Read one reply chunk and return it as a lossy string (for line/array assertions).
async fn read_chunk(client: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

async fn send(client: &mut tokio::net::TcpStream, parts: &[&str]) {
    use core::fmt::Write as _;
    use tokio::io::AsyncWriteExt;
    let mut cmd = format!("*{}\r\n", parts.len());
    for p in parts {
        let _ = write!(cmd, "${}\r\n{}\r\n", p.len(), p);
    }
    client.write_all(cmd.as_bytes()).await.unwrap();
}

/// Send `parts` and return the reply chunk.
async fn roundtrip(client: &mut tokio::net::TcpStream, parts: &[&str]) -> String {
    send(client, parts).await;
    read_chunk(client).await
}

#[test]
fn maxmemory_policy_hot_swap_reaches_all_shards() {
    // The headline test: boot two shards (separate stores) sharing ONE runtime cell,
    // under allkeys-lru. A connection on shard A issues CONFIG SET maxmemory-policy
    // allkeys-lfu; assert (1) shard A's OBJECT FREQ now works (LFU active) where it
    // errored before, and (2) a connection on shard B ALSO sees the new policy (the
    // cross-shard reach via the shared cell): OBJECT FREQ works on B too.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let boot = Config {
            maxmemory_policy: "allkeys-lru".to_owned(),
            shards: 2,
            ..Config::default()
        };
        // ONE shared runtime cell cloned into both shard contexts (the cross-shard cell).
        let runtime = RuntimeConfig::from_config(&boot);

        // Two listeners, two shards, each with its own store built from the boot policy.
        let la = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let lb = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr_a = la.local_addr().unwrap();
        let addr_b = lb.local_addr().unwrap();

        let ctx_a = ctx(Arc::clone(&runtime), boot.clone());
        let ctx_b = ctx(Arc::clone(&runtime), boot.clone());

        let acc_a = tokio::task::spawn_local(async move {
            let (s, _) = la.accept().await.unwrap();
            let store = ShardStore::with_hooks(
                16,
                map_policy_name("allkeys-lru", 1).unwrap(),
                CountingAccounting::new(),
            );
            serve_one(s, ctx_a, store, "allkeys-lru").await;
        });
        let acc_b = tokio::task::spawn_local(async move {
            let (s, _) = lb.accept().await.unwrap();
            let store = ShardStore::with_hooks(
                16,
                map_policy_name("allkeys-lru", 1).unwrap(),
                CountingAccounting::new(),
            );
            serve_one(s, ctx_b, store, "allkeys-lru").await;
        });

        let mut ca = tokio::net::TcpStream::connect(addr_a).await.unwrap();
        let mut cb = tokio::net::TcpStream::connect(addr_b).await.unwrap();

        // Plant a key on each shard.
        assert!(
            roundtrip(&mut ca, &["SET", "ka", "va"])
                .await
                .starts_with("+OK")
        );
        assert!(
            roundtrip(&mut cb, &["SET", "kb", "vb"])
                .await
                .starts_with("+OK")
        );

        // Under allkeys-lru, OBJECT FREQ errors (requires an LFU policy) on BOTH shards.
        let fa = roundtrip(&mut ca, &["OBJECT", "FREQ", "ka"]).await;
        assert!(
            fa.contains("An LFU maxmemory policy is not selected"),
            "before swap, shard A FREQ must error: {fa}"
        );
        let fb = roundtrip(&mut cb, &["OBJECT", "FREQ", "kb"]).await;
        assert!(
            fb.contains("An LFU maxmemory policy is not selected"),
            "before swap, shard B FREQ must error: {fb}"
        );

        // Swap to allkeys-lfu on shard A.
        assert!(
            roundtrip(
                &mut ca,
                &["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"]
            )
            .await
            .starts_with("+OK")
        );

        // Shard A now has LFU active: OBJECT FREQ returns an integer (re-access the key
        // first so it is tracked in the sketch).
        let _ = roundtrip(&mut ca, &["GET", "ka"]).await;
        let fa2 = roundtrip(&mut ca, &["OBJECT", "FREQ", "ka"]).await;
        assert!(
            fa2.starts_with(':'),
            "after swap, shard A FREQ must be an integer: {fa2}"
        );
        // The configured policy round-trips verbatim on shard A.
        let ga = roundtrip(&mut ca, &["CONFIG", "GET", "maxmemory-policy"]).await;
        assert!(ga.contains("allkeys-lfu"), "shard A GET policy: {ga}");

        // THE CROSS-SHARD REACH: shard B (a DIFFERENT shard / store) sees the new policy
        // on its next command via the shared cell. OBJECT FREQ now works on B too.
        let _ = roundtrip(&mut cb, &["GET", "kb"]).await;
        let fb2 = roundtrip(&mut cb, &["OBJECT", "FREQ", "kb"]).await;
        assert!(
            fb2.starts_with(':'),
            "after swap, shard B FREQ must be an integer (cross-shard reach): {fb2}"
        );
        let gb = roundtrip(&mut cb, &["CONFIG", "GET", "maxmemory-policy"]).await;
        assert!(gb.contains("allkeys-lfu"), "shard B GET policy: {gb}");

        drop(ca);
        drop(cb);
        acc_a.await.unwrap();
        acc_b.await.unwrap();
    });
}

#[test]
fn maxmemory_hot_set_changes_the_admission_budget() {
    // CONFIG SET maxmemory at runtime tightens the ceiling so a denyoom write triggers
    // -OOM where it did not before. Single shard, noeviction so the over-budget write is
    // rejected (not evicted) for a deterministic assertion.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let boot = Config {
            maxmemory: 0, // start unlimited
            maxmemory_policy: "noeviction".to_owned(),
            shards: 1,
            ..Config::default()
        };
        let runtime = RuntimeConfig::from_config(&boot);
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server_ctx = ctx(Arc::clone(&runtime), boot.clone());
        let acc = tokio::task::spawn_local(async move {
            let (s, _) = listener.accept().await.unwrap();
            let store = ShardStore::with_hooks(16, Policy::NoEviction, CountingAccounting::new());
            serve_one(s, server_ctx, store, "noeviction").await;
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // Unlimited: a big write succeeds.
        let big = "v".repeat(200);
        assert!(
            roundtrip(&mut client, &["SET", "k1", &big])
                .await
                .starts_with("+OK"),
            "unlimited write should succeed"
        );

        // Tighten maxmemory to a tiny ceiling at runtime. The store already holds > the
        // new budget, so the NEXT denyoom write is rejected -OOM (noeviction).
        assert!(
            roundtrip(&mut client, &["CONFIG", "SET", "maxmemory", "50"])
                .await
                .starts_with("+OK")
        );
        let r = roundtrip(&mut client, &["SET", "k2", &big]).await;
        assert!(
            r.starts_with("-OOM"),
            "after CONFIG SET maxmemory, a denyoom write over the new budget must -OOM: {r}"
        );
        // INFO reflects the new maxmemory.
        let info = roundtrip(&mut client, &["INFO", "memory"]).await;
        assert!(
            info.contains("maxmemory:50\r\n"),
            "INFO maxmemory after set: {info}"
        );

        // Loosen it back to unlimited; the write succeeds again.
        assert!(
            roundtrip(&mut client, &["CONFIG", "SET", "maxmemory", "0"])
                .await
                .starts_with("+OK")
        );
        assert!(
            roundtrip(&mut client, &["SET", "k3", &big])
                .await
                .starts_with("+OK"),
            "after loosening maxmemory, the write should succeed again"
        );

        drop(client);
        acc.await.unwrap();
    });
}

#[test]
fn requirepass_gates_new_connections() {
    // CONFIG SET requirepass enables auth for NEW commands: a fresh connection is
    // NOAUTH-gated until AUTH succeeds; a wrong password is WRONGPASS; an empty
    // requirepass disables auth again.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let boot = Config {
            shards: 1,
            ..Config::default()
        };
        let runtime = RuntimeConfig::from_config(&boot);
        // Two listeners so the admin connection (no auth at boot) can set requirepass,
        // then a fresh connection observes the gate.
        let l1 = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let l2 = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr1 = l1.local_addr().unwrap();
        let addr2 = l2.local_addr().unwrap();
        let ctx1 = ctx(Arc::clone(&runtime), boot.clone());
        let ctx2 = ctx(Arc::clone(&runtime), boot.clone());
        let acc1 = tokio::task::spawn_local(async move {
            let (s, _) = l1.accept().await.unwrap();
            let store =
                ShardStore::with_hooks(16, Policy::cache_default(), CountingAccounting::new());
            serve_one(s, ctx1, store, "allkeys-lru").await;
        });
        let acc2 = tokio::task::spawn_local(async move {
            let (s, _) = l2.accept().await.unwrap();
            let store =
                ShardStore::with_hooks(16, Policy::cache_default(), CountingAccounting::new());
            serve_one(s, ctx2, store, "allkeys-lru").await;
        });

        let mut admin = tokio::net::TcpStream::connect(addr1).await.unwrap();
        // No auth at boot: PING works.
        assert!(roundtrip(&mut admin, &["PING"]).await.starts_with("+PONG"));
        // Set a password.
        assert!(
            roundtrip(&mut admin, &["CONFIG", "SET", "requirepass", "s3cr3t"])
                .await
                .starts_with("+OK")
        );

        // A NEW connection is now NOAUTH-gated.
        let mut user = tokio::net::TcpStream::connect(addr2).await.unwrap();
        let p = roundtrip(&mut user, &["PING"]).await;
        assert!(
            p.starts_with("-NOAUTH"),
            "new conn must be NOAUTH-gated: {p}"
        );
        // Wrong password -> WRONGPASS.
        let w = roundtrip(&mut user, &["AUTH", "nope"]).await;
        assert!(w.starts_with("-WRONGPASS"), "wrong pass -> WRONGPASS: {w}");
        // Correct password -> OK, then PING works.
        assert!(
            roundtrip(&mut user, &["AUTH", "s3cr3t"])
                .await
                .starts_with("+OK")
        );
        assert!(roundtrip(&mut user, &["PING"]).await.starts_with("+PONG"));

        // Disable auth again (empty requirepass): the SAME (already-authed) connection
        // continues to work, and the runtime cell now reports no auth required. (A fresh
        // connection's gate-off behavior is covered byte-for-byte by the unit test
        // `config_set_requirepass_empty_clears_auth`; here both single-shot acceptors
        // have served their one connection, so we assert the cell state directly.)
        assert!(
            roundtrip(&mut admin, &["CONFIG", "SET", "requirepass", ""])
                .await
                .starts_with("+OK")
        );
        assert!(!runtime.requires_auth(), "empty requirepass disables auth");

        drop(admin);
        drop(user);
        acc1.await.unwrap();
        acc2.await.unwrap();
    });
}

#[test]
fn config_resetstat_and_info_reflect_set() {
    // CONFIG RESETSTAT zeroes the serving shard's stat counters (visible in INFO), and
    // INFO reflects a CONFIG SET maxmemory-policy.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let boot = Config {
            maxmemory_policy: "allkeys-lru".to_owned(),
            shards: 1,
            ..Config::default()
        };
        let runtime = RuntimeConfig::from_config(&boot);
        let listener = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server_ctx = ctx(Arc::clone(&runtime), boot.clone());
        let acc = tokio::task::spawn_local(async move {
            let (s, _) = listener.accept().await.unwrap();
            let store =
                ShardStore::with_hooks(16, Policy::cache_default(), CountingAccounting::new());
            serve_one(s, server_ctx, store, "allkeys-lru").await;
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // Generate some keyspace hits/misses.
        roundtrip(&mut client, &["SET", "k", "v"]).await;
        roundtrip(&mut client, &["GET", "k"]).await; // hit
        roundtrip(&mut client, &["GET", "nope"]).await; // miss
        let before = roundtrip(&mut client, &["INFO", "stats"]).await;
        assert!(
            before.contains("keyspace_hits:1\r\n"),
            "hits before reset: {before}"
        );
        assert!(
            before.contains("keyspace_misses:1\r\n"),
            "misses before reset: {before}"
        );

        // RESETSTAT zeroes them.
        assert!(
            roundtrip(&mut client, &["CONFIG", "RESETSTAT"])
                .await
                .starts_with("+OK")
        );
        let after = roundtrip(&mut client, &["INFO", "stats"]).await;
        assert!(
            after.contains("keyspace_hits:0\r\n"),
            "hits after reset: {after}"
        );
        assert!(
            after.contains("keyspace_misses:0\r\n"),
            "misses after reset: {after}"
        );

        // INFO reflects a CONFIG SET maxmemory-policy.
        roundtrip(
            &mut client,
            &["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"],
        )
        .await;
        let mem = roundtrip(&mut client, &["INFO", "memory"]).await;
        assert!(
            mem.contains("maxmemory_policy:volatile-ttl\r\n"),
            "INFO must reflect the policy CONFIG SET: {mem}"
        );

        drop(client);
        acc.await.unwrap();
    });
}
