// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use bytes::Bytes;
use ironcache_env::{Monotonic, TestEnv};
use ironcache_eviction::{Policy, map_policy_name};
use ironcache_storage::CountingAccounting;
use ironcache_store::ShardStore;

/// The store type the dispatch tests drive: the concrete per-shard store wired
/// with a real eviction policy (so it satisfies the `Admit` bound dispatch now
/// requires). Defaults to the cache-mode S3-FIFO policy.
type TestStore = ShardStore<Policy, CountingAccounting>;

/// A test store with `databases` DBs and the given policy.
fn store_with(databases: u32, policy: Policy) -> TestStore {
    ShardStore::with_hooks(databases, policy, CountingAccounting::new())
}

/// The default test store (cache-mode S3-FIFO, ceiling off).
fn test_store(databases: u32) -> TestStore {
    store_with(databases, Policy::cache_default())
}

fn ctx(pass: Option<&str>) -> ServerContext {
    ctx_full(pass, 0, "allkeys-lru")
}

/// A test context with an explicit requirepass, maxmemory ceiling, and policy name
/// seeded into the runtime overlay (so the generation-gated swap + ceiling tests
/// can drive the shared cell directly).
fn ctx_full(pass: Option<&str>, maxmemory: u64, policy: &str) -> ServerContext {
    let boot = ironcache_config::Config {
        maxmemory,
        maxmemory_policy: policy.to_owned(),
        // `Config::requirepass` holds the SHA-256 HEX at rest (#65), so the test
        // harness hashes the test PLAINTEXT just as a real boot would (resolve()
        // hashes it). AUTH with the plaintext then verifies by hashing the guess and
        // matching this digest.
        requirepass: pass.map(|p| ironcache_config::sha256_hex(p.as_bytes())),
        databases: 16,
        shards: 1,
        ..ironcache_config::Config::default()
    };
    let runtime = RuntimeConfig::from_config(&boot);
    let acl = crate::acl::AclState::from_requirepass(boot.requirepass.as_deref());
    ServerContext {
        runtime,
        acl,
        databases: 16,
        shards: 1,
        info: ServerInfo {
            tcp_port: 6379,
            shards: 1,
            pid: 1,
            started_at: Monotonic::ZERO,
            maxmemory,
            maxmemory_policy: "allkeys-lru",
            mem_allocator: "jemalloc",
            cluster_node_id: "0000000000000000000000000000000000000000",
            run_id: "0000000000000000000000000000000000000000",
            cluster_enabled: false,
        },
        cluster: None,
        raft: None,
        repl_status: None,
        in_sync_replicas: None,
        repl_history_id: None,
        metrics_registry: None,
        persist_stats: None,
        process_memory: std::sync::Arc::new(ironcache_observe::ProcessMemoryGauge::new()),
        conn_gate: std::sync::Arc::new(ironcache_observe::ConnectionGate::new()),
        slowlog: std::sync::Arc::new(ironcache_observe::SlowLog::new()),
        latency: std::sync::Arc::new(ironcache_observe::LatencyMonitor::new()),
        clients: std::sync::Arc::new(ironcache_observe::ClientRegistry::new()),
        hotkeys: std::sync::Arc::new(ironcache_observe::Hotkeys::new()),
        boot,
    }
}

fn state(ctx: &ServerContext) -> ConnState {
    ConnState::new(
        7,
        ProtoVersion::Resp2,
        ctx.requires_auth(),
        "127.0.0.1:1".to_owned(),
        "127.0.0.1:6379".to_owned(),
    )
}

fn req(parts: &[&[u8]]) -> Request {
    Request {
        args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
    }
}

fn run(ctx: &ServerContext, st: &mut ConnState, parts: &[&[u8]]) -> Value {
    let mut env = TestEnv::new(1);
    let mut store = test_store(ctx.databases);
    let mut wheel = TimingWheel::new();
    let zero = || CounterSnapshot::default();
    let mut deltas = CounterDeltas::default();
    let mut shard_gen = ctx.runtime.generation();
    dispatch(
        ctx,
        st,
        &mut env,
        &mut store,
        &mut wheel,
        UnixMillis(0),
        &mut shard_gen,
        &zero,
        &|| (String::new(), String::new()),
        &|| None,
        MemoryInfo::default(),
        &mut deltas,
        &req(parts),
    )
}

/// Like [`run`] but threads a caller-owned store and `now`, for the data-command
/// tests that need state to persist across calls (SET then GET) and a clock to
/// advance (EX/lazy expiry).
fn run_on(
    ctx: &ServerContext,
    st: &mut ConnState,
    store: &mut TestStore,
    now: UnixMillis,
    parts: &[&[u8]],
) -> Value {
    let mut wheel = TimingWheel::new();
    run_on_wheel(ctx, st, store, &mut wheel, now, parts)
}

/// Like [`run_on`] but threads a caller-owned [`TimingWheel`] (and surfaces the
/// counter deltas), for the EXPIRE / active-drain tests that need the wheel to
/// persist across calls (register on one command, drain on a later one).
fn run_on_wheel(
    ctx: &ServerContext,
    st: &mut ConnState,
    store: &mut TestStore,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    parts: &[&[u8]],
) -> Value {
    let (reply, _deltas) = run_on_wheel_deltas(ctx, st, store, wheel, now, parts);
    reply
}

/// Like [`run_on_wheel`] but also returns the [`CounterDeltas`] dispatch produced
/// (the active-drain expiry count and keyspace hit/miss), for the counter tests.
fn run_on_wheel_deltas(
    ctx: &ServerContext,
    st: &mut ConnState,
    store: &mut TestStore,
    wheel: &mut TimingWheel,
    now: UnixMillis,
    parts: &[&[u8]],
) -> (Value, CounterDeltas) {
    let mut env = TestEnv::new(1);
    let zero = || CounterSnapshot::default();
    let mut deltas = CounterDeltas::default();
    let mut shard_gen = ctx.runtime.generation();
    let reply = dispatch(
        ctx,
        st,
        &mut env,
        store,
        wheel,
        now,
        &mut shard_gen,
        &zero,
        &|| (String::new(), String::new()),
        &|| None,
        MemoryInfo::default(),
        &mut deltas,
        &req(parts),
    );
    (reply, deltas)
}

#[test]
fn ping_variants() {
    let c = ctx(None);
    let mut s = state(&c);
    assert_eq!(run(&c, &mut s, &[b"PING"]), Value::simple("PONG"));
    assert_eq!(
        run(&c, &mut s, &[b"ping", b"hi"]),
        Value::BulkString(Some(Bytes::from_static(b"hi")))
    );
    assert_eq!(run(&c, &mut s, &[b"PinG"]), Value::simple("PONG")); // case-insensitive
}

#[test]
fn lolwut_returns_version_banner() {
    let c = ctx(None);
    let mut s = state(&c);
    // Bare form: a non-error bulk string naming the server (health probes rely on this).
    match run(&c, &mut s, &[b"LOLWUT"]) {
        Value::BulkString(Some(b)) => {
            assert!(b.starts_with(b"IronCache ver. "), "got {b:?}");
        }
        other => panic!("expected bulk, got {other:?}"),
    }
    // VERSION option with an integer: banner. Command name is case-insensitive.
    match run(&c, &mut s, &[b"lolwut", b"version", b"5"]) {
        Value::BulkString(Some(b)) => assert!(b.starts_with(b"IronCache ver. ")),
        other => panic!("expected bulk, got {other:?}"),
    }
    // Redis is lenient: any non-VERSION trailing args still draw the banner (no error),
    // so a health probe never fails.
    match run(&c, &mut s, &[b"LOLWUT", b"NOPE"]) {
        Value::BulkString(Some(b)) => assert!(b.starts_with(b"IronCache ver. ")),
        other => panic!("expected bulk, got {other:?}"),
    }
    // The ONE error path, byte-faithful to Redis: VERSION with a non-integer value.
    match run(&c, &mut s, &[b"LOLWUT", b"VERSION", b"notanint"]) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR value is not an integer or out of range"),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn unknown_command_is_byte_exact() {
    let c = ctx(None);
    let mut s = state(&c);
    let v = run(&c, &mut s, &[b"FROBNICATE", b"a", b"b"]);
    match v {
        Value::Error(e) => assert_eq!(
            e.line(),
            "-ERR unknown command 'FROBNICATE', with args beginning with: 'a' 'b' "
        ),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn hello_no_version_keeps_proto_and_returns_map() {
    let c = ctx(None);
    let mut s = state(&c);
    let v = run(&c, &mut s, &[b"HELLO"]);
    assert!(matches!(v, Value::Map(_)));
    assert_eq!(s.proto, ProtoVersion::Resp2);
}

#[test]
fn hello_3_upgrades_proto() {
    let c = ctx(None);
    let mut s = state(&c);
    let v = run(&c, &mut s, &[b"HELLO", b"3"]);
    assert!(matches!(v, Value::Map(_)));
    assert_eq!(s.proto, ProtoVersion::Resp3);
}

#[test]
fn hello_bad_version_is_noproto_and_does_not_switch() {
    let c = ctx(None);
    let mut s = state(&c);
    let v = run(&c, &mut s, &[b"HELLO", b"4"]);
    match v {
        Value::Error(e) => assert_eq!(e.line(), "-NOPROTO unsupported protocol version"),
        other => panic!("expected NOPROTO, got {other:?}"),
    }
    assert_eq!(s.proto, ProtoVersion::Resp2);
}

#[test]
fn hello_with_setname() {
    let c = ctx(None);
    let mut s = state(&c);
    let _ = run(&c, &mut s, &[b"HELLO", b"3", b"SETNAME", b"app1"]);
    assert_eq!(s.name, "app1");
    assert_eq!(s.proto, ProtoVersion::Resp3);
}

#[test]
fn hello_auth_success_and_failure() {
    let c = ctx(Some("s3cr3t"));
    let mut s = state(&c);
    // Wrong pass -> wrongpass, proto unchanged, not authenticated.
    let v = run(&c, &mut s, &[b"HELLO", b"3", b"AUTH", b"default", b"nope"]);
    assert!(matches!(v, Value::Error(_)));
    assert!(!s.authenticated);
    // Correct pass -> map, authenticated, proto upgraded.
    let v = run(
        &c,
        &mut s,
        &[b"HELLO", b"3", b"AUTH", b"default", b"s3cr3t"],
    );
    assert!(matches!(v, Value::Map(_)));
    assert!(s.authenticated);
    assert_eq!(s.proto, ProtoVersion::Resp3);
}

#[test]
fn auth_no_password_configured() {
    let c = ctx(None);
    let mut s = state(&c);
    let v = run(&c, &mut s, &[b"AUTH", b"whatever"]);
    match v {
        Value::Error(e) => assert!(e.line().starts_with(
            "-ERR AUTH <password> called without any password configured for the default user"
        )),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn noauth_gate_blocks_until_authenticated() {
    let c = ctx(Some("pw"));
    let mut s = state(&c);
    // PING before auth is refused.
    let v = run(&c, &mut s, &[b"PING"]);
    match v {
        Value::Error(e) => assert_eq!(e.line(), "-NOAUTH Authentication required."),
        other => panic!("expected NOAUTH, got {other:?}"),
    }
    // AUTH then PING works.
    assert_eq!(run(&c, &mut s, &[b"AUTH", b"pw"]), Value::ok());
    assert_eq!(run(&c, &mut s, &[b"PING"]), Value::simple("PONG"));
}

#[test]
fn auth_correct_password_succeeds_wrong_password_is_wrongpass() {
    // The constant-time compare must still be CORRECT: the exact password
    // authenticates, and any mismatch (wrong content, or a prefix/suffix of the
    // secret) is WRONGPASS. We cannot test timing here, only that the constant-time
    // path returns the right answer.
    let c = ctx(Some("s3cr3t"));
    // Correct password authenticates.
    let mut ok = state(&c);
    assert_eq!(run(&c, &mut ok, &[b"AUTH", b"s3cr3t"]), Value::ok());
    // A same-length wrong password is WRONGPASS.
    let mut bad = state(&c);
    match run(&c, &mut bad, &[b"AUTH", b"s3cr3T"]) {
        Value::Error(e) => assert_eq!(
            e.line(),
            "-WRONGPASS invalid username-password pair or user is disabled."
        ),
        other => panic!("expected WRONGPASS, got {other:?}"),
    }
    // A shorter password sharing the secret's prefix is WRONGPASS (length differs).
    let mut shortp = state(&c);
    match run(&c, &mut shortp, &[b"AUTH", b"s3cr3"]) {
        Value::Error(e) => assert!(e.line().starts_with("-WRONGPASS")),
        other => panic!("expected WRONGPASS, got {other:?}"),
    }
    // A longer password with the secret as a prefix is WRONGPASS.
    let mut longp = state(&c);
    match run(&c, &mut longp, &[b"AUTH", b"s3cr3t!"]) {
        Value::Error(e) => assert!(e.line().starts_with("-WRONGPASS")),
        other => panic!("expected WRONGPASS, got {other:?}"),
    }
}

#[test]
fn requirepass_stored_as_hash_not_plaintext() {
    // SECURITY (#65): the runtime overlay the auth path reads holds ONLY the SHA-256
    // hex digest of the password, never the plaintext.
    let c = ctx(Some("s3cr3t"));
    let stored = c.runtime.requirepass().expect("requirepass should be set");
    assert_eq!(stored, ironcache_config::sha256_hex(b"s3cr3t"));
    assert_eq!(stored.len(), 64);
    assert_ne!(stored, "s3cr3t");
    // The boot config likewise holds the digest, not the plaintext.
    assert_eq!(
        c.boot.requirepass.as_deref(),
        Some(ironcache_config::sha256_hex(b"s3cr3t").as_str())
    );
    assert_ne!(c.boot.requirepass.as_deref(), Some("s3cr3t"));
}

#[test]
fn config_set_requirepass_then_auth_with_plaintext_succeeds() {
    // SECURITY (#65): hash-on-set (CONFIG SET) and hash-on-verify (AUTH) converge.
    // A CONFIG SET requirepass <plaintext> stores the digest; AUTH with that SAME
    // plaintext then authenticates (the guess is hashed and matches the stored
    // digest), while a wrong plaintext is WRONGPASS.
    let c = ctx(None);
    let mut admin = state(&c);
    // No password yet: AUTH reports no-password-configured.
    match run(&c, &mut admin, &[b"AUTH", b"newpass"]) {
        Value::Error(e) => assert!(e.line().starts_with(
            "-ERR AUTH <password> called without any password configured for the default user"
        )),
        other => panic!("expected auth_no_password_set, got {other:?}"),
    }
    // CONFIG SET requirepass with a plaintext password.
    assert_eq!(
        run(
            &c,
            &mut admin,
            &[b"CONFIG", b"SET", b"requirepass", b"newpass"]
        ),
        Value::ok()
    );
    // The overlay now holds the DIGEST, not the plaintext.
    assert_eq!(
        c.runtime.requirepass().as_deref(),
        Some(ironcache_config::sha256_hex(b"newpass").as_str())
    );
    // A fresh connection (built once a password is configured) starts unauthenticated.
    let mut fresh = state(&c);
    assert!(!fresh.authenticated);
    assert_eq!(run(&c, &mut fresh, &[b"AUTH", b"newpass"]), Value::ok());
    assert!(fresh.authenticated);
    // A wrong plaintext is a digest mismatch -> WRONGPASS.
    let mut wrong = state(&c);
    match run(&c, &mut wrong, &[b"AUTH", b"nope"]) {
        Value::Error(e) => assert!(e.line().starts_with("-WRONGPASS")),
        other => panic!("expected WRONGPASS, got {other:?}"),
    }
}

#[test]
fn constant_time_eq_matches_naive_equality() {
    // The hand-rolled constant-time compare agrees with naive equality on a spread
    // of length/content cases (correctness of the timing-safe path).
    let cases: &[(&[u8], &[u8])] = &[
        (b"", b""),
        (b"a", b"a"),
        (b"a", b"b"),
        (b"", b"x"),
        (b"abc", b"ab"),
        (b"abc", b"abc"),
        (b"abc", b"abd"),
        (b"secret", b"secret"),
        (b"secret", b"Secret"),
    ];
    for &(a, b) in cases {
        assert_eq!(
            constant_time_eq(a, b),
            a == b,
            "constant_time_eq disagreed for {a:?} vs {b:?}"
        );
    }
}

#[test]
fn select_range_validation() {
    let c = ctx(None);
    let mut s = state(&c);
    assert_eq!(run(&c, &mut s, &[b"SELECT", b"3"]), Value::ok());
    assert_eq!(s.db, 3);
    match run(&c, &mut s, &[b"SELECT", b"16"]) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR DB index is out of range"),
        other => panic!("expected range error, got {other:?}"),
    }
    match run(&c, &mut s, &[b"SELECT", b"-1"]) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR DB index is out of range"),
        other => panic!("expected range error, got {other:?}"),
    }
    match run(&c, &mut s, &[b"SELECT", b"abc"]) {
        Value::Error(e) => assert!(e.line().contains("not an integer")),
        other => panic!("expected int error, got {other:?}"),
    }
}

#[test]
fn quit_sets_close_and_replies_ok() {
    let c = ctx(None);
    let mut s = state(&c);
    assert_eq!(run(&c, &mut s, &[b"QUIT"]), Value::ok());
    assert!(s.should_close);
}

#[test]
fn reset_clears_state() {
    let c = ctx(None);
    let mut s = state(&c);
    let _ = run(&c, &mut s, &[b"HELLO", b"3", b"SETNAME", b"x"]);
    let _ = run(&c, &mut s, &[b"SELECT", b"5"]);
    let v = run(&c, &mut s, &[b"RESET"]);
    assert_eq!(v, Value::SimpleString("RESET".to_owned()));
    assert_eq!(s.proto, ProtoVersion::Resp2);
    assert_eq!(s.db, 0);
    assert_eq!(s.name, "");
}

#[test]
fn client_subcommands() {
    let c = ctx(None);
    let mut s = state(&c);
    assert_eq!(run(&c, &mut s, &[b"CLIENT", b"ID"]), Value::Integer(7));
    assert_eq!(
        run(&c, &mut s, &[b"CLIENT", b"SETNAME", b"app"]),
        Value::ok()
    );
    assert_eq!(s.name, "app");
    assert_eq!(
        run(&c, &mut s, &[b"CLIENT", b"GETNAME"]),
        Value::bulk_str("app")
    );
    // Name with space rejected.
    assert!(matches!(
        run(&c, &mut s, &[b"CLIENT", b"SETNAME", b"a b"]),
        Value::Error(_)
    ));
    // INFO is a bulk string mentioning the id.
    match run(&c, &mut s, &[b"CLIENT", b"INFO"]) {
        Value::BulkString(Some(b)) => {
            assert!(String::from_utf8_lossy(&b).contains("id=7"));
        }
        other => panic!("expected bulk, got {other:?}"),
    }
}

#[test]
fn command_stubs_well_formed() {
    let c = ctx(None);
    let mut s = state(&c);
    // Bare COMMAND now returns the REAL command table (one flat entry per client command), not
    // an empty array (#158: cluster clients build their key-routing table from this).
    let table = run(&c, &mut s, &[b"COMMAND"]);
    let Value::Array(Some(entries)) = table else {
        panic!("COMMAND must be a non-null array, got {table:?}");
    };
    assert!(
        entries.len() > 100,
        "COMMAND table looks truncated: {} entries",
        entries.len()
    );
    // COUNT now reports the real count (matching the table length), not 0.
    assert_eq!(
        run(&c, &mut s, &[b"COMMAND", b"COUNT"]),
        Value::Integer(crate::command_spec::CLIENT_COMMAND_NAMES.len() as i64)
    );
    // DOCS stays an empty (well-formed) map.
    assert!(matches!(
        run(&c, &mut s, &[b"COMMAND", b"DOCS"]),
        Value::Map(_)
    ));
}

/// #158: a COMMAND INFO entry carries the key positions a cluster client routes from. GET is a
/// single-key readonly command at (first=1, last=1, step=1); MGET is variadic (1, -1, 1); MSET
/// strides by 2 (1, -1, 2). A wrong shape here re-breaks cluster-client MOVED-routing.
#[test]
fn command_info_carries_key_positions_for_routing() {
    let c = ctx(None);
    let mut s = state(&c);
    // Helper: pull the (arity, first, last, step) ints from a COMMAND INFO <name> entry.
    let probe = |conn: &mut ConnState, name: &[u8]| -> (i64, i64, i64, i64) {
        let reply = run(&c, conn, &[b"COMMAND", b"INFO", name]);
        let Value::Array(Some(items)) = reply else {
            panic!("COMMAND INFO must be an array, got {reply:?}");
        };
        assert_eq!(items.len(), 1, "one requested name -> one entry");
        let Value::Array(Some(entry)) = &items[0] else {
            panic!("entry must be an array, got {:?}", items[0]);
        };
        let int = |idx: usize| match &entry[idx] {
            Value::Integer(num) => *num,
            other => panic!("field {idx} must be an integer, got {other:?}"),
        };
        // [name, arity, flags, first, last, step, ...]
        (int(1), int(3), int(4), int(5))
    };
    assert_eq!(probe(&mut s, b"GET"), (2, 1, 1, 1));
    assert_eq!(probe(&mut s, b"MGET"), (-2, 1, -1, 1));
    assert_eq!(probe(&mut s, b"MSET"), (-3, 1, -1, 2));
    // An unknown command -> a NULL array element (Redis parity).
    let v = run(&c, &mut s, &[b"COMMAND", b"INFO", b"NOSUCHCMD"]);
    assert!(
        matches!(&v, Value::Array(Some(items)) if items.len() == 1 && matches!(items[0], Value::Array(None))),
        "unknown COMMAND INFO name must be a null element, got {v:?}"
    );
}

/// #158: COMMAND GETKEYS extracts the routable keys via the registry's key-spec (the cluster
/// client's movable-key fallback). MSET strides; ZUNIONSTORE resolves numkeys; GET yields one.
#[test]
fn command_getkeys_extracts_routable_keys() {
    let c = ctx(None);
    let mut s = state(&c);
    let keys = |s: &mut ConnState, args: &[&[u8]]| -> Vec<String> {
        let mut full: Vec<&[u8]> = vec![b"COMMAND", b"GETKEYS"];
        full.extend_from_slice(args);
        match run(&c, s, &full) {
            Value::Array(Some(items)) => items
                .iter()
                .map(|i| match i {
                    Value::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                    other => panic!("key must be a bulk string, got {other:?}"),
                })
                .collect(),
            other => panic!("GETKEYS must be an array, got {other:?}"),
        }
    };
    assert_eq!(keys(&mut s, &[b"GET", b"foo"]), vec!["foo"]);
    assert_eq!(
        keys(&mut s, &[b"MSET", b"k1", b"v1", b"k2", b"v2"]),
        vec!["k1", "k2"]
    );
    assert_eq!(
        keys(&mut s, &[b"ZUNIONSTORE", b"dst", b"2", b"a", b"b"]),
        vec!["dst", "a", "b"]
    );
}

#[test]
fn info_delegates_and_includes_port() {
    let c = ctx(None);
    let mut s = state(&c);
    match run(&c, &mut s, &[b"INFO"]) {
        Value::BulkString(Some(b)) => {
            assert!(String::from_utf8_lossy(&b).contains("tcp_port:6379"));
        }
        other => panic!("expected bulk, got {other:?}"),
    }
}

/// The INFO body as a `String` (the test reads the bulk reply text).
fn info_text(c: &ServerContext, s: &mut ConnState, section: &[&[u8]]) -> String {
    let mut args: Vec<&[u8]> = vec![b"INFO"];
    args.extend_from_slice(section);
    match run(c, s, &args) {
        Value::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("expected bulk INFO, got {other:?}"),
    }
}

/// HA-7e: with NO repl status cell (the default static path), INFO `# Replication` reports the
/// byte-compatible standalone posture: role:master, connected_slaves:0, master_repl_offset:0.
#[test]
fn info_replication_default_is_standalone_master() {
    let c = ctx(None); // ctx has repl_status: None
    let mut s = state(&c);
    let body = info_text(&c, &mut s, &[b"replication"]);
    assert!(body.contains("# Replication\r\n"), "{body}");
    assert!(body.contains("role:master\r\n"), "{body}");
    assert!(body.contains("connected_slaves:0\r\n"), "{body}");
    assert!(body.contains("master_repl_offset:0\r\n"), "{body}");
    assert!(!body.contains("slave0:"), "{body}");
}

/// HA-7e: a master with a connected replica reports connected_slaves:1 + a slave0: line with
/// the slave's offset + lag.
#[test]
fn info_replication_master_with_connected_slave() {
    let mut c = ctx(None);
    let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
    status.set_master_head(ironcache_repl::ReplOffset(200));
    status.set_replica(0, ironcache_repl::ReplOffset(190)); // lag 10, no advertised id
    c.repl_status = Some(status);
    let mut s = state(&c);
    let body = info_text(&c, &mut s, &[b"replication"]);
    assert!(body.contains("role:master\r\n"), "{body}");
    assert!(body.contains("connected_slaves:1\r\n"), "{body}");
    // The slaveN line carries the offset + lag (the endpoint is a placeholder in the MVP
    // handshake; the offset/lag are the load-bearing fields).
    assert!(
        body.contains("state=online,offset=190,lag=10\r\n"),
        "{body}"
    );
    assert!(body.contains("master_repl_offset:200\r\n"), "{body}");
}

/// #365 stage 3: with the replica's advertised id captured (stage 2) AND the cluster slot map
/// holding that member, the `slaveN` line reports the replica's REAL advertised endpoint,
/// resolved via `node_id_from_announce`, not the `ip=,port=0` placeholder.
#[test]
fn info_replication_resolves_the_replica_endpoint_from_the_slot_map() {
    // The replica advertised this 40-hex announce id; its NodeId is the first 16 hex.
    let replica_id = "aaaaaaaaaaaaaaaa000000000000000000000000";
    let node_id = ironcache_raft_net::node_id_from_announce(replica_id).0;
    let self_id = "1111111111111111111111111111111111111111";
    let map = ironcache_cluster::SlotMap::build(
        vec![
            (
                ironcache_cluster::NodeEntry {
                    id: self_id.into(),
                    host: "10.0.0.1".into(),
                    port: 7001,
                },
                vec![[0, 16383]],
            ),
            (
                ironcache_cluster::NodeEntry {
                    id: replica_id.into(),
                    host: "10.0.0.5".into(),
                    port: 7005,
                },
                vec![],
            ),
        ],
        self_id,
    )
    .expect("a full map with the replica as a no-slot member is valid");

    let mut c = ctx(None);
    c.cluster = Some(std::sync::Arc::new(map));
    let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
    status.set_master_head(ironcache_repl::ReplOffset(200));
    status.set_replica(node_id, ironcache_repl::ReplOffset(190)); // lag 10, captured at attach
    c.repl_status = Some(status);

    let mut s = state(&c);
    let body = info_text(&c, &mut s, &[b"replication"]);
    assert!(
        body.contains("slave0:ip=10.0.0.5,port=7005,state=online,offset=190,lag=10\r\n"),
        "the slaveN line resolves the replica's real endpoint: {body}"
    );
}

/// #365 stage 3 fallback: without a cluster (standalone) the endpoint stays the `ip=,port=0`
/// placeholder; the offset/lag are still real, so an operator loses nothing load-bearing.
#[test]
fn info_replication_replica_endpoint_is_a_placeholder_without_a_cluster() {
    let mut c = ctx(None); // no cluster set
    let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
    status.set_master_head(ironcache_repl::ReplOffset(200));
    status.set_replica(0xABCD, ironcache_repl::ReplOffset(190)); // an id, but no cluster to resolve it
    c.repl_status = Some(status);
    let mut s = state(&c);
    let body = info_text(&c, &mut s, &[b"replication"]);
    assert!(
        body.contains("slave0:ip=,port=0,state=online,offset=190,lag=10\r\n"),
        "{body}"
    );
}

/// HA-7e: a replica reports role:replica, its master endpoint, master_link_status, the offsets,
/// and slave_read_only:1.
#[test]
fn info_replication_replica_view() {
    let mut c = ctx(None);
    let status = std::sync::Arc::new(ironcache_repl::ReplNodeStatus::new());
    status.set_replica_attached("10.0.0.9", 6400, ironcache_repl::ReplOffset(50));
    status.set_observed_master_head(ironcache_repl::ReplOffset(60));
    status.set_replica_applied(ironcache_repl::ReplOffset(58));
    c.repl_status = Some(status);
    let mut s = state(&c);
    let body = info_text(&c, &mut s, &[b"replication"]);
    assert!(body.contains("role:replica\r\n"), "{body}");
    assert!(body.contains("master_host:10.0.0.9\r\n"), "{body}");
    assert!(body.contains("master_port:6400\r\n"), "{body}");
    assert!(body.contains("master_link_status:up\r\n"), "{body}");
    assert!(body.contains("slave_read_only:1\r\n"), "{body}");
    assert!(body.contains("slave_repl_offset:58\r\n"), "{body}");
    assert!(body.contains("master_repl_offset:60\r\n"), "{body}");
}

// -- Data commands (PR-2a) through dispatch over a real ShardStore. --

fn bulk(b: &[u8]) -> Value {
    Value::BulkString(Some(Bytes::copy_from_slice(b)))
}

#[test]
fn set_then_get_round_trips() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"foo", b"bar"]),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"foo"]),
        bulk(b"bar")
    );
    // Missing key -> null.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"nope"]),
        Value::Null
    );
}

#[test]
fn set_nx_only_when_absent() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v1", b"NX"]),
        Value::ok()
    );
    // Second NX on a present key -> nil, value unchanged.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2", b"NX"]),
        Value::Null
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v1"));
}

#[test]
fn set_xx_only_when_present() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // XX on absent key -> nil, nothing written.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v", b"XX"]),
        Value::Null
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
    // Create, then XX overwrite works.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2", b"XX"]),
        Value::ok()
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v2"));
}

#[test]
fn set_get_returns_old_value() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"old"]);
    // SET k new XX GET -> returns old, writes new.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", b"k", b"new", b"XX", b"GET"]
        ),
        bulk(b"old")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]),
        bulk(b"new")
    );
    // SET GET on an absent key returns null and writes the new value.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"fresh", b"v", b"GET"]),
        Value::Null
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"fresh"]),
        bulk(b"v")
    );
}

#[test]
fn set_keepttl_preserves_deadline() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    // Set with a 100-second TTL at t=0 (deadline 100000ms).
    run_on(
        &c,
        &mut s,
        &mut st,
        UnixMillis(0),
        &[b"SET", b"k", b"a", b"EX", b"100"],
    );
    // KEEPTTL overwrite at t=1000: value changes, deadline preserved.
    run_on(
        &c,
        &mut s,
        &mut st,
        UnixMillis(1_000),
        &[b"SET", b"k", b"b", b"KEEPTTL"],
    );
    // Alive AT the original deadline (Valkey boundary is `now > deadline`).
    assert_eq!(
        run_on(&c, &mut s, &mut st, UnixMillis(100_000), &[b"GET", b"k"]),
        bulk(b"b")
    );
    // Expired one ms past the original deadline (KEEPTTL kept it, did not extend).
    assert_eq!(
        run_on(&c, &mut s, &mut st, UnixMillis(100_001), &[b"GET", b"k"]),
        Value::Null
    );
}

#[test]
fn set_ex_stores_deadline_and_lazy_expires() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    // EX 10 at t=0 -> deadline 10000ms.
    run_on(
        &c,
        &mut s,
        &mut st,
        UnixMillis(0),
        &[b"SET", b"k", b"v", b"EX", b"10"],
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, UnixMillis(9_999), &[b"GET", b"k"]),
        bulk(b"v")
    );
    // Alive AT the deadline (Valkey boundary is `now > deadline`).
    assert_eq!(
        run_on(&c, &mut s, &mut st, UnixMillis(10_000), &[b"GET", b"k"]),
        bulk(b"v")
    );
    // Expired one ms past the deadline.
    assert_eq!(
        run_on(&c, &mut s, &mut st, UnixMillis(10_001), &[b"GET", b"k"]),
        Value::Null
    );
}

#[test]
fn set_conflicting_options_is_syntax_error() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for opts in [
        vec![b"SET".as_slice(), b"k", b"v", b"NX", b"XX"],
        vec![b"SET", b"k", b"v", b"EX", b"1", b"PX", b"1"],
        vec![b"SET", b"k", b"v", b"EX", b"1", b"KEEPTTL"],
        vec![b"SET", b"k", b"v", b"BOGUS"],
    ] {
        match run_on(&c, &mut s, &mut st, t, &opts) {
            Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error", "{opts:?}"),
            other => panic!("expected syntax error for {opts:?}, got {other:?}"),
        }
    }
}

#[test]
fn set_non_positive_or_overflowing_expire_is_invalid_expire_time() {
    // Redis emits `-ERR invalid expire time in 'set' command` (a class DISTINCT
    // from syntax error) for an EX/PX/EXAT/PXAT value <= 0 or one that overflows
    // the millisecond computation. Nothing is written.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for opts in [
        vec![b"SET".as_slice(), b"k", b"v", b"EX", b"0"],
        vec![b"SET", b"k", b"v", b"EX", b"-1"],
        vec![b"SET", b"k", b"v", b"PX", b"0"],
        vec![b"SET", b"k", b"v", b"EXAT", b"0"],
        vec![b"SET", b"k", b"v", b"PXAT", b"0"],
        // EX * 1000 overflows i64 -> invalid expire (an integer, but out of ms range).
        vec![b"SET", b"k", b"v", b"EX", b"9223372036854775807"],
    ] {
        match run_on(&c, &mut s, &mut st, t, &opts) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR invalid expire time in 'set' command",
                "{opts:?}"
            ),
            other => panic!("expected invalid expire time for {opts:?}, got {other:?}"),
        }
    }
    // No key was ever written.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
}

#[test]
fn set_non_integer_expire_is_not_an_integer_error() {
    // A NON-integer expire argument is the shared not-an-integer error, thrown
    // BEFORE the <= 0 check (a distinct class from invalid expire time). A
    // leading '+' is also rejected (Redis string2ll rejects '+').
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for opts in [
        vec![b"SET".as_slice(), b"k", b"v", b"EX", b"abc"],
        vec![b"SET", b"k", b"v", b"PX", b"1.5"],
        vec![b"SET", b"k", b"v", b"EX", b"+5"],
    ] {
        match run_on(&c, &mut s, &mut st, t, &opts) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-ERR value is not an integer or out of range",
                "{opts:?}"
            ),
            other => panic!("expected not-an-integer for {opts:?}, got {other:?}"),
        }
    }
}

#[test]
fn setnx_and_getset() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SETNX", b"k", b"v1"]),
        Value::Integer(1)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SETNX", b"k", b"v2"]),
        Value::Integer(0)
    );
    // GETSET returns old and writes new.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"k", b"v3"]),
        bulk(b"v1")
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"v3"));
    // GETSET on absent key returns null.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"new", b"x"]),
        Value::Null
    );
}

#[test]
fn del_and_exists_variadic_counts() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
    // EXISTS counts repeats (Redis semantics).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"EXISTS", b"a", b"a", b"b", b"missing"]
        ),
        Value::Integer(3)
    );
    // DEL removes live keys, returns count removed.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"DEL", b"a", b"b", b"missing"]),
        Value::Integer(2)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"a", b"b"]),
        Value::Integer(0)
    );
}

#[test]
fn type_and_strlen() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
        Value::simple("none")
    );
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"hello"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
        Value::simple("string")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"k"]),
        Value::Integer(5)
    );
    // STRLEN of an int value is the decimal length.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"-12345"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
        Value::Integer(6)
    );
    // STRLEN of an absent key is 0.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"gone"]),
        Value::Integer(0)
    );
}

#[test]
fn wrongtype_on_get_against_non_string() {
    use ironcache_storage::{DataType, Encoding};
    use ironcache_store::kvobj::{Header, KvObj, ValueRepr};

    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);

    // Plant a non-String value directly (PR-2a commands only ever produce
    // Strings, so this is the only way to reach the WRONGTYPE branch before
    // collections land). A List-typed kvobj under key "lst".
    let mut obj = KvObj::from_bytes(b"lst", b"x", None);
    obj.header = Header {
        data_type: DataType::List,
        encoding: Encoding::ListPack,
        eviction_rank: 0,
        ttl_present: false,
        snapshot_version: 0,
    };
    obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
    st.insert_object(0, obj);

    // GET / STRLEN / GETSET against the non-string -> WRONGTYPE.
    match run_on(&c, &mut s, &mut st, t, &[b"GET", b"lst"]) {
        Value::Error(e) => assert_eq!(
            e.line(),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        ),
        other => panic!("expected WRONGTYPE, got {other:?}"),
    }
    assert!(matches!(
        run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"lst"]),
        Value::Error(_)
    ));
    assert!(matches!(
        run_on(&c, &mut s, &mut st, t, &[b"GETSET", b"lst", b"v"]),
        Value::Error(_)
    ));
    // TYPE never returns WRONGTYPE; it reports the type name.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"lst"]),
        Value::simple("list")
    );
}

#[test]
fn mget_returns_null_for_missing_and_non_string_never_wrongtype() {
    use ironcache_storage::{DataType, Encoding};
    use ironcache_store::kvobj::{Header, KvObj, ValueRepr};

    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);

    // A real string, a missing key, and a non-string (list) value.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"hi"]),
        Value::ok()
    );
    let mut obj = KvObj::from_bytes(b"lst", b"x", None);
    obj.header = Header {
        data_type: DataType::List,
        encoding: Encoding::ListPack,
        eviction_rank: 0,
        ttl_present: false,
        snapshot_version: 0,
    };
    obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
    st.insert_object(0, obj);

    // MGET str missing lst -> [bulk("hi"), Null, Null]. The non-string yields Null,
    // NOT a WRONGTYPE error (MGET never errors on a wrong-type element, matching Redis).
    let reply = run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"MGET", b"str", b"missing", b"lst"],
    );
    assert_eq!(
        reply,
        Value::Array(Some(vec![
            Value::BulkString(Some(bytes::Bytes::from_static(b"hi"))),
            Value::Null,
            Value::Null,
        ])),
        "MGET: present string -> bulk, missing -> Null, non-string -> Null (no WRONGTYPE)"
    );

    // MGET arity: bare MGET (no key) is the wrong-arity error.
    match run_on(&c, &mut s, &mut st, t, &[b"MGET"]) {
        Value::Error(e) => {
            assert_eq!(
                e.line(),
                "-ERR wrong number of arguments for 'mget' command"
            );
        }
        other => panic!("bare MGET must be wrong-arity, got {other:?}"),
    }
}

#[test]
fn mset_sets_pairs_clears_ttl_and_rejects_odd_args() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);

    // A pre-existing key WITH a TTL, to prove MSET clears it (default SET semantics).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", b"k1", b"old", b"EX", b"100"]
        ),
        Value::ok()
    );
    assert!(
        matches!(run_on(&c, &mut s, &mut st, t, &[b"TTL", b"k1"]), Value::Integer(n) if n > 0),
        "k1 has a TTL before MSET"
    );

    // MSET k1 v1 k2 v2 -> +OK; overwrites k1 (clearing its TTL) and creates k2.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"MSET", b"k1", b"v1", b"k2", b"v2"]
        ),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"k1"]),
        Value::BulkString(Some(bytes::Bytes::from_static(b"v1")))
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"k2"]),
        Value::BulkString(Some(bytes::Bytes::from_static(b"v2")))
    );
    // TTL cleared by MSET (-1 = no expire).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"TTL", b"k1"]),
        Value::Integer(-1),
        "MSET must CLEAR the existing TTL (default SET semantics)"
    );

    // Odd arg count (argc-1 odd) -> wrong-arity error.
    match run_on(&c, &mut s, &mut st, t, &[b"MSET", b"a", b"1", b"b"]) {
        Value::Error(e) => {
            assert_eq!(
                e.line(),
                "-ERR wrong number of arguments for 'mset' command"
            );
        }
        other => panic!("odd-arg MSET must be wrong-arity, got {other:?}"),
    }
    // Bare MSET (no pair) -> wrong-arity too.
    assert!(matches!(
        run_on(&c, &mut s, &mut st, t, &[b"MSET"]),
        Value::Error(_)
    ));
}

#[test]
fn arity_errors_on_data_commands() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for cmd in [
        vec![b"GET".as_slice()],
        vec![b"SET", b"k"],
        vec![b"DEL"],
        vec![b"EXISTS"],
        vec![b"TYPE"],
        vec![b"STRLEN"],
        vec![b"SETNX", b"k"],
        vec![b"GETSET", b"k"],
        // PR-2b numeric/append arity.
        vec![b"INCR"],
        vec![b"DECR", b"a", b"b"],
        vec![b"INCRBY", b"k"],
        vec![b"DECRBY", b"k"],
        vec![b"INCRBYFLOAT", b"k"],
        vec![b"APPEND", b"k"],
    ] {
        assert!(
            matches!(run_on(&c, &mut s, &mut st, t, &cmd), Value::Error(_)),
            "expected arity error for {cmd:?}"
        );
    }
}

// -- Numeric RMW + APPEND (PR-2b). --

/// The store-level encoding of `key` in db 0 (for int-encoding assertions). The
/// command layer only ever sees bytes; the test reaches the store directly to
/// confirm the result is stored int-encoded, which is the ENCODINGS.md contract.
fn encoding_of(st: &mut TestStore, key: &[u8]) -> Option<ironcache_storage::Encoding> {
    st.read(0, key, UnixMillis(0)).map(|v| v.encoding())
}

fn err_line(v: Value) -> String {
    match v {
        Value::Error(e) => e.line(),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn incr_decr_from_absent_and_existing() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Absent key starts at 0: INCR -> 1.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCR", b"n"]),
        Value::Integer(1)
    );
    // The result is int-encoded.
    assert_eq!(
        encoding_of(&mut st, b"n"),
        Some(ironcache_storage::Encoding::Int)
    );
    // STRLEN reflects the decimal length of the result.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
        Value::Integer(1)
    );
    // INCRBY and DECR/DECRBY.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"5"]),
        Value::Integer(6)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"DECR", b"n"]),
        Value::Integer(5)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"DECRBY", b"n", b"10"]),
        Value::Integer(-5)
    );
    // After several ops the decimal length is 2 ("-5").
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"STRLEN", b"n"]),
        Value::Integer(2)
    );
    // A negative increment via INCRBY works.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"-5"]),
        Value::Integer(-10)
    );
}

#[test]
fn incr_on_existing_string_set_value() {
    // SET n 10 (stored int-encoded), then INCR/INCRBY/DECR through dispatch.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"10"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCR", b"n"]),
        Value::Integer(11)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"5"]),
        Value::Integer(16)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"DECR", b"n"]),
        Value::Integer(15)
    );
}

#[test]
fn incr_non_integer_value_and_arg_are_not_an_integer() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Non-integer EXISTING value (an embstr) -> not-an-integer.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"s", b"hello"]);
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"s"])),
        "-ERR value is not an integer or out of range"
    );
    // A leading-zero / non-canonical existing string is also rejected (string2ll).
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"z", b"007"]);
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"z"])),
        "-ERR value is not an integer or out of range"
    );
    // Non-integer INCREMENT argument -> not-an-integer.
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"1.5"])),
        "-ERR value is not an integer or out of range"
    );
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"INCRBY", b"n", b"abc"])),
        "-ERR value is not an integer or out of range"
    );
}

#[test]
fn incr_overflow_and_decr_underflow_and_decrby_min_edge() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // INCR of i64::MAX overflows.
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"SET", b"max", b"9223372036854775807"],
    );
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"max"])),
        "-ERR increment or decrement would overflow"
    );
    // DECR of i64::MIN underflows.
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"SET", b"min", b"-9223372036854775808"],
    );
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"DECR", b"min"])),
        "-ERR increment or decrement would overflow"
    );
    // DECRBY key i64::MIN: the decrement cannot be negated -> overflow error.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"x", b"0"]);
    assert_eq!(
        err_line(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"DECRBY", b"x", b"-9223372036854775808"]
        )),
        "-ERR increment or decrement would overflow"
    );
    // The value was not modified by any of the failed ops.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"x"]), bulk(b"0"));
}

#[test]
fn incr_wrongtype_against_non_string() {
    use ironcache_storage::{DataType, Encoding};
    use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let mut obj = KvObj::from_bytes(b"lst", b"x", None);
    obj.header = Header {
        data_type: DataType::List,
        encoding: Encoding::ListPack,
        eviction_rank: 0,
        ttl_present: false,
        snapshot_version: 0,
    };
    obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
    st.insert_object(0, obj);
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"INCR", b"lst"])),
        "-WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    assert_eq!(
        err_line(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"INCRBYFLOAT", b"lst", b"1"]
        )),
        "-WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    assert_eq!(
        err_line(run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"lst", b"x"])),
        "-WRONGTYPE Operation against a key holding the wrong kind of value"
    );
}

#[test]
fn incrbyfloat_wrongtype_beats_invalid_increment() {
    // Redis `incrbyfloatCommand` checks the TYPE before parsing the increment
    // argument, so `INCRBYFLOAT <list-key> abc` is WRONGTYPE, NOT
    // "value is not a valid float" (the malformed increment is irrelevant once
    // the key is the wrong type). Plant a non-string via the store seam as the
    // other WRONGTYPE tests do.
    use ironcache_storage::{DataType, Encoding};
    use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let mut obj = KvObj::from_bytes(b"lst", b"x", None);
    obj.header = Header {
        data_type: DataType::List,
        encoding: Encoding::ListPack,
        eviction_rank: 0,
        ttl_present: false,
        snapshot_version: 0,
    };
    obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
    st.insert_object(0, obj);
    assert_eq!(
        err_line(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"INCRBYFLOAT", b"lst", b"abc"]
        )),
        "-WRONGTYPE Operation against a key holding the wrong kind of value"
    );
}

#[test]
fn incrbyfloat_absent_format_and_storage() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Absent -> 0 + 10.5 = "10.5" (bulk string).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"f", b"10.5"]),
        bulk(b"10.5")
    );
    // Stored as a STRING (its decimal); GET returns the same bytes.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"f"]),
        bulk(b"10.5")
    );
    // Add 0.1 -> "10.6" (shortest round-trip, no trailing zeros).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"f", b"0.1"]),
        bulk(b"10.6")
    );
}

#[test]
fn incrbyfloat_integer_valued_result_round_trips_to_incr() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // 5.0e3 -> "5000" (integer-valued result, no dot), stored as a string that
    // is int-encoded (since "5000" is a canonical integer), so a later INCR
    // works (matching Redis INCRBYFLOAT -> INCR round-trip for integer results).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCRBYFLOAT", b"v", b"5.0e3"]),
        bulk(b"5000")
    );
    assert_eq!(
        encoding_of(&mut st, b"v"),
        Some(ironcache_storage::Encoding::Int)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCR", b"v"]),
        Value::Integer(5001)
    );
}

#[test]
fn incrbyfloat_invalid_float_and_nan_inf() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Non-float existing value -> not-a-valid-float.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"s", b"hello"]);
    assert_eq!(
        err_line(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"INCRBYFLOAT", b"s", b"1.0"]
        )),
        "-ERR value is not a valid float"
    );
    // Non-float increment argument -> not-a-valid-float.
    assert_eq!(
        err_line(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"INCRBYFLOAT", b"f", b"abc"]
        )),
        "-ERR value is not a valid float"
    );
    // An infinite increment produces an infinite result -> NaN/Inf error.
    assert_eq!(
        err_line(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"INCRBYFLOAT", b"f", b"inf"]
        )),
        "-ERR increment would produce NaN or Infinity"
    );
    // None of the failed ops created the key.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"f"]), Value::Null);
}

#[test]
fn append_absent_existing_and_binary_safe() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Absent: APPEND creates and returns len(value).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", b"abc"]),
        Value::Integer(3)
    );
    // Existing string: appends, returns new len.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", b"de"]),
        Value::Integer(5)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"s"]),
        bulk(b"abcde")
    );
    // DIVERGENCE (documented in cmd_append): the frozen waist classifies the
    // rebuilt value by LENGTH, so a SHORT append result is embstr where Redis
    // (which never re-embstrs an appended SDS) would report raw. A result over
    // the embstr threshold is raw, which is the promotion the brief pins; assert
    // that explicitly below.
    assert_eq!(
        encoding_of(&mut st, b"s"),
        Some(ironcache_storage::Encoding::EmbStr)
    );
    // Appending past the embstr threshold promotes the result to raw.
    let big = vec![b'q'; 60];
    run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"s", &big]);
    assert_eq!(
        encoding_of(&mut st, b"s"),
        Some(ironcache_storage::Encoding::Raw)
    );
    // Binary-safe append (NUL bytes preserved).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"\x00\x01"]),
        Value::Integer(2)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"\x02"]),
        Value::Integer(3)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"b"]),
        bulk(b"\x00\x01\x02")
    );
}

#[test]
fn append_promotes_int_off_the_int_encoding() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // SET n 10 is int-encoded; APPEND promotes the concatenation OFF int (to a
    // string encoding). The exact string encoding is length-based in the frozen
    // waist (embstr here for the short "10x"; raw past the threshold).
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"10"]);
    assert_eq!(
        encoding_of(&mut st, b"n"),
        Some(ironcache_storage::Encoding::Int)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"n", b"x"]),
        Value::Integer(3)
    );
    // "10x" is not an integer -> a string encoding (no longer int), and GET sees
    // the decimal+suffix.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"n"]),
        bulk(b"10x")
    );
    assert_ne!(
        encoding_of(&mut st, b"n"),
        Some(ironcache_storage::Encoding::Int),
        "APPEND must promote off the int encoding"
    );
}

// -- maxmemory admission (PR-3a, ADMISSION.md #128, ADR-0007). --

/// Run a command against a caller-owned store with the ceiling ON, returning the
/// reply and the number of keys the admission gate evicted.
fn run_admit(
    ctx: &ServerContext,
    st: &mut ConnState,
    store: &mut TestStore,
    now: UnixMillis,
    parts: &[&[u8]],
) -> (Value, u64) {
    let mut env = TestEnv::new(1);
    let mut wheel = TimingWheel::new();
    let zero = || CounterSnapshot::default();
    let mut deltas = CounterDeltas::default();
    let mut shard_gen = ctx.runtime.generation();
    let reply = dispatch(
        ctx,
        st,
        &mut env,
        store,
        &mut wheel,
        now,
        &mut shard_gen,
        &zero,
        &|| (String::new(), String::new()),
        &|| None,
        MemoryInfo::default(),
        &mut deltas,
        &req(parts),
    );
    (reply, deltas.evicted)
}

/// A context with the ceiling enabled at `per_shard_budget` bytes (single-shard
/// tests, so maxmemory == per_shard_budget). The ceiling is seeded into the runtime
/// overlay (the highest-precedence layer), where the admission gate reads it.
fn ctx_with_budget(per_shard_budget: u64) -> ServerContext {
    ctx_full(None, per_shard_budget, "allkeys-lru")
}

fn err_of(v: Value) -> String {
    match v {
        Value::Error(e) => e.line(),
        other => panic!("expected error, got {other:?}"),
    }
}

/// PROD-SAFETY #1/#2: the over-limit DECISION is driven off the PROCESS-GLOBAL allocator figure
/// (the gauge), not only the per-shard logical counter. With the gauge reporting memory ABOVE
/// `maxmemory`, `over_maxmemory` is true EVEN when this shard's logical `used` is well below its
/// per-shard budget (the host-OOM / hot-shard fixes); with the gauge at or below `maxmemory`
/// (and the shard logically under budget) it is false; and with the gauge UNAVAILABLE (0) it
/// falls back to the per-shard logical-vs-budget test (byte-unchanged).
#[test]
fn over_maxmemory_uses_the_global_allocator_figure() {
    // maxmemory == 1000 (single shard, so per_shard_budget == 1000).
    let c = ctx_with_budget(1000);
    assert_eq!(c.per_shard_budget(), 1000);

    // (a) Gauge ABOVE maxmemory -> OVER, regardless of the shard's tiny logical figure. This is
    // the host-protecting trigger: the real allocator figure (which undercounts ~2x as the
    // logical counter, so the logical 10 here is a fiction vs a real 2000 bytes) drives the
    // decision against the FULL maxmemory.
    c.process_memory.publish(2000, 4096);
    assert!(
        c.over_maxmemory(10),
        "global allocator figure over maxmemory must trigger even with tiny shard-logical bytes"
    );

    // (b) Gauge AT/under maxmemory AND shard under its per-shard budget -> NOT over.
    c.process_memory.publish(500, 1024);
    assert!(
        !c.over_maxmemory(10),
        "under the ceiling on both the global figure and the per-shard logical counter"
    );
    // ... but a per-shard logical OVERSHOOT still triggers (the fallback test still fires even
    // when the global figure is calm, so a local overshoot between gauge refreshes is caught).
    assert!(
        c.over_maxmemory(1001),
        "per-shard logical over budget still triggers regardless of the global figure"
    );

    // (c) Gauge UNAVAILABLE (0, the system-allocator / pre-publish / MSVC case) -> fall back to
    // the per-shard logical-vs-budget test ONLY (byte-unchanged default behavior).
    c.process_memory.publish(0, 0);
    assert!(
        !c.over_maxmemory(1000),
        "used == budget is under-limit (strict >)"
    );
    assert!(
        c.over_maxmemory(1001),
        "used > budget triggers via the logical fallback"
    );

    // maxmemory == 0 (disabled) is never over, whatever the gauge says.
    let off = ctx_with_budget(0);
    off.process_memory.publish(9_999_999, 9_999_999);
    assert!(!off.over_maxmemory(9_999_999));
}

/// PROD-SAFETY #1/#2: end-to-end through the admission gate -- with the allocator gauge over
/// `maxmemory`, a `denyoom` write triggers eviction (cache mode) off the GLOBAL figure even
/// though this shard's logical bytes are under its per-shard budget, and is OOM'd under
/// `noeviction`. The pre-fix code never looked at the allocator figure, so this write would
/// have sailed through and let the host OOM.
#[test]
fn admission_gate_triggers_off_global_allocator_figure() {
    // Strict (noeviction) mode so the trigger surfaces as a clean -OOM (no eviction noise).
    let c = ctx_full(None, 1000, "noeviction");
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::NoEviction);
    let t = UnixMillis(0);
    // The shard is logically near-empty (one tiny key, well under the 1000-byte budget), so the
    // OLD per-shard-logical gate would NOT trigger.
    let (r0, _) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
    assert_eq!(r0, Value::ok());
    assert!(st.used_memory() < 1000, "shard is logically under budget");
    // But the PROCESS allocator figure is over maxmemory (the ~2x undercount the logical
    // counter hides): a denyoom write is now rejected -OOM off the GLOBAL trigger.
    c.process_memory.publish(5000, 8192);
    let (r1, _) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", b"v2"]);
    assert_eq!(
        err_of(r1),
        "-OOM command not allowed when used memory > 'maxmemory'.",
        "the global allocator figure over maxmemory must OOM a denyoom write even when the \
             shard is logically under its per-shard budget"
    );
    // Once the allocator figure drops back under the ceiling, writes are served again.
    c.process_memory.publish(100, 512);
    let (r2, _) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k3", b"v3"]);
    assert_eq!(r2, Value::ok());
}

#[test]
fn noeviction_over_budget_rejects_denyoom_write_with_byte_exact_oom() {
    // Strict datastore mode: a denyoom write at/over the budget gets the exact
    // -OOM string, and nothing is written.
    let c = ctx_with_budget(50);
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::NoEviction);
    let t = UnixMillis(0);
    let big = vec![b'v'; 100];
    // The first SET: used_memory starts at 0 (< 50), so the gate lets it through;
    // the store is now over budget.
    let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
    assert_eq!(r, Value::ok());
    assert_eq!(ev, 0);
    assert!(st.used_memory() >= 50);
    // A SECOND denyoom write is rejected -OOM (byte-exact), nothing evicted.
    let (r2, ev2) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
    assert_eq!(
        err_of(r2),
        "-OOM command not allowed when used memory > 'maxmemory'."
    );
    assert_eq!(ev2, 0, "noeviction evicts nothing");
    // k2 was not written.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"k2"]),
        Value::Null
    );
}

#[test]
fn denyoom_write_at_exactly_used_equals_budget_is_served() {
    // Strict-over semantics (Redis getMaxmemoryState: under-limit at
    // `used <= maxmemory`). With used == budget EXACTLY, a denyoom write is served
    // (the gate's `used > budget` is false), NOT OOM'd, even under `noeviction`.
    let mut probe = store_with(16, Policy::NoEviction);
    let t = UnixMillis(0);
    let big = vec![b'v'; 100];
    // Plant one key with no ceiling, then read the resulting footprint and set the
    // budget to EXACTLY that, so used == budget on the next gated write.
    probe.upsert(
        0,
        b"k",
        ironcache_storage::NewValue::Bytes(&big),
        ironcache_storage::ExpireWrite::Clear,
        t,
    );
    let exact = probe.used_memory();
    assert!(exact > 0);

    let c = ctx_with_budget(exact);
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::NoEviction);
    // Replay the same plant against the gated store so used == budget exactly.
    let (r0, ev0) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
    assert_eq!(r0, Value::ok());
    assert_eq!(ev0, 0);
    assert_eq!(
        st.used_memory(),
        exact,
        "used must equal the budget exactly"
    );

    // A denyoom write that does NOT grow memory (overwrite same key, same size) at
    // used == budget is SERVED: the gate is strict `>`, so used==budget passes.
    let (r1, ev1) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
    assert_eq!(r1, Value::ok(), "at used==budget the write must be served");
    assert_eq!(ev1, 0);

    // Now push STRICTLY over the budget (a second, larger key with no ceiling
    // would not be gated; instead grow via the gated path: the first overwrite was
    // served and left used==budget, so a NEW key now tips strictly over and the
    // NEXT denyoom write is OOM'd under noeviction).
    let (r2, _ev2) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
    // The k2 write happened at used==budget (served), pushing used strictly over.
    assert_eq!(r2, Value::ok());
    assert!(st.used_memory() > exact, "used is now strictly over budget");
    // The FOLLOWING denyoom write is rejected -OOM (strictly over, noeviction).
    let (r3, ev3) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k3", &big]);
    assert_eq!(
        err_of(r3),
        "-OOM command not allowed when used memory > 'maxmemory'."
    );
    assert_eq!(ev3, 0);
}

#[test]
fn cache_mode_at_exactly_budget_serves_without_evicting() {
    // Cache mode mirror of the strict-over boundary: at used == budget the gate is
    // not entered, so evict_to_fit does NOT run and nothing is evicted.
    let mut probe = store_with(16, Policy::cache_default());
    let t = UnixMillis(0);
    let big = vec![b'v'; 100];
    probe.upsert(
        0,
        b"k",
        ironcache_storage::NewValue::Bytes(&big),
        ironcache_storage::ExpireWrite::Clear,
        t,
    );
    let exact = probe.used_memory();

    let c = ctx_with_budget(exact);
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::cache_default());
    run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
    assert_eq!(st.used_memory(), exact);
    // Overwrite at used==budget: served, and the eviction gate did not fire.
    let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
    assert_eq!(r, Value::ok());
    assert_eq!(ev, 0, "at used==budget cache mode must not evict");
}

#[test]
fn reads_and_del_are_served_over_budget() {
    // Non-denyoom commands are ALWAYS served even over budget (a client must be
    // able to read and free under memory pressure).
    let c = ctx_with_budget(50);
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::NoEviction);
    let t = UnixMillis(0);
    let big = vec![b'v'; 100];
    run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", &big]);
    assert!(st.used_memory() >= 50);
    // GET still works over budget.
    let (got_get, _) = run_admit(&c, &mut s, &mut st, t, &[b"GET", b"k"]);
    assert_eq!(
        got_get,
        Value::BulkString(Some(Bytes::copy_from_slice(&big)))
    );
    // DEL (memory-releasing) still works over budget and frees space.
    let (got_del, _) = run_admit(&c, &mut s, &mut st, t, &[b"DEL", b"k"]);
    assert_eq!(got_del, Value::Integer(1));
    assert!(st.used_memory() < 50, "DEL freed space");
}

#[test]
fn cache_mode_over_budget_evicts_then_the_write_succeeds() {
    // Cache mode: a denyoom write at the budget triggers evict-to-fit; once there
    // is room the write proceeds and the evicted count is reported.
    let c = ctx_with_budget(300);
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::cache_default());
    let t = UnixMillis(0);
    let val = vec![b'v'; 100];
    // Write several keys to get over the 300-byte budget.
    for i in 0u32..5 {
        run_admit(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("k{i}").as_bytes(), &val],
        );
    }
    assert!(
        st.used_memory() >= 300,
        "should be over budget after the fills"
    );
    // The next denyoom write evicts to fit, then succeeds.
    let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"new", &val]);
    assert_eq!(r, Value::ok(), "the write should succeed after eviction");
    assert!(ev > 0, "cache mode should have evicted at least one key");
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"new"]),
        Value::BulkString(Some(Bytes::copy_from_slice(&val)))
    );
}

#[test]
fn cache_mode_eviction_clears_oom_even_with_a_stale_high_global_gauge() {
    // M1 REGRESSION GUARD: in cache/evicting mode, the per-command -OOM decision is driven off
    // the FRESH per-shard LOGICAL figure after eviction, NOT the ~100ms-stale process-global
    // allocator gauge. With the gauge pinned ABOVE maxmemory (the near-ceiling case where the
    // gauge has not refreshed and the allocator may still hold freed pages), a denyoom write
    // must STILL SUCCEED once eviction frees logical room -- matching Redis (an evicting policy
    // clears OOM within the command). Pre-M1, the post-eviction re-check used `over_maxmemory`,
    // which ORs the stale-high gauge, so this write was spuriously -OOM'd.
    let c = ctx_with_budget(300);
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::cache_default());
    let t = UnixMillis(0);
    let val = vec![b'v'; 100];
    // Fill past the 300-byte budget so the next write must evict to fit.
    for i in 0u32..5 {
        run_admit(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("k{i}").as_bytes(), &val],
        );
    }
    assert!(st.used_memory() >= 300, "over budget after the fills");
    // Pin the GLOBAL gauge ABOVE maxmemory and keep it there: it never refreshes during this
    // command (it would only move on the next ~100ms expiry tick). This is the stale-high
    // near-ceiling condition that pre-M1 spuriously -OOM'd.
    c.process_memory.publish(5000, 8192);
    assert!(
        c.over_maxmemory(st.used_memory()),
        "the global gauge is over maxmemory (the stale-high trigger condition)"
    );
    // The denyoom write triggers eviction (off the global gauge) and -- the M1 fix -- is then
    // ALLOWED because eviction got the per-shard LOGICAL figure under budget (the post-eviction
    // -OOM decision now reads the FRESH per-shard logical figure, not the stale-high global
    // gauge that still reads over). Pre-M1 this write was spuriously -OOM'd.
    let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"new", &val]);
    assert_eq!(
        r,
        Value::ok(),
        "cache mode must serve the write after eviction frees logical room, despite the \
             stale-high global gauge"
    );
    assert!(ev > 0, "eviction must have run to make room");
    // The global gauge is STILL stale-high (it never moved during the command): proof the
    // success was driven off the per-shard logical figure, not the global gauge.
    assert!(
        c.over_maxmemory(st.used_memory()),
        "the global gauge is still over (it only refreshes on the next tick); the write \
             succeeded off the per-shard logical figure, not this gauge"
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"new"]),
        Value::BulkString(Some(Bytes::copy_from_slice(&val)))
    );
}

#[test]
fn cache_mode_oom_when_eviction_cannot_free_enough_logical_room() {
    // M1 companion: cache mode STILL -OOMs when eviction CANNOT get the per-shard logical figure
    // under budget. Under `volatile-lru` ONLY TTL-bearing keys are evictable; with the store
    // full of NON-TTL keys nothing is evictable, so a denyoom write over budget evicts nothing,
    // the post-eviction per-shard-logical check stays over budget, and the write is correctly
    // -OOM'd (Redis `volatile-*` with no expirable key). This is the "eviction could not free
    // enough" branch, decided off the per-shard logical figure (not the global gauge).
    let c = ctx_full(None, 300, "volatile-lru");
    let mut s = state(&c);
    let mut st = store_with(
        c.databases,
        map_policy_name("volatile-lru", 1).expect("volatile-lru maps"),
    );
    assert!(
        st.policy_evicts(),
        "volatile-lru is an evicting (cache) policy"
    );
    let t = UnixMillis(0);
    let val = vec![b'v'; 100];
    // Fill past the 300-byte budget with NON-TTL keys (no EX), so none are evictable.
    for i in 0u32..5 {
        run_admit(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("k{i}").as_bytes(), &val],
        );
    }
    assert!(st.used_memory() >= 300, "over budget with non-TTL keys");
    // A denyoom write triggers eviction, but nothing is evictable (no TTL keys), so the shard
    // stays logically over budget and the write is -OOM'd.
    let (r, ev) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"new", &val]);
    assert_eq!(ev, 0, "no TTL-bearing key is evictable under volatile-lru");
    assert_eq!(
        err_of(r),
        "-OOM command not allowed when used memory > 'maxmemory'.",
        "cache mode -OOMs when eviction cannot bring the per-shard logical figure under budget"
    );
}

#[test]
fn noeviction_global_rss_gauge_is_the_hard_ceiling() {
    // M1 companion (the NOEVICTION half stays the global-RSS hard ceiling): with the shard
    // logically UNDER its per-shard budget but the process-global allocator gauge OVER maxmemory,
    // a denyoom write under `noeviction` is -OOM'd off the global gauge (no eviction can clear
    // it). This is the host-OOM protection the global trigger exists for, and M1 leaves it
    // intact (M1 only changed the CACHE-mode post-eviction decision).
    let c = ctx_full(None, 1000, "noeviction");
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::NoEviction);
    let t = UnixMillis(0);
    let (r0, _) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
    assert_eq!(r0, Value::ok());
    assert!(st.used_memory() < 1000, "shard is logically under budget");
    // Global gauge over maxmemory while the shard is logically lean: -OOM (hard ceiling).
    c.process_memory.publish(5000, 8192);
    let (r1, _) = run_admit(&c, &mut s, &mut st, t, &[b"SET", b"k2", b"v2"]);
    assert_eq!(
        err_of(r1),
        "-OOM command not allowed when used memory > 'maxmemory'.",
        "noeviction keeps the global RSS gauge as the hard ceiling"
    );
}

#[test]
fn wtinylfu_eviction_preserves_a_hot_key_under_the_ceiling() {
    // End-to-end W-TinyLFU through the real evict_to_fit flow, demonstrating the
    // ACTUAL #57 mechanism (the candidate-admission door): a hot resident survives
    // under memory pressure NOT because it was GET'd (on_access is now a no-op under
    // #57, so GETs build no frequency), but because each cold SET candidate LOSES the
    // admission door and self-evicts (stored-then-evicted), sparing the hot key.
    // Frequency is built on the DECISION PATH only; here the hot key is warmed via
    // REPEATED SETs (each on_insert is a decision-path bump), not GETs.
    let c = ctx_with_budget(400);
    let mut s = state(&c);
    let mut st = store_with(
        c.databases,
        map_policy_name("allkeys-lfu", 1).expect("allkeys-lfu maps"),
    );
    // Sanity: it is genuinely the W-TinyLFU engine, not a stand-in.
    assert_eq!(st.policy_name(), "allkeys-lfu");
    let t = UnixMillis(0);
    let val = vec![b'v'; 100];

    // Warm the hot key via REPEATED SETs: each SET is a decision-path bump
    // (on_insert min-increments the candidate), so the sketch records a high
    // frequency for "hot". (A GET loop would be INERT here under #57.) These early
    // SETs are under the budget, so no eviction yet.
    for _ in 0..20 {
        run_admit(&c, &mut s, &mut st, t, &[b"SET", b"hot", &val]);
    }
    // Now stream many cold keys, each written once. Each cold SET becomes the pending
    // admission candidate; when the write pushes the shard over budget, evict_to_fit
    // runs the door: the cold candidate (estimate ~1) does NOT strictly beat the hot
    // incumbent, so the COLD candidate self-evicts. The hot key is never the victim.
    let mut total_evicted = 0u64;
    for i in 0u32..15 {
        let (_r, ev) = run_admit(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("cold{i}").as_bytes(), &val],
        );
        total_evicted += ev;
    }
    // The hot key must still be present: it survived because the cold SET candidates
    // lost the admission door, NOT because it was read. This is the #57 door
    // mechanism (the SELECTABLE W-TinyLFU variant's scan resistance).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"hot"]),
        Value::BulkString(Some(Bytes::copy_from_slice(&val))),
        "the hot resident survives: cold SET candidates lose the door and self-evict"
    );
    // Eviction actually happened (the budget is small, so the cold flood forced the
    // door to fire). Every victim was a COLD candidate, never the hot incumbent.
    assert!(
        total_evicted > 0,
        "the cold-candidate flood must have driven W-TinyLFU door evictions"
    );
    // The keyspace stayed small (bounded by the budget): far fewer than the 16 keys
    // written, since rejected cold candidates were continually self-evicted.
    assert!(
        st.len() < 8,
        "W-TinyLFU kept the resident set bounded under the ceiling ({} keys)",
        st.len()
    );
}

#[test]
fn ceiling_off_serves_every_write() {
    // maxmemory == 0 (unlimited): the gate is off; writes always succeed.
    let c = ctx(None);
    assert!(!c.ceiling_enabled());
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let big = vec![b'v'; 10_000];
    for i in 0u32..5 {
        let (r, ev) = run_admit(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("k{i}").as_bytes(), &big],
        );
        assert_eq!(r, Value::ok());
        assert_eq!(ev, 0);
    }
}

// -- TTL / EXPIRE family (PR-3b). --

fn int(v: Value) -> i64 {
    match v {
        Value::Integer(n) => n,
        other => panic!("expected integer, got {other:?}"),
    }
}

#[test]
fn expire_sets_ttl_and_ttl_pttl_reflect_it_then_lazy_expires() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    // SET then EXPIRE 10 at t=0 -> deadline 10000ms.
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        UnixMillis(0),
        &[b"SET", b"k", b"v"],
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"EXPIRE", b"k", b"10"]
        )),
        1
    );
    // TTL ~10s, PTTL ~10000ms.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"TTL", b"k"]
        )),
        10
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"PTTL", b"k"]
        )),
        10_000
    );
    // Alive AT the deadline (Valkey boundary now > deadline).
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(10_000),
            &[b"GET", b"k"]
        ),
        bulk(b"v")
    );
    // Expired one ms past the deadline.
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(10_001),
            &[b"GET", b"k"]
        ),
        Value::Null
    );
}

#[test]
fn pexpire_expireat_pexpireat_set_ttl() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"a", b"v"]);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"b", b"v"]);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"d", b"v"]);
    // PEXPIRE a 5000 -> 5000ms.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIRE", b"a", b"5000"]
        )),
        1
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PTTL", b"a"]
        )),
        5_000
    );
    // EXPIREAT b 100 (absolute seconds) -> 100000ms.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIREAT", b"b", b"100"]
        )),
        1
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIRETIME", b"b"]
        )),
        100_000
    );
    // PEXPIREAT d 250000 (absolute ms).
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIREAT", b"d", b"250000"]
        )),
        1
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIRETIME", b"d"]
        )),
        250_000
    );
}

#[test]
fn expire_on_missing_key_replies_zero() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"nope", b"10"]
        )),
        0
    );
}

#[test]
fn expire_past_deadline_deletes_the_key_and_replies_one() {
    // A resolved deadline strictly in the PAST deletes the key and replies 1.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    // now = 100000ms.
    let t = UnixMillis(100_000);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    // EXPIREAT in the past (unix second 1 -> 1000ms, well before now): reply 1,
    // key deleted.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIREAT", b"k", b"1"]
        )),
        1
    );
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
        Value::Null
    );
}

#[test]
fn expire_nx_xx_gt_lt_accept_and_reject() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);

    // NX on a key with NO TTL: applies (reply 1). Sets deadline 10000.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"10", b"NX"]
        )),
        1
    );
    // NX again now that a TTL exists: rejected (reply 0).
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"20", b"NX"]
        )),
        0
    );
    // XX with a TTL present: applies (reply 1). Set to 20000.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"20", b"XX"]
        )),
        1
    );
    // GT with a GREATER new expiry (30 > 20): applies.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"30", b"GT"]
        )),
        1
    );
    // GT with a LESSER new expiry (5 < 30): rejected.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"5", b"GT"]
        )),
        0
    );
    // LT with a LESSER new expiry (5 < 30): applies.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"5", b"LT"]
        )),
        1
    );
    // LT with a GREATER new expiry (100 > 5): rejected.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"100", b"LT"]
        )),
        0
    );
}

#[test]
fn expire_gt_lt_treat_no_ttl_as_infinite() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    // A key with NO TTL is treated as +infinity.
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"g", b"v"]);
    // GT against a no-TTL key NEVER applies (nothing is greater than infinity).
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"g", b"10", b"GT"]
        )),
        0
    );
    // Still no TTL.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"g"]
        )),
        -1
    );
    // LT against a no-TTL key ALWAYS applies (anything is less than infinity).
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"l", b"v"]);
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"l", b"10", b"LT"]
        )),
        1
    );
}

#[test]
fn expire_conflicting_options_are_specific_errors() {
    // The three EXPIRE-option conflicts / the unknown token each map to their
    // SPECIFIC Redis message (src/expire.c parseExtendedExpireArgumentsOrReply),
    // NOT the generic syntax error (the #7 fix).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    let cases: &[(&[&[u8]], &str)] = &[
        (
            &[b"EXPIRE", b"k", b"10", b"NX", b"XX"],
            "-ERR NX and XX, GT or LT options at the same time are not compatible",
        ),
        (
            &[b"EXPIRE", b"k", b"10", b"NX", b"GT"],
            "-ERR NX and XX, GT or LT options at the same time are not compatible",
        ),
        (
            &[b"EXPIRE", b"k", b"10", b"NX", b"LT"],
            "-ERR NX and XX, GT or LT options at the same time are not compatible",
        ),
        (
            &[b"EXPIRE", b"k", b"10", b"GT", b"LT"],
            "-ERR GT and LT options at the same time are not compatible",
        ),
        // The unknown-option token is echoed verbatim.
        (
            &[b"EXPIRE", b"k", b"10", b"BOGUS"],
            "-ERR Unsupported option BOGUS",
        ),
    ];
    for (opts, want) in cases {
        match run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, opts) {
            Value::Error(e) => assert_eq!(&e.line(), want, "{opts:?}"),
            other => panic!("expected {want} for {opts:?}, got {other:?}"),
        }
    }
    // GT+XX and LT+XX are LEGAL (no error). With a TTL present XX is satisfied.
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"EXPIRE", b"k", b"10"],
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"20", b"GT", b"XX"]
        )),
        1
    );
}

#[test]
fn expire_lt_xx_independent_gates_drop_xx_on_no_ttl() {
    // The #1 fix: EXPIRE evaluates the existence gate (NX/XX) and the ordering gate
    // (GT/LT) INDEPENDENTLY, and BOTH must pass. `LT XX` on a key with NO current
    // TTL: XX fails (no TTL), so the timeout is NOT set and the reply is 0 even
    // though LT alone (no-TTL = +infinity) would have applied. The old collapsed
    // enum dropped the XX gate and (wrongly) set the TTL.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    // LT XX on a no-TTL key -> reply 0, nothing set.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"10", b"LT", b"XX"]
        )),
        0,
        "LT XX must fail the XX gate on a key with no TTL"
    );
    // TTL is still -1 (no TTL was set).
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        -1,
        "no TTL was set"
    );
    // Now give it a TTL, then LT XX with a SMALLER deadline applies (both gates
    // pass: XX has a TTL, LT is strictly less).
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"EXPIRE", b"k", b"100"],
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRE", b"k", b"50", b"LT", b"XX"]
        )),
        1,
        "LT XX applies when a TTL exists and the new deadline is smaller"
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        50
    );
}

#[test]
fn ttl_pttl_minus_two_minus_one_conventions() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    // Missing key -> -2.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"missing"]
        )),
        -2
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PTTL", b"missing"]
        )),
        -2
    );
    // Present, no TTL -> -1.
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        -1
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PTTL", b"k"]
        )),
        -1
    );
    // EXPIRETIME/PEXPIRETIME conventions too.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRETIME", b"missing"]
        )),
        -2
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRETIME", b"k"]
        )),
        -1
    );
}

#[test]
fn expiretime_pexpiretime_are_absolute() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    // PEXPIREAT to an absolute ms; EXPIRETIME is that / 1000, PEXPIRETIME is it.
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"PEXPIREAT", b"k", b"123456"],
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIRETIME", b"k"]
        )),
        123_456
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRETIME", b"k"]
        )),
        123 // (123456 + 500) / 1000 = 123 (ms component < 500 rounds down)
    );
}

#[test]
fn expiretime_rounds_to_nearest_second() {
    // EXPIRETIME rounds the absolute ms deadline to the NEAREST second
    // (`(ms + 500) / 1000`, Redis ttlGenericCommand output_abs), so an ms component
    // >= 500 rounds UP. PEXPIRETIME stays exact ms (the #5 fix).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    // 123556ms: ms component 556 >= 500 -> EXPIRETIME rounds up to 124.
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"PEXPIREAT", b"k", b"123556"],
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"EXPIRETIME", b"k"]
        )),
        124,
        "(123556 + 500) / 1000 = 124"
    );
    // PEXPIRETIME is the exact ms, unrounded.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIRETIME", b"k"]
        )),
        123_556
    );
}

#[test]
fn expire_deadline_equal_to_now_deletes_immediately() {
    // The #6 command-time boundary: a resolved deadline EQUAL to now is treated as
    // already past (Redis checkAlreadyExpired, `when <= now`), so PEXPIREAT k <now>
    // replies 1 and the key is gone the same tick. This is DISTINCT from the store's
    // lazy-read backstop (`now > deadline`, alive at now==deadline), which governs a
    // SET deadline reached later.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let now = UnixMillis(100_000);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"SET", b"k", b"v"]);
    // PEXPIREAT to exactly `now` -> reply 1, key deleted immediately.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            now,
            &[b"PEXPIREAT", b"k", b"100000"]
        )),
        1,
        "deadline == now deletes and replies 1 (checkAlreadyExpired <= now)"
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            now,
            &[b"EXISTS", b"k"]
        )),
        0,
        "key is gone same-tick"
    );
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"GET", b"k"]),
        Value::Null
    );
}

#[test]
fn getex_exat_in_the_past_returns_value_then_deletes() {
    // The #6 boundary for GETEX: an ABSOLUTE EXAT/PXAT deadline at or before now
    // returns the value AND deletes the key (Redis checkAlreadyExpired). A past
    // RELATIVE EX/PX is still the invalid-expire error, not this path.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let now = UnixMillis(100_000);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, now, &[b"SET", b"k", b"v"]);
    // PXAT exactly at now (100000ms): value returned, key deleted.
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            now,
            &[b"GETEX", b"k", b"PXAT", b"100000"]
        ),
        bulk(b"v"),
        "GETEX returns the value even when the absolute deadline is past"
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            now,
            &[b"EXISTS", b"k"]
        )),
        0,
        "the key is deleted after the read (past absolute deadline)"
    );
}

#[test]
fn persist_removes_ttl_and_stops_expiring() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"SET", b"k", b"v", b"EX", b"10"],
    );
    // PERSIST removes the TTL -> reply 1.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PERSIST", b"k"]
        )),
        1
    );
    // TTL now -1 (no TTL).
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        -1
    );
    // PERSIST again (no TTL) -> reply 0.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PERSIST", b"k"]
        )),
        0
    );
    // PERSIST on a missing key -> 0.
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PERSIST", b"gone"]
        )),
        0
    );
    // The key no longer expires at the old deadline.
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(20_000),
            &[b"GET", b"k"]
        ),
        bulk(b"v")
    );
}

#[test]
fn getex_matrix() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"SET", b"k", b"v", b"EX", b"100"],
    );
    // Bare GETEX returns the value and does NOT change the TTL.
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"k"]),
        bulk(b"v")
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        100
    );
    // GETEX EX 5 sets a new TTL.
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"EX", b"5"]
        ),
        bulk(b"v")
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        5
    );
    // GETEX PERSIST clears the TTL.
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"GETEX", b"k", b"PERSIST"]
        ),
        bulk(b"v")
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        -1
    );
    // GETEX on an absent key -> nil.
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"absent"]),
        Value::Null
    );
    // GETEX PXAT (absolute ms).
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"GETEX", b"k", b"PXAT", b"50000"],
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PEXPIRETIME", b"k"]
        )),
        50_000
    );
}

#[test]
fn getex_wrongtype_and_invalid_expire() {
    use ironcache_storage::{DataType, Encoding};
    use ironcache_store::kvobj::{Header, KvObj, ValueRepr};
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    // GETEX against a non-string -> WRONGTYPE.
    let mut obj = KvObj::from_bytes(b"lst", b"x", None);
    obj.header = Header {
        data_type: DataType::List,
        encoding: Encoding::ListPack,
        eviction_rank: 0,
        ttl_present: false,
        snapshot_version: 0,
    };
    obj.value = ValueRepr::Inline(Box::from(&b"x"[..]));
    st.insert_object(0, obj);
    match run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"lst"]) {
        Value::Error(e) => assert_eq!(
            e.line(),
            "-WRONGTYPE Operation against a key holding the wrong kind of value"
        ),
        other => panic!("expected WRONGTYPE, got {other:?}"),
    }
    // GETEX with an invalid (<= 0) expire -> invalid expire time in 'getex'.
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    match run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"GETEX", b"k", b"EX", b"0"],
    ) {
        Value::Error(e) => {
            assert_eq!(e.line(), "-ERR invalid expire time in 'getex' command");
        }
        other => panic!("expected invalid expire time, got {other:?}"),
    }
    // GETEX with conflicting options -> syntax error.
    match run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"GETEX", b"k", b"EX", b"5", b"PERSIST"],
    ) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR syntax error"),
        other => panic!("expected syntax error, got {other:?}"),
    }
}

#[test]
fn setex_psetex_set_value_and_ttl() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    // SETEX k 10 v -> +OK, value set, TTL 10s.
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"SETEX", b"k", b"10", b"v"]
        ),
        Value::ok()
    );
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
        bulk(b"v")
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"TTL", b"k"]
        )),
        10
    );
    // PSETEX p 5000 v -> TTL 5000ms.
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PSETEX", b"p", b"5000", b"v"]
        ),
        Value::ok()
    );
    assert_eq!(
        int(run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            t,
            &[b"PTTL", b"p"]
        )),
        5_000
    );
}

#[test]
fn setex_psetex_non_positive_is_invalid_expire_and_writes_nothing() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    match run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"SETEX", b"k", b"0", b"v"],
    ) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR invalid expire time in 'setex' command"),
        other => panic!("expected invalid expire time, got {other:?}"),
    }
    match run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t,
        &[b"PSETEX", b"k", b"-1", b"v"],
    ) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR invalid expire time in 'psetex' command"),
        other => panic!("expected invalid expire time, got {other:?}"),
    }
    // Nothing was written.
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]),
        Value::Null
    );
}

#[test]
fn expire_family_arity_errors() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    for cmd in [
        vec![b"EXPIRE".as_slice(), b"k"],
        vec![b"PEXPIRE", b"k"],
        vec![b"TTL"],
        vec![b"PTTL", b"a", b"b"],
        vec![b"PERSIST"],
        vec![b"EXPIRETIME"],
        vec![b"GETEX"],
        vec![b"SETEX", b"k", b"10"],
        vec![b"PSETEX", b"k", b"10"],
    ] {
        assert!(
            matches!(
                run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &cmd),
                Value::Error(_)
            ),
            "expected arity error for {cmd:?}"
        );
    }
}

// -- Active drain + counters (PR-3b). --

#[test]
fn active_drain_reclaims_expired_keys_and_bumps_expired_counter() {
    // Set short TTLs, advance now via the dispatch `now`, then issue a command:
    // the active drain pops the due keys from the wheel and reaps them, bumping the
    // expired delta.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    // Establish the wheel origin at t=0 (the first advance only sets the base).
    let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
    // Three keys each with a 1s TTL (deadline 1000ms), registered in the wheel.
    for k in [b"a".as_slice(), b"b", b"c"] {
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", k, b"v", b"EX", b"1"],
        );
    }
    assert_eq!(st.len(), 3);
    // Advance well past the deadline and issue a command: the active drain reaps
    // all three before the command body. The drain count is in the expired delta.
    let (_r, deltas) = run_on_wheel_deltas(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        UnixMillis(5_000),
        &[b"PING"],
    );
    assert_eq!(
        deltas.expired, 3,
        "active drain reaped the three expired keys"
    );
    // The store no longer holds them (the drain deleted them, not just the lazy
    // backstop on a read).
    assert_eq!(
        st.len(),
        0,
        "expired keys are resident-evicted by the drain"
    );
}

#[test]
fn active_drain_skips_re_ttld_key_via_store_recheck() {
    // A stale wheel entry (a key whose TTL was extended) must NOT be reaped early:
    // the store re-checks the real expire_at, so the drain skips it.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
    // SET with a 1s TTL (deadline 1000), then EXTEND to 100s (deadline 100000).
    // The wheel still holds the OLD 1000ms registration (a stale entry).
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        UnixMillis(0),
        &[b"SET", b"k", b"v", b"EX", b"1"],
    );
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        UnixMillis(0),
        &[b"EXPIRE", b"k", b"100"],
    );
    // Advance past the OLD deadline (2000ms) but not the new one: the drain offers
    // the stale entry, but the store re-check finds the key NOT expired and skips.
    let (_r, deltas) = run_on_wheel_deltas(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        UnixMillis(2_000),
        &[b"PING"],
    );
    assert_eq!(
        deltas.expired, 0,
        "stale wheel entry must not reap a re-TTL'd key"
    );
    assert_eq!(
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(2_000),
            &[b"GET", b"k"]
        ),
        bulk(b"v"),
        "the re-TTL'd key is still alive"
    );
}

#[test]
fn drain_due_keys_helper_reaps_bounded_batch_deterministically() {
    // The SHARED bounded-drain helper (PR-3c) both the opportunistic per-command
    // path and the background timer task call. Drive it directly: register keys with
    // deadlines, advance the TestEnv-equivalent `now` past them, and assert it reaps
    // exactly the due keys, bumps the count, and respects the `max` bound.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    // Establish the wheel origin at t=0.
    let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
    // 5 keys each with a 1s TTL (deadline 1000ms), registered in the wheel via SET EX.
    for k in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
        run_on_wheel(
            &c,
            &mut s,
            &mut st,
            &mut wheel,
            UnixMillis(0),
            &[b"SET", k, b"v", b"EX", b"1"],
        );
    }
    assert_eq!(st.len(), 5);
    // Drain with a small bound (max=2): the helper reaps at most 2 per call.
    let now = UnixMillis(5_000);
    let first = drain_due_keys(&mut wheel, &mut st, now, 2);
    assert!(first <= 2, "the helper respects the max bound");
    // Keep draining until nothing more is due; total reaped is exactly the 5 keys.
    let mut total = first;
    loop {
        let n = drain_due_keys(&mut wheel, &mut st, now, 2);
        if n == 0 {
            break;
        }
        assert!(n <= 2, "every call respects the max bound");
        total += n;
    }
    assert_eq!(total, 5, "the helper reaps exactly the due keys");
    assert_eq!(st.len(), 0, "all expired keys are resident-evicted");

    // Determinism (ADR-0003): a fresh replay against the same registrations + the
    // same `now` reaps the identical count (the helper reads time only via `now`).
    let mut st2 = test_store(c.databases);
    let mut wheel2 = TimingWheel::new();
    let mut s2 = state(&c);
    let _ = run_on_wheel_deltas(
        &c,
        &mut s2,
        &mut st2,
        &mut wheel2,
        UnixMillis(0),
        &[b"PING"],
    );
    for k in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
        run_on_wheel(
            &c,
            &mut s2,
            &mut st2,
            &mut wheel2,
            UnixMillis(0),
            &[b"SET", k, b"v", b"EX", b"1"],
        );
    }
    let replay = drain_due_keys(&mut wheel2, &mut st2, now, 100);
    assert_eq!(
        replay, 5,
        "same now + same registrations => same reclamation"
    );
}

#[test]
fn drain_due_keys_helper_skips_stale_re_ttld_entry() {
    // The helper reaps ONLY genuinely-expired keys: a re-TTL'd key whose stale wheel
    // entry is offered is re-checked by the store and skipped (no false reap).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        UnixMillis(0),
        &[b"SET", b"k", b"v", b"EX", b"1"],
    );
    run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        UnixMillis(0),
        &[b"EXPIRE", b"k", b"100"],
    );
    // Past the OLD deadline (2000ms) but not the new one: the stale entry is offered,
    // the store re-check finds it live, the helper reaps nothing.
    let reaped = drain_due_keys(&mut wheel, &mut st, UnixMillis(2_000), 100);
    assert_eq!(reaped, 0, "stale wheel entry must not reap a re-TTL'd key");
    assert_eq!(st.len(), 1, "the re-TTL'd key survives");
}

#[test]
fn keyspace_hits_and_misses_are_counted_for_reads() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    // GET hit.
    let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"k"]);
    assert_eq!((d.keyspace_hits, d.keyspace_misses), (1, 0));
    // GET miss.
    let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GET", b"absent"]);
    assert_eq!((d.keyspace_hits, d.keyspace_misses), (0, 1));
    // GETEX is also counted (a real keyspace lookup): a hit on a present key.
    let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"k"]);
    assert_eq!((d.keyspace_hits, d.keyspace_misses), (1, 0));
    let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &[b"GETEX", b"absent"]);
    assert_eq!((d.keyspace_hits, d.keyspace_misses), (0, 1));
}

#[test]
fn ttl_family_does_not_count_keyspace_hits_or_misses() {
    // TTL-family introspection (TTL/PTTL/EXPIRETIME/PEXPIRETIME) uses LOOKUP_NOTOUCH
    // and must NOT bump keyspace_hits/keyspace_misses (the #8 fix), unlike GET/GETEX.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t = UnixMillis(0);
    run_on_wheel(&c, &mut s, &mut st, &mut wheel, t, &[b"SET", b"k", b"v"]);
    for cmd in [
        vec![b"TTL".as_slice(), b"k"],
        vec![b"TTL", b"absent"],
        vec![b"PTTL", b"k"],
        vec![b"PTTL", b"absent"],
        vec![b"EXPIRETIME", b"k"],
        vec![b"EXPIRETIME", b"absent"],
        vec![b"PEXPIRETIME", b"k"],
        vec![b"PEXPIRETIME", b"absent"],
    ] {
        let (_r, d) = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, t, &cmd);
        assert_eq!(
            (d.keyspace_hits, d.keyspace_misses),
            (0, 0),
            "{cmd:?} must not count keyspace hits/misses (LOOKUP_NOTOUCH)"
        );
    }
}

#[test]
fn determinism_replay_drives_identical_expiry_sets() {
    // The same command + now sequence replays the identical expiry outcome (the
    // determinism contract, ADR-0003: the wheel + store read time only via `now`).
    let run = || -> (usize, u64) {
        let c = ctx(None);
        let mut s = state(&c);
        let mut st = test_store(c.databases);
        let mut wheel = TimingWheel::new();
        let _ = run_on_wheel_deltas(&c, &mut s, &mut st, &mut wheel, UnixMillis(0), &[b"PING"]);
        for i in 0..10u32 {
            run_on_wheel(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(0),
                &[b"SET", format!("k{i}").as_bytes(), b"v", b"PX", b"500"],
            );
        }
        let mut total_expired = 0u64;
        for step in [200u64, 600, 1_000, 5_000] {
            let (_r, d) = run_on_wheel_deltas(
                &c,
                &mut s,
                &mut st,
                &mut wheel,
                UnixMillis(step),
                &[b"PING"],
            );
            total_expired += d.expired;
        }
        (st.len(), total_expired)
    };
    let a = run();
    let b = run();
    assert_eq!(a, b, "identical now sequence => identical expiry outcome");
    // All ten keys expired (deadline 500ms, drained by step 600+).
    assert_eq!(a.0, 0);
    assert_eq!(a.1, 10);
}

// -- Generic keyspace + introspection commands (PR-4a) through dispatch. --

/// A test store wired with an LFU policy (for OBJECT FREQ/IDLETIME gating tests).
fn lfu_store(databases: u32) -> TestStore {
    let policy = map_policy_name("allkeys-lfu", 1).expect("allkeys-lfu maps");
    ShardStore::with_hooks(databases, policy, CountingAccounting::new())
}

/// Extract a Bulk string's bytes (panics on any other reply shape).
fn bulk_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::BulkString(Some(b)) => b.to_vec(),
        other => panic!("expected bulk string, got {other:?}"),
    }
}

#[test]
fn keys_matches_glob_and_equals_full_scan() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for k in [b"user:1".as_slice(), b"user:2", b"post:1", b"misc"] {
        run_on(&c, &mut s, &mut st, t, &[b"SET", k, b"v"]);
    }
    // KEYS user:* -> the two user keys (order-independent compare).
    let v = run_on(&c, &mut s, &mut st, t, &[b"KEYS", b"user:*"]);
    let mut got: Vec<Vec<u8>> = match v {
        Value::Array(Some(items)) => items.iter().map(bulk_bytes).collect(),
        other => panic!("expected array, got {other:?}"),
    };
    got.sort();
    assert_eq!(got, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
    // KEYS * -> all four.
    let all = run_on(&c, &mut s, &mut st, t, &[b"KEYS", b"*"]);
    match all {
        Value::Array(Some(items)) => assert_eq!(items.len(), 4),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn scan_to_completion_collects_all_keys() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for i in 0..40 {
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("k{i}").as_bytes(), b"v"],
        );
    }
    // Loop SCAN with a small COUNT to completion, collecting every key.
    let mut collected: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut cursor = b"0".to_vec();
    loop {
        let v = run_on(&c, &mut s, &mut st, t, &[b"SCAN", &cursor, b"COUNT", b"3"]);
        let items = match v {
            Value::Array(Some(items)) => items,
            other => panic!("SCAN reply must be a 2-array, got {other:?}"),
        };
        assert_eq!(items.len(), 2, "[next_cursor, [keys]]");
        let next = bulk_bytes(&items[0]);
        if let Value::Array(Some(keys)) = &items[1] {
            for k in keys {
                collected.insert(bulk_bytes(k));
            }
        } else {
            panic!("SCAN keys element must be an array");
        }
        if next == b"0" {
            break;
        }
        cursor = next;
    }
    assert_eq!(
        collected.len(),
        40,
        "SCAN to completion collected every key"
    );
}

#[test]
fn scan_match_and_type_filters() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for i in 0..10 {
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("s{i}").as_bytes(), b"v"],
        );
    }
    // SCAN 0 MATCH s1* -> just s1 (s1 only; s10..s19 do not exist).
    let v = run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"SCAN", b"0", b"MATCH", b"s1", b"COUNT", b"100"],
    );
    if let Value::Array(Some(items)) = v {
        if let Value::Array(Some(keys)) = &items[1] {
            assert_eq!(keys.len(), 1);
            assert_eq!(bulk_bytes(&keys[0]), b"s1");
        }
    }
    // SCAN 0 TYPE list -> nothing (all are strings).
    let v = run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"SCAN", b"0", b"TYPE", b"list", b"COUNT", b"100"],
    );
    if let Value::Array(Some(items)) = v {
        if let Value::Array(Some(keys)) = &items[1] {
            assert!(keys.is_empty(), "no list-typed keys");
        }
    }
}

#[test]
fn scan_invalid_cursor_errors() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    match run_on(
        &c,
        &mut s,
        &mut st,
        UnixMillis(0),
        &[b"SCAN", b"notanumber"],
    ) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR invalid cursor"),
        other => panic!("expected invalid cursor, got {other:?}"),
    }
}

#[test]
fn dbsize_counts_keys() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"DBSIZE"]),
        Value::Integer(0)
    );
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"DBSIZE"]),
        Value::Integer(2)
    );
}

// The short test-fixture names (c/s/st/t plus the a/b reply bindings) are the
// established convention across this test module; the lint trips only because this
// case names a couple of reply temporaries too.
#[allow(clippy::many_single_char_names)]
#[test]
fn randomkey_member_nil_and_deterministic_under_seeded_env() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Empty DB -> nil.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]), Value::Null);
    for i in 0..10 {
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SET", format!("k{i}").as_bytes(), b"v"],
        );
    }
    // The reply is a live member.
    let v = run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]);
    let key = bulk_bytes(&v);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", &key]),
        Value::Integer(1),
        "RANDOMKEY returned a live member"
    );
    // Deterministic under the seeded TestEnv: `run_on` builds a fresh TestEnv(seed=1)
    // each call, so the first RNG draw (the pick) is identical, yielding the same key.
    let a = run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]);
    let b = run_on(&c, &mut s, &mut st, t, &[b"RANDOMKEY"]);
    assert_eq!(a, b, "RANDOMKEY deterministic under a seeded env");
}

#[test]
fn set_spop_srandmember_sscan_are_deterministic_through_the_env_seam() {
    // ADR-0003: SPOP/SRANDMEMBER draw their seed from the Env RNG via dispatch (the
    // caller-draws seam); SSCAN reads no RNG. `run_on` builds a fresh TestEnv(seed=1)
    // each call, so the first RNG draw (the SPOP/SRANDMEMBER seed) is identical across
    // calls, yielding the same selection. This pins that the randomness enters through
    // the seam (the store/handler read no RNG) and is deterministic for a fixed seed.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"SADD", b"k", b"a", b"b", b"c", b"d", b"e"],
    );
    // SRANDMEMBER (no removal): two calls with the same fresh-seed env match.
    let rand_a = run_on(&c, &mut s, &mut st, t, &[b"SRANDMEMBER", b"k", b"3"]);
    let rand_b = run_on(&c, &mut s, &mut st, t, &[b"SRANDMEMBER", b"k", b"3"]);
    assert_eq!(
        rand_a, rand_b,
        "SRANDMEMBER deterministic under the seeded env"
    );

    // SSCAN reads no RNG: identical across calls (cursor 0, small set -> all at once).
    let scan_a = run_on(&c, &mut s, &mut st, t, &[b"SSCAN", b"k", b"0"]);
    let scan_b = run_on(&c, &mut s, &mut st, t, &[b"SSCAN", b"k", b"0"]);
    assert_eq!(scan_a, scan_b, "SSCAN deterministic (reads no RNG)");

    // SPOP on two FRESH identical stores with the same seeded env pops the SAME member.
    let mut st1 = test_store(c.databases);
    let mut st2 = test_store(c.databases);
    for store in [&mut st1, &mut st2] {
        run_on(
            &c,
            &mut s,
            store,
            t,
            &[b"SADD", b"k", b"a", b"b", b"c", b"d"],
        );
    }
    let p1 = run_on(&c, &mut s, &mut st1, t, &[b"SPOP", b"k"]);
    let p2 = run_on(&c, &mut s, &mut st2, t, &[b"SPOP", b"k"]);
    assert_eq!(p1, p2, "SPOP deterministic under the seeded env");
}

#[test]
fn rename_preserves_value_and_renamenx_copy_semantics() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"src", b"hello"]);
    // RENAME -> +OK, src gone, dst holds the value.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RENAME", b"src", b"dst"]),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"dst"]),
        bulk(b"hello")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"src"]),
        Value::Null
    );
    // RENAME of a missing key -> no such key.
    match run_on(&c, &mut s, &mut st, t, &[b"RENAME", b"gone", b"x"]) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR no such key"),
        other => panic!("expected no such key, got {other:?}"),
    }
    // RENAMENX: dst exists -> 0; dst free -> 1.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RENAMENX", b"a", b"b"]),
        Value::Integer(0)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RENAMENX", b"a", b"c"]),
        Value::Integer(1)
    );
    // COPY with REPLACE overwrites; without REPLACE onto an existing dst -> 0.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"from", b"X"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"to", b"Y"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"COPY", b"from", b"to"]),
        Value::Integer(0),
        "COPY declines without REPLACE when dst exists"
    );
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"COPY", b"from", b"to", b"REPLACE"]
        ),
        Value::Integer(1)
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"to"]), bulk(b"X"));
}

#[test]
fn move_across_dbs_and_noop_when_dest_occupied() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // The connection is on db 0 (default).
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
    // MOVE k 1 -> 1; gone from db 0.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"k", b"1"]),
        Value::Integer(1)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"k"]),
        Value::Integer(0)
    );
    // A fresh k in db 0; MOVE to db 1 where k already exists -> 0 (no-op).
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v2"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"k", b"1"]),
        Value::Integer(0)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"k"]),
        Value::Integer(1)
    );
    // MOVE to the same db is an error.
    assert!(matches!(
        run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"k", b"0"]),
        Value::Error(_)
    ));
}

#[test]
fn swapdb_swaps_contents() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Put a in db 0, b in db 1 (via MOVE), then SWAPDB 0 1.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"in0"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"in0too"]);
    run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"b", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SWAPDB", b"0", b"1"]),
        Value::ok()
    );
    // After swap, db 0 holds what was db 1 (b), and a is now in db 1.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"a"]),
        Value::Integer(0)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"b"]),
        Value::Integer(1)
    );
}

#[test]
fn touch_and_unlink_counts() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
    // TOUCH counts live keys (repeats counted, like EXISTS).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"TOUCH", b"a", b"a", b"b", b"missing"]
        ),
        Value::Integer(3)
    );
    // UNLINK removes live keys, returns the count (== DEL today).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"UNLINK", b"a", b"b", b"missing"]),
        Value::Integer(2)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"a", b"b"]),
        Value::Integer(0)
    );
}

#[test]
fn flushdb_and_flushall_empty_scope() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
    run_on(&c, &mut s, &mut st, t, &[b"MOVE", b"b", b"1"]);
    // FLUSHDB (with the SYNC option accepted) empties only db 0.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"FLUSHDB", b"SYNC"]),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"DBSIZE"]),
        Value::Integer(0)
    );
    // FLUSHALL ASYNC empties everything.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"FLUSHALL", b"ASYNC"]),
        Value::ok()
    );
    // An unknown flush option is a syntax error.
    assert!(matches!(
        run_on(&c, &mut s, &mut st, t, &[b"FLUSHDB", b"BOGUS"]),
        Value::Error(_)
    ));
}

#[test]
fn object_encoding_int_embstr_raw() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // int
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"n", b"12345"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"n"]),
        bulk(b"int")
    );
    // embstr (short string)
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"e", b"hello"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"e"]),
        bulk(b"embstr")
    );
    // raw (long string > 44 bytes)
    let big = vec![b'z'; 100];
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"r", &big]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"r"]),
        bulk(b"raw")
    );
    // Missing key -> null (Redis replies the null bulk, not an error).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"nope"]),
        Value::Null
    );
}

#[test]
fn object_encoding_append_stays_short_is_a_known_divergence() {
    // KNOWN DIVERGENCE (ADR-0009, recorded for the conformance suite): an APPEND
    // whose result stays SHORT reports `embstr`/`int` here where REDIS reports
    // `raw` (Redis converts any APPENDed string to raw unconditionally). IronCache's
    // APPEND rebuilds-and-reclassifies through the rmw waist, so a short result
    // reclassifies. The fix needs the deferred in-place-mutation waist extension; it
    // is NOT fixed here. This test asserts the CURRENT (divergent) behavior so the
    // conformance suite tracks it.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // APPEND to a fresh key with a short value -> Redis would report `raw`; we report
    // `embstr` (the documented divergence).
    run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"a", b"abc"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"a"]),
        bulk(b"embstr"),
        "KNOWN DIVERGENCE: APPEND-stays-short reports embstr here, raw in Redis"
    );
    // An APPEND producing a pure-integer string reports `int` here (Redis: raw).
    run_on(&c, &mut s, &mut st, t, &[b"APPEND", b"b", b"42"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"b"]),
        bulk(b"int"),
        "KNOWN DIVERGENCE: APPEND of digits reports int here, raw in Redis"
    );
}

#[test]
fn object_refcount_shared_int_vs_one() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // A shared small int (0..=9999) reports OBJ_SHARED_REFCOUNT = 2147483647.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"small", b"100"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"REFCOUNT", b"small"]),
        Value::Integer(2_147_483_647)
    );
    // A large int (>= 10000) is not shared -> 1.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"big", b"100000"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"REFCOUNT", b"big"]),
        Value::Integer(1)
    );
    // A non-int string -> 1.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"hello"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"REFCOUNT", b"str"]),
        Value::Integer(1)
    );
}

#[test]
fn object_idletime_zero_under_non_lfu_and_errors_under_lfu() {
    let c = ctx(None);
    let mut s = state(&c);
    // Non-LFU (default cache policy): IDLETIME is 0.
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"IDLETIME", b"k"]),
        Value::Integer(0)
    );
    // LFU policy: IDLETIME errors (idle time not tracked under LFU).
    let mut lfu = lfu_store(c.databases);
    run_on(&c, &mut s, &mut lfu, t, &[b"SET", b"k", b"v"]);
    match run_on(&c, &mut s, &mut lfu, t, &[b"OBJECT", b"IDLETIME", b"k"]) {
        Value::Error(e) => assert_eq!(
            e.line(),
            "-ERR An LFU maxmemory policy is selected, idle time not tracked. \
                 Please note that when switching between policies at runtime LRU and \
                 LFU data will take some time to adjust."
        ),
        other => panic!("expected LFU idletime error, got {other:?}"),
    }
}

#[test]
fn object_freq_under_lfu_and_errors_under_non_lfu() {
    let c = ctx(None);
    let mut s = state(&c);
    let t = UnixMillis(0);
    // Non-LFU: FREQ errors (requires an LFU policy).
    let mut st = test_store(c.databases);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
    match run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"FREQ", b"k"]) {
        Value::Error(e) => assert_eq!(
            e.line(),
            "-ERR An LFU maxmemory policy is not selected, access frequency not \
                 tracked. Please note that when switching between policies at runtime \
                 LRU and LFU data will take some time to adjust."
        ),
        other => panic!("expected LFU freq error, got {other:?}"),
    }
    // LFU: FREQ returns an integer estimate (>= 0).
    let mut lfu = lfu_store(c.databases);
    run_on(&c, &mut s, &mut lfu, t, &[b"SET", b"k", b"v"]);
    // Access it a few times so the sketch estimate is non-trivial.
    for _ in 0..5 {
        run_on(&c, &mut s, &mut lfu, t, &[b"GET", b"k"]);
    }
    match run_on(&c, &mut s, &mut lfu, t, &[b"OBJECT", b"FREQ", b"k"]) {
        Value::Integer(n) => assert!((0..=15).contains(&n), "FREQ estimate in 0..=15, got {n}"),
        other => panic!("expected integer freq, got {other:?}"),
    }
    // FREQ of a missing key (under LFU) -> null.
    assert_eq!(
        run_on(&c, &mut s, &mut lfu, t, &[b"OBJECT", b"FREQ", b"absent"]),
        Value::Null
    );
}

#[test]
fn object_help_and_unknown_subcommand() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // HELP -> a non-empty array of bulk strings.
    match run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"HELP"]) {
        Value::Array(Some(items)) => assert!(!items.is_empty()),
        other => panic!("expected help array, got {other:?}"),
    }
    // An unknown subcommand errors.
    assert!(matches!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"BOGUS", b"k"]),
        Value::Error(_)
    ));
}

#[test]
fn keyspace_command_arity_errors() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for cmd in [
        vec![b"KEYS".as_slice()],
        vec![b"SCAN"],
        vec![b"RENAME", b"a"],
        vec![b"RENAMENX", b"a"],
        vec![b"MOVE", b"a"],
        vec![b"SWAPDB", b"0"],
        vec![b"TOUCH"],
        vec![b"UNLINK"],
        vec![b"OBJECT"],
    ] {
        assert!(
            matches!(run_on(&c, &mut s, &mut st, t, &cmd), Value::Error(_)),
            "expected arity error for {cmd:?}"
        );
    }
}

// -- CONFIG maxmemory-policy hot-swap through dispatch (PR-4b). --

/// Drive ONE command through dispatch against a caller-owned store + per-shard
/// generation, with a seeded [`TestEnv`] (so the swap seed is deterministic), and
/// return the reply.
fn run_swap(
    ctx: &ServerContext,
    st: &mut ConnState,
    store: &mut TestStore,
    shard_gen: &mut u64,
    seed: u64,
    parts: &[&[u8]],
) -> Value {
    let mut env = TestEnv::new(seed);
    let mut wheel = TimingWheel::new();
    let zero = || CounterSnapshot::default();
    let mut deltas = CounterDeltas::default();
    dispatch(
        ctx,
        st,
        &mut env,
        store,
        &mut wheel,
        UnixMillis(0),
        shard_gen,
        &zero,
        &|| (String::new(), String::new()),
        &|| None,
        MemoryInfo::default(),
        &mut deltas,
        &req(parts),
    )
}

#[test]
fn dispatch_hot_swaps_policy_on_generation_change() {
    // A CONFIG SET maxmemory-policy bumps the shared generation; the NEXT command on
    // a shard whose last-seen generation is behind rebuilds that shard's policy from
    // the new name (the per-command atomic load + compare at the top of dispatch).
    let c = ctx_full(None, 0, "allkeys-lru");
    let mut s = state(&c);
    let mut st = store_with(c.databases, map_policy_name("allkeys-lru", 1).unwrap());
    let mut shard_gen = c.runtime.generation();

    // A no-op command does not swap (generation unchanged).
    let _ = run_swap(&c, &mut s, &mut st, &mut shard_gen, 1, &[b"PING"]);
    assert_eq!(st.policy_name(), "allkeys-lru");

    // CONFIG SET maxmemory-policy allkeys-lfu (bumps the shared generation).
    let _ = run_swap(
        &c,
        &mut s,
        &mut st,
        &mut shard_gen,
        1,
        &[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-lfu"],
    );
    // The swap happens at the TOP of the NEXT dispatch (the CONFIG SET command that
    // bumped the generation observed the OLD generation at its own top). Issue
    // another command: now the store has swapped.
    let _ = run_swap(&c, &mut s, &mut st, &mut shard_gen, 1, &[b"PING"]);
    assert_eq!(
        st.policy_name(),
        "allkeys-lfu",
        "store swapped to the new policy"
    );
    assert_eq!(
        shard_gen,
        c.runtime.generation(),
        "shard caught up to the gen"
    );
}

#[test]
fn dispatch_swap_seed_is_deterministic() {
    // Two identical seeded runs that swap to a *-random policy through dispatch
    // produce the same victim ordering (ADR-0003: the swap seeds the RNG through the
    // Env seam, so a fixed seed is reproducible; the shared atomic reads add no
    // nondeterminism for a fixed command sequence).
    fn build_and_swap(seed: u64) -> TestStore {
        let c = ctx_full(None, 0, "allkeys-lru");
        let mut s = state(&c);
        let mut st = store_with(c.databases, map_policy_name("allkeys-lru", 1).unwrap());
        let mut shard_gen = c.runtime.generation();
        // Plant keys.
        for i in 0..8u8 {
            let key = [b'k', i];
            let _ = run_swap(
                &c,
                &mut s,
                &mut st,
                &mut shard_gen,
                seed,
                &[b"SET", &key, b"v"],
            );
        }
        // Swap to allkeys-random; the swap draws its seed from the seeded TestEnv on
        // the FIRST command after the generation bump.
        let _ = run_swap(
            &c,
            &mut s,
            &mut st,
            &mut shard_gen,
            seed,
            &[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-random"],
        );
        // The next command triggers the swap (and re-tracks the keys via reads).
        for i in 0..8u8 {
            let key = [b'k', i];
            let _ = run_swap(&c, &mut s, &mut st, &mut shard_gen, seed, &[b"GET", &key]);
        }
        assert_eq!(st.policy_name(), "allkeys-random");
        st
    }
    // The swap-seed determinism is anchored by the FIRST command after the gen bump:
    // both runs draw the SAME seed value from a TestEnv seeded the same way, because
    // that command's env is `TestEnv::new(seed)` and the RNG draw for the swap is the
    // first draw on that fresh env. So both stores swap to a Random policy seeded
    // identically; their used_memory and policy name match deterministically.
    let a = build_and_swap(99);
    let b = build_and_swap(99);
    assert_eq!(a.policy_name(), b.policy_name());
    assert_eq!(a.used_memory(), b.used_memory());
    assert_eq!(a.len(), b.len());
}

// -- List commands (PR-5) through dispatch over a real ShardStore. --

/// An integer reply value (named `iv` to avoid colliding with the existing `int`
/// helper, which EXTRACTS an i64 from a Value).
fn iv(n: i64) -> Value {
    Value::Integer(n)
}

/// A bulk-string array reply from byte slices.
fn arr(items: &[&[u8]]) -> Value {
    Value::Array(Some(items.iter().map(|b| bulk(b)).collect()))
}

#[test]
fn lpush_rpush_order_and_return_len() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // RPUSH appends: k = [a, b, c]; returns the running length.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a"]),
        iv(1)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"b", b"c"]),
        iv(3)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"a", b"b", b"c"])
    );
    // LPUSH prepends each in turn: LPUSH k x y -> y then x at the head, so the
    // list becomes [y, x, a, b, c].
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPUSH", b"k", b"x", b"y"]),
        iv(5)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"y", b"x", b"a", b"b", b"c"])
    );
    // TYPE is list; OBJECT ENCODING is listpack while small.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
        Value::simple("list")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"k"]),
        bulk(b"listpack")
    );
}

#[test]
fn pushx_only_when_exists() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // LPUSHX/RPUSHX on a missing key -> 0, no create.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPUSHX", b"k", b"a"]),
        iv(0)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RPUSHX", b"k", b"a"]),
        iv(0)
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"LLEN", b"k"]), iv(0));
    // Create with RPUSH, then PUSHX appends.
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RPUSHX", b"k", b"b"]),
        iv(2)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPUSHX", b"k", b"z"]),
        iv(3)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"z", b"a", b"b"])
    );
}

#[test]
fn lpop_rpop_single_count_and_nil() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"RPUSH", b"k", b"a", b"b", b"c", b"d"],
    );
    // Single LPOP -> bulk "a".
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k"]), bulk(b"a"));
    // RPOP -> bulk "d".
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"RPOP", b"k"]), bulk(b"d"));
    // LPOP with count -> array.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k", b"2"]),
        arr(&[b"b", b"c"])
    );
    // The list is now empty -> key deleted; LPOP -> nil (no count), nil array (count).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k"]),
        Value::Null
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k", b"3"]),
        Value::Array(None)
    );
    // A negative count is the must-be-positive error.
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"x"]);
    match run_on(&c, &mut s, &mut st, t, &[b"LPOP", b"k", b"-1"]) {
        Value::Error(e) => {
            assert_eq!(e.line(), "-ERR value is out of range, must be positive");
        }
        other => panic!("expected must-be-positive error, got {other:?}"),
    }
}

#[test]
fn lrange_inclusive_and_negative_indices() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"RPUSH", b"k", b"a", b"b", b"c", b"d", b"e"],
    );
    // Inclusive range.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"1", b"3"]),
        arr(&[b"b", b"c", b"d"])
    );
    // Negative indices from the tail.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"-2", b"-1"]),
        arr(&[b"d", b"e"])
    );
    // Out-of-range / inverted -> empty array.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"5", b"10"]),
        Value::Array(Some(vec![]))
    );
    // Absent key -> empty array.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"nope", b"0", b"-1"]),
        Value::Array(Some(vec![]))
    );
}

#[test]
fn lindex_nil_out_of_range() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a", b"b", b"c"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LINDEX", b"k", b"0"]),
        bulk(b"a")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LINDEX", b"k", b"-1"]),
        bulk(b"c")
    );
    // Out of range -> nil.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LINDEX", b"k", b"5"]),
        Value::Null
    );
}

#[test]
fn lset_no_such_key_index_out_of_range_and_success() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // LSET on a missing key -> -ERR no such key.
    match run_on(&c, &mut s, &mut st, t, &[b"LSET", b"k", b"0", b"v"]) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR no such key"),
        other => panic!("expected no such key, got {other:?}"),
    }
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a", b"b", b"c"]);
    // Out-of-range index -> -ERR index out of range.
    match run_on(&c, &mut s, &mut st, t, &[b"LSET", b"k", b"9", b"v"]) {
        Value::Error(e) => assert_eq!(e.line(), "-ERR index out of range"),
        other => panic!("expected index out of range, got {other:?}"),
    }
    // Success.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LSET", b"k", b"1", b"B"]),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"a", b"B", b"c"])
    );
}

#[test]
fn linsert_before_after_pivot_not_found_and_key_absent() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Absent key -> 0.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LINSERT", b"k", b"BEFORE", b"x", b"y"]
        ),
        iv(0)
    );
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a", b"c"]);
    // BEFORE c -> insert b: [a, b, c]; returns new len 3.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LINSERT", b"k", b"BEFORE", b"c", b"b"]
        ),
        iv(3)
    );
    // AFTER a -> insert A: [a, A, b, c]; returns 4.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LINSERT", b"k", b"AFTER", b"a", b"A"]
        ),
        iv(4)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"a", b"A", b"b", b"c"])
    );
    // Pivot not found -> -1, no change.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LINSERT", b"k", b"BEFORE", b"zzz", b"q"]
        ),
        iv(-1)
    );
}

#[test]
fn lrem_positive_negative_zero() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let seed = |st: &mut TestStore, s: &mut ConnState| {
        run_on(&c, s, st, t, &[b"DEL", b"k"]);
        run_on(
            &c,
            s,
            st,
            t,
            &[b"RPUSH", b"k", b"a", b"b", b"a", b"c", b"a"],
        );
    };
    // count > 0: remove first 2 'a' head->tail: [b, c, a].
    seed(&mut st, &mut s);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LREM", b"k", b"2", b"a"]),
        iv(2)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"b", b"c", b"a"])
    );
    // count < 0: remove first 1 'a' tail->head: [a, b, a, c].
    seed(&mut st, &mut s);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LREM", b"k", b"-1", b"a"]),
        iv(1)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"a", b"b", b"a", b"c"])
    );
    // count == 0: remove ALL 'a': [b, c].
    seed(&mut st, &mut s);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LREM", b"k", b"0", b"a"]),
        iv(3)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"b", b"c"])
    );
}

#[test]
fn ltrim_inclusive_and_empty_deletes_key() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"RPUSH", b"k", b"a", b"b", b"c", b"d", b"e"],
    );
    // Keep [1, 3] -> [b, c, d].
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LTRIM", b"k", b"1", b"3"]),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"k", b"0", b"-1"]),
        arr(&[b"b", b"c", b"d"])
    );
    // An out-of-range trim empties the list -> key deleted.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LTRIM", b"k", b"5", b"10"]),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"TYPE", b"k"]),
        Value::simple("none")
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"k"]), iv(0));
}

#[test]
fn lmove_and_rpoplpush_including_src_eq_dst_rotate() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"RPUSH", b"src", b"a", b"b", b"c"],
    );
    // LMOVE src dst LEFT RIGHT: pop 'a' from src head, push to dst tail.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]
        ),
        bulk(b"a")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"src", b"0", b"-1"]),
        arr(&[b"b", b"c"])
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"dst", b"0", b"-1"]),
        arr(&[b"a"])
    );
    // RPOPLPUSH src dst: pop 'c' from src tail, push to dst head.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RPOPLPUSH", b"src", b"dst"]),
        bulk(b"c")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"dst", b"0", b"-1"]),
        arr(&[b"c", b"a"])
    );
    // src == dst rotate: RPOPLPUSH dst dst moves the tail to the head.
    // dst = [c, a] -> rotate -> [a, c].
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RPOPLPUSH", b"dst", b"dst"]),
        bulk(b"a")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"dst", b"0", b"-1"]),
        arr(&[b"a", b"c"])
    );
    // LMOVE from an absent src -> nil.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMOVE", b"nope", b"dst", b"LEFT", b"LEFT"]
        ),
        Value::Null
    );
}

#[test]
fn lpos_rank_count_maxlen() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // [a, b, c, a, b, c, a]
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"RPUSH", b"k", b"a", b"b", b"c", b"a", b"b", b"c", b"a"],
    );
    // First 'a' -> index 0.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPOS", b"k", b"a"]),
        iv(0)
    );
    // RANK 2 -> the SECOND 'a' -> index 3.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LPOS", b"k", b"a", b"RANK", b"2"]
        ),
        iv(3)
    );
    // RANK -1 -> the last 'a' -> index 6.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LPOS", b"k", b"a", b"RANK", b"-1"]
        ),
        iv(6)
    );
    // COUNT 0 -> all 'a' positions [0, 3, 6].
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LPOS", b"k", b"a", b"COUNT", b"0"]
        ),
        Value::Array(Some(vec![iv(0), iv(3), iv(6)]))
    );
    // MAXLEN 2 with COUNT 0 -> only the first 2 elements are scanned -> [0].
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LPOS", b"k", b"a", b"COUNT", b"0", b"MAXLEN", b"2"]
        ),
        Value::Array(Some(vec![iv(0)]))
    );
    // No match -> nil (no COUNT), empty array (with COUNT).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LPOS", b"k", b"zzz"]),
        Value::Null
    );
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LPOS", b"k", b"zzz", b"COUNT", b"0"]
        ),
        Value::Array(Some(vec![]))
    );
}

#[test]
fn wrongtype_on_a_string_key() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"hello"]);
    for cmd in [
        vec![b"LPUSH".as_slice(), b"str", b"x"],
        vec![b"RPUSH", b"str", b"x"],
        vec![b"LPOP", b"str"],
        vec![b"LLEN", b"str"],
        vec![b"LRANGE", b"str", b"0", b"-1"],
        vec![b"LINDEX", b"str", b"0"],
        vec![b"LSET", b"str", b"0", b"v"],
        vec![b"LINSERT", b"str", b"BEFORE", b"a", b"b"],
        vec![b"LREM", b"str", b"0", b"a"],
        vec![b"LTRIM", b"str", b"0", b"-1"],
        vec![b"LPOS", b"str", b"a"],
    ] {
        match run_on(&c, &mut s, &mut st, t, &cmd) {
            Value::Error(e) => assert_eq!(
                e.line(),
                "-WRONGTYPE Operation against a key holding the wrong kind of value",
                "{cmd:?}"
            ),
            other => panic!("expected WRONGTYPE for {cmd:?}, got {other:?}"),
        }
    }
    // The string value is untouched.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"str"]),
        bulk(b"hello")
    );
}

#[test]
fn object_encoding_listpack_then_quicklist() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", b"a"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"k"]),
        bulk(b"listpack")
    );
    // Push a value over the 8 KB byte budget -> quicklist.
    let big = vec![b'q'; 9000];
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"k", &big]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"OBJECT", b"ENCODING", b"k"]),
        bulk(b"quicklist")
    );
}

#[test]
fn list_command_arity_errors() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    for bad in [
        vec![b"LPUSH".as_slice(), b"k"],         // needs >= 1 element
        vec![b"LPOP", b"k", b"1", b"extra"],     // at most key + count
        vec![b"LRANGE", b"k", b"0"],             // needs start AND stop
        vec![b"LSET", b"k", b"0"],               // needs index AND element
        vec![b"LINSERT", b"k", b"BEFORE", b"p"], // needs pivot AND element
        vec![b"LLEN"],                           // needs a key
    ] {
        match run_on(&c, &mut s, &mut st, t, &bad) {
            Value::Error(e) => assert!(
                e.line().contains("wrong number of arguments"),
                "{bad:?} -> {}",
                e.line()
            ),
            other => panic!("expected arity error for {bad:?}, got {other:?}"),
        }
    }
}

#[test]
fn hash_commands_through_dispatch() {
    // Drive the HASH commands through the full dispatcher (so the HRANDFIELD RNG draw
    // off the Env seam, the denyoom gate, and the command-table wiring are exercised).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // HSET two new fields -> :2.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"HSET", b"h", b"a", b"1", b"b", b"2"]
        ),
        Value::Integer(2)
    );
    // HGET -> bulk.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"HGET", b"h", b"a"]),
        bulk(b"1")
    );
    // HRANDFIELD with no count -> one of the fields (the RNG seed comes off the Env
    // seam inside dispatch; we just assert it is a member).
    match run_on(&c, &mut s, &mut st, t, &[b"HRANDFIELD", b"h"]) {
        Value::BulkString(Some(f)) => {
            assert!(f.as_ref() == b"a" || f.as_ref() == b"b", "got {f:?}");
        }
        other => panic!("HRANDFIELD -> {other:?}"),
    }
    // HGETALL -> a map value (the encoder degrades it per proto).
    match run_on(&c, &mut s, &mut st, t, &[b"HGETALL", b"h"]) {
        Value::Map(pairs) => assert_eq!(pairs.len(), 2),
        other => panic!("HGETALL -> {other:?}"),
    }
    // HDEL both fields -> :2, then the key is gone (empty-deletes-key).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"HDEL", b"h", b"a", b"b"]),
        Value::Integer(2)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"h"]),
        Value::Integer(0)
    );
}

// -- Transactions: MULTI/EXEC/DISCARD queueing (TRANSACTIONS.md, PR-10a). These use
// the persistent-store `run_on` helper so the per-connection MULTI state (in_multi /
// queued / dirty_exec on `s`) and the store both persist across calls. --

#[test]
fn multi_opens_a_transaction_and_queues_commands() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // MULTI -> +OK and the connection is in a transaction.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert!(s.in_multi);
    // Each subsequent command is QUEUED (a SimpleString "QUEUED"), NOT executed.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]),
        Value::simple("QUEUED")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]),
        Value::simple("QUEUED")
    );
    // The queue grew; nothing applied yet (k still absent in the store).
    assert_eq!(s.queued.len(), 2);
    // Even a read like GET is QUEUED inside MULTI (it does not execute now), so it
    // replies +QUEUED rather than the value, and the queue grows to 3.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]),
        Value::simple("QUEUED")
    );
    assert_eq!(s.queued.len(), 3);
}

#[test]
fn exec_runs_queued_commands_in_order_returning_an_array() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
    // EXEC -> Array([+OK, :2]) in order.
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::ok());
            assert_eq!(items[1], Value::Integer(2));
        }
        other => panic!("EXEC -> {other:?}"),
    }
    // The transaction is over and the batch applied: k == 2.
    assert!(!s.in_multi);
    assert!(s.queued.is_empty());
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"2"));
}

#[test]
fn empty_multi_exec_is_an_empty_array() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
        Value::Array(Some(vec![]))
    );
    assert!(!s.in_multi);
}

#[test]
fn discard_drops_the_queue_and_exits_multi() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]),
        Value::simple("QUEUED")
    );
    // DISCARD -> +OK, queue dropped, not in MULTI, nothing applied.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"DISCARD"]), Value::ok());
    assert!(!s.in_multi);
    assert!(s.queued.is_empty());
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
}

#[test]
fn exec_and_discard_without_multi_are_errors() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-ERR EXEC without MULTI"
    );
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"DISCARD"])),
        "-ERR DISCARD without MULTI"
    );
}

#[test]
fn nested_multi_is_an_error_and_leaves_the_queue_intact() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(s.queued.len(), 1);
    // A nested MULTI errors and does NOT touch the queue or the transaction state.
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"MULTI"])),
        "-ERR MULTI calls can not be nested"
    );
    assert!(s.in_multi);
    assert_eq!(
        s.queued.len(),
        1,
        "the queue is intact after a nested MULTI"
    );
}

#[test]
fn queue_time_arity_error_dirties_and_exec_aborts() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    // A valid queued write first.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]),
        Value::simple("QUEUED")
    );
    // GET with no key: a queue-time ARITY error reported NOW, and the txn dirtied.
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"GET"])),
        "-ERR wrong number of arguments for 'get' command"
    );
    assert!(s.dirty_exec);
    // EXEC -> EXECABORT, nothing applied (k absent), transaction cleared.
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-EXECABORT Transaction discarded because of previous errors."
    );
    assert!(!s.in_multi);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
}

#[test]
fn queue_time_unknown_command_dirties_and_exec_aborts() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    // An unknown command inside MULTI: the unknown-command error NOW + dirty.
    match run_on(&c, &mut s, &mut st, t, &[b"FROBNICATE", b"a"]) {
        Value::Error(e) => assert!(
            e.line().starts_with("-ERR unknown command 'FROBNICATE'"),
            "{}",
            e.line()
        ),
        other => panic!("expected unknown-command error, got {other:?}"),
    }
    assert!(s.dirty_exec);
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-EXECABORT Transaction discarded because of previous errors."
    );
    assert!(!s.in_multi);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), Value::Null);
}

#[test]
fn wrong_arity_exec_inside_multi_dirties_and_next_exec_aborts() {
    // commandCheckArity runs BEFORE the MULTI queue block in Redis, so a wrong-arity
    // control verb (here EXEC) issued inside a transaction DIRTIES it: the bad EXEC
    // replies its arity error, the txn stays OPEN + dirty, and a SUBSEQUENT clean EXEC
    // returns EXECABORT. (MULTI; EXEC x; EXEC -> +OK, arity error, EXECABORT.)
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    // EXEC with an extra arg: wrong arity reported NOW, txn dirtied but still open.
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC", b"x"])),
        "-ERR wrong number of arguments for 'exec' command"
    );
    assert!(s.in_multi, "the wrong-arity EXEC does NOT exit the txn");
    assert!(s.dirty_exec, "the wrong-arity EXEC dirties the txn");
    // A subsequent clean EXEC aborts because the txn is dirty.
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-EXECABORT Transaction discarded because of previous errors."
    );
    assert!(!s.in_multi);
}

#[test]
fn wrong_arity_multi_inside_multi_dirties_and_next_exec_aborts() {
    // Same as the EXEC case but with a wrong-arity MULTI: it dirties the open txn (a
    // bad-arity control verb is rejected before the nested-MULTI check), so the later
    // clean EXEC aborts. (MULTI; MULTI x; EXEC -> +OK, arity error, EXECABORT.)
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"MULTI", b"x"])),
        "-ERR wrong number of arguments for 'multi' command"
    );
    assert!(s.in_multi, "the wrong-arity MULTI does NOT exit the txn");
    assert!(s.dirty_exec, "the wrong-arity MULTI dirties the txn");
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-EXECABORT Transaction discarded because of previous errors."
    );
    assert!(!s.in_multi);
}

#[test]
fn wrong_arity_discard_inside_multi_dirties_and_next_exec_aborts() {
    // Same with a wrong-arity DISCARD: it dirties the open txn (the arity failure is
    // before the queue block) and does NOT discard it; the later clean EXEC aborts.
    // (MULTI; DISCARD x; EXEC -> +OK, arity error, EXECABORT.)
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"DISCARD", b"x"])),
        "-ERR wrong number of arguments for 'discard' command"
    );
    assert!(s.in_multi, "the wrong-arity DISCARD does NOT exit the txn");
    assert!(s.dirty_exec, "the wrong-arity DISCARD dirties the txn");
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-EXECABORT Transaction discarded because of previous errors."
    );
    assert!(!s.in_multi);
}

#[test]
fn wrong_arity_control_verb_outside_multi_is_a_plain_error() {
    // When NOT in a transaction, a wrong-arity control verb is just its arity error
    // (nothing to dirty): EXEC x -> arity error; a later clean EXEC is EXEC-without-
    // MULTI (NOT EXECABORT), confirming nothing was left dirty.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC", b"x"])),
        "-ERR wrong number of arguments for 'exec' command"
    );
    assert!(!s.in_multi);
    assert!(!s.dirty_exec);
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"DISCARD", b"x"])),
        "-ERR wrong number of arguments for 'discard' command"
    );
    assert!(!s.dirty_exec);
    // A clean EXEC now: EXEC-without-MULTI, not EXECABORT.
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-ERR EXEC without MULTI"
    );
}

#[test]
fn exec_does_not_roll_back_on_a_runtime_error() {
    // No rollback (TRANSACTIONS.md): a per-command runtime error at EXEC time becomes
    // an Error ELEMENT in the array; the batch continues and later writes apply.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // A string value that INCR cannot parse.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"sv", b"hello"]),
        Value::ok()
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"sv"]); // will fail at run time
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"s2", b"ok"]); // must still apply
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Value::Error(e) => {
                    assert_eq!(e.line(), "-ERR value is not an integer or out of range");
                }
                other => panic!("element 0 should be the INCR error, got {other:?}"),
            }
            assert_eq!(items[1], Value::ok());
        }
        other => panic!("EXEC -> {other:?}"),
    }
    // No rollback: s2 was set despite the earlier error element.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"s2"]),
        bulk(b"ok")
    );
}

#[test]
fn reset_mid_multi_clears_the_transaction() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    // RESET inside MULTI clears the transaction (it is in the queue-gate exclusion
    // set, so it runs immediately and resets the connection).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RESET"]),
        Value::SimpleString("RESET".to_owned())
    );
    assert!(!s.in_multi);
    assert!(s.queued.is_empty());
    // A subsequent EXEC is now "without MULTI".
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"EXEC"])),
        "-ERR EXEC without MULTI"
    );
}

#[test]
fn per_command_admission_runs_inside_exec() {
    // The maxmemory denyoom gate is evaluated PER QUEUED COMMAND at EXEC time (it
    // lives in dispatch_inner). With a tiny budget + noeviction, a queued write that
    // tips strictly over budget becomes an -OOM error ELEMENT in the array; the batch
    // does not roll back the writes that already applied.
    let c = ctx_with_budget(50);
    let mut s = state(&c);
    let mut st = store_with(c.databases, Policy::NoEviction);
    let t = UnixMillis(0);
    let big = vec![b'v'; 100];
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    // First queued SET: at EXEC time used starts at 0 (< 50), so it is served and
    // pushes the store over budget.
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k1", &big]);
    // Second queued SET: at EXEC time used is now strictly over budget -> -OOM.
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k2", &big]);
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::ok(), "first write served");
            match &items[1] {
                Value::Error(e) => assert_eq!(
                    e.line(),
                    "-OOM command not allowed when used memory > 'maxmemory'."
                ),
                other => panic!("element 1 should be -OOM, got {other:?}"),
            }
        }
        other => panic!("EXEC -> {other:?}"),
    }
    // No rollback: k1 is present, k2 was rejected.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k1"]), bulk(&big));
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"k2"]),
        Value::Null
    );
}

#[test]
fn control_commands_are_not_queued_inside_multi() {
    // MULTI/EXEC/DISCARD/RESET/QUIT are NOT staged: they act on the connection even
    // while in a transaction. Here QUIT inside MULTI closes (and is not queued).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(s.queued.len(), 1);
    // QUIT runs immediately (sets should_close), not queued.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"QUIT"]), Value::ok());
    assert!(s.should_close);
    assert_eq!(s.queued.len(), 1, "QUIT was not queued");
}

// -- WATCH/UNWATCH optimistic-lock dirty-CAS (TRANSACTIONS.md, PR-10b). These drive
// dispatch over a PERSISTENT store via run_on; the cross-connection tests drive two
// ConnStates against the SAME store (the per-key version slots are shared on the one
// accept shard, single-shard-per-connection). --

#[test]
fn cas_abort_same_connection_modifies_then_exec_is_null() {
    // WATCH k; SET k v (same connection, before MULTI); MULTI; INCR k; EXEC -> Null;
    // nothing applied (the optimistic lock saw k change between WATCH and EXEC).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
        Value::ok()
    );
    // Modify the watched key (a plain SET runs now, it is not in MULTI).
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"2"]);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]),
        Value::simple("QUEUED")
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
        Value::Array(None),
        "a dirtied watch makes EXEC return the null array"
    );
    assert!(!s.in_multi);
    assert!(s.watch.is_empty(), "EXEC cleared the watch set");
    // The INCR did NOT apply: k is still "2" (from the modification), not 3.
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"2"));
}

#[test]
fn cas_pass_unmodified_then_exec_runs() {
    // WATCH k; (no modification); MULTI; INCR k; EXEC -> runs; k incremented.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
        Value::ok()
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Value::Integer(2));
        }
        other => panic!("EXEC -> {other:?}"),
    }
    assert!(s.watch.is_empty(), "EXEC cleared the watch set");
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"2"));
}

#[test]
fn cas_abort_cross_connection() {
    // conn1 WATCH k; conn2 SET k v (on the SAME store); conn1 MULTI; INCR k; EXEC ->
    // Null. Two connections, one shared accept shard (single-shard-per-connection).
    let c = ctx(None);
    let mut s1 = state(&c);
    let mut s2 = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s1, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(
        run_on(&c, &mut s1, &mut st, t, &[b"WATCH", b"k"]),
        Value::ok()
    );
    // conn2 modifies the watched key.
    let _ = run_on(&c, &mut s2, &mut st, t, &[b"SET", b"k", b"99"]);
    assert_eq!(run_on(&c, &mut s1, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s1, &mut st, t, &[b"INCR", b"k"]);
    assert_eq!(
        run_on(&c, &mut s1, &mut st, t, &[b"EXEC"]),
        Value::Array(None),
        "another connection's write on the same shard aborts the watcher's EXEC"
    );
    assert_eq!(
        run_on(&c, &mut s1, &mut st, t, &[b"GET", b"k"]),
        bulk(b"99")
    );
}

#[test]
fn unwatch_cancels_the_watch() {
    // WATCH k; UNWATCH; modify k; MULTI; INCR k; EXEC -> runs (the watch was canceled
    // so the later modification does not abort).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
        Value::ok()
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"UNWATCH"]), Value::ok());
    assert!(s.watch.is_empty(), "UNWATCH cleared the watch set");
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"5"]);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => assert_eq!(items[0], Value::Integer(6)),
        other => panic!("EXEC -> {other:?}"),
    }
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"6"));
}

#[test]
fn watch_inside_multi_errors_without_dirtying() {
    // MULTI; WATCH k -> the error, txn stays OPEN + CLEAN; a following SET queues; EXEC
    // runs (NOT EXECABORT: WATCH-inside-MULTI does not dirty the batch).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"])),
        "-ERR WATCH inside MULTI is not allowed"
    );
    // The txn is intact: still in MULTI, NOT dirty, watch set empty (WATCH did not run).
    assert!(s.in_multi);
    assert!(!s.dirty_exec, "WATCH inside MULTI does not dirty the batch");
    assert!(s.watch.is_empty());
    // A following command still queues, and EXEC runs cleanly.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"7"]),
        Value::simple("QUEUED")
    );
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Value::ok());
        }
        other => panic!("EXEC after WATCH-inside-MULTI -> {other:?}"),
    }
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]), bulk(b"7"));
}

#[test]
fn unwatch_inside_multi_queues_and_runs_at_exec() {
    // UNWATCH inside MULTI is a NORMAL command: it QUEUES (+QUEUED) and runs at EXEC
    // (as a +OK element). It is NOT control-flow (unlike WATCH).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"UNWATCH"]),
        Value::simple("QUEUED"),
        "UNWATCH queues inside MULTI"
    );
    assert_eq!(s.queued.len(), 1);
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Value::ok(), "the queued UNWATCH ran as +OK");
        }
        other => panic!("EXEC -> {other:?}"),
    }
}

#[test]
fn no_op_write_dirties_the_watch_through_dispatch() {
    // SADD s a; WATCH s; SADD s a (already a member -> no value change); MULTI; INCR x;
    // EXEC -> Null (the no-op write still bumped the version through dispatch).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SADD", b"s", b"a"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"s"]),
        Value::ok()
    );
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SADD", b"s", b"a"]); // no-op
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"x"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
        Value::Array(None)
    );
}

#[test]
fn watched_key_expiry_dirties_through_dispatch() {
    // SET k v EX (a short TTL via PEXPIRE); WATCH k; advance `now` past the deadline so
    // the lazy reap fires inside the EXEC CAS check; MULTI; INCR k; EXEC -> Null.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let mut wheel = TimingWheel::new();
    let t0 = UnixMillis(0);
    let _ = run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"SET", b"k", b"1"]);
    // Set a deadline at t=10 (PEXPIRE 10 against now=0).
    let _ = run_on_wheel(
        &c,
        &mut s,
        &mut st,
        &mut wheel,
        t0,
        &[b"PEXPIRE", b"k", b"10"],
    );
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"WATCH", b"k"]),
        Value::ok()
    );
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"MULTI"]),
        Value::ok()
    );
    let _ = run_on_wheel(&c, &mut s, &mut st, &mut wheel, t0, &[b"INCR", b"k"]);
    // EXEC at t=100 (past the deadline): the watched key has expired -> Null.
    let t_late = UnixMillis(100);
    assert_eq!(
        run_on_wheel(&c, &mut s, &mut st, &mut wheel, t_late, &[b"EXEC"]),
        Value::Array(None),
        "an expiry of the watched key aborts EXEC"
    );
}

#[test]
fn already_absent_watch_stays_clean_through_dispatch() {
    // WATCH missing; (stays missing); MULTI; SET other v; EXEC -> runs.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"missing"]),
        Value::ok()
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"other", b"v"]);
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => assert_eq!(items[0], Value::ok()),
        other => panic!("EXEC -> {other:?}"),
    }
}

#[test]
fn watched_absent_then_created_aborts_through_dispatch() {
    // WATCH missing; SET missing v; MULTI; INCR x; EXEC -> Null.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"missing"]),
        Value::ok()
    );
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"missing", b"v"]);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"x"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
        Value::Array(None)
    );
}

#[test]
fn flushdb_dirties_a_watch_through_dispatch() {
    // SET k v; WATCH k; FLUSHDB; MULTI; SET k 2; EXEC -> Null.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
        Value::ok()
    );
    let _ = run_on(&c, &mut s, &mut st, t, &[b"FLUSHDB"]);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"2"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
        Value::Array(None)
    );
}

#[test]
fn discard_clears_the_watch_set() {
    // WATCH k; MULTI; DISCARD -> the watch set is cleared (a later modification +
    // MULTI/EXEC runs, the watch was dropped by DISCARD).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
        Value::ok()
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"DISCARD"]), Value::ok());
    assert!(s.watch.is_empty(), "DISCARD cleared the watch set");
    // The watch is gone: a modification then MULTI/EXEC runs.
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"9"]);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => assert_eq!(items[0], Value::Integer(10)),
        other => panic!("EXEC -> {other:?}"),
    }
}

#[test]
fn reset_clears_the_watch_set() {
    // WATCH k; RESET -> the watch set is cleared (and the store deregistered, so a
    // later modification + MULTI/EXEC runs).
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"k"]),
        Value::ok()
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"RESET"]),
        Value::SimpleString("RESET".to_owned())
    );
    assert!(s.watch.is_empty(), "RESET cleared the watch set");
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"4"]);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"k"]);
    match run_on(&c, &mut s, &mut st, t, &[b"EXEC"]) {
        Value::Array(Some(items)) => assert_eq!(items[0], Value::Integer(5)),
        other => panic!("EXEC -> {other:?}"),
    }
}

#[test]
fn watch_arity_and_multi_key() {
    // WATCH with no key -> arity error; WATCH of several keys, any one dirtied aborts.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"WATCH"])),
        "-ERR wrong number of arguments for 'watch' command"
    );
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"a", b"1"]);
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"1"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"WATCH", b"a", b"b"]),
        Value::ok()
    );
    assert_eq!(s.watch.len(), 2, "both keys snapshotted");
    // Modify the SECOND watched key only -> EXEC still aborts.
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"b", b"2"]);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"MULTI"]), Value::ok());
    let _ = run_on(&c, &mut s, &mut st, t, &[b"INCR", b"a"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXEC"]),
        Value::Array(None)
    );
}

// -- SHUTDOWN [NOSAVE|SAVE] grammar (#139, SHUTDOWN.md). The serve layer drives the actual stop;
// these cover the SHARED modifier parser + the never-intercepted dispatch fallback. --

#[test]
fn parse_shutdown_resolves_the_three_modes() {
    // Bare SHUTDOWN -> Default (save iff a save policy is configured).
    assert_eq!(
        parse_shutdown(&req(&[b"SHUTDOWN"])),
        Ok(ShutdownMode::Default)
    );
    // SAVE / NOSAVE, case-insensitive (RESP args are byte slices; Redis matches case-insensitive).
    assert_eq!(
        parse_shutdown(&req(&[b"SHUTDOWN", b"SAVE"])),
        Ok(ShutdownMode::Save)
    );
    assert_eq!(
        parse_shutdown(&req(&[b"SHUTDOWN", b"save"])),
        Ok(ShutdownMode::Save)
    );
    assert_eq!(
        parse_shutdown(&req(&[b"SHUTDOWN", b"NOSAVE"])),
        Ok(ShutdownMode::NoSave)
    );
    assert_eq!(
        parse_shutdown(&req(&[b"SHUTDOWN", b"NoSave"])),
        Ok(ShutdownMode::NoSave)
    );
}

#[test]
fn parse_shutdown_rejects_a_bad_or_extra_modifier() {
    // An unknown modifier is a syntax error...
    match parse_shutdown(&req(&[b"SHUTDOWN", b"FORCE"])) {
        Err(e) => assert_eq!(e.line(), "-ERR syntax error"),
        Ok(m) => panic!("unknown modifier must be a syntax error, got {m:?}"),
    }
    // ...and so is more than one modifier.
    match parse_shutdown(&req(&[b"SHUTDOWN", b"SAVE", b"NOSAVE"])) {
        Err(e) => assert_eq!(e.line(), "-ERR syntax error"),
        Ok(m) => panic!("two modifiers must be a syntax error, got {m:?}"),
    }
}

#[test]
fn shutdown_fallback_validates_grammar_without_exiting() {
    // The never-intercepted fallback (e.g. a SHUTDOWN reaching dispatch directly) does NOT exit
    // the process (the serve layer owns the exit); it replies +OK on a valid form and a syntax
    // error on a bad modifier. The dispatch arm routes here, so run the real dispatch.
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"SHUTDOWN"]), Value::ok());
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SHUTDOWN", b"NOSAVE"]),
        Value::ok()
    );
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"SHUTDOWN", b"BOGUS"])),
        "-ERR syntax error"
    );
}

// ===================================================================================
// Drop-in compatibility commands: GETRANGE/SUBSTR/SETRANGE/GETDEL/MSETNX, LMPOP/ZMPOP,
// SORT/SORT_RO. Each exercises happy path + the edge cases (negative indices, empty/
// missing key, WRONGTYPE, arity, COUNT, all-or-nothing, numeric vs ALPHA, STORE).
// ===================================================================================

#[test]
fn getrange_signed_range_and_edges() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"Hello World"]);
    // A basic in-bounds range.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETRANGE", b"k", b"0", b"4"]),
        bulk(b"Hello")
    );
    // Negative indices count from the end.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETRANGE", b"k", b"-5", b"-1"]),
        bulk(b"World")
    );
    // The whole string.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETRANGE", b"k", b"0", b"-1"]),
        bulk(b"Hello World")
    );
    // An out-of-range end is clamped.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETRANGE", b"k", b"0", b"1000"]),
        bulk(b"Hello World")
    );
    // start > end -> the empty bulk.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETRANGE", b"k", b"5", b"2"]),
        bulk(b"")
    );
    // A MISSING key -> the empty bulk (NOT nil).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"GETRANGE", b"missing", b"0", b"-1"]
        ),
        bulk(b"")
    );
    // SUBSTR is byte-identical.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SUBSTR", b"k", b"0", b"4"]),
        bulk(b"Hello")
    );
    // Arity + non-integer + WRONGTYPE.
    assert_eq!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"GETRANGE", b"k", b"0"])),
        "-ERR wrong number of arguments for 'getrange' command"
    );
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"GETRANGE", b"k", b"x", b"1"]
        ))
        .contains("not an integer")
    );
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"lst", b"a"]);
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"GETRANGE", b"lst", b"0", b"1"]
        ))
        .contains("WRONGTYPE")
    );
}

#[test]
fn setrange_overwrite_zero_pad_and_edges() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Overwrite in place: "Hello World" with "Redis" at offset 6 -> "Hello Redis", len 11.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"Hello World"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SETRANGE", b"k", b"6", b"Redis"]),
        iv(11)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"k"]),
        bulk(b"Hello Redis")
    );
    // Zero-pad-extend on a missing key: offset 5, "x" -> 5 NUL bytes + "x", len 6.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SETRANGE", b"pad", b"5", b"x"]),
        iv(6)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GET", b"pad"]),
        bulk(b"\x00\x00\x00\x00\x00x")
    );
    // An EMPTY value is a no-op returning the current length; it does NOT create a key.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SETRANGE", b"empty", b"0", b""]),
        iv(0)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"empty"]),
        iv(0)
    );
    // A negative offset is the out-of-range error.
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SETRANGE", b"k", b"-1", b"x"]
        ))
        .contains("offset is out of range")
    );
    // WRONGTYPE.
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"lst", b"a"]);
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SETRANGE", b"lst", b"0", b"x"]
        ))
        .contains("WRONGTYPE")
    );
}

#[test]
fn getdel_gets_then_deletes() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"k", b"v"]);
    // GETDEL returns the value AND removes the key.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETDEL", b"k"]),
        bulk(b"v")
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"k"]), iv(0));
    // A second GETDEL on the now-missing key -> nil.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"GETDEL", b"k"]),
        Value::Null
    );
    // WRONGTYPE leaves the key intact (no delete).
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"lst", b"a"]);
    assert!(err_of(run_on(&c, &mut s, &mut st, t, &[b"GETDEL", b"lst"])).contains("WRONGTYPE"));
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"lst"]), iv(1));
}

#[test]
fn msetnx_all_or_nothing() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // All absent -> set them all, reply 1.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"MSETNX", b"a", b"1", b"b", b"2"]),
        iv(1)
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"a"]), bulk(b"1"));
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"b"]), bulk(b"2"));
    // ONE already exists (a) -> NOTHING is written, reply 0 (c stays absent).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"MSETNX", b"c", b"3", b"a", b"X"]),
        iv(0)
    );
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"EXISTS", b"c"]), iv(0));
    assert_eq!(run_on(&c, &mut s, &mut st, t, &[b"GET", b"a"]), bulk(b"1"));
    // An odd arg count is the wrong-arity error.
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"MSETNX", b"x", b"1", b"y"]
        ))
        .contains("wrong number of arguments")
    );
}

#[test]
fn lmpop_first_non_empty_and_count() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // l2 = [a, b, c]; l1 missing. LMPOP picks the FIRST non-empty (l2), LEFT pops 'a'.
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"l2", b"a", b"b", b"c"]);
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMPOP", b"2", b"l1", b"l2", b"LEFT"]
        ),
        Value::Array(Some(vec![bulk(b"l2"), arr(&[b"a"])]))
    );
    // COUNT pops several from the chosen end (RIGHT here: c then b).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMPOP", b"2", b"l1", b"l2", b"RIGHT", b"COUNT", b"2"]
        ),
        Value::Array(Some(vec![bulk(b"l2"), arr(&[b"c", b"b"])]))
    );
    // All keys missing/empty -> the null ARRAY (Redis addReplyNullArray).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMPOP", b"2", b"l1", b"l2", b"LEFT"]
        ),
        Value::Array(None)
    );
    // WRONGTYPE if the first EXISTING key is the wrong type.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"x"]);
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMPOP", b"1", b"str", b"LEFT"]
        ))
        .contains("WRONGTYPE")
    );
    // numkeys must be positive; a missing direction is a syntax error; arity.
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMPOP", b"0", b"k", b"LEFT"]
        ))
        .contains("numkeys")
    );
    assert_eq!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"LMPOP", b"1", b"k", b"SIDE"]
        )),
        "-ERR syntax error"
    );
    assert!(
        err_of(run_on(&c, &mut s, &mut st, t, &[b"LMPOP", b"1"]))
            .contains("wrong number of arguments")
    );
}

#[test]
fn zmpop_min_max_and_count() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // z2 = {a:1, b:2, c:3}. ZMPOP MIN pops the lowest (a, 1).
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"ZADD", b"z2", b"1", b"a", b"2", b"b", b"3", b"c"],
    );
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"ZMPOP", b"2", b"z1", b"z2", b"MIN"]
        ),
        Value::Array(Some(vec![
            bulk(b"z2"),
            Value::Array(Some(vec![Value::Array(Some(vec![bulk(b"a"), bulk(b"1")]))])),
        ]))
    );
    // MAX with COUNT 2 pops the two highest (c,3 then b,2).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"ZMPOP", b"2", b"z1", b"z2", b"MAX", b"COUNT", b"2"]
        ),
        Value::Array(Some(vec![
            bulk(b"z2"),
            Value::Array(Some(vec![
                Value::Array(Some(vec![bulk(b"c"), bulk(b"3")])),
                Value::Array(Some(vec![bulk(b"b"), bulk(b"2")])),
            ])),
        ]))
    );
    // All empty -> the null ARRAY (Redis addReplyNullArray).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"ZMPOP", b"2", b"z1", b"z2", b"MIN"]
        ),
        Value::Array(None)
    );
    // WRONGTYPE on the first existing non-zset key.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"x"]);
    assert!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"ZMPOP", b"1", b"str", b"MIN"]
        ))
        .contains("WRONGTYPE")
    );
}

#[test]
fn sort_numeric_alpha_limit_desc() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // A numeric list sorts ascending by default.
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"RPUSH", b"nums", b"3", b"1", b"2", b"10"],
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"nums"]),
        arr(&[b"1", b"2", b"3", b"10"])
    );
    // DESC reverses.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"nums", b"DESC"]),
        arr(&[b"10", b"3", b"2", b"1"])
    );
    // LIMIT offset count (after sort).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SORT", b"nums", b"LIMIT", b"1", b"2"]
        ),
        arr(&[b"2", b"3"])
    );
    // ALPHA sorts lexicographically (so "10" < "2").
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"nums", b"ALPHA"]),
        arr(&[b"1", b"10", b"2", b"3"])
    );
    // A non-numeric element WITHOUT ALPHA is the SORT-not-numbers error.
    run_on(&c, &mut s, &mut st, t, &[b"RPUSH", b"words", b"b", b"a"]);
    assert!(err_of(run_on(&c, &mut s, &mut st, t, &[b"SORT", b"words"])).contains("not numbers"));
    // ALPHA on those words works.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"words", b"ALPHA"]),
        arr(&[b"a", b"b"])
    );
    // SORT of a missing key is an empty array.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"missing"]),
        Value::Array(Some(vec![]))
    );
    // SORT of a string is WRONGTYPE.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"str", b"x"]);
    assert!(err_of(run_on(&c, &mut s, &mut st, t, &[b"SORT", b"str"])).contains("WRONGTYPE"));
}

#[test]
fn sort_sorts_sets_and_zsets() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // A SET sorts by member value (numeric).
    run_on(&c, &mut s, &mut st, t, &[b"SADD", b"set", b"3", b"1", b"2"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"set"]),
        arr(&[b"1", b"2", b"3"])
    );
    // A ZSET sorts by MEMBER value (the zset's own scores are ignored without BY).
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"ZADD", b"z", b"100", b"3", b"200", b"1", b"300", b"2"],
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"z"]),
        arr(&[b"1", b"2", b"3"])
    );
}

#[test]
fn sort_by_get_and_store() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    run_on(
        &c,
        &mut s,
        &mut st,
        t,
        &[b"RPUSH", b"ids", b"1", b"2", b"3"],
    );
    // BY weight_* with external string keys: weight_1=30, weight_2=10, weight_3=20.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"weight_1", b"30"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"weight_2", b"10"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"weight_3", b"20"]);
    // Sorted by external weight: 2(10), 3(20), 1(30).
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SORT", b"ids", b"BY", b"weight_*"]
        ),
        arr(&[b"2", b"3", b"1"])
    );
    // GET # returns the element; GET data_* dereferences a string key.
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"data_1", b"one"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"data_2", b"two"]);
    run_on(&c, &mut s, &mut st, t, &[b"SET", b"data_3", b"three"]);
    // Sorted by weight (2,3,1), projecting # then data_*: [2, two, 3, three, 1, one].
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[
                b"SORT",
                b"ids",
                b"BY",
                b"weight_*",
                b"GET",
                b"#",
                b"GET",
                b"data_*"
            ]
        ),
        Value::Array(Some(vec![
            bulk(b"2"),
            bulk(b"two"),
            bulk(b"3"),
            bulk(b"three"),
            bulk(b"1"),
            bulk(b"one"),
        ]))
    );
    // BY a hash field: h_1->w etc.
    run_on(&c, &mut s, &mut st, t, &[b"HSET", b"h_1", b"w", b"3"]);
    run_on(&c, &mut s, &mut st, t, &[b"HSET", b"h_2", b"w", b"1"]);
    run_on(&c, &mut s, &mut st, t, &[b"HSET", b"h_3", b"w", b"2"]);
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"ids", b"BY", b"h_*->w"]),
        arr(&[b"2", b"3", b"1"])
    );
    // BY a pattern with NO `*` is the nosort shortcut (preserve source order 1,2,3).
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"SORT", b"ids", b"BY", b"nosort"]),
        arr(&[b"1", b"2", b"3"])
    );
    // STORE writes the result as a LIST and returns the count; SORT_RO has no STORE.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SORT", b"ids", b"BY", b"weight_*", b"STORE", b"dest"]
        ),
        iv(3)
    );
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"LRANGE", b"dest", b"0", b"-1"]),
        arr(&[b"2", b"3", b"1"])
    );
    // SORT_RO rejects STORE as a syntax error.
    assert_eq!(
        err_of(run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SORT_RO", b"ids", b"STORE", b"dest"]
        )),
        "-ERR syntax error"
    );
    // SORT_RO without STORE works like SORT.
    assert_eq!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"SORT_RO", b"ids", b"BY", b"weight_*"]
        ),
        arr(&[b"2", b"3", b"1"])
    );
}

// -- HOTKEYS (#428): the faithful Redis 8.6 hot-key tracking container ----------------------

/// Pull a field's value out of a HOTKEYS GET map reply by name.
fn hk_field<'a>(reply: &'a Value, name: &str) -> &'a Value {
    let Value::Map(pairs) = reply else {
        panic!("HOTKEYS GET must be a Map, got {reply:?}");
    };
    for (k, v) in pairs {
        if *k == Value::bulk_str(name) {
            return v;
        }
    }
    panic!("HOTKEYS GET missing field {name}");
}

#[test]
fn hotkeys_lifecycle_and_get_shape() {
    let c = ctx(None);
    let mut s = state(&c);
    // No session yet: GET is null.
    assert_eq!(run(&c, &mut s, &[b"HOTKEYS", b"GET"]), Value::Null);
    // START with both metrics.
    assert_eq!(
        run(
            &c,
            &mut s,
            &[b"HOTKEYS", b"START", b"METRICS", b"2", b"CPU", b"NET"]
        ),
        Value::ok()
    );
    // Seed the sketch directly (the per-command recording hook lives in the serve layer; here we
    // exercise the COMMAND surface that reads it), mirroring the SLOWLOG test.
    c.hotkeys.record(&[b"hot"], 100, 40, 0);
    c.hotkeys.record(&[b"hot"], 100, 40, 0);
    c.hotkeys.record(&[b"cold"], 1, 1, 0);
    let g = run(&c, &mut s, &[b"HOTKEYS", b"GET"]);
    assert_eq!(*hk_field(&g, "tracking-active"), Value::Integer(1));
    assert_eq!(*hk_field(&g, "sample-ratio"), Value::Integer(1));
    assert_eq!(
        *hk_field(&g, "all-commands-all-slots-us"),
        Value::Integer(201)
    );
    // by-cpu-time-us is a flat [key, val, ...] array with `hot` ranked first (200).
    match hk_field(&g, "by-cpu-time-us") {
        Value::Array(Some(items)) => {
            assert_eq!(items[0], Value::bulk(bytes::Bytes::from_static(b"hot")));
            assert_eq!(items[1], Value::Integer(200));
        }
        other => panic!("by-cpu-time-us must be an array, got {other:?}"),
    }
    // Double START errors.
    assert!(matches!(
        run(
            &c,
            &mut s,
            &[b"HOTKEYS", b"START", b"METRICS", b"1", b"CPU"]
        ),
        Value::Error(_)
    ));
    // RESET while active errors.
    assert!(matches!(
        run(&c, &mut s, &[b"HOTKEYS", b"RESET"]),
        Value::Error(_)
    ));
    // STOP preserves data; GET now reports inactive but keeps the totals.
    assert_eq!(run(&c, &mut s, &[b"HOTKEYS", b"STOP"]), Value::ok());
    let g2 = run(&c, &mut s, &[b"HOTKEYS", b"GET"]);
    assert_eq!(*hk_field(&g2, "tracking-active"), Value::Integer(0));
    assert_eq!(
        *hk_field(&g2, "all-commands-all-slots-us"),
        Value::Integer(201)
    );
    // RESET when stopped -> GET null again.
    assert_eq!(run(&c, &mut s, &[b"HOTKEYS", b"RESET"]), Value::ok());
    assert_eq!(run(&c, &mut s, &[b"HOTKEYS", b"GET"]), Value::Null);
}

#[test]
fn hotkeys_only_selected_metric_appears() {
    let c = ctx(None);
    let mut s = state(&c);
    run(
        &c,
        &mut s,
        &[b"HOTKEYS", b"START", b"METRICS", b"1", b"CPU"],
    );
    let g = run(&c, &mut s, &[b"HOTKEYS", b"GET"]);
    // CPU selected -> its fields present; NET not selected -> its fields absent.
    let present = matches!(g, Value::Map(ref p) if p.iter().any(|(k, _)| *k == Value::bulk_str("by-cpu-time-us")));
    let net_absent = matches!(g, Value::Map(ref p) if !p.iter().any(|(k, _)| *k == Value::bulk_str("by-net-bytes")));
    assert!(present, "by-cpu-time-us present");
    assert!(net_absent, "by-net-bytes absent when NET not selected");
}

#[test]
fn hotkeys_start_validation() {
    let c = ctx(None);
    let mut s = state(&c);
    // Missing METRICS.
    assert!(matches!(
        run(&c, &mut s, &[b"HOTKEYS", b"START"]),
        Value::Error(_)
    ));
    // METRICS count mismatch / no real metric.
    assert!(matches!(
        run(
            &c,
            &mut s,
            &[b"HOTKEYS", b"START", b"METRICS", b"1", b"BOGUS"]
        ),
        Value::Error(_)
    ));
    // SAMPLE 0 is invalid (ratio must be >= 1).
    assert!(matches!(
        run(
            &c,
            &mut s,
            &[
                b"HOTKEYS", b"START", b"METRICS", b"1", b"CPU", b"SAMPLE", b"0"
            ]
        ),
        Value::Error(_)
    ));
    // STOP with no active session errors.
    assert!(matches!(
        run(&c, &mut s, &[b"HOTKEYS", b"STOP"]),
        Value::Error(_)
    ));
    // Unknown subcommand errors.
    assert!(matches!(
        run(&c, &mut s, &[b"HOTKEYS", b"BOGUS"]),
        Value::Error(_)
    ));
}

// -- PROD-7 operability: SLOWLOG / MEMORY / LATENCY / CLIENT extensions ---------------------

#[test]
fn slowlog_get_len_reset() {
    let c = ctx(None);
    let mut s = state(&c);
    // Seed the ring directly (the per-command timing hook lives in the serve layer; here we
    // exercise the COMMAND surface that reads/resets it).
    c.slowlog.record(
        100,
        50_000,
        &[b"GET".to_vec(), b"k".to_vec()],
        "1.1.1.1:1".into(),
        "app".into(),
    );
    c.slowlog.record(
        200,
        90_000,
        &[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()],
        "1.1.1.1:2".into(),
        String::new(),
    );
    // SLOWLOG LEN.
    assert_eq!(run(&c, &mut s, &[b"SLOWLOG", b"LEN"]), Value::Integer(2));
    // SLOWLOG GET: newest first, the 6-element entry shape.
    match run(&c, &mut s, &[b"SLOWLOG", b"GET"]) {
        Value::Array(Some(entries)) => {
            assert_eq!(entries.len(), 2);
            match &entries[0] {
                Value::Array(Some(fields)) => {
                    assert_eq!(fields.len(), 6);
                    assert_eq!(fields[0], Value::Integer(1)); // id (newest)
                    assert_eq!(fields[1], Value::Integer(200)); // unix ts
                    assert_eq!(fields[2], Value::Integer(90_000)); // micros
                    // args = [SET, k, v]
                    match &fields[3] {
                        Value::Array(Some(args)) => assert_eq!(args.len(), 3),
                        other => panic!("expected args array, got {other:?}"),
                    }
                    assert_eq!(fields[4], Value::bulk_str("1.1.1.1:2")); // client addr
                    assert_eq!(fields[5], Value::bulk_str("")); // client name
                }
                other => panic!("expected entry array, got {other:?}"),
            }
        }
        other => panic!("expected SLOWLOG GET array, got {other:?}"),
    }
    // SLOWLOG GET 1 returns only the newest.
    match run(&c, &mut s, &[b"SLOWLOG", b"GET", b"1"]) {
        Value::Array(Some(e)) => assert_eq!(e.len(), 1),
        other => panic!("expected one entry, got {other:?}"),
    }
    // SLOWLOG RESET empties the ring.
    assert_eq!(run(&c, &mut s, &[b"SLOWLOG", b"RESET"]), Value::ok());
    assert_eq!(run(&c, &mut s, &[b"SLOWLOG", b"LEN"]), Value::Integer(0));
    // SLOWLOG HELP is an array.
    assert!(matches!(
        run(&c, &mut s, &[b"SLOWLOG", b"HELP"]),
        Value::Array(Some(_))
    ));
    // Unknown subcommand errors.
    assert!(matches!(
        run(&c, &mut s, &[b"SLOWLOG", b"BOGUS"]),
        Value::Error(_)
    ));
}

#[test]
fn slowlog_threshold_gate_in_the_record_path() {
    // The threshold decision is the SlowLog's: a command at/over the threshold is recorded; a
    // fast one is not; -1 disables. (The per-command HOOK that applies this lives in the serve
    // layer; here we assert the underlying gate the hook relies on.)
    let sl = ironcache_observe::SlowLog::with_config(10_000, 128); // 10ms threshold
    assert!(sl.enabled());
    // A slow command (>= 10ms) appears; a fast one (1ms) does not -- the hook only calls
    // `record` when micros >= threshold, which we mimic here.
    sl.record(1, 20_000, &[b"SLOW".to_vec()], "a".into(), String::new());
    assert_eq!(sl.len(), 1);
    // Disabled threshold: the hook never reads the clock nor calls record.
    let off = ironcache_observe::SlowLog::with_config(-1, 128);
    assert!(!off.enabled());
}

#[test]
fn memory_usage_doctor_stats_help() {
    let c = ctx(None);
    let mut s = state(&c);
    let mut st = test_store(c.databases);
    let t = UnixMillis(0);
    // Plant a key, then MEMORY USAGE returns an integer estimate >= key+value bytes.
    let _ = run_on(&c, &mut s, &mut st, t, &[b"SET", b"mykey", b"value123"]);
    match run_on(&c, &mut s, &mut st, t, &[b"MEMORY", b"USAGE", b"mykey"]) {
        Value::Integer(n) => assert!(n as usize >= b"mykey".len() + b"value123".len()),
        other => panic!("expected integer estimate, got {other:?}"),
    }
    // MEMORY USAGE of a missing key is nil.
    assert_eq!(
        run_on(&c, &mut s, &mut st, t, &[b"MEMORY", b"USAGE", b"nope"]),
        Value::Null
    );
    // SAMPLES option is accepted.
    assert!(matches!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"MEMORY", b"USAGE", b"mykey", b"SAMPLES", b"5"]
        ),
        Value::Integer(_)
    ));
    // A bad option is a syntax error.
    assert!(matches!(
        run_on(
            &c,
            &mut s,
            &mut st,
            t,
            &[b"MEMORY", b"USAGE", b"mykey", b"BOGUS", b"5"]
        ),
        Value::Error(_)
    ));
    // MEMORY DOCTOR is a human bulk string.
    assert!(matches!(
        run(&c, &mut s, &[b"MEMORY", b"DOCTOR"]),
        Value::BulkString(Some(_))
    ));
    // MEMORY STATS is a field/value map.
    assert!(matches!(
        run(&c, &mut s, &[b"MEMORY", b"STATS"]),
        Value::Map(_)
    ));
    // MEMORY HELP is an array.
    assert!(matches!(
        run(&c, &mut s, &[b"MEMORY", b"HELP"]),
        Value::Array(Some(_))
    ));
}

#[test]
fn latency_reset_latest_history_doctor() {
    let c = ctx(None);
    let mut s = state(&c);
    // Seed the monitor directly (the per-command sample lives in the serve layer).
    c.latency.record("command", 100, 5);
    c.latency.record("command", 200, 42);
    // LATENCY LATEST: one 4-element [name, ts, latest-ms, max-ms] array.
    match run(&c, &mut s, &[b"LATENCY", b"LATEST"]) {
        Value::Array(Some(events)) => {
            assert_eq!(events.len(), 1);
            match &events[0] {
                Value::Array(Some(f)) => {
                    assert_eq!(f.len(), 4);
                    assert_eq!(f[0], Value::bulk_str("command"));
                    assert_eq!(f[2], Value::Integer(42)); // worst/latest ms
                }
                other => panic!("expected event array, got {other:?}"),
            }
        }
        other => panic!("expected LATEST array, got {other:?}"),
    }
    // LATENCY HISTORY command: 2-element [ts, ms] samples.
    match run(&c, &mut s, &[b"LATENCY", b"HISTORY", b"command"]) {
        Value::Array(Some(samples)) => assert_eq!(samples.len(), 2),
        other => panic!("expected HISTORY array, got {other:?}"),
    }
    // LATENCY DOCTOR is a bulk string.
    assert!(matches!(
        run(&c, &mut s, &[b"LATENCY", b"DOCTOR"]),
        Value::BulkString(Some(_))
    ));
    // LATENCY RESET command returns the count reset (1).
    assert_eq!(
        run(&c, &mut s, &[b"LATENCY", b"RESET", b"command"]),
        Value::Integer(1)
    );
    // After reset LATEST is empty.
    assert_eq!(
        run(&c, &mut s, &[b"LATENCY", b"LATEST"]),
        Value::Array(Some(vec![]))
    );
    // HELP is an array; unknown sub errors.
    assert!(matches!(
        run(&c, &mut s, &[b"LATENCY", b"HELP"]),
        Value::Array(Some(_))
    ));
    assert!(matches!(
        run(&c, &mut s, &[b"LATENCY", b"BOGUS"]),
        Value::Error(_)
    ));
}

#[test]
fn client_kill_pause_unpause_info() {
    let c = ctx(None);
    let mut s = state(&c);
    // Register two peers in the registry so KILL has targets.
    let h1 = c
        .clients
        .register(1, "1.1.1.1:1".into(), "0.0.0.0:6379".into(), 0);
    let _h2 = c
        .clients
        .register(2, "1.1.1.1:2".into(), "0.0.0.0:6379".into(), 0);
    // CLIENT INFO renders this connection's line.
    match run(&c, &mut s, &[b"CLIENT", b"INFO"]) {
        Value::BulkString(Some(b)) => {
            let line = String::from_utf8_lossy(&b);
            assert!(line.contains("id="));
            assert!(line.contains("addr="));
        }
        other => panic!("expected CLIENT INFO bulk, got {other:?}"),
    }
    // CLIENT KILL ID 1 (new filter form) returns the count killed (1) and flags the handle.
    assert_eq!(
        run(&c, &mut s, &[b"CLIENT", b"KILL", b"ID", b"1"]),
        Value::Integer(1)
    );
    assert!(h1.is_killed());
    // CLIENT KILL ADDR (old form) returns +OK on a match, an error on a miss.
    assert_eq!(
        run(&c, &mut s, &[b"CLIENT", b"KILL", b"1.1.1.1:2"]),
        Value::ok()
    );
    assert!(matches!(
        run(&c, &mut s, &[b"CLIENT", b"KILL", b"9.9.9.9:9"]),
        Value::Error(_)
    ));
    // CLIENT PAUSE 100 -> +OK and an active window; UNPAUSE clears it.
    assert_eq!(
        run(&c, &mut s, &[b"CLIENT", b"PAUSE", b"100000"]),
        Value::ok()
    );
    // The pause uses the TestEnv clock (now=0 in `run`), so the window is in the future.
    assert!(c.clients.is_paused(0));
    assert_eq!(run(&c, &mut s, &[b"CLIENT", b"UNPAUSE"]), Value::ok());
    assert!(!c.clients.is_paused(0));
    // CLIENT NO-EVICT on/off ack; a bad arg errors.
    assert_eq!(
        run(&c, &mut s, &[b"CLIENT", b"NO-EVICT", b"on"]),
        Value::ok()
    );
    assert!(matches!(
        run(&c, &mut s, &[b"CLIENT", b"NO-EVICT", b"maybe"]),
        Value::Error(_)
    ));
    // CLIENT PAUSE with a bad timeout errors.
    assert!(matches!(
        run(&c, &mut s, &[b"CLIENT", b"PAUSE", b"abc"]),
        Value::Error(_)
    ));
}

#[test]
fn info_completeness_has_new_fields_and_sections() {
    let c = ctx_full(None, 1024, "allkeys-lru");
    let mut s = state(&c);
    match run(&c, &mut s, &[b"INFO"]) {
        Value::BulkString(Some(b)) => {
            let body = String::from_utf8_lossy(&b);
            // Clients section gained maxclients + blocked_clients.
            assert!(body.contains("maxclients:"));
            assert!(body.contains("blocked_clients:"));
            // Stats section gained instantaneous_ops + rejected_connections.
            assert!(body.contains("instantaneous_ops_per_sec:"));
            assert!(body.contains("rejected_connections:"));
            assert!(body.contains("total_commands_processed:"));
            // Memory section reports maxmemory + fragmentation ratio.
            assert!(body.contains("maxmemory:"));
            assert!(body.contains("mem_fragmentation_ratio:"));
            // The new CPU section is present.
            assert!(body.contains("# CPU\r\n"));
            assert!(body.contains("used_cpu_sys:"));
        }
        other => panic!("expected INFO bulk, got {other:?}"),
    }
}

/// #549: under driven load INFO reports a NONZERO `instantaneous_ops_per_sec` that tracks the
/// rate. With the always-present metrics registry (the binary path), the ops/sec sampler is fed
/// the node-wide command total against the Env WALL clock on each INFO read; two reads a second
/// apart across 1000 driven commands report ~1000 ops/sec (0 before the second sample lands).
#[test]
fn info_instantaneous_ops_per_sec_tracks_driven_load() {
    let mut c = ctx(None);
    let reg = ironcache_observe::MetricsRegistry::new(1);
    c.metrics_registry = Some(reg.clone());
    let store = test_store(c.databases);
    let mut env = TestEnv::new(1);
    let rollup = || reg.aggregate();
    let rollup_fn: RollupFn<'_> = &rollup;
    let cmdstats = || (String::new(), String::new());
    let cmdstats_fn: CmdStatsFn<'_> = &cmdstats;
    let keyspace = || None;
    let keyspace_fn: KeyspaceFn<'_> = &keyspace;
    let info_req = req(&[b"INFO", b"stats"]);
    let body_of = |v: Value| match v {
        Value::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("expected INFO bulk, got {other:?}"),
    };
    // First read at t=0 with 0 commands: seeds the sampler, so the rate is still 0.
    let first = body_of(cmd_info(
        &c,
        &env,
        &store,
        rollup_fn,
        cmdstats_fn,
        keyspace_fn,
        MemoryInfo::default(),
        &info_req,
    ));
    assert!(
        first.contains("instantaneous_ops_per_sec:0\r\n"),
        "the seeding read reports 0: {first}"
    );
    // Drive 1000 commands into the node-wide total and advance the Env wall clock by 1s.
    let mut sc = ironcache_observe::ShardCounters::with_cell(reg.shard_cell(0));
    for _ in 0..1000 {
        sc.on_command();
    }
    env.advance(core::time::Duration::from_millis(1000));
    // Second read: 1000 commands over 1s -> a NONZERO ops/sec tracking the driven rate.
    let second = body_of(cmd_info(
        &c,
        &env,
        &store,
        rollup_fn,
        cmdstats_fn,
        keyspace_fn,
        MemoryInfo::default(),
        &info_req,
    ));
    assert!(
        second.contains("instantaneous_ops_per_sec:1000\r\n"),
        "1000 commands / 1s -> 1000 ops/sec: {second}"
    );
}
