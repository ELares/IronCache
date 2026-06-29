// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end tests for the drop-in compatibility commands (GETRANGE / SUBSTR / SETRANGE /
//! GETDEL / MSETNX, LMPOP / ZMPOP, SORT / SORT_RO) and the two CONFIG durability fixes
//! (`CONFIG SET appendonly no` -> +OK, `CONFIG GET save` -> empty when off).
//!
//! These boot the REAL server over a real socket and drive the wire, so they prove the whole
//! path (decode -> classify -> route -> dispatch -> encode), not just the unit level. A
//! single shard keeps every key home-owned so the reply bytes are clean and deterministic.

use ironcache::test_support::run_server_for_test;
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

/// Send one command and read ONE socket read of the reply as a String. The replies here are
/// small (a status line, a short bulk, a few-element array), so a single read captures them.
async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    (rt, local)
}

/// The string commands: GETRANGE / SUBSTR / SETRANGE / GETDEL / MSETNX over the wire.
#[test]
fn string_compat_commands_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["SET", "k", "Hello World"]).await, "+OK\r\n");
        // GETRANGE signed-range substring.
        assert_eq!(
            cmd(&mut c, &["GETRANGE", "k", "0", "4"]).await,
            "$5\r\nHello\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["GETRANGE", "k", "-5", "-1"]).await,
            "$5\r\nWorld\r\n"
        );
        // A missing key -> the EMPTY bulk (not nil).
        assert_eq!(
            cmd(&mut c, &["GETRANGE", "miss", "0", "-1"]).await,
            "$0\r\n\r\n"
        );
        // SUBSTR is the alias.
        assert_eq!(
            cmd(&mut c, &["SUBSTR", "k", "6", "-1"]).await,
            "$5\r\nWorld\r\n"
        );
        // SETRANGE overwrites + returns the new length.
        assert_eq!(
            cmd(&mut c, &["SETRANGE", "k", "6", "Redis"]).await,
            ":11\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$11\r\nHello Redis\r\n");
        // GETDEL returns then removes.
        assert_eq!(
            cmd(&mut c, &["GETDEL", "k"]).await,
            "$11\r\nHello Redis\r\n"
        );
        assert_eq!(cmd(&mut c, &["EXISTS", "k"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["GETDEL", "k"]).await, "$-1\r\n");
        // MSETNX all-or-nothing.
        assert_eq!(cmd(&mut c, &["MSETNX", "a", "1", "b", "2"]).await, ":1\r\n");
        assert_eq!(cmd(&mut c, &["MSETNX", "b", "X", "z", "9"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["EXISTS", "z"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["GET", "b"]).await, "$1\r\n2\r\n");
    });
}

/// LMPOP / ZMPOP over the wire (the first-non-empty pick + COUNT).
#[test]
fn mpop_commands_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["RPUSH", "l2", "a", "b", "c"]).await, ":3\r\n");
        // LMPOP picks l2 (l1 missing), LEFT pops 'a': [l2, [a]].
        assert_eq!(
            cmd(&mut c, &["LMPOP", "2", "l1", "l2", "LEFT"]).await,
            "*2\r\n$2\r\nl2\r\n*1\r\n$1\r\na\r\n"
        );
        // All empty -> the null array (RESP2 `*-1`).
        cmd(&mut c, &["DEL", "l2"]).await;
        assert_eq!(
            cmd(&mut c, &["LMPOP", "2", "l1", "l2", "LEFT"]).await,
            "*-1\r\n"
        );
        // ZMPOP all-empty is also the null array.
        assert_eq!(cmd(&mut c, &["ZMPOP", "1", "nope", "MIN"]).await, "*-1\r\n");

        // ZMPOP MIN pops the lowest: [z, [[a, 1]]].
        cmd(&mut c, &["ZADD", "z", "1", "a", "2", "b"]).await;
        assert_eq!(
            cmd(&mut c, &["ZMPOP", "1", "z", "MIN"]).await,
            "*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n"
        );
    });
}

/// SORT / SORT_RO over the wire (numeric, ALPHA, LIMIT, BY/GET, STORE).
#[test]
fn sort_commands_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        cmd(&mut c, &["RPUSH", "nums", "3", "1", "2", "10"]).await;
        // Numeric ascending.
        assert_eq!(
            cmd(&mut c, &["SORT", "nums"]).await,
            "*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$2\r\n10\r\n"
        );
        // ALPHA ("10" < "2").
        assert_eq!(
            cmd(&mut c, &["SORT", "nums", "ALPHA"]).await,
            "*4\r\n$1\r\n1\r\n$2\r\n10\r\n$1\r\n2\r\n$1\r\n3\r\n"
        );
        // LIMIT after sort.
        assert_eq!(
            cmd(&mut c, &["SORT", "nums", "LIMIT", "0", "2"]).await,
            "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"
        );
        // BY external weights + STORE: weight_1=30, weight_2=10, weight_3=20.
        cmd(&mut c, &["RPUSH", "ids", "1", "2", "3"]).await;
        cmd(
            &mut c,
            &["MSET", "weight_1", "30", "weight_2", "10", "weight_3", "20"],
        )
        .await;
        assert_eq!(
            cmd(&mut c, &["SORT", "ids", "BY", "weight_*"]).await,
            "*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["SORT", "ids", "BY", "weight_*", "STORE", "dest"]).await,
            ":3\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["LRANGE", "dest", "0", "-1"]).await,
            "*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n"
        );
        // SORT_RO rejects STORE.
        assert_eq!(
            cmd(&mut c, &["SORT_RO", "ids", "STORE", "x"]).await,
            "-ERR syntax error\r\n"
        );
    });
}

/// The two CONFIG durability fixes over the wire.
#[test]
fn config_durability_fixes_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // `CONFIG SET appendonly no` -> +OK (the no-op-OK).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "appendonly", "no"]).await,
            "+OK\r\n"
        );
        // `CONFIG SET appendonly yes` is still REFUSED.
        let yes = cmd(&mut c, &["CONFIG", "SET", "appendonly", "yes"]).await;
        assert!(yes.starts_with("-ERR"), "expected refusal, got {yes}");
        // `CONFIG GET save` -> empty string when the periodic save is off (the default).
        // The reply is a 2-element array [save, ""] (RESP2 CONFIG GET map).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "save"]).await,
            "*2\r\n$4\r\nsave\r\n$0\r\n\r\n"
        );
    });
}

/// `CONFIG SET timeout` / `CONFIG GET timeout` over the wire (PROD-SAFETY #4: `timeout` is now
/// runtime-settable, was boot-only). Proves the registry plumbing + the wire encoding round-trip;
/// the LIVE serve-loop effect (idle disconnection honoring the runtime change) is covered by the
/// serve-loop self-review + the runtime/registry unit tests -- a timed idle-close behavioral test
/// would need a multi-second sleep, which we deliberately avoid (flaky).
#[test]
fn config_set_get_timeout_round_trips_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // The default boot timeout is 0 (Redis default: idle disconnection off).
        // The reply is a 2-element array [timeout, "0"] (RESP2 CONFIG GET map).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n"
        );
        // `CONFIG SET timeout 30` -> +OK.
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "timeout", "30"]).await,
            "+OK\r\n"
        );
        // `CONFIG GET timeout` now reflects the runtime change (the overlay wins over boot).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$2\r\n30\r\n"
        );
        // `CONFIG SET timeout 0` -> +OK (disables idle disconnection again).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "timeout", "0"]).await,
            "+OK\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n"
        );
        // A negative / non-numeric value is REJECTED with an error (not a panic, not a silent 0).
        let neg = cmd(&mut c, &["CONFIG", "SET", "timeout", "-1"]).await;
        assert!(neg.starts_with("-ERR"), "expected error, got {neg}");
        let bad = cmd(&mut c, &["CONFIG", "SET", "timeout", "abc"]).await;
        assert!(bad.starts_with("-ERR"), "expected error, got {bad}");
        // The rejected SETs left the value at the last accepted value (0).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "timeout"]).await,
            "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n"
        );
    });
}

/// Area C: `CONFIG SET tcp-keepalive` / `CONFIG GET tcp-keepalive` over the wire. Proves the
/// registry plumbing + wire round-trip (the LIVE accept-path SO_KEEPALIVE application is covered by
/// the runtime-crate `set_keepalive` unit test + the serve-loop self-review). `0` disables it; a
/// negative / non-numeric value is rejected.
#[test]
fn config_set_get_tcp_keepalive_round_trips_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;
        // Default boot value is the Redis 300 s.
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "tcp-keepalive"]).await,
            "*2\r\n$13\r\ntcp-keepalive\r\n$3\r\n300\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "tcp-keepalive", "60"]).await,
            "+OK\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "tcp-keepalive"]).await,
            "*2\r\n$13\r\ntcp-keepalive\r\n$2\r\n60\r\n"
        );
        // `0` disables keepalive (accepted).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "tcp-keepalive", "0"]).await,
            "+OK\r\n"
        );
        let neg = cmd(&mut c, &["CONFIG", "SET", "tcp-keepalive", "-1"]).await;
        assert!(neg.starts_with("-ERR"), "expected error, got {neg}");
        let bad = cmd(&mut c, &["CONFIG", "SET", "tcp-keepalive", "x"]).await;
        assert!(bad.starts_with("-ERR"), "expected error, got {bad}");
    });
}

/// Area B: `CONFIG SET proto-max-bulk-len` / `CONFIG GET proto-max-bulk-len` over the wire. Reports
/// the live byte count; accepts a human size or a plain integer; `0` and garbage are rejected.
#[test]
fn config_set_get_proto_max_bulk_len_round_trips_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;
        // Default boot value is the Redis 512 MB (536870912 bytes).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "proto-max-bulk-len"]).await,
            "*2\r\n$18\r\nproto-max-bulk-len\r\n$9\r\n536870912\r\n"
        );
        // SET a human size, GET reflects it as bytes.
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "proto-max-bulk-len", "1mb"]).await,
            "+OK\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "proto-max-bulk-len"]).await,
            "*2\r\n$18\r\nproto-max-bulk-len\r\n$7\r\n1048576\r\n"
        );
        // `0` is rejected (a zero ceiling would reject every value); garbage is rejected.
        let zero = cmd(&mut c, &["CONFIG", "SET", "proto-max-bulk-len", "0"]).await;
        assert!(zero.starts_with("-ERR"), "expected error, got {zero}");
        let bad = cmd(&mut c, &["CONFIG", "SET", "proto-max-bulk-len", "huge"]).await;
        assert!(bad.starts_with("-ERR"), "expected error, got {bad}");
    });
}

/// Area A (the headline behavior): `CONFIG SET hash-max-listpack-entries 4` then creating a NEW
/// hash with five fields stores it as `hashtable` (verified via `OBJECT ENCODING`), while a hash
/// created BEFORE the change keeps its `listpack` encoding (a CONFIG SET is future-only -- existing
/// keys are never re-encoded). This is the end-to-end proof that the threshold is now LIVE (was an
/// accepted-but-ignored no-op).
#[test]
fn config_set_hash_listpack_entries_changes_new_hash_encoding_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // An EXISTING small hash under the default 512-entry cap: listpack.
        for i in 0..5 {
            cmd(&mut c, &["HSET", "existing", &format!("f{i}"), "v"]).await;
        }
        assert_eq!(
            cmd(&mut c, &["OBJECT", "ENCODING", "existing"]).await,
            "$8\r\nlistpack\r\n"
        );

        // Lower the live cap to 4 entries.
        assert_eq!(
            cmd(&mut c, &["CONFIG", "SET", "hash-max-listpack-entries", "4"]).await,
            "+OK\r\n"
        );
        // GET reflects the live value (was a lie before: it echoed the compiled 512).
        assert_eq!(
            cmd(&mut c, &["CONFIG", "GET", "hash-max-listpack-entries"]).await,
            "*2\r\n$25\r\nhash-max-listpack-entries\r\n$1\r\n4\r\n"
        );
        // The EXISTING hash is NOT re-encoded by the CONFIG SET (future-only).
        assert_eq!(
            cmd(&mut c, &["OBJECT", "ENCODING", "existing"]).await,
            "$8\r\nlistpack\r\n"
        );
        // A NEW hash with 5 fields under the lowered cap -> hashtable (5 > 4).
        for i in 0..5 {
            cmd(&mut c, &["HSET", "fresh", &format!("f{i}"), "v"]).await;
        }
        assert_eq!(
            cmd(&mut c, &["OBJECT", "ENCODING", "fresh"]).await,
            "$9\r\nhashtable\r\n"
        );
    });
}

/// SET IFEQ / IFNE compare-and-set over the wire (Redis 8.4, #412): the happy match, the
/// mismatch (no write, nil reply), the missing-key behavior (IFEQ does NOT create, IFNE
/// DOES), GET composition (returns the OLD value regardless), the EX composition, the
/// mutual-exclusivity / missing-value syntax errors, and WRONGTYPE on a non-string.
#[test]
fn set_ifeq_ifne_compare_and_set_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["SET", "k", "v1"]).await, "+OK\r\n");
        // IFEQ match -> writes; mismatch -> nil, no write.
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v2", "IFEQ", "v1"]).await,
            "+OK\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$2\r\nv2\r\n");
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v3", "IFEQ", "v1"]).await,
            "$-1\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$2\r\nv2\r\n");
        // IFEQ on a MISSING key fails and does NOT create it (Redis 8.4 wording).
        assert_eq!(
            cmd(&mut c, &["SET", "miss", "x", "IFEQ", "v1"]).await,
            "$-1\r\n"
        );
        assert_eq!(cmd(&mut c, &["EXISTS", "miss"]).await, ":0\r\n");
        // GET composition: returns the OLD value whether or not the write fires.
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v4", "IFEQ", "v2", "GET"]).await,
            "$2\r\nv2\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$2\r\nv4\r\n");
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v5", "IFEQ", "wrong", "GET"]).await,
            "$2\r\nv4\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$2\r\nv4\r\n");
        // IFNE: write when the current value DIFFERS from the comparison (here k is v4, so
        // comparing against "zz" fires); nil when it is EQUAL; a missing key IS created.
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v6", "IFNE", "zz"]).await,
            "+OK\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$2\r\nv6\r\n");
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v7", "IFNE", "v6"]).await,
            "$-1\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$2\r\nv6\r\n");
        assert_eq!(
            cmd(&mut c, &["SET", "fresh", "vY", "IFNE", "z"]).await,
            "+OK\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "fresh"]).await, "$2\r\nvY\r\n");
        // EX composes with IFEQ: a matching CAS that also sets a TTL.
        assert_eq!(
            cmd(&mut c, &["SET", "k", "v8", "IFEQ", "v6", "EX", "100"]).await,
            "+OK\r\n"
        );
        assert_eq!(cmd(&mut c, &["TTL", "k"]).await, ":100\r\n");
        // The condition options are mutually exclusive; a missing IFEQ value is a syntax error.
        for bad in [
            ["SET", "k", "v", "NX", "IFEQ"].as_slice(),
            ["SET", "k", "v", "IFEQ", "x", "XX"].as_slice(),
            ["SET", "k", "v", "IFEQ", "a", "IFNE"].as_slice(),
            ["SET", "k", "v", "IFEQ"].as_slice(),
        ] {
            let reply = cmd(&mut c, bad).await;
            assert!(
                reply.starts_with("-ERR syntax error"),
                "{bad:?} must be a syntax error, got {reply:?}"
            );
        }
        // WRONGTYPE: IFEQ cannot compare a non-string value.
        assert_eq!(cmd(&mut c, &["RPUSH", "lst", "a"]).await, ":1\r\n");
        let wt = cmd(&mut c, &["SET", "lst", "v", "IFEQ", "x"]).await;
        assert!(wt.starts_with("-WRONGTYPE"), "got {wt:?}");
    });
}

/// DELIFEQ compare-and-delete over the wire (Valkey 9.0, #412): delete only on an exact
/// string match (reply 1), no delete on a mismatch or a missing key (reply 0), WRONGTYPE on
/// a non-string, and the arity error.
#[test]
fn delifeq_compare_and_delete_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["SET", "lock", "token1"]).await, "+OK\r\n");
        // Mismatch -> 0, the key survives.
        assert_eq!(cmd(&mut c, &["DELIFEQ", "lock", "wrong"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["EXISTS", "lock"]).await, ":1\r\n");
        // Exact match -> 1, the key is gone.
        assert_eq!(cmd(&mut c, &["DELIFEQ", "lock", "token1"]).await, ":1\r\n");
        assert_eq!(cmd(&mut c, &["EXISTS", "lock"]).await, ":0\r\n");
        // A now-missing key, and an always-missing key -> 0.
        assert_eq!(cmd(&mut c, &["DELIFEQ", "lock", "token1"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["DELIFEQ", "miss", "x"]).await, ":0\r\n");
        // WRONGTYPE on a non-string; the value is untouched.
        assert_eq!(cmd(&mut c, &["RPUSH", "lst", "a"]).await, ":1\r\n");
        let wt = cmd(&mut c, &["DELIFEQ", "lst", "x"]).await;
        assert!(wt.starts_with("-WRONGTYPE"), "got {wt:?}");
        assert_eq!(cmd(&mut c, &["EXISTS", "lst"]).await, ":1\r\n");
        // Arity: DELIFEQ needs exactly key + value.
        let ar = cmd(&mut c, &["DELIFEQ", "lock"]).await;
        assert!(
            ar.starts_with("-ERR") && ar.contains("delifeq"),
            "got {ar:?}"
        );
    });
}

/// MSETEX atomic multi-key set with a shared expiration over the wire (Redis 8.4, #412): the
/// unconditional set-all (reply 1), the NX gate (all-or-nothing on a single existing key),
/// the XX gate (fails if any key is missing), the shared EX, KEEPTTL preservation, and the
/// numkeys / syntax / expire errors.
#[test]
fn msetex_atomic_multikey_over_the_wire() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // Unconditional: set every key, reply 1, no TTL (MSET default clears).
        assert_eq!(
            cmd(&mut c, &["MSETEX", "2", "a", "1", "b", "2"]).await,
            ":1\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "a"]).await, "$1\r\n1\r\n");
        assert_eq!(cmd(&mut c, &["GET", "b"]).await, "$1\r\n2\r\n");
        assert_eq!(cmd(&mut c, &["TTL", "a"]).await, ":-1\r\n");
        // Shared EX applies the SAME deadline to every key.
        assert_eq!(
            cmd(&mut c, &["MSETEX", "2", "c", "3", "d", "4", "EX", "100"]).await,
            ":1\r\n"
        );
        assert_eq!(cmd(&mut c, &["TTL", "c"]).await, ":100\r\n");
        assert_eq!(cmd(&mut c, &["TTL", "d"]).await, ":100\r\n");
        // NX gate: `a` already exists, so the whole op is rejected (reply 0) and `e` is NOT
        // created (all-or-nothing).
        assert_eq!(
            cmd(&mut c, &["MSETEX", "2", "a", "X", "e", "5", "NX"]).await,
            ":0\r\n"
        );
        assert_eq!(cmd(&mut c, &["EXISTS", "e"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["GET", "a"]).await, "$1\r\n1\r\n");
        // NX gate passes when none of the keys exist.
        assert_eq!(
            cmd(&mut c, &["MSETEX", "2", "f", "6", "g", "7", "NX"]).await,
            ":1\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "f"]).await, "$1\r\n6\r\n");
        // XX gate passes when ALL keys exist (a, b were set above).
        assert_eq!(
            cmd(&mut c, &["MSETEX", "2", "a", "10", "b", "20", "XX"]).await,
            ":1\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "a"]).await, "$2\r\n10\r\n");
        // XX gate fails if any key is missing -> 0, nothing written.
        assert_eq!(
            cmd(&mut c, &["MSETEX", "2", "a", "99", "zzz", "0", "XX"]).await,
            ":0\r\n"
        );
        assert_eq!(cmd(&mut c, &["EXISTS", "zzz"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["GET", "a"]).await, "$2\r\n10\r\n");
        // KEEPTTL preserves each key's existing TTL across the overwrite (c still ~100s).
        assert_eq!(
            cmd(&mut c, &["MSETEX", "1", "c", "30", "KEEPTTL"]).await,
            ":1\r\n"
        );
        assert_eq!(cmd(&mut c, &["GET", "c"]).await, "$2\r\n30\r\n");
        assert_eq!(cmd(&mut c, &["TTL", "c"]).await, ":100\r\n");
        // Errors: numkeys must be positive; too few pairs for numkeys; NX+XX; bad/zero expire.
        let zero = cmd(&mut c, &["MSETEX", "0", "a", "1"]).await;
        assert!(zero.starts_with("-ERR"), "numkeys 0 -> error, got {zero:?}");
        let short = cmd(&mut c, &["MSETEX", "2", "a", "1"]).await;
        assert!(
            short.starts_with("-ERR"),
            "too few pairs -> error, got {short:?}"
        );
        let both = cmd(&mut c, &["MSETEX", "1", "a", "1", "NX", "XX"]).await;
        assert!(
            both.starts_with("-ERR syntax error"),
            "NX+XX -> syntax, got {both:?}"
        );
        let badex = cmd(&mut c, &["MSETEX", "1", "a", "1", "EX", "notanum"]).await;
        assert!(badex.starts_with("-ERR"), "bad EX -> error, got {badex:?}");
        let zeroex = cmd(&mut c, &["MSETEX", "1", "a", "1", "EX", "0"]).await;
        assert!(
            zeroex.starts_with("-ERR") && zeroex.contains("expire"),
            "EX 0 -> invalid expire, got {zeroex:?}"
        );
    });
}
