// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end tests for the DEBUG conformance subcommand subset (#411): OBJECT / JMAP / SLEEP
//! / SET-ACTIVE-EXPIRE / STRINGMATCH-LEN / QUICKLIST-PACKED-THRESHOLD. These boot the REAL
//! server over a real socket and drive the wire, so they prove the whole path the upstream
//! Redis/Valkey TCL suites drive (assert_encoding -> DEBUG OBJECT, `debug stringmatch-len`,
//! `debug set-active-expire`, ...).
//!
//! The drain-GATING behavior of `DEBUG SET-ACTIVE-EXPIRE 0` (the reaper goes inert) is proven
//! directly by the `expire_cycle_tick_is_inert_when_active_expire_disabled` unit test in
//! `serve.rs`; here we prove the command SURFACE (replies + the @dangerous ACL gate).

use ironcache::test_support::run_server_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

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

fn encode_args(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// The encoding name from an `OBJECT ENCODING` bulk reply (`$N\r\n<enc>\r\n`).
fn bulk_value(reply: &str) -> String {
    reply.split("\r\n").nth(1).unwrap_or_default().to_string()
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    (rt, local)
}

/// DEBUG OBJECT reports the SAME internal encoding that OBJECT ENCODING does (aligned with the
/// #40 mapping), for an int / a short string / a small list; a missing key is `-ERR no such
/// key`. Cross-checking against OBJECT ENCODING (rather than hard-coding names) proves the two
/// cannot drift.
#[test]
fn debug_object_encoding_matches_object_encoding() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        cmd(&mut c, &["SET", "i", "12345"]).await;
        cmd(&mut c, &["SET", "s", "hello world"]).await;
        cmd(&mut c, &["RPUSH", "l", "a", "b", "c"]).await;

        for key in ["i", "s", "l"] {
            let enc = bulk_value(&cmd(&mut c, &["OBJECT", "ENCODING", key]).await);
            assert!(!enc.is_empty(), "OBJECT ENCODING {key} gave no encoding");
            let dbg = cmd(&mut c, &["DEBUG", "OBJECT", key]).await;
            assert!(
                dbg.starts_with('+'),
                "DEBUG OBJECT {key} should be a status, got {dbg:?}"
            );
            assert!(
                dbg.contains(&format!("encoding:{enc}")),
                "DEBUG OBJECT {key} must report encoding:{enc}, got {dbg:?}"
            );
        }

        // A missing key -> -ERR no such key (Redis wording).
        let miss = cmd(&mut c, &["DEBUG", "OBJECT", "nope"]).await;
        assert!(
            miss.starts_with("-ERR") && miss.contains("no such key"),
            "DEBUG OBJECT on a missing key must be no-such-key, got {miss:?}"
        );
    });
}

/// JMAP and QUICKLIST-PACKED-THRESHOLD are accepted no-ops (return +OK), so a suite that calls
/// them before building data runs unmodified.
#[test]
fn debug_jmap_and_quicklist_threshold_are_noop_ok() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["DEBUG", "JMAP"]).await, "+OK\r\n");
        assert_eq!(
            cmd(&mut c, &["DEBUG", "QUICKLIST-PACKED-THRESHOLD", "100"]).await,
            "+OK\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["DEBUG", "QUICKLIST-PACKED-THRESHOLD", "1K"]).await,
            "+OK\r\n"
        );
    });
}

/// DEBUG STRINGMATCH-LEN runs the glob matcher and replies 1 (match) / 0 (no match), reusing
/// the same matcher KEYS/SCAN use.
#[test]
fn debug_stringmatch_len_runs_the_glob_matcher() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(&mut c, &["DEBUG", "STRINGMATCH-LEN", "h?llo", "hello"]).await,
            ":1\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["DEBUG", "STRINGMATCH-LEN", "h*o", "hello"]).await,
            ":1\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["DEBUG", "STRINGMATCH-LEN", "h[a-c]llo", "hello"]).await,
            ":0\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["DEBUG", "STRINGMATCH-LEN", "abc", "xyz"]).await,
            ":0\r\n"
        );
    });
}

/// DEBUG SLEEP returns +OK after blocking; DEBUG SET-ACTIVE-EXPIRE round-trips 0/1; bad args
/// error. (The reaper-inert behavior of SET-ACTIVE-EXPIRE 0 is proven by the serve.rs unit
/// test; here we cover the command surface.)
#[test]
fn debug_sleep_and_set_active_expire_surface() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // SLEEP 0 returns immediately; a small positive sleep also returns OK.
        assert_eq!(cmd(&mut c, &["DEBUG", "SLEEP", "0"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut c, &["DEBUG", "SLEEP", "0.05"]).await, "+OK\r\n");
        let bad_sleep = cmd(&mut c, &["DEBUG", "SLEEP", "notanum"]).await;
        assert!(bad_sleep.starts_with("-ERR"), "got {bad_sleep:?}");

        // SET-ACTIVE-EXPIRE round-trips both states.
        assert_eq!(
            cmd(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).await,
            "+OK\r\n"
        );
        assert_eq!(
            cmd(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).await,
            "+OK\r\n"
        );
        let bad = cmd(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "nope"]).await;
        assert!(bad.starts_with("-ERR"), "got {bad:?}");
    });
}

/// DEBUG HELP returns an array; an unimplemented subcommand fails LOUDLY (unknown subcommand),
/// never a silent OK, so a suite relying on a DEBUG behavior we do not model does not pass
/// misleadingly.
#[test]
fn debug_help_and_unknown_subcommand() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        let help = cmd(&mut c, &["DEBUG", "HELP"]).await;
        assert!(
            help.starts_with('*'),
            "DEBUG HELP must be an array, got {help:?}"
        );
        let unknown = cmd(&mut c, &["DEBUG", "RELOAD"]).await;
        assert!(
            unknown.starts_with("-ERR") && unknown.to_uppercase().contains("SUBCOMMAND"),
            "an unimplemented DEBUG subcommand must fail loudly, got {unknown:?}"
        );
    });
}

/// DEBUG is @admin + @dangerous: a `+@all -@dangerous` user is NOPERM on it (so the conformance
/// surface cannot be reached by an unprivileged client).
#[test]
fn debug_is_gated_under_dangerous() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL",
                    "SETUSER",
                    "app",
                    "on",
                    ">pw",
                    "~*",
                    "+@all",
                    "-@dangerous"
                ]
            )
            .await,
            "+OK\r\n"
        );
        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "app", "pw"]).await, "+OK\r\n");
        // GET still works (not dangerous); DEBUG is denied (dangerous).
        let dbg = cmd(&mut a, &["DEBUG", "JMAP"]).await;
        assert!(
            dbg.starts_with("-NOPERM") && dbg.contains("debug"),
            "DEBUG must be NOPERM for a -@dangerous user, got {dbg:?}"
        );

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}
