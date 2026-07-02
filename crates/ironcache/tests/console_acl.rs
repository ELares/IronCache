// SPDX-License-Identifier: MIT OR Apache-2.0
//! Executable acceptance for the reference least-privilege console aclfile (issue #367).
//!
//! `deploy/aclfile.console.example` ships two scoped users the IronCache console authenticates AS:
//! a read-only `console_monitor` (the poll loop) and a `console_admin` (the management surface). A
//! reference aclfile is only trustworthy if its rules are PROVEN to enforce least privilege, so this
//! test loads the SHIPPED file byte-for-byte into a real server and asserts, over the wire, that each
//! user can do exactly what the console needs and NOTHING more. If someone edits the aclfile and
//! loosens (or breaks) a user, this test fails.
//!
//! The command inventory the two users must / must not have is taken from the console's actual RESP
//! calls (`ironcache-console` src/node.rs poll path + src/manage.rs management path): the monitor
//! issues PING / INFO / CLIENT LIST and touches no keys (SLOWLOG is omitted from the read-only user,
//! since it has no per-subcommand grant and the whole command includes RESET); the admin issues
//! CONFIG, CLUSTER, ACL, INFO, SAVE, key CRUD, CLIENT LIST, and SLOWLOG, but never the destructive
//! verbs (FLUSHALL / SHUTDOWN / KEYS / DEBUG / ...).

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use ironcache::test_support::run_server_with_aclfile_for_test;

// The passwords committed in the reference aclfile (placeholders operators replace). Kept in sync
// with deploy/aclfile.console.example; if you change them there, change them here.
const MONITOR_PW: &str = "CHANGE_ME_monitor";
const ADMIN_PW: &str = "CHANGE_ME_admin";

/// The path to the SHIPPED reference aclfile (validated as-is, so the test guards the real artifact).
fn reference_aclfile() -> PathBuf {
    // CARGO_MANIFEST_DIR = <workspace>/crates/ironcache; the deploy dir is two levels up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/aclfile.console.example")
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
    panic!("server never came up on port {port}");
}

fn encode_args(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// Send one command and read one socket read of the reply as a String (the replies here are small:
/// a status line, a short error, or a bounded bulk).
async fn cmd(client: &mut TcpStream, args: &[&str]) -> String {
    client.write_all(&encode_args(args)).await.unwrap();
    let mut buf = [0u8; 8192];
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

fn is_noperm(reply: &str) -> bool {
    reply.starts_with("-NOPERM")
}

#[test]
fn reference_console_aclfile_loads_and_enforces_least_privilege() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let aclfile = reference_aclfile();
        assert!(
            aclfile.exists(),
            "the reference aclfile must exist at {}",
            aclfile.display()
        );

        let port = free_port();
        // If the shipped file fails to parse (a malformed rule, an unknown subcommand token), boot
        // panics here -- so this line alone proves the reference file is syntactically valid.
        let server = run_server_with_aclfile_for_test(port, 1, aclfile.clone());

        // ---- console_monitor: read-only, no keyspace. ----
        let mut mon = connect_retry(port).await;
        assert_eq!(
            cmd(&mut mon, &["AUTH", "console_monitor", MONITOR_PW]).await,
            "+OK\r\n",
            "console_monitor must authenticate with the committed password"
        );
        // The commands the poll loop needs are ALLOWED (not NOPERM).
        assert_eq!(
            cmd(&mut mon, &["PING"]).await,
            "+PONG\r\n",
            "monitor needs PING"
        );
        assert!(
            !is_noperm(&cmd(&mut mon, &["INFO"]).await),
            "monitor needs INFO"
        );
        assert!(
            !is_noperm(&cmd(&mut mon, &["CLIENT", "LIST"]).await),
            "monitor needs CLIENT LIST (clients panel)"
        );
        // Everything mutating or key-reading is DENIED. SLOWLOG is intentionally NOT granted to the
        // read-only monitor (no per-subcommand grant exists, and the whole command includes RESET).
        for denied in [
            vec!["SET", "k", "v"],
            vec!["GET", "k"], // command denied AND no keyspace grant
            vec!["DEL", "k"],
            vec!["CONFIG", "SET", "maxmemory", "100mb"],
            vec!["SLOWLOG", "GET", "8"], // read-only monitor does not get SLOWLOG
            vec!["FLUSHALL"],
            vec!["SHUTDOWN", "NOSAVE"],
            vec!["CLIENT", "KILL", "ID", "1"],
            vec!["ACL", "SETUSER", "evil", "on", "nopass", "+@all"],
        ] {
            let reply = cmd(&mut mon, &denied).await;
            assert!(
                is_noperm(&reply),
                "console_monitor must be NOPERM on {denied:?}, got {reply:?}"
            );
        }
        drop(mon);

        // ---- console_admin: scoped management, no destructive verbs. ----
        let mut adm = connect_retry(port).await;
        assert_eq!(
            cmd(&mut adm, &["AUTH", "console_admin", ADMIN_PW]).await,
            "+OK\r\n",
            "console_admin must authenticate with the committed password"
        );
        // The management + data surface the console uses is ALLOWED.
        for allowed in [
            vec!["INFO"],
            vec!["CONFIG", "GET", "maxmemory"],
            vec!["CONFIG", "SET", "maxmemory", "128mb"],
            vec!["CLIENT", "LIST"],
            vec!["SLOWLOG", "GET", "8"], // admin gets whole SLOWLOG (GET for the panel)
            vec!["ACL", "WHOAMI"],
            vec!["SET", "console:probe", "1"], // ~* keyspace grant
            vec!["GET", "console:probe"],
            vec!["DEL", "console:probe"],
        ] {
            let reply = cmd(&mut adm, &allowed).await;
            assert!(
                !is_noperm(&reply),
                "console_admin must be ALLOWED on {allowed:?}, got {reply:?}"
            );
        }
        // The destructive tail of @dangerous stays DENIED (the least-privilege boundary).
        for denied in [
            vec!["FLUSHALL"],
            vec!["FLUSHDB"],
            vec!["SHUTDOWN", "NOSAVE"],
            vec!["KEYS", "*"],
            vec!["SWAPDB", "0", "1"],
            vec!["CLIENT", "KILL", "ID", "1"], // admin may LIST clients, not KILL them
        ] {
            let reply = cmd(&mut adm, &denied).await;
            assert!(
                is_noperm(&reply),
                "console_admin must be NOPERM on the destructive {denied:?}, got {reply:?}"
            );
        }
        drop(adm);

        // ---- the disabled default user grants no anonymous access. ----
        let mut anon = connect_retry(port).await;
        let anon_reply = cmd(&mut anon, &["GET", "k"]).await;
        assert!(
            anon_reply.starts_with("-NOAUTH") || anon_reply.starts_with("-NOPERM"),
            "an unauthenticated client must be denied (default is off), got {anon_reply:?}"
        );
        drop(anon);

        server.shutdown_and_join().unwrap();
    });
}
