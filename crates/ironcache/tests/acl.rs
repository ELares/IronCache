// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end ACL tests (#106): boot the REAL server over real sockets and drive the wire to
//! prove the production ACL model -- named users, per-command + per-key authorization, the
//! default-user backward-compatibility posture, `AUTH <user> <pass>`, the `ACL` admin family, and
//! the aclfile load/save round trip.
//!
//! These assert the value the ACL engine adds end-to-end (not just the unit level): an ACL-governed
//! user `+get ~k:*` can GET `k:1` but is `-NOPERM` on SET and on a foreign key; the default user
//! (no requirepass, no aclfile) keeps full access byte-identical; the legacy `AUTH <pass>` path
//! still works; `-@dangerous` blocks FLUSHALL/CONFIG/SHUTDOWN while GET/SET still run; a disabled
//! user cannot AUTH; and an aclfile survives a SAVE -> reboot -> the user is back.

use ironcache::test_support::{
    run_server_for_test, run_server_with_aclfile_for_test, run_server_with_auth_for_test,
};
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

/// Send one command and read ONE socket read of the reply as a String. The ACL replies here are
/// small (a status line, a short error, a few-element array), so a single read captures them.
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

/// (1) A NON-DEFAULT user `+get ~k:*` can GET `k:1`, but is `-NOPERM` on SET (command denied) and
/// `-NOPERM` on GET of a foreign key `other:1` (key denied). Proves BOTH the per-command and the
/// per-key check fire over the wire, and that a read it IS allowed still works. Single shard so the
/// keys are home-owned and the reply is clean.
#[test]
fn nondefault_user_command_and_key_enforcement() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // default (no requirepass, no aclfile) has full access: create the narrowed app user.
        assert_eq!(
            cmd(
                &mut c,
                &["ACL", "SETUSER", "app", "on", ">pw", "~k:*", "+get"]
            )
            .await,
            "+OK\r\n"
        );

        // Authenticate AS app on a fresh connection (so the cached identity is the narrowed user).
        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "app", "pw"]).await, "+OK\r\n");

        // GET k:1 is allowed (command +get AND key ~k:*) -> nil (empty keyspace).
        assert_eq!(cmd(&mut a, &["GET", "k:1"]).await, "$-1\r\n");

        // SET is NOT granted -> NOPERM command.
        let set = cmd(&mut a, &["SET", "k:1", "v"]).await;
        assert!(
            set.starts_with("-NOPERM") && set.contains("run the 'set' command"),
            "SET must be NOPERM (command denied), got {set:?}"
        );

        // GET of a key OUTSIDE ~k:* -> NOPERM key (command is allowed, the key is not).
        let other = cmd(&mut a, &["GET", "other:1"]).await;
        assert!(
            other.starts_with("-NOPERM") && other.contains("access a key"),
            "GET other:1 must be NOPERM (key denied), got {other:?}"
        );

        // The narrowed app user has no @admin, so the ACL command itself is denied (the ACL family
        // is gated at the command granularity -- a `+get`-only user cannot introspect/mutate ACLs).
        let acl_self = cmd(&mut a, &["ACL", "WHOAMI"]).await;
        assert!(
            acl_self.starts_with("-NOPERM") && acl_self.contains("run the 'acl' command"),
            "app (no @admin) must be NOPERM on ACL, got {acl_self:?}"
        );

        // The default (admin) connection sees the narrowed user via WHOAMI of its own identity.
        assert_eq!(cmd(&mut c, &["ACL", "WHOAMI"]).await, "$7\r\ndefault\r\n");

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (2) DEFAULT user with NO requirepass + NO aclfile = byte-identical legacy posture: every
/// connection is the implicit all-permissive default with full access, ACL is inactive, and a bare
/// `AUTH x` against the password-less default replies the Redis `ERR ... no password is set` (NOT a
/// silent success). WHOAMI is `default`.
#[test]
fn default_no_config_is_byte_identical_full_access() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // Full access with no AUTH at all (byte-identical to today).
        assert_eq!(cmd(&mut c, &["SET", "k", "v"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$1\r\nv\r\n");
        assert_eq!(cmd(&mut c, &["FLUSHALL"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut c, &["ACL", "WHOAMI"]).await, "$7\r\ndefault\r\n");

        // A bare AUTH against the password-less default is the Redis-faithful ERR (NOT a silent
        // success): "AUTH <password> called without any password configured for the default user".
        let auth = cmd(&mut c, &["AUTH", "anything"]).await;
        assert!(
            auth.starts_with("-ERR") && auth.contains("without any password configured"),
            "AUTH on a password-less default must be the no-password ERR, got {auth:?}"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (3) LEGACY requirepass compatibility: with a `requirepass` set, `AUTH <pass>` authenticates the
/// default user with full access (the single-password path), and a wrong password is `-WRONGPASS`.
#[test]
fn requirepass_auth_compat_full_access() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_with_auth_for_test(port, 1, "s3cr3t");
        let mut c = connect_retry(port).await;

        // Unauthenticated keyed command -> NOAUTH (the existing gate).
        let pre = cmd(&mut c, &["GET", "k"]).await;
        assert!(
            pre.starts_with("-NOAUTH"),
            "unauth GET must be NOAUTH, got {pre:?}"
        );

        // Wrong password -> WRONGPASS.
        let wrong = cmd(&mut c, &["AUTH", "nope"]).await;
        assert!(
            wrong.starts_with("-WRONGPASS"),
            "bad AUTH must be WRONGPASS, got {wrong:?}"
        );

        // AUTH <pass> -> +OK, full access (default user with the password).
        assert_eq!(cmd(&mut c, &["AUTH", "s3cr3t"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut c, &["SET", "k", "v"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut c, &["GET", "k"]).await, "$1\r\nv\r\n");
        assert_eq!(cmd(&mut c, &["FLUSHALL"]).await, "+OK\r\n");

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (4) `-@dangerous` carve-out over the wire: a user `+@all -@dangerous` can GET/SET but is NOPERM
/// on FLUSHALL / CONFIG / SHUTDOWN (the dangerous class), proving category enforcement AND that the
/// dangerous SHUTDOWN cannot exit the process from a locked-down user.
#[test]
fn dangerous_category_blocked_over_the_wire() {
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
                    "ops",
                    "on",
                    "nopass",
                    "~*",
                    "+@all",
                    "-@dangerous"
                ]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "ops", "x"]).await, "+OK\r\n");

        // Allowed: GET/SET.
        assert_eq!(cmd(&mut a, &["SET", "k", "v"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut a, &["GET", "k"]).await, "$1\r\nv\r\n");

        // Denied (dangerous): FLUSHALL, CONFIG, SHUTDOWN.
        for danger in [
            vec!["FLUSHALL"],
            vec!["CONFIG", "GET", "maxmemory"],
            vec!["SHUTDOWN", "NOSAVE"],
        ] {
            let reply = cmd(&mut a, &danger).await;
            assert!(
                reply.starts_with("-NOPERM"),
                "{danger:?} must be NOPERM (@dangerous), got {reply:?}"
            );
        }

        // The server is still up (the locked-down SHUTDOWN did NOT exit it): the control
        // connection still serves.
        assert_eq!(cmd(&mut c, &["PING"]).await, "+PONG\r\n");

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (5) A DISABLED (`off`) user cannot AUTH: the AUTH attempt is `-WRONGPASS` (never revealing the
/// user is merely disabled). Re-enabling it (`on`) lets it AUTH.
#[test]
fn disabled_user_cannot_auth() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(
                &mut c,
                &["ACL", "SETUSER", "svc", "off", ">pw", "~*", "+@all"]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        let off = cmd(&mut a, &["AUTH", "svc", "pw"]).await;
        assert!(
            off.starts_with("-WRONGPASS"),
            "disabled user AUTH must be WRONGPASS, got {off:?}"
        );

        // Enable it; now AUTH works.
        assert_eq!(
            cmd(&mut c, &["ACL", "SETUSER", "svc", "on"]).await,
            "+OK\r\n"
        );
        assert_eq!(cmd(&mut a, &["AUTH", "svc", "pw"]).await, "+OK\r\n");

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (6) `ACL SETUSER` / `GETUSER` / `DELUSER` round trip over the wire, plus `USERS` reflecting the
/// add and remove, and the `default` user being undeletable.
#[test]
fn setuser_getuser_deluser_round_trip() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(
                &mut c,
                &["ACL", "SETUSER", "app", "on", ">pw", "~k:*", "+get"]
            )
            .await,
            "+OK\r\n"
        );

        // GETUSER returns a non-empty array (the compact flags/passwords/commands/keys/channels).
        let getuser = cmd(&mut c, &["ACL", "GETUSER", "app"]).await;
        assert!(
            getuser.starts_with('*') && getuser.contains("commands"),
            "GETUSER app must be a populated map-array, got {getuser:?}"
        );
        // GETUSER of an absent user is a null array.
        assert_eq!(cmd(&mut c, &["ACL", "GETUSER", "nope"]).await, "*-1\r\n");

        // USERS lists default + app.
        let users = cmd(&mut c, &["ACL", "USERS"]).await;
        assert!(
            users.contains("app") && users.contains("default"),
            "USERS must list app + default, got {users:?}"
        );

        // DELUSER removes app.
        assert_eq!(cmd(&mut c, &["ACL", "DELUSER", "app"]).await, ":1\r\n");

        // DELUSER default is refused.
        let del_default = cmd(&mut c, &["ACL", "DELUSER", "default"]).await;
        assert!(
            del_default.starts_with("-ERR") && del_default.contains("cannot be removed"),
            "DELUSER default must error, got {del_default:?}"
        );

        // An unknown subcommand is rejected.
        let bogus = cmd(&mut c, &["ACL", "NOPE"]).await;
        assert!(
            bogus.starts_with("-ERR"),
            "unknown ACL subcommand must error, got {bogus:?}"
        );

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (7) ACLFILE round trip: boot with an aclfile holding a narrowed `app` user (and an all-
/// permissive default), prove the user was LOADED at boot (it can AUTH + is enforced), then add a
/// SECOND user live, `ACL SAVE` to the file, REBOOT the server pointed at the same file, and prove
/// BOTH users are back (the save persisted, the reload restored).
#[test]
fn aclfile_boot_load_save_reboot_round_trip() {
    let (r, local) = rt();
    local.block_on(&r, async {
        // A temp aclfile seeded with default + a narrowed app user.
        let dir = std::env::temp_dir().join(format!("ironcache-acl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let aclfile = dir.join("users.acl");
        std::fs::write(
            &aclfile,
            "user default on nopass ~* &* +@all\nuser app on >pw ~app:* +get\n",
        )
        .unwrap();

        // Boot 1: the aclfile is loaded at boot.
        let port = free_port();
        let server = run_server_with_aclfile_for_test(port, 1, aclfile.clone());
        let mut c = connect_retry(port).await;

        // app was LOADED: it can AUTH and is enforced (+get ~app:*).
        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "app", "pw"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut a, &["GET", "app:1"]).await, "$-1\r\n");
        let denied = cmd(&mut a, &["SET", "app:1", "v"]).await;
        assert!(
            denied.starts_with("-NOPERM"),
            "loaded app must be enforced, got {denied:?}"
        );
        drop(a);

        // Add a SECOND user live, then SAVE the registry to the aclfile.
        assert_eq!(
            cmd(
                &mut c,
                &["ACL", "SETUSER", "ops", "on", ">pw2", "~*", "+@all"]
            )
            .await,
            "+OK\r\n"
        );
        assert_eq!(cmd(&mut c, &["ACL", "SAVE"]).await, "+OK\r\n");

        drop(c);
        server.shutdown_and_join().unwrap();

        // The saved file must hold ops as a #digest (never the plaintext pw2).
        let saved = std::fs::read_to_string(&aclfile).unwrap();
        assert!(
            saved.contains("user ops"),
            "SAVE must persist ops, got {saved:?}"
        );
        assert!(
            !saved.contains("pw2"),
            "SAVE must NOT persist a plaintext password"
        );

        // Boot 2: a fresh server on the SAME aclfile must restore BOTH users.
        let port2 = free_port();
        let server2 = run_server_with_aclfile_for_test(port2, 1, aclfile.clone());
        let mut a2 = connect_retry(port2).await;
        assert_eq!(cmd(&mut a2, &["AUTH", "app", "pw"]).await, "+OK\r\n");
        drop(a2);
        let mut o2 = connect_retry(port2).await;
        assert_eq!(cmd(&mut o2, &["AUTH", "ops", "pw2"]).await, "+OK\r\n");
        // ops is +@all ~* -> full access.
        assert_eq!(cmd(&mut o2, &["SET", "any", "v"]).await, "+OK\r\n");

        drop(o2);
        server2.shutdown_and_join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    });
}
