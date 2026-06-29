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

/// (12) SORT/SORT_RO BY/GET external-key dereference gate (redis#10106). A `BY pat` with `*` or a
/// non-`#` `GET pat` reads keys that are NOT in the command key-spec, so the per-key ACL check
/// cannot see them. Redis 7.0 denies such a SORT unless the user has FULL key-read (allkeys). A
/// user scoped to `~ids*` (NOT allkeys): plain `SORT ids` / `SORT ids ALPHA` / `SORT ids BY nosort`
/// (no `*`) / `SORT ids GET #` are ALLOWED (no external deref); `SORT ids BY weight_*` and
/// `SORT ids GET data_*` are DENIED `-NOPERM ... access a key`. An allkeys (`~*`) user: all
/// ALLOWED. Covers both SORT and SORT_RO.
#[test]
fn sort_by_get_external_key_deref_gated_on_allkeys() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // Seed a source list the scoped user is allowed to read (`ids` matches `~ids*`).
        assert_eq!(cmd(&mut c, &["RPUSH", "ids", "1", "2"]).await, ":2\r\n");

        // A user scoped to `~ids*` (NOT allkeys) with +sort +sort_ro.
        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL", "SETUSER", "sorter", "on", ">pw", "~ids*", "+sort", "+sort_ro"
                ]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "sorter", "pw"]).await, "+OK\r\n");

        // ALLOWED (no external-key deref): plain SORT, ALPHA, `BY nosort` (no `*`), `GET #`.
        for ok in [
            vec!["SORT", "ids"],
            vec!["SORT", "ids", "ALPHA"],
            vec!["SORT", "ids", "BY", "nosort"],
            vec!["SORT", "ids", "GET", "#"],
            vec!["SORT_RO", "ids"],
            vec!["SORT_RO", "ids", "BY", "nosort"],
            vec!["SORT_RO", "ids", "GET", "#"],
        ] {
            let reply = cmd(&mut a, &ok).await;
            assert!(
                reply.starts_with('*'),
                "{ok:?} must be ALLOWED (no external deref), got {reply:?}"
            );
        }

        // DENIED (BY/GET dereferences an external key the user is not allkeys for).
        for deny in [
            vec!["SORT", "ids", "BY", "weight_*"],
            vec!["SORT", "ids", "GET", "data_*"],
            vec!["SORT", "ids", "BY", "weight_*", "GET", "data_*->f"],
            vec!["SORT_RO", "ids", "BY", "weight_*"],
            vec!["SORT_RO", "ids", "GET", "data_*"],
        ] {
            let reply = cmd(&mut a, &deny).await;
            assert!(
                reply.starts_with("-NOPERM") && reply.contains("access a key"),
                "{deny:?} must be NOPERM (external-key deref under non-allkeys), got {reply:?}"
            );
        }

        // An ALLKEYS user (`~*`) may run the dereferencing forms (full key-read permission).
        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL", "SETUSER", "all", "on", ">pw", "~*", "+sort", "+sort_ro"
                ]
            )
            .await,
            "+OK\r\n"
        );
        let mut k = connect_retry(port).await;
        assert_eq!(cmd(&mut k, &["AUTH", "all", "pw"]).await, "+OK\r\n");
        for ok in [
            vec!["SORT", "ids", "BY", "weight_*"],
            vec!["SORT", "ids", "GET", "data_*"],
            vec!["SORT_RO", "ids", "GET", "data_*"],
        ] {
            let reply = cmd(&mut k, &ok).await;
            assert!(
                reply.starts_with('*'),
                "allkeys user must run {ok:?}, got {reply:?}"
            );
        }

        drop(k);
        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (13) ACL-OFF (no requirepass, no aclfile): the implicit default is allkeys, so SORT BY/GET runs
/// unchanged -- the deref gate is byte-identical when ACL is inactive.
#[test]
fn sort_by_get_deref_acl_off_unaffected() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(cmd(&mut c, &["RPUSH", "ids", "1", "2"]).await, ":2\r\n");

        // No ACL configured -> the default connection is allkeys: the dereferencing forms run.
        for ok in [
            vec!["SORT", "ids", "BY", "weight_*"],
            vec!["SORT", "ids", "GET", "data_*"],
            vec!["SORT_RO", "ids", "GET", "data_*"],
        ] {
            let reply = cmd(&mut c, &ok).await;
            assert!(
                reply.starts_with('*'),
                "ACL-off (allkeys default) must run {ok:?}, got {reply:?}"
            );
        }

        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (14) SORT_RO is `@dangerous` (parity with Redis, which marks both SORT and SORT_RO dangerous):
/// a `+@all -@dangerous` user can GET/SET but is NOPERM on BOTH SORT and SORT_RO.
#[test]
fn sort_ro_is_dangerous() {
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

        // Both SORT and SORT_RO are @dangerous -> NOPERM (command denied).
        for danger in [vec!["SORT", "k"], vec!["SORT_RO", "k"]] {
            let reply = cmd(&mut a, &danger).await;
            assert!(
                reply.starts_with("-NOPERM") && reply.contains("command"),
                "{danger:?} must be NOPERM (@dangerous), got {reply:?}"
            );
        }

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

/// (8) F1 LIVE REVOCATION via `ACL SETUSER`: a connection authed as `app` (`+@all`) runs SET fine,
/// then an EXTERNAL `ACL SETUSER app -set` REVOKES that command. The app connection's VERY NEXT SET
/// must be `-NOPERM` immediately (the live revocation), where before the fix it stayed allowed until
/// the connection re-AUTHed or disconnected (fail-open).
#[test]
fn mid_session_setuser_revokes_live() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(
                &mut c,
                &["ACL", "SETUSER", "app", "on", ">pw", "~*", "+@all"]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "app", "pw"]).await, "+OK\r\n");
        // SET works under +@all.
        assert_eq!(cmd(&mut a, &["SET", "k", "v"]).await, "+OK\r\n");

        // External revocation of SET on the control connection.
        assert_eq!(
            cmd(&mut c, &["ACL", "SETUSER", "app", "-set"]).await,
            "+OK\r\n"
        );

        // The app connection's NEXT SET is now NOPERM (revocation took effect live, no reconnect).
        let denied = cmd(&mut a, &["SET", "k", "v2"]).await;
        assert!(
            denied.starts_with("-NOPERM") && denied.contains("run the 'set' command"),
            "mid-session SETUSER -set must revoke live, got {denied:?}"
        );
        // A still-granted command (GET) keeps working: only SET was revoked.
        assert_eq!(cmd(&mut a, &["GET", "k"]).await, "$1\r\nv\r\n");

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (9) F1 LIVE REVOCATION via `ACL DELUSER`: a connection authed as `app`, then an external `ACL
/// DELUSER app` deletes the identity. The app connection's next command is rejected (the server
/// DEAUTHENTICATES + CLOSES the connection, mirroring Redis killing a deleted user's clients).
#[test]
fn mid_session_deluser_rejects_live() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(
                &mut c,
                &["ACL", "SETUSER", "app", "on", ">pw", "~*", "+@all"]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "app", "pw"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut a, &["GET", "k"]).await, "$-1\r\n");

        // External DELUSER on the control connection.
        assert_eq!(cmd(&mut c, &["ACL", "DELUSER", "app"]).await, ":1\r\n");

        // The app connection's next command is rejected: the server replies NOAUTH and closes, so
        // the read returns the NOAUTH error then an EOF (an empty read) on the following attempt.
        let rejected = cmd(&mut a, &["GET", "k"]).await;
        assert!(
            rejected.starts_with("-NOAUTH"),
            "mid-session DELUSER must reject the connection's next command, got {rejected:?}"
        );
        // The connection was CLOSED: a follow-up read yields EOF (0 bytes).
        let mut buf = [0u8; 64];
        a.write_all(&encode_args(&["PING"])).await.ok();
        let n = a.read(&mut buf).await.unwrap_or(0);
        assert_eq!(
            n, 0,
            "DELUSER'd connection must be closed (EOF), read {n} bytes"
        );

        // The server itself is still up (the control connection still serves).
        assert_eq!(cmd(&mut c, &["PING"]).await, "+PONG\r\n");

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (10) F1 DEFAULT-USER restricted mid-session: a no-requirepass connection is the all-permissive
/// implicit default (full access). An external `ACL SETUSER default -@dangerous` must take effect on
/// it LIVE -- its next FLUSHALL is denied -- even though it never AUTHed (it is still the `default`
/// identity, so the re-resolve picks up the new restriction). A still-permitted GET/SET keeps working.
#[test]
fn mid_session_default_restricted_live() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // The connection has full access as the implicit default (never AUTHed).
        assert_eq!(cmd(&mut c, &["SET", "k", "v"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut c, &["FLUSHALL"]).await, "+OK\r\n");

        // A SECOND connection narrows the default user (this could be the same admin in practice).
        let mut admin = connect_retry(port).await;
        assert_eq!(
            cmd(&mut admin, &["ACL", "SETUSER", "default", "-@dangerous"]).await,
            "+OK\r\n"
        );

        // The FIRST connection's next FLUSHALL is now denied live (it picked up the restriction by
        // re-resolving the `default` identity), while GET/SET still work.
        assert_eq!(cmd(&mut c, &["SET", "k", "v"]).await, "+OK\r\n");
        let denied = cmd(&mut c, &["FLUSHALL"]).await;
        assert!(
            denied.starts_with("-NOPERM"),
            "mid-session SETUSER default -@dangerous must restrict the live default, got {denied:?}"
        );

        drop(c);
        drop(admin);
        server.shutdown_and_join().unwrap();
    });
}

/// (11) F1 EXEC-TIME re-check: a permission revoked BETWEEN `MULTI` and `EXEC` (external `ACL
/// SETUSER`) is enforced at EXEC replay -- the now-forbidden command in the queued batch comes back
/// NOPERM in the EXEC array, while a still-permitted command in the same batch runs. Closes the
/// MULTI/EXEC revocation race.
#[test]
fn mid_session_setuser_rechecked_at_exec() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(
                &mut c,
                &["ACL", "SETUSER", "app", "on", ">pw", "~*", "+@all"]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "app", "pw"]).await, "+OK\r\n");

        // Open a transaction and queue GET (read) + SET (write), both currently allowed.
        assert_eq!(cmd(&mut a, &["MULTI"]).await, "+OK\r\n");
        assert_eq!(cmd(&mut a, &["GET", "k"]).await, "+QUEUED\r\n");
        assert_eq!(cmd(&mut a, &["SET", "k", "v"]).await, "+QUEUED\r\n");

        // BETWEEN MULTI and EXEC, revoke SET externally.
        assert_eq!(
            cmd(&mut c, &["ACL", "SETUSER", "app", "-set"]).await,
            "+OK\r\n"
        );

        // EXEC re-checks ACL per queued command: GET still returns (nil), SET is NOPERM.
        let exec = cmd(&mut a, &["EXEC"]).await;
        assert!(
            exec.starts_with("*2\r\n"),
            "EXEC must return a 2-element array, got {exec:?}"
        );
        assert!(
            exec.contains("$-1\r\n"),
            "the GET element must still return nil, got {exec:?}"
        );
        assert!(
            exec.contains("-NOPERM") && exec.contains("run the 'set' command"),
            "the SET element must be NOPERM (revoked between MULTI and EXEC), got {exec:?}"
        );

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (14) PER-SUBCOMMAND ACL for CLUSTER (Redis 7 `+cluster|slots`): a user granted the read-only
/// cluster-introspection subcommands but `-@dangerous` can run CLUSTER SLOTS / SHARDS / NODES (the
/// ACL layer allows them -- NOT `-NOPERM`) yet is `-NOPERM` on every CLUSTER mutator (ADDSLOTS /
/// MEET / SETSLOT), and the NOPERM names the `cluster|<sub>` pair. The ACL check fires BEFORE the
/// handler, so the allowed reads need not succeed at the handler (this single node has cluster
/// support disabled); the assertion is purely "the ACL did/didn't deny".
#[test]
fn cluster_subcommand_acl_allows_reads_denies_mutators() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // svc: the exact rule string from the feature spec.
        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL",
                    "SETUSER",
                    "svc",
                    "on",
                    "nopass",
                    "~*",
                    "resetchannels",
                    "+@read",
                    "+@write",
                    "+@connection",
                    "+@transaction",
                    "-@dangerous",
                    "+cluster|slots",
                    "+cluster|shards",
                    "+cluster|nodes",
                ]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "svc", "x"]).await, "+OK\r\n");

        // The granted read subcommands are NOT denied by ACL (reply is whatever the handler gives,
        // never `-NOPERM`). Case-insensitive: `cluster slots`, `CLUSTER Slots` enforce identically.
        for read in [
            vec!["CLUSTER", "SLOTS"],
            vec!["CLUSTER", "SHARDS"],
            vec!["CLUSTER", "NODES"],
            vec!["cluster", "slots"],
            vec!["CLUSTER", "Slots"],
        ] {
            let reply = cmd(&mut a, &read).await;
            assert!(
                !reply.starts_with("-NOPERM"),
                "{read:?} must be allowed by ACL (not NOPERM), got {reply:?}"
            );
        }

        // Every CLUSTER mutator is NOPERM, and the message names `cluster|<sub>`.
        for (mutator, token) in [
            (vec!["CLUSTER", "ADDSLOTS", "0"], "cluster|addslots"),
            (vec!["CLUSTER", "MEET", "127.0.0.1", "7000"], "cluster|meet"),
            (vec!["CLUSTER", "SETSLOT", "0", "STABLE"], "cluster|setslot"),
        ] {
            let reply = cmd(&mut a, &mutator).await;
            assert!(
                reply.starts_with("-NOPERM") && reply.contains(token),
                "{mutator:?} must be NOPERM naming {token:?}, got {reply:?}"
            );
        }

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (15) `+@all -@dangerous` CLUSTER parity (Redis 7): the carve-out leaves the read subcommand
/// (CLUSTER SLOTS, `@slow` only) RUNNABLE but denies the mutator (CLUSTER ADDSLOTS, `@admin` +
/// `@dangerous`). A bare `+cluster` user, by contrast, can run BOTH (a whole-command grant covers
/// every subcommand, Redis parity).
#[test]
fn cluster_all_minus_dangerous_and_bare_grant() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // ops: +@all -@dangerous.
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
        // CLUSTER SLOTS allowed (not NOPERM); CLUSTER ADDSLOTS denied.
        assert!(
            !cmd(&mut a, &["CLUSTER", "SLOTS"])
                .await
                .starts_with("-NOPERM")
        );
        let denied = cmd(&mut a, &["CLUSTER", "ADDSLOTS", "0"]).await;
        assert!(
            denied.starts_with("-NOPERM") && denied.contains("cluster|addslots"),
            "+@all -@dangerous must be NOPERM on CLUSTER ADDSLOTS, got {denied:?}"
        );

        // svc: a bare `+cluster` whole-command grant -> reads AND mutators both pass ACL.
        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL", "SETUSER", "svc", "on", "nopass", "~*", "-@all", "+cluster"
                ]
            )
            .await,
            "+OK\r\n"
        );
        let mut b = connect_retry(port).await;
        assert_eq!(cmd(&mut b, &["AUTH", "svc", "x"]).await, "+OK\r\n");
        assert!(
            !cmd(&mut b, &["CLUSTER", "SLOTS"])
                .await
                .starts_with("-NOPERM")
        );
        assert!(
            !cmd(&mut b, &["CLUSTER", "ADDSLOTS", "0"])
                .await
                .starts_with("-NOPERM"),
            "a bare +cluster grant must allow the mutator too (Redis parity)"
        );

        drop(a);
        drop(b);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (16) ACLFILE round trip with a `+cluster|slots` rule: boot with an aclfile that grants the
/// subcommand, prove it is enforced (CLUSTER SLOTS allowed, CLUSTER ADDSLOTS NOPERM), `ACL SAVE`,
/// REBOOT on the same file, and prove the rule survived (the `+cluster|slots` line persisted and
/// re-loaded with identical enforcement).
#[test]
fn cluster_subcommand_aclfile_round_trip() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let dir = std::env::temp_dir().join(format!("ironcache-aclsub-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let aclfile = dir.join("users.acl");
        std::fs::write(
            &aclfile,
            "user default on nopass ~* &* +@all\n\
             user svc on nopass ~* resetchannels -@all +@connection +cluster|slots\n",
        )
        .unwrap();

        let port = free_port();
        let server = run_server_with_aclfile_for_test(port, 1, aclfile.clone());
        let mut c = connect_retry(port).await;
        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "svc", "x"]).await, "+OK\r\n");
        assert!(
            !cmd(&mut a, &["CLUSTER", "SLOTS"])
                .await
                .starts_with("-NOPERM")
        );
        let denied = cmd(&mut a, &["CLUSTER", "ADDSLOTS", "0"]).await;
        assert!(
            denied.starts_with("-NOPERM") && denied.contains("cluster|addslots"),
            "loaded svc must be NOPERM on CLUSTER ADDSLOTS, got {denied:?}"
        );
        drop(a);

        // SAVE, reboot on the same file.
        assert_eq!(cmd(&mut c, &["ACL", "SAVE"]).await, "+OK\r\n");
        drop(c);
        server.shutdown_and_join().unwrap();

        let saved = std::fs::read_to_string(&aclfile).unwrap();
        assert!(
            saved.contains("+cluster|slots"),
            "SAVE must persist the subcommand rule, got {saved:?}"
        );

        let port2 = free_port();
        let server2 = run_server_with_aclfile_for_test(port2, 1, aclfile.clone());
        let mut a2 = connect_retry(port2).await;
        assert_eq!(cmd(&mut a2, &["AUTH", "svc", "x"]).await, "+OK\r\n");
        assert!(
            !cmd(&mut a2, &["CLUSTER", "SLOTS"])
                .await
                .starts_with("-NOPERM")
        );
        let denied2 = cmd(&mut a2, &["CLUSTER", "ADDSLOTS", "0"]).await;
        assert!(
            denied2.starts_with("-NOPERM") && denied2.contains("cluster|addslots"),
            "reloaded svc must still be NOPERM on CLUSTER ADDSLOTS, got {denied2:?}"
        );

        drop(a2);
        server2.shutdown_and_join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// (17) PER-SUBCOMMAND ACL for CONFIG + CLIENT (read-only monitoring user). A user
/// `-@dangerous +config|get +client|info +client|list` (dangerous denied FIRST, the read halves
/// re-granted -- last-match-wins) can run CONFIG GET, CLIENT INFO, and CLIENT LIST, but is `-NOPERM`
/// on CONFIG SET and on CLIENT KILL / PAUSE, with the NOPERM naming the `cmd|sub` pair. CLIENT LIST
/// is informational yet `@dangerous` in Redis (it leaks every client), so the EXPLICIT `+client|list`
/// is what re-allows it. Case-insensitive.
#[test]
fn config_client_subcommand_acl_allows_reads_denies_mutators() {
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
                    "mon",
                    "on",
                    "nopass",
                    "resetchannels",
                    "-@dangerous",
                    "+config|get",
                    "+client|info",
                    "+client|list",
                ]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "mon", "x"]).await, "+OK\r\n");

        // Granted reads are NOT denied by ACL (the handler runs; cluster mode is irrelevant here).
        // Case-insensitive: `config get` / `CONFIG Get` enforce identically.
        for read in [
            vec!["CONFIG", "GET", "maxmemory"],
            vec!["config", "get", "maxmemory"],
            vec!["CONFIG", "Get", "maxmemory"],
            vec!["CLIENT", "INFO"],
            vec!["CLIENT", "LIST"],
            vec!["client", "list"],
        ] {
            let reply = cmd(&mut a, &read).await;
            assert!(
                !reply.starts_with("-NOPERM"),
                "{read:?} must be allowed by ACL (not NOPERM), got {reply:?}"
            );
        }

        // Mutators / ungranted subs are NOPERM, naming `cmd|sub`.
        for (op, token) in [
            (vec!["CONFIG", "SET", "maxmemory", "100mb"], "config|set"),
            (vec!["CONFIG", "REWRITE"], "config|rewrite"),
            (vec!["CLIENT", "KILL", "ID", "999999"], "client|kill"),
            (vec!["CLIENT", "PAUSE", "1"], "client|pause"),
        ] {
            let reply = cmd(&mut a, &op).await;
            assert!(
                reply.starts_with("-NOPERM") && reply.contains(token),
                "{op:?} must be NOPERM naming {token:?}, got {reply:?}"
            );
        }

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (18) `+@all -@dangerous` CONFIG/CLIENT parity (Redis 7), proving each container's split is per
/// Redis tags. CONFIG: EVERY operative subcommand is `@dangerous`, so CONFIG GET *and* SET are
/// NOPERM, while CONFIG HELP (`@slow` only) is allowed. CLIENT: the reads (INFO/ID, not dangerous)
/// are allowed while the privileged controls (LIST/KILL, `@admin`+`@dangerous`) are NOPERM.
#[test]
fn config_client_all_minus_dangerous_split() {
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

        // CONFIG: GET is @dangerous -> NOPERM even though it is "a read"; HELP (@slow) is allowed.
        let cfg_get = cmd(&mut a, &["CONFIG", "GET", "maxmemory"]).await;
        assert!(
            cfg_get.starts_with("-NOPERM") && cfg_get.contains("config|get"),
            "+@all -@dangerous must deny CONFIG GET (it is @dangerous), got {cfg_get:?}"
        );
        assert!(
            !cmd(&mut a, &["CONFIG", "HELP"])
                .await
                .starts_with("-NOPERM"),
            "CONFIG HELP (@slow only) must be allowed under -@dangerous"
        );

        // CLIENT: the reads survive, the privileged controls are denied.
        assert!(
            !cmd(&mut a, &["CLIENT", "INFO"])
                .await
                .starts_with("-NOPERM")
        );
        assert!(!cmd(&mut a, &["CLIENT", "ID"]).await.starts_with("-NOPERM"));
        let cl_list = cmd(&mut a, &["CLIENT", "LIST"]).await;
        assert!(
            cl_list.starts_with("-NOPERM") && cl_list.contains("client|list"),
            "+@all -@dangerous must deny CLIENT LIST (@dangerous), got {cl_list:?}"
        );
        let cl_kill = cmd(&mut a, &["CLIENT", "KILL", "ID", "1"]).await;
        assert!(
            cl_kill.starts_with("-NOPERM") && cl_kill.contains("client|kill"),
            "+@all -@dangerous must deny CLIENT KILL, got {cl_kill:?}"
        );

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (19) Bare `+config` / `+client` whole-command grants cover EVERY subcommand (Redis parity): a
/// `-@all +config` user runs CONFIG GET *and* CONFIG SET; a `-@all +client` user runs CLIENT INFO
/// *and* CLIENT KILL. (None is `-NOPERM`; a container grant is not narrowed by the subcommand table.)
#[test]
fn config_client_bare_grant_allows_all_subcommands() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL", "SETUSER", "cfg", "on", "nopass", "~*", "-@all", "+config"
                ]
            )
            .await,
            "+OK\r\n"
        );
        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL", "SETUSER", "cli", "on", "nopass", "~*", "-@all", "+client"
                ]
            )
            .await,
            "+OK\r\n"
        );

        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "cfg", "x"]).await, "+OK\r\n");
        assert!(
            !cmd(&mut a, &["CONFIG", "GET", "maxmemory"])
                .await
                .starts_with("-NOPERM")
        );
        assert!(
            !cmd(&mut a, &["CONFIG", "SET", "maxmemory-samples", "10"])
                .await
                .starts_with("-NOPERM"),
            "a bare +config grant must allow CONFIG SET too"
        );

        let mut b = connect_retry(port).await;
        assert_eq!(cmd(&mut b, &["AUTH", "cli", "x"]).await, "+OK\r\n");
        assert!(
            !cmd(&mut b, &["CLIENT", "INFO"])
                .await
                .starts_with("-NOPERM")
        );
        assert!(
            !cmd(&mut b, &["CLIENT", "KILL", "ID", "999999"])
                .await
                .starts_with("-NOPERM"),
            "a bare +client grant must allow CLIENT KILL too"
        );

        drop(a);
        drop(b);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}

/// (20) ACLFILE round trip with `+config|get` / `+client|info` rules: boot with the aclfile, prove
/// enforcement (CONFIG GET / CLIENT INFO allowed, CONFIG SET / CLIENT KILL NOPERM), `ACL SAVE`,
/// REBOOT on the same file, and prove the subcommand rules survived.
#[test]
fn config_client_subcommand_aclfile_round_trip() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let dir = std::env::temp_dir().join(format!("ironcache-aclcc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let aclfile = dir.join("users.acl");
        std::fs::write(
            &aclfile,
            "user default on nopass ~* &* +@all\n\
             user mon on nopass resetchannels -@dangerous +config|get +client|info\n",
        )
        .unwrap();

        let port = free_port();
        let server = run_server_with_aclfile_for_test(port, 1, aclfile.clone());
        let mut c = connect_retry(port).await;
        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "mon", "x"]).await, "+OK\r\n");
        assert!(
            !cmd(&mut a, &["CONFIG", "GET", "maxmemory"])
                .await
                .starts_with("-NOPERM")
        );
        assert!(
            !cmd(&mut a, &["CLIENT", "INFO"])
                .await
                .starts_with("-NOPERM")
        );
        let denied = cmd(&mut a, &["CONFIG", "SET", "maxmemory", "100mb"]).await;
        assert!(
            denied.starts_with("-NOPERM") && denied.contains("config|set"),
            "loaded mon must be NOPERM on CONFIG SET, got {denied:?}"
        );
        let denied_kill = cmd(&mut a, &["CLIENT", "KILL", "ID", "1"]).await;
        assert!(
            denied_kill.starts_with("-NOPERM") && denied_kill.contains("client|kill"),
            "loaded mon must be NOPERM on CLIENT KILL, got {denied_kill:?}"
        );
        drop(a);

        assert_eq!(cmd(&mut c, &["ACL", "SAVE"]).await, "+OK\r\n");
        drop(c);
        server.shutdown_and_join().unwrap();

        let saved = std::fs::read_to_string(&aclfile).unwrap();
        assert!(
            saved.contains("+config|get") && saved.contains("+client|info"),
            "SAVE must persist the subcommand rules, got {saved:?}"
        );

        let port2 = free_port();
        let server2 = run_server_with_aclfile_for_test(port2, 1, aclfile.clone());
        let mut a2 = connect_retry(port2).await;
        assert_eq!(cmd(&mut a2, &["AUTH", "mon", "x"]).await, "+OK\r\n");
        assert!(
            !cmd(&mut a2, &["CONFIG", "GET", "maxmemory"])
                .await
                .starts_with("-NOPERM")
        );
        let denied2 = cmd(&mut a2, &["CONFIG", "SET", "maxmemory", "100mb"]).await;
        assert!(
            denied2.starts_with("-NOPERM") && denied2.contains("config|set"),
            "reloaded mon must still be NOPERM on CONFIG SET, got {denied2:?}"
        );

        drop(a2);
        server2.shutdown_and_join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// (21) MSETEX gates EVERY key through the key-pattern check, not just the first (#412; the
/// Redis 8.6 MSETEX ACL-bypass fix). A user `+msetex ~k:*` can MSETEX when ALL the strided
/// keys (args[2], args[4], ...) match `~k:*`, but is `-NOPERM ... access a key` when ANY key
/// (including a non-first one) falls outside the pattern -- which only holds if the
/// `MsetexNumkeysStrided` key spec extracts the keys past the `numkeys` prefix. The denied
/// op writes NOTHING.
#[test]
fn msetex_acl_key_pattern_gates_every_strided_key() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let server = run_server_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // A user scoped to `~k:*` with +msetex (and +exists/+get to observe the effect).
        assert_eq!(
            cmd(
                &mut c,
                &[
                    "ACL", "SETUSER", "app", "on", ">pw", "~k:*", "+msetex", "+exists", "+get"
                ]
            )
            .await,
            "+OK\r\n"
        );
        let mut a = connect_retry(port).await;
        assert_eq!(cmd(&mut a, &["AUTH", "app", "pw"]).await, "+OK\r\n");

        // ALL keys inside ~k:* -> allowed (reply 1).
        assert_eq!(
            cmd(&mut a, &["MSETEX", "2", "k:1", "v1", "k:2", "v2"]).await,
            ":1\r\n"
        );
        assert_eq!(cmd(&mut a, &["GET", "k:1"]).await, "$2\r\nv1\r\n");

        // The SECOND key is OUTSIDE ~k:* -> NOPERM key (proves the non-first key is checked),
        // and NOTHING is written (the foreign key never appears).
        let denied = cmd(&mut a, &["MSETEX", "2", "k:3", "v3", "other:1", "v4"]).await;
        assert!(
            denied.starts_with("-NOPERM") && denied.contains("access a key"),
            "a foreign non-first MSETEX key must be NOPERM, got {denied:?}"
        );
        // The default (admin) connection confirms neither key was set by the denied op.
        assert_eq!(cmd(&mut c, &["EXISTS", "k:3"]).await, ":0\r\n");
        assert_eq!(cmd(&mut c, &["EXISTS", "other:1"]).await, ":0\r\n");

        drop(a);
        drop(c);
        server.shutdown_and_join().unwrap();
    });
}
