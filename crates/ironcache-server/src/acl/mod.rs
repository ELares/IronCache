// SPDX-License-Identifier: MIT OR Apache-2.0
//! The ACL ENGINE (#106): per-user authentication + per-command + per-key authorization.
//!
//! Before this module IronCache had a SINGLE auth boundary (`requirepass` -> one `default`
//! user, all-or-nothing): once authenticated, any client could run any command on any key.
//! This module adds the production model: named [`User`]s, each with passwords (SHA-256 at
//! rest), an enabled bit, and per-command (`+@cat`/`-cmd`) + per-key (`~pattern`) + per-
//! channel (`&pattern`) permissions, behind the auth gate already hoisted to the router.
//!
//! ## Backward compatibility (byte-identical default)
//!
//! With NO `requirepass` and NO ACL config, the registry holds exactly ONE user -
//! `default` = `on nopass ~* &* +@all` - and every connection authenticates as it with full
//! access. The enforcement layer's [`User::is_all_permissive`] shortcut means that default
//! deployment pays at most one bool test per command, so it stays byte-identical and O(1).
//! With `requirepass` set, `default` becomes `on >#<hash> ~* &* +@all`, so the legacy `AUTH
//! <pass>` path keeps working. ACL users are layered ON TOP.
//!
//! ## The hot path
//!
//! A connection caches its authenticated [`Arc<User>`] in `ConnState` at AUTH time, so the
//! per-command enforcement check ([`AclState::is_acl_active`] gate + the user's
//! `can_run_command` / `can_access_key`) reads it LOCK-FREE: no registry lock on the data
//! path. The registry lock is taken only on `AUTH` (to resolve the user once) and on the
//! rare `ACL SETUSER`/`DELUSER`/`LOAD`.
//!
//! ## Layout
//! - [`categories`]: the command -> category map (`@read`/`@write`/`@admin`/...).
//! - [`perms`]: the [`User`] model + compiled [`perms::CommandPerms`]/[`perms::KeyPerms`]/
//!   [`perms::ChannelPerms`] and their cheap per-command/key/channel tests.
//! - [`parse`]: the Redis ACL rule-grammar parser (`on`/`>pw`/`~pat`/`+@cat`/...).
//! - this module: the shared, runtime-mutable [`AclState`] registry + the aclfile
//!   serialize/load helpers + the constant-time compare.

pub mod categories;
pub mod parse;
pub mod perms;

pub use categories::Category;
pub use parse::{AclParseError, apply_rules_to, build_user, redacted_rule};
pub use perms::{DEFAULT_USER, User};

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Compare two byte slices in CONSTANT TIME with respect to their CONTENTS (length is not
/// secret in this model). Mirrors the dispatch-layer `constant_time_eq` so the ACL password
/// compare has the same timing-leak resistance as the legacy requirepass path: it folds
/// EVERY byte pair into an XOR accumulator and reads it through [`std::hint::black_box`]
/// before the zero test, so the optimizer cannot reintroduce a data-dependent early exit.
#[must_use]
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    std::hint::black_box(acc) == 0
}

/// The process-wide, runtime-mutable ACL user registry (the analog of
/// [`ironcache_config::RuntimeConfig`] for users). Shared as `Arc<AclState>` into every
/// shard's [`crate::ServerContext`] at boot; the users live behind ONE `Mutex` taken only
/// on `AUTH` (resolve a user once) and the rare `ACL SETUSER`/`DELUSER`/`LOAD`, NEVER on the
/// per-command data path (a connection caches its `Arc<User>` and checks it lock-free).
///
/// ## The "ACL active" fast gate
///
/// `acl_active` is an atomic bool: `false` when the registry holds ONLY the all-permissive
/// `default` user (the no-ACL deployment), `true` once any non-default user exists OR the
/// `default` user has been narrowed. The enforcement layer reads this ONE relaxed atomic
/// first; when `false` it skips ACL enforcement entirely, so the default path is byte-
/// identical and adds a single bool load. It is recomputed under the lock on every mutation.
///
/// ## Live revocation (the `generation` counter, F1)
///
/// A connection caches its resolved [`Arc<User>`] at AUTH time (a FROZEN snapshot). Without
/// a live link back to the registry, an `ACL SETUSER app -@all` / `ACL DELUSER app` to
/// revoke a misbehaving client would have NO effect until that client re-AUTHs or
/// disconnects (fail-open in the REVOCATION direction), diverging from Redis (which mutates
/// in place / kills a deleted user's clients). To close that without re-locking on the hot
/// path, every mutation (`set_user`, `del_user`, `put_user`, `load_users`) BUMPS a monotonic
/// `generation`. A connection caches the generation alongside its user; the per-command path
/// does ONE relaxed atomic load + integer compare, and RE-RESOLVES the connection's user BY
/// NAME only when the generation MOVED (rare). So a mid-session restrict / delete takes
/// effect on the connection's very next command, the unchanged-generation path stays a single
/// compare, and the no-ACL default never moves the generation at all.
#[derive(Debug)]
pub struct AclState {
    /// The users, keyed by name. `BTreeMap` so `ACL LIST`/`USERS`/aclfile SAVE emit a
    /// stable, sorted order (deterministic output, ADR-0003).
    ///
    /// This lock is a CONTROL-PLANE lock, NOT hot-path data: it is taken ONLY on `AUTH`
    /// (resolve + clone the user's `Arc<User>` once) and the rare `ACL SETUSER`/`DELUSER`/
    /// `LOAD`, NEVER on the per-command data path -- a connection caches its `Arc<User>` at
    /// AUTH time and the enforcement check reads that lock-free (see the struct + module
    /// docs). It is therefore exempt from the shared-nothing per-shard no-lock invariant
    /// (which guards the per-shard store/eviction/expiry hot path, ADR-0005), like the
    /// process-wide `RuntimeConfig` overlay it mirrors.
    users: Mutex<BTreeMap<String, Arc<User>>>, // lint-allow: shared-nothing (control-plane registry, off the per-command hot path)
    /// The fast "ACL is doing something beyond the legacy default" gate (see the struct
    /// doc). Relaxed: the enforcement read tolerates a one-command staleness window exactly
    /// like the runtime-config overlay's `maxmemory` read.
    acl_active: std::sync::atomic::AtomicBool,
    /// The monotonic mutation GENERATION (F1, live revocation). Starts at 0 and is bumped by
    /// EVERY registry mutation (`set_user` / `del_user` / `put_user` / `load_users`). A
    /// connection caches the generation it resolved its `Arc<User>` against; the per-command
    /// enforcement path does one relaxed load + compare and re-resolves the connection's user
    /// by name ONLY when this moved. Relaxed ordering: like `acl_active`, a one-command
    /// staleness window is acceptable (the next command on the connection re-checks), and the
    /// re-resolve itself takes the registry lock (which carries the real happens-before).
    generation: AtomicU64,
}

/// The outcome of re-resolving a connection's cached ACL identity after the registry
/// generation moved (F1, live revocation). See [`AclState::resolve_if_stale`].
#[derive(Debug)]
pub enum AclResolution {
    /// The user still exists: cache `user` (`None` = the all-permissive implicit default) and
    /// record `generation` as the connection's new cached generation.
    Refresh {
        /// The user to cache on the connection (`None` = all-permissive, the byte-identical
        /// implicit-default posture; `Some` = a narrowed user whose new restrictions now apply).
        user: Option<Arc<User>>,
        /// The registry generation observed during this re-resolve, to cache on the connection.
        generation: u64,
    },
    /// The user was DELETED (`ACL DELUSER`): the connection's identity is gone, so it must be
    /// deauthenticated / closed (Redis kills a deleted user's clients).
    Deauth,
}

impl AclState {
    /// Build the boot registry from the resolved `requirepass` digest (the legacy single-
    /// password path) - the ONLY user is `default`. With no requirepass (`None`) `default`
    /// is `on nopass ~* &* +@all` (the byte-identical no-auth posture); with a digest it is
    /// `on >#<digest> ~* &* +@all` (AUTH <pass> authenticates default, full access).
    ///
    /// ACL users from an aclfile are layered on AFTER boot via [`Self::load_users`].
    #[must_use]
    pub fn from_requirepass(requirepass_hash: Option<&str>) -> Arc<AclState> {
        let default = match requirepass_hash {
            None => User::default_nopass(),
            Some(h) => User::default_with_password(h.to_owned()),
        };
        let mut map = BTreeMap::new();
        map.insert(default.name.clone(), Arc::new(default));
        let state = AclState {
            users: Mutex::new(map),
            acl_active: std::sync::atomic::AtomicBool::new(false),
            generation: AtomicU64::new(0),
        };
        // A requirepass-only default is still all-permissive, so acl_active stays false.
        Arc::new(state)
    }

    /// Whether ACL enforcement is ACTIVE (any non-default user, or a narrowed default). The
    /// enforcement hot path reads this ONE relaxed atomic; `false` => skip ACL entirely
    /// (the legacy single-default-user posture, byte-identical).
    #[must_use]
    pub fn is_acl_active(&self) -> bool {
        self.acl_active.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The CURRENT mutation generation (F1, live revocation). A connection caches the value it
    /// resolved its `Arc<User>` against and compares against this once per command (one relaxed
    /// load + integer compare); it re-resolves by name only when this moved. Relaxed: a one-
    /// command staleness window is acceptable (the next command re-checks), and the re-resolve
    /// takes the registry lock, which carries the real happens-before for the user data.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Bump the mutation generation (called under the lock after every registry mutation, so a
    /// connection's cached generation goes stale and the next command re-resolves its user).
    /// Wrapping is harmless: it is only ever COMPARED for inequality, never ordered.
    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Recompute `acl_active` from the current users map (called under the lock after every
    /// mutation). It is `true` unless the registry is EXACTLY `{ default }` with an all-
    /// permissive default user.
    fn recompute_active(&self, map: &BTreeMap<String, Arc<User>>) {
        let only_default_all_permissive =
            map.len() == 1 && map.get(DEFAULT_USER).is_some_and(|u| u.is_all_permissive());
        self.acl_active.store(
            !only_default_all_permissive,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// RE-RESOLVE a connection's cached ACL identity against the CURRENT registry, used by the
    /// per-command enforcement path ONLY when the connection's cached generation differs from
    /// [`Self::generation`] (F1, live revocation). `name` is the connection's authenticated
    /// username (the conn knows it, including `"default"`). Returns:
    /// - [`AclResolution::Refresh`] with the user the connection should now cache: `None` when
    ///   that user is all-permissive (the implicit-default fast path, so a back-to-permissive
    ///   default re-collapses to the byte-identical `None` posture) or `Some(Arc<User>)` when it
    ///   is narrowed (so a SETUSER that RESTRICTS a previously-`None` default is picked up). The
    ///   caller also records [`Self::generation`] read here as the connection's new cached
    ///   generation.
    /// - [`AclResolution::Deauth`] when the user was DELETED (`ACL DELUSER`): the connection is
    ///   no longer a valid identity, so the caller deauthenticates / closes it (Redis kills a
    ///   deleted user's clients).
    ///
    /// This runs OFF the hot path (only on a generation move), so taking the registry lock here
    /// is fine; the unchanged-generation path never calls it.
    #[must_use]
    pub fn resolve_if_stale(&self, name: &str) -> AclResolution {
        match self.lock().get(name).cloned() {
            Some(user) => {
                let cached = if user.is_all_permissive() {
                    None
                } else {
                    Some(user)
                };
                AclResolution::Refresh {
                    user: cached,
                    generation: self.generation(),
                }
            }
            None => AclResolution::Deauth,
        }
    }

    /// Resolve the user `name` to its current `Arc<User>` (a cheap Arc clone under the
    /// lock). `None` if no such user. Called by `AUTH` to fetch + cache the authenticated
    /// identity ONCE; the per-command path never calls this.
    #[must_use]
    pub fn get_user(&self, name: &str) -> Option<Arc<User>> {
        self.lock().get(name).cloned()
    }

    /// AUTHENTICATE `name` with the candidate `password`: resolve the user and verify the
    /// password (constant-time, `nopass`-aware, enabled-gated). On success returns the
    /// `Arc<User>` to cache on the connection; `None` on no-such-user / disabled / wrong
    /// password (the caller maps `None` to `-WRONGPASS`, never revealing which).
    #[must_use]
    pub fn authenticate(&self, name: &str, password: &[u8]) -> Option<Arc<User>> {
        let user = self.get_user(name)?;
        if user.verify_password(password) {
            Some(user)
        } else {
            None
        }
    }

    /// `ACL SETUSER <name> <rules...>`: clone the live user (or a fresh baseline if new),
    /// apply the rules into the SCRATCH copy, and commit ONLY on full success - so a mid-
    /// sequence error leaves the live user untouched (Redis atomicity). Returns the parse
    /// error otherwise. Recomputes the `acl_active` gate.
    ///
    /// # Errors
    /// The first [`AclParseError`] from the rule sequence.
    pub fn set_user(&self, name: &str, rules: &[&[u8]]) -> Result<(), AclParseError> {
        let mut map = self.lock();
        // Seed from the existing user (SETUSER is incremental) or a fresh baseline.
        let mut scratch = map
            .get(name)
            .map_or_else(|| User::new(name), |u| (**u).clone());
        apply_rules_to(&mut scratch, rules)?;
        map.insert(name.to_owned(), Arc::new(scratch));
        self.recompute_active(&map);
        // F1: bump the generation so live connections re-resolve their cached user on the next
        // command (a mid-session restrict / re-grant takes effect immediately). Done only on the
        // SUCCESS path -- a parse error left the live user untouched, so nothing changed.
        self.bump_generation();
        Ok(())
    }

    /// Insert / replace a fully-built user (used by aclfile load, which builds each user
    /// from its full rule line). Recomputes the gate.
    pub fn put_user(&self, user: User) {
        let mut map = self.lock();
        map.insert(user.name.clone(), Arc::new(user));
        self.recompute_active(&map);
        // F1: a replaced user must reach live connections on their next command.
        self.bump_generation();
    }

    /// `ACL DELUSER <name>`: remove the user. The `default` user CANNOT be deleted (Redis
    /// refuses it). Returns `true` if a user was removed, `false` if absent. Recomputes the
    /// gate.
    pub fn del_user(&self, name: &str) -> bool {
        if name == DEFAULT_USER {
            return false;
        }
        let mut map = self.lock();
        let removed = map.remove(name).is_some();
        if removed {
            self.recompute_active(&map);
            // F1: bump so any connection still authed as the deleted user re-resolves on its
            // next command and is DEAUTHENTICATED (Redis kills a deleted user's clients).
            self.bump_generation();
        }
        removed
    }

    /// The user names, sorted (for `ACL USERS`).
    #[must_use]
    pub fn user_names(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    /// The full `user <name> <rules>` aclfile lines for every user, sorted (for `ACL LIST`
    /// and aclfile SAVE). Passwords are rendered as `#<sha256-hex>` digests, never plaintext.
    #[must_use]
    pub fn list_lines(&self) -> Vec<String> {
        self.lock()
            .values()
            .map(|u| format!("user {} {}", u.name, u.describe_rules()))
            .collect()
    }

    /// The aclfile TEXT for the whole registry (one `user ...` line per user, newline-
    /// terminated), the bytes `ACL SAVE` writes.
    #[must_use]
    pub fn serialize_aclfile(&self) -> String {
        let mut s = self.list_lines().join("\n");
        s.push('\n');
        s
    }

    /// Load users from aclfile TEXT, REPLACING the entire registry (Redis `ACL LOAD`
    /// semantics: the file is the authoritative source). Each non-blank, non-comment line is
    /// `user <name> <rule>...`. A file with no `default` line leaves the existing all-
    /// permissive default in place IFF none is defined (Redis always has a default; we keep
    /// the boot default if the file omits it). Returns the count loaded, or a parse error
    /// with the offending line.
    ///
    /// # Errors
    /// Returns `(line_number, AclParseError)` for the first malformed line.
    pub fn load_users(&self, text: &str) -> Result<usize, (usize, AclParseError)> {
        let mut parsed: Vec<User> = Vec::new();
        let mut has_default = false;
        for (lineno, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let tokens: Vec<&[u8]> = trimmed.split_whitespace().map(str::as_bytes).collect();
            // Expect `user <name> <rules...>`.
            if tokens.len() < 2 || !tokens[0].eq_ignore_ascii_case(b"user") {
                return Err((
                    lineno + 1,
                    AclParseError {
                        rule: trimmed.to_owned(),
                        reason: "aclfile line must start with 'user <name>'".to_owned(),
                    },
                ));
            }
            let name = String::from_utf8_lossy(tokens[1]).into_owned();
            if name == DEFAULT_USER {
                has_default = true;
            }
            let user = build_user(&name, &tokens[2..]).map_err(|e| (lineno + 1, e))?;
            parsed.push(user);
        }
        // Commit: replace the registry. Preserve the existing default if the file omitted it.
        let mut map = self.lock();
        let preserved_default = if has_default {
            None
        } else {
            map.get(DEFAULT_USER).cloned()
        };
        map.clear();
        if let Some(d) = preserved_default {
            map.insert(DEFAULT_USER.to_owned(), d);
        }
        let count = parsed.len();
        for u in parsed {
            map.insert(u.name.clone(), Arc::new(u));
        }
        self.recompute_active(&map);
        // F1: ACL LOAD replaces the whole registry, so every live connection must re-resolve
        // its cached user (it may have been narrowed, re-granted, or removed) on its next command.
        self.bump_generation();
        Ok(count)
    }

    /// Lock the users map, recovering from a poisoned lock (a panic in another thread must
    /// not wedge auth: the map is plain data, so the recovered state is consistent). The lock
    /// is a control-plane lock (AUTH / `ACL SETUSER`-`DELUSER`-`LOAD` only), never the per-
    /// command hot path -- see the `users` field doc for the shared-nothing-invariant exemption.
    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Arc<User>>> {
        self.users.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_no_requirepass_is_byte_identical_inactive() {
        let acl = AclState::from_requirepass(None);
        assert!(!acl.is_acl_active(), "no-ACL default must be inactive");
        // The default user authenticates with any password (nopass).
        let u = acl.authenticate(DEFAULT_USER, b"anything").expect("nopass");
        assert!(u.is_all_permissive());
    }

    #[test]
    fn requirepass_default_authenticates_full_access() {
        let hash = ironcache_config::sha256_hex(b"s3cr3t");
        let acl = AclState::from_requirepass(Some(&hash));
        assert!(
            !acl.is_acl_active(),
            "requirepass-only default stays inactive"
        );
        assert!(acl.authenticate(DEFAULT_USER, b"s3cr3t").is_some());
        assert!(acl.authenticate(DEFAULT_USER, b"wrong").is_none());
    }

    #[test]
    fn adding_a_user_activates_acl() {
        let acl = AclState::from_requirepass(None);
        acl.set_user("app", &[b"on", b">pw", b"~k:*", b"+get"])
            .expect("ok");
        assert!(acl.is_acl_active());
        // The app user can GET k:1 but not SET; key other:1 is denied.
        let u = acl.authenticate("app", b"pw").expect("auth");
        assert!(u.can_run_command(b"GET"));
        assert!(!u.can_run_command(b"SET"));
        assert!(u.can_access_key(b"k:1"));
        assert!(!u.can_access_key(b"other:1"));
        // Wrong password -> no auth.
        assert!(acl.authenticate("app", b"nope").is_none());
    }

    #[test]
    fn deluser_cannot_remove_default() {
        let acl = AclState::from_requirepass(None);
        acl.set_user("app", &[b"on", b"nopass", b"+@all", b"~*"])
            .expect("ok");
        assert!(acl.del_user("app"));
        assert!(!acl.del_user("default"));
        assert!(acl.get_user("default").is_some());
    }

    #[test]
    fn setuser_atomic_on_parse_error() {
        let acl = AclState::from_requirepass(None);
        acl.set_user("app", &[b"on", b"nopass", b"+get"])
            .expect("ok");
        // A later SETUSER with a bad rule must leave the live user untouched.
        let err = acl.set_user("app", &[b"+set", b"+boguscmd"]);
        assert!(err.is_err());
        let u = acl.get_user("app").expect("still there");
        assert!(u.can_run_command(b"GET"));
        // +set was NOT committed (the whole modifier list rolled back).
        assert!(!u.can_run_command(b"SET"));
    }

    #[test]
    fn aclfile_save_load_round_trip() {
        let acl = AclState::from_requirepass(None);
        acl.set_user("app", &[b"on", b">pw", b"~app:*", b"+@read", b"+set"])
            .expect("ok");
        let text = acl.serialize_aclfile();
        // A fresh registry loads it back and the user survives with the same perms.
        let acl2 = AclState::from_requirepass(None);
        let n = acl2.load_users(&text).expect("load");
        assert!(n >= 1);
        let u = acl2.authenticate("app", b"pw").expect("auth after reload");
        assert!(u.can_run_command(b"GET"));
        assert!(u.can_run_command(b"SET"));
        assert!(u.can_access_key(b"app:1"));
        assert!(!u.can_access_key(b"x"));
    }

    #[test]
    fn disabled_user_cannot_authenticate() {
        let acl = AclState::from_requirepass(None);
        acl.set_user("app", &[b"off", b"nopass", b"+@all", b"~*"])
            .expect("ok");
        assert!(acl.authenticate("app", b"anything").is_none());
    }

    #[test]
    fn generation_bumps_on_every_mutation() {
        // F1: a fresh registry starts at generation 0; SETUSER / DELUSER / put_user / load each
        // bump it, so a connection caching a stale generation re-resolves on its next command.
        let acl = AclState::from_requirepass(None);
        assert_eq!(acl.generation(), 0);
        acl.set_user("app", &[b"on", b"nopass", b"+@all", b"~*"])
            .expect("ok");
        let g1 = acl.generation();
        assert!(g1 > 0, "set_user bumps the generation");
        // A SETUSER that FAILS to parse must NOT bump (the live user was untouched).
        let _ = acl.set_user("app", &[b"+boguscmd"]);
        assert_eq!(acl.generation(), g1, "a failed set_user does not bump");
        acl.set_user("app", &[b"-set"]).expect("ok");
        let g2 = acl.generation();
        assert!(g2 > g1, "a successful set_user bumps again");
        assert!(acl.del_user("app"), "removed");
        let g3 = acl.generation();
        assert!(g3 > g2, "del_user bumps");
        // A DELUSER of an absent user does not bump.
        assert!(!acl.del_user("nope"));
        assert_eq!(acl.generation(), g3, "a no-op del_user does not bump");
    }

    #[test]
    fn resolve_if_stale_refresh_narrowed_default_and_deauth() {
        let acl = AclState::from_requirepass(None);
        // (a) DEFAULT narrowed: a connection cached as the all-permissive default (name
        // "default") must, after `SETUSER default -@dangerous`, re-resolve to a NARROWED user
        // (Some, not None) so the restriction applies.
        acl.set_user("default", &[b"-@dangerous"]).expect("ok");
        match acl.resolve_if_stale("default") {
            AclResolution::Refresh { user, generation } => {
                let u = user.expect("narrowed default is Some, not the all-permissive None");
                assert!(!u.can_run_command(b"FLUSHALL"));
                assert!(u.can_run_command(b"GET"));
                assert_eq!(generation, acl.generation());
            }
            AclResolution::Deauth => panic!("default still exists"),
        }
        // (b) A narrowed user re-granted back to all-permissive (`+@all ~* &*`) re-collapses to
        // None. The user starts NARROWED (only `+get`), so re-resolve sees Some; then we re-grant
        // everything and re-resolve must collapse to the byte-identical all-permissive None.
        acl.set_user("app", &[b"on", b"nopass", b"+get", b"~*"])
            .expect("ok");
        match acl.resolve_if_stale("app") {
            AclResolution::Refresh { user, .. } => assert!(user.is_some(), "narrowed -> Some"),
            AclResolution::Deauth => panic!("app exists"),
        }
        acl.set_user("app", &[b"+@all", b"&*"]).expect("ok");
        match acl.resolve_if_stale("app") {
            AclResolution::Refresh { user, .. } => {
                assert!(user.is_none(), "all-permissive re-collapses to None");
            }
            AclResolution::Deauth => panic!("app exists"),
        }
        // (c) DELUSER -> Deauth.
        assert!(acl.del_user("app"));
        assert!(matches!(acl.resolve_if_stale("app"), AclResolution::Deauth));
    }

    #[test]
    fn dangerous_category_carveout() {
        let acl = AclState::from_requirepass(None);
        acl.set_user("app", &[b"on", b"nopass", b"~*", b"+@all", b"-@dangerous"])
            .expect("ok");
        let u = acl.authenticate("app", b"x").expect("auth");
        assert!(u.can_run_command(b"GET"));
        assert!(u.can_run_command(b"SET"));
        assert!(!u.can_run_command(b"FLUSHALL"));
        assert!(!u.can_run_command(b"CONFIG"));
        assert!(!u.can_run_command(b"SHUTDOWN"));
    }
}
