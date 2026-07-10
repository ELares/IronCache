// SPDX-License-Identifier: MIT OR Apache-2.0
//! The ACL USER model and its COMPILED, hot-path permission tests (#106).
//!
//! A [`User`] is the unit of authentication + authorization: a name, an enabled bit, a
//! set of password digests (SHA-256 hex AT REST), and three permission groups -
//! [`CommandPerms`] (which commands + categories), [`KeyPerms`] (which key patterns), and
//! [`ChannelPerms`] (which pub/sub channels).
//!
//! ## The hot-path contract (cheap per-command checks)
//!
//! The per-command authorization check runs on EVERY command of an authenticated, ACL-
//! governed connection, so it must be O(1) (plus a glob over the few key args only for a
//! key-bearing command). To get that:
//! - [`CommandPerms`] is COMPILED on `ACL SETUSER` into the ALLOW/DENY rule list PLUS a
//!   fast `allcommands` shortcut. The per-command test ([`CommandPerms::allows`]) replays
//!   the rule list (a handful of bit tests + a name compare), which is bounded by the
//!   number of rules the operator wrote, not by re-parsing anything.
//! - The COMMON case - the all-permissive default user (`+@all ~* &*`) - is a SINGLE bool
//!   shortcut ([`User::is_all_permissive`]): the enforcement layer skips every check for
//!   it, so the default (no-ACL) deployment pays at most one bool test and stays
//!   byte-identical.
//! - [`KeyPerms`] / [`ChannelPerms`] are an `allkeys`/`allchannels` bool plus a small glob
//!   pattern list, matched only for the command's actual key/channel args.
//!
//! ## Security
//!
//! Passwords are stored as SHA-256 HEX digests (reusing [`ironcache_config::sha256_hex`]),
//! never plaintext, and compared in CONSTANT TIME ([`super::ct_eq`]). A `nopass` user
//! authenticates with any password (or none); a user with no passwords and not `nopass`
//! can never authenticate. None of these structs ever hold or log a plaintext password.

use super::categories::{Category, CategorySet, category_bits, subcommand_category_bits};
use crate::glob::glob_match;

/// One command-permission RULE, in the order the operator wrote it. Authorization replays
/// the rules in order; a LATER rule overrides an earlier one (Redis ACL "last match
/// wins"), which is what makes `+@all -flushall` (allow everything, then carve out one)
/// and `-@all +get` (deny everything, then allow one) both work.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CmdRule {
    /// `+@all` / `allcommands`: allow every command.
    AllowAll,
    /// `-@all` / `nocommands`: deny every command.
    DenyAll,
    /// `+@<cat>`: allow every command in the category.
    AllowCat(Category),
    /// `-@<cat>`: deny every command in the category.
    DenyCat(Category),
    /// `+<cmd>`: allow a single command (UPPERCASE token).
    AllowCmd(Vec<u8>),
    /// `-<cmd>`: deny a single command (UPPERCASE token).
    DenyCmd(Vec<u8>),
    /// `+<cmd>|<sub>`: allow a single SUBCOMMAND of a container command (both tokens UPPERCASE),
    /// e.g. `+cluster|slots`. Matches ONLY the exact `(container, subcommand)` pair; a bare
    /// `+<cmd>` ([`Self::AllowCmd`]) still grants ALL subcommands (Redis parity).
    AllowSub(Vec<u8>, Vec<u8>),
    /// `-<cmd>|<sub>`: deny a single SUBCOMMAND of a container command (both tokens UPPERCASE),
    /// e.g. `-cluster|addslots`. Matches ONLY the exact `(container, subcommand)` pair.
    DenySub(Vec<u8>, Vec<u8>),
}

/// A user's command permissions: the ordered rule list. Compiled once on `ACL SETUSER`;
/// the per-command [`Self::allows`] test replays the rules (cheap bit tests + a name
/// compare), with the `allcommands`-only fast path captured in [`Self::is_allcommands`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandPerms {
    rules: Vec<CmdRule>,
}

impl CommandPerms {
    /// The deny-everything baseline (`-@all`), the Redis ACL default for a fresh user.
    #[must_use]
    pub fn nocommands() -> CommandPerms {
        CommandPerms {
            rules: vec![CmdRule::DenyAll],
        }
    }

    /// The allow-everything permission (`+@all`), used by the all-permissive `default` user.
    #[must_use]
    pub fn allcommands() -> CommandPerms {
        CommandPerms {
            rules: vec![CmdRule::AllowAll],
        }
    }

    /// Reset to deny-everything (the `reset` / `nocommands` rule).
    pub fn reset(&mut self) {
        self.rules = vec![CmdRule::DenyAll];
    }

    /// Append `+@all` (clears prior rules: `allcommands` is absolute, matching Redis).
    pub fn allow_all(&mut self) {
        self.rules = vec![CmdRule::AllowAll];
    }

    /// Append `-@all` (clears prior rules: `nocommands` is absolute).
    pub fn deny_all(&mut self) {
        self.rules = vec![CmdRule::DenyAll];
    }

    /// Append `+@<cat>`.
    pub fn allow_category(&mut self, c: Category) {
        self.rules.push(CmdRule::AllowCat(c));
    }

    /// Append `-@<cat>`.
    pub fn deny_category(&mut self, c: Category) {
        self.rules.push(CmdRule::DenyCat(c));
    }

    /// Append `+<cmd>` (the token is stored UPPERCASE).
    pub fn allow_command(&mut self, cmd_upper: &[u8]) {
        self.rules.push(CmdRule::AllowCmd(cmd_upper.to_vec()));
    }

    /// Append `-<cmd>` (the token is stored UPPERCASE).
    pub fn deny_command(&mut self, cmd_upper: &[u8]) {
        self.rules.push(CmdRule::DenyCmd(cmd_upper.to_vec()));
    }

    /// Append `+<cmd>|<sub>` (both tokens stored UPPERCASE): allow a single subcommand of a
    /// container command.
    pub fn allow_subcommand(&mut self, cmd_upper: &[u8], sub_upper: &[u8]) {
        self.rules
            .push(CmdRule::AllowSub(cmd_upper.to_vec(), sub_upper.to_vec()));
    }

    /// Append `-<cmd>|<sub>` (both tokens stored UPPERCASE): deny a single subcommand of a
    /// container command.
    pub fn deny_subcommand(&mut self, cmd_upper: &[u8], sub_upper: &[u8]) {
        self.rules
            .push(CmdRule::DenySub(cmd_upper.to_vec(), sub_upper.to_vec()));
    }

    /// Whether the permission is EXACTLY `+@all` (the all-permissive shortcut), so the
    /// enforcement layer can skip the per-command replay entirely for the default user.
    #[must_use]
    pub fn is_allcommands(&self) -> bool {
        self.rules == [CmdRule::AllowAll]
    }

    /// Replay the rule list for `(cmd_upper, sub_upper)` against the precomputed EFFECTIVE
    /// category set `eff_cat`, last-match-wins (Redis "last match wins"). The default (no rule
    /// matches) is DENY, matching Redis's `-@all` baseline.
    ///
    /// * `AllowCat`/`DenyCat` test `eff_cat` (the subcommand's effective categories when a
    ///   recognized subcommand is being checked, else the whole-command categories).
    /// * `AllowCmd`/`DenyCmd` match the CONTAINER token only, so a bare `+cluster` grants EVERY
    ///   subcommand and `-cluster` denies EVERY subcommand (Redis parity), regardless of `sub`.
    /// * `AllowSub`/`DenySub` match the exact `(cmd, sub)` pair; they are INERT when `sub_upper`
    ///   is `None` (a no-subcommand caller), so the whole-command path is byte-identical.
    #[must_use]
    fn allows_replay(
        &self,
        cmd_upper: &[u8],
        sub_upper: Option<&[u8]>,
        eff_cat: CategorySet,
    ) -> bool {
        let mut allowed = false;
        for rule in &self.rules {
            match rule {
                CmdRule::AllowAll => allowed = true,
                CmdRule::DenyAll => allowed = false,
                CmdRule::AllowCat(c) => {
                    if eff_cat.contains(*c) {
                        allowed = true;
                    }
                }
                CmdRule::DenyCat(c) => {
                    if eff_cat.contains(*c) {
                        allowed = false;
                    }
                }
                CmdRule::AllowCmd(name) => {
                    if name.as_slice() == cmd_upper {
                        allowed = true;
                    }
                }
                CmdRule::DenyCmd(name) => {
                    if name.as_slice() == cmd_upper {
                        allowed = false;
                    }
                }
                CmdRule::AllowSub(name, sub) => {
                    if name.as_slice() == cmd_upper && Some(sub.as_slice()) == sub_upper {
                        allowed = true;
                    }
                }
                CmdRule::DenySub(name, sub) => {
                    if name.as_slice() == cmd_upper && Some(sub.as_slice()) == sub_upper {
                        allowed = false;
                    }
                }
            }
        }
        allowed
    }

    /// Whether `cmd_upper` is allowed (computes the command's categories then replays the
    /// rules). The per-command entry point used by enforcement. Delegates to [`Self::allows_sub`]
    /// with no subcommand, so the no-subcommand path is exactly the original behavior.
    #[must_use]
    pub fn allows(&self, cmd_upper: &[u8]) -> bool {
        self.allows_sub(cmd_upper, None)
    }

    /// Whether `(cmd_upper, sub_upper)` is allowed. When `sub_upper` is `Some` AND the
    /// `(cmd, sub)` pair is a recognized subcommand (in the command-spec table), the EFFECTIVE
    /// category set is the SUBCOMMAND's tags ([`subcommand_category_bits`]) so a read subcommand
    /// (CLUSTER SLOTS = `@slow`) is judged independently of its container's `@admin`/`@dangerous`;
    /// otherwise the effective set is the whole-command's [`category_bits`] (so an unknown
    /// subcommand inherits the container's categories, and a `None` caller is unchanged). Then the
    /// rule list is replayed last-match-wins.
    #[must_use]
    pub fn allows_sub(&self, cmd_upper: &[u8], sub_upper: Option<&[u8]>) -> bool {
        let eff_cat = match sub_upper {
            Some(sub) if crate::command_spec::subcommand_spec(cmd_upper, sub).is_some() => {
                subcommand_category_bits(cmd_upper, sub)
            }
            _ => category_bits(cmd_upper),
        };
        self.allows_replay(cmd_upper, sub_upper, eff_cat)
    }

    /// Render the command perms back to the Redis ACL rule string (`+@all`, `-@all +get`,
    /// ...), for `ACL GETUSER`/`LIST`/aclfile SAVE. Always starts from the implicit `-@all`
    /// baseline a fresh user has, so a round-trip reproduces the same effective perms.
    #[must_use]
    pub fn describe(&self) -> String {
        // If the first rule is not an absolute all/none, the user's baseline is the
        // implicit -@all; we render that explicitly so a reload reproduces the perms.
        let mut parts: Vec<String> = Vec::new();
        let starts_absolute = matches!(
            self.rules.first(),
            Some(CmdRule::AllowAll | CmdRule::DenyAll)
        );
        if !starts_absolute {
            parts.push("-@all".to_owned());
        }
        for rule in &self.rules {
            parts.push(match rule {
                CmdRule::AllowAll => "+@all".to_owned(),
                CmdRule::DenyAll => "-@all".to_owned(),
                CmdRule::AllowCat(c) => format!("+@{}", c.name()),
                CmdRule::DenyCat(c) => format!("-@{}", c.name()),
                CmdRule::AllowCmd(n) => {
                    format!("+{}", String::from_utf8_lossy(n).to_ascii_lowercase())
                }
                CmdRule::DenyCmd(n) => {
                    format!("-{}", String::from_utf8_lossy(n).to_ascii_lowercase())
                }
                CmdRule::AllowSub(c, s) => format!(
                    "+{}|{}",
                    String::from_utf8_lossy(c).to_ascii_lowercase(),
                    String::from_utf8_lossy(s).to_ascii_lowercase()
                ),
                CmdRule::DenySub(c, s) => format!(
                    "-{}|{}",
                    String::from_utf8_lossy(c).to_ascii_lowercase(),
                    String::from_utf8_lossy(s).to_ascii_lowercase()
                ),
            });
        }
        parts.join(" ")
    }
}

/// A user's KEY permissions: either `allkeys` (`~*`) or a list of glob patterns (`~pat`).
/// The v1 surface is FULL-access patterns (`~pattern`); read-only / write-only sub-patterns
/// (`%R~` / `%W~`) are a documented follow-up (a `~pattern` grants both read and write).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyPerms {
    /// `~*` / `allkeys`: every key is allowed; the pattern list is ignored.
    allkeys: bool,
    /// The `~pattern` globs; a key is allowed iff it matches ANY of them (binary-safe glob).
    patterns: Vec<Vec<u8>>,
}

impl KeyPerms {
    /// The no-keys baseline (a fresh user can touch no keys until `~pat`/`allkeys`).
    #[must_use]
    pub fn nokeys() -> KeyPerms {
        KeyPerms::default()
    }

    /// The `allkeys` permission (`~*`), used by the all-permissive default user.
    #[must_use]
    pub fn allkeys() -> KeyPerms {
        KeyPerms {
            allkeys: true,
            patterns: Vec::new(),
        }
    }

    /// `resetkeys`: drop all key permissions back to no-keys.
    pub fn reset(&mut self) {
        self.allkeys = false;
        self.patterns.clear();
    }

    /// Apply `allkeys` (`~*`).
    pub fn set_allkeys(&mut self) {
        self.allkeys = true;
        self.patterns.clear();
    }

    /// Add a `~pattern` glob. A no-op once `allkeys` is set (every key is already allowed).
    pub fn add_pattern(&mut self, pat: &[u8]) {
        if !self.allkeys {
            self.patterns.push(pat.to_vec());
        }
    }

    /// Whether this is exactly `allkeys` (the fast shortcut the enforcement layer checks).
    #[must_use]
    pub fn is_allkeys(&self) -> bool {
        self.allkeys
    }

    /// Whether `key` is allowed: `allkeys`, or it matches at least one `~pattern` (binary-
    /// safe Redis glob). Called only for a key-bearing command, over its few key args.
    #[must_use]
    pub fn allows(&self, key: &[u8]) -> bool {
        self.allkeys || self.patterns.iter().any(|p| glob_match(p, key))
    }

    /// Render back to the Redis ACL key-rule string (`~*` or `~pat1 ~pat2`).
    #[must_use]
    pub fn describe(&self) -> String {
        if self.allkeys {
            return "~*".to_owned();
        }
        self.patterns
            .iter()
            .map(|p| format!("~{}", String::from_utf8_lossy(p)))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// A user's CHANNEL permissions: either `allchannels` (`&*`) or a list of `&pattern` globs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChannelPerms {
    /// `&*` / `allchannels`: every channel allowed.
    allchannels: bool,
    /// The `&pattern` globs; a channel is allowed iff it matches ANY of them.
    patterns: Vec<Vec<u8>>,
}

impl ChannelPerms {
    /// The no-channels baseline.
    #[must_use]
    pub fn nochannels() -> ChannelPerms {
        ChannelPerms::default()
    }

    /// The `allchannels` permission (`&*`), used by the all-permissive default user.
    #[must_use]
    pub fn allchannels() -> ChannelPerms {
        ChannelPerms {
            allchannels: true,
            patterns: Vec::new(),
        }
    }

    /// `resetchannels`: drop all channel permissions.
    pub fn reset(&mut self) {
        self.allchannels = false;
        self.patterns.clear();
    }

    /// Apply `allchannels` (`&*`).
    pub fn set_allchannels(&mut self) {
        self.allchannels = true;
        self.patterns.clear();
    }

    /// Add a `&pattern` glob.
    pub fn add_pattern(&mut self, pat: &[u8]) {
        if !self.allchannels {
            self.patterns.push(pat.to_vec());
        }
    }

    /// Whether this is exactly `allchannels`.
    #[must_use]
    pub fn is_allchannels(&self) -> bool {
        self.allchannels
    }

    /// Whether `channel` is allowed.
    #[must_use]
    pub fn allows(&self, channel: &[u8]) -> bool {
        self.allchannels || self.patterns.iter().any(|p| glob_match(p, channel))
    }

    /// Render back to the Redis ACL channel-rule string (`&*` or `&pat1 &pat2`). An empty
    /// (no-channels) set renders `resetchannels` so a reload reproduces the locked-down set.
    #[must_use]
    pub fn describe(&self) -> String {
        if self.allchannels {
            return "&*".to_owned();
        }
        if self.patterns.is_empty() {
            return "resetchannels".to_owned();
        }
        self.patterns
            .iter()
            .map(|p| format!("&{}", String::from_utf8_lossy(p)))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// An ACL USER: the unit of auth + authz (#106). Holds the name, the enabled bit, the
/// password digests (SHA-256 hex AT REST, never plaintext), the `nopass` flag, and the
/// three permission groups. Cheap to clone; the live registry hands each connection an
/// `Arc<User>` at AUTH so the hot path reads it lock-free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    /// The username (e.g. `default`, `app`).
    pub name: String,
    /// Whether the user is enabled (`on`). A disabled (`off`) user cannot authenticate.
    pub enabled: bool,
    /// Whether the user authenticates with NO password (`nopass`): any password (or none)
    /// is accepted. Mutually exclusive in effect with a non-empty [`Self::passwords`] set
    /// at auth time (nopass short-circuits the password compare).
    pub nopass: bool,
    /// The accepted password SHA-256 HEX digests (AT REST, #65). Empty + not `nopass`
    /// means the user can never authenticate (Redis: a user with no password is unusable).
    pub passwords: Vec<String>,
    /// The command permissions (compiled allow/deny rule list).
    pub commands: CommandPerms,
    /// The key-pattern permissions.
    pub keys: KeyPerms,
    /// The channel-pattern permissions.
    pub channels: ChannelPerms,
}

impl User {
    /// A fresh, locked-down user named `name`: disabled, no password, `-@all`, no keys, no
    /// channels - the Redis `ACL SETUSER <new>` starting point. Rules then layer on top.
    #[must_use]
    pub fn new(name: &str) -> User {
        User {
            name: name.to_owned(),
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            commands: CommandPerms::nocommands(),
            keys: KeyPerms::nokeys(),
            channels: ChannelPerms::nochannels(),
        }
    }

    /// The all-permissive `default` user with NO password (`on nopass ~* &* +@all`): the
    /// no-requirepass byte-identical legacy posture (every connection is this user with
    /// full access).
    #[must_use]
    pub fn default_nopass() -> User {
        User {
            name: "default".to_owned(),
            enabled: true,
            nopass: true,
            passwords: Vec::new(),
            commands: CommandPerms::allcommands(),
            keys: KeyPerms::allkeys(),
            channels: ChannelPerms::allchannels(),
        }
    }

    /// The all-permissive `default` user WITH the given password digest (`on >#hash ~* &*
    /// +@all`): the legacy `requirepass` posture (AUTH <pass> authenticates default with
    /// full access). `pass_hash` is the SHA-256 hex digest at rest.
    #[must_use]
    pub fn default_with_password(pass_hash: String) -> User {
        User {
            name: "default".to_owned(),
            enabled: true,
            nopass: false,
            passwords: vec![pass_hash],
            commands: CommandPerms::allcommands(),
            keys: KeyPerms::allkeys(),
            channels: ChannelPerms::allchannels(),
        }
    }

    /// Whether this user is ALL-PERMISSIVE (`+@all` AND `~*` AND `&*`): the enforcement
    /// fast path. When true the per-command + per-key + per-channel checks are ALL skipped
    /// (the default user passes everything at the cost of this single shortcut), keeping
    /// the no-ACL deployment byte-identical and O(1).
    #[must_use]
    pub fn is_all_permissive(&self) -> bool {
        self.commands.is_allcommands() && self.keys.is_allkeys() && self.channels.is_allchannels()
    }

    /// Verify a candidate plaintext password against this user (constant-time). `nopass`
    /// accepts any guess. Otherwise the guess is hashed and compared, in constant time,
    /// against EVERY stored digest (examining all to avoid leaking which matched). Returns
    /// `true` iff the user is enabled AND the guess is accepted. The plaintext lives only
    /// as `guess` during hashing and is never stored or logged.
    #[must_use]
    pub fn verify_password(&self, guess: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        if self.nopass {
            return true;
        }
        let guess_hash = ironcache_config::sha256_hex(guess);
        // Fold over all digests so the time does not reveal WHICH (or how many) matched.
        let mut any = false;
        for stored in &self.passwords {
            any |= super::ct_eq(guess_hash.as_bytes(), stored.as_bytes());
        }
        any
    }

    /// Whether `cmd_upper` is allowed for this user (the per-command authorization test).
    /// O(1)-ish: the all-permissive shortcut, else the compiled command-rule replay.
    #[must_use]
    pub fn can_run_command(&self, cmd_upper: &[u8]) -> bool {
        self.commands.is_allcommands() || self.commands.allows(cmd_upper)
    }

    /// Whether `(cmd_upper, sub_upper)` is allowed for this user (the per-SUBCOMMAND authorization
    /// test for a container command like CLUSTER). `sub_upper` is `Some(<UPPERCASE subcommand>)`
    /// when the request carries one; `None` falls back to whole-command semantics
    /// ([`Self::can_run_command`]). The `+@all` shortcut still allows everything; otherwise the
    /// compiled rule replay decides on the subcommand's EFFECTIVE categories so a read subcommand
    /// can be granted without the container's `@dangerous` mutators (Redis 7 `+cluster|slots`).
    #[must_use]
    pub fn can_run_command_sub(&self, cmd_upper: &[u8], sub_upper: Option<&[u8]>) -> bool {
        self.commands.is_allcommands() || self.commands.allows_sub(cmd_upper, sub_upper)
    }

    /// Whether `key` is allowed for this user (the per-key authorization test).
    #[must_use]
    pub fn can_access_key(&self, key: &[u8]) -> bool {
        self.keys.allows(key)
    }

    /// Whether `channel` is allowed for this user (the per-channel authorization test).
    #[must_use]
    pub fn can_access_channel(&self, channel: &[u8]) -> bool {
        self.channels.allows(channel)
    }

    /// Render this user back to its Redis aclfile / `ACL LIST` rule line (WITHOUT the
    /// leading `user <name>`): `on`/`off`, each `#<digest>` (or `nopass`), the key rules,
    /// the channel rules, and the command rules - in the order Redis `ACL LIST` emits.
    /// Passwords are emitted as `#<sha256-hex>` (the AT-REST digest), never plaintext.
    #[must_use]
    pub fn describe_rules(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        parts.push(if self.enabled { "on" } else { "off" }.to_owned());
        if self.nopass {
            parts.push("nopass".to_owned());
        } else {
            for d in &self.passwords {
                parts.push(format!("#{d}"));
            }
        }
        // Key rules, then channel rules, then command rules (Redis ACL LIST order).
        let keys = self.keys.describe();
        if keys.is_empty() {
            parts.push("resetkeys".to_owned());
        } else {
            parts.push(keys);
        }
        parts.push(self.channels.describe());
        parts.push(self.commands.describe());
        parts.join(" ")
    }
}

/// The DEFAULT username (`default`): the implicit user every legacy connection authenticates
/// as, and the one that cannot be deleted (`ACL DELUSER default` is refused).
pub const DEFAULT_USER: &str = "default";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nocommands_denies_everything_allcommands_allows() {
        let deny = CommandPerms::nocommands();
        assert!(!deny.allows(b"GET"));
        let allow = CommandPerms::allcommands();
        assert!(allow.allows(b"GET"));
        assert!(allow.is_allcommands());
    }

    #[test]
    fn last_match_wins_allow_one_after_deny_all() {
        let mut p = CommandPerms::nocommands();
        p.allow_command(b"GET");
        assert!(p.allows(b"GET"));
        assert!(!p.allows(b"SET"));
        assert!(!p.is_allcommands());
    }

    #[test]
    fn deny_category_after_allow_all() {
        let mut p = CommandPerms::allcommands();
        p.deny_category(Category::Dangerous);
        assert!(p.allows(b"GET"));
        assert!(p.allows(b"SET"));
        // FLUSHALL is @dangerous -> denied; CONFIG too.
        assert!(!p.allows(b"FLUSHALL"));
        assert!(!p.allows(b"CONFIG"));
        // No longer the absolute allcommands shortcut.
        assert!(!p.is_allcommands());
    }

    #[test]
    fn deny_admin_denies_operability_commands() {
        // PROD-7: a `+@all -@admin` user is denied the operability/introspection admin commands
        // (SLOWLOG / MEMORY / LATENCY) and the existing CLIENT/CONFIG, but can still run data
        // commands. The enforcement layer derives the deny from `category_bits` (tested separately);
        // here we prove the compiled allow-set denies them.
        let mut admin = CommandPerms::allcommands();
        admin.deny_category(Category::Admin);
        assert!(admin.allows(b"GET"));
        for cmd in [
            b"SLOWLOG".as_slice(),
            b"MEMORY",
            b"LATENCY",
            b"CLIENT",
            b"CONFIG",
        ] {
            assert!(!admin.allows(cmd), "{cmd:?} must be denied under -@admin");
        }
        // A `+@all -@dangerous` user is denied SLOWLOG (its RESET wipes the log) + CLIENT + CONFIG,
        // but MEMORY / LATENCY (admin but NOT dangerous) remain allowed.
        let mut dangerous = CommandPerms::allcommands();
        dangerous.deny_category(Category::Dangerous);
        assert!(!dangerous.allows(b"SLOWLOG"));
        assert!(!dangerous.allows(b"CLIENT"));
        assert!(!dangerous.allows(b"CONFIG"));
        assert!(dangerous.allows(b"MEMORY"));
        assert!(dangerous.allows(b"LATENCY"));
    }

    #[test]
    fn key_patterns_glob() {
        let mut k = KeyPerms::nokeys();
        k.add_pattern(b"k:*");
        assert!(k.allows(b"k:1"));
        assert!(!k.allows(b"other:1"));
        assert!(!k.is_allkeys());
        let all = KeyPerms::allkeys();
        assert!(all.allows(b"anything"));
        assert!(all.is_allkeys());
    }

    #[test]
    fn disabled_user_cannot_auth() {
        let mut u = User::new("app");
        u.nopass = true;
        u.enabled = false;
        assert!(!u.verify_password(b"whatever"));
        u.enabled = true;
        assert!(u.verify_password(b"whatever"));
    }

    #[test]
    fn password_verify_constant_time_hash() {
        let mut u = User::new("app");
        u.enabled = true;
        u.passwords.push(ironcache_config::sha256_hex(b"s3cr3t"));
        assert!(u.verify_password(b"s3cr3t"));
        assert!(!u.verify_password(b"wrong"));
    }

    #[test]
    fn all_permissive_shortcut() {
        assert!(User::default_nopass().is_all_permissive());
        let mut u = User::default_nopass();
        u.keys.reset();
        assert!(!u.is_all_permissive());
    }

    #[test]
    fn describe_round_trips_basic_user() {
        let mut u = User::new("app");
        u.enabled = true;
        u.passwords.push(ironcache_config::sha256_hex(b"pw"));
        u.commands.allow_command(b"GET");
        u.keys.add_pattern(b"k:*");
        let line = u.describe_rules();
        assert!(line.starts_with("on "));
        assert!(line.contains('#'));
        assert!(line.contains("~k:*"));
        assert!(line.contains("+get"));
        assert!(line.contains("resetchannels"));
    }

    #[test]
    fn subcommand_grant_allows_read_denies_mutator() {
        // `+@read +@write +@connection +@transaction -@dangerous +cluster|slots +cluster|shards
        // +cluster|nodes`: the three read subcommands are allowed; every CLUSTER mutator is NOPERM.
        let mut p = CommandPerms::nocommands();
        p.allow_category(Category::Read);
        p.allow_category(Category::Write);
        p.allow_category(Category::Connection);
        p.allow_category(Category::Transaction);
        p.deny_category(Category::Dangerous);
        p.allow_subcommand(b"CLUSTER", b"SLOTS");
        p.allow_subcommand(b"CLUSTER", b"SHARDS");
        p.allow_subcommand(b"CLUSTER", b"NODES");

        for sub in [b"SLOTS".as_slice(), b"SHARDS", b"NODES"] {
            assert!(
                p.allows_sub(b"CLUSTER", Some(sub)),
                "CLUSTER {} must be allowed",
                String::from_utf8_lossy(sub)
            );
        }
        for sub in [
            b"ADDSLOTS".as_slice(),
            b"MEET",
            b"SETSLOT",
            b"DELSLOTS",
            b"FORGET",
        ] {
            assert!(
                !p.allows_sub(b"CLUSTER", Some(sub)),
                "CLUSTER {} must be NOPERM (mutator under -@dangerous)",
                String::from_utf8_lossy(sub)
            );
        }
    }

    #[test]
    fn all_minus_dangerous_allows_read_subcommand_denies_mutator() {
        // Redis 7 parity: `+@all -@dangerous` runs CLUSTER SLOTS but is NOPERM on CLUSTER ADDSLOTS.
        let mut p = CommandPerms::allcommands();
        p.deny_category(Category::Dangerous);
        assert!(p.allows_sub(b"CLUSTER", Some(b"SLOTS")));
        assert!(!p.allows_sub(b"CLUSTER", Some(b"ADDSLOTS")));
        // An unrecognized subcommand inherits the container's @dangerous tag -> denied.
        assert!(!p.allows_sub(b"CLUSTER", Some(b"BOGUS")));
    }

    #[test]
    fn bare_cluster_grant_allows_all_subcommands() {
        // A bare `+cluster` grants EVERY subcommand (reads AND mutators); `-cluster` denies all.
        let mut grant = CommandPerms::nocommands();
        grant.allow_command(b"CLUSTER");
        assert!(grant.allows_sub(b"CLUSTER", Some(b"SLOTS")));
        assert!(grant.allows_sub(b"CLUSTER", Some(b"ADDSLOTS")));
        assert!(grant.allows(b"CLUSTER"));

        let mut deny = CommandPerms::allcommands();
        deny.deny_command(b"CLUSTER");
        assert!(!deny.allows_sub(b"CLUSTER", Some(b"SLOTS")));
        assert!(!deny.allows_sub(b"CLUSTER", Some(b"ADDSLOTS")));
    }

    #[test]
    fn subcommand_rules_are_inert_for_other_commands_and_no_sub() {
        // A `+cluster|slots` rule must not leak to other commands or to the no-subcommand path.
        let mut p = CommandPerms::nocommands();
        p.allow_subcommand(b"CLUSTER", b"SLOTS");
        // Other commands unaffected (still denied by the -@all baseline).
        assert!(!p.allows(b"GET"));
        assert!(!p.allows_sub(b"GET", Some(b"SLOTS")));
        // Bare CLUSTER (no subcommand) is NOT granted by a `+cluster|slots` rule.
        assert!(!p.allows(b"CLUSTER"));
        assert!(!p.allows_sub(b"CLUSTER", None));
    }

    #[test]
    fn monitor_shape_grants_slowlog_get_and_client_list_reads_only() {
        // The least-privilege monitoring shape (#367): `-@all +ping +info +slowlog|get
        // +client|list`. The two granted introspection subcommands pass; the destructive
        // siblings (SLOWLOG RESET, CLIENT KILL) and everything else stay denied.
        let mut p = CommandPerms::nocommands();
        p.allow_command(b"PING");
        p.allow_command(b"INFO");
        p.allow_subcommand(b"SLOWLOG", b"GET");
        p.allow_subcommand(b"CLIENT", b"LIST");

        assert!(p.allows(b"PING"));
        assert!(p.allows(b"INFO"));
        assert!(p.allows_sub(b"SLOWLOG", Some(b"GET")));
        assert!(p.allows_sub(b"CLIENT", Some(b"LIST")));
        // The destructive siblings of the granted subcommands are NOPERM.
        assert!(!p.allows_sub(b"SLOWLOG", Some(b"RESET")));
        assert!(!p.allows_sub(b"SLOWLOG", Some(b"LEN")));
        assert!(!p.allows_sub(b"CLIENT", Some(b"KILL")));
        // No leak to the bare containers, to CONFIG, or to the data plane.
        assert!(!p.allows(b"SLOWLOG"));
        assert!(!p.allows(b"CLIENT"));
        assert!(!p.allows_sub(b"CONFIG", Some(b"GET")));
        assert!(!p.allows(b"GET"));
        assert!(!p.allows(b"SET"));
        assert!(!p.allows(b"FLUSHALL"));
    }

    #[test]
    fn describe_round_trips_subcommand_rule() {
        // A subcommand rule renders as `+cluster|slots` (lowercased pipe form) with the implicit
        // `-@all` baseline, so an aclfile SAVE -> LOAD reproduces it.
        let mut u = User::new("svc");
        u.enabled = true;
        u.commands.allow_subcommand(b"CLUSTER", b"SLOTS");
        u.commands.deny_subcommand(b"CLUSTER", b"ADDSLOTS");
        let desc = u.commands.describe();
        assert_eq!(desc, "-@all +cluster|slots -cluster|addslots");
    }
}
