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

use super::categories::{Category, CategorySet, category_bits};
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

    /// Whether the permission is EXACTLY `+@all` (the all-permissive shortcut), so the
    /// enforcement layer can skip the per-command replay entirely for the default user.
    #[must_use]
    pub fn is_allcommands(&self) -> bool {
        self.rules == [CmdRule::AllowAll]
    }

    /// Whether `cmd_upper` is allowed under these permissions. Replays the rule list in
    /// order; the LAST matching rule wins (Redis "last match wins"). The default (no rule
    /// matches) is DENY, matching Redis's `-@all` baseline. `cmd_cat` is the command's
    /// precomputed [`CategorySet`] (the caller passes it so the hot path computes the
    /// command's categories at most once).
    #[must_use]
    fn allows_with(&self, cmd_upper: &[u8], cmd_cat: CategorySet) -> bool {
        let mut allowed = false;
        for rule in &self.rules {
            match rule {
                CmdRule::AllowAll => allowed = true,
                CmdRule::DenyAll => allowed = false,
                CmdRule::AllowCat(c) => {
                    if cmd_cat.contains(*c) {
                        allowed = true;
                    }
                }
                CmdRule::DenyCat(c) => {
                    if cmd_cat.contains(*c) {
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
            }
        }
        allowed
    }

    /// Whether `cmd_upper` is allowed (computes the command's categories then replays the
    /// rules). The per-command entry point used by enforcement.
    #[must_use]
    pub fn allows(&self, cmd_upper: &[u8]) -> bool {
        self.allows_with(cmd_upper, category_bits(cmd_upper))
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
}
