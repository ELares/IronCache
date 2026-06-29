// SPDX-License-Identifier: MIT OR Apache-2.0
//! Command CATEGORIES for the ACL engine (#106, Redis ACL `@category` rules).
//!
//! Redis groups commands into named categories (`@read`, `@write`, `@admin`,
//! `@dangerous`, `@keyspace`, `@fast`, `@slow`, `@connection`, `@string`, ...) so an
//! operator can grant or revoke a whole class at once (`+@read`, `-@dangerous`). This
//! module is the SINGLE SOURCE OF TRUTH mapping an UPPERCASE command token to the set
//! of categories it belongs to.
//!
//! ## How the mapping is derived
//!
//! Two categories are DERIVED from the existing #89 command registry so they can never
//! drift from the command's real semantics:
//! - `@write` iff [`crate::command_spec::CommandSpec::is_write`],
//! - `@read` iff the command touches a key and is NOT a write (a keyed read).
//!
//! The remaining categories (`@admin`, `@dangerous`, `@keyspace`, `@connection`, the
//! per-type `@string`/`@list`/`@hash`/`@set`/`@sortedset`/`@bitmap`/`@hyperloglog`/
//! `@pubsub`/`@transaction`/`@scripting`-absent, and the `@fast`/`@slow` speed split)
//! are explicit tables here, transcribed from the canonical Redis ACL category
//! assignment (src/commands/*.json `acl_categories`). They are a stable Redis fact.
//!
//! ## Determinism / hot path
//!
//! [`category_bits`] is a PURE function of the uppercased token (no I/O, no state, no
//! clock). It is called ONLY when (re)compiling a user's permission bitset on `ACL
//! SETUSER` (rare), never per command: the hot-path per-command check is the already
//! compiled per-command allow bitset (see [`super::perms`]). So the cost of this table
//! is paid at rule-compile time, not on the data path.

use crate::command_spec::{self, CommandClass};

/// The ACL categories IronCache recognizes (a solid v1 subset of the Redis set). Each is
/// a bit in a [`CategorySet`] so a command's membership is a cheap bit test and a user's
/// `+@cat`/`-@cat` rule is a bit operation. The names map 1:1 to the `@<name>` spelling
/// `ACL CAT` lists and `ACL SETUSER ... +@<name>` parses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    /// Commands that READ the keyspace (keyed, non-write): GET, MGET, LRANGE, ...
    Read,
    /// Commands that WRITE the keyspace (the registry `is_write` flag): SET, DEL, LPUSH, ...
    Write,
    /// Administrative / operator commands: CONFIG, CLIENT, CLUSTER, INFO, ACL, SHUTDOWN, ...
    Admin,
    /// DANGEROUS commands an unprivileged user should not run: FLUSHALL, FLUSHDB, CONFIG,
    /// SHUTDOWN, CLUSTER, SWAPDB, KEYS, DEBUG-class, SAVE/BGSAVE, ACL, ...
    Dangerous,
    /// Keyspace-management commands: DEL, EXISTS, EXPIRE, TTL, RENAME, KEYS, SCAN, TYPE, ...
    Keyspace,
    /// Connection / handshake commands: PING, ECHO, HELLO, AUTH, SELECT, RESET, QUIT, CLIENT.
    Connection,
    /// Pub/Sub commands: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB.
    Pubsub,
    /// Transaction commands: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
    Transaction,
    /// String type commands.
    String,
    /// List type commands.
    List,
    /// Hash type commands.
    Hash,
    /// Set type commands.
    Set,
    /// Sorted-set type commands.
    Sortedset,
    /// Bitmap commands.
    Bitmap,
    /// HyperLogLog commands.
    Hyperloglog,
    /// O(1)/O(log N) "fast" commands (Redis ACL `@fast`).
    Fast,
    /// O(N)-or-worse "slow" commands (Redis ACL `@slow`). Every command is exactly one of
    /// `@fast`/`@slow` in Redis; we mirror that.
    Slow,
}

impl Category {
    /// The lowercase `@<name>` spelling WITHOUT the leading `@` (e.g. `"read"`), used by
    /// `ACL CAT` output and `+@<name>` parsing.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Category::Read => "read",
            Category::Write => "write",
            Category::Admin => "admin",
            Category::Dangerous => "dangerous",
            Category::Keyspace => "keyspace",
            Category::Connection => "connection",
            Category::Pubsub => "pubsub",
            Category::Transaction => "transaction",
            Category::String => "string",
            Category::List => "list",
            Category::Hash => "hash",
            Category::Set => "set",
            Category::Sortedset => "sortedset",
            Category::Bitmap => "bitmap",
            Category::Hyperloglog => "hyperloglog",
            Category::Fast => "fast",
            Category::Slow => "slow",
        }
    }

    /// The single bit this category occupies in a [`CategorySet`].
    #[must_use]
    fn bit(self) -> u32 {
        1u32 << (self as u32)
    }

    /// Parse a lowercase category name (no leading `@`) into a [`Category`], case-insensitively.
    /// `None` for an unrecognized name (the parser rejects an unknown `@cat` rule).
    #[must_use]
    pub fn from_name(name: &str) -> Option<Category> {
        Some(match name.to_ascii_lowercase().as_str() {
            "read" => Category::Read,
            "write" => Category::Write,
            "admin" => Category::Admin,
            "dangerous" => Category::Dangerous,
            "keyspace" => Category::Keyspace,
            "connection" => Category::Connection,
            "pubsub" => Category::Pubsub,
            "transaction" => Category::Transaction,
            "string" => Category::String,
            "list" => Category::List,
            "hash" => Category::Hash,
            "set" => Category::Set,
            "sortedset" => Category::Sortedset,
            "bitmap" => Category::Bitmap,
            "hyperloglog" => Category::Hyperloglog,
            "fast" => Category::Fast,
            "slow" => Category::Slow,
            _ => return None,
        })
    }

    /// Every category, in `ACL CAT` listing order.
    #[must_use]
    pub fn all() -> &'static [Category] {
        &[
            Category::Read,
            Category::Write,
            Category::Admin,
            Category::Dangerous,
            Category::Keyspace,
            Category::Connection,
            Category::Pubsub,
            Category::Transaction,
            Category::String,
            Category::List,
            Category::Hash,
            Category::Set,
            Category::Sortedset,
            Category::Bitmap,
            Category::Hyperloglog,
            Category::Fast,
            Category::Slow,
        ]
    }
}

/// A set of [`Category`] flags as a packed bitset (one `u32` is ample for the v1 list).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CategorySet(u32);

impl CategorySet {
    /// The empty set.
    #[must_use]
    pub const fn empty() -> CategorySet {
        CategorySet(0)
    }

    /// Add `c` to the set.
    fn insert(&mut self, c: Category) {
        self.0 |= c.bit();
    }

    /// Whether `c` is in the set (a single bit test, the cheap query the compiled
    /// per-user category test uses).
    #[must_use]
    pub fn contains(self, c: Category) -> bool {
        self.0 & c.bit() != 0
    }
}

/// The categories the per-type membership is keyed on, by the leading byte(s) of the
/// command. Returns the type category for a known data command, or `None` for a
/// command with no data-type category (admin/connection/etc.). This is an explicit
/// transcription of Redis's per-type `acl_categories`.
// Flat, canonical Redis const tables (one per data type) plus a single match: splitting it would
// scatter the single-source-of-truth transcription with no readability gain, so the line count is
// allowed here.
#[allow(clippy::too_many_lines)]
fn type_category(cmd: &[u8]) -> Option<Category> {
    // The string / numeric family.
    const STRING: &[&[u8]] = &[
        b"GET",
        b"SET",
        b"SETNX",
        b"SETEX",
        b"PSETEX",
        b"GETSET",
        b"GETEX",
        b"GETDEL",
        b"GETRANGE",
        b"SUBSTR",
        b"SETRANGE",
        b"STRLEN",
        b"APPEND",
        b"INCR",
        b"DECR",
        b"INCRBY",
        b"DECRBY",
        b"INCRBYFLOAT",
        b"MGET",
        b"MSET",
        b"MSETNX",
    ];
    const LIST: &[&[u8]] = &[
        b"LPUSH",
        b"RPUSH",
        b"LPUSHX",
        b"RPUSHX",
        b"LPOP",
        b"RPOP",
        b"LLEN",
        b"LRANGE",
        b"LINDEX",
        b"LSET",
        b"LINSERT",
        b"LREM",
        b"LTRIM",
        b"LMOVE",
        b"RPOPLPUSH",
        b"LPOS",
        b"LMPOP",
    ];
    const HASH: &[&[u8]] = &[
        b"HSET",
        b"HMSET",
        b"HSETNX",
        b"HGET",
        b"HMGET",
        b"HDEL",
        b"HGETALL",
        b"HKEYS",
        b"HVALS",
        b"HLEN",
        b"HEXISTS",
        b"HSTRLEN",
        b"HINCRBY",
        b"HINCRBYFLOAT",
        b"HRANDFIELD",
        b"HSCAN",
        b"HEXPIRE",
        b"HPEXPIRE",
        b"HEXPIREAT",
        b"HPEXPIREAT",
        b"HTTL",
        b"HPTTL",
        b"HEXPIRETIME",
        b"HPEXPIRETIME",
        b"HPERSIST",
    ];
    const SET: &[&[u8]] = &[
        b"SADD",
        b"SREM",
        b"SMEMBERS",
        b"SISMEMBER",
        b"SMISMEMBER",
        b"SCARD",
        b"SPOP",
        b"SRANDMEMBER",
        b"SMOVE",
        b"SINTER",
        b"SUNION",
        b"SDIFF",
        b"SINTERCARD",
        b"SINTERSTORE",
        b"SUNIONSTORE",
        b"SDIFFSTORE",
        b"SSCAN",
    ];
    const ZSET: &[&[u8]] = &[
        b"ZADD",
        b"ZINCRBY",
        b"ZREM",
        b"ZSCORE",
        b"ZMSCORE",
        b"ZCARD",
        b"ZCOUNT",
        b"ZLEXCOUNT",
        b"ZRANK",
        b"ZREVRANK",
        b"ZRANGE",
        b"ZREVRANGE",
        b"ZRANGEBYSCORE",
        b"ZREVRANGEBYSCORE",
        b"ZRANGEBYLEX",
        b"ZREVRANGEBYLEX",
        b"ZRANGESTORE",
        b"ZPOPMIN",
        b"ZPOPMAX",
        b"ZMPOP",
        b"ZRANDMEMBER",
        b"ZREMRANGEBYRANK",
        b"ZREMRANGEBYSCORE",
        b"ZREMRANGEBYLEX",
        b"ZSCAN",
        b"ZUNION",
        b"ZINTER",
        b"ZDIFF",
        b"ZINTERCARD",
        b"ZUNIONSTORE",
        b"ZINTERSTORE",
        b"ZDIFFSTORE",
    ];
    const BITMAP: &[&[u8]] = &[
        b"SETBIT",
        b"GETBIT",
        b"BITCOUNT",
        b"BITPOS",
        b"BITOP",
        b"BITFIELD",
        b"BITFIELD_RO",
    ];
    const HLL: &[&[u8]] = &[b"PFADD", b"PFCOUNT", b"PFMERGE"];

    if STRING.contains(&cmd) {
        Some(Category::String)
    } else if LIST.contains(&cmd) {
        Some(Category::List)
    } else if HASH.contains(&cmd) {
        Some(Category::Hash)
    } else if SET.contains(&cmd) {
        Some(Category::Set)
    } else if ZSET.contains(&cmd) {
        Some(Category::Sortedset)
    } else if BITMAP.contains(&cmd) {
        Some(Category::Bitmap)
    } else if HLL.contains(&cmd) {
        Some(Category::Hyperloglog)
    } else {
        None
    }
}

/// The ADMIN command set (Redis ACL `@admin`): operator / introspection commands that
/// manage the server rather than the keyspace.
fn is_admin(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"CONFIG"
            | b"CLIENT"
            | b"CLUSTER"
            | b"INFO"
            | b"ACL"
            | b"SHUTDOWN"
            | b"SAVE"
            | b"BGSAVE"
            | b"LASTSAVE"
            | b"SWAPDB"
            | b"COMMAND"
            // PROD-7 operability / introspection containers.
            | b"SLOWLOG"
            | b"LATENCY"
            | b"MEMORY"
    )
}

/// The DANGEROUS command set (Redis ACL `@dangerous`): commands that can wipe data, leak
/// the keyspace, or reconfigure the server, which an unprivileged user typically should
/// not run. `-@dangerous` is the canonical "lock down" rule.
fn is_dangerous(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"FLUSHALL"
            | b"FLUSHDB"
            | b"CONFIG"
            | b"SHUTDOWN"
            | b"CLUSTER"
            | b"CLIENT"
            | b"ACL"
            | b"INFO"
            | b"KEYS"
            | b"SWAPDB"
            | b"SAVE"
            | b"BGSAVE"
            | b"LASTSAVE"
            | b"SORT"
            | b"SORT_RO"
            | b"MIGRATE"
            | b"RESTORE"
            | b"MOVE"
            // PROD-7: SLOWLOG (RESET wipes the log) and CLIENT (KILL closes connections) carry
            // @dangerous in Redis; CLIENT is already @dangerous above. MEMORY/LATENCY are @admin but
            // NOT @dangerous in Redis, so they are intentionally only in is_admin.
            | b"SLOWLOG"
    )
}

/// The KEYSPACE command set (Redis ACL `@keyspace`): generic key-management commands.
fn is_keyspace(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"DEL"
            | b"UNLINK"
            | b"EXISTS"
            | b"TYPE"
            | b"KEYS"
            | b"SCAN"
            | b"DBSIZE"
            | b"RANDOMKEY"
            | b"RENAME"
            | b"RENAMENX"
            | b"COPY"
            | b"MOVE"
            | b"TOUCH"
            | b"FLUSHDB"
            | b"FLUSHALL"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            | b"TTL"
            | b"PTTL"
            | b"EXPIRETIME"
            | b"PEXPIRETIME"
            | b"PERSIST"
            | b"OBJECT"
            | b"SWAPDB"
    )
}

/// The CONNECTION command set (Redis ACL `@connection`): handshake / connection control.
fn is_connection(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"PING"
            | b"ECHO"
            | b"HELLO"
            | b"AUTH"
            | b"SELECT"
            | b"RESET"
            | b"QUIT"
            | b"CLIENT"
            | b"COMMAND"
            | b"READONLY"
            | b"READWRITE"
    )
}

/// The PUBSUB command set (Redis ACL `@pubsub`).
fn is_pubsub(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PUBLISH" | b"PUBSUB"
    )
}

/// The TRANSACTION command set (Redis ACL `@transaction`).
fn is_transaction(cmd: &[u8]) -> bool {
    matches!(cmd, b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH")
}

/// The FAST command set (Redis ACL `@fast`, O(1)/O(log N)). Every command is exactly one
/// of `@fast`/`@slow`; this is the explicit `@fast` list, everything else is `@slow`. It
/// follows Redis's per-command speed flag closely for the v1 surface.
fn is_fast(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        // O(1) string / numeric.
        b"GET" | b"SET"
            | b"SETNX"
            | b"SETEX"
            | b"PSETEX"
            | b"GETSET"
            | b"GETEX"
            // GETDEL is O(1) (Redis flags it @fast). GETRANGE/SUBSTR/SETRANGE/MSETNX are
            // O(N) -> @slow (the default for any command not listed here).
            | b"GETDEL"
            | b"STRLEN"
            | b"APPEND"
            | b"INCR"
            | b"DECR"
            | b"INCRBY"
            | b"DECRBY"
            | b"INCRBYFLOAT"
            | b"SETBIT"
            | b"GETBIT"
            // O(1) keyspace / ttl.
            | b"EXISTS"
            | b"TYPE"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            | b"TTL"
            | b"PTTL"
            | b"EXPIRETIME"
            | b"PEXPIRETIME"
            | b"PERSIST"
            | b"MOVE"
            | b"RENAMENX"
            // O(1) list ends.
            | b"LPUSH"
            | b"RPUSH"
            | b"LPUSHX"
            | b"RPUSHX"
            | b"LPOP"
            | b"RPOP"
            | b"LLEN"
            | b"LINDEX"
            | b"LSET"
            // O(1) hash.
            | b"HSET"
            | b"HSETNX"
            | b"HGET"
            | b"HDEL"
            | b"HLEN"
            | b"HEXISTS"
            | b"HSTRLEN"
            | b"HINCRBY"
            | b"HINCRBYFLOAT"
            // Hash field TTL (#408): O(N) over the FIELDS count, like HDEL/HMGET (Redis @fast).
            | b"HEXPIRE"
            | b"HPEXPIRE"
            | b"HEXPIREAT"
            | b"HPEXPIREAT"
            | b"HTTL"
            | b"HPTTL"
            | b"HEXPIRETIME"
            | b"HPEXPIRETIME"
            | b"HPERSIST"
            // O(1) set.
            | b"SADD"
            | b"SREM"
            | b"SISMEMBER"
            | b"SMISMEMBER"
            | b"SCARD"
            // O(log N) sortedset.
            | b"ZADD"
            | b"ZINCRBY"
            | b"ZREM"
            | b"ZSCORE"
            | b"ZMSCORE"
            | b"ZCARD"
            | b"ZRANK"
            | b"ZREVRANK"
            // O(1) hll add (amortized).
            | b"PFADD"
            // O(1) connection / control.
            | b"PING"
            | b"ECHO"
            | b"LOLWUT"
            | b"HELLO"
            | b"AUTH"
            | b"SELECT"
            | b"RESET"
            | b"QUIT"
            | b"MULTI"
            | b"EXEC"
            | b"DISCARD"
            | b"WATCH"
            | b"UNWATCH"
            | b"READONLY"
            | b"READWRITE"
            | b"ASKING"
            | b"DBSIZE"
            | b"TOUCH"
    )
}

/// Compute the full [`CategorySet`] for an UPPERCASE command token. PURE; called only at
/// rule-compile time (`ACL SETUSER`), never per command (the hot path reads the compiled
/// per-command allow bitset). An unknown token (no registry entry, no table membership)
/// yields the empty set.
#[must_use]
pub fn category_bits(cmd_upper: &[u8]) -> CategorySet {
    let mut set = CategorySet::empty();

    // @read / @write are DERIVED from the #89 registry so they never drift from the
    // command's real semantics: @write iff the registry marks it a write; @read iff it is
    // a keyed command that is NOT a write (a keyed read). A non-keyed admin/connection
    // command is neither @read nor @write, matching Redis.
    if let Some(spec) = command_spec::spec_of(cmd_upper) {
        let keyed = matches!(
            spec.class,
            CommandClass::KeyedSingle | CommandClass::KeyedMulti
        );
        // F3 (PFCOUNT @write parity): real Redis marks PFCOUNT `@write` (its `write` command flag
        // is set) because it MAY rewrite the cached cardinality on the key, even though IronCache's
        // ROUTING keeps it `is_write == false` (so replica-read / denyoom / MOVED routing are
        // unchanged -- the security review found those correct and not to touch). This per-command
        // ACL-CATEGORY override (folded into the @write condition) is the smallest change that
        // gives `+@read -@write` users the Redis-correct (deny) result without disturbing routing.
        let acl_write = spec.is_write || cmd_upper == b"PFCOUNT";
        if acl_write {
            set.insert(Category::Write);
        } else if keyed {
            set.insert(Category::Read);
        }
        // A WholeKeyspace read (KEYS/SCAN/DBSIZE/RANDOMKEY) is also @read in Redis even
        // though it owns no single key; classify those as @read too.
        if !spec.is_write && matches!(spec.class, CommandClass::WholeKeyspace) {
            set.insert(Category::Read);
        }
    }

    if let Some(tc) = type_category(cmd_upper) {
        set.insert(tc);
    }
    if is_admin(cmd_upper) {
        set.insert(Category::Admin);
    }
    if is_dangerous(cmd_upper) {
        set.insert(Category::Dangerous);
    }
    if is_keyspace(cmd_upper) {
        set.insert(Category::Keyspace);
    }
    if is_connection(cmd_upper) {
        set.insert(Category::Connection);
    }
    if is_pubsub(cmd_upper) {
        set.insert(Category::Pubsub);
    }
    if is_transaction(cmd_upper) {
        set.insert(Category::Transaction);
    }
    // Every known command is exactly one of @fast / @slow.
    if command_spec::spec_of(cmd_upper).is_some() || is_pubsub(cmd_upper) {
        if is_fast(cmd_upper) {
            set.insert(Category::Fast);
        } else {
            set.insert(Category::Slow);
        }
    }

    set
}

/// Compute the EFFECTIVE [`CategorySet`] for a recognized `(cmd_upper, sub_upper)` SUBCOMMAND
/// pair (per-subcommand ACL, Redis 7 `+cluster|slots`). Unlike [`category_bits`], which returns
/// the CONTAINER's whole-command categories (e.g. `CLUSTER` = `{Admin, Dangerous, Slow}`), this
/// reads the per-subcommand tags off the [`command_spec::SubcommandSpec`] so a read subcommand
/// (CLUSTER SLOTS) is `{Slow}` while a mutator (CLUSTER ADDSLOTS) is `{Admin, Dangerous, Slow}`.
///
/// Derivation:
/// * @write iff [`command_spec::SubcommandSpec::is_write`] (a CLUSTER subcommand touches no key,
///   so neither @write nor @read applies today; the field is here for a future writing
///   subcommand). There is no @read for a subcommand: a non-write CLUSTER subcommand owns no key.
/// * @admin / @dangerous straight from the spec flags.
/// * @slow ALWAYS (none of the CLUSTER subcommands are O(1) `@fast`); this preserves the
///   fast XOR slow totality the `category_bits` partition guarantees, so a `-@slow` rule reaches
///   them exactly as it would the container.
///
/// Returns the EMPTY set for an unrecognized pair (the caller falls back to [`category_bits`] of
/// the container in that case, so an unknown subcommand inherits the container's @dangerous tag).
/// PURE; called only at authorization time for a container command carrying a subcommand.
#[must_use]
pub fn subcommand_category_bits(cmd_upper: &[u8], sub_upper: &[u8]) -> CategorySet {
    let mut set = CategorySet::empty();
    let Some(spec) = command_spec::subcommand_spec(cmd_upper, sub_upper) else {
        return set;
    };
    // @write iff the subcommand mutates the keyspace; CLUSTER subcommands never do, so this stays
    // unset for them and they are neither @read nor @write (they own no key).
    if spec.is_write {
        set.insert(Category::Write);
    }
    if spec.admin {
        set.insert(Category::Admin);
    }
    if spec.dangerous {
        set.insert(Category::Dangerous);
    }
    // No CLUSTER subcommand is @fast, so each is @slow (totality preserved: exactly one of
    // fast/slow, matching the per-command partition).
    set.insert(Category::Slow);
    set
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write_are_derived_from_registry() {
        let g = category_bits(b"GET");
        assert!(g.contains(Category::Read));
        assert!(!g.contains(Category::Write));
        let s = category_bits(b"SET");
        assert!(s.contains(Category::Write));
        assert!(!s.contains(Category::Read));
    }

    #[test]
    fn pfcount_is_write_for_acl_parity() {
        // F3: PFCOUNT is @write (Redis parity -- it may rewrite the cached cardinality), even
        // though it stays a non-write for ROUTING. So a `+@read -@write` user is denied PFCOUNT.
        let pf = category_bits(b"PFCOUNT");
        assert!(pf.contains(Category::Write), "PFCOUNT must be @write");
        assert!(!pf.contains(Category::Read), "PFCOUNT must not be @read");
        // It is still classified by type as @hyperloglog.
        assert!(pf.contains(Category::Hyperloglog));
        // The routing classification is UNCHANGED (PFCOUNT is not a write for routing).
        assert!(!crate::command_spec::is_write(b"PFCOUNT"));
    }

    #[test]
    fn dangerous_and_admin_membership() {
        assert!(category_bits(b"FLUSHALL").contains(Category::Dangerous));
        assert!(category_bits(b"CONFIG").contains(Category::Admin));
        assert!(category_bits(b"CONFIG").contains(Category::Dangerous));
        assert!(category_bits(b"SHUTDOWN").contains(Category::Dangerous));
        // GET/SET are neither admin nor dangerous.
        assert!(!category_bits(b"GET").contains(Category::Dangerous));
        assert!(!category_bits(b"SET").contains(Category::Admin));
    }

    #[test]
    fn operability_commands_are_admin_and_slow() {
        // PROD-7: SLOWLOG / MEMORY / LATENCY are @admin + @slow; SLOWLOG (RESET wipes the log) is
        // also @dangerous, so a `-@admin` or `-@dangerous` user is denied them. CLIENT is already
        // @admin + @dangerous.
        for &c in &[b"SLOWLOG".as_slice(), b"MEMORY", b"LATENCY"] {
            let bits = category_bits(c);
            assert!(bits.contains(Category::Admin), "{c:?} must be @admin");
            assert!(bits.contains(Category::Slow), "{c:?} must be @slow");
        }
        assert!(category_bits(b"SLOWLOG").contains(Category::Dangerous));
        // MEMORY / LATENCY are @admin but NOT @dangerous (Redis parity).
        assert!(!category_bits(b"MEMORY").contains(Category::Dangerous));
        assert!(!category_bits(b"LATENCY").contains(Category::Dangerous));
    }

    #[test]
    fn type_categories() {
        assert!(category_bits(b"LPUSH").contains(Category::List));
        assert!(category_bits(b"HSET").contains(Category::Hash));
        assert!(category_bits(b"SADD").contains(Category::Set));
        assert!(category_bits(b"ZADD").contains(Category::Sortedset));
        assert!(category_bits(b"SETBIT").contains(Category::Bitmap));
        assert!(category_bits(b"PFADD").contains(Category::Hyperloglog));
        assert!(category_bits(b"GET").contains(Category::String));
    }

    #[test]
    fn fast_slow_partition_is_total_for_known_commands() {
        for &name in &crate::command_spec::tests::all_registry_names() {
            // Internal coordinator verbs are not client categories; skip the __IC* ones.
            if name.starts_with(b"__") {
                continue;
            }
            let bits = category_bits(name);
            let fast = bits.contains(Category::Fast);
            let slow = bits.contains(Category::Slow);
            assert!(
                fast ^ slow,
                "command {} must be exactly one of @fast/@slow (fast={fast}, slow={slow})",
                String::from_utf8_lossy(name)
            );
        }
    }

    #[test]
    fn names_round_trip() {
        for &c in Category::all() {
            assert_eq!(Category::from_name(c.name()), Some(c));
        }
        assert_eq!(Category::from_name("nope"), None);
    }

    #[test]
    fn subcommand_categories_split_reads_from_mutators() {
        // A read subcommand (CLUSTER SLOTS): @slow only -- NOT @admin / @dangerous / @write, so it
        // survives a `-@dangerous` carve-out.
        let slots = subcommand_category_bits(b"CLUSTER", b"SLOTS");
        assert!(slots.contains(Category::Slow));
        assert!(!slots.contains(Category::Admin));
        assert!(!slots.contains(Category::Dangerous));
        assert!(!slots.contains(Category::Write));
        assert!(!slots.contains(Category::Read));

        // A mutator subcommand (CLUSTER ADDSLOTS): @admin + @dangerous + @slow, so `-@dangerous`
        // denies it. Same for MEET / SETSLOT.
        for sub in [
            b"ADDSLOTS".as_slice(),
            b"MEET",
            b"SETSLOT",
            b"FAILOVER",
            b"RESET",
        ] {
            let bits = subcommand_category_bits(b"CLUSTER", sub);
            assert!(bits.contains(Category::Admin), "{sub:?} must be @admin");
            assert!(
                bits.contains(Category::Dangerous),
                "{sub:?} must be @dangerous"
            );
            assert!(bits.contains(Category::Slow), "{sub:?} must be @slow");
            // Exactly one of fast/slow (totality preserved): never @fast.
            assert!(!bits.contains(Category::Fast), "{sub:?} must not be @fast");
        }

        // An unrecognized pair yields the EMPTY set (caller falls back to category_bits).
        assert_eq!(
            subcommand_category_bits(b"CLUSTER", b"BOGUS"),
            CategorySet::empty()
        );
        assert_eq!(
            subcommand_category_bits(b"GET", b"SLOTS"),
            CategorySet::empty()
        );

        // CRITICAL INVARIANT: the bare-CLUSTER whole-command categories are UNCHANGED.
        let cluster = category_bits(b"CLUSTER");
        assert!(cluster.contains(Category::Admin));
        assert!(cluster.contains(Category::Dangerous));
        assert!(cluster.contains(Category::Slow));
    }

    #[test]
    fn config_client_subcommand_categories_match_redis() {
        // CONFIG: all four operative subcommands are @admin + @dangerous + @slow; HELP is @slow only.
        for sub in [b"GET".as_slice(), b"SET", b"REWRITE", b"RESETSTAT"] {
            let bits = subcommand_category_bits(b"CONFIG", sub);
            assert!(bits.contains(Category::Admin), "config|{sub:?} @admin");
            assert!(
                bits.contains(Category::Dangerous),
                "config|{sub:?} @dangerous"
            );
            assert!(bits.contains(Category::Slow), "config|{sub:?} @slow");
            assert!(!bits.contains(Category::Write), "config|{sub:?} not @write");
        }
        let help = subcommand_category_bits(b"CONFIG", b"HELP");
        assert!(help.contains(Category::Slow));
        assert!(!help.contains(Category::Admin));
        assert!(!help.contains(Category::Dangerous));

        // CLIENT reads (@slow, non-dangerous): a `-@dangerous` user keeps them when granted.
        for sub in [
            b"ID".as_slice(),
            b"GETNAME",
            b"SETNAME",
            b"SETINFO",
            b"INFO",
            b"NO-TOUCH",
        ] {
            let bits = subcommand_category_bits(b"CLIENT", sub);
            assert!(bits.contains(Category::Slow), "client|{sub:?} @slow");
            assert!(!bits.contains(Category::Admin), "client|{sub:?} not @admin");
            assert!(
                !bits.contains(Category::Dangerous),
                "client|{sub:?} not @dangerous"
            );
        }
        // CLIENT privileged (@admin + @dangerous): denied by `-@dangerous`. NO-EVICT IS dangerous,
        // while its look-alike NO-TOUCH (above) is NOT -- the non-obvious Redis split.
        for sub in [
            b"LIST".as_slice(),
            b"KILL",
            b"PAUSE",
            b"UNPAUSE",
            b"NO-EVICT",
        ] {
            let bits = subcommand_category_bits(b"CLIENT", sub);
            assert!(bits.contains(Category::Admin), "client|{sub:?} @admin");
            assert!(
                bits.contains(Category::Dangerous),
                "client|{sub:?} @dangerous"
            );
            assert!(!bits.contains(Category::Fast), "client|{sub:?} not @fast");
        }

        // CRITICAL INVARIANT: the bare-CONFIG / bare-CLIENT whole-command categories are UNCHANGED
        // (the no-subcommand path + every existing aclfile stay byte-identical).
        let config = category_bits(b"CONFIG");
        assert!(config.contains(Category::Admin));
        assert!(config.contains(Category::Dangerous));
        assert!(config.contains(Category::Slow));
        assert!(!config.contains(Category::Connection)); // CONFIG is not @connection in Redis
        let client = category_bits(b"CLIENT");
        assert!(client.contains(Category::Admin));
        assert!(client.contains(Category::Dangerous));
        assert!(client.contains(Category::Connection));
        assert!(client.contains(Category::Slow));
    }
}
