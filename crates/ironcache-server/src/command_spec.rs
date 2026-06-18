// SPDX-License-Identifier: MIT OR Apache-2.0
//! The SINGLE source of truth for per-command DATA attributes (#89).
//!
//! Before this module, a command's metadata was spread across SEVERAL hand-synced
//! per-command tables that had to be edited in lockstep: the queue-time arity table
//! ([`crate::cmd_txn::arity_of`]), the cross-shard routing class
//! ([`crate::route::classify`]), the key-extraction pattern
//! ([`crate::route::command_keys`]), the `maxmemory` denyoom set
//! ([`crate::admission::is_denyoom`]), and the MULTI queue-gate control set
//! (`dispatch.rs`). Keeping six tables in agreement by hand (guarded only by a dual
//! 148-entry cross-check array) was the worst hand-sync debt in the server.
//!
//! This module collapses those DATA attributes into ONE [`CommandSpec`] per command,
//! looked up by [`spec_of`]. The legacy functions are now THIN WRAPPERS that read this
//! registry, so their call sites and signatures are unchanged but they can no longer
//! drift from each other: there is exactly one place to edit a command's arity, class,
//! key spec, denyoom flag, or control flag.
//!
//! ## What stays a match arm (NOT data)
//!
//! The dispatch HANDLER (cmd -> the function that runs the command) cannot be const
//! data: the handlers have varied signatures (some take `wheel`, some draw an RNG seed,
//! some take `ctx`), so the dispatch match arms in `dispatch.rs` STAY as match arms.
//! This registry is the source for every command's DATA attributes; the dispatch
//! handler match is the ONE remaining hand-sync, and it is cross-checked against this
//! registry by `crate::cmd_txn::tests::table_covers_every_dispatch_arm` (a single
//! registry-vs-dispatch-arm check, which replaces the old dual hand-listed arrays).
//!
//! ## Determinism (ADR-0003)
//!
//! [`spec_of`] is a PURE function of the UPPERCASED command token: no I/O, no state, no
//! clock, no RNG. The attributes are transcribed from the canonical Redis command table
//! (src/commands.def) for arity and from the IronCache routing/admission semantics for
//! the rest.

#![forbid(unsafe_code)]

use crate::route::KeySpec;
use ironcache_protocol::Request;

/// The INTERNAL token the cross-shard coordinator uses to fan a PUBLISH out to every shard's
/// LOCAL subscriber table (SERVER_PUSH.md #20 / COORDINATOR.md #107, PR 91a). NOT a client
/// command: the serve-loop router gates it (like the `__ICSTORE*` dest-write verbs) so a
/// client sending it gets `unknown command`; only the coordinator issues it (broadcasting
/// `__ICPUBLISH <channel> <payload>` to peer shards, each of which delivers to its local
/// subscribers and returns the local receiver count). It is in the [`spec_of`] registry so
/// the registry-vs-dispatch cross-check stays exact and `classify` returns `AlwaysHome` (it
/// has no routable key; the coordinator dispatches it directly, never through the router's
/// keyed branches).
pub const ICPUBLISH: &[u8] = b"__ICPUBLISH";

/// The INTERNAL token the cross-shard coordinator uses to gather PUBSUB introspection from every
/// shard's LOCAL subscription table (SERVER_PUSH.md #20 / COORDINATOR.md #107, PR 91b). NOT a
/// client command: the serve-loop router gates it (like `__ICPUBLISH` / the `__ICSTORE*` verbs)
/// so a client sending it gets `unknown command`; only the coordinator issues it (broadcasting
/// `__ICPUBSUB <subcommand> [args]` to peer shards, each of which returns its LOCAL partial --
/// channel names / per-channel counts / pattern names -- which the home core merges per
/// subcommand). It is in the [`spec_of`] registry so the registry-vs-dispatch cross-check stays
/// exact and `classify` returns `AlwaysHome` (it has no routable key; the coordinator dispatches
/// it directly, never through the router's keyed branches).
pub const ICPUBSUB: &[u8] = b"__ICPUBSUB";

/// The INTERNAL token the cross-shard coordinator uses to ask the shard that OWNS a key whether
/// that key is PRESENT and LIVE (HA-6 online slot migration on a MULTI-SHARD node, COORDINATOR.md
/// #107). NOT a client command: the serve-loop router gates it (like `__ICPUBLISH` / the
/// `__ICSTORE*` verbs) so a client sending it gets `unknown command`; only the coordinator issues
/// it (`__ICEXISTS <key>` to the key's owner shard, which replies `:1` / `:0` from a pure
/// [`crate::route`]-routed `contains_live` read -- never reaping, never folding a counter).
///
/// Unlike `__ICPUBLISH` / `__ICPUBSUB`, this verb is DELIBERATELY ABSENT from the [`spec_of`]
/// registry: it is dispatched DIRECTLY by the coordinator's presence hop and is NEVER seen by
/// `classify` or the dispatch-arm match (the migration source builds + sends it itself, and the
/// owner shard's drain loop answers it in `run_remote` BEFORE any keyed dispatch). `spec_of`
/// therefore returns `None` for it (an unknown token), exactly as for any unregistered byte
/// string, so the registry-vs-dispatch cross-check is untouched.
pub const ICEXISTS: &[u8] = b"__ICEXISTS";

/// The queue-time arity rule for a known command, mirroring the `arity` field of the
/// Redis command table (src/commands.def). Redis encodes arity as a single signed int:
/// a POSITIVE `n` means EXACTLY `n` total arguments (command token included); a NEGATIVE
/// `-n` means AT LEAST `n`. We split that into two explicit variants.
///
/// This enum is the canonical home of the arity rule; [`crate::cmd_txn`] re-exports it so
/// existing `use cmd_txn::Arity` paths and the `queue_validate` arity gate keep working.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arity {
    /// Exactly `n` arguments total (the command token counts as one).
    Exact(usize),
    /// At least `n` arguments total (variadic tail).
    Min(usize),
}

impl Arity {
    /// Whether `argc` (the total argument count, command token included) satisfies this
    /// rule. Matches Redis `commandCheckArity`: `(arity > 0 && argc != arity) || argc <
    /// -arity` is the REJECT condition, so here we return the ACCEPT.
    #[must_use]
    pub fn accepts(self, argc: usize) -> bool {
        match self {
            Arity::Exact(n) => argc == n,
            Arity::Min(n) => argc >= n,
        }
    }
}

/// How a command must be routed across shards (COORDINATOR.md #107).
///
/// This enum is the canonical home of the routing class; [`crate::route`] re-exports it
/// so existing `use route::CommandClass` paths keep working. See [`crate::route`] for the
/// full per-variant routing semantics (home fast path vs single owner hop vs whole
/// keyspace fan-out).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandClass {
    /// Control / connection / transaction commands (and PING/ECHO/INFO/CONFIG/...): no
    /// single owned key, so they ALWAYS run on the home shard.
    AlwaysHome,
    /// A single-key data command whose key is `args[1]` and whose handler touches only
    /// the store/wheel/db/now/env-rng (no [`crate::ConnState`]).
    KeyedSingle,
    /// A multi-key (or non-`args[1]`-keyed) data command; its keys are extracted by
    /// [`crate::route::command_keys`] for owner-set routing.
    KeyedMulti,
    /// A whole-keyspace command (KEYS/SCAN/DBSIZE/FLUSHALL/FLUSHDB/RANDOMKEY).
    WholeKeyspace,
}

/// The KEY-EXTRACTION PATTERN a command uses, named after WHAT it extracts. This is the
/// const, per-command shape; [`extract_keys`] turns a kind + a concrete [`Request`] into
/// a [`KeySpec`], preserving exactly the per-pattern logic `command_keys` used before
/// this registry existed (the `numkeys` parse, the MSET stride, the dest+sources walk).
///
/// The variants enumerate EXACTLY the patterns the legacy `command_keys` match used. In
/// particular [`KeySpecKind::Arg1`] is the legacy FALLBACK arm
/// (`single_key(req).map_or(None, One)`): it returns `args[1]` as the single key, or
/// `None` if `args[1]` is absent. Every command that hit that fallback arm before (all
/// `KeyedSingle` commands, MOVE, and every `AlwaysHome`/`WholeKeyspace` command) maps to
/// `Arg1` here, so `command_keys`'s observable output is byte-identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySpecKind {
    /// No routable key: return [`KeySpec::None`] unconditionally. (Reserved; no command
    /// currently maps here -- the legacy `command_keys` fallback returned `Arg1`, never an
    /// unconditional `None`, so preserving behavior means using `Arg1` for non-keyed
    /// commands. Kept for a future command whose key spec is genuinely "none".)
    None,
    /// The single key is `args[1]` (the legacy fallback arm `single_key`): `args[1]` ->
    /// [`KeySpec::One`], or [`KeySpec::None`] if `args[1]` is absent.
    Arg1,
    /// All of `args[1..]` are keys (DEL/EXISTS/UNLINK/TOUCH/SINTER/SUNION/SDIFF/PFCOUNT/
    /// PFMERGE/MGET, and the dest+sources *STORE commands whose dest=args[1] joins the
    /// set so the whole span is args[1..]).
    AllFromArg1,
    /// MSET stride: keys at `args[1]`, `args[3]`, `args[5]`, ...; the interleaved values
    /// are NOT keys. A malformed (no pair / odd-arg) MSET -> [`KeySpec::None`].
    MsetStrided,
    /// Two keys at `args[1]` and `args[2]` (RENAME/RENAMENX/COPY/SMOVE/LMOVE/RPOPLPUSH/
    /// ZRANGESTORE; trailing options follow the two keys).
    TwoKeysArg1Arg2,
    /// BITOP: `args[1]` is the OPERATION (not a key); dest=`args[2]`, sources=`args[3..]`.
    BitopDestArg2SourcesFrom3,
    /// `numkeys` at `args[1]`, keys=`args[2..2+numkeys]` (ZUNION/ZINTER/ZDIFF/ZINTERCARD/
    /// SINTERCARD; any trailing LIMIT/WEIGHTS option is after the keys).
    NumkeysAtArg1,
    /// dest=`args[1]`, `numkeys` at `args[2]`, source keys=`args[3..3+numkeys]`; the dest
    /// joins the routed set (ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE).
    ZstoreDestNumkeysAtArg2,
    /// OBJECT `<subcommand> <key>`: the key is `args[2]` (the subcommand is `args[1]`).
    ObjectArg2,
}

/// The SINGLE per-command record: all the DATA attributes that used to live in separate
/// hand-synced tables. The dispatch HANDLER is deliberately NOT a field (handlers have
/// varied signatures and stay as match arms); everything else about a command is here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandSpec {
    /// The UPPERCASE command token, e.g. `b"GET"`.
    pub name: &'static [u8],
    /// The queue-time arity rule (src/commands.def).
    pub arity: Arity,
    /// The cross-shard routing class.
    pub class: CommandClass,
    /// The key-extraction pattern (see [`KeySpecKind`]).
    ///
    /// CLUSTER (CLUSTER_CONTRACT.md, slice 2): adding a keyed
    /// (`KeyedSingle`/`KeyedMulti`) command requires choosing the CORRECT `KeySpecKind` so the
    /// cluster slot check (`serve::cluster_redirect`) extracts the right keys; a wrong kind
    /// yields the wrong slot (wrong MOVED / missing CROSSSLOT / local exec of a foreign key).
    /// The `keyed_command_set_is_pinned_for_cluster_slot_correctness` guard test enforces a
    /// conscious review by failing whenever the keyed-command set changes.
    pub key_spec: KeySpecKind,
    /// `true` iff this is a `denyoom` write the `maxmemory` ceiling gates (ADMISSION.md).
    pub denyoom: bool,
    /// `true` iff this is a transaction-control verb that BYPASSES MULTI queueing
    /// (MULTI/EXEC/DISCARD/RESET/QUIT/WATCH); these fall through to their dispatch arms
    /// instead of being staged.
    pub control: bool,
    /// `true` iff this command MUTATES the keyspace (HA-7d replica-read gate, REPLICA_READ.md
    /// #147). A REPLICA serves a keyed command locally (under the connection READONLY bit) ONLY
    /// when `is_write == false`; a write (`is_write == true`) always returns `-MOVED` to the slot
    /// owner, never a stale local mutation. CONSERVATIVE: an unknown command (no registry entry)
    /// is treated as a write by [`is_write`], so a replica never serves an unrecognized command
    /// locally. This flag is consulted ONLY on the cold cluster-redirect path; it does not touch
    /// the hot owns() routing.
    pub is_write: bool,
}

/// Whether `cmd_upper` (UPPERCASE token) MUTATES the keyspace (HA-7d replica-read gate). A thin
/// wrapper over the registry's [`CommandSpec::is_write`]; an UNKNOWN command (no registry entry)
/// is conservatively treated as a write (`true`), so the replica-read router never serves an
/// unrecognized command locally on a replica. Pure function of the bytes (no I/O, no state).
#[must_use]
pub fn is_write(cmd_upper: &[u8]) -> bool {
    spec_of(cmd_upper).is_none_or(|s| s.is_write)
}

/// Extract the routing KEY(s) of a command from `req` per its [`KeySpecKind`]
/// (COORDINATOR.md #107, Stage 1). This is the GENERIC extraction keyed by pattern: it
/// preserves EXACTLY the per-pattern logic the legacy `command_keys` match used (the
/// `numkeys` parse, the MSET stride, the BITOP/dest+sources walk), now in one place.
///
/// A malformed/short request (an index past the end, an unparseable `numkeys`) yields
/// [`KeySpec::None`] so the home handler emits the proper wrong-arity error rather than
/// the routing layer guessing.
#[must_use]
pub fn extract_keys(kind: KeySpecKind, req: &Request) -> KeySpec<'_> {
    match kind {
        KeySpecKind::None => KeySpec::None,
        // The legacy fallback arm: single_key(req).map_or(None, One). `args[1]` -> One,
        // absent -> None.
        KeySpecKind::Arg1 => crate::route::single_key(req).map_or(KeySpec::None, KeySpec::One),
        // All of args[1..] are keys.
        KeySpecKind::AllFromArg1 => keys_from(req, 1),
        // MSET key value [key value ...]: keys at args[1], args[3], ... There must be at
        // least one pair and an EVEN number of pair args (else malformed -> home).
        KeySpecKind::MsetStrided => {
            if req.args.len() < 3 || (req.args.len() - 1) % 2 != 0 {
                return KeySpec::None;
            }
            let idxs: Vec<usize> = (1..req.args.len()).step_by(2).collect();
            keys_at(req, &idxs)
        }
        // Two keys at args[1], args[2] (extra options follow the keys).
        KeySpecKind::TwoKeysArg1Arg2 => keys_at(req, &[1, 2]),
        // BITOP <op> <dest> <src...>: args[1] is the operation; dest=args[2], sources=args[3..].
        KeySpecKind::BitopDestArg2SourcesFrom3 => {
            if req.args.len() < 4 {
                return KeySpec::None;
            }
            keys_from(req, 2)
        }
        // numkeys=args[1], keys=args[2..2+numkeys].
        KeySpecKind::NumkeysAtArg1 => {
            let Some(numkeys) = req.args.get(1).and_then(|a| parse_count(a)) else {
                return KeySpec::None;
            };
            if numkeys == 0 || 2usize.saturating_add(numkeys) > req.args.len() {
                return KeySpec::None;
            }
            keys_range(req, 2, 2 + numkeys)
        }
        // dest=args[1], numkeys=args[2], keys=args[3..3+numkeys]; dest joins the set.
        KeySpecKind::ZstoreDestNumkeysAtArg2 => {
            let Some(numkeys) = req.args.get(2).and_then(|a| parse_count(a)) else {
                return KeySpec::None;
            };
            if numkeys == 0 || 3usize.saturating_add(numkeys) > req.args.len() {
                return KeySpec::None;
            }
            let mut idxs = Vec::with_capacity(1 + numkeys);
            idxs.push(1usize); // dest
            idxs.extend(3..3 + numkeys); // source keys
            keys_at(req, &idxs)
        }
        // OBJECT <subcommand> <key>: the key is args[2].
        KeySpecKind::ObjectArg2 => keys_at(req, &[2]),
    }
}

/// Parse `args[i]` as a NON-NEGATIVE decimal integer (a `numkeys`-style count). Returns
/// `None` on a non-numeric / negative / overflowing token, so the caller falls back HOME.
/// (Same logic as the legacy `route::parse_count`.)
fn parse_count(arg: &[u8]) -> Option<usize> {
    if arg.is_empty() || !arg.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(arg).ok()?.parse::<usize>().ok()
}

/// Collect `req.args[start..]` (all trailing args) as borrowed key slices, collapsing to
/// `One`/`None` for 1/0 keys so the single-key fast path stays alloc-free.
fn keys_from(req: &Request, start: usize) -> KeySpec<'_> {
    let Some(tail) = req.args.get(start..) else {
        return KeySpec::None;
    };
    match tail {
        [] => KeySpec::None,
        [one] => KeySpec::One(one.as_ref()),
        many => KeySpec::Many(many.iter().map(bytes::Bytes::as_ref).collect()),
    }
}

/// Collect the CONTIGUOUS range `req.args[start..end]`. An out-of-range `end` yields
/// `None` -> home. 0 -> `None`, 1 -> `One`, else `Many`.
fn keys_range(req: &Request, start: usize, end: usize) -> KeySpec<'_> {
    let Some(slice) = req.args.get(start..end) else {
        return KeySpec::None;
    };
    match slice {
        [] => KeySpec::None,
        [one] => KeySpec::One(one.as_ref()),
        many => KeySpec::Many(many.iter().map(bytes::Bytes::as_ref).collect()),
    }
}

/// Collect the args at `idxs` (each an index into `req.args`) as borrowed key slices. An
/// out-of-range index yields `None` -> home. 0 -> `None`, 1 -> `One`, else `Many`.
fn keys_at<'a>(req: &'a Request, idxs: &[usize]) -> KeySpec<'a> {
    let mut keys: Vec<&'a [u8]> = Vec::with_capacity(idxs.len());
    for &i in idxs {
        match req.args.get(i) {
            Some(b) => keys.push(b.as_ref()),
            None => return KeySpec::None,
        }
    }
    match keys.len() {
        0 => KeySpec::None,
        1 => KeySpec::One(keys[0]),
        _ => KeySpec::Many(keys),
    }
}

/// The [`CommandSpec`] for a known UPPERCASED command token, or `None` if the token is
/// not a command this server implements. This is THE registry: a single `match` over the
/// 148 command names, each returning a `&'static CommandSpec` whose every field is the
/// single source of truth for that command's arity, class, key spec, denyoom, and control
/// attributes.
///
/// This is a flat lookup TABLE, so its length (`too_many_lines`) and the many arms
/// sharing the same field values (`match_same_arms`) are intentional: collapsing
/// same-valued arms would group unrelated commands and defeat the one-arm-per-command
/// registry-vs-dispatch cross-check. Both lints are allowed here with that justification
/// (matching the legacy `arity_of`/`classify` tables this registry replaces).
#[allow(clippy::too_many_lines, clippy::match_same_arms)]
#[must_use]
pub fn spec_of(cmd_upper: &[u8]) -> Option<&'static CommandSpec> {
    use Arity::{Exact, Min};
    use CommandClass::{AlwaysHome, KeyedMulti, KeyedSingle, WholeKeyspace};
    use KeySpecKind::{
        AllFromArg1, Arg1, BitopDestArg2SourcesFrom3, MsetStrided, NumkeysAtArg1, ObjectArg2,
        TwoKeysArg1Arg2, ZstoreDestNumkeysAtArg2,
    };
    let spec: &'static CommandSpec = match cmd_upper {
        // -- Tier-0 / connection (dispatch.rs). --
        b"PING" => &CommandSpec {
            name: b"PING",
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ECHO" => &CommandSpec {
            name: b"ECHO",
            arity: Exact(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HELLO" => &CommandSpec {
            name: b"HELLO",
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"AUTH" => &CommandSpec {
            name: b"AUTH",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SELECT" => &CommandSpec {
            name: b"SELECT",
            arity: Exact(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // QUIT's command-table arity is -1 (Min(1)) in src/commands.def, not Exact(1).
        b"QUIT" => &CommandSpec {
            name: b"QUIT",
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: true,
            is_write: false,
        },
        b"RESET" => &CommandSpec {
            name: b"RESET",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: true,
            is_write: false,
        },
        b"CLIENT" => &CommandSpec {
            name: b"CLIENT",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"COMMAND" => &CommandSpec {
            name: b"COMMAND",
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"INFO" => &CommandSpec {
            name: b"INFO",
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"CONFIG" => &CommandSpec {
            name: b"CONFIG",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // CLUSTER (CLUSTER_CONTRACT.md #70, slice 1): the read-only/introspection CLUSTER
        // surface. Like CONFIG it is an admin container command: AlwaysHome (never
        // key-routed -- KEYSLOT computes the slot of an ARGUMENT but the command itself
        // owns no key), arity Min(2) (the token plus a subcommand), not denyoom, not a txn
        // control verb.
        b"CLUSTER" => &CommandSpec {
            name: b"CLUSTER",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- PERSISTENCE (#58 durable on-disk snapshot). SAVE/BGSAVE/LASTSAVE are admin
        // commands with NO key: AlwaysHome (never key-routed), arity Exact(1) each (the bare
        // token; src/commands.def gives SAVE/BGSAVE/LASTSAVE arity 1), not denyoom (they do not
        // allocate keyspace), not a txn control verb, and NOT is_write (they do not mutate the
        // keyspace -- they DUMP it; the replica-read gate must not treat SAVE as a write, and a
        // snapshot is taken on the home shard then fanned out to dump every shard's partition).
        // The cross-shard SAVE/BGSAVE fan-out + the manifest commit live in the binary's serve
        // layer (it owns the per-shard stores + the data_dir + the env Clock for the timestamp).
        b"SAVE" => &CommandSpec {
            name: b"SAVE",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"BGSAVE" => &CommandSpec {
            name: b"BGSAVE",
            // Redis BGSAVE accepts an optional SCHEDULE arg (arity -1); we accept the bare form
            // and ignore a trailing arg, so Min(1).
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"LASTSAVE" => &CommandSpec {
            name: b"LASTSAVE",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- Transaction control (cmd_txn / dispatch). The 6 control verbs (control: true)
        // bypass MULTI queueing; WATCH/UNWATCH arities are -2 / 1 (src/commands.def). --
        b"MULTI" => &CommandSpec {
            name: b"MULTI",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: true,
            is_write: false,
        },
        b"EXEC" => &CommandSpec {
            name: b"EXEC",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: true,
            is_write: false,
        },
        b"DISCARD" => &CommandSpec {
            name: b"DISCARD",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: true,
            is_write: false,
        },
        b"WATCH" => &CommandSpec {
            name: b"WATCH",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: true,
            is_write: false,
        },
        b"UNWATCH" => &CommandSpec {
            name: b"UNWATCH",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // READONLY / READWRITE (REPLICA_READ.md #147, HA-7d): connection commands that set/clear
        // the per-connection read-only bit. AlwaysHome (no key), arity 1, not a write.
        b"READONLY" => &CommandSpec {
            name: b"READONLY",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"READWRITE" => &CommandSpec {
            name: b"READWRITE",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // ASKING (HA-6 online slot migration): the one-shot connection command a client sends after
        // an -ASK redirect, before re-issuing the command at the destination. AlwaysHome (no key),
        // arity 1, not a write. Intercepted by the serve router (which owns the one-shot flag), but
        // registered here so COMMAND/arity see it and the home dispatch has a real arm.
        b"ASKING" => &CommandSpec {
            name: b"ASKING",
            arity: Exact(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- Strings (cmd_string). --
        b"GET" => &CommandSpec {
            name: b"GET",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SET" => &CommandSpec {
            name: b"SET",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"SETNX" => &CommandSpec {
            name: b"SETNX",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"GETSET" => &CommandSpec {
            name: b"GETSET",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"STRLEN" => &CommandSpec {
            name: b"STRLEN",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"INCR" => &CommandSpec {
            name: b"INCR",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"DECR" => &CommandSpec {
            name: b"DECR",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"INCRBY" => &CommandSpec {
            name: b"INCRBY",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"DECRBY" => &CommandSpec {
            name: b"DECRBY",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"INCRBYFLOAT" => &CommandSpec {
            name: b"INCRBYFLOAT",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"APPEND" => &CommandSpec {
            name: b"APPEND",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"MGET" => &CommandSpec {
            name: b"MGET",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"MSET" => &CommandSpec {
            name: b"MSET",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: MsetStrided,
            denyoom: true,
            control: false,
            is_write: true,
        },
        // -- Generic keyspace (cmd_keyspace). --
        b"DEL" => &CommandSpec {
            name: b"DEL",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"EXISTS" => &CommandSpec {
            name: b"EXISTS",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"TYPE" => &CommandSpec {
            name: b"TYPE",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"KEYS" => &CommandSpec {
            name: b"KEYS",
            arity: Exact(2),
            class: WholeKeyspace,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SCAN" => &CommandSpec {
            name: b"SCAN",
            arity: Min(2),
            class: WholeKeyspace,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"DBSIZE" => &CommandSpec {
            name: b"DBSIZE",
            arity: Exact(1),
            class: WholeKeyspace,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"RANDOMKEY" => &CommandSpec {
            name: b"RANDOMKEY",
            arity: Exact(1),
            class: WholeKeyspace,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"RENAME" => &CommandSpec {
            name: b"RENAME",
            arity: Exact(3),
            class: KeyedMulti,
            key_spec: TwoKeysArg1Arg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"RENAMENX" => &CommandSpec {
            name: b"RENAMENX",
            arity: Exact(3),
            class: KeyedMulti,
            key_spec: TwoKeysArg1Arg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"COPY" => &CommandSpec {
            name: b"COPY",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: TwoKeysArg1Arg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        // MOVE has exactly ONE key (args[1]); args[2] is the destination DB index, NOT a
        // key -- so its key_spec is Arg1, and it is NOT denyoom (Redis flags it write-fast).
        b"MOVE" => &CommandSpec {
            name: b"MOVE",
            arity: Exact(3),
            class: KeyedMulti,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"SWAPDB" => &CommandSpec {
            name: b"SWAPDB",
            arity: Exact(3),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"TOUCH" => &CommandSpec {
            name: b"TOUCH",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"UNLINK" => &CommandSpec {
            name: b"UNLINK",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"FLUSHDB" => &CommandSpec {
            name: b"FLUSHDB",
            arity: Min(1),
            class: WholeKeyspace,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"FLUSHALL" => &CommandSpec {
            name: b"FLUSHALL",
            arity: Min(1),
            class: WholeKeyspace,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        // -- TTL / EXPIRE family (cmd_expire). --
        b"EXPIRE" => &CommandSpec {
            name: b"EXPIRE",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"PEXPIRE" => &CommandSpec {
            name: b"PEXPIRE",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"EXPIREAT" => &CommandSpec {
            name: b"EXPIREAT",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"PEXPIREAT" => &CommandSpec {
            name: b"PEXPIREAT",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"TTL" => &CommandSpec {
            name: b"TTL",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"PTTL" => &CommandSpec {
            name: b"PTTL",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"EXPIRETIME" => &CommandSpec {
            name: b"EXPIRETIME",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"PEXPIRETIME" => &CommandSpec {
            name: b"PEXPIRETIME",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"PERSIST" => &CommandSpec {
            name: b"PERSIST",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"GETEX" => &CommandSpec {
            name: b"GETEX",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"SETEX" => &CommandSpec {
            name: b"SETEX",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"PSETEX" => &CommandSpec {
            name: b"PSETEX",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        // -- Lists (cmd_list). --
        b"LPUSH" => &CommandSpec {
            name: b"LPUSH",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"RPUSH" => &CommandSpec {
            name: b"RPUSH",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"LPUSHX" => &CommandSpec {
            name: b"LPUSHX",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"RPUSHX" => &CommandSpec {
            name: b"RPUSHX",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"LPOP" => &CommandSpec {
            name: b"LPOP",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"RPOP" => &CommandSpec {
            name: b"RPOP",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"LLEN" => &CommandSpec {
            name: b"LLEN",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"LRANGE" => &CommandSpec {
            name: b"LRANGE",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"LINDEX" => &CommandSpec {
            name: b"LINDEX",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"LSET" => &CommandSpec {
            name: b"LSET",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"LINSERT" => &CommandSpec {
            name: b"LINSERT",
            arity: Exact(5),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"LREM" => &CommandSpec {
            name: b"LREM",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"LTRIM" => &CommandSpec {
            name: b"LTRIM",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"LMOVE" => &CommandSpec {
            name: b"LMOVE",
            arity: Exact(5),
            class: KeyedMulti,
            key_spec: TwoKeysArg1Arg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"RPOPLPUSH" => &CommandSpec {
            name: b"RPOPLPUSH",
            arity: Exact(3),
            class: KeyedMulti,
            key_spec: TwoKeysArg1Arg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"LPOS" => &CommandSpec {
            name: b"LPOS",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- Hashes (cmd_hash). --
        b"HSET" => &CommandSpec {
            name: b"HSET",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"HMSET" => &CommandSpec {
            name: b"HMSET",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"HSETNX" => &CommandSpec {
            name: b"HSETNX",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"HGET" => &CommandSpec {
            name: b"HGET",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HMGET" => &CommandSpec {
            name: b"HMGET",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HDEL" => &CommandSpec {
            name: b"HDEL",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"HGETALL" => &CommandSpec {
            name: b"HGETALL",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HKEYS" => &CommandSpec {
            name: b"HKEYS",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HVALS" => &CommandSpec {
            name: b"HVALS",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HLEN" => &CommandSpec {
            name: b"HLEN",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HEXISTS" => &CommandSpec {
            name: b"HEXISTS",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HSTRLEN" => &CommandSpec {
            name: b"HSTRLEN",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HINCRBY" => &CommandSpec {
            name: b"HINCRBY",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"HINCRBYFLOAT" => &CommandSpec {
            name: b"HINCRBYFLOAT",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"HRANDFIELD" => &CommandSpec {
            name: b"HRANDFIELD",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"HSCAN" => &CommandSpec {
            name: b"HSCAN",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- Sets (cmd_set). --
        b"SADD" => &CommandSpec {
            name: b"SADD",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"SREM" => &CommandSpec {
            name: b"SREM",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"SMEMBERS" => &CommandSpec {
            name: b"SMEMBERS",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SISMEMBER" => &CommandSpec {
            name: b"SISMEMBER",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SMISMEMBER" => &CommandSpec {
            name: b"SMISMEMBER",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SCARD" => &CommandSpec {
            name: b"SCARD",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SPOP" => &CommandSpec {
            name: b"SPOP",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"SRANDMEMBER" => &CommandSpec {
            name: b"SRANDMEMBER",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SMOVE" => &CommandSpec {
            name: b"SMOVE",
            arity: Exact(4),
            class: KeyedMulti,
            key_spec: TwoKeysArg1Arg2,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"SINTER" => &CommandSpec {
            name: b"SINTER",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SUNION" => &CommandSpec {
            name: b"SUNION",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SDIFF" => &CommandSpec {
            name: b"SDIFF",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SINTERCARD" => &CommandSpec {
            name: b"SINTERCARD",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: NumkeysAtArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"SINTERSTORE" => &CommandSpec {
            name: b"SINTERSTORE",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"SUNIONSTORE" => &CommandSpec {
            name: b"SUNIONSTORE",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"SDIFFSTORE" => &CommandSpec {
            name: b"SDIFFSTORE",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"SSCAN" => &CommandSpec {
            name: b"SSCAN",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- Sorted sets (cmd_zset). --
        b"ZADD" => &CommandSpec {
            name: b"ZADD",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"ZINCRBY" => &CommandSpec {
            name: b"ZINCRBY",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"ZREM" => &CommandSpec {
            name: b"ZREM",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"ZSCORE" => &CommandSpec {
            name: b"ZSCORE",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZMSCORE" => &CommandSpec {
            name: b"ZMSCORE",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZCARD" => &CommandSpec {
            name: b"ZCARD",
            arity: Exact(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZRANK" => &CommandSpec {
            name: b"ZRANK",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZREVRANK" => &CommandSpec {
            name: b"ZREVRANK",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZCOUNT" => &CommandSpec {
            name: b"ZCOUNT",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZLEXCOUNT" => &CommandSpec {
            name: b"ZLEXCOUNT",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZRANGE" => &CommandSpec {
            name: b"ZRANGE",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZREVRANGE" => &CommandSpec {
            name: b"ZREVRANGE",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZRANGEBYSCORE" => &CommandSpec {
            name: b"ZRANGEBYSCORE",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZREVRANGEBYSCORE" => &CommandSpec {
            name: b"ZREVRANGEBYSCORE",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZRANGEBYLEX" => &CommandSpec {
            name: b"ZRANGEBYLEX",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZREVRANGEBYLEX" => &CommandSpec {
            name: b"ZREVRANGEBYLEX",
            arity: Min(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZREMRANGEBYRANK" => &CommandSpec {
            name: b"ZREMRANGEBYRANK",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"ZREMRANGEBYSCORE" => &CommandSpec {
            name: b"ZREMRANGEBYSCORE",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"ZREMRANGEBYLEX" => &CommandSpec {
            name: b"ZREMRANGEBYLEX",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"ZPOPMIN" => &CommandSpec {
            name: b"ZPOPMIN",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"ZPOPMAX" => &CommandSpec {
            name: b"ZPOPMAX",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        b"ZRANDMEMBER" => &CommandSpec {
            name: b"ZRANDMEMBER",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZSCAN" => &CommandSpec {
            name: b"ZSCAN",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZRANGESTORE" => &CommandSpec {
            name: b"ZRANGESTORE",
            arity: Min(5),
            class: KeyedMulti,
            key_spec: TwoKeysArg1Arg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"ZUNION" => &CommandSpec {
            name: b"ZUNION",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: NumkeysAtArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZINTER" => &CommandSpec {
            name: b"ZINTER",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: NumkeysAtArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZDIFF" => &CommandSpec {
            name: b"ZDIFF",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: NumkeysAtArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"ZUNIONSTORE" => &CommandSpec {
            name: b"ZUNIONSTORE",
            arity: Min(4),
            class: KeyedMulti,
            key_spec: ZstoreDestNumkeysAtArg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"ZINTERSTORE" => &CommandSpec {
            name: b"ZINTERSTORE",
            arity: Min(4),
            class: KeyedMulti,
            key_spec: ZstoreDestNumkeysAtArg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"ZDIFFSTORE" => &CommandSpec {
            name: b"ZDIFFSTORE",
            arity: Min(4),
            class: KeyedMulti,
            key_spec: ZstoreDestNumkeysAtArg2,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"ZINTERCARD" => &CommandSpec {
            name: b"ZINTERCARD",
            arity: Min(3),
            class: KeyedMulti,
            key_spec: NumkeysAtArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- Bitmaps (cmd_bitmap). --
        b"SETBIT" => &CommandSpec {
            name: b"SETBIT",
            arity: Exact(4),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"GETBIT" => &CommandSpec {
            name: b"GETBIT",
            arity: Exact(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"BITCOUNT" => &CommandSpec {
            name: b"BITCOUNT",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"BITPOS" => &CommandSpec {
            name: b"BITPOS",
            arity: Min(3),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"BITOP" => &CommandSpec {
            name: b"BITOP",
            arity: Min(4),
            class: KeyedMulti,
            key_spec: BitopDestArg2SourcesFrom3,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"BITFIELD" => &CommandSpec {
            name: b"BITFIELD",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"BITFIELD_RO" => &CommandSpec {
            name: b"BITFIELD_RO",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- HyperLogLog (cmd_hll). All three are Redis arity -2 (Min(2)). --
        b"PFADD" => &CommandSpec {
            name: b"PFADD",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        b"PFCOUNT" => &CommandSpec {
            name: b"PFCOUNT",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"PFMERGE" => &CommandSpec {
            name: b"PFMERGE",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: AllFromArg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        // -- Introspection (cmd_introspect). --
        b"OBJECT" => &CommandSpec {
            name: b"OBJECT",
            arity: Min(2),
            class: KeyedMulti,
            key_spec: ObjectArg2,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- INTERNAL cross-shard verb (cmd_set::cmd_icstoreset), COORDINATOR.md #107 Stage
        // 2b. `__ICSTORESET dest m...` writes a spanning set-*STORE result to the dest owner
        // (a single-key denyoom write keyed on args[1]). It is in the registry so it routes /
        // admits like any keyed write AND so the registry-vs-dispatch cross-check stays exact,
        // but it is CLIENT-UNREACHABLE: the serve-loop router and the queue-time validator
        // reject it before routing, so a client `__ICSTORESET` gets unknown-command; only the
        // coordinator issues it internally. Arity Min(2) (token + dest; members optional). --
        b"__ICSTORESET" => &CommandSpec {
            name: b"__ICSTORESET",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        // -- INTERNAL cross-shard verb (cmd_zset::cmd_icstorezset), COORDINATOR.md #107 Stage
        // 2b-2. `__ICSTOREZSET dest m1 s1 ...` writes a spanning zset *STORE / ZRANGESTORE
        // result to the dest owner (a single-key denyoom write keyed on args[1]). It is in the
        // registry so it routes / admits like any keyed write AND so the registry-vs-dispatch
        // cross-check stays exact, but it is CLIENT-UNREACHABLE: the serve-loop router and the
        // queue-time validator reject it before routing, so a client `__ICSTOREZSET` gets
        // unknown-command; only the coordinator issues it internally. Arity Min(2) (token +
        // dest; member/score pairs optional). --
        b"__ICSTOREZSET" => &CommandSpec {
            name: b"__ICSTOREZSET",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        // -- INTERNAL cross-shard verb (cmd_hll::cmd_icstorehll), COORDINATOR.md #107 Stage
        // 2b-3. `__ICSTOREHLL dest <dense-hll-bytes>` writes a spanning-PFMERGE merged HLL to
        // the dest owner (a single-key denyoom write keyed on args[1]) with the dest TTL
        // PRESERVED (unlike the set/zset *STORE verbs, which clear it). It is in the registry
        // so it routes / admits like any keyed write AND so the registry-vs-dispatch
        // cross-check stays exact, but it is CLIENT-UNREACHABLE: the serve-loop router rejects
        // it before routing, so a client `__ICSTOREHLL` gets unknown-command; only the
        // coordinator issues it internally. Arity Min(2) (token + dest; the object follows). --
        b"__ICSTOREHLL" => &CommandSpec {
            name: b"__ICSTOREHLL",
            arity: Min(2),
            class: KeyedSingle,
            key_spec: Arg1,
            denyoom: true,
            control: false,
            is_write: true,
        },
        // -- Pub/Sub (SERVER_PUSH.md #20, PR 91a; handled in the SERVE layer, NOT
        // `dispatch_inner`). SUBSCRIBE/UNSUBSCRIBE/PUBLISH are AlwaysHome (no routable key:
        // they register/look-up the per-shard subscription table on the connection's home
        // shard, and PUBLISH fans out via the coordinator), control: false, denyoom: false.
        // They are in the registry so their arity is validated and `classify` returns
        // AlwaysHome (the router never treats them as keyed/whole-keyspace); the serve loop
        // intercepts them before dispatch, so they have NO `dispatch_inner` arm and are NOT
        // in `dispatch_arm_names` (the cross-check enumerates only the dispatch-arm list). --
        b"SUBSCRIBE" => &CommandSpec {
            name: b"SUBSCRIBE",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"UNSUBSCRIBE" => &CommandSpec {
            name: b"UNSUBSCRIBE",
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        // -- Pattern Pub/Sub + introspection (SERVER_PUSH.md #20, PR 91b; also SERVE-layer
        // routed, NOT `dispatch_inner`). PSUBSCRIBE (arity Min 2) / PUNSUBSCRIBE (arity Min 1,
        // zero-pattern unsubscribe-all) register/look-up the per-shard `patterns` table on the
        // home shard; PUBSUB (arity Min 2: a subcommand is required) fans the introspection
        // gather out via the coordinator. All AlwaysHome (no routable key), control: false,
        // denyoom: false. In the registry so arity validates + `classify` returns AlwaysHome;
        // intercepted in the serve loop, so NO `dispatch_inner` arm + NOT in `dispatch_arm_names`. --
        b"PSUBSCRIBE" => &CommandSpec {
            name: b"PSUBSCRIBE",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"PUNSUBSCRIBE" => &CommandSpec {
            name: b"PUNSUBSCRIBE",
            arity: Min(1),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"PUBSUB" => &CommandSpec {
            name: b"PUBSUB",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        b"PUBLISH" => &CommandSpec {
            name: b"PUBLISH",
            arity: Exact(3),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        // -- INTERNAL cross-shard pub/sub fan-out verb (SERVER_PUSH.md #20 / COORDINATOR.md
        // #107, PR 91a). `__ICPUBLISH <channel> <payload>` delivers to a shard's LOCAL
        // subscribers and returns the local receiver count. AlwaysHome (no routable key); in
        // the registry so the cross-check stays exact, but CLIENT-UNREACHABLE: the serve-loop
        // router rejects a client `__ICPUBLISH` with unknown-command (the same gate as the
        // `__ICSTORE*` verbs); only the coordinator issues it. Arity Exact(3) (token + channel
        // + payload). It is handled by the coordinator's run_remote pub/sub branch, NOT a
        // `dispatch_inner` arm, so it too is absent from `dispatch_arm_names`. --
        b"__ICPUBLISH" => &CommandSpec {
            name: b"__ICPUBLISH",
            arity: Exact(3),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: true,
        },
        // -- INTERNAL cross-shard PUBSUB-introspection gather verb (SERVER_PUSH.md #20 /
        // COORDINATOR.md #107, PR 91b). `__ICPUBSUB <subcommand> [args]` returns a shard's LOCAL
        // introspection partial. AlwaysHome (no routable key); in the registry so the cross-check
        // stays exact, but CLIENT-UNREACHABLE: the serve-loop router rejects a client `__ICPUBSUB`
        // with unknown-command (the same gate as `__ICPUBLISH`); only the coordinator issues it.
        // Arity Min(2) (token + subcommand; NUMSUB carries channels after). It is handled by the
        // coordinator's run_remote pub/sub branch, NOT a `dispatch_inner` arm, so it too is absent
        // from `dispatch_arm_names`. --
        b"__ICPUBSUB" => &CommandSpec {
            name: b"__ICPUBSUB",
            arity: Min(2),
            class: AlwaysHome,
            key_spec: Arg1,
            denyoom: false,
            control: false,
            is_write: false,
        },
        _ => return None,
    };
    Some(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    /// REGISTRY-CONSISTENCY (#89): spot-check that `spec_of` returns the SAME attributes a
    /// representative command had in the legacy per-attribute tables. A wrong field here is
    /// a latent routing/admission/arity regression; this is the registry's self-guard
    /// alongside the existing arity / classify / denyoom / command_keys tests (which all
    /// pass UNCHANGED because the wrappers derive from this registry).
    #[test]
    fn spec_of_spot_checks_match_the_legacy_tables() {
        let g = spec_of(b"GET").unwrap();
        assert_eq!(g.arity, Arity::Exact(2));
        assert_eq!(g.class, CommandClass::KeyedSingle);
        assert_eq!(g.key_spec, KeySpecKind::Arg1);
        assert!(!g.denyoom);
        assert!(!g.control);

        let s = spec_of(b"SET").unwrap();
        assert_eq!(s.arity, Arity::Min(3));
        assert_eq!(s.class, CommandClass::KeyedSingle);
        assert_eq!(s.key_spec, KeySpecKind::Arg1);
        assert!(s.denyoom);

        let mset = spec_of(b"MSET").unwrap();
        assert_eq!(mset.arity, Arity::Min(3));
        assert_eq!(mset.class, CommandClass::KeyedMulti);
        assert_eq!(mset.key_spec, KeySpecKind::MsetStrided);
        assert!(mset.denyoom);

        let del = spec_of(b"DEL").unwrap();
        assert_eq!(del.arity, Arity::Min(2));
        assert_eq!(del.class, CommandClass::KeyedMulti);
        assert_eq!(del.key_spec, KeySpecKind::AllFromArg1);
        assert!(!del.denyoom);

        let bitop = spec_of(b"BITOP").unwrap();
        assert_eq!(bitop.arity, Arity::Min(4));
        assert_eq!(bitop.class, CommandClass::KeyedMulti);
        assert_eq!(bitop.key_spec, KeySpecKind::BitopDestArg2SourcesFrom3);
        assert!(bitop.denyoom);

        let object = spec_of(b"OBJECT").unwrap();
        assert_eq!(object.arity, Arity::Min(2));
        assert_eq!(object.class, CommandClass::KeyedMulti);
        assert_eq!(object.key_spec, KeySpecKind::ObjectArg2);

        let exec = spec_of(b"EXEC").unwrap();
        assert_eq!(exec.arity, Arity::Exact(1));
        assert_eq!(exec.class, CommandClass::AlwaysHome);
        // NOTE (ambiguity surfaced by #89): the issue text described EXEC/KEYS key_spec as
        // `None`, but the LEGACY `command_keys` had no unconditional-None arm: every command
        // not matched by a specific arm fell through to the `single_key` FALLBACK (= args[1]
        // -> One). So EXEC/KEYS key_spec is `Arg1`, NOT `None`. We preserve that exact legacy
        // behavior (the routing layer never consumes command_keys for AlwaysHome/
        // WholeKeyspace commands, so the dead `One(args[1])` is harmless), and the existing
        // command_keys unit test stays green unchanged.
        assert_eq!(exec.key_spec, KeySpecKind::Arg1);
        assert!(exec.control);

        let keys = spec_of(b"KEYS").unwrap();
        // NOTE: the legacy arity_of table has KEYS = Exact(2) (IronCache's KEYS takes
        // exactly one pattern arg), NOT Min(2) -- we assert the ACTUAL transcribed value so
        // this guard matches the legacy table byte-for-byte.
        assert_eq!(keys.arity, Arity::Exact(2));
        assert_eq!(keys.class, CommandClass::WholeKeyspace);
        assert_eq!(keys.key_spec, KeySpecKind::Arg1);
        assert!(!keys.control);
    }

    /// The ONLY control=true commands are the 6 transaction-control verbs that bypass MULTI
    /// queueing (the dispatch.rs queue-gate exclusion set). Nothing else is control.
    #[test]
    fn control_set_is_exactly_the_six_queue_gate_verbs() {
        let control_verbs: &[&[u8]] = &[b"MULTI", b"EXEC", b"DISCARD", b"RESET", b"QUIT", b"WATCH"];
        // The 6 are control.
        for c in control_verbs {
            assert!(
                spec_of(c).is_some_and(|s| s.control),
                "{:?} must be control",
                String::from_utf8_lossy(c)
            );
        }
        // Nothing else is control: count the control=true specs across the whole registry by
        // walking the dispatch-arm list (the registry name set) and asserting exactly 6.
        let all = super::tests::all_registry_names();
        let n_control = all
            .iter()
            .filter(|c| spec_of(c).is_some_and(|s| s.control))
            .count();
        assert_eq!(n_control, control_verbs.len(), "exactly 6 control verbs");
    }

    /// CLUSTER SLOT-CHECK GUARD (CLUSTER_CONTRACT.md #70, slice 2). The per-command
    /// `KeySpecKind` is the SINGLE chokepoint the cluster redirect check
    /// (`serve::cluster_redirect`) reads to extract a command's keys and compute its slot. A
    /// future multi-key / odd-key-position command (SORT, EVAL, GEORADIUS ... STORE, LMPOP,
    /// ZMPOP, XREAD) added as `KeyedSingle`/`KeyedMulti` WITHOUT a correct `KeySpecKind` would
    /// silently get the WRONG slot (wrong MOVED / missing CROSSSLOT / local exec of a foreign
    /// key). This guard PINS the exact set of keyed (`KeyedSingle` + `KeyedMulti`) command
    /// names: adding (or reclassifying) a keyed command FAILS this test until the author
    /// consciously updates the list AND, in doing so, reviews the new command's `KeySpecKind`
    /// for cluster correctness. (`AlwaysHome` WATCH is keyed in Redis but is handled by its own
    /// dedicated cluster WATCH guard in `serve`, so it is intentionally NOT in this set.)
    #[test]
    #[allow(clippy::too_many_lines)] // the pinned keyed-command name list is intentionally long
    fn keyed_command_set_is_pinned_for_cluster_slot_correctness() {
        let mut keyed: Vec<&'static [u8]> = all_registry_names()
            .into_iter()
            .filter(|c| {
                spec_of(c).is_some_and(|s| {
                    matches!(
                        s.class,
                        CommandClass::KeyedSingle | CommandClass::KeyedMulti
                    )
                })
            })
            .collect();
        keyed.sort_unstable();

        // The EXACT sorted set of keyed (KeyedSingle + KeyedMulti) commands. To add a keyed
        // command: add it here AND verify its `KeySpecKind` extracts the right keys so the
        // cluster slot check (CROSSSLOT / MOVED) is correct for it.
        // NOTE: byte (ASCII) order, NOT lexical-case order: `_` (0x5F) sorts AFTER `Z` (0x5A),
        // so the `__ICSTORE*` internal store verbs come LAST.
        let expected: &[&[u8]] = &[
            b"APPEND",
            b"BITCOUNT",
            b"BITFIELD",
            b"BITFIELD_RO",
            b"BITOP",
            b"BITPOS",
            b"COPY",
            b"DECR",
            b"DECRBY",
            b"DEL",
            b"EXISTS",
            b"EXPIRE",
            b"EXPIREAT",
            b"EXPIRETIME",
            b"GET",
            b"GETBIT",
            b"GETEX",
            b"GETSET",
            b"HDEL",
            b"HEXISTS",
            b"HGET",
            b"HGETALL",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            b"HKEYS",
            b"HLEN",
            b"HMGET",
            b"HMSET",
            b"HRANDFIELD",
            b"HSCAN",
            b"HSET",
            b"HSETNX",
            b"HSTRLEN",
            b"HVALS",
            b"INCR",
            b"INCRBY",
            b"INCRBYFLOAT",
            b"LINDEX",
            b"LINSERT",
            b"LLEN",
            b"LMOVE",
            b"LPOP",
            b"LPOS",
            b"LPUSH",
            b"LPUSHX",
            b"LRANGE",
            b"LREM",
            b"LSET",
            b"LTRIM",
            b"MGET",
            b"MOVE",
            b"MSET",
            b"OBJECT",
            b"PERSIST",
            b"PEXPIRE",
            b"PEXPIREAT",
            b"PEXPIRETIME",
            b"PFADD",
            b"PFCOUNT",
            b"PFMERGE",
            b"PSETEX",
            b"PTTL",
            b"RENAME",
            b"RENAMENX",
            b"RPOP",
            b"RPOPLPUSH",
            b"RPUSH",
            b"RPUSHX",
            b"SADD",
            b"SCARD",
            b"SDIFF",
            b"SDIFFSTORE",
            b"SET",
            b"SETBIT",
            b"SETEX",
            b"SETNX",
            b"SINTER",
            b"SINTERCARD",
            b"SINTERSTORE",
            b"SISMEMBER",
            b"SMEMBERS",
            b"SMISMEMBER",
            b"SMOVE",
            b"SPOP",
            b"SRANDMEMBER",
            b"SREM",
            b"SSCAN",
            b"STRLEN",
            b"SUNION",
            b"SUNIONSTORE",
            b"TOUCH",
            b"TTL",
            b"TYPE",
            b"UNLINK",
            b"ZADD",
            b"ZCARD",
            b"ZCOUNT",
            b"ZDIFF",
            b"ZDIFFSTORE",
            b"ZINCRBY",
            b"ZINTER",
            b"ZINTERCARD",
            b"ZINTERSTORE",
            b"ZLEXCOUNT",
            b"ZMSCORE",
            b"ZPOPMAX",
            b"ZPOPMIN",
            b"ZRANDMEMBER",
            b"ZRANGE",
            b"ZRANGEBYLEX",
            b"ZRANGEBYSCORE",
            b"ZRANGESTORE",
            b"ZRANK",
            b"ZREM",
            b"ZREMRANGEBYLEX",
            b"ZREMRANGEBYRANK",
            b"ZREMRANGEBYSCORE",
            b"ZREVRANGE",
            b"ZREVRANGEBYLEX",
            b"ZREVRANGEBYSCORE",
            b"ZREVRANK",
            b"ZSCAN",
            b"ZSCORE",
            b"ZUNION",
            b"ZUNIONSTORE",
            b"__ICSTOREHLL",
            b"__ICSTORESET",
            b"__ICSTOREZSET",
        ];
        assert_eq!(
            keyed, expected,
            "the keyed (KeyedSingle/KeyedMulti) command set changed: a keyed command was added \
             or reclassified. Update the pinned list AND verify the new command's KeySpecKind \
             extracts the correct keys so the cluster slot check (CROSSSLOT/MOVED) is correct."
        );
    }

    /// `extract_keys` is byte-identical to the legacy `command_keys` per-pattern logic. A
    /// few representative shapes (the full per-command surface is covered by the unchanged
    /// `route::tests::command_keys_key_spec_table`, which now routes through this function).
    #[test]
    fn extract_keys_preserves_the_legacy_patterns() {
        assert_eq!(
            extract_keys(KeySpecKind::AllFromArg1, &req(&[b"DEL", b"a", b"b"])),
            KeySpec::Many(vec![b"a", b"b"])
        );
        assert_eq!(
            extract_keys(KeySpecKind::Arg1, &req(&[b"GET", b"k"])),
            KeySpec::One(b"k")
        );
        assert_eq!(
            extract_keys(KeySpecKind::Arg1, &req(&[b"GET"])),
            KeySpec::None
        );
        assert_eq!(
            extract_keys(
                KeySpecKind::MsetStrided,
                &req(&[b"MSET", b"k1", b"v1", b"k2", b"v2"])
            ),
            KeySpec::Many(vec![b"k1", b"k2"])
        );
        assert_eq!(
            extract_keys(
                KeySpecKind::BitopDestArg2SourcesFrom3,
                &req(&[b"BITOP", b"AND", b"d", b"s"])
            ),
            KeySpec::Many(vec![b"d", b"s"])
        );
        assert_eq!(
            extract_keys(KeySpecKind::None, &req(&[b"X", b"k"])),
            KeySpec::None
        );
    }

    /// The exhaustive registry name set, in dispatch-arm order. Shared by the consistency
    /// tests here and the registry-vs-dispatch cross-check in `cmd_txn`. It IS the single
    /// hand-listed dispatch-arm list (see `cmd_txn::tests::table_covers_every_dispatch_arm`
    /// for why ONE hand-list of the dispatch handler arms is the lone remaining hand-sync).
    pub(crate) fn all_registry_names() -> Vec<&'static [u8]> {
        crate::cmd_txn::tests::dispatch_arm_names().to_vec()
    }
}
