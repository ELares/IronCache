// SPDX-License-Identifier: MIT OR Apache-2.0
//! Keyspace notifications (PROD-8, SERVER_PUSH.md "keyspace notifications off by
//! default", the pinned claim [keyspace-notifications-off-by-default]).
//!
//! On a successful key MUTATION (and on a TTL expiry / a maxmemory eviction), Redis
//! optionally PUBLISHes two events, gated by the `notify-keyspace-events` config:
//!
//! - the KEYSPACE event, on channel `__keyspace@<db>__:<key>`, payload = the EVENT name
//!   (e.g. `set`, `del`, `lpush`, `expired`);
//! - the KEYEVENT event, on channel `__keyevent@<db>__:<event>`, payload = the KEY.
//!
//! Whether either fires is driven by the `notify-keyspace-events` FLAG STRING: `K`
//! turns the keyspace channel on, `E` the keyevent channel, and the per-class letters
//! (`g $ l s h z x e t d m n`, with `A` an alias for `g$lshzxet`) select WHICH event
//! classes are reported. An EMPTY flag set means notifications are DISABLED (the Redis
//! default), and the emit helpers short-circuit on that single check so the default
//! deployment is byte-identical and pays zero hot-path cost.
//!
//! ## What this module owns vs what the engine owns
//!
//! This module owns (a) [`NotifyFlags`]: the parse/render of the flag string into a
//! compact bitset, with the canonical Redis re-emit order; and (b) a PER-SHARD
//! thread-local [`PENDING`] buffer the engine RECORDS events into during a command and
//! DRAINS after the reply, plus the free functions over it ([`record`], [`drain`],
//! [`flags_for_command`]). The engine (the `ironcache-server` command handlers + the
//! `ironcache-store` expiry/eviction paths) calls [`record`] after a successful
//! mutation; the serve layer reads [`flags_for_command`] / [`drain`] and PUBLISHes each
//! recorded event through the existing Pub/Sub fan-out. Keeping the buffer here (the one
//! crate both the command layer AND the store depend on) avoids a new cross-crate edge
//! while staying lock-free (a thread-local, ADR-0002 shared-nothing).

use std::cell::{Cell, RefCell};

/// The `notify-keyspace-events` flag bits (PROD-8). A compact bitset parsed from /
/// rendered to the Redis flag STRING (`K`/`E` + the class letters). The whole set
/// fits in a `u16`; it is stored as a `u32` in the runtime overlay's atomic so the
/// per-command hot-path read is a single relaxed load (0 == disabled).
///
/// The class letters and their meaning (matching Redis `keyspaceEventsStringToFlags`,
/// the pinned claim [keyspace-notifications-off-by-default]):
/// - `K` keyspace (publish to `__keyspace@db__:<key>`)
/// - `E` keyevent (publish to `__keyevent@db__:<event>`)
/// - `g` generic (DEL / EXPIRE / RENAME / MOVE / COPY / PERSIST / RESTORE ...)
/// - `$` string  (SET / SETRANGE / INCRBY / APPEND / GETSET / GETDEL ...)
/// - `l` list    (LPUSH / RPUSH / LPOP / RPOP / LSET / LINSERT / LREM ...)
/// - `s` set     (SADD / SREM / SPOP / SINTERSTORE ...)
/// - `h` hash    (HSET / HDEL / HINCRBY ...)
/// - `z` zset    (ZADD / ZREM / ZINCR / ZPOPMIN ...)
/// - `x` expired (a key removed because its TTL passed -> the `expired` event)
/// - `e` evicted (a key removed by the maxmemory policy -> the `evicted` event)
/// - `n` new key (a key created where none existed -> the `new` event; EXCLUDED from `A`)
/// - `m` key miss (a read of an absent key -> the `keymiss` event; EXCLUDED from `A`)
/// - `t` stream / `d` module: recognized in the flag string for Redis parity but NEVER
///   fire here (IronCache has no streams or modules; documented as skipped).
/// - `A` alias = `g$lshzxet` (everything EXCEPT `K`/`E`/`m`/`n`, exactly Redis's `A`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotifyFlags(u16);

// The bit layout. `K`/`E` are the two channel selectors; the rest are event classes.
const F_KEYSPACE: u16 = 1 << 0; // K
const F_KEYEVENT: u16 = 1 << 1; // E
const F_GENERIC: u16 = 1 << 2; // g
const F_STRING: u16 = 1 << 3; // $
const F_LIST: u16 = 1 << 4; // l
const F_SET: u16 = 1 << 5; // s
const F_HASH: u16 = 1 << 6; // h
const F_ZSET: u16 = 1 << 7; // z
const F_EXPIRED: u16 = 1 << 8; // x
const F_EVICTED: u16 = 1 << 9; // e
const F_STREAM: u16 = 1 << 10; // t (recognized, never fires)
const F_KEY_MISS: u16 = 1 << 11; // m
const F_MODULE: u16 = 1 << 12; // d (recognized, never fires)
const F_NEW: u16 = 1 << 13; // n

/// The `A` alias expansion: `g$lshzxet` (Redis EXCLUDES `K`, `E`, `m`, and `n`).
const F_ALL_ALIAS: u16 =
    F_GENERIC | F_STRING | F_LIST | F_SET | F_HASH | F_ZSET | F_EXPIRED | F_EVICTED | F_STREAM;

/// The one event-class letter a [`record`] call carries (the `class` argument). Maps
/// to a single class bit so the emit gate is one mask test. Kept as a small enum (not
/// a raw char) so a handler cannot pass an unrecognized class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventClass {
    /// `g` generic (DEL / EXPIRE / RENAME / PERSIST / MOVE / COPY).
    Generic,
    /// `$` string.
    String,
    /// `l` list.
    List,
    /// `s` set.
    Set,
    /// `h` hash.
    Hash,
    /// `z` zset.
    Zset,
    /// `x` expired (a TTL reap).
    Expired,
    /// `e` evicted (a maxmemory eviction).
    Evicted,
    /// `n` new key.
    New,
    /// `m` key miss.
    KeyMiss,
}

impl EventClass {
    /// The class bit this event belongs to (the bit `notify-keyspace-events` must have
    /// set for the event to fire).
    const fn bit(self) -> u16 {
        match self {
            EventClass::Generic => F_GENERIC,
            EventClass::String => F_STRING,
            EventClass::List => F_LIST,
            EventClass::Set => F_SET,
            EventClass::Hash => F_HASH,
            EventClass::Zset => F_ZSET,
            EventClass::Expired => F_EXPIRED,
            EventClass::Evicted => F_EVICTED,
            EventClass::New => F_NEW,
            EventClass::KeyMiss => F_KEY_MISS,
        }
    }
}

impl NotifyFlags {
    /// The disabled (empty) flag set: the default. Notifications are OFF and the emit
    /// helpers short-circuit. [`Self::is_enabled`] is false.
    #[must_use]
    pub const fn empty() -> Self {
        NotifyFlags(0)
    }

    /// Reconstruct from the raw bits stored in the runtime overlay's atomic (the inverse
    /// of [`Self::bits`]). Unknown high bits are masked off defensively so a corrupt
    /// store can never select a class that does not exist.
    #[must_use]
    pub const fn from_bits(bits: u32) -> Self {
        // Mask to the 14 defined bits; anything else is ignored.
        NotifyFlags((bits as u16) & 0x3FFF)
    }

    /// The raw bits for storing in the runtime overlay's atomic.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0 as u32
    }

    /// Whether notifications are ENABLED. Redis treats the set as active only when a
    /// CHANNEL selector (`K` or `E`) AND at least one event class are present: a flag
    /// string of only `K` (no class) or only `g` (no channel) publishes nothing. This
    /// is the single check the hot-path emit gate makes; an empty/channel-only/class-only
    /// set returns false, so the default deployment never builds a channel or publishes.
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        let has_channel = self.0 & (F_KEYSPACE | F_KEYEVENT) != 0;
        let has_class = self.0 & !(F_KEYSPACE | F_KEYEVENT) != 0;
        has_channel && has_class
    }

    /// Whether the keyspace channel (`K`) is selected (publish `__keyspace@db__:<key>`).
    #[must_use]
    pub const fn keyspace(self) -> bool {
        self.0 & F_KEYSPACE != 0
    }

    /// Whether the keyevent channel (`E`) is selected (publish `__keyevent@db__:<event>`).
    #[must_use]
    pub const fn keyevent(self) -> bool {
        self.0 & F_KEYEVENT != 0
    }

    /// Whether `class` is selected (its class bit is set), i.e. an event of that class
    /// should fire. Combined with [`Self::is_enabled`] this is the full emit decision.
    #[must_use]
    pub const fn has_class(self, class: EventClass) -> bool {
        self.0 & class.bit() != 0
    }

    /// Parse a `notify-keyspace-events` flag STRING into the bitset (the `CONFIG SET`
    /// path), matching Redis `keyspaceEventsStringToFlags`. Each character toggles a
    /// flag; `A` expands to `g$lshzxet`. An EMPTY string is the disabled set. Returns
    /// `Err` with the offending character on any UNRECOGNIZED flag (Redis rejects a bad
    /// `CONFIG SET notify-keyspace-events` with an error rather than silently ignoring).
    ///
    /// # Errors
    /// Returns the unrecognized flag character when `s` contains a byte that is not one
    /// of `KEg$lshzxetdmnA`.
    pub fn parse(s: &str) -> Result<Self, char> {
        let mut bits: u16 = 0;
        for ch in s.chars() {
            bits |= match ch {
                'K' => F_KEYSPACE,
                'E' => F_KEYEVENT,
                'g' => F_GENERIC,
                '$' => F_STRING,
                'l' => F_LIST,
                's' => F_SET,
                'h' => F_HASH,
                'z' => F_ZSET,
                'x' => F_EXPIRED,
                'e' => F_EVICTED,
                't' => F_STREAM,
                'm' => F_KEY_MISS,
                'd' => F_MODULE,
                'n' => F_NEW,
                'A' => F_ALL_ALIAS,
                other => return Err(other),
            };
        }
        Ok(NotifyFlags(bits))
    }

    /// Render the bitset back to the canonical Redis flag STRING (the `CONFIG GET` path),
    /// matching Redis `keyspaceEventsFlagsToString`. Redis re-emits a CANONICAL form: if
    /// every `A`-class bit is set it writes `A` (collapsing `g$lshzxet`), then any class
    /// letter NOT covered by `A` (`m`/`n`/`d` -- note `d` is in `A`? no: Redis's `A` is
    /// `g$lshzxet`, so `d` is NOT in `A` and is rendered separately), then `K`/`E` LAST.
    /// An empty set renders the empty string.
    #[must_use]
    pub fn render(self) -> String {
        let mut out = String::new();
        // The class letters first. If the full A-alias set is present, collapse to `A`;
        // otherwise emit each present class letter in the canonical g$lshzxet order.
        if self.0 & F_ALL_ALIAS == F_ALL_ALIAS {
            out.push('A');
        } else {
            for (bit, ch) in [
                (F_GENERIC, 'g'),
                (F_STRING, '$'),
                (F_LIST, 'l'),
                (F_SET, 's'),
                (F_HASH, 'h'),
                (F_ZSET, 'z'),
                (F_EXPIRED, 'x'),
                (F_EVICTED, 'e'),
                (F_STREAM, 't'),
            ] {
                if self.0 & bit != 0 {
                    out.push(ch);
                }
            }
        }
        // Classes EXCLUDED from the `A` alias are always rendered explicitly (whether or
        // not `A` collapsed the rest): `d` (module), `m` (key-miss), `n` (new-key).
        for (bit, ch) in [(F_MODULE, 'd'), (F_KEY_MISS, 'm'), (F_NEW, 'n')] {
            if self.0 & bit != 0 {
                out.push(ch);
            }
        }
        // The channel selectors LAST (Redis appends K then E after the class letters).
        if self.0 & F_KEYSPACE != 0 {
            out.push('K');
        }
        if self.0 & F_KEYEVENT != 0 {
            out.push('E');
        }
        out
    }
}

/// One recorded keyspace event awaiting fan-out (the PER-SHARD buffer entry). Carries the
/// EVENT name (the keyspace-channel payload / the keyevent-channel suffix), the KEY (the
/// keyspace-channel suffix / the keyevent-channel payload), the logical DB, and which
/// channels to publish on (resolved from the live flags at record time, so the drain side
/// needs no flags). All owned (`String`/`Vec<u8>`) so the entry crosses the
/// record-shard -> publish boundary without borrowing the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceEvent {
    /// The logical DB the mutation happened in (the `@<db>` in the channel name).
    pub db: u32,
    /// The canonical Redis event name (e.g. `set`, `del`, `lpush`, `expired`). The
    /// keyspace channel's PAYLOAD and the keyevent channel's name SUFFIX.
    pub event: &'static str,
    /// The mutated key. The keyspace channel's name SUFFIX and the keyevent channel's
    /// PAYLOAD. Binary-safe (raw bytes), matching the key's wire form.
    pub key: Vec<u8>,
    /// Whether to publish the `__keyspace@db__:<key>` event (the `K` flag was set).
    pub keyspace: bool,
    /// Whether to publish the `__keyevent@db__:<event>` event (the `E` flag was set).
    pub keyevent: bool,
}

impl KeyspaceEvent {
    /// The `__keyspace@<db>__:<key>` channel name (the `K` channel). Built only when the
    /// event is actually drained for publishing, so a disabled deployment never allocates.
    #[must_use]
    pub fn keyspace_channel(&self) -> Vec<u8> {
        let mut c = format!("__keyspace@{}__:", self.db).into_bytes();
        c.extend_from_slice(&self.key);
        c
    }

    /// The `__keyevent@<db>__:<event>` channel name (the `E` channel).
    #[must_use]
    pub fn keyevent_channel(&self) -> Vec<u8> {
        format!("__keyevent@{}__:{}", self.db, self.event).into_bytes()
    }
}

thread_local! {
    /// The PER-SHARD live `notify-keyspace-events` flags for the command currently being
    /// dispatched. The serve loop SNAPSHOTS the runtime overlay's flags into this once at
    /// the TOP of each command (one relaxed atomic load + a `Cell` write), so the store +
    /// command handlers read the SAME flags for the whole command without re-loading the
    /// atomic per event. Defaults to the empty (disabled) set, so any code path that runs
    /// before / outside a command (or in a unit test) records nothing.
    static COMMAND_FLAGS: Cell<NotifyFlags> = const { Cell::new(NotifyFlags::empty()) };

    /// The PER-SHARD pending keyspace-event buffer. [`record`] pushes here during a
    /// command (from a handler or the store's expiry/eviction path); the serve loop
    /// [`drain`]s it AFTER encoding the command's reply and PUBLISHes each event. A
    /// thread-local (not a shared structure) keeps it lock-free under shared-nothing
    /// (ADR-0002): each shard records + drains its own buffer.
    static PENDING: RefCell<Vec<KeyspaceEvent>> = const { RefCell::new(Vec::new()) };
}

/// SET the live per-command flags for THIS shard (the serve loop calls this once at the
/// top of each command, from the runtime overlay). A single `Cell` write.
pub fn set_command_flags(flags: NotifyFlags) {
    COMMAND_FLAGS.with(|f| f.set(flags));
}

/// The live per-command flags for THIS shard. A single `Cell` read; the engine reads it
/// to decide whether to even build an event (the zero-cost-when-disabled gate).
#[must_use]
pub fn flags_for_command() -> NotifyFlags {
    COMMAND_FLAGS.with(Cell::get)
}

/// RECORD a keyspace event for `key` of class `class` named `event` in DB `db`, IFF
/// notifications are enabled AND `class` is selected (the ZERO-COST-WHEN-DISABLED gate:
/// the FIRST thing this does is read the per-command flags `Cell` and short-circuit on a
/// disabled / unselected-class set, BEFORE any allocation). When it fires, it pushes one
/// [`KeyspaceEvent`] (with the resolved `K`/`E` channel selectors) into the per-shard
/// [`PENDING`] buffer; the serve loop drains + publishes it after the reply. The `key` is
/// copied into the owned event only on the firing path.
///
/// This is the SINGLE emit entry point the command handlers + the store expiry/eviction
/// paths call, so the gate + the channel-selector resolution live in one place.
pub fn record(class: EventClass, event: &'static str, key: &[u8], db: u32) {
    let flags = flags_for_command();
    // The hot-path short-circuit: a disabled set OR an unselected class records nothing
    // and allocates nothing. On the default deployment (`notify-keyspace-events` empty)
    // `is_enabled()` is false, so this returns after one `Cell` read + two mask tests.
    if !flags.is_enabled() || !flags.has_class(class) {
        return;
    }
    let keyspace = flags.keyspace();
    let keyevent = flags.keyevent();
    // If neither channel is selected there is nothing to publish (is_enabled already
    // guaranteed at least one, but stay defensive).
    if !keyspace && !keyevent {
        return;
    }
    PENDING.with(|p| {
        p.borrow_mut().push(KeyspaceEvent {
            db,
            event,
            key: key.to_vec(),
            keyspace,
            keyevent,
        });
    });
}

/// DRAIN the per-shard pending events (the serve loop calls this AFTER encoding the
/// command's reply, then publishes each through the existing Pub/Sub fan-out). Returns
/// the recorded events in record order (per-connection FIFO: the events of one command
/// publish after that command's reply, SERVER_PUSH.md). The common path (no events) is a
/// single `is_empty` check + an empty Vec return.
#[must_use]
pub fn drain() -> Vec<KeyspaceEvent> {
    PENDING.with(|p| {
        let mut b = p.borrow_mut();
        if b.is_empty() {
            Vec::new()
        } else {
            std::mem::take(&mut *b)
        }
    })
}

/// Whether the per-shard pending buffer has any events to drain (the serve loop's
/// post-reply fast check, so the drain + the async fan-out are skipped entirely when no
/// event was recorded -- the common case). A single `borrow` + `is_empty`.
#[must_use]
pub fn has_pending() -> bool {
    PENDING.with(|p| !p.borrow().is_empty())
}

/// CLEAR the per-shard pending buffer WITHOUT publishing (the serve loop calls this on a
/// path that records events but does NOT reach the publish drain -- e.g. a connection
/// teardown -- so a stale event can never leak into the next command on this shard).
pub fn clear_pending() {
    PENDING.with(|p| p.borrow_mut().clear());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_disabled_and_round_trips() {
        let f = NotifyFlags::parse("").unwrap();
        assert!(!f.is_enabled());
        assert_eq!(f.render(), "");
        assert_eq!(f, NotifyFlags::empty());
        // bits round-trip through the atomic representation.
        assert_eq!(NotifyFlags::from_bits(f.bits()), f);
    }

    #[test]
    fn kea_parses_and_renders_canonically() {
        // KEA = keyspace + keyevent + the A-alias classes. Renders canonically as `AKE`
        // (the A-alias collapses the classes; K then E last), matching Redis.
        let f = NotifyFlags::parse("KEA").unwrap();
        assert!(f.is_enabled());
        assert!(f.keyspace());
        assert!(f.keyevent());
        assert!(f.has_class(EventClass::String));
        assert!(f.has_class(EventClass::List));
        assert!(f.has_class(EventClass::Expired));
        assert!(f.has_class(EventClass::Evicted));
        // `A` EXCLUDES new + key-miss (Redis semantics).
        assert!(!f.has_class(EventClass::New));
        assert!(!f.has_class(EventClass::KeyMiss));
        assert_eq!(f.render(), "AKE");
        // Re-parsing the rendered form is idempotent.
        assert_eq!(NotifyFlags::parse(&f.render()).unwrap(), f);
    }

    #[test]
    fn channel_only_or_class_only_is_not_enabled() {
        // Only K (no class) publishes nothing; only g (no channel) publishes nothing.
        assert!(!NotifyFlags::parse("K").unwrap().is_enabled());
        assert!(!NotifyFlags::parse("E").unwrap().is_enabled());
        assert!(!NotifyFlags::parse("g").unwrap().is_enabled());
        assert!(!NotifyFlags::parse("g$lshzxe").unwrap().is_enabled());
        // K + one class IS enabled.
        assert!(NotifyFlags::parse("Kg").unwrap().is_enabled());
    }

    #[test]
    fn partial_classes_render_in_canonical_order() {
        // A subset of classes renders in the g$lshzxet order, channels last; no `A`.
        let f = NotifyFlags::parse("Elg").unwrap();
        assert_eq!(f.render(), "glE");
        // Re-parse idempotent.
        assert_eq!(NotifyFlags::parse(&f.render()).unwrap(), f);
        // K$ -> string only, keyspace channel.
        let f = NotifyFlags::parse("K$").unwrap();
        assert_eq!(f.render(), "$K");
    }

    #[test]
    fn module_keymiss_new_render_outside_the_alias() {
        // m (key-miss), n (new), d (module) are NOT in the A alias and render explicitly
        // even alongside A.
        let f = NotifyFlags::parse("KEAmn").unwrap();
        assert_eq!(f.render(), "AmnKE");
        assert!(f.has_class(EventClass::New));
        assert!(f.has_class(EventClass::KeyMiss));
    }

    #[test]
    fn parse_rejects_unknown_flag() {
        assert_eq!(NotifyFlags::parse("KEQ"), Err('Q'));
        assert_eq!(NotifyFlags::parse("w"), Err('w'));
    }

    #[test]
    fn record_short_circuits_when_disabled() {
        // Default (disabled) flags: record does nothing, nothing pends.
        set_command_flags(NotifyFlags::empty());
        clear_pending();
        record(EventClass::String, "set", b"k", 0);
        assert!(!has_pending());
        assert!(drain().is_empty());
    }

    #[test]
    fn record_fires_for_selected_class_only() {
        // KE$ = keyspace + keyevent + string class only.
        set_command_flags(NotifyFlags::parse("KE$").unwrap());
        clear_pending();
        // A string event fires (string class selected).
        record(EventClass::String, "set", b"k", 0);
        // A list event does NOT (list class not selected).
        record(EventClass::List, "lpush", b"l", 0);
        let events = drain();
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.event, "set");
        assert_eq!(e.key, b"k");
        assert!(e.keyspace && e.keyevent);
        assert_eq!(e.keyspace_channel(), b"__keyspace@0__:k");
        assert_eq!(e.keyevent_channel(), b"__keyevent@0__:set");
        // Drained: the buffer is empty again.
        assert!(!has_pending());
        // Cleanup so the thread-local does not leak into another test on this thread.
        set_command_flags(NotifyFlags::empty());
    }

    #[test]
    fn keyevent_only_skips_keyspace_channel() {
        // Elg = keyevent + list + generic (no K). A list event records keyevent-only.
        set_command_flags(NotifyFlags::parse("Elg").unwrap());
        clear_pending();
        record(EventClass::List, "lpush", b"mylist", 3);
        let events = drain();
        assert_eq!(events.len(), 1);
        assert!(!events[0].keyspace);
        assert!(events[0].keyevent);
        assert_eq!(events[0].keyevent_channel(), b"__keyevent@3__:lpush");
        set_command_flags(NotifyFlags::empty());
    }
}
