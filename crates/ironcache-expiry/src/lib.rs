// SPDX-License-Identifier: MIT OR Apache-2.0
//! The per-shard hierarchical timing wheel for active TTL reclamation
//! (EXPIRATION.md #51, ADR-0002/0003/0005).
//!
//! ## What this is (and is not)
//!
//! This wheel is the ACTIVE-EXPIRY OPTIMIZATION described in EXPIRATION.md: it lets
//! the owning core find the keys whose deadline has passed in `O(due keys)`, with no
//! random sampling and no scan of unrelated keys, so resident memory for
//! expired-but-not-yet-reclaimed keys stays bounded under traffic. It is NOT the
//! correctness guarantee: the lazy expiry-on-read backstop already in
//! `ironcache-store` (`now > expire_at` on every read/rmw/contains/type_of) is what
//! guarantees a client never OBSERVES an expired key. The wheel only bounds the
//! memory those reaped-on-read keys would otherwise hold while idle.
//!
//! ## Why a stale wheel entry is harmless (no kvobj back-pointer)
//!
//! Because the lazy backstop is the correctness guarantee, the wheel need NOT be
//! perfectly consistent with the store's real `expire_at`. A key that was re-TTL'd
//! (EXPIRE again), PERSISTed, or overwritten still has its OLD deadline registered
//! in the wheel, but the active drain RE-CHECKS the store's real `expire_at` before
//! deleting: a key whose stored deadline is not actually past `now` is skipped, and a
//! key that no longer exists is a no-op. So a stale registration costs at most one
//! wasted store probe; it never deletes a live key. This is precisely why the kvobj
//! carries NO wheel back-pointer (no slot index to keep in sync, OBJECT_LAYOUT.md):
//! registrations are append-only and self-correcting.
//!
//! ## Structure: a hashed hierarchical wheel
//!
//! Four levels of [`WHEEL_SIZE`] slots each, with the bottom level at
//! [`TICK_MS`]-millisecond resolution (the slot resolution / level count is a
//! #8/EXPIRATION.md harness-tunable; the consts here are the documented defaults).
//! A deadline lands in the lowest level whose span still contains it; as the wheel
//! advances, entries CASCADE down from coarse levels into finer ones until they
//! reach level 0, where advancing a tick yields the keys due in that tick. This is
//! the classic hashed-hierarchical timing wheel (Varghese and Lauck): O(1)
//! registration and O(due) extraction without an O(keyspace) scan.
//!
//! ## Determinism and shared-nothing (ADR-0002/0003/0005)
//!
//! Time enters ONLY as the `now: UnixMillis` argument the owning core passes (from
//! the Env clock at the binary edge); this crate imports neither `std::time` nor
//! `rand`. The wheel is per-shard and unsynchronized: plain owned `Vec`/`VecDeque`
//! fields, no `std::sync` lock, no atomic, no interior mutability beyond `&mut self`
//! (the server owns it as `Rc<RefCell<TimingWheel>>`, exactly like the store/env).

#![forbid(unsafe_code)]

use std::collections::VecDeque;

use ironcache_storage::UnixMillis;

/// The bottom-level slot resolution in milliseconds (EXPIRATION.md "wheel
/// granularity", a #8 harness-tunable). A deadline is bucketed at this resolution,
/// so the active drain may lag a key's true deadline by up to one tick; the lazy
/// backstop still prevents OBSERVING the key in that window, so this only affects
/// how promptly idle memory is reclaimed, never correctness. 100ms balances wheel
/// memory against reclamation latency.
pub const TICK_MS: u64 = 100;

/// Slots per wheel level. A power of two so the slot index is a cheap mask, and so
/// the four levels span `TICK_MS * WHEEL_SIZE^4` milliseconds before a deadline
/// overflows the top level (with `TICK_MS=100`, `WHEEL_SIZE=64`: ~1.6e9 seconds,
/// ~52 years), comfortably past any practical TTL.
pub const WHEEL_SIZE: usize = 64;

/// The number of hierarchical levels.
pub const LEVELS: usize = 4;

/// A registered expiry: the `(db, key)` the active drain will offer for reclamation
/// once its tick is reached. The deadline is carried so cascading can re-bucket it
/// at a finer level without re-deriving it.
#[derive(Debug, Clone)]
struct Pending {
    db: u32,
    key: Box<[u8]>,
    /// The absolute deadline this entry was registered for, as a tick index
    /// (`deadline_ms / TICK_MS`). Cascading re-buckets against this.
    tick: u64,
}

/// A per-shard hierarchical timing wheel keyed by absolute deadline ([`UnixMillis`]).
///
/// The owning core registers a deadline whenever a command sets a TTL and drains due
/// entries with [`TimingWheel::advance`]. See the module docs for why a stale entry
/// is harmless (the active drain re-checks the store's real `expire_at`).
#[derive(Debug)]
pub struct TimingWheel {
    /// `slots[level][slot]` is the queue of pending entries bucketed at that level.
    /// Level 0 is the finest ([`TICK_MS`] per slot); each higher level's slot spans
    /// `WHEEL_SIZE` times the level below it.
    slots: Vec<Vec<VecDeque<Pending>>>,
    /// The current logical tick the wheel has advanced to (`now_ms / TICK_MS`). An
    /// entry whose tick is `<= current_tick` has had its deadline reached. `None`
    /// until the first `advance` establishes the time base; the first advance does
    /// not fire anything (it only sets the origin), matching the Valkey
    /// `now > deadline` boundary, where a key registered for the current tick is not
    /// yet due.
    current_tick: Option<u64>,
    /// The number of entries currently registered (for tests/introspection and the
    /// `is_empty` fast path).
    len: usize,
}

impl Default for TimingWheel {
    fn default() -> Self {
        Self::new()
    }
}

impl TimingWheel {
    /// A fresh, empty wheel with no time base yet (the first [`Self::advance`]
    /// establishes the origin tick).
    #[must_use]
    pub fn new() -> Self {
        let mut slots = Vec::with_capacity(LEVELS);
        for _ in 0..LEVELS {
            let mut level = Vec::with_capacity(WHEEL_SIZE);
            for _ in 0..WHEEL_SIZE {
                level.push(VecDeque::new());
            }
            slots.push(level);
        }
        TimingWheel {
            slots,
            current_tick: None,
            len: 0,
        }
    }

    /// The number of entries currently registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the wheel holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The span, in ticks, of one slot at `level` (`WHEEL_SIZE^level`).
    fn level_span(level: usize) -> u64 {
        (WHEEL_SIZE as u64).pow(level as u32)
    }

    /// Pick the level and slot for an entry whose deadline is `tick`, given the
    /// wheel is currently at `base` ticks. The entry lands in the LOWEST level whose
    /// per-slot span still distinguishes its remaining delay; a deadline already at
    /// or behind `base` is bucketed into level 0's current slot so the next advance
    /// offers it immediately.
    fn position(base: u64, tick: u64) -> (usize, usize) {
        let delay = tick.saturating_sub(base);
        // Find the finest level whose total span (WHEEL_SIZE slots) covers `delay`.
        for level in 0..LEVELS {
            let span = Self::level_span(level);
            if delay < span * WHEEL_SIZE as u64 {
                // Slot within this level: which of the WHEEL_SIZE buckets at this
                // resolution the absolute `tick` falls into.
                let slot = ((tick / span) % WHEEL_SIZE as u64) as usize;
                return (level, slot);
            }
        }
        // Beyond the top level's reach: clamp into the top level's slot for `tick`
        // (a deadline ~52 years out with the defaults; it cascades down as time
        // approaches). Correctness is unaffected because the lazy backstop governs.
        let top = LEVELS - 1;
        let span = Self::level_span(top);
        let slot = ((tick / span) % WHEEL_SIZE as u64) as usize;
        (top, slot)
    }

    /// Register `(db, key)` to be offered for reclamation once `deadline` has passed.
    ///
    /// Registration is append-only and need not be consistent with the store's real
    /// `expire_at` (the active drain re-checks before deleting; see the module docs),
    /// so a re-TTL or PERSIST simply leaves a harmless stale entry rather than
    /// requiring a wheel update. Cheap: O(1) amortized.
    pub fn register(&mut self, db: u32, key: &[u8], deadline: UnixMillis) {
        let tick = deadline.0 / TICK_MS;
        let base = self.current_tick.unwrap_or(tick);
        let (level, slot) = Self::position(base, tick);
        self.slots[level][slot].push_back(Pending {
            db,
            key: key.to_vec().into_boxed_slice(),
            tick,
        });
        self.len += 1;
    }

    /// Advance the wheel to `now` and pop up to `max` entries whose deadline has
    /// strictly passed (the Valkey boundary `now > deadline`, i.e. tick strictly
    /// before the current `now` tick); entries exactly at `now == deadline` stay.
    /// Returns the `(db, key)` pairs the caller should attempt to reclaim (after
    /// re-checking the store's real `expire_at`).
    ///
    /// Cascades coarse-level entries down into finer levels as ticks elapse. The
    /// `max` bound keeps the drain off the command-path critical section: the caller
    /// uses a small cap (e.g. `MAX_RECLAIM_PER_CALL`) so a flood of co-expiring keys
    /// is reclaimed across several calls instead of stalling one command. Entries not
    /// returned this call remain bucketed in level 0's elapsed slots and are returned
    /// by a subsequent advance.
    ///
    /// The first advance only establishes the time origin (it sets `current_tick`
    /// and returns nothing), so a deadline registered before any advance is measured
    /// against real elapsed ticks rather than firing spuriously at startup.
    pub fn advance(&mut self, now: UnixMillis, max: usize) -> Vec<(u32, Box<[u8]>)> {
        let now_tick = now.0 / TICK_MS;
        let Some(mut cur) = self.current_tick else {
            self.current_tick = Some(now_tick);
            return Vec::new();
        };
        if max == 0 {
            // Nothing to pop; do not move the clock so a later non-zero-max call still
            // cascades these ticks. (A zero-budget drain is a no-op.)
            return Vec::new();
        }

        let mut due: Vec<(u32, Box<[u8]>)> = Vec::new();

        // Walk tick by tick from the current position up to (but NOT including)
        // now_tick: a key with deadline tick T is due only once we have advanced PAST
        // T (now_tick > T), matching `now > deadline`. At each elapsed tick we cascade
        // higher levels down, then drain level 0's slot for that tick.
        while cur < now_tick {
            // The tick we are about to retire is `cur` (entries due strictly before
            // now_tick). Cascade higher levels at level boundaries so their entries
            // reach finer slots before we drain.
            self.cascade(cur);

            let slot = (cur % WHEEL_SIZE as u64) as usize;
            // Drain this slot up to the remaining budget.
            while due.len() < max {
                let Some(p) = self.slots[0][slot].pop_front() else {
                    break;
                };
                self.len -= 1;
                due.push((p.db, p.key));
            }
            if due.len() >= max {
                // Budget exhausted mid-slot: keep `current_tick` at `cur` so the
                // remaining entries in this (and later) elapsed slots are drained by
                // the next call. We have NOT finished retiring `cur`.
                self.current_tick = Some(cur);
                return due;
            }
            cur += 1;
        }

        self.current_tick = Some(cur);
        due
    }

    /// Cascade entries from higher levels down toward level 0 as the wheel reaches a
    /// level boundary at `tick`. When `tick` crosses a slot boundary of level
    /// `level >= 1` (i.e. `tick` is a multiple of that level's span), the entries in
    /// that level's now-current slot are re-bucketed into the appropriate finer level
    /// against the new base, so they land in level 0 by the time their tick is
    /// retired.
    fn cascade(&mut self, tick: u64) {
        for level in 1..LEVELS {
            let span = Self::level_span(level);
            // Only re-bucket this level when `tick` is at one of its slot boundaries
            // (its resolution): between boundaries the level's contents are not yet
            // ready to move down.
            if tick % span != 0 {
                continue;
            }
            let slot = ((tick / span) % WHEEL_SIZE as u64) as usize;
            if self.slots[level][slot].is_empty() {
                continue;
            }
            let drained: Vec<Pending> = self.slots[level][slot].drain(..).collect();
            for p in drained {
                // Re-bucket against the new base `tick`; an entry whose deadline is
                // now within a finer level moves there, otherwise it stays at this
                // resolution one slot further along (handled by position()).
                let (new_level, new_slot) = Self::position(tick, p.tick);
                self.slots[new_level][new_slot].push_back(p);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(t: u64) -> UnixMillis {
        UnixMillis(t)
    }

    fn drain_all(w: &mut TimingWheel, now: UnixMillis, max: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let batch = w.advance(now, max);
            if batch.is_empty() {
                break;
            }
            for (_, k) in batch {
                out.push(k.into_vec());
            }
        }
        out
    }

    #[test]
    fn first_advance_sets_origin_and_fires_nothing() {
        let mut w = TimingWheel::new();
        w.register(0, b"k", ms(50));
        // Before any time base, the first advance only establishes the origin.
        let fired = w.advance(ms(10_000), 100);
        assert!(fired.is_empty(), "first advance must not fire");
        assert_eq!(w.len(), 1, "the entry is still registered");
    }

    #[test]
    fn alive_at_deadline_due_one_tick_past() {
        // Valkey boundary `now > deadline`: a key with deadline D is ALIVE at now==D
        // and due only once now strictly exceeds D by at least one tick.
        let mut w = TimingWheel::new();
        // Establish the origin at t=0.
        assert!(w.advance(ms(0), 100).is_empty());
        // Deadline at exactly 100ms (tick 1).
        w.register(0, b"k", ms(100));
        // At now == 100 (tick 1): the key's tick (1) is not strictly before now_tick
        // (1), so it is NOT due.
        assert!(w.advance(ms(100), 100).is_empty(), "alive at deadline");
        // At now == 200 (tick 2): now_tick 2 > deadline tick 1, so it fires.
        let fired = w.advance(ms(200), 100);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].1.as_ref(), b"k");
        assert!(w.is_empty());
    }

    #[test]
    fn advance_yields_exactly_due_keys_none_early() {
        let mut w = TimingWheel::new();
        assert!(w.advance(ms(0), 100).is_empty());
        w.register(0, b"soon", ms(100)); // tick 1
        w.register(0, b"later", ms(1_000)); // tick 10
        // Advance to t=500 (tick 5): only "soon" (tick 1 < 5) is due; "later" is not.
        let fired = w.advance(ms(500), 100);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].1.as_ref(), b"soon");
        // "later" is still registered.
        assert_eq!(w.len(), 1);
        // Advance past tick 10 -> "later" fires.
        let fired2 = w.advance(ms(1_100), 100);
        assert_eq!(fired2.len(), 1);
        assert_eq!(fired2[0].1.as_ref(), b"later");
        assert!(w.is_empty());
    }

    #[test]
    fn bounded_drain_takes_ceil_n_over_m_advances() {
        // N due entries, max M per advance => ceil(N/M) advances to drain all.
        let mut w = TimingWheel::new();
        assert!(w.advance(ms(0), 100).is_empty());
        let n = 25usize;
        for i in 0..n {
            // All share the same tick (1) so they are all due at once.
            w.register(0, format!("k{i}").as_bytes(), ms(100));
        }
        let m = 10usize;
        let now = ms(10_000);
        let mut advances = 0;
        let mut total = 0;
        loop {
            let batch = w.advance(now, m);
            if batch.is_empty() {
                break;
            }
            advances += 1;
            total += batch.len();
            assert!(batch.len() <= m, "each advance respects the max bound");
        }
        assert_eq!(total, n, "all entries eventually drained");
        // ceil(25/10) = 3.
        assert_eq!(advances, 3, "ceil(N/M) advances");
        assert!(w.is_empty());
    }

    #[test]
    fn cascade_across_levels_brings_distant_deadline_due() {
        // A deadline far enough out to land in a higher level must cascade down and
        // still fire at the right tick.
        let mut w = TimingWheel::new();
        assert!(w.advance(ms(0), 100).is_empty());
        // Level 0 spans TICK_MS*WHEEL_SIZE = 100*64 = 6400ms. Pick a deadline beyond
        // that so it starts in level 1+.
        let deadline = TICK_MS * (WHEEL_SIZE as u64) * 3; // tick 192, in level 1
        w.register(0, b"far", ms(deadline));
        // Not due before the deadline tick.
        assert!(
            w.advance(ms(deadline), 100).is_empty(),
            "alive at the deadline tick"
        );
        // Due once strictly past (advance one full tick beyond).
        let fired = w.advance(ms(deadline + TICK_MS), 100);
        assert_eq!(fired.len(), 1, "cascaded entry fires after its deadline");
        assert_eq!(fired[0].1.as_ref(), b"far");
        assert!(w.is_empty());
    }

    #[test]
    fn many_distinct_deadlines_fire_in_order_across_levels() {
        let mut w = TimingWheel::new();
        assert!(w.advance(ms(0), 100).is_empty());
        // A spread of deadlines crossing level boundaries.
        let deadlines = [
            100u64,    // level 0
            6_400,     // level 0 edge / level 1 start
            12_800,    // level 1
            409_600,   // level 2 range
            1_000_000, // level 2/3
        ];
        for (i, d) in deadlines.iter().enumerate() {
            w.register(0, format!("k{i}").as_bytes(), ms(*d));
        }
        // Drain fully at a time well past the largest deadline.
        let fired = drain_all(&mut w, ms(2_000_000), 100);
        assert_eq!(fired.len(), deadlines.len(), "all distinct deadlines fired");
        assert!(w.is_empty());
    }

    #[test]
    fn determinism_under_fixed_now_sequence() {
        // The same registrations replayed against the same `now` sequence yield the
        // identical due-key sequence (ADR-0003: the wheel reads time only via `now`).
        let run = || -> Vec<(u32, Vec<u8>)> {
            let mut w = TimingWheel::new();
            w.advance(ms(0), 100);
            for i in 0..50u32 {
                w.register(i % 3, format!("key{i}").as_bytes(), ms(100 + u64::from(i) * 37));
            }
            let mut out = Vec::new();
            // A fixed sequence of advances.
            for step in [200u64, 600, 1_500, 3_000, 10_000] {
                for (db, k) in w.advance(ms(step), 7) {
                    out.push((db, k.into_vec()));
                }
                // Drain the rest at this step with the same bounded cap.
                loop {
                    let b = w.advance(ms(step), 7);
                    if b.is_empty() {
                        break;
                    }
                    for (db, k) in b {
                        out.push((db, k.into_vec()));
                    }
                }
            }
            out
        };
        let a = run();
        let b = run();
        assert_eq!(a, b, "identical now sequence => identical due sequence");
        assert_eq!(a.len(), 50, "every registered key eventually fired");
    }

    #[test]
    fn zero_max_is_a_noop_and_preserves_entries() {
        let mut w = TimingWheel::new();
        w.advance(ms(0), 100);
        w.register(0, b"k", ms(100));
        let fired = w.advance(ms(10_000), 0);
        assert!(fired.is_empty(), "max==0 fires nothing");
        assert_eq!(w.len(), 1, "max==0 keeps the entry");
        // A later non-zero drain still gets it.
        let fired2 = w.advance(ms(10_000), 100);
        assert_eq!(fired2.len(), 1);
    }
}
