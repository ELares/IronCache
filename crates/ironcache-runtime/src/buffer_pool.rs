// SPDX-License-Identifier: MIT OR Apache-2.0
//! The per-shard fixed-buffer group pool bookkeeping (#284 io_uring datapath, IOURING_DATAPATH.md).
//!
//! The io_uring fast path registers ONE contiguous slab with the ring at shard init and splits it
//! into a kernel buffer group of `count` fixed-size buffers (IOURING_DATAPATH.md "Registered
//! fixed-buffer slab and buffer groups"). On a recv completion the kernel returns the id of the
//! buffer it filled; the shard parses the request in place and RETURNS that buffer to the group on
//! reply completion. Because no buffer ever leaves the shard, the pool needs no synchronization --
//! it is a single-threaded free/outstanding ledger over buffer IDS.
//!
//! This module is that PURE ledger, separated from the Linux-only ring: it tracks which buffer ids
//! are FREE vs OUTSTANDING (handed to the kernel) and computes each buffer's byte OFFSET into the
//! slab. It owns no memory and makes no syscall -- the actual `io_uring_register_buffers` of the slab
//! and the `[u8]` slicing at `offset(id)` are the thin Linux layer built ON this. So the dangerous
//! bookkeeping -- handing the SAME buffer to the kernel twice (aliasing a live read), releasing a
//! buffer that was never outstanding (a double-free that would later double-issue), or an off-by-one
//! in the id->offset arithmetic -- is validated here, on any host, with `cargo test` (and `miri`,
//! trivially: there is no `unsafe`).
//!
//! It also carries the paired READ BACK-PRESSURE rule (IOURING_DATAPATH.md "Resolved open
//! decisions"): the slab is a FIXED budget, not grown on demand, so when the group is DRAINED the
//! shard must DEFER re-arming recv rather than allocate. [`BufferPool::can_rearm`] encodes exactly
//! that bounded-memory guarantee (never re-arm with zero free buffers).

/// A buffer's id within its group: `0..count`. Doubles as the kernel buffer-group buffer id and the
/// index for the slab-offset arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufId(pub u16);

/// The per-shard fixed-buffer group ledger: `count` buffers of `buf_size` bytes over one slab.
///
/// `acquire` hands out a FREE buffer (marking it outstanding); `release` returns an OUTSTANDING
/// buffer to the free set. The invariants the io_uring fast path depends on -- an outstanding buffer
/// is never handed out again (no aliasing of a live kernel read), a buffer that is not outstanding
/// cannot be released (no double-free), and exhaustion yields `None` rather than reusing a live
/// buffer -- are enforced here.
#[derive(Debug)]
pub struct BufferPool {
    /// The byte size of each buffer in the group (the max a single recv writes before back-pressure).
    buf_size: usize,
    /// The FREE buffer ids, as a stack (LIFO reuse keeps a small hot working set of buffers).
    free: Vec<BufId>,
    /// `in_use[i]` is `true` while buffer `i` is OUTSTANDING (handed to the kernel). The single
    /// source of truth for "is this id currently issued", so a double-release / foreign-release is a
    /// clean rejection rather than silent corruption of the free set.
    in_use: Vec<bool>,
}

impl BufferPool {
    /// A pool of `count` buffers of `buf_size` bytes, all initially free. `count` is a `u16` because
    /// a kernel buffer group is bounded (a few hundred to a few thousand buffers per shard); the id
    /// space fits comfortably.
    ///
    /// # Panics
    ///
    /// Panics if `buf_size` is 0 (a zero-size buffer group is a configuration error) -- callers pass
    /// a real page-multiple buffer size.
    #[must_use]
    pub fn new(count: u16, buf_size: usize) -> Self {
        assert!(buf_size > 0, "buffer size must be non-zero");
        // Push ids in REVERSE so the stack pops 0, 1, 2, ... first: acquisition order is stable +
        // easy to reason about, and a fresh slab is touched front-to-back.
        let free: Vec<BufId> = (0..count).rev().map(BufId).collect();
        BufferPool {
            buf_size,
            free,
            in_use: vec![false; count as usize],
        }
    }

    /// The number of buffers in the group.
    #[must_use]
    pub fn capacity(&self) -> u16 {
        // in_use has one slot per buffer; its length is the group size.
        u16::try_from(self.in_use.len()).expect("count fits u16 by construction")
    }

    /// The number of buffers currently FREE (available to hand to the kernel).
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.free.len()
    }

    /// The total slab size in bytes (`count * buf_size`): the fixed per-shard budget registered with
    /// the ring and counted against the shard's maxmemory share.
    #[must_use]
    pub fn slab_len(&self) -> usize {
        self.in_use.len() * self.buf_size
    }

    /// The byte OFFSET of buffer `id` within the slab (`id * buf_size`). The buffer occupies
    /// `offset(id) .. offset(id) + buf_size`, always within `0..slab_len()`.
    #[must_use]
    pub fn offset(&self, id: BufId) -> usize {
        id.0 as usize * self.buf_size
    }

    /// The byte size of each buffer.
    #[must_use]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Take a FREE buffer, marking it OUTSTANDING, or `None` when the group is drained. On `None` the
    /// shard applies read back-pressure (see [`Self::can_rearm`]) instead of allocating -- the slab
    /// is a fixed budget.
    pub fn acquire(&mut self) -> Option<BufId> {
        let id = self.free.pop()?;
        // Invariant: a popped id is by construction not outstanding.
        debug_assert!(!self.in_use[id.0 as usize], "free buffer was marked in-use");
        self.in_use[id.0 as usize] = true;
        Some(id)
    }

    /// Return an OUTSTANDING buffer `id` to the free set. Returns `true` on success; returns `false`
    /// WITHOUT mutating anything if `id` is out of range or was not outstanding (a double-release or
    /// a foreign id) -- so a buggy caller can never make the same buffer appear twice in the free set
    /// and thus be handed to the kernel twice.
    pub fn release(&mut self, id: BufId) -> bool {
        let Some(slot) = self.in_use.get_mut(id.0 as usize) else {
            return false; // out of range
        };
        if !*slot {
            return false; // not currently outstanding (double / foreign release)
        }
        *slot = false;
        self.free.push(id);
        true
    }

    /// Whether the shard may RE-ARM a recv: only when at least one buffer is free. This is the
    /// bounded-memory guarantee -- with zero free buffers the shard MUST defer re-arming (back-
    /// pressure) rather than allocate beyond the fixed slab, so a read burst cannot blow the memory
    /// bound (IOURING_DATAPATH.md).
    #[must_use]
    pub fn can_rearm(&self) -> bool {
        !self.free.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{BufId, BufferPool};
    use std::collections::HashSet;

    #[test]
    fn acquire_hands_out_distinct_in_range_ids_then_exhausts() {
        let mut pool = BufferPool::new(4, 2048);
        let mut seen = HashSet::new();
        for _ in 0..4 {
            let id = pool.acquire().expect("a free buffer");
            assert!(id.0 < 4, "id in [0, count)");
            assert!(
                seen.insert(id),
                "acquire never hands out an outstanding id twice"
            );
        }
        // Drained: further acquire is None (never a panic, never a reused live id).
        assert_eq!(pool.acquire(), None);
        assert_eq!(pool.free_count(), 0);
        assert!(
            !pool.can_rearm(),
            "no free buffer -> back-pressure, do not re-arm"
        );
    }

    #[test]
    fn release_returns_the_exact_id_and_only_it_is_reusable() {
        let mut pool = BufferPool::new(3, 512);
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        assert!(pool.release(a));
        // The freed id (and no other) is what the next acquire hands back.
        assert_eq!(pool.acquire(), Some(a));
        // `b` is still outstanding; it is NOT re-issued while in use.
        assert_ne!(pool.acquire(), Some(b)); // the remaining fresh id, not b
    }

    #[test]
    fn double_and_foreign_release_are_rejected_without_corruption() {
        let mut pool = BufferPool::new(2, 256);
        let a = pool.acquire().unwrap();
        assert!(pool.release(a), "first release ok");
        // Double release of an already-free id: rejected, free set unchanged (still 2 free).
        assert!(!pool.release(a), "double release rejected");
        assert_eq!(pool.free_count(), 2);
        // A foreign / out-of-range id is rejected.
        assert!(!pool.release(BufId(99)));
        assert!(!pool.release(BufId(1)), "id 1 is free, not outstanding");
        assert_eq!(pool.free_count(), 2);
        // Because double-release did not corrupt the free set, every acquire is still distinct.
        let x = pool.acquire().unwrap();
        let y = pool.acquire().unwrap();
        assert_ne!(x, y);
        assert_eq!(pool.acquire(), None);
    }

    #[test]
    fn offset_arithmetic_is_exact_and_within_the_slab() {
        let pool = BufferPool::new(5, 4096);
        assert_eq!(pool.slab_len(), 5 * 4096);
        for i in 0..5u16 {
            let off = pool.offset(BufId(i));
            assert_eq!(off, i as usize * 4096);
            assert!(
                off + pool.buf_size() <= pool.slab_len(),
                "buffer fits in the slab"
            );
        }
        assert_eq!(pool.capacity(), 5);
    }

    #[test]
    fn can_rearm_tracks_free_availability() {
        let mut pool = BufferPool::new(1, 128);
        assert!(pool.can_rearm());
        let id = pool.acquire().unwrap();
        assert!(!pool.can_rearm(), "drained -> back-pressure");
        assert!(pool.release(id));
        assert!(pool.can_rearm(), "a release lifts back-pressure");
    }

    #[test]
    fn matches_a_hashset_oracle_over_a_deterministic_op_stream() {
        // A deterministic LCG drives an acquire/release stream; the pool's outstanding set must match
        // a HashSet oracle at every step, and every live id must be distinct + in range (no aliasing).
        const COUNT: u16 = 16;
        let mut pool = BufferPool::new(COUNT, 1024);
        let mut outstanding: HashSet<BufId> = HashSet::new();
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state
        };
        for _ in 0..50_000 {
            // Bias slightly toward acquire when there is room, else release, so both paths run.
            let want_acquire = next() % 2 == 0;
            if want_acquire {
                match pool.acquire() {
                    Some(id) => {
                        assert!(id.0 < COUNT);
                        assert!(
                            outstanding.insert(id),
                            "acquired an already-outstanding id (aliasing!)"
                        );
                    }
                    None => assert_eq!(outstanding.len(), COUNT as usize, "None only when full"),
                }
            } else if let Some(&id) = outstanding.iter().next() {
                assert!(pool.release(id), "releasing an outstanding id must succeed");
                outstanding.remove(&id);
            }
            // The pool's free count is always the complement of the oracle's outstanding set.
            assert_eq!(pool.free_count(), COUNT as usize - outstanding.len());
            assert_eq!(pool.can_rearm(), outstanding.len() < COUNT as usize);
        }
    }
}
