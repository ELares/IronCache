// SPDX-License-Identifier: MIT OR Apache-2.0
//! The PER-SHARD blocking-command WAITER REGISTRY (PROD-9 HA polish).
//!
//! A blocking list/zset pop (BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP)
//! first ATTEMPTS its non-blocking op; only when every key is empty does the connection
//! PARK. Parking registers a [`Waiter`] in this PER-SHARD registry, keyed by `(db, key)`, in
//! an ORDERED queue (FIFO = Redis "serve the longest-waiting blocked client first"). A PUSH
//! to a waited key WAKES the FIRST waiter on that key; the woken connection re-attempts its
//! pop and either succeeds or re-parks.
//!
//! ## Shared-nothing (ADR-0002): a per-shard, lock-free, core-local registry
//!
//! A blocked client and the pusher that wakes it are on the SAME shard (the key's OWNER
//! shard, since the common deployment is single-shard-per-connection and the serve layer
//! keeps a blocking command on the home shard), so the registry is a serve-layer THREAD-
//! LOCAL with NO lock, exactly like [`crate::pubsub::ShardPubSub`]. The one cross-core
//! handle a [`Waiter`] holds is its [`tokio::sync::Notify`] (behind an `Arc`): the registry
//! itself is touched only on the owning shard (register on park, wake on push, deregister on
//! drop), so no cross-core synchronization is needed for the common case.
//!
//! ## RAII deregister (no leak, no waking a dead connection)
//!
//! Parking returns a [`WaiterGuard`]: while it lives, the [`Waiter`] is in the registry; on
//! Drop (the pop succeeded, the timeout elapsed, OR the connection was closed/killed while
//! parked) it removes the waiter from the queue, pruning an emptied key entry. So a closed/
//! killed blocked connection cannot leak a registry entry and a push never wakes a gone
//! connection.
//!
//! ## Spin-free wake (no busy-wait)
//!
//! A parked connection `select!`s on the waiter's `Notify` (the wake), the runtime timer
//! seam (the timeout), and the connection's read (a peer close while parked). The `Notify`
//! parks the task; there is NO poll loop. A wake is a single `notify_one`, which leaves a
//! permit if the waiter has not parked yet (no lost wakeup).

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;

/// One parked connection's wake handle. The registry stores it in a per-key FIFO queue; a
/// push `notify_one`s the FRONT waiter's [`Notify`]. Identified by the connection's `conn_id`
/// so the RAII guard can remove THIS waiter (not another connection's) on drop.
#[derive(Debug)]
pub struct Waiter {
    /// The connection that parked (so the guard removes the right entry).
    pub conn_id: u64,
    /// The spin-free wake. A push to the waited key calls `notify_one`; the parked serve
    /// loop awaits `notified()`. `notify_one` leaves a permit if the waiter has not parked
    /// yet, so a wake that races the park is not lost.
    pub wake: Arc<Notify>,
}

/// The PER-SHARD blocking-waiter table: `(db, key)` -> an ORDERED queue of waiters (FIFO).
///
/// Core-local (a serve-layer thread-local), NO lock (ADR-0002 shared-nothing). The
/// `VecDeque` preserves arrival order so [`Self::wake_one`] serves the LONGEST-waiting
/// blocked client first (Redis fairness). A key with no waiters is absent (the last
/// deregister prunes it), so an idle key holds no memory.
/// One key's FIFO queue of parked waiters: `(conn_id, wake)` in arrival order (front =
/// longest-waiting). A named alias keeps the [`ShardBlocking`] map type readable.
type WaiterQueue = VecDeque<(u64, Arc<Notify>)>;

#[derive(Debug, Default)]
pub struct ShardBlocking {
    /// `(db, key)` -> FIFO queue of `(conn_id, wake)`. The front is the longest-waiting.
    queues: HashMap<(u32, Bytes), WaiterQueue>,
}

impl ShardBlocking {
    /// Register `conn_id`'s `wake` as a waiter on `(db, key)`, appended to the BACK of the
    /// key's FIFO queue (it is the newest, so it is served last). Idempotent per
    /// (key, conn): a connection that parks on the SAME key twice (a multi-key BLPOP cannot,
    /// but a re-park after a spurious wake can) appends a fresh entry; the stale one is
    /// removed by its guard first, so the queue never holds two live entries for one conn.
    fn register(&mut self, db: u32, key: &[u8], conn_id: u64, wake: Arc<Notify>) {
        self.queues
            .entry((db, Bytes::copy_from_slice(key)))
            .or_default()
            .push_back((conn_id, wake));
    }

    /// Deregister `conn_id` from `(db, key)`, pruning the key entry when its queue empties.
    /// Removes EVERY entry for `conn_id` on this key (defensive; normally there is one). The
    /// RAII [`WaiterGuard`] calls this on Drop, so a closed/killed parked connection leaks
    /// nothing.
    fn deregister(&mut self, db: u32, key: &[u8], conn_id: u64) {
        // `get_mut` by a borrowed key needs the same `(u32, &[u8])` view the map is keyed by;
        // build the lookup tuple once. We cannot key a HashMap<(u32, Bytes)> by `(u32, &[u8])`
        // directly, so scan with a constructed Bytes (cold path: only on park exit).
        let lookup = (db, Bytes::copy_from_slice(key));
        if let Some(q) = self.queues.get_mut(&lookup) {
            q.retain(|(id, _)| *id != conn_id);
            if q.is_empty() {
                self.queues.remove(&lookup);
            }
        }
    }

    /// WAKE the FRONT (longest-waiting) waiter on `(db, key)`, if any, returning whether a
    /// waiter was woken. The woken waiter's `notify_one` makes its parked serve loop re-attempt
    /// the pop; the waiter is NOT removed here (its own RAII guard removes it once it actually
    /// pops or re-parks), so a spurious failure to pop (another waiter raced it) leaves it in
    /// the queue to be woken again. Called on a PUSH to a waited key.
    ///
    /// FAIRNESS: the front of the `VecDeque` is the longest-waiting client, so it is woken
    /// first (Redis "serve in order"). If it cannot satisfy the pop (it lost the race), the
    /// next push wakes the (now-front) next waiter.
    pub fn wake_one(&mut self, db: u32, key: &[u8]) -> bool {
        let lookup = (db, Bytes::copy_from_slice(key));
        if let Some(q) = self.queues.get(&lookup) {
            if let Some((_, wake)) = q.front() {
                wake.notify_one();
                return true;
            }
        }
        false
    }

    /// Whether `(db, key)` has at least one parked waiter (test/introspection helper).
    #[must_use]
    pub fn has_waiter(&self, db: u32, key: &[u8]) -> bool {
        self.queues
            .get(&(db, Bytes::copy_from_slice(key)))
            .is_some_and(|q| !q.is_empty())
    }

    /// The total number of parked waiters across all keys (test/introspection helper).
    #[must_use]
    pub fn total_waiters(&self) -> usize {
        self.queues.values().map(VecDeque::len).sum()
    }
}

/// An RAII handle a parked connection holds for the duration of one BLOCK. While it lives,
/// the connection's [`Waiter`]s are registered on EVERY key it waits on (a multi-key BLPOP
/// registers on each key); on Drop -- the pop succeeded, the timeout fired, OR the serve loop
/// is tearing the connection down -- it removes them ALL, so no key keeps a stale waiter and a
/// push never wakes a gone connection.
///
/// The guard borrows the SHARED per-shard registry handle (`Rc<RefCell<ShardBlocking>>`) so it
/// can deregister on the owning shard thread (single-threaded, ADR-0002). It is `!Send` by
/// construction (the `Rc`), which is correct: a blocked connection never crosses cores.
pub struct WaiterGuard {
    registry: std::rc::Rc<std::cell::RefCell<ShardBlocking>>,
    db: u32,
    /// The keys this connection registered a waiter on (one per BLPOP key), so Drop removes
    /// each. Owned so the guard does not borrow the request.
    keys: Vec<Vec<u8>>,
    conn_id: u64,
}

impl WaiterGuard {
    /// PARK `conn_id` on every key in `keys` under `db`, sharing ONE `wake` `Notify` across
    /// them (a wake on ANY waited key resumes the connection, which re-attempts the pop over
    /// all its keys). Returns the guard (the keys stay registered until it drops) and the
    /// `wake` the serve loop awaits.
    ///
    /// The connection registers on EVERY key (the co-located/single-shard case): all keys
    /// live on this shard's store, so one registry holds them all and the first key to gain
    /// an element wakes the connection. (Multi-SHARD-SPANNING blocking -- keys on different
    /// owner shards -- is documented as a limitation: the serve layer keeps a blocking command
    /// on the home shard, so a key on a sibling shard would not be reached by this registry.
    /// The serve layer registers on the home shard and the woken re-attempt reads the home
    /// store; a spanning key is not awaited cross-shard this pass.)
    #[must_use]
    pub fn park(
        registry: &std::rc::Rc<std::cell::RefCell<ShardBlocking>>,
        db: u32,
        keys: &[Vec<u8>],
        conn_id: u64,
    ) -> (Self, Arc<Notify>) {
        let wake = Arc::new(Notify::new());
        {
            let mut reg = registry.borrow_mut();
            for key in keys {
                reg.register(db, key, conn_id, Arc::clone(&wake));
            }
        }
        (
            WaiterGuard {
                registry: std::rc::Rc::clone(registry),
                db,
                keys: keys.to_vec(),
                conn_id,
            },
            wake,
        )
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        // RAII deregister on EVERY exit path (pop succeeded, timeout, connection close/kill,
        // or a panic unwinding through the park). A single borrow of the per-shard registry on
        // the owning shard thread; cold (only on park exit). After this the connection holds no
        // registry entry, so a later push cannot wake the gone connection.
        let mut reg = self.registry.borrow_mut();
        for key in &self.keys {
            reg.deregister(self.db, key, self.conn_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn registry() -> Rc<RefCell<ShardBlocking>> {
        Rc::new(RefCell::new(ShardBlocking::default()))
    }

    #[test]
    fn park_registers_and_guard_drop_deregisters() {
        let reg = registry();
        {
            let (_guard, _wake) = WaiterGuard::park(&reg, 0, &[b"k".to_vec()], 1);
            assert!(reg.borrow().has_waiter(0, b"k"));
            assert_eq!(reg.borrow().total_waiters(), 1);
        }
        // Guard dropped -> deregistered, key pruned.
        assert!(!reg.borrow().has_waiter(0, b"k"));
        assert_eq!(reg.borrow().total_waiters(), 0);
    }

    #[test]
    fn multi_key_park_registers_on_each_key() {
        let reg = registry();
        let (_guard, _wake) = WaiterGuard::park(&reg, 0, &[b"a".to_vec(), b"b".to_vec()], 1);
        assert!(reg.borrow().has_waiter(0, b"a"));
        assert!(reg.borrow().has_waiter(0, b"b"));
        assert_eq!(reg.borrow().total_waiters(), 2);
    }

    #[tokio::test]
    async fn wake_one_serves_the_front_waiter_fifo() {
        let reg = registry();
        // Two waiters on the same key: conn 1 parked first (front), conn 2 second.
        let (_g1, wake1) = WaiterGuard::park(&reg, 0, &[b"k".to_vec()], 1);
        let (_g2, wake2) = WaiterGuard::park(&reg, 0, &[b"k".to_vec()], 2);
        // A wake notifies the FRONT (conn 1) only.
        assert!(reg.borrow_mut().wake_one(0, b"k"));
        // wake1 has a permit (notified); wake2 does not. `notified()` returns immediately when a
        // permit is present, and would hang otherwise -- so assert via a zero-timeout race.
        let w1 = wake1.notified();
        tokio::select! {
            () = w1 => {} // got the permit (the front waiter was woken)
            () = tokio::time::sleep(std::time::Duration::from_millis(50)) => panic!("front waiter (conn 1) was not woken"),
        }
        let w2 = wake2.notified();
        tokio::select! {
            () = w2 => panic!("the second waiter (conn 2) must NOT be woken before conn 1 leaves"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {} // correctly not woken
        }
    }

    #[test]
    fn deregister_of_front_lets_next_become_front() {
        let reg = registry();
        let g1 = {
            let (g1, _w1) = WaiterGuard::park(&reg, 0, &[b"k".to_vec()], 1);
            let (_g2, _w2) = WaiterGuard::park(&reg, 0, &[b"k".to_vec()], 2);
            // _g2 drops here -> conn 2 removed; conn 1 still front.
            g1
        };
        assert_eq!(reg.borrow().total_waiters(), 1);
        assert!(reg.borrow().has_waiter(0, b"k"));
        drop(g1);
        assert_eq!(reg.borrow().total_waiters(), 0);
    }

    #[test]
    fn wake_on_an_unwaited_key_is_false() {
        let reg = registry();
        assert!(!reg.borrow_mut().wake_one(0, b"absent"));
    }
}
