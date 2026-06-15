// SPDX-License-Identifier: MIT OR Apache-2.0
//! The cross-shard coordinator substrate (COORDINATOR.md #107, PASS 1).
//!
//! The server is shared-nothing thread-per-core (ADR-0002): each shard owns a
//! PARTITION of the keyspace (by [`ironcache_server::owner_shard`]) and per-shard state
//! (STORE/WHEEL/ENV/ShardState) lives in thread-local `Rc<RefCell<..>>` on that shard's
//! single thread. A connection is pinned for life to the random "home" shard the kernel
//! SO_REUSEPORT-routed it to. So a single-key command whose key is NOT home-owned must
//! HOP to the owning shard, run there against that shard's partition, and return its
//! reply for the home connection to encode.
//!
//! This module is that hop's substrate:
//! - [`ShardWork`] / [`ShardReply`]: the request-in / reply-out envelope (all `Send`:
//!   [`Request`] is `Vec<Bytes>`, [`Value`]/[`CounterDeltas`] are `Send`).
//! - [`Inbox`] + [`build_inboxes`]: one bounded MPSC queue PER shard (the cross-thread
//!   channel; back-pressure is await-on-full).
//! - [`run_drain_loop`]: the per-shard consumer the bootstrap spawns on each shard's
//!   LocalSet; it runs each unit of remote work against THIS shard's thread-locals.
//! - [`dispatch_via`]: the home-core side that enqueues work to the owning shard and
//!   awaits the oneshot reply, then encodes on the home core with the home proto.
//!
//! ## Borrow discipline (critical, ADR-0002/0005)
//!
//! The drain loop runs on the SAME single-threaded LocalSet as the shard's connection
//! tasks and its expiry timer. A `RefCell` borrow of any per-shard cell held ACROSS an
//! `.await` would double-borrow-panic when an interleaved connection task on the same
//! thread borrows the same cell. So [`run_remote`] takes and releases every borrow
//! INSIDE one synchronous call and holds NOTHING across the `rx.recv().await` in the
//! drain loop, exactly the contract the expiry timer task already follows.

use crate::serve::{ShardState, ShardStoreImpl, shard_env, shard_state, shard_store, shard_wheel};
use ironcache_env::Clock;
use ironcache_server::dispatch::ServerContext;
use ironcache_server::{
    CommandClass, CounterDeltas, ProtoVersion, Request, UnixMillis, Value, classify,
    dispatch_remote_keyed, dispatch_remote_whole_keyspace,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// The bounded depth of each shard's cross-shard inbound queue (COORDINATOR.md #107).
///
/// A bounded channel gives back-pressure for free: when a shard's queue is full, the
/// enqueuing home core AWAITS in [`dispatch_via`] until the owning shard drains one,
/// rather than growing an unbounded backlog under a cross-shard hot-key flood. 1024 is a
/// deliberate first cut: deep enough that a momentary burst does not serialize home
/// cores, shallow enough to bound memory. A fast `-BUSY`-style rejection threshold (fail
/// rather than await past a high-water mark) is a deferred knob (the open `-BUSY` knob,
/// COORDINATOR.md); PASS 1 uses pure await-on-full.
pub const INBOX_DEPTH: usize = 1024;

/// One unit of cross-shard work: a single-key command to run on the shard that OWNS its
/// key, plus the oneshot the owning shard sends the reply back on.
///
/// All fields are `Send` so the envelope crosses the thread boundary: [`Request`] is
/// `Vec<Bytes>` (refcounted byte buffers), `db` is a `u32`, and the oneshot sender is
/// `Send`. The reply travels back as a [`ShardReply`].
#[derive(Debug)]
pub struct ShardWork {
    /// The decoded request to run on the owning shard (cloned/moved from the home core;
    /// the clone is cheap, `Bytes` are refcounted).
    pub request: Request,
    /// The logical database the issuing connection had selected (`SELECT`), so the
    /// remote command runs against the right DB on the owning shard.
    pub db: u32,
    /// The channel the owning shard sends the reply back on (consumed once).
    pub reply: oneshot::Sender<ShardReply>,
}

/// The reply for one [`ShardWork`]: the command's [`Value`] plus the counter deltas it
/// produced on the owning shard.
///
/// The `deltas` are carried back ONLY so the home core does not DOUBLE-COUNT the data
/// deltas: the owning shard has ALREADY folded them into its own counters (where the
/// data lives), so the home core ignores `deltas` for the data figures and only
/// attributes the connection-level `commands_processed`. They are returned (not dropped
/// remotely) so a future observability pass can attribute cross-shard work if desired.
#[derive(Debug)]
pub struct ShardReply {
    /// The reply value to encode on the home core with the home connection's proto.
    pub value: Value,
    /// The counter deltas the command produced on the owning shard (already folded
    /// there; see the struct docs for why they ride back).
    pub deltas: CounterDeltas,
}

/// The set of per-shard inbound queues, indexed by shard. Shared (cloned) into every
/// shard's serve closure so any home core can enqueue to any owning shard.
///
/// `Arc<[Sender]>` (a shared SLICE, not a `Vec`) is the right shape: it is built once at
/// boot, never resized, and cloned cheaply per connection; the senders are `Send + Sync`
/// (tokio MPSC). This is NOT a `std::sync` lock (the invariant the hot-path lint guards):
/// it is an `Arc` over lock-free channel senders.
pub type Inbox = Arc<[mpsc::Sender<ShardWork>]>;

/// Build `n` bounded per-shard inbound queues, returning the shared [`Inbox`] of senders
/// (one per shard, captured into the serve closure) and the matching receivers (one per
/// shard, handed to that shard's [`run_drain_loop`] by the bootstrap).
///
/// Each channel is bounded to [`INBOX_DEPTH`] for await-on-full back-pressure. The
/// returned `Vec<Receiver>` is in shard-index order, so `receivers[i]` belongs to shard
/// `i`; the bootstrap moves each out by index.
///
/// # Panics
///
/// Panics if `n == 0` (a running server has at least one shard; the caller passes
/// `config.shards.max(1)`).
#[must_use]
pub fn build_inboxes(n: usize) -> (Inbox, Vec<mpsc::Receiver<ShardWork>>) {
    assert!(n >= 1, "build_inboxes requires at least one shard");
    let mut senders = Vec::with_capacity(n);
    let mut receivers = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = mpsc::channel::<ShardWork>(INBOX_DEPTH);
        senders.push(tx);
        receivers.push(rx);
    }
    (Inbox::from(senders), receivers)
}

/// The per-shard DRAIN LOOP (COORDINATOR.md #107): consume cross-shard work for the keys
/// THIS shard owns, run each unit against this shard's thread-locals, and reply.
///
/// Spawned once per shard on the shard's LocalSet by the bootstrap (alongside the accept
/// loop), parameterized by `ctx` (the shard's [`ServerContext`], for the admission budget
/// / policy generation / databases / boot policy name). It loops until every [`Inbox`]
/// sender is dropped (server shutdown), running [`run_remote`] per unit and sending the
/// reply on the unit's oneshot (a dropped receiver -- the home connection went away -- is
/// ignored).
///
/// ## Borrow discipline
///
/// NO `RefCell` borrow is held across the `rx.recv().await`: [`run_remote`] is a
/// synchronous call that acquires + releases every per-shard borrow before returning, so
/// when the loop suspends on `recv()` nothing of this shard's state is borrowed and an
/// interleaved connection task can borrow freely (the same contract the expiry timer
/// follows). See the module docs.
pub async fn run_drain_loop(mut rx: mpsc::Receiver<ShardWork>, ctx: ServerContext) {
    // Bring up THIS shard's background tasks AT SHARD BOOT (COORDINATOR.md #107): lazily
    // init the per-shard handles + spawn the active-expiry timer ONCE. The drain loop is
    // spawned on the shard's LocalSet, so this is the shard-boot hook a connectionless
    // (but key-owning) shard needs -- a shard can now own keys without ever accepting a
    // connection, so its expiry timer must start here, not on first connection. Idempotent
    // (guarded), so the serve loop calling it again per connection is harmless.
    crate::serve::ensure_shard_started(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    while let Some(work) = rx.recv().await {
        // run_remote borrows + releases the shard thread-locals ENTIRELY within this
        // synchronous call; nothing is borrowed when we loop back to `recv().await`.
        let reply = run_remote(&ctx, &work.request, work.db);
        // The home connection may have closed (oneshot receiver dropped); that is fine,
        // the reply is simply discarded.
        let _ = work.reply.send(reply);
    }
}

/// Run ONE unit of remote keyed work against THIS shard's thread-local state, returning
/// the reply + the deltas it produced (already folded into this shard's counters).
///
/// This is the synchronous heart of the drain loop: it lazily inits + BRIEFLY borrows
/// this shard's thread-local ENV / STORE / WHEEL / ShardState (the SAME accessors
/// `handle_request` uses, so the per-shard lazy-init is shared), reads `now` from THIS
/// shard's Env clock (the determinism seam, ADR-0003 -- NOT a home-supplied now), runs
/// [`dispatch_remote_keyed`], folds the resulting [`CounterDeltas`] into THIS shard's
/// counters (the data lives here, so the data counters live here too), and returns the
/// reply + a COPY of the deltas (so the home core can avoid double-counting).
///
/// Every borrow is taken and dropped inside this function: nothing escapes to be held
/// across the caller's `.await` (the no-borrow-across-await contract).
fn run_remote(ctx: &ServerContext, request: &Request, db: u32) -> ShardReply {
    // Lazily init + clone the per-shard handles (Rc clones, cheap), exactly as
    // serve_connection / handle_request do. These accessors are the shared per-shard
    // lazy-init, so the drain loop and the connection tasks see the SAME store/wheel/env.
    let env = shard_env();
    let store_rc = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    let wheel_rc = shard_wheel();
    let state_rc = shard_state();

    // Read `now` once from THIS shard's wall clock (ADR-0003: the determinism seam is the
    // owning shard's Env, not a home-supplied timestamp), via a SHORT shared borrow that
    // drops before the mutable store/wheel borrows below (distinct RefCells, no alias).
    let now = UnixMillis(env.borrow().now_unix_millis());

    // Copy the shard's last-seen policy generation OUT into a local so dispatch can take
    // `&mut` it without holding a state_rc borrow across the store/wheel borrows (mirrors
    // handle_request's discipline; the rollup closure does not exist here, but the
    // separate-borrow discipline is identical).
    let mut shard_generation = state_rc.borrow().last_policy_generation;

    // Pick the per-shard dispatcher by command class. KEYED commands (single/multi) run
    // the full keyed path (policy hot-swap + active drain + admission gate); WHOLE-KEYSPACE
    // partials (the scatter-gather fan-out, COORDINATOR.md #107) run the lean keyspace path
    // (no admission/expiry: a count/iterate/flush/random is not a denyoom write). Anything
    // else never reaches the drain loop (the serve loop only enqueues those two classes);
    // dispatch_remote_* refuses a mis-routed command defensively.
    let is_whole_keyspace = matches!(
        classify(&crate::serve::ascii_upper(request.command())),
        CommandClass::WholeKeyspace
    );

    let mut deltas = CounterDeltas::default();
    let lazy_expired;
    let value = {
        let mut store = store_rc.borrow_mut();
        let mut wheel = wheel_rc.borrow_mut();
        // The Env is a SEPARATE RefCell from store/wheel; the mutable borrow here (for the
        // RNG-drawing members + the policy hot-swap seed) does not alias the held
        // store/wheel borrows. `now` was read above from a distinct, now-dropped borrow.
        let mut env_ref = env.borrow_mut();
        let v = if is_whole_keyspace {
            // The whole-keyspace partial reads no wheel / generation; it runs the SAME
            // cmd_keyspace::* handlers against THIS shard's partition.
            dispatch_remote_whole_keyspace(&mut *env_ref, &mut *store, db, now, request)
        } else {
            dispatch_remote_keyed(
                ctx,
                &mut *env_ref,
                &mut *store,
                &mut wheel,
                db,
                now,
                &mut shard_generation,
                &mut deltas,
                request,
            )
        };
        drop(env_ref);
        // Drain the lazy-backstop expiry the command produced (the store accumulates it
        // inside the primitives), folding it into expired_keys alongside the active drain,
        // exactly like handle_request.
        lazy_expired = store.take_lazy_expired();
        v
        // store + wheel borrows DROP here, before the state borrow below.
    };

    // Fold this command's deltas into THIS shard's counters (the data lives here) and
    // write back the possibly-advanced policy generation. The home core will NOT re-apply
    // these data deltas (it only attributes commands_processed for the issuing conn).
    {
        deltas.expired += lazy_expired;
        let mut st = state_rc.borrow_mut();
        if deltas != CounterDeltas::default() {
            st.counters.apply(deltas);
        }
        st.last_policy_generation = shard_generation;
    }

    ShardReply { value, deltas }
}

/// Run a [`CommandClass::WholeKeyspace`](ironcache_server::CommandClass) command's PARTIAL
/// against THIS (home) shard's thread-local state, SYNCHRONOUSLY, returning the home
/// shard's [`ShardReply`] (COORDINATOR.md #107, the whole-keyspace fan-out). This is the
/// `local` closure [`fan_out_all`] runs for the home shard -- the home core does NOT
/// round-trip its OWN partial through its channel; it runs it inline, exactly like the
/// single-key local fast path.
///
/// It reads `now` from THIS shard's Env clock (the determinism seam, ADR-0003) and runs
/// the SAME [`dispatch_remote_whole_keyspace`] the remote shards run, so the home shard's
/// partial is byte-identical to every other shard's. Whole-keyspace partials produce no
/// counter deltas to fold (a count/iterate/flush/random is not counted), so the returned
/// [`ShardReply`] carries default deltas. Every per-shard borrow is taken + released inside
/// this synchronous call (the no-borrow-across-await contract; the caller awaits remote
/// replies AFTER this returns).
pub fn run_local_whole_keyspace(ctx: &ServerContext, request: &Request, db: u32) -> ShardReply {
    let env = shard_env();
    let store_rc = shard_store(
        ctx.databases,
        ctx.info.maxmemory_policy,
        crate::serve::scan_reserved_bits(ctx.shards),
    );
    let now = UnixMillis(env.borrow().now_unix_millis());
    let lazy_expired;
    let value = {
        let mut store = store_rc.borrow_mut();
        let mut env_ref = env.borrow_mut();
        let v = dispatch_remote_whole_keyspace(&mut *env_ref, &mut *store, db, now, request);
        drop(env_ref);
        // A whole-keyspace read may lazily expire keys it skips; drain + fold the backstop
        // count into THIS shard's expired_keys, exactly as run_remote / handle_request do.
        lazy_expired = store.take_lazy_expired();
        v
    };
    if lazy_expired > 0 {
        let state_rc = shard_state();
        state_rc.borrow_mut().counters.apply(CounterDeltas {
            expired: lazy_expired,
            ..CounterDeltas::default()
        });
    }
    ShardReply {
        value,
        deltas: CounterDeltas::default(),
    }
}

/// The HOME-CORE side of a cross-shard hop (COORDINATOR.md #107): enqueue `request` to
/// the shard that owns its key, await the reply, and encode it on the home core with the
/// home connection's `proto`.
///
/// Build a oneshot, send the [`ShardWork`] to `inbox[target]` (AWAITS if that shard's
/// queue is full -- the back-pressure), then await the reply. If the send fails or the
/// oneshot errs (the owning shard's drain loop is gone, e.g. during shutdown), encode a
/// proto-shaped error so the connection gets a well-formed reply rather than a hang.
///
/// The home core does NOT re-apply `reply.deltas` (the owning shard already folded the
/// data deltas into its own counters); attributing the issuing connection's
/// `commands_processed` is the serve loop's job (it does so the same way for the local
/// fast path), so this function only produces the encoded reply bytes.
pub async fn dispatch_via(
    inbox: &Inbox,
    target: usize,
    request: &Request,
    db: u32,
    out: &mut Vec<u8>,
    proto: ProtoVersion,
) {
    let (tx, rx) = oneshot::channel::<ShardReply>();
    let work = ShardWork {
        // Clone is cheap: Request is Vec<Bytes> (refcounted buffers).
        request: request.clone(),
        db,
        reply: tx,
    };
    // Await-on-full back-pressure. A send error means the owning shard's receiver is gone
    // (shutdown / shard died); reply with a proto-shaped error rather than hang.
    if inbox[target].send(work).await.is_err() {
        encode_into(out, &Value::error(shard_unavailable_error()), proto);
        return;
    }
    match rx.await {
        Ok(reply) => {
            // The home core deliberately does NOT re-apply `reply.deltas`: the OWNING
            // shard already folded those data counters into its own ShardState (the data
            // lives there), so applying them here too would double-count. They ride back
            // only so a future observability pass could attribute cross-shard work; PASS 1
            // discards them here. The issuing connection's commands_processed is bumped by
            // the serve loop (matching the local fast path), not from these deltas.
            let _ = &reply.deltas;
            encode_into(out, &reply.value, proto);
        }
        Err(_) => encode_into(out, &Value::error(shard_unavailable_error()), proto),
    }
}

/// A SINGLE-TARGET cross-shard hop that returns the owning shard's reply [`Value`] (NOT
/// encoded), so the home core can POST-PROCESS it before encoding -- used by the
/// cross-shard SCAN, which hops to ONE shard per call (the current composite-cursor shard
/// index) and must REWRITE the returned inner cursor into the composite wire cursor before
/// encoding. On a send/await failure (the owning shard's drain loop is gone) it returns
/// the shard-unavailable error [`Value`] so the caller still produces a well-formed reply.
///
/// Like [`dispatch_via`], the home core does NOT re-apply the reply's deltas (the owning
/// shard already folded them); the serve loop bumps the issuing connection's
/// `commands_processed` separately.
pub async fn dispatch_one_value(inbox: &Inbox, target: usize, request: &Request, db: u32) -> Value {
    let (tx, rx) = oneshot::channel::<ShardReply>();
    let work = ShardWork {
        request: request.clone(),
        db,
        reply: tx,
    };
    if inbox[target].send(work).await.is_err() {
        return Value::error(shard_unavailable_error());
    }
    match rx.await {
        Ok(reply) => reply.value,
        Err(_) => Value::error(shard_unavailable_error()),
    }
}

/// SCATTER-GATHER a [`CommandClass::WholeKeyspace`](ironcache_server::CommandClass)
/// command across ALL `n_shards` shards and gather the per-shard replies, paired by shard
/// index (COORDINATOR.md #107, the whole-keyspace fan-out). The home core MERGES the
/// returned partials per command (DBSIZE sums, KEYS concatenates, FLUSH all-OK, RANDOMKEY
/// picks one); SCAN uses the single-target [`dispatch_via`] instead (it hops to ONE shard
/// per call, so fan-out is overkill for it).
///
/// The HOME shard (`home`) runs LOCALLY and SYNCHRONOUSLY via the `local` closure (the
/// caller runs `dispatch_remote_whole_keyspace` against the home thread-locals, like the
/// existing local fast path) -- it does NOT round-trip through the home shard's own
/// channel. Every OTHER shard gets a [`ShardWork`] (the same `request` + `db` + a oneshot)
/// and the home core awaits its reply with the usual await-on-full back-pressure. A shard
/// whose drain loop is gone (send error / oneshot cancelled, e.g. during shutdown) yields
/// a SHARD-UNAVAILABLE error reply for that shard rather than hanging or panicking; the
/// caller's merge surfaces it (FLUSH turns any error into a surfaced error; DBSIZE/KEYS
/// treat it as that shard contributing nothing -- documented at each merge site).
///
/// The returned vector is sorted by shard index `0..n_shards` (ordering is irrelevant for
/// DBSIZE/KEYS/FLUSH/RANDOMKEY but the deterministic order keeps the merge reproducible).
/// The requests are dispatched concurrently (all oneshots are created and enqueued, then
/// awaited), so a slow shard does not serialize the others beyond the await-on-full bound.
pub async fn fan_out_all(
    inbox: &Inbox,
    request: &Request,
    db: u32,
    home: usize,
    local: impl FnOnce() -> ShardReply,
) -> Vec<(usize, ShardReply)> {
    let n_shards = inbox.len();
    let mut replies: Vec<(usize, ShardReply)> = Vec::with_capacity(n_shards);

    // Enqueue the work to every NON-home shard first (creating each oneshot), collecting
    // the receivers, so the shards process concurrently while the home core then runs its
    // OWN partial locally and finally gathers the remote replies in shard order.
    let mut pending: Vec<(usize, oneshot::Receiver<ShardReply>)> = Vec::with_capacity(n_shards);
    for target in 0..n_shards {
        if target == home {
            continue;
        }
        let (tx, rx) = oneshot::channel::<ShardReply>();
        let work = ShardWork {
            request: request.clone(),
            db,
            reply: tx,
        };
        // Await-on-full back-pressure. A send error means the owning shard's receiver is
        // gone (shutdown / shard died): record a shard-unavailable reply for it directly
        // (no receiver to await) rather than hang.
        if inbox[target].send(work).await.is_err() {
            replies.push((target, shard_unavailable_reply()));
        } else {
            pending.push((target, rx));
        }
    }

    // The HOME shard's partial: run LOCALLY + SYNCHRONOUSLY on the home thread-locals (the
    // `local` closure), exactly like the single-key local fast path -- no self-channel hop.
    replies.push((home, local()));

    // Gather the remote replies. A cancelled oneshot (the owning shard's drain loop went
    // away after we enqueued) maps to a shard-unavailable reply, never a hang/panic.
    for (target, rx) in pending {
        match rx.await {
            Ok(reply) => replies.push((target, reply)),
            Err(_) => replies.push((target, shard_unavailable_reply())),
        }
    }

    // Sort by shard index so the merge is deterministic (irrelevant for DBSIZE/KEYS/FLUSH/
    // RANDOMKEY, but reproducible). n_shards is small (one per core), so this is cheap.
    replies.sort_by_key(|&(shard, _)| shard);
    replies
}

/// A [`ShardReply`] carrying the cross-shard unavailable error (the owning shard's drain
/// loop / receiver is gone, only during shutdown or a shard panic). Used by
/// [`fan_out_all`] so a dead shard contributes a well-formed error rather than a hang;
/// no counter deltas are attributed (the command never ran on that shard).
fn shard_unavailable_reply() -> ShardReply {
    ShardReply {
        value: Value::error(shard_unavailable_error()),
        deltas: CounterDeltas::default(),
    }
}

/// The SINGLE canonical message text for the shard-unavailable degradation (the owning
/// shard's drain loop / receiver is gone, only during shutdown or a shard panic). The
/// PRODUCER ([`shard_unavailable_error`]) and every CONSUMER (the whole-keyspace merge
/// classifiers that must tell a genuine command Error apart from this degradation) both
/// reference this one item via [`is_shard_unavailable`], so the wording lives in ONE
/// place and a hand-copied literal can never drift out of sync (FIX 6). This is the
/// `ErrorReply` MESSAGE (the text after `-ERR `), not the full wire line.
pub const SHARD_UNAVAILABLE_MSG: &str = "cross-shard target unavailable";

/// Whether `e` is the shard-unavailable degradation (vs a genuine command Error such as
/// a wrong-arity reply, which is identical on every shard and must be SURFACED). The
/// single classifier the producer and all three whole-keyspace merges share, comparing
/// the `ErrorReply` MESSAGE against [`SHARD_UNAVAILABLE_MSG`] (no `line()` String
/// allocation). FIX 6: replaces the hand-copied `"-ERR cross-shard target unavailable"`
/// literals that were coupled by convention only.
#[must_use]
pub fn is_shard_unavailable(e: &ironcache_protocol::ErrorReply) -> bool {
    e.message() == SHARD_UNAVAILABLE_MSG
}

/// The error a home core encodes when the owning shard is unreachable (its drain loop /
/// receiver is gone, only during shutdown or a shard panic). A generic `-ERR` so the
/// client gets a well-formed RESP reply instead of a stalled connection. Built from the
/// shared [`SHARD_UNAVAILABLE_MSG`] so the wording matches [`is_shard_unavailable`].
fn shard_unavailable_error() -> ironcache_protocol::ErrorReply {
    ironcache_protocol::ErrorReply::err(SHARD_UNAVAILABLE_MSG)
}

/// Encode `value` for `proto` and append to `out` (the home-core encode, mirroring the
/// serve loop's `encode_into`). Encoding stays on the home core and uses the home
/// connection's negotiated proto, never the owning shard's.
fn encode_into(out: &mut Vec<u8>, value: &Value, proto: ProtoVersion) {
    let mut bm = bytes::BytesMut::with_capacity(64);
    ironcache_protocol::encode(&mut bm, value, proto);
    out.extend_from_slice(&bm);
}

// A tiny compile-time anchor that the per-shard handle types stay reachable from this
// module (the coordinator owns the concrete ShardStoreImpl + ShardState references via
// the serve accessors). Kept as a type alias use so a future refactor that moves the
// thread-locals breaks here loudly rather than silently.
#[allow(dead_code)]
type _ShardHandles = (Rc<RefCell<ShardStoreImpl>>, Rc<RefCell<ShardState>>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_inboxes_makes_one_queue_per_shard() {
        let (inbox, rxs) = build_inboxes(4);
        assert_eq!(inbox.len(), 4);
        assert_eq!(rxs.len(), 4);
    }

    #[test]
    #[should_panic(expected = "at least one shard")]
    fn build_inboxes_rejects_zero() {
        let _ = build_inboxes(0);
    }
}
