// SPDX-License-Identifier: MIT OR Apache-2.0
//! HA-7a loopback acceptance test: a primary and a replica on real loopback TCP.
//!
//! Mirrors the Raft loopback proof shape ([`ironcache_raft_net`]'s loopback test):
//! both ends run on a single current-thread tokio runtime + `LocalSet` (the
//! shared-nothing, `!Send`, thread-per-core shape, ADR-0002), each driven ENTIRELY
//! through the [`ironcache_runtime::Runtime`] seam. The primary binds its dedicated
//! replication listener and emits `REPLPING` heartbeats with a per-tick-advancing
//! offset; the replica dials, exchanges `REPLCONF`, and observes the offset advance.
//!
//! It proves the two HA-7a transport properties over a real socket:
//!   1. ATTACH + STREAM: the replica connects, REPLCONF/REPLPING flow, and the
//!      replica's observed/acked offset ADVANCES.
//!   2. RECONNECT-RESUME: tearing the replica link down and bringing a NEW one up
//!      (same node id, resuming from the last-acked offset) re-attaches and the
//!      offset advances further (never backwards).
//!
//! Not a deterministic test (that is the DST suite); it runs on the real clock and
//! polls with generous deadlines read through the env seam.

use core::cell::Cell;
use core::time::Duration;
use std::net::SocketAddr;
use std::rc::Rc;

use ironcache_env::{Clock, SystemEnv};
use ironcache_repl::{
    ReplId, ReplOffset, ReplState, ReplicaObserver, run_primary_repl_listener, run_replica_link,
};
use ironcache_runtime::Runtime;
use ironcache_runtime::TokioRuntime;
use ironcache_runtime::tokio_rt::bind_reuseport;

const REPLICA_NODE: u64 = 2;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Poll `f` until it returns `true`, up to `timeout`, yielding to the local runtime
/// between tries (the test runs inside the LocalSet). The deadline is read through
/// the env seam, never `std::time` (ADR-0003 / invariant 2: even tests read real
/// time only through Env).
async fn poll_until(rt: &TokioRuntime, timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let env = SystemEnv::new();
    let start = env.now();
    loop {
        if f() {
            return true;
        }
        if env.now().saturating_duration_since(start) >= timeout {
            return false;
        }
        rt.timer(Duration::from_millis(10)).await;
    }
}

/// Spawn the primary: a background ticker advancing the live offset (modeling the 7c
/// per-write advance) plus the replication listener advertising it on each heartbeat.
fn spawn_primary(addr: SocketAddr) -> Rc<Cell<u64>> {
    let listener = bind_reuseport(addr).unwrap();
    let replid = ReplId::from_bytes([0xCD; 20]);
    let offset = Rc::new(Cell::new(0u64));

    let ticker_off = Rc::clone(&offset);
    tokio::task::spawn_local(async move {
        let rt = TokioRuntime::new();
        loop {
            rt.timer(Duration::from_millis(20)).await;
            ticker_off.set(ticker_off.get() + 1);
        }
    });

    let listen_off = Rc::clone(&offset);
    tokio::task::spawn_local(async move {
        run_primary_repl_listener::<TokioRuntime, _>(
            TokioRuntime::new(),
            listener,
            replid,
            move || ReplOffset(listen_off.get()),
        )
        .await;
    });
    offset
}

/// Spawn a replica link to `addr` resuming from `start_acked`, gated by `should_run`,
/// returning the observer to watch its progress.
fn spawn_replica(
    addr: SocketAddr,
    start_acked: ReplOffset,
    should_run: impl FnMut() -> bool + 'static,
) -> Rc<ReplicaObserver> {
    let observer = ReplicaObserver::new();
    let obs = Rc::clone(&observer);
    tokio::task::spawn_local(async move {
        run_replica_link::<TokioRuntime>(
            TokioRuntime::new(),
            addr,
            REPLICA_NODE,
            start_acked,
            obs,
            should_run,
        )
        .await;
    });
    observer
}

#[test]
fn primary_replica_attach_stream_and_reconnect_resume() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let timeout = Duration::from_secs(10);
        let runtime = TokioRuntime::new();
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();

        // The primary advancing its offset and serving the replication listener.
        let _offset = spawn_primary(addr);

        // ---- (1) The replica attaches and its observed offset advances. ----
        let run1 = Rc::new(Cell::new(true));
        let gate = Rc::clone(&run1);
        let observer = spawn_replica(addr, ReplOffset::ZERO, move || gate.get());

        let synced = poll_until(&runtime, timeout, || {
            matches!(observer.state(), Some(ReplState::Synced { .. }))
                && observer.acked() > ReplOffset::ZERO
        })
        .await;
        assert!(
            synced,
            "the replica must sync and observe an advancing offset"
        );

        // Let it run a bit, then capture the resume point.
        runtime.timer(Duration::from_millis(200)).await;
        let acked_before = observer.acked();
        assert!(acked_before > ReplOffset::ZERO);

        // ---- (2) Tear the replica link down and bring a NEW one up, resuming. ----
        run1.set(false); // stop the first replica loop on its next iteration
        runtime.timer(Duration::from_millis(150)).await; // let it wind down

        // A fresh link with the SAME node id, resuming from the last-acked offset.
        let observer2 = spawn_replica(addr, acked_before, || true);

        let resumed = poll_until(&runtime, timeout, || {
            matches!(observer2.state(), Some(ReplState::Synced { .. }))
                && observer2.acked() >= acked_before
        })
        .await;
        assert!(
            resumed,
            "the reconnected replica must resume and advance past {acked_before:?}; now {:?}",
            observer2.acked()
        );
        // It must never have observed an offset below the resume point.
        assert!(
            observer2.acked() >= acked_before,
            "the resumed link's offset regressed below the resume point"
        );
    });
}
