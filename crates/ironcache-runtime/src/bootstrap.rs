// SPDX-License-Identifier: MIT OR Apache-2.0
//! Thread-per-core bootstrap (RUNTIME.md "topology", ADR-0002).
//!
//! [`run_shards`] spawns one OS thread per shard, each with its own current-thread
//! tokio runtime and `LocalSet` (the shard executor) and its own `SO_REUSEPORT`
//! accept loop. A connection accepted on a shard is served entirely on that shard,
//! so per-connection state is core-local with no shared hot-path structure.
//!
//! The per-connection serve logic is supplied by the caller as an async closure
//! over the shard's [`crate::Runtime`], keeping this layer free of any protocol or
//! command knowledge.

use core::time::Duration;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A shard identity: which shard (`index`) of how many (`total`). Used for
/// per-shard counters (OBSERVABILITY.md) and for the `k = HASH(KEY) % N` routing
/// rule once a store exists (ADR-0002); PR-1 has no store, so it is identity only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardId {
    /// This shard's index in `[0, total)`.
    pub index: usize,
    /// The total number of shards `N`.
    pub total: usize,
}

/// How many shards to run and where to bind. The shard count defaults to the
/// available parallelism (CONFIG.md) but is overridable.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Number of shards (one OS thread / current-thread runtime each).
    pub shards: usize,
    /// The address every shard binds with `SO_REUSEPORT`.
    pub bind: SocketAddr,
}

/// A handle to a running set of shards, used to signal graceful shutdown and join.
#[derive(Debug)]
pub struct ShardSet {
    shutdown: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl ShardSet {
    /// Signal all shards to stop accepting and drain, then wait for their threads.
    pub fn shutdown_and_join(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        for h in self.handles {
            let _ = h.join();
        }
    }

    /// The shared shutdown flag, so a signal handler can flip it without holding
    /// the [`ShardSet`]. Reads are relaxed-acceptable: shutdown is level-triggered.
    #[must_use]
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }
}

/// The default shard count: the host's available parallelism (CONFIG.md). Never
/// zero (a degenerate host reports at least one).
#[must_use]
pub fn available_shards() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

#[cfg(feature = "tokio")]
mod tokio_bootstrap {
    use super::{Arc, AtomicBool, Duration, Ordering, ShardConfig, ShardId, ShardSet};
    use crate::TokioRuntime;
    use crate::tokio_rt::{bind_reuseport, bind_reuseport_std};
    use std::future::Future;

    /// Run the shard set. For each shard this spawns an OS thread that:
    ///   1. builds a current-thread tokio runtime (NOT multi-thread; ADR-0002),
    ///   2. binds the shared address with `SO_REUSEPORT`,
    ///   3. accepts connections in a loop, spawning `serve` per connection on the
    ///      shard-local `LocalSet`,
    ///   4. stops accepting when the shutdown flag is set.
    ///
    /// `serve` is cloned per shard and invoked per connection with the shard's
    /// [`TokioRuntime`], the accepted [`tokio::net::TcpStream`], and the
    /// [`ShardId`]. It returns a `'static` future (the connection task).
    ///
    /// Returns a [`ShardSet`] for shutdown/join. If a shard thread fails to bind
    /// it logs to stderr and exits that thread; at least one bound shard is
    /// required for a useful server (the binary checks this separately).
    pub fn run_shards<S, Fut>(cfg: &ShardConfig, serve: S) -> std::io::Result<ShardSet>
    where
        S: Fn(TokioRuntime, tokio::net::TcpStream, ShardId) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let shutdown = Arc::new(AtomicBool::new(false));
        let total = cfg.shards.max(1);

        // Pre-flight bind probe so a bind failure (e.g. port in use) surfaces as
        // an error from this synchronous call rather than silently inside a shard
        // thread. The probe uses the std (reactor-free) binder; shards re-bind
        // with SO_REUSEPORT inside their own tokio runtimes.
        let probe = bind_reuseport_std(cfg.bind)?;
        drop(probe);

        let mut handles = Vec::with_capacity(total);
        for index in 0..total {
            let shutdown = Arc::clone(&shutdown);
            let serve = serve.clone();
            let bind = cfg.bind;
            let shard = ShardId { index, total };
            let handle = std::thread::Builder::new()
                .name(format!("ironcache-shard-{index}"))
                .spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            eprintln!("shard {index}: failed to build runtime: {e}");
                            return;
                        }
                    };
                    let local = tokio::task::LocalSet::new();
                    local.block_on(&rt, async move {
                        let listener = match bind_reuseport(bind) {
                            Ok(l) => l,
                            Err(e) => {
                                eprintln!("shard {index}: bind {bind} failed: {e}");
                                return;
                            }
                        };
                        let runtime = TokioRuntime::new();
                        accept_loop(&listener, &runtime, &serve, shard, &shutdown).await;
                    });
                })?;
            handles.push(handle);
        }

        Ok(ShardSet { shutdown, handles })
    }

    async fn accept_loop<S, Fut>(
        listener: &tokio::net::TcpListener,
        runtime: &TokioRuntime,
        serve: &S,
        shard: ShardId,
        shutdown: &Arc<AtomicBool>,
    ) where
        S: Fn(TokioRuntime, tokio::net::TcpStream, ShardId) -> Fut + Clone + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let _ = runtime;
        while !shutdown.load(Ordering::Relaxed) {
            // Race the accept against a short timer so a shutdown is observed even
            // when no new connection arrives (no blocking accept that ignores the
            // flag).
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((stream, _peer)) => {
                            let _ = stream.set_nodelay(true);
                            let fut = serve(TokioRuntime::new(), stream, shard);
                            // Pin to this shard's LocalSet: the connection lives
                            // its whole life on this core (ADR-0002).
                            tokio::task::spawn_local(fut);
                        }
                        Err(e) => {
                            // Transient accept errors (e.g. EMFILE) should not kill
                            // the shard; back off briefly and continue.
                            eprintln!("shard {}: accept error: {e}", shard.index);
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }
}

#[cfg(feature = "tokio")]
pub use tokio_bootstrap::run_shards;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_shards_is_at_least_one() {
        assert!(available_shards() >= 1);
    }

    #[test]
    fn shard_id_fields() {
        let s = ShardId { index: 2, total: 4 };
        assert_eq!(s.index, 2);
        assert_eq!(s.total, 4);
    }
}
