// SPDX-License-Identifier: MIT OR Apache-2.0
//! #676 Phase 1b measurement: the DIRTY-KEY FRACTION go/no-go for incremental delta snapshots.
//!
//! The during-snapshot p99.9 (291ms) is a persist-thread READ-bandwidth floor; the only lever is
//! reading FEWER bytes, i.e. a delta save that re-encodes only the keys written since the last base.
//! Whether that helps hinges on ONE number the owner gated the XL delta phases on (#676 sec 4/6):
//! what FRACTION of the keyspace is dirty per snapshot interval? If most keys are dirty, a delta
//! re-reads almost everything and the floor is untouched; if few are, the delta save is small.
//!
//! This measures that fraction directly, driving REAL writes through the Phase-1a dirty-key set
//! (`enable_dirty_tracking` / `dirty_key_count`), so it also validates that machinery end to end.
//! The dirty COUNT fraction equals the dirty BYTES fraction under a uniform value size (as here);
//! a skewed value-size workload would weight it, but the count fraction is the first-order signal.
//!
//! Key insight it quantifies: under zipf skew the DISTINCT keys touched by W writes is far below W
//! (hot keys repeat), so skew SHRINKS the dirty set and HELPS the delta. The table shows, per skew,
//! how the dirty fraction grows with writes-per-interval = write_qps * snapshot_interval_secs.
//!
//! Run: `cargo run --release -p ironcache-bench --bin dirty_fraction`

use ironcache_bench::workload::Zipf;
use ironcache_env::SplitMix64;
use ironcache_storage::{ExpireWrite, NewValue, Store};
use ironcache_store::ShardStore;

/// The `k:<idx>` key encoding (matches the loadgen's `Workload::key_bytes`).
fn key_bytes(idx: u64) -> Vec<u8> {
    format!("k:{idx}").into_bytes()
}

fn main() {
    const N: u64 = 1_000_000; // keyspace
    const SEED: u64 = 0x5DEE_CE66_D1CE_5EED;
    // Writes-per-interval checkpoints. Interpret W = write_qps * snapshot_interval_secs, e.g.
    // 60k = 30k write/s * 2s, or 300k write/s * 0.2s; 1.8M = 30k write/s * 60s.
    let checkpoints: [u64; 8] = [
        10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000,
    ];
    let thetas = [0.99_f64, 0.90, 0.50];
    let value = vec![b'x'; 128];

    println!("# {N} keys, 128B values, seed fixed. Dirty fraction = distinct keys written / N.");
    println!("# W = writes in one snapshot interval (= write_qps * interval_secs).");

    for &theta in &thetas {
        // Fresh store per skew so the dirty set starts empty at the epoch cut.
        let mut store = ShardStore::new(1);
        for i in 0..N {
            store.upsert(
                0,
                &key_bytes(i),
                NewValue::Bytes(&value),
                ExpireWrite::Clear,
                ionow(),
            );
        }
        // Epoch cut: enable tracking AFTER the base is fully populated, so only the interval's
        // writes accumulate (exactly what a delta save captures between two bases).
        store.enable_dirty_tracking();

        let zipf = Zipf::new(N, theta);
        let mut rng = SplitMix64::new(SEED ^ theta.to_bits());
        println!("\ntheta = {theta}");
        println!(
            "  {:>10}  {:>10}  {:>9}  {:>9}",
            "W", "distinct", "frac", "reuse"
        );
        let mut applied: u64 = 0;
        for &w in &checkpoints {
            while applied < w {
                let idx = zipf.next(&mut rng);
                store.upsert(
                    0,
                    &key_bytes(idx),
                    NewValue::Bytes(&value),
                    ExpireWrite::Clear,
                    ionow(),
                );
                applied += 1;
            }
            let distinct = store.dirty_key_count().expect("tracking on") as u64;
            let pct = (distinct as f64 / N as f64) * 100.0;
            // reuse = writes per distinct key (how much skew collapsed W into fewer keys).
            let reuse = applied as f64 / distinct.max(1) as f64;
            println!("  {applied:>10}  {distinct:>10}  {pct:>7.1}%  {reuse:>7.2}x");
        }
    }
}

/// A fixed logical clock stamp: this measurement never sets TTLs, so any constant works and keeps
/// the run deterministic (ADR-0003, no wall clock).
fn ionow() -> ironcache_storage::UnixMillis {
    ironcache_storage::UnixMillis(0)
}
