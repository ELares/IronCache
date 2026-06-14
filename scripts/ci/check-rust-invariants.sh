#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Requires bash (NUL-safe `read -d ''` for the SPDX scan); CI invokes it as
# `bash scripts/ci/check-rust-invariants.sh`.
#
# Rust invariant lints (INVARIANTS.md "CI-checkability"). Grep-based, offline, and
# deterministic. Enforces the mechanical halves of the load-bearing invariants on
# the engine source:
#
#   1. No fork: no `fork(` call or libc fork binding anywhere (invariant 4).
#   2. Determinism: no direct clock/RNG use (std::time, Instant/SystemTime, and the
#      sanctioned-elsewhere clock/RNG crates chrono/time/fastrand/rand/quanta/
#      coarsetime/minstant/web-time/getrandom) outside the `ironcache-env` crate
#      (invariant 2, ADR-0003). Line comments are stripped before matching so prose
#      mentioning these APIs does not false-positive.
#   3. Shared-nothing: no `std::sync::{Mutex,RwLock}` in the store/shard hot-path
#      crates (invariant 1, ADR-0002). PR-1 has no store yet, so the guarded set
#      is the per-shard crates that exist; it grows as the store lands.
#   4. SPDX header: every `.rs` file in the REPO (not just crates/) starts with the
#      SPDX-License-Identifier line. Scans from the repo root so root-level
#      build.rs / examples are covered; iterates NUL-safely for paths with spaces.
#
# Exit non-zero on any violation, printing the offending lines. Run from anywhere.
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CRATES="$ROOT/crates"
fail=0

# Collect the crate Rust source files (skip target/ and any vendored dirs). Used
# by the no-fork / determinism / shared-nothing checks, which lint engine source.
rs_files() {
    find "$CRATES" -name '*.rs' -not -path '*/target/*' | sort
}

# Collect ALL Rust source files under the repo root (skip target/ and .git), for
# the SPDX scan, which must cover root-level files too (build.rs, examples).
all_rs_files() {
    find "$ROOT" -name '*.rs' -not -path '*/target/*' -not -path '*/.git/*' | sort
}

# Helper: grep across a set of files, excluding lines that are comments or are in
# #[cfg(test)] is out of scope here (we lint production AND test source uniformly
# for the determinism seam, since tests must also go through Env for DST).
section() {
    printf '== %s ==\n' "$1"
}

# ---------------------------------------------------------------------------
# 1. No fork() anywhere (invariant 4).
# ---------------------------------------------------------------------------
section "no fork()"
# Match a fork( call or a libc::fork binding. Allow the word "fork" in prose by
# anchoring on a call-shaped or path-shaped token.
if rs_files | xargs grep -nE '\bfork\s*\(|libc::fork|::fork\b' 2>/dev/null \
    | grep -v '// *lint-allow: fork-mention'; then
    echo "ERROR: fork() is forbidden (invariant 4, no-fork)."
    fail=1
else
    echo "ok: no fork() found"
fi

# ---------------------------------------------------------------------------
# 2. Determinism: no direct time/rand outside ironcache-env (invariant 2).
# ---------------------------------------------------------------------------
section "no direct time/rand outside ironcache-env / ironcache-runtime"
# Build the file list excluding the two sanctioned boundaries: ironcache-env (the
# clock/RNG seam, ADR-0003) and ironcache-runtime (the I/O and TIMER seam, whose
# tokio-backed `timer()` is the canonical timer abstraction every backend arms
# under, RUNTIME_ABSTRACTION.md). All OTHER crates (the command/decision paths)
# must route time/RNG through Env.
nonenv_files() {
    rs_files | grep -v -e '/crates/ironcache-env/' -e '/crates/ironcache-runtime/'
}
# Patterns that must not appear in the linted crates: the real-time std APIs plus
# the clock/RNG crates that read real time or OS entropy. We match the
# NONDETERMINISM CRATES by crate-root path (chrono/fastrand/rand/quanta/
# coarsetime/minstant/web_time/getrandom and the `time` crate). The bare `time`
# crate is matched only at a crate-root boundary (`time::`, not `std::time::` /
# `core::time::` / `tokio::time::`, which are the deterministic Duration type and
# the runtime timer seam) by requiring a non-`:` char before it. `Duration` is a
# value type and is never flagged. Comments are stripped below.
TIME_RAND='Instant::now|SystemTime::now|std::time::Instant|std::time::SystemTime|\b(chrono|fastrand|rand|quanta|coarsetime|minstant|web_time|getrandom)::|(^|[^:[:alnum:]_])time::[A-Za-z]'
det_hits=""
for f in $(nonenv_files); do
    # Strip `//` line comments before matching so a comment that mentions one of
    # these APIs is not a false positive. (Block comments are rare here and the
    # codebase uses `//`/`//!`/`///`; this is the mechanical-CI bar, not a parser.)
    hits="$(sed 's://.*$::' "$f" \
        | grep -nE "$TIME_RAND" 2>/dev/null \
        | grep -v '// *lint-allow: env-seam' || true)"
    if [ -n "$hits" ]; then
        det_hits="$det_hits\n$f:\n$hits"
    fi
done
if [ -n "$det_hits" ]; then
    printf '%b\n' "$det_hits"
    echo "ERROR: direct time/rand outside the env/runtime seams (invariant 2, determinism)."
    echo "       Route clock/RNG through the ironcache-env Env seam (ADR-0003)."
    fail=1
else
    echo "ok: no direct time/rand outside ironcache-env / ironcache-runtime"
fi

# ---------------------------------------------------------------------------
# 3. Shared-nothing: no std::sync lock types in hot-path crates (invariant 1).
# ---------------------------------------------------------------------------
section "no std::sync locks in hot-path crates"
# The hot-path crates: those that own per-shard state. As of PR-3a these are the
# storage waist (ironcache-storage), the concrete per-shard store (ironcache-store),
# the per-shard eviction policy (ironcache-eviction: S3-FIFO queues / Random roster,
# unsynchronized per ADR-0005), plus ironcache-server (connection state) and
# ironcache-observe (per-shard counters). ironcache-runtime is the I/O seam and is
# allowed Arc<AtomicBool> for the cross-thread shutdown FLAG only (not hot-path data),
# so we lint for Mutex/RwLock specifically, not all of std::sync. The store/eviction
# crates are shared-nothing (ADR-0005): the per-shard state is unsynchronized, no lock.
HOTPATH_CRATES="ironcache-storage ironcache-store ironcache-eviction ironcache-server ironcache-observe"
lock_hits=""
for c in $HOTPATH_CRATES; do
    dir="$CRATES/$c"
    [ -d "$dir" ] || continue
    hits="$(find "$dir" -name '*.rs' -not -path '*/target/*' \
        | xargs grep -nE 'std::sync::(Mutex|RwLock)|\bMutex<|\bRwLock<' 2>/dev/null \
        | grep -v '// *lint-allow: shared-nothing' || true)"
    if [ -n "$hits" ]; then
        lock_hits="$lock_hits\n$hits"
    fi
done
if [ -n "$lock_hits" ]; then
    printf '%b\n' "$lock_hits"
    echo "ERROR: std::sync Mutex/RwLock in a hot-path crate (invariant 1, shared-nothing)."
    fail=1
else
    echo "ok: no std::sync locks in hot-path crates ($HOTPATH_CRATES)"
fi

# ---------------------------------------------------------------------------
# 4. SPDX header on every .rs file (whole repo, not just crates/).
# ---------------------------------------------------------------------------
section "SPDX header on every .rs"
# Iterate NUL-safely (POSIX sh): pipe `find -print0` into a `read -d ''` loop.
# The loop runs in a subshell (so var assignments do not survive), therefore we
# record misses to a temp file and read it back after the pipe.
missing_file="$(mktemp)"
find "$ROOT" -name '*.rs' -not -path '*/target/*' -not -path '*/.git/*' -print0 \
    | while IFS= read -r -d '' f; do
        first="$(head -n 1 "$f")"
        case "$first" in
            *"SPDX-License-Identifier: MIT OR Apache-2.0"*) : ;;
            *) printf '  %s\n' "$f" >> "$missing_file" ;;
        esac
    done
if [ -s "$missing_file" ]; then
    echo "ERROR: missing SPDX header on:"
    cat "$missing_file"
    fail=1
else
    echo "ok: every .rs has the SPDX header"
fi
rm -f "$missing_file"

# ---------------------------------------------------------------------------
echo
if [ "$fail" -ne 0 ]; then
    echo "INVARIANT LINTS FAILED"
    exit 1
fi
echo "INVARIANT LINTS PASSED"
