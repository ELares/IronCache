#!/bin/sh
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Rust invariant lints (INVARIANTS.md "CI-checkability"). Grep-based, offline, and
# deterministic. Enforces the mechanical halves of the load-bearing invariants on
# the engine source:
#
#   1. No fork: no `fork(` call or libc fork binding anywhere (invariant 4).
#   2. Determinism: no direct `std::time` / `Instant::now` / `SystemTime::now` /
#      `rand::` outside the sanctioned `ironcache-env` crate (invariant 2,
#      ADR-0003).
#   3. Shared-nothing: no `std::sync::{Mutex,RwLock}` in the store/shard hot-path
#      crates (invariant 1, ADR-0002). PR-1 has no store yet, so the guarded set
#      is the per-shard crates that exist; it grows as the store lands.
#   4. SPDX header: every `.rs` file starts with the SPDX-License-Identifier line.
#
# Exit non-zero on any violation, printing the offending lines. Run from anywhere.
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CRATES="$ROOT/crates"
fail=0

# Collect the Rust source files once (skip target/ and any vendored dirs).
rs_files() {
    find "$CRATES" -name '*.rs' -not -path '*/target/*' | sort
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
section "no direct time/rand outside ironcache-env"
# Build the file list excluding the env crate (the sanctioned boundary).
nonenv_files() {
    rs_files | grep -v '/crates/ironcache-env/'
}
# Patterns that must not appear outside ironcache-env.
TIME_RAND='Instant::now|SystemTime::now|std::time::Instant|std::time::SystemTime|\brand::|rand::random'
if nonenv_files | xargs grep -nE "$TIME_RAND" 2>/dev/null \
    | grep -v '// *lint-allow: env-seam'; then
    echo "ERROR: direct time/rand outside ironcache-env (invariant 2, determinism)."
    echo "       Route clock/RNG through the ironcache-env Env seam (ADR-0003)."
    fail=1
else
    echo "ok: no direct time/rand outside ironcache-env"
fi

# ---------------------------------------------------------------------------
# 3. Shared-nothing: no std::sync lock types in hot-path crates (invariant 1).
# ---------------------------------------------------------------------------
section "no std::sync locks in hot-path crates"
# The hot-path crates: those that own per-shard state. The store crate lands in
# PR-2; until then the per-shard owner is ironcache-server (connection state) and
# ironcache-observe (per-shard counters). ironcache-runtime is the I/O seam and is
# allowed Arc<AtomicBool> for the cross-thread shutdown FLAG only (not hot-path
# data), so we lint for Mutex/RwLock specifically, not all of std::sync.
HOTPATH_CRATES="ironcache-server ironcache-observe"
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
# 4. SPDX header on every .rs file.
# ---------------------------------------------------------------------------
section "SPDX header on every .rs"
missing=""
for f in $(rs_files); do
    # The header must be on the first line.
    first="$(head -n 1 "$f")"
    case "$first" in
        *"SPDX-License-Identifier: MIT OR Apache-2.0"*) : ;;
        *)
            missing="$missing $f"
            ;;
    esac
done
if [ -n "$missing" ]; then
    echo "ERROR: missing SPDX header on:"
    for f in $missing; do echo "  $f"; done
    fail=1
else
    echo "ok: every .rs has the SPDX header"
fi

# ---------------------------------------------------------------------------
echo
if [ "$fail" -ne 0 ]; then
    echo "INVARIANT LINTS FAILED"
    exit 1
fi
echo "INVARIANT LINTS PASSED"
