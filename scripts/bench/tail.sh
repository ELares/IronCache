#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# tail.sh -- the #518 MOAT PROOF preset (#574). A THIN wrapper over headtohead.sh that turns ON the
# full ADVERSARIAL mix in one command: mixed op ratio + zipf hot-key skew (already the headtohead
# defaults) + concurrent EVICTION + concurrent SNAPSHOT, then reports the p50/p99/p99.9/p99.99
# OVERALL open-loop op latency for IronCache vs the competitor. The p99.9 (p999) tail UNDER A
# CONCURRENT DURABLE SAVE is the moat metric: IronCache yields between snapshot chunks (#571) so its
# per-op work stays bounded (#570), where Redis fork-COW stalls and Dragonfly snapshot-spikes.
#
# It ONLY presets env knobs (each still overridable) and then execs headtohead.sh, so every
# headtohead flag/knob and its whole lifecycle (build, pinning, cleanup, JSON, verdict) apply
# unchanged. See docs/bench/TAIL_LATENCY.md for the methodology + how to reproduce on real HW.
#
# Usage:
#   scripts/bench/tail.sh [--out-dir DIR] [--smoke]
#   SMOKE=1 scripts/bench/tail.sh                            # fast CI/local self-test (seconds)
#   COMPETITOR_BIN=$(command -v valkey-server) scripts/bench/tail.sh
#   MAXMEMORY=512mb SNAPSHOT_INTERVAL_SECS=2 scripts/bench/tail.sh
#
# Overridable presets (this wrapper only sets a DEFAULT; an inherited value wins):
#   SNAPSHOT=1    concurrent BGSAVE during the latency pass (the #571 payoff).
#   EVICT=1       every server boots in its evicting cache mode under a LOW MAXMEMORY.
#   MAXMEMORY     the LOW ceiling that forces eviction. Default 256mb: below the default
#                 keyspace*value dataset (so redis/valkey/keydb/ironcache evict) AND at
#                 Dragonfly's 256MiB-per-thread boot floor for a 1-thread pin. For a Dragonfly
#                 head-to-head on N threads set MAXMEMORY >= 256MiB*N and a dataset above it
#                 (raise KEYSPACE/KEYCOUNT), else Dragonfly refuses to boot (headtohead warns).
#
# To turn a dimension OFF for an ablation, override it: `EVICT=0 scripts/bench/tail.sh` measures
# the snapshot tail WITHOUT eviction; `SNAPSHOT=0 scripts/bench/tail.sh` is a plain eviction run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Presets: DEFAULT the adversarial knobs but let an inherited env value win (so ablations work).
export SNAPSHOT="${SNAPSHOT:-1}"
export EVICT="${EVICT:-1}"
export MAXMEMORY="${MAXMEMORY:-256mb}"

echo "[tail] #518 moat preset: SNAPSHOT=${SNAPSHOT} EVICT=${EVICT} MAXMEMORY=${MAXMEMORY}"
echo "[tail] (mixed + zipf skew are the headtohead defaults). Reporting p50/p99/p99.9/p99.99."
echo "[tail] override any knob to ablate; see docs/bench/TAIL_LATENCY.md."

exec "${SCRIPT_DIR}/headtohead.sh" "$@"
