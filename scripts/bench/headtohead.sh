#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# IronCache vs Valkey head-to-head benchmark (BENCHMARK.md #8 / #96, PR-A4 of the
# performance track). It measures IronCache and a PINNED competitor (Valkey 9.1.0, the
# ADR-0017 bar) SIDE BY SIDE on the same box, under identical knobs, and emits ONE
# comparison report of the two headline metrics:
#
#   1. QPS-per-core   (closed-loop peak throughput / server core count)
#   2. bytes-per-key  (INFO used_memory delta over a known key count)
#
# Both metrics are measured the SAME way on both servers, so the comparison is
# apples-to-apples:
#
#   - bytes-per-key: read INFO used_memory on the empty server, deterministically
#     populate EXACTLY N distinct keys (key:0..key:N-1) each with a fixed-size value via
#     `redis-cli --pipe`, re-read used_memory; bytes_per_key = delta / N. The loadgen is
#     NOT used here: its zipf SETs do not cover the keyspace uniformly, so they would not
#     land N distinct keys.
#   - QPS-per-core: run `loadgen --mode closed` with the shared workload against the
#     server; qps_per_core = qps / server_core_count.
#
# The VERDICT is the ADR-0017 bar: IronCache PASSES when its qps_per_core EXCEEDS the
# competitor's AND its bytes_per_key is BELOW the competitor's.
#
# Usage:
#   scripts/bench/headtohead.sh [--out-dir DIR] [--smoke]
#
# Competitor resolution (in order):
#   COMPETITOR_BIN env  ->  valkey-server on PATH  ->  redis-server on PATH (STAND-IN).
# redis-server is RESP/Valkey-wire-compatible and is fine for a smoke, but the PUBLISHED
# bar is the pinned valkey-server from docs/bench/COMPETITORS.md; a redis-server verdict
# is INDICATIVE only.
#
# Every knob is overridable via an environment variable (see "LOCKED KNOBS").
#
#   SMOKE=1 scripts/bench/headtohead.sh                     # fast tiny run
#   COMPETITOR_BIN=$(command -v valkey-server) ...          # explicit competitor
#   SERVER_CORES=0-3 CLIENT_CORES=4-9 ...                   # explicit pinning (Linux)
#   SNAPSHOT=1 EVICT=1 scripts/bench/headtohead.sh          # the #518 moat mix (adversarial tail)
#   SERVER_CORES=0-7 PERSIST_CORE=8 CLIENT_CORES=9-15 ...   # dedicate core 8 to IronCache's persist
#                                                           #   thread (#589): the datapath stays on
#                                                           #   0-7, the persist encode escapes to 8.
#
# TAIL LATENCY (#574): the open-loop pass reports p50/p99/p99.9(p999)/p99.99(p9999) OVERALL op
# latency for each server. SNAPSHOT=1 fires a background BGSAVE during that pass so the p99.9 tail
# CAPTURES the concurrent durable-save cost (the #518 moat proof; see docs/bench/TAIL_LATENCY.md).
# scripts/bench/tail.sh is a thin wrapper that presets the adversarial mix.

set -euo pipefail

# ---------------------------------------------------------------------------
# Repo root resolution. The script lives in <repo>/scripts/bench, so the repo root is
# two directories up. Resolve it independent of the caller's CWD.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# LOCKED KNOBS. The standard-run values; each is overridable by an environment
# variable. The defaults mirror run.sh so the head-to-head workload matches the
# single-server harness.
# ---------------------------------------------------------------------------
SEED="${SEED:-6342047879154770157}"     # 0x5DEE_CE66_D1CE_5EED, fixed reproducible workload.
KEYSPACE="${KEYSPACE:-1000000}"          # distinct keys for the loadgen zipf workload.
THETA="${THETA:-0.99}"                   # zipf exponent (YCSB default skew).
READ_RATIO="${READ_RATIO:-0.9}"          # 90% GET / 10% SET.
VALUE_SIZE="${VALUE_SIZE:-128}"          # SET value bytes (also the bytes-per-key value size).
DURATION_SECS="${DURATION_SECS:-10}"     # measured closed-loop pass duration.
CONNECTIONS="${CONNECTIONS:-50}"         # closed-loop load fan-out / open-loop dispatch pool.
RATE="${RATE:-50000}"                    # open-loop target ops/sec (optional latency pass).
WARMUP_SECS="${WARMUP_SECS:-3}"          # write-only warmup before the QPS pass.

# Bytes-per-key population size: EXACTLY this many distinct keys are inserted to measure
# the used_memory delta. The full run uses a large N; SMOKE shrinks it.
KEYCOUNT="${KEYCOUNT:-1000000}"

# Server knobs.
PORT="${PORT:-6399}"                     # RESP port (loopback only).
HOST="127.0.0.1"                         # loopback, always (isolation).
# IronCache memory ceiling, via the IRONCACHE_MAXMEMORY overlay (no CLI flag). Generous
# so the measurement never evicts. The competitor runs with --maxmemory 0 (off).
MAXMEMORY="${MAXMEMORY:-4gb}"

# EVICTION WORKLOAD mode (default OFF = the standard never-evict measurement). When EVICT=1
# the caller sets MAXMEMORY LOW (below the dataset) so eviction fires continuously, and
# every server is booted in its EVICTING cache mode (IronCache: its default allkeys-lru
# under a low IRONCACHE_MAXMEMORY; Dragonfly: --cache_mode true; redis/valkey:
# --maxmemory <low> --maxmemory-policy allkeys-lru). The populate then INTENTIONALLY
# exceeds the ceiling, so DBSIZE < KEYCOUNT is EXPECTED (keys evict during populate) and is
# tolerated; the bytes-per-key number is not meaningful under eviction but is still emitted.
# The script does NOT override MAXMEMORY itself - the caller picks the low ceiling.
EVICT="${EVICT:-0}"

# CONCURRENT-SNAPSHOT mode (default OFF = the standard latency pass with no background save).
# When SNAPSHOT=1, a background loop fires BGSAVE on the server UNDER TEST every
# SNAPSHOT_INTERVAL_SECS DURING the open-loop latency pass, so the measured p99.9/p99.99 CAPTURES
# the concurrent durable-save tail (the #518 moat proof: IronCache yields between snapshot chunks
# #571 so its per-op tail stays bounded; Redis fork-COW stalls; Dragonfly snapshot-spikes). Each
# server boots with a FRESH, PRIVATE snapshot dir (empty on boot so nothing is loaded, removed
# after) and the save is actually enabled (IronCache: IRONCACHE_DATA_DIR; redis/valkey/keydb:
# --dir into that private dir with BGSAVE still honored under --save ''; dragonfly: a real
# --dbfilename). The loop fires ONCE immediately so even a sub-second SMOKE window captures at
# least one save, then every interval. The QPS pass runs BEFORE the loop starts, so peak QPS is
# unchanged; only the latency tail reflects the save.
SNAPSHOT="${SNAPSHOT:-0}"
SNAPSHOT_INTERVAL_SECS="${SNAPSHOT_INTERVAL_SECS:-3}"   # BGSAVE cadence during the open-loop pass.

# The pinned competitor version (the published bar). Used to WARN on a version mismatch.
PINNED_VALKEY_VERSION="9.1.0"

# ---------------------------------------------------------------------------
# Flag parsing: --out-dir DIR, --smoke. Anything else is an error.
# ---------------------------------------------------------------------------
OUT_DIR=""
SMOKE="${SMOKE:-0}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      [[ $# -ge 2 ]] || { echo "error: --out-dir needs an argument" >&2; exit 2; }
      OUT_DIR="$2"
      shift 2
      ;;
    --out-dir=*)
      OUT_DIR="${1#*=}"
      shift
      ;;
    --smoke)
      SMOKE=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--out-dir DIR] [--smoke]" >&2
      echo "Competitor: COMPETITOR_BIN env, else valkey-server, else redis-server (stand-in)." >&2
      echo "Env knobs incl. EVICT=1 (eviction mix) and SNAPSHOT=1 (concurrent-BGSAVE tail, #574)." >&2
      echo "SNAPSHOT=1 EVICT=1 $0  runs the full #518 moat mix; see docs/bench/TAIL_LATENCY.md." >&2
      echo "See scripts/bench/README.md for the full knob list (env vars)." >&2
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

# SMOKE shrinks every dimension so the whole script finishes in a few seconds. It is for
# CI / local validation, NOT a publishable measurement.
if [[ "${SMOKE}" == "1" ]]; then
  KEYSPACE=1000
  KEYCOUNT=2000
  DURATION_SECS=1
  WARMUP_SECS=1
  CONNECTIONS=4
  RATE=2000
  echo "[h2h] SMOKE mode: tiny durations/keyspace/keycount; results are NOT publishable."
fi

# Announce EVICTION mode (the caller is responsible for a LOW MAXMEMORY below the dataset).
if [[ "${EVICT}" == "1" ]]; then
  echo "[h2h] EVICTION mode ON: every server boots in its evicting cache mode under"
  echo "[h2h] MAXMEMORY=${MAXMEMORY} (caller-set, expected BELOW the dataset). The populate"
  echo "[h2h] will exceed the ceiling, so DBSIZE < KEYCOUNT is EXPECTED and tolerated; the"
  echo "[h2h] bytes-per-key number is not meaningful under continuous eviction."
fi

# Announce CONCURRENT-SNAPSHOT mode.
if [[ "${SNAPSHOT}" == "1" ]]; then
  echo "[h2h] SNAPSHOT mode ON: a background BGSAVE fires every ${SNAPSHOT_INTERVAL_SECS}s DURING the"
  echo "[h2h] open-loop latency pass on the server under test, so the measured p99.9/p99.99 CAPTURES"
  echo "[h2h] the concurrent durable-save tail (IronCache yields between chunks #571; Redis forks;"
  echo "[h2h] Dragonfly snapshot-spikes). Each server boots with a fresh private snapshot dir (removed"
  echo "[h2h] after the pass); the loop fires once immediately so even the SMOKE window sees >=1 save."
fi

# ---------------------------------------------------------------------------
# Build the release binaries (ironcache + the bench crate, which produces loadgen).
# ---------------------------------------------------------------------------
echo "[h2h] building release binaries (cargo build --release -p ironcache -p ironcache-bench)..."
cargo build --release -p ironcache -p ironcache-bench

# The release binaries live under Cargo's target dir. Honor CARGO_TARGET_DIR (e.g. a shared
# container cache volume) when set; otherwise the in-tree ${REPO_ROOT}/target. This keeps the
# default (unset) behavior byte-identical while letting a container build that redirects the
# target dir still find the binaries.
TARGET_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}"
IRONCACHE_BIN="${TARGET_DIR}/release/ironcache"
LOADGEN_BIN="${TARGET_DIR}/release/loadgen"
for b in "${IRONCACHE_BIN}" "${LOADGEN_BIN}"; do
  [[ -x "${b}" ]] || { echo "error: expected binary missing after build: ${b}" >&2; exit 1; }
done

# redis-cli is required: it is how we PING, read INFO, and mass-insert via --pipe against
# BOTH servers (IronCache speaks RESP and supports ECHO for the --pipe sentinel).
if ! command -v redis-cli >/dev/null 2>&1; then
  echo "error: redis-cli not found on PATH; it is required (PING, INFO, --pipe populate)." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# IronCache version (parse "ironcache 0.0.0" -> 0.0.0). The jemalloc boot warning goes
# to stderr; stdout is clean. Take the last token of the first stdout line.
# ---------------------------------------------------------------------------
VERSION_LINE="$("${IRONCACHE_BIN}" --version 2>/dev/null | head -n1)"
IC_VER="${VERSION_LINE##* }"
[[ -n "${IC_VER}" ]] || IC_VER="unknown"

OS_NAME="$(uname -s)"
ARCH_NAME="$(uname -m)"

# Default out dir: bench-results/headtohead-<ver>-<os>-<arch> (gitignored).
if [[ -z "${OUT_DIR}" ]]; then
  OUT_DIR="${REPO_ROOT}/bench-results/headtohead-${IC_VER}-${OS_NAME}-${ARCH_NAME}"
fi
mkdir -p "${OUT_DIR}"
echo "[h2h] ironcache version=${IC_VER} os=${OS_NAME} arch=${ARCH_NAME}"
echo "[h2h] output dir: ${OUT_DIR}"

# ---------------------------------------------------------------------------
# COMPETITOR RESOLUTION: COMPETITOR_BIN env -> valkey-server -> redis-server.
# ---------------------------------------------------------------------------
COMPETITOR_BIN="${COMPETITOR_BIN:-}"
if [[ -z "${COMPETITOR_BIN}" ]]; then
  if command -v valkey-server >/dev/null 2>&1; then
    COMPETITOR_BIN="$(command -v valkey-server)"
  elif command -v redis-server >/dev/null 2>&1; then
    COMPETITOR_BIN="$(command -v redis-server)"
  fi
fi
if [[ -z "${COMPETITOR_BIN}" || ! -x "${COMPETITOR_BIN}" ]]; then
  echo "error: no competitor binary found. Set COMPETITOR_BIN, or install valkey-server" >&2
  echo "       (the pinned ${PINNED_VALKEY_VERSION} bar; see docs/bench/COMPETITORS.md) or redis-server." >&2
  exit 1
fi

# Classify the competitor by its basename + version banner. The kind drives whether the
# verdict is the published bar (valkey) or merely indicative (a redis-server stand-in).
COMPETITOR_BASENAME="$(basename "${COMPETITOR_BIN}")"
COMPETITOR_VERSION_RAW="$("${COMPETITOR_BIN}" --version 2>&1 | head -n1)"
COMPETITOR_KIND="unknown"
COMPETITOR_NAME="competitor"
# KeyDB and Memcached classification arms MUST precede the generic redis arm. KeyDB's
# banner advertises "KeyDB" but its version line also embeds "redis" (it is a Redis fork
# whose banner reads e.g. "KeyDB server v=6.3.4 ... based on Redis"), so a *[Rr]edis*
# match first would mislabel it as plain redis; the keydb arm classifies it correctly and
# routes the boot to the --server-threads branch. Memcached is a NON-RESP server measured
# for MEMORY ONLY (see its measure_server branch and the WHY note there).
case "${COMPETITOR_VERSION_RAW}" in
  *[Dd]ragonfly*)   COMPETITOR_KIND="dragonfly"; COMPETITOR_NAME="dragonfly" ;;
  *[Kk]ey[Dd][Bb]*) COMPETITOR_KIND="keydb";     COMPETITOR_NAME="keydb" ;;
  *[Mm]emcached*)   COMPETITOR_KIND="memcached"; COMPETITOR_NAME="memcached" ;;
  *[Vv]alkey*)      COMPETITOR_KIND="valkey";    COMPETITOR_NAME="valkey" ;;
  *[Rr]edis*)       COMPETITOR_KIND="redis";     COMPETITOR_NAME="redis"  ;;
  *)
    # Fall back to the binary name when the banner is unrecognized.
    case "${COMPETITOR_BASENAME}" in
      *dragonfly*)   COMPETITOR_KIND="dragonfly"; COMPETITOR_NAME="dragonfly" ;;
      *keydb*)       COMPETITOR_KIND="keydb";     COMPETITOR_NAME="keydb" ;;
      *memcached*)   COMPETITOR_KIND="memcached"; COMPETITOR_NAME="memcached" ;;
      *valkey*)      COMPETITOR_KIND="valkey";    COMPETITOR_NAME="valkey" ;;
      *redis*)       COMPETITOR_KIND="redis";     COMPETITOR_NAME="redis"  ;;
    esac
    ;;
esac

# Extract a dotted version (e.g. 9.1.0 / 7.2.1) from the banner for the report.
COMPETITOR_VERSION="$(printf '%s\n' "${COMPETITOR_VERSION_RAW}" \
  | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)"
[[ -n "${COMPETITOR_VERSION}" ]] || COMPETITOR_VERSION="unknown"

# STAND-IN flag: true when the verdict is indicative (not the pinned valkey bar). Set
# when the competitor is a redis-server, OR a valkey-server whose version != the pin.
STANDIN=0
echo "[h2h] competitor: ${COMPETITOR_BIN} (${COMPETITOR_NAME} ${COMPETITOR_VERSION})"
if [[ "${COMPETITOR_KIND}" == "redis" ]]; then
  STANDIN=1
  echo "[h2h] WARNING: redis-server is a STAND-IN competitor (RESP/Valkey-wire-compatible)."
  echo "[h2h] WARNING: the PUBLISHED bar is the pinned valkey-server ${PINNED_VALKEY_VERSION}"
  echo "[h2h] WARNING: (docs/bench/COMPETITORS.md). This verdict is INDICATIVE until run vs valkey."
elif [[ "${COMPETITOR_KIND}" == "dragonfly" ]]; then
  STANDIN=1
  echo "[h2h] NOTE: competitor is DragonflyDB (a thread-per-core io_uring cache, the"
  echo "[h2h] NOTE: hardest RESP competitor; docs/research/dragonfly.md). It is a legitimate"
  echo "[h2h] NOTE: head-to-head, but the PUBLISHED bar is the pinned valkey-server"
  echo "[h2h] NOTE: ${PINNED_VALKEY_VERSION}, so this verdict is INDICATIVE (and a GitHub runner is"
  echo "[h2h] NOTE: a small shared VM - Dragonfly's multi-core design needs real cores to shine)."
elif [[ "${COMPETITOR_KIND}" == "keydb" ]]; then
  STANDIN=1
  echo "[h2h] NOTE: competitor is KeyDB (a multi-threaded Redis fork; speaks RESP and is"
  echo "[h2h] NOTE: redis-cli compatible, so the same PING/INFO/--pipe + RESP-loadgen path"
  echo "[h2h] NOTE: drives it - only its threading flag differs: --server-threads, not"
  echo "[h2h] NOTE: --io-threads). A legitimate full head-to-head, but the PUBLISHED bar is the"
  echo "[h2h] NOTE: pinned valkey-server ${PINNED_VALKEY_VERSION}, so this verdict is INDICATIVE."
elif [[ "${COMPETITOR_KIND}" == "memcached" ]]; then
  STANDIN=1
  echo "[h2h] NOTE: competitor is Memcached, which does NOT speak RESP. redis-cli"
  echo "[h2h] NOTE: (PING/INFO/--pipe) and the RESP loadgen CANNOT drive it, so a RESP-contract"
  echo "[h2h] NOTE: THROUGHPUT/LATENCY comparison is apples-to-oranges and is OUT OF SCOPE. This"
  echo "[h2h] NOTE: run is MEMORY-ONLY: it populates KEYCOUNT keys over the memcached TEXT"
  echo "[h2h] NOTE: protocol and reports bytes-per-key from 'stats' bytes; qps/p50/p99 are"
  echo "[h2h] NOTE: emitted as 0 (NOT measured). Verdict is INDICATIVE."
elif [[ "${COMPETITOR_KIND}" == "valkey" ]]; then
  if [[ "${COMPETITOR_VERSION}" != "${PINNED_VALKEY_VERSION}" ]]; then
    STANDIN=1
    echo "[h2h] WARNING: valkey-server version ${COMPETITOR_VERSION} != pinned ${PINNED_VALKEY_VERSION}"
    echo "[h2h] WARNING: (docs/bench/COMPETITORS.md). The published bar is the pinned version;"
    echo "[h2h] WARNING: this verdict is INDICATIVE until run vs the pinned valkey-server."
  fi
else
  echo "[h2h] WARNING: could not classify the competitor as valkey or redis; treating it as a"
  echo "[h2h] WARNING: stand-in. Verdict is INDICATIVE only."
  STANDIN=1
fi

# ---------------------------------------------------------------------------
# CPU-count helper (nproc on Linux, sysctl on macOS, fallback 1).
# ---------------------------------------------------------------------------
cpu_count() {
  if command -v nproc >/dev/null 2>&1; then
    nproc
  elif command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.ncpu 2>/dev/null || echo 1
  else
    echo 1
  fi
}
NCPU="$(cpu_count)"

# Count the cores in a taskset spec like "0-3", "0,2,4", or "0-3,8-11". This count is
# the PER-CORE denominator (qps_per_core) AND the IronCache --shards / valkey --io-threads
# value, so each server uses exactly the cores it is measured on.
count_cores() {
  local spec="$1" total=0 part lo hi
  local IFS=','
  for part in ${spec}; do
    if [[ "${part}" == *-* ]]; then
      lo="${part%%-*}"
      hi="${part##*-}"
      total=$(( total + (hi - lo) + 1 ))
    else
      total=$(( total + 1 ))
    fi
  done
  echo "${total}"
}

# ---------------------------------------------------------------------------
# PINNING (mirror run.sh). If taskset exists (Linux), pin BOTH servers to the SAME
# server core set (so each is measured on identical cores) and the loadgen client to a
# DISJOINT set. Configurable via SERVER_CORES / CLIENT_CORES; defaults split the box in
# half. If taskset is absent (macOS dev box) WARN unpinned/indicative and run anyway.
# ---------------------------------------------------------------------------
PINNED=0
PIN_SERVER_CORES=""
PIN_CLIENT_CORES=""
SERVER_PREFIX=()
CLIENT_PREFIX=()
SERVER_CORE_COUNT="${NCPU}"
if command -v taskset >/dev/null 2>&1; then
  half=$(( NCPU / 2 ))
  if [[ "${half}" -lt 1 ]]; then half=1; fi
  default_server="0-$(( half - 1 ))"
  default_client="${half}-$(( NCPU - 1 ))"
  if [[ "${NCPU}" -le 1 ]]; then
    default_server="0"
    default_client="0"
  fi
  PIN_SERVER_CORES="${SERVER_CORES:-${default_server}}"
  PIN_CLIENT_CORES="${CLIENT_CORES:-${default_client}}"
  SERVER_PREFIX=(taskset -c "${PIN_SERVER_CORES}")
  CLIENT_PREFIX=(taskset -c "${PIN_CLIENT_CORES}")
  PINNED=1
  SERVER_CORE_COUNT="$(count_cores "${PIN_SERVER_CORES}")"
  [[ "${SERVER_CORE_COUNT}" -ge 1 ]] || SERVER_CORE_COUNT=1
  echo "[h2h] taskset found: BOTH servers->cores ${PIN_SERVER_CORES} (${SERVER_CORE_COUNT} cores), client->cores ${PIN_CLIENT_CORES} (loopback ${HOST})."
else
  echo "[h2h] WARNING: taskset not found (likely macOS). Running UNPINNED."
  echo "[h2h] WARNING: results are INDICATIVE only; a publishable run needs disjoint server/client core pinning on Linux."
  echo "[h2h] WARNING: per-core denominator falls back to the host cpu count (${NCPU})."
fi

# ---------------------------------------------------------------------------
# Shared server lifecycle. ONE server runs at a time; the trap kills whichever is
# current on EXIT / INT / TERM so no orphan ever survives. SERVER_PID is updated as we
# switch servers; SERVER_LOG points at the current server's log.
# ---------------------------------------------------------------------------
SERVER_PID=""
SERVER_LOG=""
# SNAPSHOT mode state: the background BGSAVE loop's pid and the current server's private
# snapshot dir. Both are cleaned by the EXIT trap so an abort mid-pass never leaks the loop
# or the temp dir. Empty when SNAPSHOT is off.
SNAP_PID=""
SNAP_DIR=""

cleanup() {
  # Kill the background BGSAVE loop FIRST (before the server) so no stray BGSAVE races the kill.
  if [[ -n "${SNAP_PID}" ]] && kill -0 "${SNAP_PID}" 2>/dev/null; then
    kill "${SNAP_PID}" 2>/dev/null || true
    wait "${SNAP_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.2
    done
    kill -9 "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  # Best-effort remove the current server's private snapshot dir (SNAPSHOT mode).
  [[ -n "${SNAP_DIR}" && -d "${SNAP_DIR}" ]] && rm -rf "${SNAP_DIR}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# port_free: true when nothing is listening on HOST:PORT. A plain /dev/tcp connect
# detects a listener that a SO_REUSEPORT bind would otherwise hide (a stale server would
# silently co-reside and split the loadgen's connections, mixing two servers' numbers).
port_free() {
  if (exec 3<>"/dev/tcp/${HOST}/${PORT}") 2>/dev/null; then
    exec 3>&- 3<&- 2>/dev/null || true
    return 1
  fi
  return 0
}

# server_ready: PING the server (bounded by the caller's loop). PONG == ready.
server_ready() {
  [[ "$(redis-cli -h "${HOST}" -p "${PORT}" PING 2>/dev/null)" == "PONG" ]]
}

# wait_ready PID LABEL: poll readiness up to ~10s; fail (and dump the log) on timeout or
# if the process exits early.
wait_ready() {
  local pid="$1" label="$2" ready=0
  for _ in $(seq 1 40); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "error: ${label} exited during startup. Log:" >&2
      cat "${SERVER_LOG}" >&2 || true
      exit 1
    fi
    if server_ready; then ready=1; break; fi
    sleep 0.25
  done
  if [[ "${ready}" -ne 1 ]]; then
    echo "error: ${label} did not become ready on ${HOST}:${PORT} within ~10s. Log:" >&2
    cat "${SERVER_LOG}" >&2 || true
    exit 1
  fi
}

# stop_server: kill the current server, wait for it to die, and verify the port frees.
# Resets SERVER_PID so the EXIT trap does not double-kill a reused PID.
stop_server() {
  local label="$1"
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.2
    done
    kill -9 "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
  # Verify the port is free again (bounded), so the next server gets a clean bind.
  local freed=0
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if port_free; then freed=1; break; fi
    sleep 0.2
  done
  if [[ "${freed}" -ne 1 ]]; then
    echo "error: ${label} stopped but ${HOST}:${PORT} did not free within ~2s." >&2
    exit 1
  fi
  echo "[h2h] ${label} stopped; port ${PORT} free."
}

# ---------------------------------------------------------------------------
# read_used_memory: echo the `used_memory:` integer from INFO memory. Works identically
# on IronCache, Valkey, and Redis (all report `used_memory:<bytes>`). The CR is stripped
# (redis INFO lines are CRLF-terminated).
# ---------------------------------------------------------------------------
read_used_memory() {
  local v
  v="$(redis-cli -h "${HOST}" -p "${PORT}" INFO memory 2>/dev/null \
    | awk -F: '/^used_memory:/ { gsub(/\r/, "", $2); print $2; exit }')"
  [[ -n "${v}" ]] || v="0"
  echo "${v}"
}

# ---------------------------------------------------------------------------
# populate_keys N: deterministically insert EXACTLY N distinct keys k:0..k:N-1, each
# with a VALUE_SIZE-byte value, via `redis-cli --pipe` (fast, RESP, works on IronCache
# too since it supports ECHO for the pipe sentinel). The loadgen is deliberately NOT used
# here: its zipf SETs do not cover the keyspace uniformly, so they would not land N
# distinct keys. We emit inline RESP commands (redis-cli --pipe accepts them) generated
# by awk: a fixed VALUE_SIZE-byte value of 'x', one SET per key.
#
# The key encoding MUST match the loadgen's `Workload::key_bytes` (`k:<idx>`,
# crates/ironcache-bench/src/workload.rs) so the throughput pass operates on the SAME
# resident keyspace it populated: the 90% GETs HIT (the real YCSB workload, not all-miss)
# and an eviction-mode run evicts/re-inserts the SAME keys the loadgen reads. A mismatch
# (`key:<idx>` here vs `k:<idx>` in the loadgen) makes every GET a guaranteed MISS.
# ---------------------------------------------------------------------------
populate_keys() {
  local n="$1"
  # redis-cli --pipe ends with an ECHO sentinel handshake to detect completion. Dragonfly's
  # strict RESP parser ERRORS on that sentinel ("ERR unknown command `*2`/`$4`/...") and
  # makes redis-cli --pipe exit NON-ZERO even though all N SETs landed (redis/valkey tolerate
  # the sentinel). So the pipe's exit code is NOT authoritative here - `|| true` keeps the
  # `set -e` shell from aborting on that benign sentinel error, and the DBSIZE check below is
  # the real verification that the populate landed.
  awk -v n="${n}" -v vsize="${VALUE_SIZE}" 'BEGIN {
    # Build the fixed-size value once.
    val = ""
    for (i = 0; i < vsize; i++) val = val "x"
    for (k = 0; k < n; k++) {
      key = "k:" k
      # RESP array: *3 SET <key> <val>. $<len> precedes each bulk string.
      printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(key), key, vsize, val
    }
  }' | redis-cli -h "${HOST}" -p "${PORT}" --pipe || true
  # VERIFY the populate landed (the bytes-per-key delta is only meaningful if all N distinct
  # keys are resident): the freshly-booted server started empty, so DBSIZE must now be >= n.
  # This fails loudly on a genuinely-broken populate while tolerating the sentinel quirk.
  local dbsize
  dbsize="$(redis-cli -h "${HOST}" -p "${PORT}" DBSIZE 2>/dev/null | tr -dc '0-9')"
  [[ -n "${dbsize}" ]] || dbsize=0
  # EVICTION mode: the populate INTENTIONALLY exceeds maxmemory, so keys evict DURING the
  # populate and DBSIZE < n is EXPECTED (the whole point of this mode). Tolerate it - just
  # note the resident count - instead of failing the run. The bytes-per-key figure is not
  # meaningful under eviction, but the run must complete so the throughput pass can run.
  if [[ "${EVICT}" == "1" ]]; then
    echo "[h2h] EVICTION mode: ${dbsize}/${n} keys resident after populate (eviction during fill is expected)."
    return 0
  fi
  if [[ "${dbsize}" -lt "${n}" ]]; then
    echo "error: populate landed only ${dbsize}/${n} keys on ${HOST}:${PORT} (DBSIZE verify)" >&2
    return 1
  fi
}

# ---------------------------------------------------------------------------
# loadgen launcher. Shared workload flags; per-pass flags appended by the caller. The
# client pinning prefix is prepended when taskset is present. The
# `${arr[@]+"${arr[@]}"}` form expands to nothing when the array is empty (portable
# empty-array expansion under set -u; Apple's stock bash 3.2 errors on a bare
# "${arr[@]}" when empty).
# ---------------------------------------------------------------------------
run_loadgen() {
  ${CLIENT_PREFIX[@]+"${CLIENT_PREFIX[@]}"} "${LOADGEN_BIN}" \
    --host "${HOST}" --port "${PORT}" \
    --seed "${SEED}" \
    --keyspace "${KEYSPACE}" \
    --theta "${THETA}" \
    --value-size "${VALUE_SIZE}" \
    "$@"
}

# ---------------------------------------------------------------------------
# json_field FILE KEY: scalar value of "KEY" from a flat JSON object (jq if present, a
# sed fallback otherwise). Used to parse the loadgen result JSON.
# ---------------------------------------------------------------------------
json_field() {
  local file="$1" key="$2"
  if command -v jq >/dev/null 2>&1; then
    jq -r ".${key}" "${file}" 2>/dev/null
  else
    sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\"\\{0,1\\}\\([^,\"}]*\\)\"\\{0,1\\}.*/\\1/p" "${file}" | head -n1
  fi
}

# ratio_div A B: A / B to 4 dp via awk; "0" when B is 0/empty. Used for per-core and
# the cross-server ratios.
ratio_div() {
  awk -v a="$1" -v b="$2" 'BEGIN { if (b == 0 || b == "") { print "0" } else { printf "%.4f", a / b } }'
}

# ---------------------------------------------------------------------------
# CONCURRENT-SNAPSHOT helpers (SNAPSHOT mode). The loop fires BGSAVE on the server under test
# DURING the open-loop latency pass so the measured tail captures the save; verify_snapshot_fired
# then PROVES a save actually executed (not merely that BGSAVE was accepted).
# ---------------------------------------------------------------------------

# snap_lastsave: the server's LASTSAVE as a bare integer (Unix seconds of the last completed
# save), "0" when unavailable. Works on every RESP server (redis/valkey/keydb/dragonfly/ironcache).
snap_lastsave() {
  local v
  v="$(redis-cli -h "${HOST}" -p "${PORT}" LASTSAVE 2>/dev/null | tr -dc '0-9')"
  [[ -n "${v}" ]] || v="0"
  echo "${v}"
}

# start_snapshot_loop NAME: launch the background BGSAVE loop against the server under test.
# Fires ONCE immediately (so even a sub-second SMOKE window captures >=1 save) then every
# SNAPSHOT_INTERVAL_SECS. Each fire's redis-cli reply is appended to the per-server bgsave log so
# the caller can count fires and show a reply. Sets SNAP_PID; stop_snapshot_loop tears it down.
SNAP_BGSAVE_LOG=""
start_snapshot_loop() {
  local name="$1"
  SNAP_BGSAVE_LOG="${OUT_DIR}/${name}-bgsave.log"
  : >"${SNAP_BGSAVE_LOG}"
  (
    # This is a best-effort background loop: disable errexit/pipefail INSIDE the subshell so a
    # benign BGSAVE error (e.g. "ERR Background save already in progress" when a prior save is
    # still running, or a connection error as the server is being stopped) is just RECORDED and
    # never aborts the loop early. The save still proceeds; stop_snapshot_loop tears it down.
    set +e +o pipefail
    while true; do
      reply="$(redis-cli -h "${HOST}" -p "${PORT}" BGSAVE 2>&1 | tr -d '\r' | tr '\n' ' ')"
      printf '%s\t%s\n' "$(date -u +%H:%M:%S)" "${reply}" >>"${SNAP_BGSAVE_LOG}"
      sleep "${SNAPSHOT_INTERVAL_SECS}"
    done
  ) &
  SNAP_PID=$!
}

# stop_snapshot_loop: kill the background BGSAVE loop and reap it. Idempotent.
stop_snapshot_loop() {
  if [[ -n "${SNAP_PID}" ]] && kill -0 "${SNAP_PID}" 2>/dev/null; then
    kill "${SNAP_PID}" 2>/dev/null || true
    wait "${SNAP_PID}" 2>/dev/null || true
  fi
  SNAP_PID=""
}

# verify_snapshot_fired NAME LASTSAVE_BEFORE: PROVE at least one BGSAVE EXECUTED during the pass
# (not just that the command was accepted). Two independent, time-robust signals:
#   (1) LASTSAVE advanced beyond the pre-pass baseline (polled up to ~10s for the async save to
#       land). The open pass runs many wall-seconds after boot, so a completed save lands in a
#       strictly later Unix second and LASTSAVE advances - even in SMOKE.
#   (2) the server log contains a save-completion line (kind-specific pattern).
# Prints a confirmation line if EITHER holds (both are reported); prints a loud WARNING otherwise.
# Never aborts the run (the tail is still reported), but the confirmation/WARNING is explicit so a
# SMOKE self-test can assert the save fired.
verify_snapshot_fired() {
  local name="$1" before="$2" kind="$3"
  local fires after advanced=0 loghits=0 i
  fires="$(wc -l <"${SNAP_BGSAVE_LOG}" 2>/dev/null | tr -dc '0-9')"
  [[ -n "${fires}" ]] || fires=0
  # (1) Poll LASTSAVE for an advance (the last BGSAVE may still be completing after the loop dies).
  after="${before}"
  for i in $(seq 1 50); do
    after="$(snap_lastsave)"
    if [[ "${after}" -gt "${before}" ]]; then advanced=1; break; fi
    sleep 0.2
  done
  # (2) Grep the server log for a save-completion line (best-effort; kind-specific patterns).
  local pat=''
  case "${kind}" in
    redis|valkey|keydb) pat='[Bb]ackground saving (started|terminated)|DB saved on disk|RDB.*saved' ;;
    dragonfly)          pat='[Ss]aving|[Ss]aved.*to|snapshot|DFS?.*save' ;;
    ironcache)          pat='[Ss]ave|[Ss]napshot|__ICSAVE|dump' ;;
    *)                  pat='[Ss]ave|[Ss]napshot' ;;
  esac
  if [[ -n "${SERVER_LOG}" && -f "${SERVER_LOG}" ]]; then
    loghits="$(grep -Ec "${pat}" "${SERVER_LOG}" 2>/dev/null || true)"
    [[ -n "${loghits}" ]] || loghits=0
  fi
  if [[ "${advanced}" -eq 1 || "${loghits}" -gt 0 ]]; then
    echo "[h2h] ${name}: SNAPSHOT CONFIRMED FIRED: BGSAVE issued ${fires}x during the pass; LASTSAVE ${before} -> ${after} (advanced=${advanced}); server-log save lines=${loghits}."
  else
    echo "[h2h] WARNING: ${name}: SNAPSHOT could NOT be confirmed: BGSAVE issued ${fires}x but LASTSAVE did not advance (${before} -> ${after}) and no save line in the log. The p99.9 tail may NOT reflect a concurrent save; check that this server persists on BGSAVE."
  fi
}

# ---------------------------------------------------------------------------
# measure_server: boot one server (pinned, persistence off, eviction off), measure
# bytes-per-key + peak QPS + an optional open-loop latency pass, then stop it cleanly.
# Args:
#   $1 = logical name ("ironcache" | competitor name)
#   $2 = kind ("ironcache" | "valkey" | "redis" | "keydb" | "dragonfly" | "memcached" |
#        "unknown")
# Writes the per-metric results to the global out-vars RES_QPS / RES_QPS_PER_CORE /
# RES_BYTES_PER_KEY / RES_P50 / RES_P99 / RES_P999 / RES_P9999 / RES_PROTOCOL (read by the caller
# before the next server). The p50/p99/p999/p9999 are the loadgen's OVERALL open-loop op latency
# (microseconds): the loadgen records GET and SET into ONE hdrhistogram (crates/ironcache-bench/
# src/open_loop.rs), so there is NO GET-vs-SET percentile split - these are whole-op-mix tails.
# RES_PROTOCOL is "resp" for every RESP server and "memcached-text" for the memory-only memcached
# path, so the JSON marks memcached as memory-only / non-RESP.
# ---------------------------------------------------------------------------
RES_QPS=""
RES_QPS_PER_CORE=""
RES_BYTES_PER_KEY=""
RES_P50=""
RES_P99=""
RES_P999=""
RES_P9999=""
RES_PROTOCOL=""

measure_server() {
  local name="$1" kind="$2"
  SERVER_LOG="${OUT_DIR}/${name}-server.log"

  echo
  echo "[h2h] ===== measuring ${name} ====="

  # (a) PRE-LAUNCH port-free check.
  if ! port_free; then
    echo "error: ${HOST}:${PORT} is already in use before starting ${name}. Free it or set PORT=." >&2
    exit 1
  fi

  # SNAPSHOT mode: give THIS server a fresh, private, EMPTY snapshot dir so (a) nothing stale is
  # loaded on boot (the bytes-per-key baseline stays an empty server) and (b) the BGSAVE loop has a
  # real place to persist. Removed after the server stops (RESP path + the EXIT trap). Not used for
  # memcached (non-RESP, no BGSAVE). Reset to "" first so a prior server's dir is not reused.
  SNAP_DIR=""
  if [[ "${SNAPSHOT}" == "1" && "${kind}" != "memcached" ]]; then
    SNAP_DIR="$(mktemp -d "${OUT_DIR}/snapdir-${name}.XXXXXX")"
    echo "[h2h] ${name}: SNAPSHOT dir ${SNAP_DIR} (fresh, private; BGSAVE persists here)."
  fi

  # ---------------------------------------------------------------------------
  # MEMCACHED: a NON-RESP, MEMORY-ONLY divergent path that boots/populates/measures over
  # the memcached TEXT protocol and RETURNS before the RESP throughput/latency passes.
  #
  # WHY a separate path (and why memory-only): memcached does NOT speak RESP, so redis-cli
  # (PING/INFO/--pipe) and the in-repo RESP loadgen CANNOT drive it. A RESP-contract
  # throughput/latency comparison would be apples-to-oranges, so it is OUT OF SCOPE here;
  # we compare ONLY bytes-per-key, the one metric we can measure honestly on both. The
  # readiness probe, populate, and memory read all use the memcached text protocol over
  # `nc` instead of redis-cli; the RESP helpers (server_ready/populate_keys/run_loadgen/
  # read_used_memory) are bypassed entirely so the RESP path stays unchanged.
  # ---------------------------------------------------------------------------
  if [[ "${kind}" == "memcached" ]]; then
    # memcached -m is a MEGABYTE item-memory cap. Parse MAXMEMORY (e.g. 4gb / 512mb / a bare
    # byte count) into whole MB. The non-evict standard run wants a GENEROUS cap so the
    # populate never hits the slab ceiling; floor at 64 MB. EVICTION mode is not modeled for
    # memcached here (its LRU is slab-class-local and not comparable to a RESP allkeys-lru);
    # the cap is still applied, but bytes-per-key under eviction is not meaningful (same
    # caveat the RESP path already documents).
    local mc_mb
    mc_mb="$(awk -v v="${MAXMEMORY}" 'BEGIN {
      s = tolower(v); mult = 1
      if (s ~ /kb?$/) { mult = 1024 } else if (s ~ /mb?$/) { mult = 1024*1024 }
      else if (s ~ /gb?$/) { mult = 1024*1024*1024 } else { mult = 1 }
      gsub(/[^0-9.]/, "", s)
      if (s == "") s = 0
      mb = (s * mult) / (1024*1024)
      mb = int(mb)
      if (mb < 64) mb = 64
      print mb
    }')"
    echo "[h2h] starting ${name} on ${HOST}:${PORT} (threads=${SERVER_CORE_COUNT}, -m ${mc_mb} MB, text protocol)..."
    # FOREGROUND (no -d) backgrounded by the script (& + SERVER_PID), matching how every
    # other server is launched and killed by the shared trap/stop_server. taskset pins it to
    # the same server cores as the RESP servers. -t is the worker-thread count (the
    # --io-threads/--server-threads analog).
    ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${COMPETITOR_BIN}" \
      -p "${PORT}" -l "${HOST}" -t "${SERVER_CORE_COUNT}" -m "${mc_mb}" \
      >"${SERVER_LOG}" 2>&1 &
    SERVER_PID=$!

    # Readiness: memcached has NO PING/PONG. Probe the text-protocol `version` command and
    # look for the `VERSION` reply line. Bounded ~10s, mirroring wait_ready, and fail loudly
    # (dumping the log) on a startup crash or timeout.
    local mc_ready=0 _i
    for _i in $(seq 1 40); do
      if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "error: ${name} exited during startup. Log:" >&2
        cat "${SERVER_LOG}" >&2 || true
        exit 1
      fi
      if printf 'version\r\n' | nc -q1 "${HOST}" "${PORT}" 2>/dev/null | grep -q '^VERSION'; then
        mc_ready=1; break
      fi
      sleep 0.25
    done
    if [[ "${mc_ready}" -ne 1 ]]; then
      echo "error: ${name} did not answer 'version' on ${HOST}:${PORT} within ~10s. Log:" >&2
      cat "${SERVER_LOG}" >&2 || true
      exit 1
    fi
    echo "[h2h] ${name} ready (pid ${SERVER_PID})."

    # bytes-per-key: read `stats` `bytes` on the EMPTY server (before), populate EXACTLY
    # KEYCOUNT keys k:0..k:N-1 each with a VALUE_SIZE-byte value over the text protocol, then
    # re-read `bytes` (after). bytes_per_key = (after - before) / KEYCOUNT.
    #
    # The key encoding MUST stay `k:<n>` to match the loadgen's Workload::key_bytes and the
    # RESP populate (a prior `key:<n>` mismatch was a benchmark bug); the loadgen never runs
    # against memcached, but keeping the SAME encoding makes the bytes-per-key number directly
    # comparable to the RESP servers' (same key strings, same value size).
    #
    # `stats` `bytes` is the count of item bytes CURRENTLY stored (key + value + memcached's
    # per-item overhead, accounted within slabs); it is the closest analog to redis
    # used_memory for a memory comparison. It is NOT process RSS, so it excludes the empty
    # slab pages memcached preallocates - a fair item-level memory figure.
    echo "[h2h] ${name}: measuring bytes-per-key over ${KEYCOUNT} keys (value_size=${VALUE_SIZE}, memcached text protocol)..."
    local mc_before mc_after mc_bpk
    mc_before="$(printf 'stats\r\nquit\r\n' | nc -q1 "${HOST}" "${PORT}" 2>/dev/null \
      | awk '/^STAT bytes / { gsub(/\r/, "", $3); print $3; exit }')"
    [[ -n "${mc_before}" ]] || mc_before="0"

    # Emit one `set` per key into a SINGLE nc connection (the text-protocol analog of the
    # awk|redis-cli --pipe populate). `set <key> <flags> <exptime> <bytes>\r\n<data>\r\n`;
    # exptime 0 = never expire. memcached replies `STORED` per key; we count them to VERIFY
    # the populate landed (the bytes-per-key delta is only meaningful if all N keys stored).
    local mc_stored
    mc_stored="$(awk -v n="${KEYCOUNT}" -v vsize="${VALUE_SIZE}" 'BEGIN {
      val = ""
      for (i = 0; i < vsize; i++) val = val "x"
      for (k = 0; k < n; k++) {
        printf "set k:%d 0 0 %d\r\n%s\r\n", k, vsize, val
      }
      printf "quit\r\n"
    }' | nc -q5 "${HOST}" "${PORT}" 2>/dev/null | grep -c '^STORED')"
    [[ -n "${mc_stored}" ]] || mc_stored="0"
    if [[ "${EVICT}" == "1" ]]; then
      echo "[h2h] EVICTION mode: ${mc_stored}/${KEYCOUNT} keys STORED (memcached slab LRU is not comparable to a RESP allkeys-lru; bytes-per-key is not meaningful here)."
    elif [[ "${mc_stored}" -lt "${KEYCOUNT}" ]]; then
      echo "error: memcached populate stored only ${mc_stored}/${KEYCOUNT} keys on ${HOST}:${PORT} (raise -m, currently ${mc_mb} MB)" >&2
      return 1
    fi

    mc_after="$(printf 'stats\r\nquit\r\n' | nc -q1 "${HOST}" "${PORT}" 2>/dev/null \
      | awk '/^STAT bytes / { gsub(/\r/, "", $3); print $3; exit }')"
    [[ -n "${mc_after}" ]] || mc_after="0"
    mc_bpk="$(awk -v a="${mc_after}" -v b="${mc_before}" -v n="${KEYCOUNT}" \
      'BEGIN { if (n == 0) { print "0" } else { printf "%.2f", (a - b) / n } }')"
    echo "[h2h] ${name}: stats bytes ${mc_before} -> ${mc_after}; bytes_per_key=${mc_bpk} (memory-only, non-RESP)"

    # SKIP the warmup/closed-loop/open-loop passes: there is no RESP loadgen for memcached.
    # qps/p50/p99 are reported as 0 (NOT measured) and RES_PROTOCOL marks this memory-only.
    stop_server "${name}"
    RES_QPS="0"
    RES_QPS_PER_CORE="0"
    RES_BYTES_PER_KEY="${mc_bpk}"
    RES_P50="0"
    RES_P99="0"
    RES_P999="0"
    RES_P9999="0"
    RES_PROTOCOL="memcached-text"
    return 0
  fi

  # Every path below here is a RESP server (redis/valkey/keydb/dragonfly/ironcache).
  RES_PROTOCOL="resp"

  # (b) Boot it, pinned, persistence OFF + eviction OFF for the measurement.
  if [[ "${kind}" == "ironcache" ]]; then
    # IRONCACHE_SHARDS lets a probe DECOUPLE the shard (= runtime-thread) count from the
    # pinned core count, to isolate thread-oversubscription effects (e.g. 1 shard vs 2
    # shards on the SAME 2 pinned cores). Defaults to one shard per pinned core (the
    # thread-per-core norm). The qps_per_core denominator stays SERVER_CORE_COUNT (cores),
    # so a probe compares RAW qps across shard counts.
    local ic_shards="${IRONCACHE_SHARDS:-${SERVER_CORE_COUNT}}"
    echo "[h2h] starting ${name} on ${HOST}:${PORT} (shards=${ic_shards} over ${SERVER_CORE_COUNT} pinned cores, maxmemory=${MAXMEMORY})..."
    if [[ "${EVICT}" == "1" ]]; then
      # No boot change needed: IronCache defaults to maxmemory-policy allkeys-lru, so under
      # the caller's low IRONCACHE_MAXMEMORY it EVICTS to fit. Just note that eviction is on.
      echo "[h2h] ${name}: EVICTION mode (default allkeys-lru under IRONCACHE_MAXMEMORY=${MAXMEMORY})."
    fi
    # Set IRONCACHE_SHARDS to the RESOLVED numeric value on the launch (not just the
    # --shards flag): the binary ALSO reads IRONCACHE_SHARDS from its env-config, and an
    # EMPTY inherited value (e.g. a blank workflow input flowing in as IRONCACHE_SHARDS="")
    # makes it fail startup with "invalid config value for shards: not a number". Pinning
    # it to ${ic_shards} here overrides any empty inherited env so the binary always gets a
    # valid count.
    # Env for the launch, built as an array so an optional IRONCACHE_DATA_DIR can be added ONLY in
    # SNAPSHOT mode (an EMPTY IRONCACHE_DATA_DIR would wrongly enable persistence into the cwd, so it
    # is added conditionally, never blank). IRONCACHE_DATA_DIR is the single enable switch for #58
    # persistence: with it set (to the fresh private SNAP_DIR) a BGSAVE performs a REAL forkless,
    # yielding (#571) cross-shard save; without it BGSAVE is the no-op persistence-disabled fallback.
    local ic_env=("IRONCACHE_MAXMEMORY=${MAXMEMORY}" "IRONCACHE_SHARDS=${ic_shards}")
    if [[ "${SNAPSHOT}" == "1" && -n "${SNAP_DIR}" ]]; then
      ic_env+=("IRONCACHE_DATA_DIR=${SNAP_DIR}")
      echo "[h2h] ${name}: SNAPSHOT persistence ON (IRONCACHE_DATA_DIR=${SNAP_DIR}; periodic save off, only the BGSAVE loop persists)."
      # INCREMENTAL DELTA SNAPSHOTS (#676): the A/B knob for the during-save p99.9 payoff. When
      # SNAPSHOT_DELTAS=1, IronCache writes a DELTA of only the keys mutated since the last save
      # (re-reading FEWER bytes on the persist thread) after the first base, so the during-save tail
      # should drop in proportion to the dirty fraction. Only meaningful under SNAPSHOT (there is no
      # save otherwise); default off = the base-every-save behavior (the 291ms read-bandwidth floor).
      if [[ "${SNAPSHOT_DELTAS:-0}" == "1" ]]; then
        ic_env+=("IRONCACHE_SNAPSHOT_DELTAS=true")
        echo "[h2h] ${name}: #676 incremental delta snapshots ON (IRONCACHE_SNAPSHOT_DELTAS=true; the during-save p99.9 A/B)."
      fi
    fi
    # DEDICATED PERSIST CORE (#589): if PERSIST_CORE is set, pin IronCache's off-datapath persist
    # thread to it via IRONCACHE_PERSIST_CPU so its encode stops stealing a serving core during a save.
    # The datapath is confined to PIN_SERVER_CORES by taskset (SERVER_PREFIX); the persist thread's
    # sched_setaffinity is bounded by the process cpuset (not the taskset mask), so it escapes onto
    # PERSIST_CORE even though it sits OUTSIDE the datapath set. Give the box one extra core:
    # SERVER_CORES=0-7 PERSIST_CORE=8. Only meaningful under SNAPSHOT (there is no persist thread
    # otherwise); a blank inherited value is ignored so persistence-off runs are byte-unchanged.
    if [[ -n "${PERSIST_CORE:-}" ]]; then
      ic_env+=("IRONCACHE_PERSIST_CPU=${PERSIST_CORE}")
      echo "[h2h] ${name}: dedicated persist core ON (IRONCACHE_PERSIST_CPU=${PERSIST_CORE}; persist thread escapes the datapath cores ${PIN_SERVER_CORES:-<unpinned>})."
    fi
    env "${ic_env[@]}" \
      ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${IRONCACHE_BIN}" \
      --port "${PORT}" --shards "${ic_shards}" server \
      >"${SERVER_LOG}" 2>&1 &
    SERVER_PID=$!
  elif [[ "${kind}" == "dragonfly" ]]; then
    # DragonflyDB: its own flags. `--proactor_threads N` is the thread-per-core knob
    # (the --io-threads analog), pinned to the same core set. Snapshots off via an empty
    # `--dbfilename`. A GENEROUS `--maxmemory` (not 0 - Dragonfly requires a positive
    # ceiling) so the populate never evicts, matching IronCache's overlay. Dragonfly uses
    # io_uring by default on a modern kernel (the runner is 6.x); no fallback flag needed.
    # EVICTION mode: Dragonfly only EVICTS in cache mode (its default REJECTS writes with
    # OOM once maxmemory is hit). `--cache_mode=true` turns on item eviction. Dragonfly uses
    # gflags, which require the `--flag=value` form: the space form `--cache_mode true` makes
    # gflags treat `true` as a stray positional arg and ABORT at boot. Injected only when
    # EVICT=1; the standard run leaves it off (the generous ceiling never evicts anyway).
    local df_cache_flag=()
    if [[ "${EVICT}" == "1" ]]; then
      df_cache_flag=(--cache_mode=true)
      echo "[h2h] ${name}: EVICTION mode (--cache_mode=true under maxmemory=${MAXMEMORY})."
      # IMPORTANT: Dragonfly REFUSES to boot unless maxmemory >= 256 MiB * proactor_threads
      # (dfly_main.cc: 'There are N threads, so X MiB are required. Exiting...'). So a fair
      # eviction h2h vs Dragonfly needs MAXMEMORY >= 256MiB*SERVER_CORE_COUNT (e.g. >= 512mb
      # for 2 cores) AND a dataset larger than that to force eviction. A low MAXMEMORY (the
      # natural choice for a cheap small-resident eviction test) boots IronCache/redis but
      # ABORTS Dragonfly; wait_ready then surfaces the Dragonfly log with the exact reason.
      echo "[h2h] NOTE: Dragonfly requires maxmemory >= 256MiB * ${SERVER_CORE_COUNT} threads to boot; set MAXMEMORY accordingly for an eviction h2h."
    fi
    # Snapshot file: OFF by default (empty --dbfilename, the never-persist measurement). Under
    # SNAPSHOT a REAL dbfilename + --dir into the fresh private dir so BGSAVE actually writes a
    # Dragonfly snapshot (and its snapshot-spike shows in the tail). gflags need the space form here.
    local df_snap_flag=(--dbfilename '')
    if [[ "${SNAPSHOT}" == "1" && -n "${SNAP_DIR}" ]]; then
      df_snap_flag=(--dbfilename dump --dir "${SNAP_DIR}")
      echo "[h2h] ${name}: SNAPSHOT persistence ON (--dbfilename dump --dir ${SNAP_DIR}; BGSAVE writes a Dragonfly snapshot)."
    fi
    echo "[h2h] starting ${name} on ${HOST}:${PORT} (proactor_threads=${SERVER_CORE_COUNT}, maxmemory=${MAXMEMORY}, snapshots off)..."
    ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${COMPETITOR_BIN}" \
      --port "${PORT}" --bind "${HOST}" --proactor_threads "${SERVER_CORE_COUNT}" \
      --maxmemory "${MAXMEMORY}" "${df_snap_flag[@]}" --primary_port_http_enabled=false \
      ${df_cache_flag[@]+"${df_cache_flag[@]}"} \
      >"${SERVER_LOG}" 2>&1 &
    SERVER_PID=$!
  elif [[ "${kind}" == "keydb" ]]; then
    # KeyDB: a multi-threaded Redis FORK. It is redis-cli/RESP/INFO/--pipe compatible, so
    # everything downstream (readiness PING, bytes-per-key, the RESP loadgen) is IDENTICAL to
    # the redis/valkey path. The ONE difference is the multi-threading flag: KeyDB uses
    # `--server-threads N` (its worker-thread count), NOT redis's `--io-threads N`. Same
    # persistence-off + eviction handling as redis/valkey: --save '' --appendonly no, and
    # --maxmemory 0 by default, or a LOW ceiling + allkeys-lru under EVICT=1. The maxmemory
    # args are built ONCE and reused in the initial + fallback launch so they stay in lockstep.
    local kdb_mem_args=(--maxmemory 0)
    local kdb_mem_desc="maxmemory off"
    if [[ "${EVICT}" == "1" ]]; then
      kdb_mem_args=(--maxmemory "${MAXMEMORY}" --maxmemory-policy allkeys-lru)
      kdb_mem_desc="maxmemory=${MAXMEMORY} policy=allkeys-lru (EVICTION mode)"
    fi
    # SNAPSHOT: point --dir at the fresh private dir so BGSAVE's RDB lands there (manual BGSAVE
    # still works under --save '', which only disables the AUTOMATIC change-based snapshot). Built
    # once and reused in the initial + fallback launch so they stay in lockstep.
    local kdb_snap_args=()
    if [[ "${SNAPSHOT}" == "1" && -n "${SNAP_DIR}" ]]; then
      kdb_snap_args=(--dir "${SNAP_DIR}")
      echo "[h2h] ${name}: SNAPSHOT persistence ON (--dir ${SNAP_DIR}; BGSAVE writes an RDB there despite --save '')."
    fi
    echo "[h2h] starting ${name} on ${HOST}:${PORT} (server-threads=${SERVER_CORE_COUNT}, ${kdb_mem_desc}, persistence off)..."
    ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${COMPETITOR_BIN}" \
      --port "${PORT}" --bind "${HOST}" --save '' --appendonly no \
      "${kdb_mem_args[@]}" ${kdb_snap_args[@]+"${kdb_snap_args[@]}"} --daemonize no --server-threads "${SERVER_CORE_COUNT}" \
      >"${SERVER_LOG}" 2>&1 &
    SERVER_PID=$!
    # Defensive fallback (mirrors redis/valkey): if KeyDB rejects --server-threads and exits
    # at boot, retry without it and note it.
    sleep 0.5
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "[h2h] NOTE: ${name} exited at boot (likely rejected --server-threads); retrying WITHOUT it."
      wait "${SERVER_PID}" 2>/dev/null || true
      SERVER_PID=""
      if ! port_free; then sleep 0.5; fi
      ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${COMPETITOR_BIN}" \
        --port "${PORT}" --bind "${HOST}" --save '' --appendonly no \
        "${kdb_mem_args[@]}" ${kdb_snap_args[@]+"${kdb_snap_args[@]}"} --daemonize no \
        >"${SERVER_LOG}" 2>&1 &
      SERVER_PID=$!
    fi
  else
    # Valkey / Redis. Persistence off (--save '' --appendonly no). EVICTION: off by default
    # (--maxmemory 0); when EVICT=1 a LOW ceiling + allkeys-lru so writes EVICT instead of
    # erroring. --io-threads lets the server use the pinned cores; if the build rejects the
    # flag we retry without it and note it. The maxmemory args are built ONCE here and reused
    # in both the initial and the fallback launch so the two stay in lockstep.
    local rv_mem_args=(--maxmemory 0)
    local rv_mem_desc="maxmemory off"
    if [[ "${EVICT}" == "1" ]]; then
      rv_mem_args=(--maxmemory "${MAXMEMORY}" --maxmemory-policy allkeys-lru)
      rv_mem_desc="maxmemory=${MAXMEMORY} policy=allkeys-lru (EVICTION mode)"
    fi
    # SNAPSHOT: point --dir at the fresh private dir so BGSAVE's RDB (a fork-COW save) lands there.
    # BGSAVE still works under --save '' (that only disables the AUTOMATIC change-based snapshot).
    # Built once and reused in the initial + fallback launch so they stay in lockstep.
    local rv_snap_args=()
    if [[ "${SNAPSHOT}" == "1" && -n "${SNAP_DIR}" ]]; then
      rv_snap_args=(--dir "${SNAP_DIR}")
      echo "[h2h] ${name}: SNAPSHOT persistence ON (--dir ${SNAP_DIR}; BGSAVE fork-COW writes an RDB there despite --save '')."
    fi
    echo "[h2h] starting ${name} on ${HOST}:${PORT} (io-threads=${SERVER_CORE_COUNT}, ${rv_mem_desc}, persistence off)..."
    ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${COMPETITOR_BIN}" \
      --port "${PORT}" --bind "${HOST}" --save '' --appendonly no \
      "${rv_mem_args[@]}" ${rv_snap_args[@]+"${rv_snap_args[@]}"} --daemonize no --io-threads "${SERVER_CORE_COUNT}" \
      >"${SERVER_LOG}" 2>&1 &
    SERVER_PID=$!
    # Detect an immediate exit caused by an unsupported flag (e.g. --io-threads) and
    # fall back without it.
    sleep 0.5
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "[h2h] NOTE: ${name} exited at boot (likely rejected --io-threads); retrying WITHOUT it."
      wait "${SERVER_PID}" 2>/dev/null || true
      SERVER_PID=""
      if ! port_free; then sleep 0.5; fi
      ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${COMPETITOR_BIN}" \
        --port "${PORT}" --bind "${HOST}" --save '' --appendonly no \
        "${rv_mem_args[@]}" ${rv_snap_args[@]+"${rv_snap_args[@]}"} --daemonize no \
        >"${SERVER_LOG}" 2>&1 &
      SERVER_PID=$!
    fi
  fi

  # (b cont.) readiness probe.
  wait_ready "${SERVER_PID}" "${name}"
  echo "[h2h] ${name} ready (pid ${SERVER_PID})."

  # (c) BYTES-PER-KEY: used_memory baseline -> populate EXACTLY KEYCOUNT keys -> delta/N.
  echo "[h2h] ${name}: measuring bytes-per-key over ${KEYCOUNT} keys (value_size=${VALUE_SIZE})..."
  local before after bytes_per_key
  before="$(read_used_memory)"
  populate_keys "${KEYCOUNT}"
  # EVICTION-mode honesty guard: the populate intentionally over-fills, so the server is now
  # AT its ceiling. A genuinely EVICTING server still ACCEPTS a write (it frees room first and
  # replies +OK); a server mis-configured to REJECT under pressure replies -OOM, which a
  # closed-loop client counts as a FAST completed op and reports as bogus extra throughput. So
  # probe one SET and require +OK: an OOM-rejecting server fails the run LOUDLY instead of
  # posting a dishonest, inflated eviction-mode QPS. (The probe key uses the k: namespace so it
  # does not perturb a later DBSIZE-by-prefix; it is one key, negligible to the measurement.)
  if [[ "${EVICT}" == "1" ]]; then
    local probe
    probe="$(redis-cli -h "${HOST}" -p "${PORT}" SET k:__evict_probe__ x 2>&1 | tr -d '[:space:]')"
    if [[ "${probe}" != "OK" ]]; then
      echo "error: ${name} did NOT accept a write under memory pressure (reply: '${probe}'): it is REJECTING, not evicting, so eviction-mode QPS would be dishonest. Check the eviction policy / --cache_mode flag." >&2
      return 1
    fi
    echo "[h2h] ${name}: eviction sanity OK (accepted a write at the memory ceiling)."
  fi
  after="$(read_used_memory)"
  bytes_per_key="$(awk -v a="${after}" -v b="${before}" -v n="${KEYCOUNT}" \
    'BEGIN { if (n == 0) { print "0" } else { printf "%.2f", (a - b) / n } }')"
  echo "[h2h] ${name}: used_memory ${before} -> ${after} bytes; bytes_per_key=${bytes_per_key}"

  # (d) PEAK QPS: warmup (write-only) then closed-loop measured pass. The bytes-per-key
  #     keys are still resident; the zipf workload reuses the keyspace.
  echo "[h2h] ${name}: warmup write-only ${WARMUP_SECS}s..."
  run_loadgen --mode closed --read-ratio 0 --duration-secs "${WARMUP_SECS}" \
    --connections "${CONNECTIONS}" --out - >/dev/null
  local closed_json="${OUT_DIR}/${name}-closed.json"
  echo "[h2h] ${name}: closed-loop peak QPS (${DURATION_SECS}s, ${CONNECTIONS} conns)..."
  run_loadgen --mode closed --read-ratio "${READ_RATIO}" --duration-secs "${DURATION_SECS}" \
    --connections "${CONNECTIONS}" --out "${closed_json}"
  local qps qps_per_core
  qps="$(json_field "${closed_json}" qps)"
  [[ -n "${qps}" ]] || qps="0"
  qps_per_core="$(ratio_div "${qps}" "${SERVER_CORE_COUNT}")"
  echo "[h2h] ${name}: qps=${qps} qps_per_core=${qps_per_core} (over ${SERVER_CORE_COUNT} cores)"

  # (e) Optional open-loop latency pass (p50/p99/p999/p9999). Under SNAPSHOT mode a background
  #     BGSAVE loop runs DURING this pass so the tail captures the concurrent durable save, then
  #     verify_snapshot_fired PROVES a save executed while the server is still up.
  local open_json="${OUT_DIR}/${name}-open.json"
  local open_hgrm="${OUT_DIR}/${name}-open.hgrm"
  local snap_lastsave_before=0
  if [[ "${SNAPSHOT}" == "1" ]]; then
    snap_lastsave_before="$(snap_lastsave)"   # baseline BEFORE any save fires this pass.
    echo "[h2h] ${name}: SNAPSHOT loop starting (BGSAVE every ${SNAPSHOT_INTERVAL_SECS}s, first fire immediate; LASTSAVE baseline=${snap_lastsave_before})."
    start_snapshot_loop "${name}"
  fi
  echo "[h2h] ${name}: open-loop latency @ ${RATE} ops/sec..."
  run_loadgen --mode open --read-ratio "${READ_RATIO}" --duration-secs "${DURATION_SECS}" \
    --connections "${CONNECTIONS}" --rate "${RATE}" \
    --out "${open_json}" --hist "${open_hgrm}"
  if [[ "${SNAPSHOT}" == "1" ]]; then
    stop_snapshot_loop
    verify_snapshot_fired "${name}" "${snap_lastsave_before}" "${kind}"
  fi
  local p50 p99 p999 p9999
  p50="$(json_field "${open_json}" p50_us)"
  p99="$(json_field "${open_json}" p99_us)"
  p999="$(json_field "${open_json}" p999_us)"
  p9999="$(json_field "${open_json}" p9999_us)"
  [[ -n "${p50}" ]] || p50="0"
  [[ -n "${p99}" ]] || p99="0"
  [[ -n "${p999}" ]] || p999="0"
  [[ -n "${p9999}" ]] || p9999="0"

  # (f) Stop the server cleanly and verify the port frees, then drop this server's private
  #     snapshot dir (SNAPSHOT mode).
  stop_server "${name}"
  if [[ -n "${SNAP_DIR}" && -d "${SNAP_DIR}" ]]; then
    rm -rf "${SNAP_DIR}" 2>/dev/null || true
    SNAP_DIR=""
  fi

  RES_QPS="${qps}"
  RES_QPS_PER_CORE="${qps_per_core}"
  RES_BYTES_PER_KEY="${bytes_per_key}"
  RES_P50="${p50}"
  RES_P99="${p99}"
  RES_P999="${p999}"
  RES_P9999="${p9999}"
}

# ---------------------------------------------------------------------------
# Measure IronCache first, then the competitor. ONE server runs at a time on the same
# port, under identical knobs.
# ---------------------------------------------------------------------------
measure_server "ironcache" "ironcache"
IC_QPS="${RES_QPS}"
IC_QPS_PER_CORE="${RES_QPS_PER_CORE}"
IC_BYTES_PER_KEY="${RES_BYTES_PER_KEY}"
IC_P50="${RES_P50}"
IC_P99="${RES_P99}"
IC_P999="${RES_P999}"
IC_P9999="${RES_P9999}"
IC_PROTOCOL="${RES_PROTOCOL}"

measure_server "${COMPETITOR_NAME}" "${COMPETITOR_KIND}"
CO_QPS="${RES_QPS}"
CO_QPS_PER_CORE="${RES_QPS_PER_CORE}"
CO_BYTES_PER_KEY="${RES_BYTES_PER_KEY}"
CO_P50="${RES_P50}"
CO_P99="${RES_P99}"
CO_P999="${RES_P999}"
CO_P9999="${RES_P9999}"
CO_PROTOCOL="${RES_PROTOCOL}"

# ---------------------------------------------------------------------------
# RATIOS + ADR-0017 VERDICT.
#   qps_per_core: IronCache PASSES when it EXCEEDS the competitor's (ratio > 1).
#   bytes_per_key: IronCache PASSES when it is BELOW the competitor's (ratio < 1).
#
# MEMORY-ONLY competitors (memcached): there is no comparable QPS (non-RESP, not measured),
# so the qps_per_core leg is N/A and the OVERALL verdict reduces to the bytes_per_key leg
# alone. The qps_per_core ratio is left as 0 (CO_QPS_PER_CORE is 0) and reported as "n/a".
# ---------------------------------------------------------------------------
MEMORY_ONLY=0
if [[ "${CO_PROTOCOL}" == "memcached-text" ]]; then MEMORY_ONLY=1; fi

QPS_RATIO="$(ratio_div "${IC_QPS_PER_CORE}" "${CO_QPS_PER_CORE}")"           # ic / competitor; >1 is good.
BYTES_RATIO="$(ratio_div "${IC_BYTES_PER_KEY}" "${CO_BYTES_PER_KEY}")"       # ic / competitor; <1 is good.
# The #518 MOAT ratio: the p99.9 (p999) tail, ic/competitor. <1 means IronCache's tail is TIGHTER
# than the competitor's - the metric to win under the adversarial mix (mixed + skew + evict + snapshot).
# This is REPORTED, not a pass/fail gate (the ADR-0017 verdict stays qps-per-core + bytes-per-key).
P999_RATIO="$(ratio_div "${IC_P999}" "${CO_P999}")"
P9999_RATIO="$(ratio_div "${IC_P9999}" "${CO_P9999}")"

BYTES_VERDICT="FAIL"
if awk -v a="${IC_BYTES_PER_KEY}" -v b="${CO_BYTES_PER_KEY}" 'BEGIN { exit !(a < b) }'; then
  BYTES_VERDICT="PASS"
fi

if [[ "${MEMORY_ONLY}" -eq 1 ]]; then
  # memory-only: no throughput leg; OVERALL == the bytes leg.
  QPS_VERDICT="N/A"
  OVERALL="${BYTES_VERDICT}"
else
  QPS_VERDICT="FAIL"
  if awk -v a="${IC_QPS_PER_CORE}" -v b="${CO_QPS_PER_CORE}" 'BEGIN { exit !(a > b) }'; then
    QPS_VERDICT="PASS"
  fi
  OVERALL="FAIL"
  if [[ "${QPS_VERDICT}" == "PASS" && "${BYTES_VERDICT}" == "PASS" ]]; then
    OVERALL="PASS"
  fi
fi

# ---------------------------------------------------------------------------
# MANIFEST + comparison artifact: headtohead.json (manifest style mirrors run.sh).
# ---------------------------------------------------------------------------
TIMESTAMP_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
UNAME_ALL="$(uname -a)"
UNAME_ESC="${UNAME_ALL//\\/\\\\}"
UNAME_ESC="${UNAME_ESC//\"/\\\"}"
COMPETITOR_BIN_ESC="${COMPETITOR_BIN//\\/\\\\}"
COMPETITOR_BIN_ESC="${COMPETITOR_BIN_ESC//\"/\\\"}"

if [[ "${PINNED}" -eq 1 ]]; then PINNED_BOOL="true"; else PINNED_BOOL="false"; fi
if [[ "${SMOKE}" == "1" ]]; then SMOKE_BOOL="true"; else SMOKE_BOOL="false"; fi
if [[ "${STANDIN}" -eq 1 ]]; then STANDIN_BOOL="true"; else STANDIN_BOOL="false"; fi
if [[ "${EVICT}" == "1" ]]; then EVICT_BOOL="true"; else EVICT_BOOL="false"; fi
if [[ "${SNAPSHOT}" == "1" ]]; then SNAPSHOT_BOOL="true"; else SNAPSHOT_BOOL="false"; fi
# indicative_only is the CONSERVATIVE headline flag: the verdict is non-authoritative
# if the competitor was a stand-in / version mismatch (STANDIN), OR the run was SMOKE
# (tiny, not publishable), OR it was UNPINNED (no disjoint server/client cores). A
# consumer can gate on this single field; the orthogonal `standin`, top-level `smoke`,
# and `pinning.pinned` fields remain for the specific reason.
if [[ "${STANDIN}" -eq 1 || "${SMOKE}" == "1" || "${PINNED}" -ne 1 ]]; then
  INDICATIVE_BOOL="true"
else
  INDICATIVE_BOOL="false"
fi
QPS_PASS_BOOL="false"; [[ "${QPS_VERDICT}" == "PASS" ]] && QPS_PASS_BOOL="true"
BYTES_PASS_BOOL="false"; [[ "${BYTES_VERDICT}" == "PASS" ]] && BYTES_PASS_BOOL="true"
OVERALL_BOOL="false"; [[ "${OVERALL}" == "PASS" ]] && OVERALL_BOOL="true"
MEMORY_ONLY_BOOL="false"; [[ "${MEMORY_ONLY}" -eq 1 ]] && MEMORY_ONLY_BOOL="true"
# Per-server wire protocol, so the JSON marks a memory-only / non-RESP competitor.
IC_PROTOCOL="${IC_PROTOCOL:-resp}"
CO_PROTOCOL="${CO_PROTOCOL:-resp}"

H2H_JSON="${OUT_DIR}/headtohead.json"
cat >"${H2H_JSON}" <<EOF
{
  "schema": "ironcache-headtohead/1",
  "timestamp_utc": "${TIMESTAMP_UTC}",
  "smoke": ${SMOKE_BOOL},
  "host": {
    "uname": "${UNAME_ESC}",
    "os": "${OS_NAME}",
    "arch": "${ARCH_NAME}",
    "cpu_count": ${NCPU}
  },
  "pinning": {
    "pinned": ${PINNED_BOOL},
    "server_cores": "${PIN_SERVER_CORES}",
    "client_cores": "${PIN_CLIENT_CORES}",
    "server_core_count": ${SERVER_CORE_COUNT},
    "host_addr": "${HOST}"
  },
  "knobs": {
    "seed": ${SEED},
    "keyspace": ${KEYSPACE},
    "keycount": ${KEYCOUNT},
    "theta": ${THETA},
    "read_ratio": ${READ_RATIO},
    "value_size": ${VALUE_SIZE},
    "duration_secs": ${DURATION_SECS},
    "warmup_secs": ${WARMUP_SECS},
    "connections": ${CONNECTIONS},
    "rate": ${RATE},
    "port": ${PORT},
    "ironcache_maxmemory": "${MAXMEMORY}",
    "eviction": ${EVICT_BOOL},
    "snapshot": ${SNAPSHOT_BOOL},
    "snapshot_interval_secs": ${SNAPSHOT_INTERVAL_SECS}
  },
  "competitor_resolution": {
    "binary": "${COMPETITOR_BIN_ESC}",
    "kind": "${COMPETITOR_KIND}",
    "standin": ${STANDIN_BOOL},
    "pinned_valkey_version": "${PINNED_VALKEY_VERSION}",
    "matrix": "docs/bench/COMPETITORS.md"
  },
  "servers": {
    "ironcache": {
      "name": "ironcache",
      "version": "${IC_VER}",
      "protocol": "${IC_PROTOCOL}",
      "qps": ${IC_QPS},
      "qps_per_core": ${IC_QPS_PER_CORE},
      "bytes_per_key": ${IC_BYTES_PER_KEY},
      "p50_us": ${IC_P50},
      "p99_us": ${IC_P99},
      "p999_us": ${IC_P999},
      "p9999_us": ${IC_P9999}
    },
    "competitor": {
      "name": "${COMPETITOR_NAME}",
      "version": "${COMPETITOR_VERSION}",
      "protocol": "${CO_PROTOCOL}",
      "qps": ${CO_QPS},
      "qps_per_core": ${CO_QPS_PER_CORE},
      "bytes_per_key": ${CO_BYTES_PER_KEY},
      "p50_us": ${CO_P50},
      "p99_us": ${CO_P99},
      "p999_us": ${CO_P999},
      "p9999_us": ${CO_P9999}
    }
  },
  "ratios": {
    "qps_per_core_ironcache_over_competitor": ${QPS_RATIO},
    "bytes_per_key_ironcache_over_competitor": ${BYTES_RATIO},
    "p999_us_ironcache_over_competitor": ${P999_RATIO},
    "p9999_us_ironcache_over_competitor": ${P9999_RATIO}
  },
  "verdict": {
    "memory_only": ${MEMORY_ONLY_BOOL},
    "qps_per_core_exceeds": ${QPS_PASS_BOOL},
    "bytes_per_key_below": ${BYTES_PASS_BOOL},
    "pass": ${OVERALL_BOOL},
    "indicative_only": ${INDICATIVE_BOOL}
  },
  "artifacts": {
    "ironcache_closed": "ironcache-closed.json",
    "ironcache_open": "ironcache-open.json",
    "ironcache_open_histogram": "ironcache-open.hgrm",
    "ironcache_server_log": "ironcache-server.log",
    "competitor_closed": "${COMPETITOR_NAME}-closed.json",
    "competitor_open": "${COMPETITOR_NAME}-open.json",
    "competitor_open_histogram": "${COMPETITOR_NAME}-open.hgrm",
    "competitor_server_log": "${COMPETITOR_NAME}-server.log"
  }
}
EOF

# ---------------------------------------------------------------------------
# READABLE TABLE + VERDICT.
# ---------------------------------------------------------------------------
echo
echo "================= IronCache vs ${COMPETITOR_NAME} head-to-head (A4) ================="
echo "  ironcache:   ${IC_VER}  (${OS_NAME}/${ARCH_NAME}, ${NCPU} cpu)"
echo "  competitor:  ${COMPETITOR_NAME} ${COMPETITOR_VERSION}  (${COMPETITOR_BIN})"
if [[ "${PINNED}" -eq 1 ]]; then
  echo "  pinning:     both servers cores ${PIN_SERVER_CORES} (${SERVER_CORE_COUNT}) | client cores ${PIN_CLIENT_CORES} | loopback ${HOST}"
else
  echo "  pinning:     UNPINNED (taskset absent; results indicative only). per-core denom=${SERVER_CORE_COUNT}"
fi
if [[ "${SMOKE}" == "1" ]]; then
  echo "  mode:        SMOKE (NOT publishable)"
fi
echo "  knobs:       keyspace=${KEYSPACE} keycount=${KEYCOUNT} theta=${THETA} read_ratio=${READ_RATIO} value_size=${VALUE_SIZE} dur=${DURATION_SECS}s conns=${CONNECTIONS} rate=${RATE}"
# Adversarial-mix banner: which of the moat dimensions were ON for this run.
adv_bits=""
[[ "${EVICT}" == "1" ]] && adv_bits="${adv_bits} eviction(maxmemory=${MAXMEMORY},allkeys-lru)"
[[ "${SNAPSHOT}" == "1" ]] && adv_bits="${adv_bits} concurrent-snapshot(BGSAVE/${SNAPSHOT_INTERVAL_SECS}s)"
if [[ -n "${adv_bits}" ]]; then
  echo "  adversarial: mixed(read_ratio=${READ_RATIO}) + zipf(theta=${THETA})${adv_bits}"
else
  echo "  adversarial: none (mixed(read_ratio=${READ_RATIO}) + zipf(theta=${THETA}) only; set EVICT=1 and/or SNAPSHOT=1 for the moat mix)"
fi
echo "  ---"
if [[ "${MEMORY_ONLY}" -eq 1 ]]; then
  echo "  mode:        MEMORY-ONLY (competitor ${COMPETITOR_NAME} is non-RESP; throughput/latency NOT measured)"
fi
printf '  %-16s %18s %18s %18s\n' "metric" "ironcache" "${COMPETITOR_NAME}" "ic/competitor"
if [[ "${MEMORY_ONLY}" -eq 1 ]]; then
  # memory-only: throughput/latency were not measured for the competitor; show n/a.
  printf '  %-16s %18s %18s %18s\n' "qps"            "${IC_QPS}"           "n/a"  "n/a"
  printf '  %-16s %18s %18s %18s\n' "qps_per_core"   "${IC_QPS_PER_CORE}"  "n/a"  "n/a"
  printf '  %-16s %18s %18s %18s\n' "bytes_per_key"  "${IC_BYTES_PER_KEY}" "${CO_BYTES_PER_KEY}" "${BYTES_RATIO}"
  printf '  %-16s %18s %18s %18s\n' "p50_us"         "${IC_P50}"           "n/a"  "-"
  printf '  %-16s %18s %18s %18s\n' "p99_us"         "${IC_P99}"           "n/a"  "-"
  printf '  %-16s %18s %18s %18s\n' "p999_us"        "${IC_P999}"          "n/a"  "-"
  printf '  %-16s %18s %18s %18s\n' "p9999_us"       "${IC_P9999}"         "n/a"  "-"
else
  printf '  %-16s %18s %18s %18s\n' "qps"            "${IC_QPS}"          "${CO_QPS}"          "-"
  printf '  %-16s %18s %18s %18s\n' "qps_per_core"   "${IC_QPS_PER_CORE}" "${CO_QPS_PER_CORE}" "${QPS_RATIO}"
  printf '  %-16s %18s %18s %18s\n' "bytes_per_key"  "${IC_BYTES_PER_KEY}" "${CO_BYTES_PER_KEY}" "${BYTES_RATIO}"
  printf '  %-16s %18s %18s %18s\n' "p50_us"         "${IC_P50}"          "${CO_P50}"          "-"
  printf '  %-16s %18s %18s %18s\n' "p99_us"         "${IC_P99}"          "${CO_P99}"          "-"
  # p999/p9999 are the #518 MOAT tails; the ratio column is ic/competitor (<1 = IronCache tighter).
  printf '  %-16s %18s %18s %18s\n' "p999_us  (moat)" "${IC_P999}"        "${CO_P999}"         "${P999_RATIO}"
  printf '  %-16s %18s %18s %18s\n' "p9999_us"        "${IC_P9999}"       "${CO_P9999}"        "${P9999_RATIO}"
fi
echo "  ---"
echo "  NOTE: p50/p99/p999/p9999 are the OVERALL open-loop op latency (us): the loadgen records GET"
echo "        and SET into ONE histogram, so there is NO GET-vs-SET percentile split. p999 (=p99.9)"
echo "        is the #518 MOAT metric; its ic/competitor ratio <1 means IronCache's tail is tighter."
echo "  ADR-0017 VERDICT:"
if [[ "${MEMORY_ONLY}" -eq 1 ]]; then
  echo "    qps_per_core EXCEEDS competitor?  N/A   (memory-only: ${COMPETITOR_NAME} is non-RESP, throughput not measured)"
else
  echo "    qps_per_core EXCEEDS competitor?  ${QPS_VERDICT}   (${IC_QPS_PER_CORE} vs ${CO_QPS_PER_CORE}, want >)"
fi
echo "    bytes_per_key BELOW competitor?   ${BYTES_VERDICT}   (${IC_BYTES_PER_KEY} vs ${CO_BYTES_PER_KEY}, want <)"
echo "    OVERALL: ${OVERALL}"
if [[ "${MEMORY_ONLY}" -eq 1 ]]; then
  echo "    NOTE: ${COMPETITOR_NAME} does NOT speak RESP; this is a MEMORY-ONLY (bytes-per-key)"
  echo "          comparison. The OVERALL verdict reduces to the bytes-per-key leg alone; the"
  echo "          throughput/latency comparison is OUT OF SCOPE (apples-to-oranges over a"
  echo "          non-RESP protocol). This verdict is INDICATIVE."
elif [[ "${STANDIN}" -eq 1 ]]; then
  if [[ "${COMPETITOR_KIND}" == "redis" ]]; then
    echo "    NOTE: competitor was a redis-server STAND-IN; the published bar is the pinned"
    echo "          valkey-server ${PINNED_VALKEY_VERSION} (docs/bench/COMPETITORS.md). This verdict is INDICATIVE."
  else
    echo "    NOTE: competitor was not the pinned valkey-server ${PINNED_VALKEY_VERSION}; this verdict is INDICATIVE."
  fi
fi
if [[ "${SNAPSHOT}" == "1" ]]; then
  echo "  ---"
  echo "  #518 MOAT (p99.9 under concurrent snapshot): a background BGSAVE fired every"
  echo "  ${SNAPSHOT_INTERVAL_SECS}s DURING the open-loop pass on each server, so the p999_us above CAPTURES"
  echo "  the durable-save tail. IronCache yields between snapshot chunks (#571) so its per-op"
  echo "  work stays bounded (#570); Redis fork-COW stalls; Dragonfly snapshot-spikes. The"
  echo "  per-server ${OUT_DIR}/<name>-bgsave.log records each BGSAVE fire + reply; grep the"
  echo "  '[h2h] ... SNAPSHOT CONFIRMED FIRED' lines above for the proof the save executed."
fi
echo "  ---"
echo "  artifacts:   ${OUT_DIR}"
echo "    headtohead.json (comparison) + per-server closed/open/hgrm/log"
if [[ "${SNAPSHOT}" == "1" ]]; then
  echo "    + per-server <name>-bgsave.log (SNAPSHOT mode: BGSAVE fire timestamps + replies)"
fi
echo "  competitor matrix: docs/bench/COMPETITORS.md"
echo "  tail methodology:  docs/bench/TAIL_LATENCY.md"
echo "==============================================================================="

# Cleanup runs on EXIT (the trap). Both servers have already been stopped by
# stop_server; SERVER_PID is empty so the trap is a no-op. Done.
