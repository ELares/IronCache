#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# IronCache reproducible benchmark run script (BENCHMARK.md #8, PR-A3 of the
# performance track). It orchestrates the A1 allocator-true memory model and the A2
# macro load generator against a real release IronCache server, with pinned,
# documented knobs, and emits machine-readable artifacts plus a manifest.
#
# It is the one scripted invocation that reproduces a published run end to end
# (BENCHMARK.md "Reproducible harness"): it builds the release binaries, pins the
# server and client to disjoint cores over loopback (where taskset exists), warms the
# hot keyset, then runs three measured passes:
#
#   1. memmodel        -> memory.json  (allocator-true bytes-per-key, per encoding)
#   2. loadgen closed  -> closed.json  (peak QPS, closed-loop)
#   3. loadgen open    -> open.json + open.hgrm  (latency tail, open-loop, COMPETITION-free)
#
# The two latency passes are NEVER conflated: peak throughput is a separate
# closed-loop pass; the tail is open-loop at a constant rate (wrk2-style), free of
# coordinated omission. See docs/design/BENCHMARK.md and scripts/bench/README.md.
#
# Usage:
#   scripts/bench/run.sh [--out-dir DIR] [--smoke]
#
# Every knob is overridable via an environment variable (the LOCKED defaults are the
# documented standard run). See the "LOCKED KNOBS" block below.
#
#   SMOKE=1 scripts/bench/run.sh           # fast tiny run for CI / local validation
#   SERVER_CORES=0-3 CLIENT_CORES=4-9 ...  # explicit core pinning (Linux/taskset)

set -euo pipefail

# ---------------------------------------------------------------------------
# Repo root resolution. The script lives in <repo>/scripts/bench, so the repo
# root is two directories up from here. Resolve it independent of the caller's CWD
# so the script can be invoked from anywhere.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# LOCKED KNOBS (BENCHMARK.md). These are the documented standard-run values; each is
# overridable by an environment variable for sweeps, but the defaults are what a
# published number is measured at. A zipf working set with a frozen exponent defines
# the canonical "memory at fixed hit ratio" benchmark (uniform keys are rejected as
# unlike cache reality).
#
# NOTE on concurrency / pipeline depth: within-connection pipeline depth > 1 is a
# DEFERRED loadgen feature (BENCHMARK.md's pipeline-depth sweep is therefore only
# partially covered here). Until it lands, "concurrency" is expressed purely via
# --connections, so the standard run fans out across connections rather than
# pipelining within one.
# ---------------------------------------------------------------------------
SEED="${SEED:-6342047879154770157}"     # 0x5DEE_CE66_D1CE_5EED, the loadgen default seed, fixed.
KEYSPACE="${KEYSPACE:-1000000}"          # distinct keys.
THETA="${THETA:-0.99}"                   # zipf exponent (YCSB default skew).
READ_RATIO="${READ_RATIO:-0.9}"          # 90% GET / 10% SET; the locked hit-ratio target.
VALUE_SIZE="${VALUE_SIZE:-128}"          # SET value bytes.
DURATION_SECS="${DURATION_SECS:-10}"     # measured-pass duration.
CONNECTIONS="${CONNECTIONS:-50}"         # load fan-out (closed) / dispatch pool (open).
RATE="${RATE:-50000}"                    # open-loop target ops/sec.
WARMUP_SECS="${WARMUP_SECS:-3}"          # write-only warmup to populate the hot keyset.

# Server knobs.
PORT="${PORT:-6399}"                     # RESP port (loopback only).
HOST="127.0.0.1"                         # loopback, always (BENCHMARK.md isolation).
# Maxmemory: there is NO --maxmemory CLI flag on the ironcache binary; the ceiling is
# a config key set via the IRONCACHE_MAXMEMORY env overlay (human sizes like "512mb"
# are accepted). We default generous so the standard keyspace never evicts mid-run.
MAXMEMORY="${MAXMEMORY:-1gb}"

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
      echo "See scripts/bench/README.md for the full knob list (env vars)." >&2
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

# SMOKE shrinks every dimension so the whole script finishes in a few seconds. It is
# for CI / local validation, NOT a publishable measurement.
if [[ "${SMOKE}" == "1" ]]; then
  KEYSPACE=1000
  DURATION_SECS=1
  WARMUP_SECS=1
  CONNECTIONS=4
  RATE=2000
  echo "[bench] SMOKE mode: tiny durations/keyspace; results are NOT publishable."
fi

# ---------------------------------------------------------------------------
# Build the release binaries (ironcache, memmodel, loadgen). The first build is slow.
# ---------------------------------------------------------------------------
echo "[bench] building release binaries (cargo build --release -p ironcache -p ironcache-bench)..."
cargo build --release -p ironcache -p ironcache-bench

IRONCACHE_BIN="${REPO_ROOT}/target/release/ironcache"
MEMMODEL_BIN="${REPO_ROOT}/target/release/memmodel"
LOADGEN_BIN="${REPO_ROOT}/target/release/loadgen"
for b in "${IRONCACHE_BIN}" "${MEMMODEL_BIN}" "${LOADGEN_BIN}"; do
  [[ -x "${b}" ]] || { echo "error: expected binary missing after build: ${b}" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# Resolve the version (parse "ironcache 0.0.0" -> 0.0.0). --version prints the version
# to stdout; the jemalloc boot warning goes to stderr, so stdout is clean. We take the
# last whitespace-separated token of the first stdout line.
# ---------------------------------------------------------------------------
VERSION_LINE="$("${IRONCACHE_BIN}" --version 2>/dev/null | head -n1)"
VER="${VERSION_LINE##* }"
[[ -n "${VER}" ]] || VER="unknown"

OS_NAME="$(uname -s)"
ARCH_NAME="$(uname -m)"

# Default out dir: bench-results/<ver>-<os>-<arch> (gitignored).
if [[ -z "${OUT_DIR}" ]]; then
  OUT_DIR="${REPO_ROOT}/bench-results/${VER}-${OS_NAME}-${ARCH_NAME}"
fi
mkdir -p "${OUT_DIR}"
echo "[bench] version=${VER} os=${OS_NAME} arch=${ARCH_NAME}"
echo "[bench] output dir: ${OUT_DIR}"

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

# Count the cores in a taskset spec like "0-3", "0,2,4", or "0-3,8-11". Used to set
# the server shard count to the number of cores it is actually pinned to (one shard =
# one OS thread; oversubscribing shards onto fewer cores would distort the QPS-per-core
# headline metric). Echoes the count.
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

# The server shard count. One shard = one OS thread (shared-nothing, thread-per-core),
# so it MUST match the number of cores the server actually runs on, not the host total.
# Unpinned: all NCPU cores. Pinned: set below to the server pinned-core count.
SHARDS="${NCPU}"

# ---------------------------------------------------------------------------
# PINNING (the reproducibility core, BENCHMARK.md). If taskset exists (Linux), pin the
# SERVER and the CLIENT to DISJOINT core sets over loopback. The sets are configurable
# via SERVER_CORES / CLIENT_CORES; the defaults split the box in half (server gets the
# first half, client the second). If taskset is absent (e.g. a macOS dev box) we WARN
# that the run is unpinned/indicative and proceed without it. Never fail just because
# taskset is missing.
#
# SERVER_PREFIX / CLIENT_PREFIX are arrays prepended to each launch; empty when
# unpinned.
# ---------------------------------------------------------------------------
PINNED=0
PIN_SERVER_CORES=""
PIN_CLIENT_CORES=""
SERVER_PREFIX=()
CLIENT_PREFIX=()
if command -v taskset >/dev/null 2>&1; then
  half=$(( NCPU / 2 ))
  if [[ "${half}" -lt 1 ]]; then half=1; fi
  # Defaults: server = [0 .. half-1], client = [half .. NCPU-1]. On a 1-core box both
  # collapse to core 0 (degenerate but valid).
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
  # Match the shard count to the cores the server is pinned to (NOT the host total),
  # so NCPU shard threads never oversubscribe a smaller pinned set and the QPS-per-core
  # denominator matches the threads actually running.
  SHARDS="$(count_cores "${PIN_SERVER_CORES}")"
  [[ "${SHARDS}" -ge 1 ]] || SHARDS=1
  echo "[bench] taskset found: pinning server->cores ${PIN_SERVER_CORES} (${SHARDS} shards), client->cores ${PIN_CLIENT_CORES} (loopback ${HOST})."
else
  echo "[bench] WARNING: taskset not found (likely macOS). Running UNPINNED."
  echo "[bench] WARNING: results are INDICATIVE only; a publishable run needs disjoint server/client core pinning on Linux."
fi

# ---------------------------------------------------------------------------
# Server lifecycle. Start in the background; install a trap so the server is always
# killed on EXIT / INT / TERM (no orphan). Poll readiness (TCP connect, redis-cli PING
# if available) up to a bounded 10s.
# ---------------------------------------------------------------------------
SERVER_PID=""
SERVER_LOG="${OUT_DIR}/server.log"

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    # Give it a moment to exit cleanly, then force.
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.2
    done
    kill -9 "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# Fail fast if the port is already serving. The server binds every shard with
# SO_REUSEPORT (and its own pre-flight bind is REUSEPORT too), so a stale ironcache on
# this port would NOT be rejected at bind: it would silently co-reside and the OS would
# split the loadgen's connections across both processes, mixing two servers' numbers. A
# plain pre-launch connect detects an existing listener that a REUSEPORT bind hides.
if (exec 3<>"/dev/tcp/${HOST}/${PORT}") 2>/dev/null; then
  echo "error: ${HOST}:${PORT} is already in use (a stale ironcache?). Free it or set PORT=." >&2
  exit 1
fi

echo "[bench] starting server on ${HOST}:${PORT} (shards=${SHARDS}, maxmemory=${MAXMEMORY})..."
# maxmemory is passed via the IRONCACHE_MAXMEMORY config overlay (no CLI flag exists);
# --port and --shards are global CLI flags consumed before the `server` subcommand.
# SHARDS == the cores the server runs on (the pinned set, or NCPU when unpinned), so the
# thread-per-core engine is never oversubscribed and QPS-per-core stays honest.
# The `${arr[@]+"${arr[@]}"}` form expands to nothing when the prefix array is empty
# (unpinned) and to its elements otherwise. This is the portable way to expand a
# possibly-empty array under `set -u` (Apple's stock bash 3.2 errors on a bare
# `"${arr[@]}"` when the array is empty).
IRONCACHE_MAXMEMORY="${MAXMEMORY}" \
  ${SERVER_PREFIX[@]+"${SERVER_PREFIX[@]}"} "${IRONCACHE_BIN}" \
  --port "${PORT}" --shards "${SHARDS}" server \
  >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

# Readiness probe. Prefer redis-cli PING; otherwise a bash /dev/tcp connect. Bounded to
# ~10s (40 * 0.25s).
server_ready() {
  if command -v redis-cli >/dev/null 2>&1; then
    [[ "$(redis-cli -h "${HOST}" -p "${PORT}" PING 2>/dev/null)" == "PONG" ]]
  else
    # /dev/tcp is a bash builtin; a successful open means the listener is up.
    (exec 3<>"/dev/tcp/${HOST}/${PORT}") 2>/dev/null && exec 3>&- 3<&-
  fi
}

ready=0
for _ in $(seq 1 40); do
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "error: server process exited during startup. Log:" >&2
    cat "${SERVER_LOG}" >&2 || true
    exit 1
  fi
  if server_ready; then ready=1; break; fi
  sleep 0.25
done
if [[ "${ready}" -ne 1 ]]; then
  echo "error: server did not become ready on ${HOST}:${PORT} within ~10s. Log:" >&2
  cat "${SERVER_LOG}" >&2 || true
  exit 1
fi
echo "[bench] server ready (pid ${SERVER_PID})."

# ---------------------------------------------------------------------------
# loadgen launcher. Common flags are shared; per-pass flags are appended by the caller.
# Client pinning prefix is prepended when taskset is present.
# ---------------------------------------------------------------------------
run_loadgen() {
  # See the SERVER_PREFIX note above: expand a possibly-empty array safely under set -u.
  ${CLIENT_PREFIX[@]+"${CLIENT_PREFIX[@]}"} "${LOADGEN_BIN}" \
    --host "${HOST}" --port "${PORT}" \
    --seed "${SEED}" \
    --keyspace "${KEYSPACE}" \
    --theta "${THETA}" \
    --value-size "${VALUE_SIZE}" \
    "$@"
}

# ---------------------------------------------------------------------------
# WARMUP / hit-ratio. Before the measured READ-HEAVY pass we POPULATE the hot keyset so
# GETs hit (the locked target is ~90% hit ratio). A write-only warmup (--read-ratio 0,
# all SETs) over the keyspace is sufficient: the measured reads draw from a zipf
# distribution that CONCENTRATES on a small set of hot keys, so once the writes have
# touched the keyspace the hot keys (which dominate the measured GETs) are present, and
# the 90%-read pass sees a high hit ratio. The warmup output is discarded.
# ---------------------------------------------------------------------------
echo "[bench] warmup: write-only (--read-ratio 0) for ${WARMUP_SECS}s to populate the hot keyset..."
run_loadgen --mode closed --read-ratio 0 --duration-secs "${WARMUP_SECS}" \
  --connections "${CONNECTIONS}" --out - >/dev/null
echo "[bench] warmup done."

# ---------------------------------------------------------------------------
# MEASURED PASSES.
# ---------------------------------------------------------------------------
MEMORY_JSON="${OUT_DIR}/memory.json"
CLOSED_JSON="${OUT_DIR}/closed.json"
OPEN_JSON="${OUT_DIR}/open.json"
OPEN_HGRM="${OUT_DIR}/open.hgrm"

# 1. Memory model (allocator-true bytes-per-key, per encoding). The memmodel binary
#    is self-contained (it builds its own in-process store), so it is not pinned to the
#    client cores: it neither talks to the server nor competes with it for the run.
echo "[bench] pass 1/3: memory model -> $(basename "${MEMORY_JSON}")"
"${MEMMODEL_BIN}" >"${MEMORY_JSON}"

# 2. Closed-loop: peak QPS. The op-mix is the locked READ_RATIO (90% GET).
echo "[bench] pass 2/3: closed-loop peak QPS -> $(basename "${CLOSED_JSON}")"
run_loadgen --mode closed --read-ratio "${READ_RATIO}" --duration-secs "${DURATION_SECS}" \
  --connections "${CONNECTIONS}" --out "${CLOSED_JSON}"

# 3. Open-loop: latency tail at a constant rate + the HdrHistogram artifact.
echo "[bench] pass 3/3: open-loop latency tail @ ${RATE} ops/sec -> $(basename "${OPEN_JSON}")"
run_loadgen --mode open --read-ratio "${READ_RATIO}" --duration-secs "${DURATION_SECS}" \
  --connections "${CONNECTIONS}" --rate "${RATE}" \
  --out "${OPEN_JSON}" --hist "${OPEN_HGRM}"

# ---------------------------------------------------------------------------
# MANIFEST. Capture every run param, host facts, the version, a UTC timestamp, and a
# pointer to the committed competitor matrix. This is a SHELL script (not Rust), so the
# determinism lint (no wall clock) does NOT apply: `date -u` is the right tool here.
# Hand-written heredoc; kept valid JSON.
# ---------------------------------------------------------------------------
MANIFEST="${OUT_DIR}/manifest.json"
TIMESTAMP_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
UNAME_ALL="$(uname -a)"
# JSON-escape the uname string (backslashes and double quotes) so the manifest stays
# valid even on hosts whose `uname -a` contains a quote.
UNAME_ESC="${UNAME_ALL//\\/\\\\}"
UNAME_ESC="${UNAME_ESC//\"/\\\"}"

if [[ "${PINNED}" -eq 1 ]]; then PINNED_BOOL="true"; else PINNED_BOOL="false"; fi
if [[ "${SMOKE}" == "1" ]]; then SMOKE_BOOL="true"; else SMOKE_BOOL="false"; fi

cat >"${MANIFEST}" <<EOF
{
  "schema": "ironcache-bench-manifest/1",
  "timestamp_utc": "${TIMESTAMP_UTC}",
  "smoke": ${SMOKE_BOOL},
  "ironcache_version": "${VER}",
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
    "host_addr": "${HOST}"
  },
  "server": {
    "port": ${PORT},
    "shards": ${SHARDS},
    "maxmemory": "${MAXMEMORY}"
  },
  "knobs": {
    "seed": ${SEED},
    "keyspace": ${KEYSPACE},
    "theta": ${THETA},
    "read_ratio": ${READ_RATIO},
    "value_size": ${VALUE_SIZE},
    "duration_secs": ${DURATION_SECS},
    "warmup_secs": ${WARMUP_SECS},
    "connections": ${CONNECTIONS},
    "rate": ${RATE},
    "pipeline_depth": 1
  },
  "artifacts": {
    "memory": "memory.json",
    "closed": "closed.json",
    "open": "open.json",
    "open_histogram": "open.hgrm",
    "server_log": "server.log"
  },
  "competitor_matrix": "docs/bench/COMPETITORS.md"
}
EOF

# ---------------------------------------------------------------------------
# SUMMARY. Read the artifacts back and print a human-readable digest. jq is used when
# present (clean parsing); otherwise we fall back to grep/sed extraction so the summary
# still prints on a box without jq.
# ---------------------------------------------------------------------------
json_field() {
  # json_field FILE KEY  -> the scalar value of "KEY" from a flat JSON object/array.
  local file="$1" key="$2"
  if command -v jq >/dev/null 2>&1; then
    jq -r "if type==\"array\" then .[0].${key} else .${key} end" "${file}" 2>/dev/null
  else
    # Flat numeric/bool/string scalar after "key": (first match).
    sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\"\\{0,1\\}\\([^,\"}]*\\)\"\\{0,1\\}.*/\\1/p" "${file}" | head -n1
  fi
}

QPS="$(json_field "${CLOSED_JSON}" qps)"
O_P50="$(json_field "${OPEN_JSON}" p50_us)"
O_P99="$(json_field "${OPEN_JSON}" p99_us)"
O_P999="$(json_field "${OPEN_JSON}" p999_us)"
O_SAT="$(json_field "${OPEN_JSON}" saturated)"
O_ACH="$(json_field "${OPEN_JSON}" achieved_rate)"

# Per-encoding bytes/key from memory.json (array of objects). jq gives a clean table;
# the no-jq fallback prints the raw array.
mem_summary() {
  if command -v jq >/dev/null 2>&1; then
    jq -r '.[] | "    \(.encoding_class)\tobject=\(.object_bytes_per_key)\ttotal=\(.total_bytes_per_key) bytes/key"' "${MEMORY_JSON}"
  else
    echo "    (install jq for a parsed table; raw below)"
    head -c 600 "${MEMORY_JSON}"
    echo
  fi
}

echo
echo "==================== IronCache benchmark summary ===================="
echo "  version:        ${VER}  (${OS_NAME}/${ARCH_NAME}, ${NCPU} cpu)"
if [[ "${PINNED}" -eq 1 ]]; then
  echo "  pinning:        server cores ${PIN_SERVER_CORES} | client cores ${PIN_CLIENT_CORES} | loopback ${HOST}"
else
  echo "  pinning:        UNPINNED (taskset absent; results indicative only)"
fi
if [[ "${SMOKE}" == "1" ]]; then
  echo "  mode:           SMOKE (NOT publishable)"
fi
echo "  knobs:          keyspace=${KEYSPACE} theta=${THETA} read_ratio=${READ_RATIO} value_size=${VALUE_SIZE} dur=${DURATION_SECS}s conns=${CONNECTIONS} rate=${RATE} (pipeline_depth=1)"
echo "  ---"
echo "  closed-loop peak QPS:   ${QPS}"
echo "  open-loop p50/p99/p999: ${O_P50} / ${O_P99} / ${O_P999} us  (target ${RATE} ops/sec, achieved ${O_ACH}, saturated=${O_SAT})"
echo "  memory (allocator-true bytes/key, per encoding):"
mem_summary
echo "  ---"
echo "  artifacts:      ${OUT_DIR}"
echo "    memory.json closed.json open.json open.hgrm manifest.json server.log"
echo "  competitor matrix: docs/bench/COMPETITORS.md"
echo "====================================================================="

# Cleanup runs on EXIT (the trap) and kills the server. Done.
