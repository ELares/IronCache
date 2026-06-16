#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# IronCache per-PR perf-regression MEASURE step (PERF_REGRESSION_GATE.md #159, PR-A5 of
# the performance track). It measures the two HEADLINE gate metrics for the CURRENT
# working tree and writes a compact JSON the compare step ratchets against a baseline:
#
#   1. bytes_per_key  - the allocator-true total_bytes_per_key per encoding class, from
#                       the A1 memmodel binary. DETERMINISTIC (no server, no clock, no
#                       network), so a TIGHT budget is fine downstream.
#   2. qps (peak)      - a SHORT closed-loop loadgen point, the per-core throughput proxy.
#                       NOISY on shared CI, so we run N reps, capture each rep's qps, and
#                       emit the MEDIAN plus the min/max (the compare step turns the
#                       min/max spread into a noise band, then a GENEROUS budget).
#
# This is the per-tree HALF of the gate: the workflow runs it TWICE on the same runner
# (once in a merge-base worktree, once on HEAD) and feeds both JSONs to perf_compare.sh.
# It NEVER compares or ratchets anything itself and NEVER fails on a metric value: its
# only job is to produce one tree's numbers honestly.
#
# Open-loop p99 / criterion micro-benches are REPORTED-not-failed (PERF_REGRESSION_GATE.md:
# tail noise on shared CI is high). They are intentionally NOT measured here to keep the
# per-PR macro point SHORT; the headline ratchet is bytes_per_key + qps only.
#
# Usage:
#   scripts/bench/perf_measure.sh --out FILE [--smoke]
#
# Knobs (env-overridable; the defaults are a SHORT per-PR point, not a publishable run):
#   QPS_REPS         number of closed-loop reps (default 5); the median is the headline.
#   DURATION_SECS    per-rep closed-loop seconds (default 2; SHORT on purpose).
#   KEYSPACE         distinct keys (default 100000; small-ish so warmup is cheap).
#   CONNECTIONS      closed-loop fan-out (default 16).
#   READ_RATIO       op-mix (default 0.9, 90% GET).
#   VALUE_SIZE       SET value bytes (default 128).
#   THETA            zipf exponent (default 0.99).
#   SEED             workload seed (default the loadgen locked seed).
#   WARMUP_SECS      write-only warmup before the reps (default 1).
#   PORT             RESP port (default 6398, loopback only).
#   MAXMEMORY        IronCache ceiling via the IRONCACHE_MAXMEMORY overlay (default 1gb).
#   SHARDS           server shard count (default: host cpu count).
#   SMOKE=1          tiny mode (1 rep, 1s, keyspace 1000, 4 conns) for CI/local validation.

set -euo pipefail

# ---------------------------------------------------------------------------
# Repo root resolution. The script lives in <repo>/scripts/bench, so the repo root is
# two directories up. Resolve it independent of the caller's CWD so it can run from the
# HEAD checkout OR a merge-base worktree.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Knobs (env-overridable). SHORT per-PR defaults, not the publishable run.sh standard.
# ---------------------------------------------------------------------------
QPS_REPS="${QPS_REPS:-5}"                 # closed-loop reps; the median is the headline.
DURATION_SECS="${DURATION_SECS:-2}"       # per-rep seconds (SHORT).
KEYSPACE="${KEYSPACE:-100000}"            # distinct keys (small-ish).
CONNECTIONS="${CONNECTIONS:-16}"          # closed-loop fan-out.
READ_RATIO="${READ_RATIO:-0.9}"           # 90% GET / 10% SET.
VALUE_SIZE="${VALUE_SIZE:-128}"           # SET value bytes.
THETA="${THETA:-0.99}"                    # zipf exponent.
SEED="${SEED:-6342047879154770157}"       # 0x5DEE_CE66_D1CE_5EED, fixed.
WARMUP_SECS="${WARMUP_SECS:-1}"           # write-only warmup before the reps.

PORT="${PORT:-6398}"                      # RESP port (loopback only).
HOST="127.0.0.1"                          # loopback, always (isolation).
MAXMEMORY="${MAXMEMORY:-1gb}"             # IronCache ceiling via IRONCACHE_MAXMEMORY overlay.

# ---------------------------------------------------------------------------
# Flag parsing: --out FILE (required), --smoke. Anything else is an error.
# ---------------------------------------------------------------------------
OUT_FILE=""
SMOKE="${SMOKE:-0}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      [[ $# -ge 2 ]] || { echo "error: --out needs an argument" >&2; exit 2; }
      OUT_FILE="$2"
      shift 2
      ;;
    --out=*)
      OUT_FILE="${1#*=}"
      shift
      ;;
    --smoke)
      SMOKE=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 --out FILE [--smoke]" >&2
      echo "Measures the perf-gate metrics (bytes_per_key + median qps) for the current tree." >&2
      echo "See scripts/bench/README.md (Perf-regression gate (A5)) for the knob list." >&2
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done
[[ -n "${OUT_FILE}" ]] || { echo "error: --out FILE is required" >&2; exit 2; }

# SMOKE shrinks every dimension so the whole script finishes in a couple of seconds. It
# is for CI / local validation, NOT a publishable measurement.
if [[ "${SMOKE}" == "1" ]]; then
  QPS_REPS=1
  DURATION_SECS=1
  WARMUP_SECS=1
  KEYSPACE=1000
  CONNECTIONS=4
  echo "[perf] SMOKE mode: 1 rep, tiny duration/keyspace; results are NOT publishable." >&2
fi

# ---------------------------------------------------------------------------
# Build the release binaries (ironcache + the bench crate, which produces memmodel and
# loadgen). NATIVE release build (the gate measures the runner's native arch); no
# cross-compile. The first build is slow; CI caches the target dir.
# ---------------------------------------------------------------------------
echo "[perf] building release binaries (cargo build --release -p ironcache -p ironcache-bench)..." >&2
cargo build --release -p ironcache -p ironcache-bench >&2

IRONCACHE_BIN="${REPO_ROOT}/target/release/ironcache"
MEMMODEL_BIN="${REPO_ROOT}/target/release/memmodel"
LOADGEN_BIN="${REPO_ROOT}/target/release/loadgen"
for b in "${IRONCACHE_BIN}" "${MEMMODEL_BIN}" "${LOADGEN_BIN}"; do
  [[ -x "${b}" ]] || { echo "error: expected binary missing after build: ${b}" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# CPU-count helper (nproc on Linux, sysctl on macOS, fallback 1). The server shard count
# defaults to the host cpu count (one shard = one OS thread, thread-per-core engine).
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
SHARDS="${SHARDS:-${NCPU}}"
[[ "${SHARDS}" -ge 1 ]] || SHARDS=1

# ---------------------------------------------------------------------------
# (1) BYTES-PER-KEY: run memmodel, capture total_bytes_per_key per encoding class. The
# memmodel binary is self-contained (it builds its own in-process store, no server, no
# clock, no network), so it is deterministic. The three classes are int / embstr / raw.
# We parse with jq when present, else an awk fallback over the flat JSON array.
# ---------------------------------------------------------------------------
echo "[perf] measuring bytes_per_key (memmodel, allocator-true, deterministic)..." >&2
MEMMODEL_JSON="$("${MEMMODEL_BIN}")"

# bytes_for CLASS: echo the total_bytes_per_key for the named encoding class, or "0".
bytes_for() {
  local cls="$1"
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "${MEMMODEL_JSON}" \
      | jq -r --arg c "${cls}" '.[] | select(.encoding_class==$c) | .total_bytes_per_key' 2>/dev/null \
      | head -n1
  else
    # Flat array of objects; isolate the object for this class, then pull its
    # total_bytes_per_key. awk: split on "}" so each record is one object, match the
    # class, then capture the numeric after "total_bytes_per_key":.
    printf '%s' "${MEMMODEL_JSON}" | awk -v c="${cls}" '
      BEGIN { RS="}" }
      $0 ~ ("\"encoding_class\":\"" c "\"") {
        if (match($0, /"total_bytes_per_key":[0-9.]+/)) {
          s = substr($0, RSTART, RLENGTH)
          sub(/"total_bytes_per_key":/, "", s)
          print s
          exit
        }
      }'
  fi
}

BYTES_INT="$(bytes_for int)"
BYTES_EMBSTR="$(bytes_for embstr)"
BYTES_RAW="$(bytes_for raw)"
# Default any missing class to 0 so the JSON stays valid even if a class is absent.
[[ -n "${BYTES_INT}" ]] || BYTES_INT="0"
[[ -n "${BYTES_EMBSTR}" ]] || BYTES_EMBSTR="0"
[[ -n "${BYTES_RAW}" ]] || BYTES_RAW="0"
echo "[perf] bytes_per_key: int=${BYTES_INT} embstr=${BYTES_EMBSTR} raw=${BYTES_RAW}" >&2

# ---------------------------------------------------------------------------
# (2) QPS: boot the server, warm the hot keyset, run QPS_REPS short closed-loop points,
# capture each rep's qps, kill the server.
#
# Server lifecycle mirrors run.sh/headtohead.sh: pre-launch /dev/tcp port-free check
# (a stale ironcache would co-reside under SO_REUSEPORT and split the loadgen's
# connections, mixing two servers' numbers), background launch, trap-kill by PID on
# EXIT/INT/TERM (no orphan), redis-cli PING readiness (else a /dev/tcp connect) bounded
# to ~10s.
# ---------------------------------------------------------------------------
SERVER_PID=""
SERVER_LOG="$(mktemp -t ironcache-perf-server.XXXXXX)"

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.2
    done
    kill -9 "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -f "${SERVER_LOG}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Pre-launch port-free check (a plain connect detects a listener a REUSEPORT bind hides).
if (exec 3<>"/dev/tcp/${HOST}/${PORT}") 2>/dev/null; then
  exec 3>&- 3<&- 2>/dev/null || true
  echo "error: ${HOST}:${PORT} is already in use (a stale ironcache?). Free it or set PORT=." >&2
  exit 1
fi

echo "[perf] starting server on ${HOST}:${PORT} (shards=${SHARDS}, maxmemory=${MAXMEMORY})..." >&2
IRONCACHE_MAXMEMORY="${MAXMEMORY}" "${IRONCACHE_BIN}" \
  --port "${PORT}" --shards "${SHARDS}" server \
  >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

server_ready() {
  if command -v redis-cli >/dev/null 2>&1; then
    [[ "$(redis-cli -h "${HOST}" -p "${PORT}" PING 2>/dev/null)" == "PONG" ]]
  else
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
echo "[perf] server ready (pid ${SERVER_PID})." >&2

# loadgen launcher. Shared workload flags; per-call flags appended by the caller.
run_loadgen() {
  "${LOADGEN_BIN}" \
    --host "${HOST}" --port "${PORT}" \
    --seed "${SEED}" \
    --keyspace "${KEYSPACE}" \
    --theta "${THETA}" \
    --value-size "${VALUE_SIZE}" \
    "$@"
}

# qps_of FILE: scalar qps from a loadgen closed-mode result JSON (jq if present, else a
# sed fallback over the flat object).
qps_of() {
  local file="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -r '.qps' "${file}" 2>/dev/null
  else
    sed -n 's/.*"qps"[[:space:]]*:[[:space:]]*\([0-9.]*\).*/\1/p' "${file}" | head -n1
  fi
}

# Warmup: write-only to populate the hot keyset so the measured GETs hit (run.sh idiom).
echo "[perf] warmup: write-only (--read-ratio 0) for ${WARMUP_SECS}s..." >&2
run_loadgen --mode closed --read-ratio 0 --duration-secs "${WARMUP_SECS}" \
  --connections "${CONNECTIONS}" --out - >/dev/null

# Reps: collect each rep's qps into a space-separated list. A temp file per rep keeps the
# loadgen JSON off this script's stdout (which is reserved for the final JSON when --out
# is "-"); we only read back the qps scalar.
REP_TMP="$(mktemp -t ironcache-perf-rep.XXXXXX)"
QPS_LIST=""
i=1
while [[ "${i}" -le "${QPS_REPS}" ]]; do
  echo "[perf] qps rep ${i}/${QPS_REPS} (closed, ${DURATION_SECS}s, ${CONNECTIONS} conns, keyspace ${KEYSPACE})..." >&2
  run_loadgen --mode closed --read-ratio "${READ_RATIO}" --duration-secs "${DURATION_SECS}" \
    --connections "${CONNECTIONS}" --out "${REP_TMP}"
  q="$(qps_of "${REP_TMP}")"
  [[ -n "${q}" ]] || q="0"
  echo "[perf]   rep ${i} qps=${q}" >&2
  if [[ -z "${QPS_LIST}" ]]; then QPS_LIST="${q}"; else QPS_LIST="${QPS_LIST} ${q}"; fi
  i=$(( i + 1 ))
done
rm -f "${REP_TMP}" 2>/dev/null || true

# Stop the server now (the reps are done); the trap is a safety net for early exits.
cleanup
SERVER_PID=""
trap - EXIT INT TERM
rm -f "${SERVER_LOG}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Reduce the reps: MEDIAN (the headline) + MIN/MAX (the band source). awk does the float
# sort/median so there is no bc/python dependency. Also emit the reps as a JSON array.
# ---------------------------------------------------------------------------
read -r QPS_MEDIAN QPS_MIN QPS_MAX <<EOF
$(printf '%s\n' "${QPS_LIST}" | awk '{
  n = NF
  for (i = 1; i <= n; i++) v[i] = $i + 0
  # insertion sort (n is tiny: a handful of reps).
  for (i = 2; i <= n; i++) { x = v[i]; j = i - 1; while (j >= 1 && v[j] > x) { v[j+1] = v[j]; j-- } v[j+1] = x }
  if (n == 0) { print "0 0 0"; exit }
  mn = v[1]; mx = v[n]
  if (n % 2 == 1) { med = v[(n+1)/2] } else { med = (v[n/2] + v[n/2+1]) / 2.0 }
  printf "%.2f %.2f %.2f\n", med, mn, mx
}')
EOF
[[ -n "${QPS_MEDIAN}" ]] || QPS_MEDIAN="0"
[[ -n "${QPS_MIN}" ]] || QPS_MIN="0"
[[ -n "${QPS_MAX}" ]] || QPS_MAX="0"

# reps as a JSON array, e.g. [73368.19, 72950.10].
QPS_REPS_JSON="$(printf '%s\n' "${QPS_LIST}" | awk '{
  out = "["
  for (i = 1; i <= NF; i++) { out = out (i > 1 ? ", " : "") ($i + 0) }
  out = out "]"
  print out
}')"
[[ -n "${QPS_REPS_JSON}" ]] || QPS_REPS_JSON="[]"

# ---------------------------------------------------------------------------
# Emit the JSON. To stdout when --out is "-", else to the file.
# ---------------------------------------------------------------------------
OS_NAME="$(uname -s)"
ARCH_NAME="$(uname -m)"
VERSION_LINE="$("${IRONCACHE_BIN}" --version 2>/dev/null | head -n1)"
VER="${VERSION_LINE##* }"
[[ -n "${VER}" ]] || VER="unknown"
if [[ "${SMOKE}" == "1" ]]; then SMOKE_BOOL="true"; else SMOKE_BOOL="false"; fi

emit_json() {
  cat <<EOF
{
  "schema": "ironcache-perf-measure/1",
  "smoke": ${SMOKE_BOOL},
  "ironcache_version": "${VER}",
  "host": { "os": "${OS_NAME}", "arch": "${ARCH_NAME}", "cpu_count": ${NCPU} },
  "knobs": {
    "qps_reps": ${QPS_REPS},
    "duration_secs": ${DURATION_SECS},
    "keyspace": ${KEYSPACE},
    "connections": ${CONNECTIONS},
    "read_ratio": ${READ_RATIO},
    "value_size": ${VALUE_SIZE},
    "theta": ${THETA},
    "warmup_secs": ${WARMUP_SECS},
    "shards": ${SHARDS}
  },
  "qps_median": ${QPS_MEDIAN},
  "qps_reps": ${QPS_REPS_JSON},
  "qps_min": ${QPS_MIN},
  "qps_max": ${QPS_MAX},
  "bytes_per_key": {
    "int": ${BYTES_INT},
    "embstr": ${BYTES_EMBSTR},
    "raw": ${BYTES_RAW}
  }
}
EOF
}

if [[ "${OUT_FILE}" == "-" ]]; then
  emit_json
else
  emit_json >"${OUT_FILE}"
  echo "[perf] wrote ${OUT_FILE}" >&2
fi
