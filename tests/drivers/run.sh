#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# PROD-8 client-DRIVER compatibility matrix (issue #158).
#
# Boots a REAL IronCache server (single-node) AND a turnkey 3-node Raft cluster, then runs each
# available real Redis CLIENT LIBRARY (redis-py / go-redis / ioredis) against both, asserting the
# values come back correct and -- for the cluster clients -- that topology DISCOVERY (CLUSTER SLOTS)
# + MOVED-routing work end to end. This is the layer the differential wire-harness does NOT cover:
# the differential proves byte-for-byte RESP parity vs redis-server; THIS proves real client
# libraries actually drive IronCache.
#
# Each per-language script prints a machine-readable result line per (mode, op-group):
#     RESULT <client> <mode> <op-group> <PASS|FAIL> [detail]
# and exits non-zero if ANY of its groups FAILed. This orchestrator collects every RESULT line and
# prints a final matrix. A client whose toolchain is absent is SKIPPED (reported, never failed): the
# floor is "the clients that are installable here all pass"; missing toolchains are honestly noted.
#
# Usage:
#   tests/drivers/run.sh                 # build (if needed) + run every available client
#   IRONCACHE_BIN=/path/to/ironcache tests/drivers/run.sh   # use a prebuilt binary
#   DRIVERS=python,go tests/drivers/run.sh                  # restrict to a subset
#
# Cleanup is unconditional (trap): every spawned ironcache process is killed and the temp dir wiped.

set -u

# ---------------------------------------------------------------------------- paths + config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ironcache-drivers.XXXXXX")"
RESULTS_FILE="$WORK_DIR/results.tsv"
: > "$RESULTS_FILE"

# Which clients to attempt (comma list); default = all three.
DRIVERS="${DRIVERS:-python,go,node}"

# Ports. Single-node on SINGLE_PORT; the cluster on the three CLUSTER_PORTS (each derives its
# bus port = port+10000 and repl port = port+20000, so the ports are spaced > 20000 apart to keep
# every derived port distinct and unused).
SINGLE_PORT="${SINGLE_PORT:-7411}"
CLUSTER_PORTS=(7421 7451 7481)

# Stable 40-hex node ids (must match the per-node topology entries below).
NODE_IDS=(
  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  "cccccccccccccccccccccccccccccccccccccccc"
)
# The shipped 3-way slot split (deploy/compose layout): node0 [0,5460], node1 [5461,10922],
# node2 [10923,16383] -- all 16384 slots covered.
SLOT_RANGES=("[[0, 5460]]" "[[5461, 10922]]" "[[10923, 16383]]")

PIDS=()

# ---------------------------------------------------------------------------- logging helpers
info() { printf '\033[1;34m[run]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[run]\033[0m %s\n' "$*" >&2; }
err()  { printf '\033[1;31m[run]\033[0m %s\n' "$*" >&2; }

cleanup() {
  # Kill exactly OUR ironcache processes by recorded pid (never `pkill -f`, which self-matches).
  for pid in "${PIDS[@]:-}"; do
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null
  done
  # Belt-and-suspenders: any stray ironcache (exact-name match only).
  pkill -x ironcache 2>/dev/null
  sleep 0.3
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT INT TERM

record() { # record <client> <mode> <group> <PASS|FAIL> [detail...]
  printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "${*:5}" >> "$RESULTS_FILE"
}

# ---------------------------------------------------------------------------- resolve the binary
resolve_binary() {
  if [ -n "${IRONCACHE_BIN:-}" ] && [ -x "${IRONCACHE_BIN:-}" ]; then
    BIN="$IRONCACHE_BIN"
    info "using prebuilt binary: $BIN"
    return 0
  fi
  BIN="$REPO_ROOT/target/release/ironcache"
  if [ ! -x "$BIN" ]; then
    info "building ironcache (release) ..."
    ( cd "$REPO_ROOT" && CARGO_INCREMENTAL=0 cargo build --release -p ironcache ) || {
      err "cargo build failed"; exit 1;
    }
  fi
  info "binary: $BIN"
}

# ---------------------------------------------------------------------------- RESP probe (no redis-cli dep)
# Resolve a redis-cli once (preferred for the readiness/convergence probes). Empty if absent; we
# then fall back to a /dev/tcp reader. CI installs redis-tools, so the cli path is the norm.
REDIS_CLI="$(command -v redis-cli 2>/dev/null || command -v redis6-cli 2>/dev/null || true)"

# Run one RESP command and print the reply text. Uses redis-cli when present (robust framing),
# else a /dev/tcp fallback that reads the full available reply.
resp_probe() { # resp_probe <port> <ARG> [ARG...]  -> prints reply text
  local port="$1"; shift
  if [ -n "$REDIS_CLI" ]; then
    "$REDIS_CLI" -p "$port" "$@" 2>/dev/null
    return 0
  fi
  # Fallback: send the RESP array, then read everything until the socket goes quiet.
  local frame a
  frame="$(printf '*%d\r\n' "$#")"
  for a in "$@"; do frame+="$(printf '$%d\r\n%s\r\n' "${#a}" "$a")"; done
  exec 3<>"/dev/tcp/127.0.0.1/$port" 2>/dev/null || return 1
  printf '%b' "$frame" >&3
  local line out=""
  while IFS= read -r -t 1 -u 3 line; do out+="$line"$'\n'; done
  exec 3<&- 2>/dev/null; exec 3>&- 2>/dev/null
  printf '%s' "$out"
}

wait_for_ping() { # wait_for_ping <port> <timeout-secs>
  local port="$1" deadline=$(( $(date +%s) + ${2:-20} ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ -n "$REDIS_CLI" ]; then
      [ "$("$REDIS_CLI" -p "$port" PING 2>/dev/null)" = "PONG" ] && return 0
    elif printf 'PING\r\n' > "/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

# ---------------------------------------------------------------------------- boot: single node
SINGLE_DATA=""
boot_single() {
  SINGLE_DATA="$WORK_DIR/single"
  mkdir -p "$SINGLE_DATA"
  # SHARDS=1: IronCache is internally sharded thread-per-core, and MULTI/EXEC requires every queued
  # key to be on the connection's home shard (cross-shard transactions are by design unsupported).
  # A default multi-shard single node would abort a transaction whose keys land on a non-home shard,
  # which a client cannot control. Running the single node with one shard makes MULTI/EXEC
  # deterministic for the driver tests, while every other op-group is shard-count-agnostic. See
  # DRIVER_MATRIX.md "Findings" for the multi-shard transaction note.
  info "booting single-node ironcache on :$SINGLE_PORT (--shards 1 for deterministic MULTI/EXEC)"
  "$BIN" server --bind 127.0.0.1 --port "$SINGLE_PORT" --shards 1 \
      > "$WORK_DIR/single.log" 2>&1 &
  PIDS+=("$!")
  if ! wait_for_ping "$SINGLE_PORT" 25; then
    err "single-node never answered PING; log tail:"; tail -20 "$WORK_DIR/single.log" >&2; return 1
  fi
  info "single-node up (PING ok)"
}

# ---------------------------------------------------------------------------- boot: turnkey cluster
write_cluster_config() { # write_cluster_config <idx>
  local i="$1" cfg="$WORK_DIR/node$i.toml" n
  {
    echo '# generated by tests/drivers/run.sh (loopback turnkey cluster)'
    echo 'bind = "127.0.0.1"'
    echo "port = ${CLUSTER_PORTS[$i]}"
    echo 'cluster_enabled = true'
    echo 'cluster_mode = "raft"'
    echo "cluster_announce_id = \"${NODE_IDS[$i]}\""
    echo "data_dir = \"$WORK_DIR/node$i-data\""
    echo 'min_replicas_to_write = 0'
    for n in 0 1 2; do
      echo '[[cluster_topology.nodes]]'
      echo "id = \"${NODE_IDS[$n]}\""
      echo 'host = "127.0.0.1"'
      echo "port = ${CLUSTER_PORTS[$n]}"
      echo "slots = ${SLOT_RANGES[$n]}"
    done
  } > "$cfg"
  echo "$cfg"
}

cluster_state_ok() { # all three report cluster_state:ok + 16384 assigned + 3 known
  local p reply
  for p in "${CLUSTER_PORTS[@]}"; do
    reply="$(resp_probe "$p" CLUSTER INFO)"
    case "$reply" in
      *cluster_state:ok*) : ;;
      *) return 1 ;;
    esac
  done
  return 0
}

boot_cluster() {
  local i cfg
  # Clean any stale raft logs for these bus ports (temp-dir convention <temp>/ironcache-raft-<bus>.log).
  for i in 0 1 2; do
    local bus=$(( CLUSTER_PORTS[i] + 10000 ))
    rm -f "${TMPDIR:-/tmp}/ironcache-raft-$bus.log" \
          "${TMPDIR:-/tmp}/ironcache-raft-$bus.log.cfg" \
          "${TMPDIR:-/tmp}/ironcache-raft-$bus.log.snap" 2>/dev/null
    mkdir -p "$WORK_DIR/node$i-data"
  done
  info "booting turnkey 3-node cluster on :${CLUSTER_PORTS[0]} :${CLUSTER_PORTS[1]} :${CLUSTER_PORTS[2]}"
  for i in 0 1 2; do
    cfg="$(write_cluster_config "$i")"
    "$BIN" server --config "$cfg" > "$WORK_DIR/node$i.log" 2>&1 &
    PIDS+=("$!")
  done
  # Each node must answer PING first ...
  for i in 0 1 2; do
    if ! wait_for_ping "${CLUSTER_PORTS[$i]}" 25; then
      err "cluster node$i never answered PING; log tail:"; tail -20 "$WORK_DIR/node$i.log" >&2; return 1
    fi
  done
  # ... then the cluster must turnkey-converge to cluster_state:ok (auto, no MEET/ADDSLOTS).
  info "waiting for turnkey convergence to cluster_state:ok ..."
  local deadline=$(( $(date +%s) + 40 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if cluster_state_ok; then info "cluster converged: cluster_state:ok on all 3 nodes"; return 0; fi
    sleep 0.5
  done
  err "cluster did NOT reach cluster_state:ok within timeout"
  for i in 0 1 2; do echo "--- node$i CLUSTER INFO ---" >&2; resp_probe "${CLUSTER_PORTS[$i]}" CLUSTER INFO >&2; echo >&2; done
  return 1
}

# ---------------------------------------------------------------------------- run a client script
want() { case ",$DRIVERS," in *,"$1",*) return 0 ;; *) return 1 ;; esac; }

CLUSTER_CSV="127.0.0.1:${CLUSTER_PORTS[0]},127.0.0.1:${CLUSTER_PORTS[1]},127.0.0.1:${CLUSTER_PORTS[2]}"

# go must run inside its module dir so `go run .` resolves go.mod; collect its RESULT lines.
run_client_go() {
  info "=== running client: go ==="
  ( cd "$SCRIPT_DIR/go" && GOFLAGS=-mod=mod go run . \
      -single-port "$SINGLE_PORT" -cluster "$CLUSTER_CSV" ) \
      > "$WORK_DIR/go.out" 2> "$WORK_DIR/go.err"
  cat "$WORK_DIR/go.out"
  grep '^RESULT' "$WORK_DIR/go.out" 2>/dev/null | while IFS=' ' read -r _ c m g s d; do
    printf '%s\t%s\t%s\t%s\t%s\n' "$c" "$m" "$g" "$s" "$d" >> "$RESULTS_FILE"
  done
  [ -s "$WORK_DIR/go.err" ] && { warn "go stderr:"; sed 's/^/    /' "$WORK_DIR/go.err" >&2; }
}

run_client() { # run_client <name> <cmd...>
  local name="$1"; shift
  info "=== running client: $name ==="
  # The client script emits RESULT lines on stdout; tee them to a per-client log and the collector.
  if "$@" 2> "$WORK_DIR/$name.err" | tee "$WORK_DIR/$name.out"; then
    :
  fi
  # Collect RESULT lines regardless of exit code (a FAIL line is a result, not a harness error).
  grep '^RESULT' "$WORK_DIR/$name.out" 2>/dev/null | while IFS=' ' read -r _ c m g s d; do
    printf '%s\t%s\t%s\t%s\t%s\n' "$c" "$m" "$g" "$s" "$d" >> "$RESULTS_FILE"
  done
  if [ -s "$WORK_DIR/$name.err" ]; then warn "$name stderr:"; sed 's/^/    /' "$WORK_DIR/$name.err" >&2; fi
}

# ---------------------------------------------------------------------------- python (redis-py)
PY=""
setup_python() {
  command -v python3 >/dev/null 2>&1 || { warn "python3 not found -> skip redis-py"; return 1; }
  local venv="$WORK_DIR/venv"
  info "creating python venv + installing redis ..."
  if ! python3 -m venv "$venv" >/dev/null 2>&1; then warn "python venv failed -> skip redis-py"; return 1; fi
  PY="$venv/bin/python"
  if ! "$venv/bin/pip" install --quiet --disable-pip-version-check "redis>=5,<7" >/dev/null 2>&1; then
    warn "pip install redis failed -> trying --user";
    if ! python3 -m pip install --user --quiet "redis>=5,<7" >/dev/null 2>&1; then
      warn "could not install redis-py -> skip"; return 1
    fi
    PY="python3"
  fi
  "$PY" -c 'import redis; print("redis-py", redis.__version__)' >&2 || { warn "redis import failed -> skip"; return 1; }
  return 0
}

# ---------------------------------------------------------------------------- go (go-redis)
GO_OK=0
setup_go() {
  command -v go >/dev/null 2>&1 || { warn "go not found -> skip go-redis"; return 1; }
  info "fetching go-redis module deps ..."
  ( cd "$SCRIPT_DIR/go" && GOFLAGS=-mod=mod go mod tidy >/dev/null 2>&1 ) || {
    warn "go mod tidy failed -> skip go-redis"; return 1; }
  GO_OK=1; return 0
}

# ---------------------------------------------------------------------------- node (ioredis)
NODE_OK=0
setup_node() {
  command -v node >/dev/null 2>&1 || { warn "node not found -> skip ioredis"; return 1; }
  command -v npm  >/dev/null 2>&1 || { warn "npm not found -> skip ioredis"; return 1; }
  info "installing ioredis ..."
  ( cd "$SCRIPT_DIR/node" && npm install --silent --no-audit --no-fund ioredis >/dev/null 2>&1 ) || {
    warn "npm install ioredis failed -> skip ioredis"; return 1; }
  NODE_OK=1; return 0
}

# ---------------------------------------------------------------------------- the final matrix
print_matrix() {
  echo
  echo "================= DRIVER COMPATIBILITY MATRIX ================="
  if [ ! -s "$RESULTS_FILE" ]; then echo "(no results recorded)"; return; fi
  # Column header.
  printf '%-12s %-9s %-22s %-6s %s\n' CLIENT MODE OP-GROUP RESULT DETAIL
  printf '%-12s %-9s %-22s %-6s %s\n' "------" "----" "--------" "------" "------"
  local pass=0 fail=0 line
  while IFS=$'\t' read -r c m g s d; do
    printf '%-12s %-9s %-22s %-6s %s\n' "$c" "$m" "$g" "$s" "$d"
    case "$s" in PASS) pass=$((pass+1)) ;; FAIL) fail=$((fail+1)) ;; esac
  done < "$RESULTS_FILE"
  echo "--------------------------------------------------------------"
  echo "TOTAL: $pass PASS, $fail FAIL"
  echo "=============================================================="
  [ "$fail" -eq 0 ]
}

# ============================================================================ MAIN
main() {
  resolve_binary

  boot_single  || { err "single-node boot failed"; exit 1; }
  boot_cluster || { err "cluster boot failed";    exit 1; }

  # Setup + run each requested + available client.
  if want python; then
    if setup_python; then
      run_client python "$PY" "$SCRIPT_DIR/python/driver_compat.py" \
        --single-port "$SINGLE_PORT" --cluster "$CLUSTER_CSV"
    else
      record redis-py single all SKIP "python/redis-py unavailable"
      record redis-py cluster all SKIP "python/redis-py unavailable"
    fi
  fi

  if want go; then
    if setup_go; then
      run_client_go
    else
      record go-redis single all SKIP "go/go-redis unavailable"
      record go-redis cluster all SKIP "go/go-redis unavailable"
    fi
  fi

  if want node; then
    if setup_node; then
      run_client node node "$SCRIPT_DIR/node/driver_compat.js" \
        --single-port "$SINGLE_PORT" --cluster "$CLUSTER_CSV"
    else
      record ioredis single all SKIP "node/ioredis unavailable"
      record ioredis cluster all SKIP "node/ioredis unavailable"
    fi
  fi

  print_matrix
}

main
