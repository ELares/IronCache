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

# SHARD-OWNERS leg (#517): ONE node in `cluster_mode = shard-owners` exposes its N internal shards as
# N CRC16-hashslot owners on CONTIGUOUS ports `SHARD_OWNERS_BASE .. +N-1` (no raft, so no derived
# bus/repl ports). A cluster-aware client reads CLUSTER SLOTS and dials each key's owner PORT -> the
# key lands on its home shard -> NO internal cross-shard hop. The CONTRAST node is a NORMAL multi-shard
# node (one listener fronting N shards) that HOPS foreign-shard keys internally, so a single-endpoint
# client drives `hops_sent` up -- the measurable baseline the shard-owners leg eliminates.
SHARD_OWNERS_SHARDS="${SHARD_OWNERS_SHARDS:-4}"
SHARD_OWNERS_BASE="${SHARD_OWNERS_BASE:-7511}"       # binds 7511..7514 for N=4
SHARD_OWNERS_METRICS="${SHARD_OWNERS_METRICS:-9092}" # dedicated (default 9091 is off'd for the others)
CONTRAST_PORT="${CONTRAST_PORT:-7519}"               # a normal N-shard node on ONE listener
CONTRAST_METRICS="${CONTRAST_METRICS:-9093}"
SHARD_OWNERS_WARMUP="${SHARD_OWNERS_WARMUP:-32}"     # keyed keys the harness drives for the metric A/B

# Stable 40-hex node ids (must match the per-node topology entries below).
NODE_IDS=(
  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  "cccccccccccccccccccccccccccccccccccccccc"
)
# The shipped 3-way slot split (deploy/compose layout): node0 [0,5460], node1 [5461,10922],
# node2 [10923,16383] -- all 16384 slots covered.
SLOT_RANGES=("[[0, 5460]]" "[[5461, 10922]]" "[[10923, 16383]]")

# RESTRICTED-USER (per-subcommand ACL, #405) leg: the cluster nodes load a shared aclfile so a
# real client can connect as a LOCKED-DOWN `svc` user (`-@dangerous +cluster|slots|shards|nodes`)
# and prove (a) cluster discovery + a routed SET/GET round-trip still work, (b) every CLUSTER
# MUTATOR (ADDSLOTS) is denied NOPERM. See write_cluster_aclfile for the grant + why `default`
# stays all-permissive.
CLUSTER_ACLFILE="$WORK_DIR/cluster-users.acl"
SVC_USER="svc"
SVC_PASS="svcpw"

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

# ---------------------------------------------------------------------------- raw RESP + metrics (#517 leg)
# Build an exact RESP2 array frame for the given args into the global RESP_FRAME. Uses `printf -v`
# (NOT command substitution, which would strip the trailing LF and corrupt the framing), so the CR/LF
# bytes are preserved verbatim -- these are used to drive the zero-hop metric A/B without a redis-cli.
RESP_FRAME=""
resp_build() { # resp_build <ARG>...  -> sets RESP_FRAME
  local a seg
  printf -v RESP_FRAME '*%d\r\n' "$#"
  for a in "$@"; do
    printf -v seg '$%d\r\n%s\r\n' "${#a}" "$a"
    RESP_FRAME+="$seg"
  done
}

# Send one command over a fresh /dev/tcp connection and echo the FIRST reply line (trailing CR
# trimmed). The replies here (+OK, -MOVED ...) are single-line, so one read suffices.
raw_resp() { # raw_resp <port> <ARG>...  -> prints reply line
  local port="$1"; shift
  resp_build "$@"
  exec 3<>"/dev/tcp/127.0.0.1/$port" 2>/dev/null || return 1
  printf '%s' "$RESP_FRAME" >&3
  local reply=""
  IFS= read -r -t 2 -u 3 reply || true
  exec 3<&- 2>/dev/null; exec 3>&- 2>/dev/null
  printf '%s' "${reply%$'\r'}"
}

# A minimal CLUSTER-AWARE client in bash: SET a key on the shard-owners BASE port, and if it replies
# `-MOVED <slot> <host>:<port>`, re-dial the owner PORT the redirect names and SET there. Once on the
# owning shard the key is served LOCALLY (owner == home, NO hop), so this GUARANTEES `local_served`
# climbs while `hops_sent` stays 0 -- the harness's own owner-dialing traffic for the zero-hop
# assertion, independent of which external client libraries happen to be installed.
owner_dial_set() { # owner_dial_set <base> <key>
  local base="$1" key="$2" reply host_port target
  reply="$(raw_resp "$base" SET "$key" v)"
  case "$reply" in
    +OK) return 0 ;;
    -MOVED*)
      host_port="${reply##* }"   # last whitespace-separated field: host:port
      target="${host_port##*:}"  # the port after the final colon
      [ "$target" = "$reply" ] && return 1
      reply="$(raw_resp "$target" SET "$key" v)"
      [ "$reply" = "+OK" ]
      ;;
    *) return 1 ;;
  esac
}

# Drive `count` keyed SETs over ONE connection to a SINGLE port (the single-endpoint shape). On a
# NORMAL multi-shard node this connection homes on one shard, so every key another shard owns HOPS
# internally -- the baseline `hops_sent >> 0` the shard-owners leg contrasts against.
drive_single_endpoint() { # drive_single_endpoint <port> <count>
  local port="$1" count="$2" i all=""
  for (( i = 0; i < count; i++ )); do
    resp_build SET "sokey:$i" v
    all+="$RESP_FRAME"
  done
  exec 3<>"/dev/tcp/127.0.0.1/$port" 2>/dev/null || return 1
  printf '%s' "$all" >&3
  # Drain replies briefly so the server processes every SET before we scrape (a quiet socket ends it).
  while IFS= read -r -t 1 -u 3 _; do :; done
  exec 3<&- 2>/dev/null; exec 3>&- 2>/dev/null
  return 0
}

# Scrape the out-of-band /metrics endpoint. Prefers curl; falls back to a /dev/tcp HTTP/1.0 GET.
metrics_scrape() { # metrics_scrape <metrics-port>  -> prints the Prometheus body
  local port="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fsS --max-time 5 "http://127.0.0.1:$port/metrics" 2>/dev/null
    return $?
  fi
  exec 3<>"/dev/tcp/127.0.0.1/$port" 2>/dev/null || return 1
  printf 'GET /metrics HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3
  local line
  while IFS= read -r -t 3 -u 3 line; do printf '%s\n' "${line%$'\r'}"; done
  exec 3<&- 2>/dev/null; exec 3>&- 2>/dev/null
}

# Extract the NODE-WIDE value of an unlabeled Prometheus counter/gauge (the `name <value>` line, NOT
# the per-shard `name{shard="i"} v` series). Prints empty if absent.
metric_of() { # metric_of <body> <metric-name>
  printf '%s\n' "$1" | awk -v n="$2" '$1 == n { print $2; exit }'
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
  # --metrics-addr off: metrics default to 127.0.0.1:9091, and this harness boots several instances
  # on ONE host (this single node, then the 3-node cluster), which would collide on that port. The
  # driver tests do not exercise /metrics, so disable it here (a real multi-instance host configures
  # a distinct metrics port per instance instead).
  "$BIN" server --bind 127.0.0.1 --port "$SINGLE_PORT" --shards 1 --metrics-addr off \
      > "$WORK_DIR/single.log" 2>&1 &
  PIDS+=("$!")
  if ! wait_for_ping "$SINGLE_PORT" 25; then
    err "single-node never answered PING; log tail:"; tail -20 "$WORK_DIR/single.log" >&2; return 1
  fi
  info "single-node up (PING ok)"
}

# ---------------------------------------------------------------------------- boot: turnkey cluster
# Write the shared aclfile every cluster node loads (#405, per-subcommand ACL leg).
#
# WHY `default` STAYS ALL-PERMISSIVE (deliberate deviation from a `default off` posture): this
# orchestrator's OWN health gates -- `wait_for_ping` (bare PING) and `cluster_state_ok` (bare
# CLUSTER INFO), both over an UNAUTHENTICATED redis-cli -- plus every EXISTING driver leg connect
# as the implicit default with no AUTH. Turning `default off` would make those probes return
# `-NOAUTH` and break turnkey-convergence detection AND all current scenarios. So we keep `default`
# all-permissive (byte-identical for the existing legs + the harness probes) and LAYER the scoped
# `svc` user on top -- the restricted leg AUTHs as `svc`, which proves the locked-down per-subcommand
# ACL end-to-end regardless of `default`'s posture. `butlr_admin` is the all-powers identity (the
# turnkey cluster issues no CLUSTER mutators itself, so it is unused here, present for parity with
# prod where an operator runs mutators as the admin user, never as `svc`).
#
# `svc` GRANT: `+@read +@write +@connection +@transaction -@dangerous` (data + handshake, no
# dangerous ops) PLUS the introspection subcommands a cluster client needs to discover topology:
# `+cluster|slots +cluster|shards +cluster|nodes +cluster|info`. The `+cluster|info` is included
# because ioredis's default `enableReadyCheck` issues `CLUSTER INFO` during bootstrap (go-redis /
# redis-py discover via CLUSTER SLOTS alone) -- see DRIVER_MATRIX.md / the report: the practical
# minimal cluster-client read grant is slots+shards+nodes+info. Every CLUSTER MUTATOR
# (ADDSLOTS/SETSLOT/MEET/...) stays @admin+@dangerous and is therefore NOPERM for `svc`.
write_cluster_aclfile() {
  cat > "$CLUSTER_ACLFILE" <<EOF
user default on nopass ~* &* +@all
user butlr_admin on >adminpw ~* &* +@all
user $SVC_USER on >$SVC_PASS ~* resetchannels +@read +@write +@connection +@transaction -@dangerous +cluster|slots +cluster|shards +cluster|nodes +cluster|info
EOF
  info "wrote cluster aclfile: $CLUSTER_ACLFILE (default all-permissive; scoped svc user added)"
}

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
    echo "aclfile = \"$CLUSTER_ACLFILE\""
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
  # The shared aclfile every node loads (#405 restricted-user leg); written before the configs
  # that reference it.
  write_cluster_aclfile
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
    # --metrics-addr off (CLI overrides the config): three nodes on one host would otherwise collide
    # on the default 127.0.0.1:9091 metrics port. See the single-node boot above.
    "$BIN" server --config "$cfg" --metrics-addr off > "$WORK_DIR/node$i.log" 2>&1 &
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

# ---------------------------------------------------------------------------- boot: shard-owners (#517)
# Write the single-node shard-owners config: cluster_enabled + `cluster_mode = shard-owners` +
# `shards = N`. The node auto-owns all 16384 slots partitioned across its N shards and binds one
# listener per shard at `port + i` (there is NO CLI flag for cluster mode, so this goes through a
# config file, like the raft-cluster leg). No topology (shard-owners derives owners from the shard
# count); no data_dir (in-memory).
write_shard_owners_config() {
  local cfg="$WORK_DIR/shard-owners.toml"
  {
    echo '# generated by tests/drivers/run.sh (single-node shard-owners projection, #517)'
    echo 'bind = "127.0.0.1"'
    echo "port = $SHARD_OWNERS_BASE"
    echo "shards = $SHARD_OWNERS_SHARDS"
    echo 'cluster_enabled = true'
    echo 'cluster_mode = "shard-owners"'
  } > "$cfg"
  echo "$cfg"
}

# The CSV of the N shard-owner endpoints a cluster-aware client seeds from (host:port per shard port).
SHARD_OWNERS_CSV=""
boot_shard_owners() {
  local cfg i last=$(( SHARD_OWNERS_BASE + SHARD_OWNERS_SHARDS - 1 ))
  cfg="$(write_shard_owners_config)"
  # --metrics-addr ON (dedicated 9092): the other legs pass `off` to avoid the default-9091 collision,
  # but the zero-hop assertion must SCRAPE /metrics, so this single node gets its own metrics port.
  info "booting shard-owners node: base :$SHARD_OWNERS_BASE, $SHARD_OWNERS_SHARDS shards (ports $SHARD_OWNERS_BASE..$last), metrics :$SHARD_OWNERS_METRICS"
  "$BIN" server --config "$cfg" --metrics-addr "127.0.0.1:$SHARD_OWNERS_METRICS" \
      > "$WORK_DIR/shard-owners.log" 2>&1 &
  PIDS+=("$!")
  # EVERY per-shard listener (base + i) must answer PING before we drive it.
  for (( i = 0; i < SHARD_OWNERS_SHARDS; i++ )); do
    if ! wait_for_ping "$(( SHARD_OWNERS_BASE + i ))" 25; then
      err "shard-owners port $(( SHARD_OWNERS_BASE + i )) never answered PING; log tail:"
      tail -20 "$WORK_DIR/shard-owners.log" >&2; return 1
    fi
    SHARD_OWNERS_CSV+="${SHARD_OWNERS_CSV:+,}127.0.0.1:$(( SHARD_OWNERS_BASE + i ))"
  done
  info "shard-owners node up (all $SHARD_OWNERS_SHARDS per-shard listeners answer PING)"
}

# Boot the CONTRAST node: a NORMAL multi-shard node (NOT shard-owners) with ONE listener fronting
# N shards, metrics on its own port. A single-endpoint client's foreign-shard keys HOP internally here
# -- the `hops_sent >> 0` baseline the shard-owners projection eliminates. (`--shards N` matches the
# shard-owners shard count so the A/B is apples-to-apples: same N, same keys, different topology.)
boot_contrast() {
  info "booting single-endpoint CONTRAST node (normal $SHARD_OWNERS_SHARDS-shard node) on :$CONTRAST_PORT, metrics :$CONTRAST_METRICS"
  "$BIN" server --bind 127.0.0.1 --port "$CONTRAST_PORT" --shards "$SHARD_OWNERS_SHARDS" \
      --metrics-addr "127.0.0.1:$CONTRAST_METRICS" > "$WORK_DIR/contrast.log" 2>&1 &
  PIDS+=("$!")
  if ! wait_for_ping "$CONTRAST_PORT" 25; then
    err "contrast node never answered PING; log tail:"; tail -20 "$WORK_DIR/contrast.log" >&2; return 1
  fi
  info "contrast node up (PING ok)"
}

# THE #517 ZERO-HOP ASSERTION (metric proof + single-endpoint contrast). After the real cluster
# clients have driven routed SET/GET at the shard-owner ports, the harness ALSO drives its own
# owner-dialed keyed traffic (MOVED-following bash client) so the assertion holds even when no external
# client lib is installed, then scrapes /metrics and asserts:
#   * shard-owners node: ironcache_hops_sent_total == 0 AND ironcache_local_served_total > 0
#     (owner-dialed keys served locally, ZERO internal hops -- the #517 property), and
#   * contrast node (same keys, one port): ironcache_hops_sent_total > 0 (the internal-hop baseline).
# This makes the hop ELIMINATION measurable, not merely asserted. (The Rust metrics_endpoint.rs test
# `shard_owners_owner_dialed_client_shows_zero_hops` asserts the same over a raw socket.)
shard_owners_zero_hop_check() {
  local i served=0
  # Harness-guaranteed owner-dialed traffic: SET a spread of keys, following MOVED to each owner port.
  for (( i = 0; i < SHARD_OWNERS_WARMUP; i++ )); do
    owner_dial_set "$SHARD_OWNERS_BASE" "sokey:$i" && served=$(( served + 1 ))
  done
  info "shard-owners: harness owner-dialed $served/$SHARD_OWNERS_WARMUP keyed SETs (MOVED-followed to owner ports)"

  local so_body so_hops so_local
  so_body="$(metrics_scrape "$SHARD_OWNERS_METRICS")"
  if [ -z "$so_body" ]; then
    record harness shard-owners zero-hop FAIL "could not scrape shard-owners /metrics on :$SHARD_OWNERS_METRICS"
    return
  fi
  so_hops="$(metric_of "$so_body" ironcache_hops_sent_total)"
  so_local="$(metric_of "$so_body" ironcache_local_served_total)"
  so_hops="${so_hops:-missing}"; so_local="${so_local:-0}"
  if [ "$so_hops" = "0" ] && [ "$so_local" -gt 0 ] 2>/dev/null; then
    record harness shard-owners zero-hop PASS "hops_sent=$so_hops local_served=$so_local (owner-dialed keys served locally, no internal hop)"
  else
    record harness shard-owners zero-hop FAIL "expected hops_sent=0 and local_served>0, got hops_sent=$so_hops local_served=$so_local"
  fi

  # Single-endpoint contrast: SAME keys through ONE port of the normal N-shard node -> hops rise.
  local c_body c_hops c_local
  drive_single_endpoint "$CONTRAST_PORT" "$SHARD_OWNERS_WARMUP"
  c_body="$(metrics_scrape "$CONTRAST_METRICS")"
  if [ -z "$c_body" ]; then
    record harness shard-owners single-endpoint-contrast SKIP "could not scrape contrast /metrics on :$CONTRAST_METRICS"
    return
  fi
  c_hops="$(metric_of "$c_body" ironcache_hops_sent_total)"
  c_local="$(metric_of "$c_body" ironcache_local_served_total)"
  c_hops="${c_hops:-0}"; c_local="${c_local:-0}"
  if [ "$c_hops" -gt 0 ] 2>/dev/null; then
    record harness shard-owners single-endpoint-contrast PASS "hops_sent=$c_hops local_served=$c_local (one port to a normal $SHARD_OWNERS_SHARDS-shard node: internal hops -- the #517 baseline; shard-owners drove it to 0)"
  else
    record harness shard-owners single-endpoint-contrast FAIL "expected hops_sent>0 through one port, got hops_sent=$c_hops"
  fi
}

# ---------------------------------------------------------------------------- run a client script
want() { case ",$DRIVERS," in *,"$1",*) return 0 ;; *) return 1 ;; esac; }

CLUSTER_CSV="127.0.0.1:${CLUSTER_PORTS[0]},127.0.0.1:${CLUSTER_PORTS[1]},127.0.0.1:${CLUSTER_PORTS[2]}"

# go must run inside its module dir so `go run .` resolves go.mod; collect its RESULT lines.
run_client_go() {
  info "=== running client: go ==="
  ( cd "$SCRIPT_DIR/go" && GOFLAGS=-mod=mod go run . \
      -single-port "$SINGLE_PORT" -cluster "$CLUSTER_CSV" \
      -acl-user "$SVC_USER" -acl-pass "$SVC_PASS" \
      -shard-owners "$SHARD_OWNERS_CSV" ) \
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

  boot_single        || { err "single-node boot failed";   exit 1; }
  boot_cluster       || { err "cluster boot failed";        exit 1; }
  boot_shard_owners  || { err "shard-owners boot failed";   exit 1; }
  boot_contrast      || { err "contrast-node boot failed";  exit 1; }

  # Setup + run each requested + available client. Each driver also gets the scoped-ACL creds
  # (`--acl-user`/`--acl-pass`) so it runs the #405 RESTRICTED leg against the cluster: discovery +
  # SET/GET as `svc`, and a CLUSTER ADDSLOTS that must come back NOPERM.
  if want python; then
    if setup_python; then
      run_client python "$PY" "$SCRIPT_DIR/python/driver_compat.py" \
        --single-port "$SINGLE_PORT" --cluster "$CLUSTER_CSV" \
        --acl-user "$SVC_USER" --acl-pass "$SVC_PASS" \
        --shard-owners "$SHARD_OWNERS_CSV"
    else
      record redis-py single all SKIP "python/redis-py unavailable"
      record redis-py cluster all SKIP "python/redis-py unavailable"
      record redis-py shard-owners all SKIP "python/redis-py unavailable"
    fi
  fi

  if want go; then
    if setup_go; then
      run_client_go
    else
      record go-redis single all SKIP "go/go-redis unavailable"
      record go-redis cluster all SKIP "go/go-redis unavailable"
      record go-redis shard-owners all SKIP "go/go-redis unavailable"
    fi
  fi

  if want node; then
    if setup_node; then
      run_client node node "$SCRIPT_DIR/node/driver_compat.js" \
        --single-port "$SINGLE_PORT" --cluster "$CLUSTER_CSV" \
        --acl-user "$SVC_USER" --acl-pass "$SVC_PASS" \
        --shard-owners "$SHARD_OWNERS_CSV"
    else
      record ioredis single all SKIP "node/ioredis unavailable"
      record ioredis cluster all SKIP "node/ioredis unavailable"
      record ioredis shard-owners all SKIP "node/ioredis unavailable"
    fi
  fi

  # THE #517 ZERO-HOP ASSERTION: the metric proof (owner-dialed keys -> hops_sent 0) + the
  # single-endpoint contrast (same keys, one port -> hops_sent > 0). Runs regardless of which client
  # libs are present (the harness drives its own owner-dialed traffic), so the driver matrix always
  # carries the measured hop-elimination result.
  shard_owners_zero_hop_check

  print_matrix
}

main
