#!/usr/bin/env bash
# #630 CLUSTER ROLLING-UPGRADE DOCKER SMOKE: proves `ironcache upgrade --cluster` end to end on a
# live 3-node raft cluster under write load -- replica-first, RPO=0 failover-freeze, primary LAST.
#
# It asserts the four properties the decomposed CI + DST cannot (they stub the live signals):
#   1. the driver reports SUCCESS,
#   2. EVERY node (incl. the OLD PRIMARY) actually reaches the target version (regression gate for
#      #733: the old primary must not be silently left on the old binary),
#   3. ZERO acked-write loss across the roll (RPO=0),
#   4. ZERO writer outages during the roll (zero-downtime).
#
# Prereqs: a Linux docker engine + `./build.sh` already run (populates bin/). On Apple Silicon use
# colima/lima and set IC_DOCKER_BIN to your docker-CLI dir if it is not already on PATH.
# Usage: ./smoke.sh   (self-contained; tears everything down on exit)
set -euo pipefail
export PATH="${IC_DOCKER_BIN:+$IC_DOCKER_BIN:}$PATH"
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE"

COMPOSE=(docker compose --env-file versions.env -f docker-compose.smoke.yml)
NET=ic630_default
ID1=1111111111111111111111111111111111111111
ID3=3333333333333333333333333333333333333333
rc() { docker run --rm --network "$NET" redis:7-alpine redis-cli -h "$1" -p 6379 "${@:2}" 2>&1 | tr -d '\r'; }

cleanup() {
  docker rm -f ic630-load >/dev/null 2>&1 || true
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

# --- 0. prereqs -----------------------------------------------------------------------------------
for v in v1 v2; do [ -x "bin/ironcache-$v" ] || fail "missing bin/ironcache-$v -- run ./build.sh first"; done
docker image inspect ic630-driver >/dev/null 2>&1 || fail "missing ic630-driver image -- run ./build.sh first"
cp versions.env.example versions.env
mkdir -p out

# --- 1. bring up the cluster on v1, form the raft quorum ------------------------------------------
echo "== bringing up the 3-node cluster (all v1) =="
"${COMPOSE[@]}" up -d
for _ in $(seq 1 20); do
  [ "$("${COMPOSE[@]}" ps --format '{{.Health}}' 2>/dev/null | grep -c healthy)" = 3 ] && break; sleep 3
done
for _ in $(seq 1 12); do rc ironcache-1 CLUSTER INFO | grep -q cluster_state:ok && break; sleep 3; done
rc ironcache-1 CLUSTER INFO | grep -q cluster_state:ok || fail "cluster never reached cluster_state:ok"

# --- 2. attach node-3 as a replica of node-1's slots ---------------------------------------------
echo "== attaching node-3 as a replica of node-1 =="
rc ironcache-1 CLUSTER REPLICATE "$ID3" $(seq 0 16383) >/dev/null 2>&1 || true
for _ in $(seq 1 10); do rc ironcache-1 INFO replication | grep -q connected_slaves:1 && break; sleep 2; done
rc ironcache-1 INFO replication | grep -q connected_slaves:1 || fail "node-3 never attached as a replica"
echo "  versions before roll: node-1=$(rc ironcache-1 INFO server | grep -i ironcache_version) node-3=$(rc ironcache-3 INFO server | grep -i ironcache_version)"

# --- 3. start the load writer (acked writes + outage log), continuous across the roll -------------
echo "== starting the load writer =="
docker run -d --name ic630-load --network "$NET" -e DURATION="${DURATION:-90}" \
  -v "$HERE/load.sh":/load.sh -v "$HERE/out":/out redis:7-alpine sh /load.sh >/dev/null

# --- 4. RUN THE ROLL: ironcache upgrade --cluster, in-network, via the CommandUpgrader actuator ---
echo "== running: ironcache upgrade --cluster (v1 -> v2, replica-first, RPO=0 fence) =="
driver_rc=0
docker run --rm --network "$NET" \
  -v /var/run/docker.sock:/var/run/docker.sock -v "$HERE":"$HERE" \
  -v "$HERE/bin/ironcache-v2":/usr/local/bin/ironcache -w "$HERE" \
  ic630-driver ironcache upgrade --cluster --inventory inventory.toml --to 2.0.0 \
    --actuator-command "bash $HERE/recreate.sh {target}" || driver_rc=$?
[ "$driver_rc" = 0 ] || fail "the driver exited non-zero ($driver_rc)"

# --- 5. stop the writer, verify zero loss --------------------------------------------------------
docker wait ic630-load >/dev/null 2>&1 || true
writer_line=$(docker logs ic630-load 2>&1 | grep '^writer:' | tail -1 || true)
outages=$(printf '%s' "$writer_line" | sed -n 's/.*outages=\([0-9]*\).*/\1/p')
echo "  $writer_line"
verify_line=$(docker run --rm --network "$NET" -v "$HERE/verify.sh":/verify.sh -v "$HERE/out":/out \
  redis:7-alpine sh /verify.sh | grep '^verify:' | tail -1)
lost=$(printf '%s' "$verify_line" | sed -n 's/.*lost=\([0-9]*\).*/\1/p')
echo "  $verify_line"

# --- 6. assert the four properties ---------------------------------------------------------------
v1=$(rc ironcache-1 INFO server | sed -n 's/.*ironcache_version:\([0-9.]*\).*/\1/p' | tr -d '\r')
v3=$(rc ironcache-3 INFO server | sed -n 's/.*ironcache_version:\([0-9.]*\).*/\1/p' | tr -d '\r')
echo "== post-roll: node-1=$v1 node-3=$v3 lost=${lost:-?} outages=${outages:-?} =="
[ "$v1" = 2.0.0 ] || fail "node-1 (old primary) not on target: $v1 (regression of #733)"
[ "$v3" = 2.0.0 ] || fail "node-3 not on target: $v3"
[ "${lost:-1}" = 0 ] || fail "acked-write loss: lost=$lost (RPO>0)"
[ "${outages:-1}" = 0 ] || fail "writer outages during the roll: $outages (not zero-downtime)"

echo "SMOKE PASS: rolling upgrade v1->v2 complete -- every node on target, 0 acked-write loss, 0 outages."
