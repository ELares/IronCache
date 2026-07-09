#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# MIXED-VERSION compatibility SMOKE (#527, rolling-upgrade + #391/#392 upgrade epic).
#
# The load-bearing direction for a rolling upgrade is FORWARD-LOAD: a NEW binary (vN, HEAD) must
# load a snapshot a PREVIOUS binary (vN-1, the latest release) wrote. If the on-disk snapshot format
# silently drifts, a rolling upgrade would start the new node with an EMPTY keyspace (or refuse to
# boot). This script proves the forward-load direction end to end, plus a small RESP wire smoke, so
# a format regression is caught in CI rather than in a live upgrade.
#
# What it does:
#   1. boot OLD (vN-1) with a data_dir, seed a representative multi-type dataset, SAVE, stop it;
#   2. assert the on-disk snapshot files exist (dump.manifest + per-shard dump-shard-*.icss);
#   3. boot NEW (vN, HEAD) on the SAME data_dir (load-on-boot), wait until /readyz reports every
#      shard finished loading, then read every seeded key back and assert the values MATCH;
#   4. run a handful of representative RESP commands against BOTH servers as a wire smoke.
#
# SCOPE (deliberately BOUNDED, this is a smoke not the full suite): a curated set of the core value
# types (string, integer-shaped string, large string, TTL, list, hash, set, zset) round-tripped
# across versions. The DEEP cross-version RESP surface (every command, RESP2 vs RESP3, a vN client
# against a vN-1 server for the whole command table) is DEFERRED; the differential gate already pins
# an exact Redis and asserts the RESP surface per version, so the highest-value cross-VERSION check
# here is the snapshot forward-load that no other gate covers.
#
# Usage:
#   OLD_IRONCACHE_BIN=/path/to/old/ironcache NEW_IRONCACHE_BIN=/path/to/new/ironcache \
#     scripts/ci/mixed-version-compat.sh
#   (or positionally: scripts/ci/mixed-version-compat.sh OLD_BIN NEW_BIN)
#
# A local self-test passes the SAME HEAD binary as both OLD and NEW: it exercises the whole
# save -> reload -> readback path (proving the script + the snapshot round-trip), while the REAL
# cross-version guarantee can only be confirmed once CI runs it with an actual prior-release binary.
set -euo pipefail

OLD_BIN="${1:-${OLD_IRONCACHE_BIN:-}}"
NEW_BIN="${2:-${NEW_IRONCACHE_BIN:-}}"

if [ -z "${OLD_BIN}" ] || [ -z "${NEW_BIN}" ]; then
  echo "usage: OLD_IRONCACHE_BIN=... NEW_IRONCACHE_BIN=... $0 (or: $0 OLD_BIN NEW_BIN)" >&2
  exit 2
fi
for b in "${OLD_BIN}" "${NEW_BIN}"; do
  if [ ! -x "${b}" ]; then echo "not an executable: ${b}" >&2; exit 2; fi
done
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required (the RESP client is a tiny embedded python3 script)" >&2
  exit 2
fi

echo "OLD (vN-1): ${OLD_BIN}"
"${OLD_BIN}" --version || true
echo "NEW (vN)  : ${NEW_BIN}"
"${NEW_BIN}" --version || true

WORK="$(mktemp -d)"
DATA_DIR="${WORK}/data"
mkdir -p "${DATA_DIR}"
# A tiny, dependency-free RESP2 client: connect, send ONE command array, print the reply in a
# normalized textual form (OK/PONG/int/bulk/array), exit 3 on an unexpected RESP error, 4 on a
# connection failure. Kept in a temp file so the orchestration below stays readable.
RESP_PY="${WORK}/resp.py"
cat > "${RESP_PY}" <<'PY'
import socket, sys

def read_reply(f):
    line = f.readline()
    if not line:
        raise SystemExit(4)
    t, rest = line[:1], line[1:].rstrip(b"\r\n")
    if t == b"+":
        return rest.decode("utf-8", "replace")
    if t == b"-":
        sys.stderr.write("RESP error: " + rest.decode("utf-8", "replace") + "\n")
        raise SystemExit(3)
    if t == b":":
        return rest.decode()
    if t == b"$":
        n = int(rest)
        if n < 0:
            return "(nil)"
        data = b""
        while len(data) < n:
            chunk = f.read(n - len(data))
            if not chunk:
                raise SystemExit(4)
            data += chunk
        f.read(2)  # trailing CRLF
        return data.decode("utf-8", "replace")
    if t == b"*":
        n = int(rest)
        if n < 0:
            return "(nil)"
        return "\n".join(read_reply(f) for _ in range(n))
    raise SystemExit("unexpected RESP type: %r" % t)

def main():
    host, port = sys.argv[1], int(sys.argv[2])
    args = [a.encode() for a in sys.argv[3:]]
    frame = b"*%d\r\n" % len(args)
    for a in args:
        frame += b"$%d\r\n%s\r\n" % (len(a), a)
    try:
        s = socket.create_connection((host, port), timeout=5)
    except OSError:
        raise SystemExit(4)
    with s:
        s.sendall(frame)
        f = s.makefile("rb")
        sys.stdout.write(read_reply(f))

main()
PY

resp() { python3 "${RESP_PY}" 127.0.0.1 "$@"; }

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if [ -n "${pid}" ]; then kill "${pid}" 2>/dev/null || true; fi
  done
  rm -rf "${WORK}"
}
trap cleanup EXIT

# Grab a free TCP port by binding :0 and releasing it (racy in theory, fine for a CI smoke).
free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# Poll RESP PING until the server answers PONG (it accepts connections only after its shards bind).
wait_ping() {
  local port="$1"
  for _ in $(seq 1 150); do
    if [ "$(resp "${port}" PING 2>/dev/null || true)" = "PONG" ]; then return 0; fi
    sleep 0.2
  done
  echo "server on :${port} never answered PING" >&2
  return 1
}

# Poll /readyz until HTTP 200: on a load-on-boot restart this flips to ready ONLY after EVERY shard
# has finished loading its snapshot, so it is the correct gate before reading a reloaded keyspace.
wait_readyz() {
  local mport="$1"
  for _ in $(seq 1 150); do
    if curl -fsS -o /dev/null "http://127.0.0.1:${mport}/readyz" 2>/dev/null; then return 0; fi
    sleep 0.2
  done
  echo "NEW server /readyz on :${mport} never reported ready" >&2
  return 1
}

# The representative corpus. Each entry is a set of write commands; the checks run after reload.
BIG_VALUE="$(python3 -c 'print("x" * 512)')"

seed_dataset() {
  local port="$1"
  resp "${port}" SET compat:str "hello world" >/dev/null
  resp "${port}" SET compat:int 1234567890 >/dev/null
  resp "${port}" SET compat:big "${BIG_VALUE}" >/dev/null
  # A comfortably-long TTL so it is still positive after the reload.
  resp "${port}" SET compat:ttl persists EX 100000 >/dev/null
  resp "${port}" RPUSH compat:list a b c >/dev/null
  resp "${port}" HSET compat:hash f1 v1 f2 v2 >/dev/null
  resp "${port}" SADD compat:set x y z >/dev/null
  resp "${port}" ZADD compat:zset 1 one 2 two >/dev/null
}

FAILED=0
check() {
  local what="$1" got="$2" want="$3"
  if [ "${got}" = "${want}" ]; then
    echo "  ok   ${what}: '${got}'"
  else
    echo "  FAIL ${what}: got '${got}' want '${want}'" >&2
    FAILED=1
  fi
}

verify_dataset() {
  local port="$1"
  check "GET compat:str"        "$(resp "${port}" GET compat:str)"        "hello world"
  check "GET compat:int"        "$(resp "${port}" GET compat:int)"        "1234567890"
  check "GET compat:big"        "$(resp "${port}" GET compat:big)"        "${BIG_VALUE}"
  check "LRANGE compat:list"    "$(resp "${port}" LRANGE compat:list 0 -1)" "$(printf 'a\nb\nc')"
  check "HGET compat:hash f1"   "$(resp "${port}" HGET compat:hash f1)"   "v1"
  check "HGET compat:hash f2"   "$(resp "${port}" HGET compat:hash f2)"   "v2"
  check "SISMEMBER compat:set y" "$(resp "${port}" SISMEMBER compat:set y)" "1"
  check "ZSCORE compat:zset two" "$(resp "${port}" ZSCORE compat:zset two)" "2"
  # TTL must be a positive integer (the key persisted WITH its expiry across the version boundary):
  # -2 = missing key, -1 = no TTL, both are failures here.
  local ttl
  ttl="$(resp "${port}" TTL compat:ttl)"
  if [ "${ttl}" -gt 0 ] 2>/dev/null; then
    echo "  ok   TTL compat:ttl: ${ttl} (>0)"
  else
    echo "  FAIL TTL compat:ttl: got '${ttl}', want a positive TTL" >&2
    FAILED=1
  fi
}

# A minimal RESP wire smoke: representative reads across types must answer (non-error) on this
# server version. Used against BOTH servers to confirm each speaks the shared RESP surface.
resp_wire_smoke() {
  local port="$1" label="$2"
  echo "RESP wire smoke against ${label} (:${port})"
  check "${label} PING"                 "$(resp "${port}" PING)"                 "PONG"
  check "${label} GET compat:str"       "$(resp "${port}" GET compat:str)"       "hello world"
  check "${label} LLEN compat:list"     "$(resp "${port}" LLEN compat:list)"     "3"
  check "${label} HLEN compat:hash"     "$(resp "${port}" HLEN compat:hash)"     "2"
  check "${label} SCARD compat:set"     "$(resp "${port}" SCARD compat:set)"     "3"
  check "${label} ZCARD compat:zset"    "$(resp "${port}" ZCARD compat:zset)"    "2"
}

# ---- phase 1: OLD writes the snapshot -------------------------------------------------
OLD_PORT="$(free_port)"
echo "== boot OLD (vN-1) on :${OLD_PORT}, seed + SAVE =="
IRONCACHE_DATA_DIR="${DATA_DIR}" "${OLD_BIN}" server --bind 127.0.0.1 --port "${OLD_PORT}" \
  >"${WORK}/old.log" 2>&1 &
OLD_PID=$!
PIDS+=("${OLD_PID}")
if ! wait_ping "${OLD_PORT}"; then tail -n 40 "${WORK}/old.log" >&2; exit 1; fi
seed_dataset "${OLD_PORT}"
resp_wire_smoke "${OLD_PORT}" "OLD"
save_reply="$(resp "${OLD_PORT}" SAVE)"
check "OLD SAVE" "${save_reply}" "OK"

# The snapshot must be committed on disk: the manifest (the single commit point) plus at least one
# per-shard file. A missing manifest here means SAVE did not persist (a hard failure).
if [ ! -f "${DATA_DIR}/dump.manifest" ]; then
  echo "FAIL: no dump.manifest written by OLD SAVE" >&2
  ls -la "${DATA_DIR}" >&2 || true
  FAILED=1
fi
shard_files="$(find "${DATA_DIR}" -name 'dump-shard-*.icss' | wc -l | tr -d ' ')"
echo "OLD wrote manifest + ${shard_files} per-shard snapshot file(s) in ${DATA_DIR}"

# Stop OLD cleanly and reap it BEFORE booting NEW (so nothing races on the data_dir or ports).
kill -TERM "${OLD_PID}" 2>/dev/null || true
wait "${OLD_PID}" 2>/dev/null || true
sleep 1

# ---- phase 2: NEW loads the OLD snapshot (the load-bearing forward-load) --------------
NEW_PORT="$(free_port)"
NEW_METRICS_PORT="$(free_port)"
echo "== boot NEW (vN) on :${NEW_PORT} against the SAME data_dir (forward-load) =="
IRONCACHE_DATA_DIR="${DATA_DIR}" "${NEW_BIN}" server --bind 127.0.0.1 --port "${NEW_PORT}" \
  --metrics-addr "127.0.0.1:${NEW_METRICS_PORT}" >"${WORK}/new.log" 2>&1 &
NEW_PID=$!
PIDS+=("${NEW_PID}")
if ! wait_ping "${NEW_PORT}"; then tail -n 40 "${WORK}/new.log" >&2; exit 1; fi
# Gate on /readyz so EVERY shard has finished loading the reloaded snapshot before we read it back.
if ! wait_readyz "${NEW_METRICS_PORT}"; then tail -n 40 "${WORK}/new.log" >&2; exit 1; fi

echo "== verify NEW loaded every seeded key from the OLD-written snapshot =="
verify_dataset "${NEW_PORT}"
resp_wire_smoke "${NEW_PORT}" "NEW"

if [ "${FAILED}" -ne 0 ]; then
  echo "MIXED-VERSION COMPAT: FAILED" >&2
  echo "---- NEW server log tail ----" >&2
  tail -n 40 "${WORK}/new.log" >&2 || true
  exit 1
fi
echo "MIXED-VERSION COMPAT: PASS (NEW loaded a snapshot written by OLD; RESP surface answered on both)"
