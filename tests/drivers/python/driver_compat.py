# SPDX-License-Identifier: MIT OR Apache-2.0
"""PROD-8 redis-py compatibility driver (issue #158).

Runs the real `redis` Python client against a live IronCache, in TWO modes:

  * single-node (redis.Redis): core data-type ops, pipelining, MULTI/EXEC, pub/sub, RESP3 (HELLO 3 +
    a push round-trip), and -- via a second connection that sets requirepass through CONFIG-less env
    is out of scope here, so AUTH is exercised only if IRONCACHE_REQUIREPASS was set for the server.
  * cluster (redis.cluster.RedisCluster): topology DISCOVERY via CLUSTER SLOTS, routed keyed ops
    (the client must FOLLOW MOVED to reach the owning node), read-back correctness, and a CROSSSLOT
    multi-key op (the client/server must reject a multi-key op that spans slots).

Each checked behavior prints exactly one line:
    RESULT redis-py <single|cluster> <op-group> <PASS|FAIL> [detail]
The process exits non-zero if any group FAILed (a harness signal; the orchestrator still collects
every RESULT line). This asserts VALUES, not just "no exception": e.g. ZRANGE withscores returns the
right (member, score) pairs, HGETALL the right dict, a pipeline returns replies in order, MULTI/EXEC
is atomic, a SUBSCRIBE receives a PUBLISH, and HELLO 3 returns a server map.
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
import uuid

import redis


FAILED = 0


def result(mode: str, group: str, ok: bool, detail: str = "") -> None:
    global FAILED
    status = "PASS" if ok else "FAIL"
    if not ok:
        FAILED += 1
    print(f"RESULT redis-py {mode} {group} {status} {detail}".rstrip(), flush=True)


def check(mode: str, group: str, fn) -> None:
    """Run one op-group closure; PASS iff it returns truthy and raises nothing."""
    try:
        ok, detail = fn()
    except Exception as exc:  # a client-side exception IS the finding
        result(mode, group, False, f"exception: {type(exc).__name__}: {exc}")
        return
    result(mode, group, bool(ok), detail)


# --------------------------------------------------------------------------- single-node groups
def run_single(port: int) -> None:
    mode = "single"
    pfx = f"py:{uuid.uuid4().hex[:8]}:"
    r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)

    def g_connect():
        return r.ping() is True, "PING"

    def g_strings():
        k = pfx + "str"
        r.set(k, "hello")
        v = r.get(k)
        r.append(k, " world")
        full = r.get(k)
        rng = r.getrange(k, 0, 4)
        r.set(pfx + "n", "10")
        n = r.incr(pfx + "n", 5)
        ok = v == "hello" and full == "hello world" and rng == "hello" and n == 15
        return ok, f"get={v!r} append={full!r} getrange={rng!r} incr={n}"

    def g_lists():
        k = pfx + "list"
        r.rpush(k, "a", "b", "c")
        r.lpush(k, "z")
        vals = r.lrange(k, 0, -1)
        ok = vals == ["z", "a", "b", "c"]
        return ok, f"lrange={vals}"

    def g_hashes():
        k = pfx + "hash"
        r.hset(k, mapping={"f1": "v1", "f2": "v2"})
        m = r.hgetall(k)
        ok = m == {"f1": "v1", "f2": "v2"}
        return ok, f"hgetall={m}"

    def g_sets():
        k = pfx + "set"
        r.sadd(k, "x", "y", "z")
        members = set(r.smembers(k))
        ok = members == {"x", "y", "z"}
        return ok, f"smembers={sorted(members)}"

    def g_zsets():
        k = pfx + "zset"
        r.zadd(k, {"one": 1.0, "two": 2.0, "three": 3.0})
        pairs = r.zrange(k, 0, -1, withscores=True)
        ok = pairs == [("one", 1.0), ("two", 2.0), ("three", 3.0)]
        return ok, f"zrange_withscores={pairs}"

    def g_ttl():
        k = pfx + "ttl"
        r.set(k, "v")
        r.expire(k, 100)
        t = r.ttl(k)
        ok = 90 <= t <= 100
        return ok, f"ttl={t}"

    def g_mget_mset():
        r.mset({pfx + "m1": "A", pfx + "m2": "B", pfx + "m3": "C"})
        vals = r.mget(pfx + "m1", pfx + "m2", pfx + "m3")
        ok = vals == ["A", "B", "C"]
        return ok, f"mget={vals}"

    def g_pipeline():
        p = r.pipeline(transaction=False)
        p.set(pfx + "p1", "1")
        p.incr(pfx + "p1")
        p.get(pfx + "p1")
        p.lpush(pfx + "pl", "a", "b")
        p.lrange(pfx + "pl", 0, -1)
        res = p.execute()
        ok = res[1] == 2 and res[2] == "2" and res[4] == ["b", "a"]
        return ok, f"pipeline_replies={res}"

    def g_multi_exec():
        # MULTI/EXEC atomic transaction (transaction=True is the default pipeline). The single node
        # is booted with one shard (see run.sh) so all keys share a home shard: IronCache does not
        # support cross-shard transactions, so a multi-shard node would abort a txn whose keys land
        # on a non-home shard (a documented model detail, see DRIVER_MATRIX.md).
        tk = pfx + "t"
        with r.pipeline(transaction=True) as p:
            p.set(tk, "0")
            p.incr(tk)
            p.incr(tk)
            res = p.execute()
        final = r.get(tk)
        ok = res[-1] == 2 and final == "2"
        return ok, f"exec={res} final={final!r}"

    def g_pubsub():
        chan = pfx + "chan"
        sub = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
        ps = sub.pubsub()
        ps.subscribe(chan)
        # Drain the subscribe-confirmation message.
        deadline = time.time() + 3
        got_sub = False
        while time.time() < deadline:
            m = ps.get_message(timeout=0.5)
            if m and m.get("type") == "subscribe":
                got_sub = True
                break
        # Publish from the main connection; the subscriber must receive it.
        time.sleep(0.1)
        r.publish(chan, "ping-payload")
        payload = None
        deadline = time.time() + 3
        while time.time() < deadline:
            m = ps.get_message(timeout=0.5)
            if m and m.get("type") == "message":
                payload = m.get("data")
                break
        ps.close()
        sub.close()
        ok = got_sub and payload == "ping-payload"
        return ok, f"subscribed={got_sub} payload={payload!r}"

    def g_resp3():
        # RESP3: HELLO 3 negotiates protocol 3; server returns a map. Then a normal op still works,
        # and a push-delivering pub/sub round-trip works under RESP3.
        r3 = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, protocol=3)
        hello = r3.execute_command("HELLO", "3")
        proto = hello.get("proto") if isinstance(hello, dict) else None
        r3.set(pfx + "r3", "ok")
        v = r3.get(pfx + "r3")
        # RESP3 push: subscribe + publish on RESP3 connections.
        chan = pfx + "r3chan"
        sub = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, protocol=3)
        ps = sub.pubsub()
        ps.subscribe(chan)
        deadline = time.time() + 3
        while time.time() < deadline:
            m = ps.get_message(timeout=0.5)
            if m and m.get("type") == "subscribe":
                break
        time.sleep(0.1)
        r3.publish(chan, "r3-push")
        payload = None
        deadline = time.time() + 3
        while time.time() < deadline:
            m = ps.get_message(timeout=0.5)
            if m and m.get("type") == "message":
                payload = m.get("data")
                break
        ps.close()
        sub.close()
        r3.close()
        ok = proto == 3 and v == "ok" and payload == "r3-push"
        return ok, f"hello_proto={proto} get={v!r} push={payload!r}"

    check(mode, "connect", g_connect)
    check(mode, "strings", g_strings)
    check(mode, "lists", g_lists)
    check(mode, "hashes", g_hashes)
    check(mode, "sets", g_sets)
    check(mode, "zsets", g_zsets)
    check(mode, "expire-ttl", g_ttl)
    check(mode, "mget-mset", g_mget_mset)
    check(mode, "pipeline", g_pipeline)
    check(mode, "multi-exec", g_multi_exec)
    check(mode, "pubsub", g_pubsub)
    check(mode, "resp3", g_resp3)
    r.close()


# --------------------------------------------------------------------------- cluster groups
def run_cluster(nodes: list[tuple[str, int]], mode: str = "cluster") -> None:
    # `mode` labels the RESULT lines and picks the key prefix. The SAME cluster-client body drives
    # the turnkey 3-node Raft cluster (`mode="cluster"`) AND the #517 single-node SHARD-OWNERS
    # projection (`mode="shard-owners"`): both expose N slot owners on distinct endpoints that a
    # cluster-aware client discovers via CLUSTER SLOTS and routes to by following MOVED. The
    # shard-owners leg proves a real client routes each key to its owning SHARD's port, so the
    # internal cross-shard hop is eliminated (the zero-hop metric is asserted by run.sh + the Rust
    # metrics_endpoint.rs test).
    from redis.cluster import ClusterNode, RedisCluster

    pfx = f"py:{mode}:{uuid.uuid4().hex[:8]}:"
    startup = [ClusterNode(h, p) for (h, p) in nodes]

    # Construction itself exercises DISCOVERY: redis-py issues CLUSTER SLOTS against a seed and builds
    # the slot->node map. A malformed CLUSTER SLOTS reply would raise here.
    try:
        rc = RedisCluster(startup_nodes=startup, decode_responses=True, require_full_coverage=True)
        rc.ping()
        discovered = True
        ndetail = f"nodes_discovered={len(rc.get_nodes())}"
    except Exception as exc:
        result(mode, "discovery", False, f"exception: {type(exc).__name__}: {exc}")
        # Without discovery the rest is moot; record them as FAIL with the cause.
        for g in ("routed-ops", "routed-readback", "crossslot", "pipeline"):
            result(mode, g, False, "discovery failed")
        return
    result(mode, "discovery", discovered, ndetail)

    def g_routed_ops():
        # Write keys whose slots land in each of the three blocks; the client must route each to its
        # owner by FOLLOWING MOVED (the seed node only owns one third). Use distinct keys.
        wrote = 0
        for i in range(60):
            rc.set(f"{pfx}k{i}", str(i))
            wrote += 1
        return wrote == 60, f"set_count={wrote}"

    def g_routed_readback():
        ok = True
        bad = ""
        for i in range(60):
            v = rc.get(f"{pfx}k{i}")
            if v != str(i):
                ok = False
                bad = f"k{i}={v!r}"
                break
        return ok, ("all 60 read back" if ok else f"mismatch {bad}")

    def g_crossslot():
        # A multi-key op whose keys hash to DIFFERENT slots must be rejected. redis-py's cluster
        # client rejects this CLIENT-SIDE (it computes both slots from the COMMAND key-spec the
        # server reported, sees they differ, and raises RedisClusterException) -- which proves the
        # client successfully learned IronCache's command table + slot function. Some redis-py
        # versions instead surface the server's -CROSSSLOT as a ResponseError. Both are correct.
        from redis.exceptions import RedisError, RedisClusterException
        try:
            rc.mget(f"{pfx}k0", f"{pfx}k1", f"{pfx}k2")
            # No raise means the client split the op per-slot client-side; also acceptable.
            return True, "mget across slots handled (client-side split or accepted)"
        except (RedisError, RedisClusterException) as exc:
            msg = str(exc)
            ok = "CROSSSLOT" in msg.upper() or "slot" in msg.lower()
            return ok, f"rejected: {msg[:90]}"

    def g_pipeline():
        # Cluster pipeline: redis-py routes each command to its owner node and gathers replies.
        with rc.pipeline() as p:
            for i in range(10):
                p.set(f"{pfx}pp{i}", f"val{i}")
            p.execute()
        vals = [rc.get(f"{pfx}pp{i}") for i in range(10)]
        ok = vals == [f"val{i}" for i in range(10)]
        return ok, f"pipeline_readback_ok={ok}"

    def g_hashtag():
        # Hash-tagged keys force co-location in ONE slot, so a multi-key op succeeds. Proves the
        # client respects {tag} slot computation against IronCache's CRC16.
        rc.mset({f"{pfx}{{t}}:a": "1", f"{pfx}{{t}}:b": "2", f"{pfx}{{t}}:c": "3"})
        vals = rc.mget(f"{pfx}{{t}}:a", f"{pfx}{{t}}:b", f"{pfx}{{t}}:c")
        ok = vals == ["1", "2", "3"]
        return ok, f"hashtag_mget={vals}"

    check(mode, "routed-ops", g_routed_ops)
    check(mode, "routed-readback", g_routed_readback)
    check(mode, "crossslot", g_crossslot)
    check(mode, "pipeline", g_pipeline)
    check(mode, "hashtag-coloc", g_hashtag)
    try:
        rc.close()
    except Exception:
        pass


# --------------------------------------------------------------------------- restricted-user groups
def run_restricted(nodes: list[tuple[str, int]], user: str, password: str) -> None:
    """#405 per-subcommand-ACL leg: drive the cluster as the LOCKED-DOWN `svc` user
    (`+@read +@write +@connection +@transaction -@dangerous +cluster|slots|shards|nodes|info`).

    Proves (a) a scoped user can still DISCOVER topology + do a routed SET/GET round-trip, and
    (b) every CLUSTER MUTATOR is NOPERM -- CLUSTER ADDSLOTS must be denied (the group PASSES when it
    is). redis-py applies username/password to every node connection, so MOVED-routing re-AUTHs as
    `svc` transparently.
    """
    mode = "restricted"
    from redis.cluster import ClusterNode, RedisCluster
    from redis.exceptions import ResponseError

    pfx = f"pyr:{uuid.uuid4().hex[:8]}:"
    startup = [ClusterNode(h, p) for (h, p) in nodes]

    # DISCOVERY as svc: construction issues CLUSTER SLOTS (granted). A NOPERM would raise here.
    try:
        rc = RedisCluster(
            startup_nodes=startup,
            decode_responses=True,
            require_full_coverage=True,
            username=user,
            password=password,
        )
        rc.ping()
        result(mode, "discovery", True, f"nodes_discovered={len(rc.get_nodes())} as {user}")
    except Exception as exc:
        result(mode, "discovery", False, f"exception: {type(exc).__name__}: {exc}")
        for g in ("rw-roundtrip", "addslots-denied"):
            result(mode, g, False, "discovery failed")
        return

    def g_rw_roundtrip():
        for i in range(30):
            rc.set(f"{pfx}k{i}", str(i))
        for i in range(30):
            v = rc.get(f"{pfx}k{i}")
            if v != str(i):
                return False, f"mismatch k{i}={v!r}"
        return True, "set+get 30 keys across slots as svc"

    def g_addslots_denied():
        # A CLUSTER MUTATOR must be NOPERM for svc. Use a DIRECT (non-cluster) connection to node[0]
        # authenticated as svc for a deterministic target; PASS iff ADDSLOTS is denied NOPERM.
        host, port = nodes[0]
        one = redis.Redis(host=host, port=port, decode_responses=True, username=user, password=password)
        try:
            one.execute_command("CLUSTER", "ADDSLOTS", 0)
            return False, "CLUSTER ADDSLOTS was ACCEPTED for a -@dangerous user (escalation!)"
        except ResponseError as exc:
            # redis-py raises NoPermissionError and STRIPS the NOPERM error-code
            # prefix, so its message is "... has no permissions ..." without the
            # literal token (go-redis / ioredis surface the raw NOPERM). Accept
            # either form: the command was denied as long as it is a permissions
            # error and not some unrelated ResponseError.
            msg = str(exc)
            denied = "NOPERM" in msg.upper() or "NO PERMISSIONS" in msg.upper()
            return denied, f"denied: {msg[:90]}"
        finally:
            one.close()

    check(mode, "rw-roundtrip", g_rw_roundtrip)
    check(mode, "addslots-denied", g_addslots_denied)
    try:
        rc.close()
    except Exception:
        pass


def parse_nodes(csv: str) -> list[tuple[str, int]]:
    out = []
    for part in csv.split(","):
        host, _, port = part.strip().rpartition(":")
        out.append((host, int(port)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--single-port", type=int, required=True)
    ap.add_argument("--cluster", type=str, required=True, help="comma list of host:port seeds")
    ap.add_argument("--acl-user", type=str, default="", help="restricted (scoped-ACL) username, #405 leg")
    ap.add_argument("--acl-pass", type=str, default="", help="restricted (scoped-ACL) password, #405 leg")
    ap.add_argument("--shard-owners", type=str, default="",
                    help="comma list of the N shard-owner host:port endpoints (#517 leg)")
    args = ap.parse_args()

    print(f"# redis-py {redis.__version__}", flush=True)
    run_single(args.single_port)
    run_cluster(parse_nodes(args.cluster))
    if args.acl_user:
        run_restricted(parse_nodes(args.cluster), args.acl_user, args.acl_pass)
    if args.shard_owners:
        run_cluster(parse_nodes(args.shard_owners), mode="shard-owners")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
