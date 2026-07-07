// SPDX-License-Identifier: MIT OR Apache-2.0
//
// PROD-8 ioredis compatibility driver (issue #158).
//
// Runs the real `ioredis` client against a live IronCache, in TWO modes:
//
//   * single-node (new Redis()): core data-type ops, pipelining, MULTI/EXEC, pub/sub, and a RESP3
//     HELLO probe.
//   * cluster (new Redis.Cluster([...])): topology DISCOVERY (ioredis loads the slot map via
//     CLUSTER SLOTS), routed keyed ops (the client routes/follows MOVED), read-back, a cross-slot
//     multi-key op, and a hash-tag co-located multi-key op.
//
// Each checked behavior prints exactly one line:
//     RESULT ioredis <single|cluster> <op-group> <PASS|FAIL> [detail]
// The process exits non-zero if any group FAILed. Assertions check VALUES, not just "no throw".

"use strict";

const Redis = require("ioredis");

let failed = 0;

function result(mode, group, ok, detail = "") {
  const status = ok ? "PASS" : "FAIL";
  if (!ok) failed += 1;
  console.log(`RESULT ioredis ${mode} ${group} ${status} ${detail}`.trimEnd());
}

async function check(mode, group, fn) {
  try {
    const [ok, detail] = await fn();
    result(mode, group, ok, detail);
  } catch (err) {
    result(mode, group, false, `exception: ${err && err.message ? err.message : err}`);
  }
}

function parseArgs() {
  const args = { singlePort: 0, cluster: "", aclUser: "", aclPass: "", shardOwners: "" };
  const a = process.argv.slice(2);
  for (let i = 0; i < a.length; i++) {
    if (a[i] === "--single-port") args.singlePort = parseInt(a[++i], 10);
    else if (a[i] === "--cluster") args.cluster = a[++i];
    else if (a[i] === "--acl-user") args.aclUser = a[++i];
    else if (a[i] === "--acl-pass") args.aclPass = a[++i];
    else if (a[i] === "--shard-owners") args.shardOwners = a[++i];
  }
  return args;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function runSingle(port) {
  const mode = "single";
  const pfx = `js:${Date.now() % 1000000}:`;
  const r = new Redis({ port, host: "127.0.0.1", lazyConnect: false });

  await check(mode, "connect", async () => {
    const pong = await r.ping();
    return [pong === "PONG", `PING=${pong}`];
  });

  await check(mode, "strings", async () => {
    const k = pfx + "str";
    await r.set(k, "hello");
    const v = await r.get(k);
    await r.append(k, " world");
    const full = await r.get(k);
    const rng = await r.getrange(k, 0, 4);
    await r.set(pfx + "n", "10");
    const n = await r.incrby(pfx + "n", 5);
    const ok = v === "hello" && full === "hello world" && rng === "hello" && n === 15;
    return [ok, `get=${v} append=${full} getrange=${rng} incr=${n}`];
  });

  await check(mode, "lists", async () => {
    const k = pfx + "list";
    await r.rpush(k, "a", "b", "c");
    await r.lpush(k, "z");
    const vals = await r.lrange(k, 0, -1);
    const ok = JSON.stringify(vals) === JSON.stringify(["z", "a", "b", "c"]);
    return [ok, `lrange=${JSON.stringify(vals)}`];
  });

  await check(mode, "hashes", async () => {
    const k = pfx + "hash";
    await r.hset(k, "f1", "v1", "f2", "v2");
    const m = await r.hgetall(k);
    const ok = m.f1 === "v1" && m.f2 === "v2" && Object.keys(m).length === 2;
    return [ok, `hgetall=${JSON.stringify(m)}`];
  });

  await check(mode, "sets", async () => {
    const k = pfx + "set";
    await r.sadd(k, "x", "y", "z");
    const members = (await r.smembers(k)).sort();
    const ok = JSON.stringify(members) === JSON.stringify(["x", "y", "z"]);
    return [ok, `smembers=${JSON.stringify(members)}`];
  });

  await check(mode, "zsets", async () => {
    const k = pfx + "zset";
    await r.zadd(k, 1, "one", 2, "two", 3, "three");
    const flat = await r.zrange(k, 0, -1, "WITHSCORES");
    // flat = [member, score, member, score, ...]
    const ok =
      flat[0] === "one" && flat[1] === "1" &&
      flat[2] === "two" && flat[3] === "2" &&
      flat[4] === "three" && flat[5] === "3";
    return [ok, `zrange_withscores=${JSON.stringify(flat)}`];
  });

  await check(mode, "expire-ttl", async () => {
    const k = pfx + "ttl";
    await r.set(k, "v");
    await r.expire(k, 100);
    const t = await r.ttl(k);
    const ok = t > 90 && t <= 100;
    return [ok, `ttl=${t}`];
  });

  await check(mode, "mget-mset", async () => {
    await r.mset(pfx + "m1", "A", pfx + "m2", "B", pfx + "m3", "C");
    const vals = await r.mget(pfx + "m1", pfx + "m2", pfx + "m3");
    const ok = JSON.stringify(vals) === JSON.stringify(["A", "B", "C"]);
    return [ok, `mget=${JSON.stringify(vals)}`];
  });

  await check(mode, "pipeline", async () => {
    const res = await r
      .pipeline()
      .set(pfx + "p1", "1")
      .incr(pfx + "p1")
      .get(pfx + "p1")
      .rpush(pfx + "pl", "a", "b")
      .lrange(pfx + "pl", 0, -1)
      .exec();
    // res = [[err, val], ...] in order
    const ok = res[1][1] === 2 && res[2][1] === "2" && JSON.stringify(res[4][1]) === JSON.stringify(["a", "b"]);
    return [ok, `pipeline=${JSON.stringify(res.map((x) => x[1]))}`];
  });

  await check(mode, "multi-exec", async () => {
    const res = await r.multi().set(pfx + "t", "0").incr(pfx + "t").incr(pfx + "t").exec();
    const final = await r.get(pfx + "t");
    const ok = res[res.length - 1][1] === 2 && final === "2";
    return [ok, `exec=${JSON.stringify(res.map((x) => x[1]))} final=${final}`];
  });

  await check(mode, "pubsub", async () => {
    const chan = pfx + "chan";
    const sub = new Redis({ port, host: "127.0.0.1" });
    const got = new Promise((resolve) => {
      sub.on("message", (c, m) => {
        if (c === chan) resolve(m);
      });
    });
    await sub.subscribe(chan);
    await sleep(100);
    await r.publish(chan, "ping-payload");
    const payload = await Promise.race([got, sleep(3000).then(() => null)]);
    await sub.quit();
    return [payload === "ping-payload", `payload=${payload}`];
  });

  await check(mode, "resp3", async () => {
    // CLIENT LIMITATION (documented finding): ioredis (v5) is RESP2-only. Its bundled `redis-parser`
    // has no case for the RESP3 map type byte `%` (nor `~`/`>`), so `HELLO 3` -- which IronCache
    // correctly answers with a RESP3 map -- makes the parser throw
    // `Protocol error, got "%" as reply type byte`. IronCache is NOT at fault here (redis-py and
    // go-redis both negotiate RESP3 against the SAME server cleanly); the client cannot consume
    // RESP3. We assert HELLO 2 works (the protocol ioredis supports) and record the RESP3 gap as a
    // KNOWN client limitation rather than an IronCache defect.
    const hello2 = await r.call("HELLO", "2");
    let proto = null;
    for (let i = 0; i + 1 < hello2.length; i += 2) {
      if (String(hello2[i]) === "proto") proto = Number(hello2[i + 1]);
    }
    const ok = proto === 2;
    return [ok, `hello2_proto=${proto} (RESP3 N/A: ioredis is RESP2-only -- client limitation, not IronCache)`];
  });

  r.disconnect();
}

// runCluster drives the ioredis cluster client against a set of slot-owner endpoints. The SAME body
// serves TWO legs, picked by `mode`: the turnkey 3-node Raft cluster (mode="cluster") and the #517
// single-node SHARD-OWNERS projection (mode="shard-owners"), which exposes the node's N internal
// shards as N slot owners on distinct ports. In both, a cluster-aware client discovers the topology
// via CLUSTER SLOTS and routes each key to its owner by following MOVED; for shard-owners that owner
// is the key's home SHARD's port, so the internal cross-shard hop is eliminated (the zero-hop metric
// is asserted by run.sh + the Rust metrics_endpoint.rs test).
async function runCluster(seeds, mode = "cluster") {
  const pfx = `js-${mode}:${Date.now() % 1000000}:`;
  const nodes = seeds.map((s) => {
    const [host, port] = s.split(":");
    return { host, port: parseInt(port, 10) };
  });

  const cluster = new Redis.Cluster(nodes, {
    // IronCache advertises 127.0.0.1; no NAT mapping needed.
    redisOptions: {},
    clusterRetryStrategy: (times) => (times > 5 ? null : 200),
  });

  // DISCOVERY: ioredis loads the slot map via CLUSTER SLOTS once connected.
  let discovered = false;
  await check(mode, "discovery", async () => {
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("cluster ready timeout")), 15000);
      cluster.on("ready", () => {
        clearTimeout(t);
        resolve();
      });
      cluster.on("error", () => {}); // transient connect errors during discovery are expected
    });
    const pong = await cluster.ping();
    const nodeCount = cluster.nodes("master").length;
    discovered = pong === "PONG" && nodeCount >= 1;
    return [discovered, `masters_discovered=${nodeCount}`];
  });

  if (!discovered) {
    for (const g of ["routed-ops", "routed-readback", "crossslot", "hashtag-coloc", "pipeline"]) {
      result(mode, g, false, "discovery failed");
    }
    cluster.disconnect();
    return;
  }

  await check(mode, "routed-ops", async () => {
    for (let i = 0; i < 60; i++) {
      await cluster.set(`${pfx}k${i}`, String(i));
    }
    return [true, "set 60 keys across slots"];
  });

  await check(mode, "routed-readback", async () => {
    for (let i = 0; i < 60; i++) {
      const v = await cluster.get(`${pfx}k${i}`);
      if (v !== String(i)) return [false, `k${i}=${v}`];
    }
    return [true, "all 60 read back"];
  });

  await check(mode, "crossslot", async () => {
    try {
      await cluster.mget(`${pfx}k0`, `${pfx}k1`, `${pfx}k2`);
      return [true, "mget across slots accepted (client may split)"];
    } catch (err) {
      const msg = (err.message || String(err)).toUpperCase();
      const ok = msg.includes("CROSSSLOT") || msg.includes("SLOT");
      return [ok, `rejected: ${err.message}`];
    }
  });

  await check(mode, "hashtag-coloc", async () => {
    await cluster.mset(`${pfx}{t}:a`, "1", `${pfx}{t}:b`, "2", `${pfx}{t}:c`, "3");
    const vals = await cluster.mget(`${pfx}{t}:a`, `${pfx}{t}:b`, `${pfx}{t}:c`);
    const ok = JSON.stringify(vals) === JSON.stringify(["1", "2", "3"]);
    return [ok, `hashtag_mget=${JSON.stringify(vals)}`];
  });

  await check(mode, "pipeline", async () => {
    // A cluster pipeline must be slot-coherent in ioredis; use a hash tag so all keys co-locate.
    const p = cluster.pipeline();
    for (let i = 0; i < 10; i++) p.set(`${pfx}{pp}:${i}`, `val${i}`);
    await p.exec();
    for (let i = 0; i < 10; i++) {
      const v = await cluster.get(`${pfx}{pp}:${i}`);
      if (v !== `val${i}`) return [false, `pp${i}=${v}`];
    }
    return [true, "cluster pipeline 10 co-located keys ok"];
  });

  cluster.disconnect();
}

async function runRestricted(seeds, user, pass) {
  // #405 per-subcommand-ACL leg: drive the cluster as the LOCKED-DOWN `svc` user
  // (`+@read +@write +@connection +@transaction -@dangerous +cluster|slots|shards|nodes|info`).
  // Proves (a) a scoped user can DISCOVER + do a routed SET/GET round-trip and (b) every CLUSTER
  // MUTATOR is NOPERM (CLUSTER ADDSLOTS denied -> the group PASSES). NOTE: ioredis's default
  // enableReadyCheck issues `CLUSTER INFO`, which is WHY the svc grant includes `+cluster|info`
  // (go-redis / redis-py discover via CLUSTER SLOTS alone) -- the documented #405 finding.
  const mode = "restricted";
  const pfx = `jsr:${Date.now() % 1000000}:`;
  const nodes = seeds.map((s) => {
    const [host, port] = s.split(":");
    return { host, port: parseInt(port, 10) };
  });

  const cluster = new Redis.Cluster(nodes, {
    redisOptions: { username: user, password: pass },
    clusterRetryStrategy: (times) => (times > 5 ? null : 200),
  });

  let discovered = false;
  await check(mode, "discovery", async () => {
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("cluster ready timeout")), 15000);
      cluster.on("ready", () => {
        clearTimeout(t);
        resolve();
      });
      cluster.on("error", () => {}); // transient connect errors during discovery are expected
    });
    const pong = await cluster.ping();
    const nodeCount = cluster.nodes("master").length;
    discovered = pong === "PONG" && nodeCount >= 1;
    return [discovered, `masters_discovered=${nodeCount} as ${user}`];
  });

  if (!discovered) {
    for (const g of ["rw-roundtrip", "addslots-denied"]) {
      result(mode, g, false, "discovery failed");
    }
    cluster.disconnect();
    return;
  }

  await check(mode, "rw-roundtrip", async () => {
    for (let i = 0; i < 30; i++) await cluster.set(`${pfx}k${i}`, String(i));
    for (let i = 0; i < 30; i++) {
      const v = await cluster.get(`${pfx}k${i}`);
      if (v !== String(i)) return [false, `mismatch k${i}=${v}`];
    }
    return [true, "set+get 30 keys across slots as svc"];
  });

  // SECURITY BOUNDARY: a CLUSTER MUTATOR must be NOPERM for svc. Use a DIRECT (non-cluster)
  // connection to node[0] as svc for a deterministic target; PASS iff ADDSLOTS is denied NOPERM.
  await check(mode, "addslots-denied", async () => {
    const one = new Redis({ host: nodes[0].host, port: nodes[0].port, username: user, password: pass });
    try {
      await one.call("CLUSTER", "ADDSLOTS", "0");
      return [false, "CLUSTER ADDSLOTS was ACCEPTED for a -@dangerous user (escalation!)"];
    } catch (err) {
      const msg = (err && err.message ? err.message : String(err)).toUpperCase();
      return [msg.includes("NOPERM"), `denied: ${err && err.message ? err.message : err}`];
    } finally {
      one.disconnect();
    }
  });

  cluster.disconnect();
}

async function main() {
  const args = parseArgs();
  console.log(`# ioredis ${require("ioredis/package.json").version}`);
  if (args.singlePort > 0) await runSingle(args.singlePort);
  if (args.cluster) await runCluster(args.cluster.split(","));
  if (args.cluster && args.aclUser) await runRestricted(args.cluster.split(","), args.aclUser, args.aclPass);
  if (args.shardOwners) await runCluster(args.shardOwners.split(","), "shard-owners");
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
