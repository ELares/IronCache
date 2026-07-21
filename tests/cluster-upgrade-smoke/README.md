# Cluster rolling-upgrade docker smoke (#630)

A live, on-demand docker smoke for `ironcache upgrade --cluster`: it rolls a real 3-node raft
cluster from one binary version to the next **under continuous write load**, replica-first with the
RPO=0 failover-freeze fence, and asserts the properties the decomposed unit tests + DST cannot
observe (they stub the live cluster signals).

It reliably reproduced the bugs #630 was filed to catch (#733 driver false-success, #728/#735 the
post-promotion CLUSTER SHARDS membership drift) and is now the standing regression gate for them.

## What it asserts

1. the driver reports **SUCCESS**;
2. **every node -- including the old primary -- reaches the target version** (regression gate for
   [#733], where the old primary was silently left on the old binary);
3. **zero acked-write loss** across the roll (RPO=0, the durability guarantee);
4. only a **brief, bounded write-pause** (< 5% of writes): the RPO=0 fence deliberately
   `CLIENT PAUSE WRITE`s the old primary for the drain window, and this single-entry writer also
   blips when its entry node is recreated -- so a short pause is expected + correct; a SUSTAINED
   outage (the cluster going unavailable) is the real failure this catches.

## Why its own topology

This is a **1-master + in-sync-replica** cluster (node-1 owns all 16384 slots; node-2/3 attach as
replicas at runtime), which is what a replica-first-then-promote rolling upgrade needs. That is
deliberately different from `deploy/compose/docker-compose.cluster.yml`, which is a 3-way **sharded,
no-replica** topology on a single published image -- not drivable by a rolling upgrade. It is also
single-shard (`shards = 1`): the HA replica path is single-shard by design (see [#731]), so a
multi-shard node cannot deliver RPO=0 on promotion.

The two node binaries come from **one source tree** and differ only by the compile-time
`IRONCACHE_BUILD_VERSION` stamp (v1 = 1.0.0, v2 = 2.0.0); v2 also carries the `upgrade --cluster`
driver. So this harness builds them from source rather than committing ~130MB artifacts.

## Prerequisites

- A **Linux docker engine** (the cluster runs `debian:bookworm-slim` containers). On Apple Silicon,
  [colima]/[lima] work; if your docker CLI is not already on `PATH`, set `IC_DOCKER_BIN` to its
  directory (e.g. `export IC_DOCKER_BIN="$HOME/.local/ctr/bin"`).
- `bash`, and this repo checked out (the build mounts it to compile `ironcache`).

## Run

```sh
cd tests/cluster-upgrade-smoke
./build.sh     # builds bin/ironcache-v1 (1.0.0) + bin/ironcache-v2 (2.0.0) + the driver image
./smoke.sh     # brings up the cluster, rolls v1 -> v2 under load, asserts, tears down
```

`smoke.sh` is self-contained and tears everything (containers + volumes) down on exit. Knobs:
`DURATION` (writer seconds, default 90), `IRONCACHE_CLUSTER_SECRET` (local-only smoke value),
`IC_DOCKER_BIN` (extra docker-CLI PATH dir).

## Not a required CI gate

This boots real containers and forms a live raft quorum, which is **not** how this repo runs CI
(docker is used there only to build images on tags and to `config`-validate compose). Live 3-node
raft formation is also load-sensitive/flaky, which is why the in-process cousin
`crates/ironcache/tests/cluster_upgrade_live.rs` is `#[ignore]`d (nightly-only). Run this smoke
**on demand / nightly on a docker host**, not as a required PR check. The always-on CI proof of the
safety mechanism is the deterministic single-node freeze-seam test (`freeze_seam_holds_a_real_write`).

## Files

| file | role |
|---|---|
| `build.sh` | build the two versioned node binaries + the driver-runner image from source |
| `smoke.sh` | the orchestrator: bring up, form, replicate, load, roll, verify, assert, tear down |
| `docker-compose.smoke.yml` | the 3-node master+replica cluster (bind-mounts `bin/ironcache-${IC_VER_N}`) |
| `config/node{1,2,3}.toml` | per-node server config (single-shard, raft, node-1 owns all slots) |
| `inventory.toml` | the driver's static actuation map (compose-network addresses) |
| `recreate.sh` | the `--actuator-command`: bump a node's binary tag + `--force-recreate` it |
| `load.sh` / `verify.sh` | the writer (record every acked key) / the post-roll zero-loss verifier |
| `Dockerfile.driver` | the driver-runner image (glibc + docker CLI + compose plugin) |
| `versions.env.example` | the all-v1 baseline `smoke.sh` seeds `versions.env` from |

[#630]: https://github.com/ELares/IronCache/issues/630
[#731]: https://github.com/ELares/IronCache/issues/731
[#733]: https://github.com/ELares/IronCache/issues/733
[#728]: https://github.com/ELares/IronCache/issues/728
[#735]: https://github.com/ELares/IronCache/issues/735
[colima]: https://github.com/abiosoft/colima
[lima]: https://github.com/lima-vm/lima
