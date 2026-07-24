<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Backup, restore, and DR for IronCache on Kubernetes

IronCache persists to a PersistentVolume, but **a PVC is not a backup**. A deleted
namespace, a corrupted volume, a fat-fingered `kubectl delete pvc`, or -- on
single-node k3s with the local-path provisioner -- a lost node, all destroy the
data with the volume. This document is the backup/restore/DR runbook.

> **On single-node / edge k3s with `local-path`, an off-node backup is
> MANDATORY, not optional.** local-path is hostPath-backed on ONE node: there is
> no CSI snapshot support and (with one replica) no peer to re-replicate from, so
> PVC or node loss = **total data loss**. Multi-node raft clusters survive a
> single node loss via re-replication, but still need backups for logical errors,
> correlated failures, and full-cluster loss.

The chart deliberately ships **no always-on backup CronJob**: getting data off a
ReadWriteOnce volume to object storage is environment-specific (your storage
class, your object store, your credentials) and IronCache has no native
object-store integration, so a bundled job would be half-working. Instead this
runbook gives you the artifact, the methods, and copy-paste references to wire up
in your environment.

---

## 1. What a backup actually is

IronCache's durable state lives entirely under `data_dir` (the chart's
`persistence.mountPath`, default `/var/lib/ironcache`). A complete backup is a
copy of that directory. It contains:

| File(s) | What | Notes |
| --- | --- | --- |
| `dump-shard-<n>.icss` | Per-shard base snapshot (one file per shard) | Binary "ICSS" format, CRC-32 per file |
| `dump-shard-<n>-delta-<k>.icsd` | Incremental delta snapshots | Only when `snapshot_deltas` is enabled |
| `dump.manifest` | The commit point | Written + fsync'd **LAST**; names the committed shard/delta files |
| `ironcache-raft-<port>.log` | The durable raft log (cluster mode) | Carries writes committed **after** the last snapshot cut |
| `ironcache-raft-<port>.log.snap` | The raft log's snapshot sidecar | Co-located with the log under `data_dir` |

Two properties that make this safe to copy:

- **Crash-safe writes.** Each shape is written `tmp -> fsync -> rename`, and the
  manifest is fsync'd LAST. So whenever a `dump.manifest` exists it points only at
  fully-written shard files; a crash (or a copy that races a save) mid-write
  leaves the *previous* good manifest intact. Each shard file carries a CRC-32, so
  a torn file is detected and skipped on load rather than loaded as garbage.
- **Portable across nodes and shard counts.** A snapshot taken with 8 shards
  loads on a 2-shard node: each shard re-hashes every key through the ownership
  router and keeps only the keys it owns. So a backup is not pinned to the exact
  shard layout it was taken on.

Two properties to respect:

- **Snapshot alone is stale for a cluster.** In raft mode the log keeps
  accumulating writes after the snapshot is cut, so a consistent restore needs the
  snapshot files **and** the raft log together. Back up the **whole `data_dir`**,
  not just the `.icss` files.
- **Not a global point-in-time.** Shards dump at slightly different instants (no
  cross-shard lock -- a deliberate cache tradeoff). Each shard is
  self-consistent; the set is "fuzzy" across shards. Fine for a cache; do not
  treat an IronCache backup as an ACID transactional snapshot.

---

## 2. RPO and RTO

- **RPO (how much you can lose).** A file-copy backup is only as fresh as the last
  SAVE it captured. The chart's default periodic policy is `saveIntervalSecs: 900`
  (a save every 15 min if >= `saveMinChanges` writes happened), so a naive daily
  copy of the periodic snapshot has an RPO up to 15 min *plus* the backup interval.
  Tighten it by issuing an explicit `SAVE` (or `BGSAVE`) immediately before each
  copy, and/or shortening `saveIntervalSecs`. In cluster mode, copying the raft log
  too captures writes committed after the last snapshot, improving the effective
  RPO -- but the log is only replayable against a matching snapshot base, so always
  copy them together.
- **RTO (how long to come back).** On restore the node reloads the snapshot on
  boot; time scales with dataset size and storage throughput. Size the
  `startupProbe` budget (`failureThreshold * periodSeconds`, default 10 min) to
  your worst-case reload so a restoring node is not CrashLooped mid-load. For a
  cluster, add re-replication time for a node rejoining behind its peers.

---

## 3. Method 1 -- CSI VolumeSnapshot / Velero (managed Kubernetes)

**The cleanest path where your StorageClass has a CSI driver with snapshot
support** (most managed clouds; NOT local-path k3s). A `VolumeSnapshot` captures
the whole PVC -- snapshot files, manifest, AND raft log -- as one crash-consistent
block/filesystem image, which is exactly the atomic `data_dir` unit you want.

- **Ad-hoc / scripted:** create a `VolumeSnapshot` per pod's PVC:

  ```yaml
  apiVersion: snapshot.storage.k8s.io/v1
  kind: VolumeSnapshot
  metadata:
    name: ic-data-ironcache-0-20260101
  spec:
    volumeSnapshotClassName: <your-csi-snapshot-class>
    source:
      persistentVolumeClaimName: data-<release>-ironcache-0   # one per ordinal
  ```

  Snapshot **every** ordinal's PVC (each node owns a distinct slot range). For a
  tighter cross-node point, quiesce writes first (see the `CLIENT PAUSE WRITE`
  fence used by `ironcache upgrade --cluster`, `docs/UPGRADE.md`) -- though for a
  cache the per-PVC crash-consistent image is usually sufficient.

- **Scheduled / off-cluster:** use a backup operator such as **Velero** (with its
  CSI snapshot plugin) or an external-snapshotter schedule to take recurring
  snapshots and ship them to object storage with retention. This is the
  recommended production setup on clusters that support it; IronCache needs no
  special handling beyond "snapshot the whole PVC of each pod".

CSI snapshots are storage-native (fast, offloaded to the backend) and capture the
raft log automatically, so this method sidesteps every ReadWriteOnce / shell-free
constraint below. Prefer it when available.

---

## 4. Method 2 -- app-level file copy (local-path / no CSI)

When there is no CSI snapshot support (single-node k3s local-path, hostPath, some
edge storage), copy the `data_dir` files to object storage yourself. Two shape
constraints drive the design:

- The PVC is **ReadWriteOnce** and mounted by the running pod, so the copier must
  share that mount.
- The IronCache image is **distroless / shell-free**, so you cannot `kubectl exec
  ... sh -c 'tar ...'` inside the cache container, and there is no "dump snapshot
  to stdout" RESP verb. The copier must be a **separate image** that has your
  object-store client.

The portable pattern is a **backup sidecar** in the StatefulSet pod (it shares the
data volume) that, on a schedule: (1) triggers a fresh save over RESP to
localhost, then (2) copies the manifest-referenced files to your bucket. Reference
sketch (adapt the image + destination + credentials to your environment; this is
NOT shipped by the chart):

```yaml
# An extra container in the cache pod (via a chart values override / kustomize
# patch) that shares the data volume. Uses an image with your object-store client
# (rclone / aws-cli / restic) plus a RESP client to trigger the save.
- name: backup
  image: <your-image-with-rclone-and-redis-cli>
  volumeMounts:
    - name: data                       # SAME PVC the cache mounts
      mountPath: /var/lib/ironcache
      readOnly: true                   # copier only reads
  env:
    - name: RCLONE_CONFIG_DEST_TYPE
      value: s3
    # ... object-store credentials from a Secret ...
  command: ["/bin/sh", "-c"]
  args:
    - |
      while true; do
        # 1. Trigger a consistent save (blocks until the manifest is committed).
        redis-cli -h 127.0.0.1 -p 6379 SAVE
        # 2. Copy the committed set. Read dump.manifest FIRST, then the files it
        #    names, so you never capture a half-written generation. Copying the
        #    whole dir after a completed SAVE is the simple, safe choice.
        rclone copy /var/lib/ironcache dest:my-bucket/$(hostname)/ \
          --include 'dump.manifest' --include 'dump-shard-*.ic*' \
          --include 'ironcache-raft-*.log*'
        sleep 3600
      done
```

Notes and gotchas:

- **Consistency:** trigger `SAVE` (blocking; returns `+OK` when the manifest is
  committed) or `BGSAVE` + poll before copying. Always include `dump.manifest` AND
  every file it references; include the `ironcache-raft-*.log*` files for a cluster
  node. Copying the whole `data_dir` right after a completed SAVE is the simplest
  safe approach.
- **Alternative -- a node-pinned CronJob:** a `CronJob` whose pod mounts the same
  PVC read-only can work where the CSI driver permits multiple pods per node for a
  ReadWriteOnce volume (node-affinity it to the cache pod's node). The sidecar
  avoids that fragility; pick per your driver.
- **Do not** attempt to `kubectl cp` / `exec tar` against the cache container --
  the shell-free image has no `tar`/`sh`.

---

## 5. Method 3 -- per-key logical dump (small / selective / cross-version)

For small datasets, selective keys, or a **version-portable** export (e.g. moving
between binary versions where a snapshot's manifest version would block a
downgrade, section 6), use `DUMP`/`RESTORE` over RESP -- a pure network client, no
PVC access needed:

- Iterate keys (`SCAN`) and `DUMP <key>` each; the blob is Redis/Valkey-compatible
  (round-trip verified against a redis oracle).
- Restore with `RESTORE <key> <ttl> <blob>` per key.

This has **no bulk-export command** -- it is a per-key loop, so it is slow and
O(keyspace); use it for small or selective backups, not as the primary path for a
large cache. Its upside is format portability: a `DUMP` blob is not tied to the
IronCache snapshot manifest version.

---

## 6. Restore runbook

1. **Provision a clean target.** Recreate the PVC (or the whole release). Ensure
   the node you are restoring into is **not serving yet** -- restore into the PVC
   before the pod boots, or scale the workload down first. A load-on-boot reads
   whatever is in `data_dir` at startup.
2. **Place the artifact.** Put the backed-up files back under `data_dir`
   (`/var/lib/ironcache`): `dump.manifest`, its `dump-shard-*.icss` (+ any
   `*.icsd`), and -- for a cluster node -- the `ironcache-raft-<port>.log` (+
   `.log.snap`). From a CSI `VolumeSnapshot`, restore by creating the PVC
   `dataSource` from the snapshot instead.
3. **Keep the port consistent (cluster).** The raft log filename embeds the raft
   port (`ironcache-raft-<port>.log`). Restore onto a node configured with the same
   port, or the log will not be found and the node starts from the snapshot only
   (losing post-snapshot writes).
4. **Boot and watch the load.** Start the pod; it auto-loads the snapshot on
   startup. The `startupProbe` suspends liveness/readiness until the reload
   finishes, so size its budget to the dataset (section 2). Confirm with
   `DBSIZE` / `CLUSTER INFO`.
5. **Let the cluster reconcile.** For a single restored node in a healthy cluster,
   raft re-replication catches it up to current state. For a **full-cluster**
   restore (all nodes lost), restore each node's own `data_dir` from its backup;
   shard-count changes are handled by re-sharding on load, but keep the topology /
   announce-id assignment consistent so each node reloads its own shards.
6. **Verify, then resume traffic.**

### Version caveat (#530): do not restore a newer snapshot onto an older binary

The manifest carries a format version. A base-only snapshot is v1; enabling
`snapshot_deltas` writes a v2 manifest that a pre-delta binary does **not**
understand. On purpose, an older binary refuses to boot on a newer manifest
(fail-loud) rather than silently starting empty. **Restore onto a binary version
>= the one that took the backup.** If you must move data to an older version, use
the logical `DUMP`/`RESTORE` path (section 5), which is manifest-version
independent. Keep your backups tagged with the IronCache version that produced
them.

---

## 7. DR principles

- **Off-node and off-cluster.** A backup on the same node/cluster does not survive
  the failure that takes them out. Ship backups to object storage in a different
  failure domain (ideally a different region/account).
- **Test restores.** An untested backup is a hypothesis. Periodically restore into
  a scratch namespace and verify `DBSIZE` / spot-check keys. RTO is only real once
  measured.
- **Retention + immutability.** Keep enough history to survive a
  logical-corruption you notice late; object-lock / versioned buckets defend
  backups against the same credential compromise that could delete your cluster.
- **Right-size the safety budgets.** `terminationGracePeriodSeconds` must cover the
  save-on-exit and the `startupProbe` must cover the reload, or a large dataset
  loses its shutdown save or CrashLoops on restore. See `deploy/values.yaml` and
  `docs/UPGRADE.md`.

Related: `deploy/SCALING.md` (resize safety), `docs/UPGRADE.md` (rolling upgrades),
`deploy/K8S_READINESS_PLAN.md` (the broader operational picture and the planned
operator).
