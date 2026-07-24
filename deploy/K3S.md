<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache on k3s (edge / single-node / ARM)

The chart runs on k3s as-is, and is already largely k3s-friendly: RWO PVCs, soft
anti-affinity by default, CPU-quota-aware shard sizing, multi-arch (amd64 + arm64)
distroless images, and a CRD-gated ServiceMonitor. This guide covers the handful of
k3s-specific choices and sharp edges. The `deploy/helm/ironcache/values-k3s.yaml`
overlay encodes the recommended single-node posture:

```sh
helm install ic deploy/helm/ironcache -f deploy/helm/ironcache/values-k3s.yaml
```

## Storage: the local-path provisioner

k3s's default StorageClass `local-path` is RWO, **node-local** (hostPath-backed), and
`WaitForFirstConsumer`. The chart's empty `storageClassName` selects it automatically;
the overlay pins it explicitly. Two caveats that matter:

1. **No cross-node reattach.** A pod rescheduled to a different node **cannot**
   reattach its PVC -- the snapshot + raft log are stranded on the old node. Keep the
   chart's **soft** anti-affinity (the default) so a pod stays schedulable. On k3s,
   durable recovery relies on Raft re-replication (multi-node) or backups
   (single-node), **not** PVC reattach.
2. **No volume expansion.** local-path cannot grow a PVC -- you can only recreate it.
   Size `persistence.size` with headroom up front. For real durable/expandable
   storage, point `storageClassName` at Longhorn or an external CSI driver.

## Single-node posture

The intended single-node config is `replicas: 1` + `cluster.enabled: false`
(standalone). The chart renders no PDB below 3 replicas and stamps no cluster
identity. Do **not**:

- run a 1-node "cluster" (`cluster.enabled: true`, `replicas: 1`) -- it triples memory
  for zero fault tolerance; or
- run `replicas: 2` -- two nodes have no quorum majority.

For real HA on k3s, use **>= 3 nodes** with `cluster.enabled: true` **and** a
non-local-path StorageClass (Longhorn/CSI) so PVCs survive a node loss. See
`deploy/SCALING.md` and `docs/UPGRADE.md`.

On a single node, save-on-exit + adequate `terminationGracePeriodSeconds` is the only
thing between a drain and data loss (no peer to re-replicate from) -- keep a save
policy (`saveIntervalSecs > 0`, the default) and size the grace period to your dataset.

## Memory (the most important k3s setting)

An unbounded cache on a shared edge node OOMKills itself **and** evicts co-tenant
workloads. Always set `resources.limits.memory`: IronCache auto-derives `maxmemory`
from the cgroup limit (~70%) at boot, so it EVICTS (allkeys-lru) under pressure instead
of being OOMKilled. The overlay sets a 1Gi limit; tune it to your box. (On local-path
the cgroup-reader path is what matters -- you cannot grow the PVC regardless.)

## External access (RESP is L4)

RESP is a TCP/L4 protocol, so a Traefik **HTTP** Ingress does not front the cache. Keep
`service.type: ClusterIP` for in-cluster clients. For external RESP, choose one:

- **NodePort** (`service.type: NodePort`) -- simplest, always available.
- **servicelb / klipper LoadBalancer** (`service.type: LoadBalancer`) -- k3s's built-in
  bare-metal LB.
- **Traefik `IngressRouteTCP`** -- needs the Traefik CRD, which stock k3s bundles but
  hardened/air-gapped builds (`k3s --disable traefik`) omit; without it there is no
  IngressRouteTCP path, so fall back to NodePort.

A single LB in front of a multi-node Raft cluster is **not** cluster-aware (nodes answer
`MOVED` for slots they do not own) -- prefer cluster-aware clients on the per-pod
headless DNS. (Moot at `replicas: 1`.) The optional console UI is plain HTTP and can use
a normal Traefik Ingress.

## Air-gapped installs

Pre-load **three** images (the chart keeps `pullPolicy: IfNotPresent`):

- `ghcr.io/elares/ironcache`
- `busybox:1.36` -- the **easy-to-miss** init-container dependency
- `ghcr.io/elares/ironcache-console` (only if you enable the console)

Import them with `k3s ctr images import <tarball>` or drop tarballs into
`/var/lib/rancher/k3s/agent/images/`. Leave `metrics.serviceMonitor.enabled: false`
(the default) since the Prometheus Operator CRD is usually absent.

## NetworkPolicy

k3s's default **flannel** CNI does not enforce NetworkPolicy, so `networkPolicy.enabled`
(and the console policy) are **inert / fail-open**. For real isolation, run Calico,
Cilium, or the kube-router policy controller.

## Backup is mandatory here

On single-node local-path, PVC or node loss = **total data loss** -- there is no peer to
re-replicate from. Set up an off-node backup before storing anything you care about. See
**`deploy/BACKUP.md`** (the app-level file-copy method is the local-path path; CSI
VolumeSnapshot is unavailable on local-path).
