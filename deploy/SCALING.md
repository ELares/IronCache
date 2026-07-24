<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Scaling IronCache on Kubernetes

**Read this before you resize a cluster.** IronCache is a sharded, Raft-governed
store: the 16384 hash slots are partitioned across nodes and consensus membership
is quorum-sensitive. That makes it fundamentally different from a stateless
Deployment -- `kubectl scale`, HPA, and VPA are **not** safe scaling tools here,
and using them can orphan slots, split the quorum, or corrupt the slot map. This
document is the runbook for doing it correctly.

The single most important rule:

> **Never change the replica count of a running cluster with `kubectl scale`,
> HPA, or a bare `helm upgrade --set replicas=N`. Horizontal membership changes
> are a deliberate, ordered procedure -- not a knob.**

If you only remember one thing: **size the cluster correctly at install time**
and treat online resizing as an advanced, operator-driven operation.

---

## 1. Why a cache cluster is not a stateless Deployment

Three properties of IronCache make blunt replica-count changes unsafe:

1. **Slot ownership is static at bootstrap.** The 16384 slots are split evenly
   across `replicas` nodes by the chart's topology template
   (`deploy/helm/ironcache/templates/configmap.yaml`) and baked into the
   `cluster_topology` ConfigMap. Each node reads that ConfigMap at boot, matches
   its own `cluster_announce_id`, and learns exactly which slots it owns. A pod
   whose ordinal is **not** in the topology (e.g. a freshly added `-3` in a
   3-node cluster) matches nothing and boots owning **zero** slots -- it serves
   no traffic until you reshard onto it.

2. **Membership is a Raft quorum.** Availability requires a majority of voters.
   Deleting a voter without first demoting it shrinks the quorum abruptly and can
   cost you write availability (or, if you drop below majority, the whole
   cluster). This is why the replica count should stay **odd** (3, 5, 7).

3. **The static ConfigMap and the live slot map can diverge.** Once you perform
   an online reshard (below), the authoritative slot map lives in the Raft log,
   **not** in the ConfigMap. The ConfigMap is now stale. See the hazard in
   section 5 -- this is the crux of why scaling needs an operator.

Related design docs: `docs/design/NODE_LIFECYCLE.md`,
`docs/design/MIGRATION.md`, `docs/design/REBALANCING.md`,
`docs/design/CONTROL_PLANE.md`.

---

## 2. Vertical scaling (resize each node) -- the SAFE default

Giving each node more CPU / memory / disk does **not** touch membership or the
slot map, so it is the preferred way to grow capacity.

- **CPU / memory:** edit `resources.requests` / `resources.limits` and
  `helm upgrade`. This rolls the StatefulSet one pod at a time (the default
  `updateStrategy` is RollingUpdate). Each pod restart is a brief unavailability
  of that node's slots; the `minReadySeconds` + readiness gate + PodDisruptionBudget
  keep the roll safe.
  - If you raise the memory **limit**, remember IronCache auto-derives `maxmemory`
    from the cgroup limit (~70%) at boot, so the effective cache size grows with
    it (see `values.yaml` `resources.limits.memory`). No separate change needed
    unless you set `maxmemory` explicitly.
- **Disk (PVC) growth:** if your StorageClass has `allowVolumeExpansion: true`,
  raise `persistence.size` and `helm upgrade`; Kubernetes expands the PVCs in
  place. Shrinking a PVC is **not** supported -- do not attempt it.
- **Thread-per-core note:** IronCache sizes its runtime from the CPU it is
  allotted (cgroup-aware). Prefer **integer** CPU limits (or no CPU limit) so the
  per-core runtime is not CFS-throttled mid-slice; the chart ships with no CPU
  limit by default for exactly this reason.

Vertical scaling has a ceiling (one node is one machine), but within that ceiling
it is the low-risk lever. Reach for horizontal scaling only when a single node
can no longer hold its share.

---

## 3. Horizontal scale-OUT (add nodes)

**This is not a `kubectl scale` operation.** Adding a pod does not give it slots,
and a bare replica bump does not reshard. The runtime primitives to do it online
exist but are **manual** today (there is no turnkey autoscaler, and the
`CLUSTER REBALANCE APPLY` slot-moving driver is a tracked follow-up -- see
`docs/design/REBALANCE_APPLY.md`; only `REBALANCE ... DRYRUN` is wired up now).

### Preferred approach today: size correctly, re-provision if you must grow

Because online scale-out is manual and leaves the ConfigMap stale (section 5),
the least-surprising path for most operators is:

1. Provision the cluster at the replica count you need (`replicas: N`, odd).
2. If you outgrow it, stand up a **new**, larger cluster and migrate clients to
   it (dual-write / backfill / cutover), rather than growing in place.

This trades some effort for avoiding the divergence hazard entirely.

### Advanced: online scale-out (manual, operator-supervised)

If you must grow in place and understand the risks:

1. `kubectl scale statefulset/<name> --replicas N+1` (or `helm upgrade` with the
   new count). The new pod `-N` boots, but owns **zero** slots and is not yet a
   voter.
2. On a current leader, admit the node:
   `CLUSTER MEET <new-pod-dns-or-ip> <clientPort>`. It enters as a non-voting
   **learner** (receives log deltas, does not vote), per
   `docs/design/NODE_LIFECYCLE.md`.
3. Once caught up, promote it to a voter (control-plane membership change --
   single-server, no joint consensus).
4. Plan the reshard: `CLUSTER REBALANCE DRYRUN` prints which slots should move
   where. Then move them **one slot at a time** with the migration handshake:
   `CLUSTER SETSLOT <slot> IMPORTING <src>` on the destination and
   `CLUSTER SETSLOT <slot> MIGRATING <dst>` on the source, finishing with the
   Raft-committed ownership flip `CLUSTER SETSLOT <slot> NODE <dst>` (atomic and
   crash-safe; see `docs/design/MIGRATION.md`). `CLUSTER SETSLOT <slot> STABLE`
   clears migration state.
5. Verify with `CLUSTER SLOTS` that all 16384 slots are owned and the new node
   carries its share.
6. **Reconcile the ConfigMap** (section 5) so a future roll does not revert the
   new ownership.

---

## 4. Horizontal scale-IN (remove nodes)

> **`kubectl scale --replicas N-1` on a live cluster causes data loss.**

A StatefulSet scale-down deletes the **highest-ordinal pod first, immediately,
with no slot-drain hook.** That pod still owns a slot range and is a Raft voter,
so a bare scale-down orphans its slots (`cluster_state: fail` for that keyspace)
and yanks a voter from the quorum in one step.

### Safe decommission sequence

Do this **before** reducing the replica count:

1. **Drain the slots.** Migrate every slot owned by the target (highest-ordinal)
   node onto the surviving nodes with the `CLUSTER SETSLOT ... IMPORTING /
   MIGRATING / NODE` handshake from section 3. Confirm via `CLUSTER SLOTS` that
   the target owns **zero** slots.
2. **Leave the quorum cleanly.** Demote the node from voter to learner, then
   remove it from committed membership. `CLUSTER FORGET <node-id>` on every
   surviving peer drops it from their rosters.
   (`docs/design/NODE_LIFECYCLE.md`: "a node is never removed from the map while
   it still owns a slot.")
3. **Only now** reduce the count: `kubectl scale statefulset/<name>
   --replicas N-1` (or `helm upgrade`). The pod is deleted with nothing to lose.
4. **The PVC is retained by design** (the chart's
   `persistentVolumeClaimRetentionPolicy` keeps it). If the decommission is
   permanent, delete the leftover PVC manually:
   `kubectl delete pvc data-<name>-<N>`.
5. **Reconcile the ConfigMap** (section 5).

Keep the resulting voter count **odd**. Removing one node from a 3-node cluster
leaves 2 voters (no clean majority for one failure) -- prefer removing down to 1
(standalone) or not at all, and generally scale in by returning to the next odd
size.

---

## 5. THE hazard: static ConfigMap vs. live slot map divergence

This is the reason horizontal scaling needs care (and, ultimately, an operator).

- The chart renders slot ownership into the `cluster_topology` ConfigMap from
  `replicas` at **template time**.
- Any online reshard (sections 3-4) writes the new ownership into the **Raft
  log**, which becomes authoritative. The ConfigMap is now **stale**.
- A later `helm upgrade`, `kubectl rollout restart`, or even a single pod
  reschedule re-reads that stale ConfigMap. If the boot path re-applies the
  ConfigMap's slot ranges over the live Raft-committed map, **it reverts your
  reshard and can corrupt ownership** (two nodes disagreeing about who owns a
  slot).

**Mitigations, in order of preference:**

1. **Do not reshard in place.** Use the "size correctly / re-provision" path
   (section 3). No divergence is possible if the live map always matches the
   rendered one.
2. **If you did reshard, reconcile before any roll.** Re-render the ConfigMap so
   its slot ranges match the live `CLUSTER SLOTS` output, commit that to your
   values/GitOps source, and only then allow an upgrade or restart.
3. **Adopt an operator (planned).** The durable fix is a controller that owns the
   slot map, reconciles desired vs. live membership, and drives
   MEET / REBALANCE / SETSLOT / FORGET automatically -- so `replicas` becomes a
   safe declarative intent again. This is the P2 operator track in
   `deploy/K8S_READINESS_PLAN.md`.

---

## 6. Do NOT use HPA or VPA on the StatefulSet

**HorizontalPodAutoscaler is unsafe and must not target the IronCache
StatefulSet.** HPA scales replica count on CPU/memory signals, which is exactly
the blunt `kubectl scale` action forbidden above: on a scale-up it spawns
zero-slot dead pods; on a scale-down it deletes a slot-owning voter with no
drain, losing data and risking the quorum. There is no metric on which
autoscaling the membership is safe. Leave HPA off entirely.

**VerticalPodAutoscaler must be used with care, if at all.** VPA in `Auto` /
`Recreate` mode **evicts and restarts pods** to resize them -- churning a Raft
quorum on the autoscaler's schedule, potentially several nodes in a window. If
you use VPA at all, restrict it to `Off` (recommendation-only) or `Initial`
(applies only at pod creation) mode and drive the actual resize through a
controlled `helm upgrade` (section 2). Never let VPA restart a live quorum
automatically.

For load spikes, prefer vertical headroom (section 2) and cluster-aware clients
that pipeline and spread across nodes, not reactive replica autoscaling.

---

## 7. Quick reference

| Action | Safe tool | Never use |
| --- | --- | --- |
| More CPU / RAM per node | `helm upgrade` (resources.*) | -- |
| Bigger disk | `helm upgrade` (persistence.size) + expandable StorageClass | PVC shrink |
| Add a node | manual MEET + REBALANCE + SETSLOT, then reconcile ConfigMap | `kubectl scale`, HPA |
| Remove a node | drain slots -> FORGET -> `kubectl scale N-1` -> reconcile | `kubectl scale` first |
| Autoscaling | none (size correctly; vertical headroom) | HPA (any), VPA Auto/Recreate |

When in doubt, prefer standing up a correctly-sized new cluster and migrating
clients over resharding a live one. See `deploy/K8S_READINESS_PLAN.md` for the
broader operational picture and the planned operator that will make declarative
scaling safe.
