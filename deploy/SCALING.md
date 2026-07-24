<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Scaling IronCache on Kubernetes

**Read this before you resize a cluster.** IronCache is a sharded, Raft-governed
store: the 16384 hash slots are partitioned across nodes and consensus membership
is quorum-sensitive. That makes it fundamentally different from a stateless
Deployment -- `kubectl scale`, HPA, and VPA are **not** safe scaling tools here.
A bare scale-up leaves the new pod **crash-looping** (it is not in the cluster
topology, so it refuses to boot); a bare scale-down **orphans a slot range and
yanks a Raft voter**. This document is the runbook for doing it correctly.

The single most important rule:

> **Never change the replica count of a running cluster with `kubectl scale`,
> HPA, or a bare `helm upgrade --set replicas=N`. Horizontal membership changes
> are a deliberate, multi-step, operator-supervised procedure -- not a knob.**

The strongest recommendation in this document: **size the cluster correctly at
install time**, and when you outgrow it prefer standing up a new, correctly-sized
cluster and migrating clients over resizing a live one. Online in-place resizing
is possible but is a **manual, not-yet-turnkey** procedure today (see the gaps in
section 3); there is no autoscaler and no fully-wired one-command resize.

---

## 1. Why a cache cluster is not a stateless Deployment

Three properties of IronCache make blunt replica-count changes unsafe:

1. **Slot ownership is static at bootstrap.** The 16384 slots are split evenly
   across `replicas` nodes by the chart's topology template
   (`deploy/helm/ironcache/templates/configmap.yaml`) and baked into the
   `cluster_topology` ConfigMap. An init container stamps each pod's stable
   `cluster_announce_id` from its ordinal, and **boot requires that id to be
   present in the topology** -- a cluster-enabled pod whose id is not listed
   (e.g. a freshly added ordinal `-3` in a 3-node cluster) fails config
   validation and **crash-loops**; it does not come up as an idle empty node.

2. **Membership is a Raft quorum.** Availability requires a majority of voters.
   Deleting a voter without first demoting it shrinks the quorum abruptly and can
   cost you write availability (or, below majority, the whole cluster). Keep the
   replica count **odd** (3, 5, 7).

3. **The static ConfigMap and the live slot map can diverge.** Once you perform
   an online reshard (section 3), the authoritative slot map lives in the Raft
   log, not in the ConfigMap. The ConfigMap is now a stale description of
   reality. This does **not** silently corrupt a running cluster (the Raft log
   wins on restart -- section 5), but it has real consequences you must manage.

Related design docs: `docs/design/NODE_LIFECYCLE.md`,
`docs/design/MIGRATION.md`, `docs/design/REBALANCING.md`,
`docs/design/REBALANCE_APPLY.md`, `docs/design/CONTROL_PLANE.md`.

---

## 2. Vertical scaling (resize each node) -- the SAFE default

Giving each node more CPU / memory / disk does **not** touch membership or the
slot map, so it is the preferred way to grow capacity.

- **CPU / memory:** edit `resources.requests` / `resources.limits` and
  `helm upgrade` **without changing `replicas`**. This rolls the StatefulSet one
  pod at a time (RollingUpdate); `minReadySeconds` + the readiness gate + the
  PodDisruptionBudget keep the roll safe.
  - If you raise the memory **limit**, IronCache auto-derives `maxmemory` from the
    cgroup limit (~70%) at boot, so the effective cache size grows with it (see
    `values.yaml` `resources.limits.memory`). No separate change is needed unless
    you set `maxmemory` explicitly.
- **Disk (PVC) growth:** if your StorageClass has `allowVolumeExpansion: true`,
  raise `persistence.size` and `helm upgrade`; Kubernetes expands the PVCs in
  place. Shrinking a PVC is **not** supported -- do not attempt it.
- **Thread-per-core note:** IronCache sizes its runtime from the CPU it is
  allotted (cgroup-aware). Prefer **integer** CPU limits (or no CPU limit) so the
  per-core runtime is not CFS-throttled mid-slice; the chart ships with no CPU
  limit by default for exactly this reason.

Vertical scaling has a ceiling (one node is one machine), but within it, it is the
low-risk lever. Reach for horizontal scaling only when a single node can no longer
hold its share.

---

## 3. Horizontal scale-OUT (add nodes) -- manual, not turnkey

**This is not a `kubectl scale` operation, and it is not one command.** The
Raft-mode primitives to do it online exist and each works, but there is **no
automation stitching them together** and **no integration test of a full
add-a-new-node cycle** -- treat online scale-out as an advanced, operator-driven
procedure and validate it in staging first.

### Preferred approach: size correctly, or re-provision to grow

Because online scale-out is manual and leaves the ConfigMap stale (section 5), the
least-surprising path for most operators is:

1. Provision at the replica count you need (`replicas: N`, odd).
2. If you outgrow it, stand up a **new**, larger cluster and migrate clients to it
   (dual-write / backfill / cutover) rather than growing in place.

### Advanced: online scale-out (manual, operator-supervised)

If you must grow in place and accept the manual steps and risks:

1. **Make the new pod bootable first.** A new pod cannot boot until it is in the
   topology, so a bare `kubectl scale`/replica bump alone yields a crash-looping
   pod. Add the new node's `[[cluster_topology.nodes]]` entry (its stamped id +
   headless-Service host; `slots = []`) to the rendered topology, and set the new
   pod's `cluster_raft_joining = true` (env `IRONCACHE_CLUSTER_RAFT_JOINING`, #663)
   so it joins as a non-voter and learns membership from the log instead of
   expecting to be a founding voter. **The chart does not template this grow path
   today -- it is a hand-edited ConfigMap change.** Roll so the new pod boots; it
   comes up owning **zero** slots.
2. **Admit it to the quorum.** On a current leader:
   `CLUSTER MEET <new-pod-dns> <clientPort>`. It enters as a non-voting **learner**
   and is auto-promoted to voter once caught up (`docs/design/NODE_LIFECYCLE.md`).
3. **Arm the slot copies.** `CLUSTER REBALANCE DRYRUN` prints the target
   distribution; `CLUSTER REBALANCE APPLY` then arms the migration copies
   (`SETSLOT ... MIGRATING`/`IMPORTING`, batched) toward an even spread across all
   known nodes. Note APPLY balances toward **equal** ownership; it has no per-node
   weighting.
4. **Finalize each slot by hand.** APPLY does **not** commit the ownership flip.
   For each slot, once `CLUSTER COUNTKEYSINSLOT <slot>` shows the destination
   caught up, run `CLUSTER SETSLOT <slot> NODE <new-id>` to flip ownership
   (Raft-committed, atomic; `docs/design/MIGRATION.md`). A background auto-flip
   controller is a tracked follow-up (`docs/design/REBALANCE_APPLY.md`).
5. **Verify** with `CLUSTER SLOTS` that all 16384 slots are owned and the new node
   carries its share.
6. **Reconcile the rendered topology** (section 5) so a future re-provision does
   not fall back to the old layout.

**Gaps to be aware of (all pending the planned operator):** (a) a new pod cannot
boot until it is hand-added to the topology; (b) `REBALANCE APPLY` never
finalizes -- you flip every slot by hand; (c) there is no end-to-end test of
adding a node that was not in the boot topology, so exercise it in staging.

---

## 4. Horizontal scale-IN (remove nodes) -- fully manual per-slot

> **`kubectl scale --replicas N-1` on a live cluster causes data loss.**

A StatefulSet scale-down deletes the **highest-ordinal pod first, immediately,
with no slot-drain hook.** The chart's `preStop` is only a lame-duck sleep (it
deprograms the Service endpoint); it does **not** hand off slots. That pod still
owns a slot range and is a Raft voter, so a bare scale-down orphans its slots
(`cluster_state: fail` for that keyspace) and yanks a voter from the quorum in one
step.

`CLUSTER REBALANCE APPLY` does **not** help here: it spreads slots evenly across
all known nodes and cannot target a node to zero, so it will never drain a node
for decommission. Scale-in is entirely manual.

### Safe decommission sequence

Do this **before** reducing the replica count. To drain the highest-ordinal node
`-N` to zero slots:

1. **Migrate every slot off the target.** For each slot `-N` owns:
   `CLUSTER SETSLOT <slot> MIGRATING <survivor-id>` on the source and
   `CLUSTER SETSLOT <slot> IMPORTING <target-id>` on the destination (arms the
   copy); once `CLUSTER COUNTKEYSINSLOT <slot>` shows the survivor caught up,
   `CLUSTER SETSLOT <slot> NODE <survivor-id>` to flip. Repeat until
   `CLUSTER SLOTS` shows the target owns **zero** slots.
2. **Leave the quorum cleanly.** `CLUSTER FORGET <target-id>` on every surviving
   peer. FORGET is **refused while the node still owns any slot**
   (`NodeOwnsSlots`), which enforces step 1; it also demotes the node from voter
   before removing it from committed membership
   (`docs/design/NODE_LIFECYCLE.md`: "a node is never removed from the map while
   it still owns a slot").
3. **Only now** reduce the count: `kubectl scale statefulset/<name>
   --replicas N-1` (or `helm upgrade`), and shrink the topology ConfigMap to
   match.
4. **The PVC is retained by design** (the chart's
   `persistentVolumeClaimRetentionPolicy`). If the decommission is permanent,
   delete the leftover PVC manually: `kubectl delete pvc data-<name>-<N>`.

Keep the resulting voter count **odd**. Removing one node from a 3-node cluster
leaves 2 voters (no clean majority for one failure) -- prefer removing down to 1
(standalone) or returning to the next odd size.

---

## 5. Divergence: the static ConfigMap vs. the live slot map

This is the reason horizontal scaling needs care (and, ultimately, an operator) --
but it is **not** the "a restart silently corrupts my cluster" story it might
first appear to be.

**What is safe:** the **Raft log wins on restart.** The chart's turnkey bootstrap
that applies the ConfigMap's slot layout runs **only on a genuinely fresh
cluster** -- it is hard-gated on there being no persisted Raft log and a pristine
committed config (`crates/ironcache/src/turnkey_bootstrap.rs`), with a regression
test that a no-snapshot restart does **not** re-apply the layout. And a topology
that conflicts with committed membership makes a node **refuse to boot** rather
than silently overwrite it. So a routine `helm upgrade`, `kubectl rollout
restart`, or single-pod reschedule after an online reshard does **not** revert
your slot moves.

**What is genuinely at risk after a reshard:**

1. **The ConfigMap becomes a misleading source of truth.** Anything that reads or
   re-derives topology from it -- an operator eyeballing the manifest, a GitOps
   diff, a fresh re-render from `replicas` -- now describes a layout the live
   cluster no longer has.
2. **A fresh re-provision falls back to the stale layout.** If you wipe state
   (delete the PVCs / Raft logs) and reinstall, the cluster bootstraps from the
   ConfigMap's slot ranges, which are the *pre-reshard* layout, not your live one.
3. **A conflicting topology edit can block boot.** If you later change `replicas`
   (or the slot ranges) in values and upgrade, a node whose new topology conflicts
   with committed membership will refuse to start. That is a fail-safe (no
   corruption), but it is still an outage of that pod until reconciled.

**Mitigations, in order of preference:**

1. **Do not reshard in place.** Use the "size correctly / re-provision" path
   (section 3). No divergence is possible if the live map always matches the
   rendered one.
2. **If you did reshard, reconcile the source.** Re-render the topology so its
   slot ranges match live `CLUSTER SLOTS`, and commit that to your values / GitOps
   source before any re-provision.
3. **Adopt an operator (planned).** The durable fix is a controller that owns the
   slot map, reconciles desired vs. live membership, and drives
   MEET / REBALANCE / SETSLOT / FORGET (including the ownership flips)
   automatically -- so `replicas` becomes a safe declarative intent again. This is
   the P2 operator track in `deploy/K8S_READINESS_PLAN.md`.

---

## 6. Do NOT use HPA or VPA on the StatefulSet

**HorizontalPodAutoscaler is unsafe and must not target the IronCache
StatefulSet.** HPA scales replica count on CPU/memory signals -- exactly the blunt
replica change forbidden above: on a scale-up it spawns pods that crash-loop (not
in the topology); on a scale-down it deletes a slot-owning voter with no drain,
losing data and risking the quorum. There is no metric on which autoscaling the
membership is safe. Leave HPA off entirely.

**VerticalPodAutoscaler must be used with care, if at all.** VPA in `Auto` /
`Recreate` mode **evicts and restarts pods** to resize them -- churning a Raft
quorum on the autoscaler's schedule, potentially several nodes in a window. If you
use VPA at all, restrict it to `Off` (recommendation-only) or `Initial` (applies
only at pod creation) mode and drive the actual resize through a controlled
`helm upgrade` (section 2). Never let VPA restart a live quorum automatically.

For load spikes, prefer vertical headroom (section 2) and cluster-aware clients
that pipeline and spread across nodes, not reactive replica autoscaling.

---

## 7. Quick reference

| Action | Safe tool | Never use |
| --- | --- | --- |
| More CPU / RAM per node | `helm upgrade` (resources.*), replicas unchanged | -- |
| Bigger disk | `helm upgrade` (persistence.size) + expandable StorageClass | PVC shrink |
| Add a node | expand topology ConfigMap + roll -> MEET -> REBALANCE APPLY -> **manual per-slot SETSLOT NODE** -> reconcile | `kubectl scale`, HPA |
| Remove a node | drain slots (manual per-slot SETSLOT) -> FORGET -> `kubectl scale N-1` -> reconcile | `kubectl scale` first |
| Autoscaling | none (size correctly; vertical headroom) | HPA (any), VPA Auto/Recreate |

Online scale-out and scale-in are **manual, operator-supervised** procedures
today: the Raft primitives (MEET / REBALANCE APPLY / SETSLOT / FORGET) each work,
but stitching them into a safe end-to-end resize -- making a new pod bootable,
finalizing each slot flip, draining a node -- is not yet automated or
end-to-end-tested. When in doubt, prefer standing up a correctly-sized new cluster
and migrating clients. See `deploy/K8S_READINESS_PLAN.md` for the broader
operational picture and the planned operator that will make declarative scaling
safe.
