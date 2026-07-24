# IronCache on Kubernetes + k3s: Comprehensive Readiness Plan

## 1. Executive Summary

IronCache's Kubernetes scaffolding is **genuinely mature and, in several dimensions, convention-leading**  -- not a checkbox exercise. The hard, easy-to-get-wrong parts of running a Raft-clustered stateful cache on k8s are already correct:

- **Workload & identity:** StatefulSet (not Deployment), `podManagementPolicy: Parallel` for quorum bootstrap, headless Service with `publishNotReadyAddresses: true`, deterministic per-pod `cluster_announce_id` stamped from the ordinal by an init container, ConfigMap-rendered topology with even 16384-slot split  -- all present and correct.
- **Availability:** quorum-aware PDB (rendered only for N≥3, `maxUnavailable: 1`), soft/hard podAntiAffinity, resource requests + memory limit (CPU limit deliberately omitted to avoid CFS throttling).
- **Lifecycle:** a *substantive* `/readyz` (per-shard load-on-boot AND-reduce + raft-leader-known, 503 until loaded) that correctly paces rolling upgrades, `/livez` that only flips at end-of-boot, SIGTERM save-on-exit with bounded drain, `checksum/config` annotation to roll pods on config change.
- **Security:** PSS *restricted*-compliant (nonroot 65532, `readOnlyRootFilesystem`, drop ALL caps, seccomp RuntimeDefault, fsGroup for PVC), distroless/shell-free runtime image, Secret-backed credentials with `existingSecret` delegation.
- **Cluster day-2 (CLI):** turnkey fresh-cluster auto-apply (fresh-only + idempotent), and the #392 RPO=0 failover-freeze rolling-upgrade driver  -- the actual crown jewel.
- **Observability:** `/metrics`, gated ServiceMonitor, a full Prometheus alert bundle and Grafana dashboard.
- **CPU cgroup awareness:** `default_shards()` uses `available_parallelism()` (honors CFS quota), so the thread-per-core runtime auto-right-sizes on constrained nodes.

**Headline gaps** (concentrated, not pervasive):

1. **`maxmemory` is not derived from the pod memory limit**  -- `maxmemory` defaults to `0` (unlimited) and nothing wires `resources.limits.memory` to it. On a limited pod this OOMKills (exit 137, data loss) instead of evicting. **This is the single biggest correctness gap**  -- and it is only a real guard if `maxmemory-policy` is simultaneously set to a non-`noeviction` value (default `noeviction` returns OOM errors to clients rather than evicting).
2. **No `startupProbe`**  -- a large snapshot reload can exceed the implicit ~65s liveness budget and CrashLoopBackOff exactly the persistence-heavy deployments that need warm restart most.
3. **No `preStop` lame-duck hook**  -- the endpoint-deprogramming race causes client-visible resets on every rolling update; and because the image is **distroless/shell-free**, the obvious `exec: sleep` hook would silently fail  -- the native `SleepAction` is the only correct form.
4. **Scaling is not `kubectl scale`-safe**  -- a static topology is rendered at install time; changing replicas does not reslot. Naive scale-out/in orphans slots (data loss / `cluster_state:fail`). Worse, `kubectl scale`-in deletes the highest-ordinal pod *first, immediately*, making a "drain-then-remove" sequence impossible to interleave  -- scale-in cannot use `kubectl scale` at all on a sharded cluster.
5. **`terminationGracePeriodSeconds: 60` is a fixed foot-gun** for large exit-saves; no size-aware guidance.
6. **No backup / restore / DR story at all**  -- no snapshot+raft-log backup, no VolumeSnapshot guidance, no restore runbook, no RPO/RTO framing. A glaring omission for a "production-readiness" plan, and acute on k3s local-path where PVC loss = data loss.
7. **No k3s story**  -- zero mentions of local-path, Traefik, servicelb, air-gapped, single-node anywhere.
8. **Packaging polish**  -- no `values.schema.json`, no `helm test`/`ct`, no OCI publish, `appVersion: latest`.

**A factual correction that runs through the whole plan:** the PDB does **not** pace or gate the rolling upgrade. StatefulSet rolling updates delete pods directly and are **not limited by PodDisruptionBudgets**  -- PDBs only gate *voluntary evictions* via the Eviction API (node drain, autoscaler, `kubectl drain`). Rollout safety rests entirely on the **`/readyz` readiness gate + one-at-a-time ordinal serialization of `RollingUpdate`**, plus `minReadySeconds` as the soak lever. The PDB's real job is protecting quorum against *involuntary* disruption (node drain / autoscaler). Every "PDB paces the rollout" claim below has been corrected accordingly.

Bottom line: **the data-plane and cluster mechanisms are production-grade; the gaps are almost entirely in the packaging/config-wiring layer and in documentation.** Most are S/M effort. The one architectural question is whether an Operator is needed for day-2 scaling (Section 5)  -- and the sharded-reshard story makes that case stronger than the draft implied.

---

## 2. Convention Checklist → Status Table

| Convention | Status | Note |
|---|---|---|
| StatefulSet (not Deployment) | **DONE** | Stable ordinal identity + sticky PVCs; `statefulset.yaml`. |
| Headless Service + `publishNotReadyAddresses` | **DONE** | `clusterIP: None` + not-ready peers published for boot-time Raft discovery. DNS-propagation caveat: raft peer-join must retry, not fail-fast (M4). |
| Separate client ClusterIP Service | **DONE** | With documented MOVED / cluster-aware-client caveat. |
| `volumeClaimTemplates` + StorageClass + RWO | **DONE** | Per-pod sticky PVC at `data_dir`, settable class, emptyDir fallback. |
| `persistentVolumeClaimRetentionPolicy: Retain` | **MISSING** | Never set; must be explicitly `Retain` (never `whenScaled: Delete`) so scale-in / rollout never nukes a data PVC. |
| `podManagementPolicy: Parallel` | **DONE** | Correct for quorum bootstrap (avoids OrderedReady deadlock). Note: does **not** affect rolling updates (always one-at-a-time by ordinal). |
| Quorum-preserving PDB (N≥3 only, odd N) | **DONE (scope corrected)** | `maxUnavailable: 1`; distinct selector vs console. Protects against **involuntary** disruption (drain/autoscaler)  -- does **NOT** gate rolling upgrades. |
| podAntiAffinity (soft default / hard opt-in) | **DONE** | `topologyKey: kubernetes.io/hostname`. |
| `topologySpreadConstraints` (zone spread) | **MISSING** | No zone-aware spread; a zone outage can kill quorum in multi-AZ. |
| Resource requests + limits; no CPU limit | **DONE** | Burstable posture, memory-limit-above-maxmemory documented. |
| Guaranteed-QoS opt-in documented | **MISSING** | The requests==limits knob for strongest eviction protection undocumented. |
| Liveness `/livez` + readiness `/readyz` (membership-aware) | **DONE** | `/readyz` = per-shard AND-reduce + leader-known  -- best-in-class. Verify listener binds *before* snapshot reload (C3). |
| `startupProbe` for slow snapshot reload | **MISSING** | Large restore can trip liveness (~65s) → CrashLoopBackOff. |
| `preStop` lame-duck hook (native SleepAction) | **MISSING** | Endpoint-deprogram race; exec-sleep would break on distroless. |
| Readiness flips to 503 on drain | **MISSING** | Shutdown flag not surfaced in `readiness()`. |
| `terminationGracePeriodSeconds` sized to drain+save | **PARTIAL** | Fixed 60s; no size-aware guidance; risks SIGKILL mid-save. |
| `minReadySeconds` (rollout soak) | **MISSING** | The *actual* soak lever for a safe rollout (not the PDB). |
| Explicit `updateStrategy: RollingUpdate` (+ partition) | **PARTIAL** | Relies on k8s default; no partition/canary lever exposed. |
| `revisionHistoryLimit` | **MISSING** | Defaults to 10; unbounded with manual patches. |
| Security context PSS *restricted* | **DONE** | nonroot 65532, RO-root, drop ALL, seccomp; init container too. |
| `fsGroupChangePolicy: OnRootMismatch` | **UNVERIFIED** | Needed or every mount does a recursive chown on large PVCs → slow restarts (worsens startupProbe budget). Confirm + set. |
| `automountServiceAccountToken: false` | **MISSING** | Cache pods + init need no API access; standard restricted hardening. |
| PSA namespace-enforce label guidance | **MISSING** | Chart complies but doesn't recommend the enforce label. |
| ConfigMap + Secret + `checksum/config` roll | **DONE** | Full topology in ConfigMap; checksum annotation rolls pods. |
| `existingSecret` delegation | **DONE** | Keeps creds out of Helm history. |
| Cluster secret stable across upgrade | **PARTIAL** | `randAlphaNum` regenerates on bare `helm upgrade` → split-brain; warned in NOTES only. |
| Init-container ordinal identity stamp | **DONE** | `stamp-identity.sh`; TOML prepend gotcha handled. |
| Turnkey fresh-cluster auto-apply (fresh-only, idempotent) | **DONE** | Integration-tested; restart doesn't re-bootstrap. |
| Readiness-gated one-at-a-time rolling upgrade | **DONE** | `/readyz` gate + ordinal serialization pace the rollout (NOT the PDB). |
| Primary-failover-before-drain (RPO=0) | **PARTIAL** | #392 mechanism exists but is CLI-only, **not** wired as preStop/operator. |
| Online scale-out (learner-join + reshard) | **PARTIAL** | Runtime primitives exist (MEET/REBALANCE/SETSLOT/FORGET) but manual; chart replica bump ≠ reshard; runtime slot map diverges from static ConfigMap after any reshard (S2). |
| CPU cgroup awareness (shards) | **DONE** | `available_parallelism()` honors CFS quota. |
| Memory cgroup awareness (maxmemory from limit) | **MISSING** | No `memory.max` reader; `maxmemory=0`; not wired from limit → **OOMKill**. Requires non-`noeviction` policy to actually guard. |
| No HPA / VPA-caution documented | **MISSING** | HPA on a sharded raft store = automated orphaned-slot/re-election disaster; must be explicitly forbidden. |
| Backup / restore / DR (snapshot, VolumeSnapshot, PITR) | **MISSING** | No backup CronJob, no CSI-snapshot guidance, no restore runbook, no RPO/RTO. |
| ServiceMonitor (CRD-gated) | **DONE** | Rendered only when enabled; correct for CRD-less k3s. |
| Prometheus alerts + Grafana dashboard | **PARTIAL** | Both exist as files; dashboard not auto-provisioned via ConfigMap. |
| Cache-pods NetworkPolicy | **MISSING** | Only console has one; cluster-bus/repl ports wide open. |
| `values.schema.json` | **MISSING** | No install-time validation of cross-field invariants. |
| `helm test` + `ct` (kind/k3d install) | **MISSING** | Only static render/kubeconform today. |
| OCI chart publish + signing | **MISSING** | Checkout-only; not GitOps/Argo-consumable. |
| `appVersion` / default `image.tag` pinned | **PARTIAL** | Both `latest`; not reproducible. |
| Ingress template (console UI) | **MISSING** | No Ingress; RESP is L4 so needs TCP path documented. |
| k3s guidance (local-path/Traefik/servicelb/air-gap) | **MISSING** | Zero k3s notes in deploy tree/DEPLOY.md. |

---

## 3. Prioritized Gap-Closure Roadmap

### P0  -- Correctness / safety blockers

**P0-1 · Derive `maxmemory` from the pod memory limit AND set an evicting policy (OOMKill guard)**  -- effort **M**

This is the single biggest correctness gap. Two halves; **prefer the Downward-API + binary path as the robust one** (the review corrected the draft's ordering emphasis).

- **Robust path (preferred)  -- Downward API + binary fraction:** inject the limit via Downward API `resourceFieldRef: { resource: limits.memory }` as an env var (e.g. `IRONCACHE_MEMORY_LIMIT_BYTES`), which resolves to **raw bytes at runtime** and sidesteps Helm/Sprig's lack of a Kubernetes-quantity byte parser (`Gi` vs `G`, `Mi` vs `M`). The binary computes `maxmemory = fraction × limit` (fraction default ~0.70, a values knob). **Critical guard:** `resourceFieldRef` on `limits.memory` **falls back to node allocatable when no limit is set**  -- so the binary must distinguish "limit was set" from "fell back to node capacity" (e.g. only apply the derivation when the chart also sets an explicit `IRONCACHE_MEMORY_LIMIT_PRESENT=1`, rendered only when `resources.limits.memory` is non-empty) or it will size maxmemory to the whole node.
- **Binary cgroup reader (cleanest long-term, matches ADR-0007):** add a cgroup `memory.max` reader (v2 `/sys/fs/cgroup/memory.max`, v1 `memory.limit_in_bytes`, `max`/sentinel → unlimited, fall back to `/proc/meminfo`). Copy the existing `handoff.rs` `#[cfg(target_os="linux")]` + graceful-`None` pattern. When `maxmemory` is unset, default it to a fraction of the detected limit at boot. This makes the guard robust to VPA / in-place limit changes without re-render.
- **Fragile path (fallback only)  -- pure-template byte math:** rendering `maxmemory = <fraction of parsed resources.limits.memory>` directly in `_helpers.tpl` is error-prone precisely because of the quantity-suffix parsing; use only if a runtime env path is impossible.
- **The other required half  -- eviction policy:** `maxmemory` alone does nothing useful if `maxmemory-policy` is `noeviction` (the default returns OOM errors to clients instead of evicting). The chart must render a non-`noeviction` policy (e.g. `allkeys-lru`) as the default-when-derivation-is-active, exposed as `maxmemory_policy` with an enum in the schema (P2-2). **Without this, the OOM guard silently doesn't guard.**
- **Files:** `values.yaml` (resources block + `maxmemoryFraction` + `maxmemory_policy`), `templates/statefulset.yaml` (Downward-API env + presence flag), `templates/configmap.yaml` (policy render), `_helpers.tpl` (only if template math path is used); binary: new reader in `ironcache-config` near `default_shards()` (`lib.rs:167`), default-application at `lib.rs:764`.
- **Why:** with `maxmemory=0` and a 2Gi limit the working set grows unbounded → OOMKill (exit 137) + data loss + cluster disruption  -- the exact failure a maxmemory cache exists to prevent. Redis-in-k8s guidance is explicit: limit ~50% above maxmemory, let eviction reclaim.

**P0-2 · Add a `startupProbe` (slow-snapshot warm restart)**  -- effort **S**
- **Change:** add `startupProbe` (httpGet `/readyz`, `periodSeconds: 10`, `failureThreshold` large + tunable, e.g. 60 = 10-min budget). While unsatisfied it suspends **both** liveness and readiness. Then drop `initialDelaySeconds` to 0 on liveness/readiness. No new server code  -- `/readyz` already reports load-incomplete.
- **Correctness precondition (C3):** confirm the **metrics HTTP listener binds *before* the snapshot reload begins**. If the listener only comes up post-reload, the startupProbe gets connection-refused for the whole reload window  -- it still works (budget is `failureThreshold × periodSeconds` either way), but the "substantive `/readyz` paces the reload" claim is only true once the listener is bound. If it does not bind early, either move the bind earlier or accept connection-refused semantics and document it.
- **Shared sizing:** the startup budget and `terminationGracePeriodSeconds` (P0-4) must **derive from the same dataset-size estimate** so they don't drift: `budget ≥ worst-case-snapshot-bytes / observed-reload-throughput + margin`.
- **Files:** `templates/statefulset.yaml`, raw `k8s/ironcache.yaml`, `values.yaml` (probe knobs).
- **Why:** the current implicit liveness budget (~5s + 6×10s ≈ 65s) crash-loops a healthy node doing a multi-GB reload  -- precisely the persistence-heavy deployments that most need warm restart.

**P0-3 · Add a `preStop` lame-duck hook using native `SleepAction`**  -- effort **S**
- **Change:** `lifecycle.preStop.sleep` (KEP-3960, k8s 1.29+/GA 1.30), duration a values knob (`preStop.sleepSeconds`, default ~10; ~5 single-node k3s). **Must NOT use `exec: [/bin/sh,-c,sleep N]`**  -- the distroless/shell-free image has no shell or `sleep`, so it would silently fail. `SleepAction` is compatible with `readOnlyRootFilesystem`/drop-ALL precisely because it needs no in-image binary. Gate the block on a `kubeVersion` capability check (Chart.yaml has no floor); fall back to readiness-fail-on-drain (P0-5) where `SleepAction` is unavailable (old air-gapped k3s).
- **Files:** `templates/statefulset.yaml`, `k8s/ironcache.yaml`, `values.yaml`, `Chart.yaml` (kubeVersion note).
- **Why:** kubelet SIGTERM and endpoint-deprogramming race; without the lame-duck, in-flight clients are routed to a draining pod → resets on every rolling update.

**P0-4 · Size `terminationGracePeriodSeconds` as a documented, dataset-size-aware knob**  -- effort **S/M**
- **Change:** keep the knob, but document that grace must cover `preStop.sleepSeconds + DRAIN_GRACE(5s) + worst-case save-on-exit + margin`, derived from the **same dataset-size estimate** as P0-2. Recommend 120-180s for datasets >5GB or slow storage. Optionally warn in `_helpers.tpl`/NOTES if grace looks too small.
- **Files:** `values.yaml` (comment + guidance), `NOTES.txt`, DEPLOY.md/SHUTDOWN.md cross-ref.
- **Why:** if grace < sleep+drain+save, kubelet SIGKILLs mid-save, truncating the snapshot and violating SHUTDOWN.md's "exit 0 iff save durable" contract.

**P0-5 · Flip readiness to 503 during drain**  -- effort **S** (server-side)
- **Change:** surface the existing shutdown `AtomicBool` in `metrics_http` `readiness()` so `/readyz` returns 503 once drain starts (belt-and-suspenders with P0-3's preStop; also the fallback when `SleepAction` is unavailable).
- **Files:** `metrics_http.rs` `readiness()`.
- **Why:** the preStop sleep covers the propagation race; readiness-fail covers any client that re-resolves during the drain window.

### P1  -- Production hardening

**P1-1 · Explicit `updateStrategy` + `partition` passthrough**  -- effort **S**
- `updateStrategy: { type: RollingUpdate, rollingUpdate: { partition: {{ .Values.updateStrategy.partition | default 0 }} } }` in `statefulset.yaml`. Document canary: patch `partition=N-1`, verify, step down.
- **Why:** makes the readiness-gated, ordinal-serialized upgrade *visible* and gives operators the canary lever. Document its relationship to the #392 driver  -- they must not fight.

**P1-2 · `topologySpreadConstraints` passthrough (zone spread)**  -- effort **M**
- Optional `topologySpreadConstraints.enabled` + `maxSkew` (default 1) over `topology.kubernetes.io/zone`; document as OPTIONAL / multi-zone only.
- **Why:** hostname anti-affinity spreads across nodes only; a zone outage can drop 2 of 3 pods → quorum loss.

**P1-3 · Cache-pods NetworkPolicy**  -- effort **M**
- New `templates/networkpolicy.yaml`, gated `networkPolicy.enabled` (default false). Ingress: clients on client port, console on metrics port, self on cluster-bus/repl; egress: self bus/repl, DNS. Mirror `console-networkpolicy.yaml`.
- **Why:** cluster-bus (port+10000) and repl (port+20000) are unguarded; on a policy-enforcing CNI a foreign-namespace pod can forge RAFTMSG / pull replication.

**P1-4 · `minReadySeconds` + `revisionHistoryLimit`**  -- effort **S**
- `minReadySeconds: 15-30` (soak so "ready" means "stably ready"  -- **this, not the PDB, is what keeps a rollout from marching on a flapping raft pod behind one-at-a-time ordinal**), `revisionHistoryLimit: 3`.
- **Why:** minReadySeconds is the real rollout soak lever (see the C1 correction); revisionHistoryLimit bounds etcd bloat from manual partition patches.

**P1-5 · Guard the cluster secret against `helm upgrade` rotation**  -- effort **M**
- For a real cluster, require `clusterSecret.value` or `existingSecret` (fail-closed when `cluster.enabled` and both empty), instead of silently `randAlphaNum` per install.
- **Files:** `secret.yaml`, `values.schema.json` (P2-2), `NOTES.txt` (keep the warning prominent).
- **Why:** a bare upgrade regenerates the peer-handshake secret and split-brains the mesh  -- a correctness issue, not just security.

**P1-6 · Wire #392 failover-freeze as a preStop / document the rolling-restart gap**  -- effort **M/L**
- Investigate wiring the RPO=0 fence into a preStop step for a leader/primary pod (needs a shell-free trigger  -- the admin/metrics HTTP surface or a signal like the SIGUSR1 cutover #638). At minimum, document that a *plain* StatefulSet rollout does NOT auto-fence the leader (use `ironcache upgrade --cluster`).
- **Why:** a naive pod delete forces a post-death election + MOVED storm and risks RPO>0; the mechanism exists but isn't StatefulSet-native. Strongest operator argument (§5).

**P1-7 · Scaling / reslot runbook (interim) + hard safety documentation**  -- effort **M**

The draft's instinct was right but understated how un-`kubectl scale`-safe this is. The runbook (`deploy/SCALING.md`) must state plainly:

- **Scale-in cannot use `kubectl scale` at all.** A StatefulSet scale-down deletes the **highest-ordinal pod first, immediately, with no slot-drain hook**  -- so "migrate slots off + FORGET *before* deleting" is impossible to sequence against `kubectl scale --replicas`. The only safe scale-in: (1) `CLUSTER SETSLOT`/`REBALANCE` to migrate all slots off the target ordinal, (2) `CLUSTER FORGET` it from every peer, (3) *then* scale the StatefulSet down, (4) the removed ordinal's PVC **persists by design** (StatefulSet never auto-deletes PVCs unless `persistentVolumeClaimRetentionPolicy: whenScaled: Delete`  -- which must be explicitly left `Retain` here; see S1/P0-checklist).
- **Scale-out:** new pod boots as non-voter → `CLUSTER MEET` on leader → learner auto-promotes → `CLUSTER REBALANCE APPLY` + per-slot `SETSLOT`, finalized by polling `COUNTKEYSINSLOT`.
- **The static ConfigMap diverges from the live map after any online reshard (S2).** Topology is rendered statically at install; runtime `REBALANCE`/`SETSLOT` is the *authoritative* owner map. After any online reshard the **ConfigMap is stale/informational only**  -- a subsequent `helm upgrade` or pod-roll that re-applies the ConfigMap slot map will *revert* live slot ownership and corrupt the cluster. Document that once online reshard has occurred, the ConfigMap slot ranges must be re-rendered to match (or the operator must own the map). **This sharp edge is the core reason an operator is genuinely needed (§5).**
- **Files:** new `deploy/SCALING.md`, DEPLOY.md link, `values.yaml` replica comment.
- **Why:** naive scaling orphans slots (data loss, `cluster_state:fail`)  -- the exact long-standing Bitnami redis-cluster pain.

**P1-8 · Backup / restore / DR**  -- effort **M/L** *(new; the draft omitted this entirely)*
- Provide, at minimum: (a) **VolumeSnapshot / CSI-snapshot** guidance where the StorageClass supports it (per-PVC point-in-time), with the caveat that a raft-consistent multi-pod snapshot needs coordination (snapshot the leader's PVC, or quiesce via the #392 fence); (b) an **app-level export CronJob** option (`DUMP`/snapshot-export to object storage) as the portable path; (c) a documented **restore-into-fresh-PVC** runbook (restore snapshot → let raft re-replicate to peers); (d) explicit **RPO/RTO framing** (save-on-exit + snapshot cadence define RPO; warm reload + startupProbe budget define RTO).
- **Files:** `templates/backup-cronjob.yaml` (gated `backup.enabled`, default false, restricted securityContext), `deploy/BACKUP-RESTORE.md`.
- **Why:** pairs badly with the k3s local-path caveat where PVC loss = data loss unless raft re-replicates; a production plan cannot ship without a DR posture.

**P1-9 · Forbid HPA; caution VPA**  -- effort **S** *(new)*
- Document loudly: **do NOT attach a HorizontalPodAutoscaler to the cache StatefulSet**  -- HPA replica churn on a sharded raft store triggers the orphaned-slot / re-election disaster of P1-7, *automatically and repeatedly*. **VPA is also unsafe** unless it uses in-place pod resize AND maxmemory is re-derived on resize (P0-1's binary cgroup reader makes this tractable, but VPA-driven eviction/recreate still disrupts quorum).
- **Files:** `deploy/SCALING.md`, `values.yaml` comment.

**P1-10 · PSS enforce-label guidance + `automountServiceAccountToken: false` + console parity**  -- effort **S**
- Recommend `pod-security.kubernetes.io/enforce=restricted` on the install namespace in README/NOTES; set `automountServiceAccountToken: false` on cache pods + init (no runtime API access needed); verify console pod-level context mirrors the full container-level set (drop-ALL/RO-root/seccomp). Optionally a rendered-manifest CI assertion so the context can't silently regress.

**P1-11 · Confirm/set `fsGroupChangePolicy: OnRootMismatch` + writable-mount audit**  -- effort **S** *(new)*
- Set `fsGroupChangePolicy: OnRootMismatch` so large PVCs don't get a full recursive chown on every mount (slow restarts, directly worsening the P0-2 startup budget). Confirm the *only* writable mounts are `data_dir` (PVC) and `/run/ironcache` (emptyDir); everything else RO under `readOnlyRootFilesystem: true`.

**P1-12 · Guaranteed-QoS + memory-request lower-bound documentation**  -- effort **S**
- Document the opt-in Guaranteed-QoS knob (matching CPU limit → strongest eviction protection, trading the CFS-throttle risk) and that `requests.memory` should be ≥256Mi and explicitly set.

**P1-13 · `persistentVolumeClaimRetentionPolicy: Retain`**  -- effort **S** *(new, called out separately given its data-loss weight)*
- Explicitly set `spec.persistentVolumeClaimRetentionPolicy: { whenDeleted: Retain, whenScaled: Retain }` on the StatefulSet. Never `Delete`.
- **Why:** without this made explicit, a future edit or a `whenScaled: Delete` misconfiguration turns a scale-in or StatefulSet delete into permanent data loss for the removed ordinals.

### P2  -- Polish

**P2-1 · Grafana dashboard auto-provision ConfigMap**  -- effort **S**  -- `templates/grafana-dashboard-configmap.yaml` gated `grafana.dashboard.enabled`, keyed `ironcache.json`, labeled for the Grafana provisioner sidecar.

**P2-2 · `values.schema.json` (draft-07)**  -- effort **S**  -- type the values surface and encode cross-field invariants Helm rejects early: replicas odd/positive; `resources.limits.memory` required and > derived maxmemory; `maxmemory_policy` enum **excluding `noeviction` when derivation is on** (or at least warned); `auth.enabled` ⇒ password-or-existingSecret; `cluster.enabled` ⇒ clusterSecret-or-existingSecret (P1-5); `clusterTls.enabled` ⇒ cert+key (+ the #660 `ca` rule); enums for `logLevel`/`maxmemory_policy`. Slots into `deploy-lint.yml` at ~zero cost.

**P2-3 · `helm test` + `chart-testing (ct)` in CI**  -- effort **M**  -- `templates/tests/` connection/PING Pod (restricted securityContext) + `ct lint/install` against kind/k3d, exercising install AND upgrade  -- catches charts that lint clean but fail to form/upgrade a cluster.

**P2-4 · OCI publish + chart signing**  -- effort **M**  -- push to `ghcr.io/elares/charts` from release workflow, cosign keyless-sign (extend the existing SBOM/provenance/Sigstore posture from image → chart), pin by digest. Enables Flux/Argo consumption.

**P2-5 · `appVersion` / default `image.tag` off `latest`**  -- effort **S**  -- stamp both to the concrete release (no leading `v`) from the release workflow.

**P2-6 · ServiceMonitor `relabelings`/`sampleLimit` passthrough**  -- effort **S**  -- soft knob to stagger/undersample scrapes on large clusters.

**P2-7 · Optional Ingress template (console UI)**  -- effort **S**  -- `ingress.enabled` with `ingressClassName` (default `traefik` on k3s), for the stateless console; document RESP is L4 (client Service / IngressRouteTCP, not HTTP Ingress).

**P2-8 · Doc-only clarifications**  -- effort **S**  -- VCT labels don't sync to PVCs; `readOnlyRootFilesystem` applies to `/` (writable `/run/ironcache` emptyDir is by-design); `runAsUser` must match image UID 65532; PVC `reclaimPolicy` (Retain for prod) guidance; pin busybox init image by digest for supply-chain hardening.

---

## 4. k3s-Specific Section

k3s is a *named* target (edge/ARM/single-node/air-gapped) but has **zero** current coverage. Add a `deploy/k3s/` section + `values-k3s.yaml` overlay. The chart is already largely k3s-friendly (RWO PVCs, soft anti-affinity default, CPU-quota-aware shards, multi-arch amd64+arm64 distroless images, CRD-gated ServiceMonitor); the deltas are mostly documentation + one overlay + the P0-1 memory fix (which matters *most* here).

- **Storage (local-path-provisioner):** k3s default StorageClass `rancher.io/local-path` is RWO + **node-local** + WaitForFirstConsumer. The chart's empty `storageClassName` picks it up automatically. **Critical caveats to document:** (1) local-path is hostPath-backed  -- a pod rescheduled to another node **cannot reattach** its PVC (raft log/snapshot stranded on the old node); with hard anti-affinity + node loss the pod is stuck Pending. On k3s, durable recovery relies on **Raft re-replication, not PVC reattach**  -- treat local-path data as best-effort or point `storageClassName` at Longhorn/external CSI for real durability. Keep **soft** anti-affinity (the default) so pods stay schedulable. (2) **local-path does not support volume expansion**  -- the P0-1 maxmemory/grow story and any resize assume a CSI that can expand; on local-path you cannot grow the PVC, only recreate. (K1)
- **Single-node k3s:** intended posture is `replicas: 1` + `cluster.enabled: false` (standalone, no quorum)  -- the chart correctly renders no PDB below 3 replicas and stamps no cluster identity. Do **not** run a 1-node "cluster" (triples memory, zero fault tolerance). Two-node (replicas: 2) is explicitly **not recommended**. Here save-on-exit + adequate grace is the *only* thing between a drain and data loss, so P0-4 grace sizing matters even more.
- **Ingress / LB:** RESP is L4  -- Traefik HTTP Ingress does **not** front the cache. Keep `service.type: ClusterIP` for in-cluster clients; for external RESP use Traefik `IngressRouteTCP`, NodePort, or a servicelb/klipper LoadBalancer  -- and warn that a single LB in front of a Raft cluster is **not** cluster-aware (nodes answer MOVED for un-owned slots); prefer cluster-aware clients on per-pod headless DNS. **`IngressRouteTCP` requires the Traefik CRD**, which is bundled in stock k3s but **disabled in hardened/air-gapped builds (`--disable traefik`)**  -- if Traefik is disabled there is no IngressRouteTCP path; fall back to NodePort/hostPort for external RESP. (K2) Optional Ingress template (P2-7) is for the **console UI** only.
- **Air-gapped:** three images must be pre-loaded  -- `ghcr.io/elares/ironcache`, `busybox:1.36` (**easy-to-miss** init dependency), and optionally `ghcr.io/elares/ironcache-console`. Recipe: `k3s ctr images import <tar>` or drop tarballs in `/var/lib/rancher/k3s/agent/images/`; keep `pullPolicy: IfNotPresent` (default). Gate ServiceMonitor off (default) since the Prometheus Operator CRD is usually absent. Chart-from-checkout is air-gap-friendly.
- **ARM / low footprint:** multi-arch static musl distroless images already build. Default `requests` (500m / 512Mi) is heavy for Pi-class nodes  -- the `values-k3s.yaml` overlay should lower them. `shards` auto-scales to the CFS quota, so a 4-core Pi auto-sizes to 4 shards.
- **CPU limits + throttling:** default (no CPU limit) is correct. If k3s admission *imposes* one, `available_parallelism()` tracks the quota → fewer shards (graceful) rather than throttling.
- **Memory (the critical k3s gap):** P0-1 is *most* important here  -- an unbounded cache on a shared edge node OOMKills itself and evicts co-tenant workloads. Note that on local-path the cgroup reader path (not the resize path) is what matters, since you can't grow the PVC anyway.
- **NetworkPolicy:** k3s default flannel does **not** enforce NetworkPolicy  -- the console policy (and any P1-3 cache policy) is inert / fails-open. Isolation requires Calico/Cilium (or k3s's kube-router policy controller).
- **PSS restricted:** works unchanged (PSA built into k3s); recommend the enforce label (P1-10). `SleepAction` preStop (P0-3) is compatible with `readOnlyRootFilesystem`/drop-ALL precisely because it needs no in-image binary; fall back to readiness-fail-on-drain (P0-5) if an old air-gapped k3s predates `SleepAction`.
- **`startupProbe` budget (P0-2)** should be sized *more generously* on edge/ARM  -- slower CPUs + SD-class local-path media make snapshot reload slower per byte. Use the concrete formula (`budget ≥ worst-case-snapshot-bytes / observed-reload-throughput + margin`) and keep it derived from the same dataset estimate as `terminationGracePeriodSeconds`. The tunable `failureThreshold` is the lever.
- **DR on k3s:** because local-path PVC loss = data loss, the P1-8 backup story (app-level export CronJob to off-node object storage) is effectively **mandatory** on single-node/edge k3s, not optional  -- raft re-replication does not help when there's only one replica.

---

## 5. The Operator Question

**Recommendation: Ship the Helm chart (with the P0/P1 fixes above) as the fully-supported baseline. Do NOT build an Operator now. Do scope and design one specifically for automated day-2 *scaling* and *leader-failover-on-restart*, treating it as an eventual add-on  -- not a prerequisite for production.**

**Why the chart + #392 suffices for most deployments:**
- The chart already does everything a *declarative* renderer *can* do correctly: discovery, turnkey fresh-cluster formation (fresh-only + idempotent, integration-tested), quorum PDB (for involuntary disruption), readiness-gated one-at-a-time rolling upgrades, TLS/secrets, hardened security context.
- For a **symmetric, fixed-size** cluster (the common case), a readiness-gated `RollingUpdate` + one-at-a-time ordinal serialization + `minReadySeconds` is a genuinely safe in-place upgrade. **(Note the corrected mechanism: the safety comes from the readiness gate + ordinal serialization + minReadySeconds, NOT from the PDB  -- the PDB does not limit rolling updates.)** The #392 `ironcache upgrade --cluster` driver adds the *RPO=0, election-free* path when operators want it  -- the piece a naive Helm rollout lacks, and it exists today.
- An operator is a **long-term maintenance commitment**. The coreos etcd-operator is the cautionary tale (effectively abandoned; the ecosystem moved to static-pod/learner-mode management). Scope creep here would be expensive.

**Where the chart genuinely can't reach  -- the operator's real charter (observe→decide→act loops Helm cannot express):**
1. **Safe scale-out / scale-in**  -- learner-join, online reshard (`REBALANCE APPLY` + per-slot `SETSLOT` finalized by polling `COUNTKEYSINSLOT`), and drain-slots-then-`FORGET`-then-scale-then-retain-PVC. This is the biggest chart shortfall (§P1-7) and the classic Bitnami redis-cluster failure mode. **The decisive sharp edge: after any online reshard the runtime slot map diverges from the static ConfigMap (S2), and a subsequent `helm upgrade`/pod-roll will silently revert ownership and corrupt the cluster.** A declarative chart *cannot* own a slot map that mutates at runtime; an operator reconciling desired-vs-actual slot ownership is the only correct home for it. This is the strongest single argument for the operator.
2. **Failover-before-drain on every restart**  -- reuse the #392 fence automatically on a StatefulSet pod deletion (not just the CLI path), turning a timeout-driven election into an instant handoff.
3. **Rebalance orchestration, health-regression-paused rolling upgrades, and snapshot+raft-log backup/restore (P1-8) driven from cluster state.**

**The decisive fact:** IronCache is *unusually well-positioned* for an operator because **the hard runtime mechanisms already exist and are tested**  -- RPO=0 failover-freeze fence, learner-join (MEET-stages-a-learner-auto-promotes-then-FORGET), online `REBALANCE`/`SETSLOT`. An operator would be a **thin control loop** invoking existing primitives from a desired-state `IronCacheCluster` CRD  -- *not* new data-plane work. That lowers the risk/cost and changes the recommendation from "maybe someday" to "a well-bounded, high-value follow-on once P0/P1 land."

**Sequencing:**
- **Now:** close P0 (OOM safety incl. eviction policy, startupProbe with early listener bind, preStop, grace, drain-readiness) and P1 (topologySpread, cache NetworkPolicy, updateStrategy, minReadySeconds, secret-rotation guard, backup/restore, no-HPA, PVC-retention, fsGroup, scaling *runbook*). These make the chart production-safe for fixed-size clusters and document the manual scale/upgrade paths and their sharp edges.
- **Next (scoped operator):** an `IronCacheCluster` CRD + a **single lightweight controller Deployment with minimal RBAC** (must stay k3s-footprint-friendly, no hard dependency on cluster-scoped webhooks/cert-manager) that automates (1) scale reshard **and ownership of the authoritative slot map** (resolving the S2 ConfigMap-divergence hazard), and (2) failover-before-drain by driving the existing #392 fence and MEET/REBALANCE/SETSLOT/FORGET primitives. Keep the Helm chart the answer for single-node/edge k3s; the operator's loops matter chiefly for multi-node clusters that actually scale.

---

### Change-summary of corrections folded in from review
- **C1 (factual):** PDB does **not** gate/pace StatefulSet rolling updates (only voluntary evictions); corrected everywhere. Rollout safety = readiness gate + one-at-a-time ordinal + `minReadySeconds`. `podManagementPolicy: Parallel` affects bootstrap only, not updates. PDB's real job = involuntary-disruption quorum protection.
- **C2:** P0-1 reordered to prefer Downward-API + binary fraction over fragile template byte-math; added the node-allocatable fallback guard and the mandatory non-`noeviction` `maxmemory-policy`.
- **C3:** startupProbe requires the metrics listener to bind *before* reload; startup budget + grace period derive from one dataset-size estimate.
- **M1→P1-8:** backup/restore/DR added.
- **M2→P1-9:** no-HPA / VPA-caution added.
- **M3→P1-11:** `fsGroupChangePolicy: OnRootMismatch` + writable-mount audit.
- **M4:** headless-DNS-propagation race → raft peer-join must retry, not fail-fast.
- **M5→P1-10:** `automountServiceAccountToken: false`.
- **S1→P1-7/P1-13:** scale-in cannot use `kubectl scale`; `persistentVolumeClaimRetentionPolicy: Retain` explicit.
- **S2→P1-7/§5:** static ConfigMap diverges from runtime reshard map  -- the core operator justification.
- **K1/K2:** local-path no volume-expansion; Traefik-CRD-disabled air-gap fallback to NodePort.
