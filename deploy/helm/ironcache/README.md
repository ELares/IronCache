<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# IronCache Helm chart

Deploys IronCache as a **StatefulSet Raft cluster** (or, with `cluster.enabled=false`,
independent standalone nodes). The full walkthrough with production posture lives
in [`DEPLOY.md` section 5 (Kubernetes)](../../../DEPLOY.md); this file is the
chart-local summary.

## What it deploys

- A StatefulSet of `replicas` nodes with stable per-pod identity. The chart
  renders the full `[[cluster_topology.nodes]]` TOML into a ConfigMap
  (`templates/configmap.yaml`), splitting the 16384 hash slots EVENLY across the
  replicas (node `i` owns `[i*16384/N, (i+1)*16384/N)`); no manual slot math.
- Each node id is a deterministic `sha256("<fullname>-<ordinal>")` truncated to
  40 hex chars (`ironcache.nodeId` in `templates/_helpers.tpl`). A BusyBox init
  container (`stamp-identity.sh`) recomputes THIS pod's id from its StatefulSet
  ordinal and PREPENDS `cluster_announce_id` to the config the main container
  reads via `--config`, so the announce id always matches the topology entry.
- A headless Service with `publishNotReadyAddresses` (peers must resolve by DNS
  before Raft reaches quorum), a client Service on `service.clientPort`, a
  quorum-preserving PodDisruptionBudget (`pdb.maxUnavailable`), and liveness /
  readiness probes on `/livez` + `/readyz` at the metrics port.
- Optionally (`console.enabled=true`) a separate STATELESS console Deployment
  with its own Service, PDB, and an egress-allowlisting NetworkPolicy.

## Install

### From the GHCR OCI registry (published + cosign-signed)

Each release is packaged and pushed to GHCR as an OCI artifact and keyless-signed
with cosign (Sigstore/Fulcio via GitHub OIDC):

```sh
helm install ironcache oci://ghcr.io/elares/charts/ironcache --version 0.3.0

# (optional) verify the signature before installing:
cosign verify ghcr.io/elares/charts/ironcache:0.3.0 \
  --certificate-identity-regexp 'https://github.com/ELares/IronCache/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

### From a git checkout

```sh
git clone https://github.com/ELares/IronCache
cd IronCache
helm install ironcache deploy/helm/ironcache
```

Upgrades are `helm upgrade ironcache <chart-ref>` with a newer `--version` (OCI) or
checkout. `NOTES.txt` prints the endpoints and loud warnings (auto-generated cluster
secret, auth off, cleartext bus without `clusterTls`) after install.

## Key values

See `values.yaml` for the full commented surface. The ones that matter first:

| Value | Default | Notes |
| --- | --- | --- |
| `replicas` | `3` | Keep ODD (3, 5, 7) for an unambiguous Raft majority. `1` = standalone. |
| `image.tag` | `""` | Empty = the chart `appVersion` (an immutable pinned release) -- the recommended default. Set only to override. The registry tag has NO leading `v` (the image CI strips it); `"v0.1.0"` does not exist and fails the pull. |
| `cluster.enabled` | `true` | `false` deploys `replicas` INDEPENDENT standalone nodes (only sensible with `replicas=1`). |
| `persistence.enabled` | `true` | `data_dir` on a PVC (`persistence.size`, `persistence.storageClassName`); the Raft log must survive a pod restart. `false` = emptyDir, data lost on reschedule. |
| `persistence.saveIntervalSecs` / `saveMinChanges` | `900` / `1` | The background-save cadence that bounds the RPO. |
| `auth.enabled` | `false` | Client AUTH (`requirepass`) via a chart Secret (`auth.password`) or `auth.existingSecret` (key `requirepass`). STRONGLY recommended. |
| `clusterSecret.value` / `existingSecret` | `""` | The shared bus/replication handshake secret. Auto-generated on first install and PRESERVED across `helm upgrade` (#747). Under GitOps / `helm template` / `--dry-run` the lookup is blind and it WOULD regenerate, so set an explicit value there. |
| `clusterTls.enabled` | `false` | Encrypts the node-to-node bus + replication links. Without it the cluster secret travels in cleartext on the pod network. |
| `clusterTls.ca` | `""` | Optional since #660: when empty the chart writes `cluster.ca` = the cert, so a self-signed cluster cert acts as its own CA. Set it only for a distinct external-PKI trust chain. |
| `tls.enabled` | `false` | TLS on the PUBLIC client RESP listener (separate from `clusterTls`). |
| `metrics.port` | `9121` | The chart passes `--metrics-addr 0.0.0.0:<port>`; probes and Prometheus scrape here. `metrics.serviceMonitor.enabled` renders a ServiceMonitor (needs the Prometheus Operator CRD). |
| `console.enabled` | `false` | The stateless monitoring/management console; see `deploy/CONSOLE_DEPLOY.md` for the HA + security walkthrough. |

## Observability

Every node serves `/metrics` + `/livez` + `/readyz` on `metrics.port`; the
starter Grafana dashboard ships in the chart at
`dashboards/ironcache-dashboard.json` (set `metrics.grafanaDashboard.enabled=true`
to auto-provision it as a sidecar-discoverable ConfigMap, or import it manually),
and the Prometheus alert rules in `alerts/ironcache-alerts.yml` (set
`metrics.prometheusRule.enabled=true` to ship them as a PrometheusRule; see
`docs/METRICS.md`).

The chart is CI-gated by `helm lint` + `helm template | kubeconform` across a matrix of
value sets plus `values.schema.json` negative tests (`.github/workflows/deploy-lint.yml`),
and by a live `helm install` + `helm upgrade` + `helm test` on a kind cluster
(`.github/workflows/chart-e2e.yml`).
