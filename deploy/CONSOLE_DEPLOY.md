<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# Deploying the console (HA, stateless, off the data path)

The IronCache console is a SEPARATE monitoring + management server, not part of the
cache data plane. This is the deployment runbook for running it in HA: **N
identical stateless replicas behind a load balancer**, so a replica loss is
transparent and the cache data path is never touched. It complements the full
config reference in [`../DEPLOY.md`](../DEPLOY.md); read this for the console
topology, the security posture, and the two-replicas-behind-an-LB walkthroughs.

Artifacts referenced here:

| Path | What it is |
| --- | --- |
| `../Dockerfile.console` | The stateless, nonroot, distroless console image. |
| `compose/docker-compose.console.yml` + `compose/config/console-nginx.conf` | Two replicas behind an nginx LB (an overlay on a cache compose file). |
| `k8s/ironcache-console.yaml` | Raw manifests: Secret + Deployment + Service + PDB + NetworkPolicy. |
| `helm/ironcache` (`console.*` values) | The same set, templated, off by default. |
| `aclfile.console.example` | The least-privilege node ACL users the console dials as. |

## 1. Data-path isolation (the load-bearing guarantee)

The console NEVER sits between a client and a shard. Clients talk RESP straight to
the cache nodes; the console is a side-car observer that POLLS the nodes (as a
scoped ACL user) and serves a dashboard. Removing, restarting, or overloading the
console cannot slow, drop, or corrupt a single cache request, because no cache
traffic flows through it. Deploy it as its own workload (its own Deployment /
compose services), never in the request path, and this holds by construction.

## 2. The HA model

Run two or more replicas behind one load balancer:

- **Stateless replicas.** A replica keeps no durable state: no volume, no per-pod
  identity, no sticky session. Auth is an `Authorization: Bearer` header (never a
  cookie), resolved from config identical on every replica, so ANY replica serves
  ANY request. The topology view is re-derived per replica by polling the nodes.
- **Load balancer + per-replica readiness.** The console's own `GET /livez` and
  `GET /readyz` are the LB health checks. `/readyz` returns `503` until that
  replica's first successful poll, so a cold replica is held out of rotation; a
  replica whose process is down fails the probe and is routed around. A single
  replica loss is therefore transparent to the operator.
- **A PodDisruptionBudget.** With 2+ replicas a PDB (`maxUnavailable: 1`) keeps at
  least one console serving through a voluntary disruption (a node drain, a rolling
  node upgrade), so operator visibility is never fully interrupted. The console has
  NO quorum (each replica is independent), so unlike the cache it needs no odd
  count -- two is enough for HA.
- **Shared history for consistency.** Point every replica at ONE shared metrics
  backend with `IRONCACHE_CONSOLE_PROMETHEUS_URL`. The alternative (the per-replica
  embedded trend buffer) makes each replica show a different history window behind
  the LB and drops that window on a replica loss; the console logs a boot WARNING if
  the embedded buffer is used on a non-loopback bind for exactly this reason.

## 3. Security posture

The console is a web UI that proxies a metrics backend and reaches internal nodes,
so it is hardened against the classic monitoring-console exposure and SSRF risks:

- **Not internet-facing.** Keep the console on a private / VPN-restricted network,
  reachable by operators but not from the data-client network or the public
  internet. In k8s the Service is `ClusterIP` (never a public `LoadBalancer` by
  default); front it with an Ingress that YOU lock down (source allowlist / VPN /
  mTLS). Terminate TLS at that edge.
- **Egress allowlist.** The console may reach ONLY DNS, the cache node ports (RESP +
  the node metrics/admin port), and the shared metrics backend -- nothing else. The
  shipped `NetworkPolicy` (both raw k8s and Helm) pins that reachable set so a
  tricked or compromised replica cannot pivot. See section 6.
- **Non-root, read-only container.** The image runs as an unprivileged user (uid
  65532), and the manifests set `runAsNonRoot`, `readOnlyRootFilesystem: true`,
  `allowPrivilegeEscalation: false`, `capabilities: drop: [ALL]`, and the
  `RuntimeDefault` seccomp profile. The console needs no writable filesystem.
- **Least-privilege node credential.** Each replica dials the nodes as the scoped
  read-only `console_monitor` ACL user from `aclfile.console.example` (no key
  access, no mutation: it can PING / INFO / read the slowlog + client list and
  nothing else). A leaked poll credential cannot change or destroy anything.
  Management actions belong on the separate scoped `console_admin` user, supplied
  only when an operator performs an action -- never held by the poll loop.
- **Secrets, never literals.** The node password and the console API tokens are
  mounted from a Secret (the password as a file via
  `IRONCACHE_CONSOLE_NODE_PASSWORD_FILE`, the tokens as env from `secretKeyRef`),
  never written as plaintext into a manifest. Setting a read token flips the
  console's `/api/*` into ENFORCE mode, so ALWAYS set one for a behind-LB deploy or
  the privileged API is open.

## 4. Two replicas behind an LB: docker-compose

The console overlay ships TWO named, identical replicas (`console-1`, `console-2`)
plus an nginx LB (`console-lb`) that proxies `/` to either and marks a dead replica
down + retries the other, so a replica loss is invisible. Overlay it onto a cache
compose file:

```sh
cd deploy/compose
# Optional: point at your cache node(s) and a shared metrics backend, and gate the
# privileged API with a read token, via a .env file beside the compose files:
#   IRONCACHE_CONSOLE_SEEDS=ironcache-1:6379,ironcache-2:6379,ironcache-3:6379
#   IRONCACHE_CONSOLE_PROMETHEUS_URL=http://prometheus:9090
#   IRONCACHE_CONSOLE_READ_TOKEN=<generate-a-token>

docker compose -f docker-compose.cluster.yml -f docker-compose.console.yml up -d
curl localhost:9180/readyz     # served by whichever replica the LB picks
docker compose -f docker-compose.cluster.yml -f docker-compose.console.yml down
```

Kill one replica (`docker compose ... stop console-1`) and the LB keeps serving from
the other -- the transparent-replica-loss acceptance, demonstrated locally.

Scaling note: the LB upstream lists the replicas by name (`console-1`,
`console-2`), which is deterministic and needs no service-discovery. To go beyond
two, add a `console-3` service and a matching `server console-3:9180;` line in
`config/console-nginx.conf`. (Plain `docker compose up --scale` is not used here:
open-source nginx resolves a named upstream server once at load, so a single scaled
service would not fan out across replicas without a `resolver` directive; the named
services avoid that footgun.)

## 5. Two replicas behind an LB: Kubernetes / Helm

**Raw manifests.** Apply AFTER the cache (`k8s/ironcache.yaml`) is up:

```sh
kubectl create namespace ironcache      # if not already created for the cache
# Edit the Secret placeholders (node password + console tokens), set the seeds +
# the shared metrics backend URL, and pin the image to an immutable tag first.
kubectl -n ironcache apply -f k8s/ironcache-console.yaml
kubectl -n ironcache rollout status deploy/ironcache-console
kubectl -n ironcache get pods -l app.kubernetes.io/component=console   # 2 replicas
```

This renders a Secret + a 2-replica Deployment + a `ClusterIP` Service + a PDB + a
NetworkPolicy. Front the Service with a VPN-locked Ingress for operator access;
do not expose it publicly.

**Helm.** Enable the console in the same release (it is off by default):

```sh
helm upgrade --install ironcache deploy/helm/ironcache \
  --namespace ironcache --create-namespace \
  --set console.enabled=true \
  --set console.replicas=2 \
  --set 'console.seeds={ironcache.ironcache.svc.cluster.local:6379}' \
  --set console.prometheusUrl=http://prometheus.monitoring.svc.cluster.local:9090 \
  --set console.nodePasswordSecret.existingSecret=ironcache-console-secret \
  --set console.tokensSecret.existingSecret=ironcache-console-secret
```

Create the referenced Secret out-of-band (keys `console_node_password`,
`console_read_token`, `console_admin_token`) so the credentials never land in
values or the Helm release history. `console.replicas=2` renders the Deployment,
the Service, a PDB, and the egress NetworkPolicy. Delete one console pod
(`kubectl -n ironcache delete pod <console-pod>`) and the Service keeps serving from
the survivor while the Deployment reschedules the replacement -- transparent replica
loss.

## 6. The egress allowlist (NetworkPolicy)

The `NetworkPolicy` is the SSRF-containment control. It selects only console pods
and restricts them to:

- **Ingress:** the single HTTP port (`9180`) -- every other port is closed. Who may
  reach it is enforced at the VPN-locked Ingress / LB in front (tighten further with
  `console.networkPolicy.ingressFrom` for your ingress controller).
- **Egress:** DNS, the cache pods on the RESP + metrics ports, and the shared
  metrics backend. The backend usually lives in another namespace, so its egress
  peer is operator-supplied: set `console.networkPolicy.prometheusEgress` (Helm) or
  uncomment the backend block in `k8s/ironcache-console.yaml` to match your
  `IRONCACHE_CONSOLE_PROMETHEUS_URL`. Left unset, history queries fail CLOSED.

Enforcement needs a NetworkPolicy-aware CNI; on a CNI that ignores it the policy is
inert (fails open), so treat it as defense-in-depth, alongside the private-network
posture, not the only egress control.

## 7. Pre-production checklist

- [ ] `console.replicas` >= 2 and the PDB is present.
- [ ] Image pinned to an immutable tag (not `latest`).
- [ ] `IRONCACHE_CONSOLE_SEEDS` points at the cache node(s) / client Service.
- [ ] A SHARED `IRONCACHE_CONSOLE_PROMETHEUS_URL` (consistent history across replicas).
- [ ] `aclfile.console.example` loaded on the nodes; the console dials as
      `console_monitor` (read-only).
- [ ] Node password + console tokens come from a Secret; a read token is set
      (ENFORCE mode).
- [ ] Service is `ClusterIP`; a VPN-locked Ingress with TLS at the edge fronts it
      (not a public LoadBalancer).
- [ ] The NetworkPolicy is applied and its backend egress peer matches the metrics
      backend.
