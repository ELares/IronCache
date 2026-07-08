# TLS operator guide

How to run IronCache with TLS: certificate generation, enabling TLS on the public
client port, the intra-cluster (bus + replication) transport, what the TLS layer
does and does NOT cover, and the certificate-rotation story. This is the OPERATOR
guide; the design rationale lives in `docs/design/TLS.md`, and the trust boundaries
in `docs/THREAT_MODEL.md` + `SECURITY.md`.

Every claim below is verified against the code (file citations inline).

## What ships (the honest surface)

IronCache terminates TLS IN-PROCESS with rustls (pure Rust) and the `ring` crypto
provider. There is no OpenSSL, no C TLS library, no `cmake`, and no sidecar: the
single static binary is the whole deployment (`crates/ironcache-runtime/src/tls.rs`).
The version floor is TLS 1.2 with TLS 1.3 preferred; SSLv3 / TLS 1.0 / TLS 1.1 are
refused by rustls construction.

TLS is compiled into the shipped binary BY DEFAULT (the `tls` feature is default-on
in `ironcache-runtime`, `ironcache-clusterbus`, and `ironcache-raft-net`; the
`ironcache` binary takes `tokio-rustls` as a normal dependency). You do NOT pass a
`--features tls` flag for a normal build. TLS is OFF at runtime until you configure
it, and the plaintext hot path is byte-unchanged when it is off.

| Plane | Covered? | How | Peer authentication |
| --- | --- | --- | --- |
| Public client port | Yes (opt-in, `tls = on`) | rustls server-auth, TLS-only listener | server cert only; client identity via AUTH/ACL inside the session |
| Cluster bus (RAFTMSG consensus) | Yes (opt-in, `cluster_tls = on`) | rustls, both listener + dial | shared `cluster_secret` (constant-time) + optional CA-verified peer cert |
| Replication stream | Yes (opt-in, same `cluster_tls`) | rustls, both listener + dial | same as the bus (shared handle) |

### What is NOT covered (read this before you deploy)

- **No client-certificate mTLS on the public client port.** The client listener is
  built with `with_no_client_auth()` (`crates/ironcache-runtime/src/tls.rs`,
  `build_acceptor`): it authenticates the SERVER to the client, not the client to the
  server. Client identity is established INSIDE the TLS session by AUTH / ACL
  (`requirepass`, `aclfile`), not by a client certificate. Full per-node client-cert
  mTLS is a documented follow-up (`docs/THREAT_MODEL.md`, "Explicitly out of scope").
- **The cluster bus + replication links are PLAINTEXT and UNAUTHENTICATED by
  default.** In a clustered mode, if you set neither `cluster_secret` nor
  `cluster_tls`, any party that can reach the bus port (`port + 10000`) or the
  replication stream can join consensus, forge RAFTMSG, or siphon the entire keyspace.
  The node emits a LOUD boot warning in that state
  (`crates/ironcache/src/cluster_bus.rs`, `warn_if_unauthenticated`). Either run the
  cluster on a trusted / isolated network, or set `cluster_secret` (and ideally
  `cluster_tls = on`) as below.
- **The built-in `ironcache cli` sub-command is a plaintext WIP smoke client.** It
  dials a bare TCP socket (`crates/ironcache/src/main.rs`, `cmd_cli`) and CANNOT talk
  to a TLS-only listener. Use a Redis-compatible client with TLS (for example
  `redis-cli --tls`) against a `tls = on` server.
- **The CLIENT listener hot-reloads its cert on `SIGHUP` (#563); the cluster bus does
  not yet.** Replace the configured `tls_cert_path` / `tls_key_path` files and send the
  node `SIGHUP` to rotate the client-listener cert with no restart and no dropped
  connections. The intra-cluster (`cluster_tls`) cert still needs a restart to rotate.
  See "Certificate rotation" below.

## 1. Generate certificates

These are quickstart commands for a self-signed setup suitable for a private
deployment. For a public-facing client port, obtain a cert from your CA / ACME
provider instead; the file formats are the same (a PEM cert chain + a PEM private
key). IronCache accepts PKCS#8, RSA (PKCS#1), and SEC1 (EC) private keys.

### A self-signed cert for the client port (dev / internal)

```sh
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \
  -keyout server.key.pem -out server.cert.pem \
  -subj "/CN=my-ironcache" \
  -addext "subjectAltName=DNS:my-ironcache,IP:127.0.0.1"
```

`server.cert.pem` is the cert chain (leaf first, then any intermediates);
`server.key.pem` is the matching private key. Clients must trust `server.cert.pem`
(or its issuing CA).

### A cluster CA + a shared cluster cert (for `cluster_tls`)

The cluster TLS peer name is a fixed logical name (`ironcache-cluster`,
`crates/ironcache-runtime/src/tls.rs`, `CLUSTER_TLS_SERVER_NAME`); peer IDENTITY is
carried by the shared secret and (optionally) CA verification, not by hostname
matching against a public PKI. The simplest secure setup is ONE self-signed cert used
as both the presented cert AND the CA that verifies it (it verifies against itself):

```sh
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \
  -keyout cluster.key.pem -out cluster.cert.pem \
  -subj "/CN=ironcache-cluster" \
  -addext "subjectAltName=DNS:ironcache-cluster"
```

Distribute `cluster.cert.pem` + `cluster.key.pem` to every node and point BOTH
`cluster_tls_cert_path` and `cluster_ca_path` at `cluster.cert.pem`. (For a stricter
posture, run a real CA and issue a distinct cert per node, all signed by the CA
`cluster.cert.pem`.)

## 2. Enable TLS on the public client port

Set the posture plus the cert/key. TOML keys and the equivalent env vars:

| TOML | Env var | Meaning |
| --- | --- | --- |
| `tls = "on"` | `IRONCACHE_TLS=on` | make the client port TLS-only (default `off`) |
| `tls_cert_path = "..."` | `IRONCACHE_TLS_CERT_PATH` | PEM cert chain the listener presents |
| `tls_key_path = "..."` | `IRONCACHE_TLS_KEY_PATH` | PEM private key matching the cert |

When `tls = on` the client port is TLS-ONLY: a plaintext client fails the handshake
and is dropped (`crates/ironcache-config/src/lib.rs`, `TlsMode`;
`crates/ironcache/src/serve.rs`, `accept_tls`). The cert + key are validated readable
at boot; a missing or malformed PEM fails boot loudly rather than starting a listener
that rejects every handshake. The handshake is bounded (10s) as a slow-loris guard
(`HANDSHAKE_TIMEOUT`).

```sh
IRONCACHE_TLS=on \
IRONCACHE_TLS_CERT_PATH=/etc/ironcache/server.cert.pem \
IRONCACHE_TLS_KEY_PATH=/etc/ironcache/server.key.pem \
  ironcache server
```

### Connect a client

```sh
# redis-cli, trusting the server cert (or its CA):
redis-cli --tls --cacert server.cert.pem -h my-ironcache -p 6379 PING
```

AUTH still applies inside the TLS session:

```sh
redis-cli --tls --cacert server.cert.pem -h my-ironcache -p 6379 -a "$PASSWORD" PING
```

The built-in `ironcache cli` sub-command is plaintext-only (see the note above); use a
TLS-capable Redis client for a `tls = on` server.

## 3. Enable TLS on the cluster bus + replication

The intra-cluster links (the Raft RAFTMSG consensus bus and the replication stream)
share ONE security handle (`crates/ironcache/src/raft_boot.rs`,
`build_cluster_security`; `crates/ironcache-clusterbus/src/security.rs`). It is
SEPARATE from the public client-port TLS: a deployment can run a TLS client port and a
TLS cluster bus independently.

| TOML | Env var | Meaning |
| --- | --- | --- |
| `cluster_secret = "..."` | `IRONCACHE_CLUSTER_SECRET` | shared peer-auth token (constant-time compare) |
| `cluster_tls = "on"` | `IRONCACHE_CLUSTER_TLS` | encrypt bus + repl (default `off`) |
| `cluster_tls_cert_path = "..."` | `IRONCACHE_CLUSTER_TLS_CERT_PATH` | PEM cert the cluster listener presents |
| `cluster_tls_key_path = "..."` | `IRONCACHE_CLUSTER_TLS_KEY_PATH` | matching PEM private key |
| `cluster_ca_path = "..."` | `IRONCACHE_CLUSTER_CA_PATH` | CA the dial verifies the peer cert against |
| `cluster_tls_insecure_skip_verify = true` | `IRONCACHE_CLUSTER_TLS_INSECURE_SKIP_VERIFY` | NOT recommended: encrypt but skip peer-cert verification |

Two things authenticate a peer, and they are complementary:

1. **The shared `cluster_secret`** is sent + verified in a constant-time handshake
   right after the TLS handshake, on BOTH the dial and the accept side
   (`crates/ironcache-runtime/src/tls.rs`, `authenticate_peer_bounded`). A peer that
   does not present the exact secret is dropped, even if it completed a TLS handshake.
   It is held in memory in a `Zeroizing` buffer and scrubbed on drop (#145).
2. **`cluster_ca_path`** makes the dialer verify the peer's SERVER cert against the
   cluster CA (standard webpki verification). This defeats an active man-in-the-middle
   BEFORE the secret is ever sent: an attacker's cert is not CA-signed, so the
   handshake fails first. `cluster_ca_path` is REQUIRED when `cluster_tls = on`, unless
   you set the explicit `cluster_tls_insecure_skip_verify = true` opt-out (which
   encrypts but does NOT verify the peer, exposing the secret to a MITM, and logs a
   loud boot warning).

Recommended cluster posture (per node, same cert/CA/secret on all nodes):

```sh
IRONCACHE_CLUSTER_SECRET=$(cat /etc/ironcache/cluster.secret) \
IRONCACHE_CLUSTER_TLS=on \
IRONCACHE_CLUSTER_TLS_CERT_PATH=/etc/ironcache/cluster.cert.pem \
IRONCACHE_CLUSTER_TLS_KEY_PATH=/etc/ironcache/cluster.key.pem \
IRONCACHE_CLUSTER_CA_PATH=/etc/ironcache/cluster.cert.pem \
  ironcache server
```

You MAY set only `cluster_secret` without `cluster_tls` to authenticate a plaintext
bus, but then the secret travels in cleartext, so TLS + secret is the recommended
pairing. Setting neither in a clustered mode triggers the unauthenticated-bus warning.

## 4. Certificate rotation

### 4.1 Client listener: hot reload on `SIGHUP` (#563)

**The client-listener cert rotates with NO restart.** The rustls acceptor is held behind
an atomic `ArcSwap` (`crates/ironcache-runtime/src/tls.rs`, `ReloadableAcceptor`), and the
binary installs a `SIGHUP` handler at boot when `tls = on`
(`crates/ironcache/src/serve.rs`, `spawn_tls_reload_on_sighup`). To rotate:

1. Write the new cert + key to the SAME configured `tls_cert_path` / `tls_key_path`.
2. Send the node `SIGHUP` (for example `kill -HUP <pid>`, or `nginx`/`haproxy`-style from
   your process manager).

On `SIGHUP` the node re-reads those paths, rebuilds and validates the `ServerConfig`, and
atomically publishes it. Every handshake started AFTER the reload presents the new cert;
every in-flight connection keeps the cert it handshook with (rustls config is
per-handshake), so **no existing connection is dropped**. The outcome is logged (a success
line, or the failure reason).

**Fail-safe:** a bad, missing, or mismatched replacement is REJECTED. The reload logs the
error and KEEPS the previous good cert live, so a fat-fingered rotation never tears down
the listener or breaks existing TLS. Re-issue a valid pair and `SIGHUP` again.

`SIGHUP` can be sent repeatedly, so successive rotations each take effect. The reload reads
the SAME configured paths (it does not accept new paths at runtime); to change the paths
themselves, restart with the new config.

### 4.2 Cluster bus / replication: still restart-only

The intra-cluster (`cluster_tls`) acceptor + connector are still built ONCE at boot and
cloned onto every dial / accept (`crates/ironcache/src/raft_boot.rs`,
`build_cluster_security`); `SIGHUP` does NOT reload them (extending the same `ArcSwap`
mechanism to the bus + repl dials is a documented follow-up). To rotate a cluster cert:

1. Write the new cert + key to the configured cluster paths.
2. Restart the node. In a cluster, do a ROLLING restart (one node at a time), waiting for
   each node to rejoin and report healthy before moving on, so the cluster stays available.
   Because the cluster peer identity rests on the `cluster_secret` (and the CA), a node
   presenting a NEW cert signed by the SAME cluster CA is accepted by peers that have not
   yet rotated, which lets a rolling cert rotation proceed without a flag-day.

Plan rotations before expiry either way (a cert past its validity fails new handshakes).

## 5. Version and cipher floor

- TLS 1.3 preferred, TLS 1.2 accepted; SSLv3 / TLS 1.0 / TLS 1.1 refused (rustls
  construction, `tls12` feature on).
- Cipher suites are rustls' vetted defaults (not operator-tuned).
- Crypto provider: `ring` (pinned at the workspace level for a reproducible
  musl / aarch64 static cross-build). rustls' handshake RNG is transport entropy and
  does not cross the ADR-0003 determinism boundary.

## References

- Design rationale: `docs/design/TLS.md` (issue #105).
- Trust boundaries + accepted risks: `docs/THREAT_MODEL.md`, `SECURITY.md`.
- Deployment: `DEPLOY.md` section 6 (auth and TLS).
- Code: `crates/ironcache-runtime/src/tls.rs` (the TLS layer),
  `crates/ironcache/src/serve.rs` (client-listener wiring),
  `crates/ironcache/src/raft_boot.rs` + `crates/ironcache-clusterbus/src/security.rs`
  (cluster bus + repl), `crates/ironcache/src/cluster_bus.rs` (the unauthenticated-bus
  warning), `crates/ironcache-config/src/lib.rs` (the config fields).
- Hot client-listener cert reload: #563 (`ReloadableAcceptor` + `SIGHUP`); extending the
  same swap to the cluster bus / repl dials is the remaining follow-up.
