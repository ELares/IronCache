# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Production container image for IronCache (PROD-10).
#
# This image reuses the SAME fully-static musl binary the release pipeline
# already produces (.github/workflows/release.yml / rolling-release.yml build the
# `*-unknown-linux-musl` targets with `crt-static`, so libc is linked in and the
# binary has zero shared-library runtime dependency). The image-publish CI job
# (.github/workflows/image.yml) downloads the per-arch release tarball, unpacks it
# into `dist/<arch>/ironcache` (dist/amd64/, dist/arm64/), then invokes buildx; so
# this Dockerfile only STAGES an already-built static binary onto a minimal,
# nonroot, distroless runtime. There is no Rust toolchain in either stage, so the
# final image is tiny (the distroless/static base is ~2 MiB + the binary) and the
# build cannot drift from the released, attested artifact.
#
# Local / manual multi-arch build (after the release tarballs are unpacked under
# dist/, see the image CI for the exact layout):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     -t ghcr.io/elares/ironcache:dev --load .
#
# Ports (all derived from the client port; the defaults below assume 6379):
#   6379  RESP client listener            (Config.port / IRONCACHE_PORT)
#   16379 Raft cluster-bus / RAFTMSG      (port + 10000, BUS_PORT_OFFSET; raft mode)
#   26379 replication data plane          (port + 20000, REPL_PORT_OFFSET; raft mode)
#   9121  HTTP /metrics + /livez + /readyz (only when --metrics-addr is set)
#
# The data_dir (Config.data_dir / IRONCACHE_DATA_DIR) is the single enable switch
# for durable persistence (the on-disk snapshot dump-shard-<n>.icss + dump.manifest)
# AND the durable Raft log; it is exposed as a VOLUME so an orchestrator can mount a
# PersistentVolume there.

# --- stage: take the prebuilt static musl binary for the target arch ----------
# `alpine` is used only as a throwaway staging filesystem to chmod the binary; it
# never reaches the final image. TARGETARCH is set by buildx (amd64 | arm64) and
# selects which prebuilt binary to copy, so one Dockerfile serves both arches.
FROM alpine:3 AS stage
ARG TARGETARCH
WORKDIR /stage
# dist/amd64/ironcache and dist/arm64/ironcache are the fully-static musl binaries
# unpacked from the release tarballs by the image-publish CI. Static = no libc to copy.
COPY dist/${TARGETARCH}/ironcache /stage/ironcache
RUN chmod 0755 /stage/ironcache

# --- final: distroless static, nonroot, least-privilege -----------------------
# gcr.io/distroless/static:nonroot ships CA certs + a nonroot user (uid/gid 65532)
# and NOTHING else (no shell, no package manager, no libc) -- the minimal, secure
# runtime for a static binary. Use `scratch` for the absolute minimum if CA certs
# are not needed.
FROM gcr.io/distroless/static:nonroot

# OCI image metadata (source, license, version). The version label is overwritten
# at build time by the CI `--label org.opencontainers.image.version=<tag>`.
LABEL org.opencontainers.image.title="ironcache"
LABEL org.opencontainers.image.description="The most efficient Redis-compatible cache, in one static Rust binary."
LABEL org.opencontainers.image.source="https://github.com/ELares/IronCache"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"

COPY --from=stage /stage/ironcache /usr/local/bin/ironcache

# Run as the distroless nonroot user (uid 65532), never root. The data VOLUME is
# owned/writable by this user when the orchestrator sets fsGroup=65532 (see the
# Helm chart / k8s manifests securityContext).
USER 65532:65532

# Durable state: the snapshot dump + the Raft log live under data_dir. Declared a
# VOLUME so a `docker run -v` / a k8s PVC mounts persistent storage here. The
# config / CMD below points data_dir at this path.
VOLUME ["/var/lib/ironcache"]

# Document the ports (EXPOSE is metadata only; it does not publish). See header.
EXPOSE 6379
EXPOSE 16379
EXPOSE 26379
EXPOSE 9121

# Entry point is the binary; default mode is `server`. An operator overrides the
# args (e.g. to add `--config /etc/ironcache/ironcache.toml --metrics-addr
# 0.0.0.0:9121`) via the container `command`/`args`, and tunes everything else via
# IRONCACHE_* env vars or the mounted TOML. The binary reads /etc/ironcache/ironcache.toml
# by default when present, so a ConfigMap mounted there needs no extra flag.
ENTRYPOINT ["/usr/local/bin/ironcache"]
CMD ["server"]
