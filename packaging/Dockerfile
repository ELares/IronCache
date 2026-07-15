# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Distroless-static container image for IronCache (#122): the static musl binary
# on gcr.io/distroless/static (~2 MiB, CA certs + nonroot user), well under the
# 5 MiB ceiling. Use `scratch` instead for the absolute minimum (no certs).
#
# UNUSED local-build variant: the image pipeline (.github/workflows/image.yml)
# builds and pushes the ROOT ./Dockerfile (and ./Dockerfile.console), NOT this
# file. Kept as a minimal manual-build reference; prefer the root Dockerfile,
# which is the hadolint-gated, shipped image. The binary either way is produced
# by the cargo-zigbuild matrix in .github/workflows/release.yml. See
# docs/design/PACKAGING.md.
#
# Build wiring: the release job downloads the per-target archives
# (ironcache-<ver>-<target>.tar.gz) produced by the cross-build matrix and
# unpacks each into dist/<arch>/ironcache (dist/amd64/, dist/arm64/) before
# invoking buildx, so this Dockerfile only copies an already-built static
# binary. Build (multi-arch):
#   docker buildx build --platform linux/amd64,linux/arm64 -t ironcache:__VERSION__ .

# --- stage: stage the prebuilt static musl binary for the target arch ---------
FROM alpine:3 AS stage
ARG TARGETARCH
WORKDIR /stage
# dist/amd64/ironcache and dist/arm64/ironcache are produced by the release
# cross-build matrix and are fully static (crt-static musl), so no libc is copied.
COPY dist/${TARGETARCH}/ironcache /stage/ironcache
RUN chmod 0755 /stage/ironcache

# --- final: distroless static, nonroot ---------------------------------------
FROM gcr.io/distroless/static:nonroot
LABEL org.opencontainers.image.source="https://github.com/OWNER/REPO"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
LABEL org.opencontainers.image.version="__VERSION__"
COPY --from=stage /stage/ironcache /usr/local/bin/ironcache
USER nonroot:nonroot
EXPOSE 6379
ENTRYPOINT ["/usr/local/bin/ironcache"]
CMD ["server"]
