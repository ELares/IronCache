#!/usr/bin/env bash
# #630: build the two VERSIONED node binaries the smoke bind-mounts, plus the driver-runner image.
#
# The two binaries come from ONE source tree and differ ONLY by the compile-time
# IRONCACHE_BUILD_VERSION stamp (v1=1.0.0, v2=2.0.0) -- so we build from source instead of
# committing ~130MB artifacts. v2 also carries the `ironcache upgrade --cluster` driver used to
# roll the cluster. Both are Linux ELF (the compose runs debian:bookworm-slim), so they are built
# inside a rust container; warm named volumes make the 2nd build a near-instant relink.
#
# Prereqs: a Linux docker engine (on Apple Silicon, colima/lima -- point IC_DOCKER_BIN at your
# docker-CLI dir if it is not already on PATH). Run from this directory: ./build.sh
set -euo pipefail
export PATH="${IC_DOCKER_BIN:+$IC_DOCKER_BIN:}$PATH"
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE"
REPO="${IRONCACHE_REPO:-$(git rev-parse --show-toplevel)}"
IMG=rust:1-bookworm
mkdir -p bin
docker volume create ic-smoke-cargo  >/dev/null
docker volume create ic-smoke-target >/dev/null

build_node() { # $1=version-stamp  $2=bin-tag
  echo "==> building bin/ironcache-$2 (IRONCACHE_BUILD_VERSION=$1)"
  docker run --rm \
    -e IRONCACHE_BUILD_VERSION="$1" -e CARGO_HOME=/cargo \
    -v ic-smoke-cargo:/cargo -v ic-smoke-target:/build/target -v "$REPO":/build:ro \
    -w /build "$IMG" \
    cargo build --bin ironcache --target-dir /build/target
  docker run --rm -v ic-smoke-target:/t -v "$HERE/bin":/o "$IMG" \
    sh -c "cp /t/debug/ironcache /o/ironcache-$2 && chmod +x /o/ironcache-$2"
}

build_node 1.0.0 v1
build_node 2.0.0 v2

echo "==> building the ic630-driver image (docker CLI + compose plugin)"
docker build -q -t ic630-driver -f Dockerfile.driver . >/dev/null

echo "==> verifying the version stamps landed:"
docker run --rm -v "$HERE/bin":/o "$IMG" sh -c '/o/ironcache-v1 --version; /o/ironcache-v2 --version'
echo "build.sh done -- now run ./smoke.sh"
