#!/usr/bin/env sh
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# IronCache owned, pinned curl|sh installer (#122).
#
# Usage:
#   curl --proto '=https' --tlsv1.2 -LsSf \
#     https://github.com/OWNER/REPO/releases/latest/download/install.sh | sh
#
# This script is vendored in-repo (not a third-party hosted script) so the
# supply chain is ours end to end. It SHA256-validates the artifact before
# unpacking and manages PATH (opt out with IRONCACHE_NO_MODIFY_PATH=1).
#
# SCAFFOLD: templated and inert until the first release. The release pipeline
# substitutes __VERSION__ and the per-target __SHA256_*__ digests (from the
# release SHA256SUMS) before this script is published as a release asset.
# See docs/design/PACKAGING.md and packaging/README.md.
set -eu

APP="ironcache"
VERSION="__VERSION__"
REPO="OWNER/REPO"
BASE_URL="https://github.com/${REPO}/releases/download/v${VERSION}"
INSTALL_DIR="${IRONCACHE_INSTALL_DIR:-${HOME}/.local/bin}"

# Per-target SHA256 digests, filled from the release SHA256SUMS at publish time.
SHA256_x86_64_linux="__SHA256_X86_64_LINUX_MUSL__"
SHA256_aarch64_linux="__SHA256_AARCH64_LINUX_MUSL__"

err() { printf 'install: %s\n' "$1" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || err "required tool not found: $1"; }

detect_target() {
  _os="$(uname -s)"; _arch="$(uname -m)"
  case "$_os" in
    Linux) ;;
    Darwin) err "on macOS use: brew install ironcache" ;;
    *) err "unsupported OS: $_os (Windows is served via Docker/WSL, see docs)" ;;
  esac
  case "$_arch" in
    x86_64|amd64)  TARGET="x86_64-unknown-linux-musl";  EXPECTED_SHA="$SHA256_x86_64_linux" ;;
    aarch64|arm64) TARGET="aarch64-unknown-linux-musl"; EXPECTED_SHA="$SHA256_aarch64_linux" ;;
    *) err "unsupported architecture: $_arch" ;;
  esac
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum  >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  else err "no sha256 tool (sha256sum or shasum) available"; fi
}

main() {
  need curl; need tar; need uname; need mkdir; need install
  detect_target
  [ "$VERSION" = "__VERSION__" ] && err "scaffold installer: VERSION not substituted (no release yet)"

  _archive="${APP}-${VERSION}-${TARGET}.tar.gz"
  _tmp="$(mktemp -d)"
  trap 'rm -rf "$_tmp"' EXIT

  printf 'install: downloading %s\n' "$_archive"
  curl --proto '=https' --tlsv1.2 -fLsS "${BASE_URL}/${_archive}" -o "${_tmp}/${_archive}"

  # Validate the SHA256 BEFORE unpacking a single byte.
  _got="$(sha256_of "${_tmp}/${_archive}")"
  if [ "$_got" != "$EXPECTED_SHA" ]; then
    err "checksum mismatch for ${_archive}: expected ${EXPECTED_SHA}, got ${_got}"
  fi
  printf 'install: checksum OK\n'

  tar -xzf "${_tmp}/${_archive}" -C "$_tmp"
  mkdir -p "$INSTALL_DIR"
  install -m 0755 "${_tmp}/${APP}" "${INSTALL_DIR}/${APP}"
  printf 'install: installed %s to %s\n' "$APP" "$INSTALL_DIR"

  if [ "${IRONCACHE_NO_MODIFY_PATH:-0}" != "1" ]; then
    case ":${PATH}:" in
      *":${INSTALL_DIR}:"*) : ;;
      *) printf 'install: add %s to PATH (set IRONCACHE_NO_MODIFY_PATH=1 to skip):\n  export PATH="%s:$PATH"\n' "$INSTALL_DIR" "$INSTALL_DIR" ;;
    esac
  fi
  printf 'install: done. run: %s server\n' "$APP"
}

main "$@"
