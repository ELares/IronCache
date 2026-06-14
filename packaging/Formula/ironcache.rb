# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Plain Homebrew formula for IronCache (#122): no tap, no cask, no conflicting
# formula. `brew install ironcache` is strictly easier than Redis/Valkey on
# macOS, where the two upstream formulae mutually conflict.
#
# SCAFFOLD: templated and inert until the first release. The release pipeline
# substitutes __VERSION__ and the per-bottle __SHA256_*__ digests (from the
# release SHA256SUMS) before this formula is submitted to homebrew-core.
# See docs/design/PACKAGING.md and packaging/README.md.
#
# NOTE (implementer): the canonical entrypoint is `ironcache cli` (ADR-0020). A
# `redis-cli` symlink is NOT shipped here because homebrew-core rejects a
# formula that installs a binary name owned by another formula (the redis
# formula's `redis-cli`), which would reintroduce exactly the conflict this
# formula advertises avoiding. The muscle-memory alias is left to the user
# (e.g. `alias redis-cli="ironcache cli"`).
class Ironcache < Formula
  desc "Reproducible single static binary in-memory cache (Redis-compatible wire)"
  homepage "https://github.com/OWNER/REPO"
  version "__VERSION__"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_arm do
      url "https://github.com/OWNER/REPO/releases/download/v#{version}/ironcache-#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "__SHA256_AARCH64_APPLE_DARWIN__"
    end
    on_intel do
      url "https://github.com/OWNER/REPO/releases/download/v#{version}/ironcache-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "__SHA256_X86_64_APPLE_DARWIN__"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/OWNER/REPO/releases/download/v#{version}/ironcache-#{version}-aarch64-unknown-linux-musl.tar.gz"
      sha256 "__SHA256_AARCH64_LINUX_MUSL__"
    end
    on_intel do
      url "https://github.com/OWNER/REPO/releases/download/v#{version}/ironcache-#{version}-x86_64-unknown-linux-musl.tar.gz"
      sha256 "__SHA256_X86_64_LINUX_MUSL__"
    end
  end

  def install
    bin.install "ironcache"
  end

  service do
    run [opt_bin/"ironcache", "server"]
    keep_alive true
    working_dir var
    log_path var/"log/ironcache.log"
    error_log_path var/"log/ironcache.log"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/ironcache --version")
  end
end
