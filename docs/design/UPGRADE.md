# Design: ironcache upgrade with verified rollback

Issue: #83. Decisions: ADR-0020 (clap `upgrade` subcommand + minisign artifact
signing), ADR-0017 (Simple gate: kernel-only at runtime, static-binary size
ceiling, install-time bound). Related: #81 (single static binary), #82 (the
`upgrade` verb surface), #84 (packaging, artifact, and signature distribution),
#62 (warm-restart handoff; open cross-ref), #1 (vision EPIC).

## Goal and scope

Replacing a long-lived cache process is the highest-risk routine operation
IronCache has: an in-flight working set is at stake, and a corrupt or
incompatible binary that survives the swap takes the node down until a human
intervenes. `ironcache upgrade` (the clap subcommand, ADR-0020) upgrades the
single static binary (#81) in place with a verified, automatically reversible
swap, so a bad build can never strand a node. The mechanism (1) verifies the new
artifact's detached signature before touching disk, (2) swaps atomically while
keeping the prior binary recoverable in exactly one slot, (3) probes the new
process for liveness, version, and readiness, and (4) rolls back on its own if
the probe fails, all without writing to the state directory.

In scope: the on-node upgrade mechanism and its rollback contract. Out of scope:
the release pipeline, how artifacts and signatures are produced and published
(#84), and signing-key generation and rotation (the on-node side only consumes
and pins the public key per ADR-0020). Upgrade is operator-initiated only; there
is no background self-updater.

## Design

### Verify before swap (minisign, offline single key, no PKI)

The upgrade refuses to swap an unverified artifact. `ironcache upgrade` fetches
the new binary and its detached minisign signature, then verifies the signature
over the new binary against a public key pinned on the node before any on-disk
change. Minisign verifies a detached signature over a file with a single small
Ed25519 public key, offline, with no PKI and no transparency log or network
dependency [minisign-offline-ed25519-detached-verify]; the public key ships in
the repo and the docs, so any operator can verify on any box with no
infrastructure (ADR-0020 rejected cosign/sigstore for exactly this reason). The
embedded reproducible-build SBOM (cargo-auditable, no timestamps, sorted
[cargo-auditable-version-reproducible]) is checked alongside the signature so the
artifact's dependency manifest is consistent with the supply-chain gate. The
upgrade only checks the SBOM's presence and consistency with the verified
artifact; what the SBOM is allowed to contain (the cargo-deny/license/RUSTSEC
policy) is the merge/release gate's job, not the on-node mechanism's
(SUPPLY_CHAIN.md, #144). A signature mismatch, a missing signature, or an SBOM
mismatch aborts the upgrade with no on-disk change and a nonzero exit; the
verifying key is the only trust anchor.

Resolved decision (signature scheme and key pinning): minisign detached
signature, verified before the swap, against an in-repo/in-docs public key
verified offline. Key rotation is a release-pipeline concern (#84), out of scope
for the on-node mechanism, which consumes whatever key is pinned at install
time.

### Atomic swap with one retained slot

The verified new binary is written to a temporary file in the binary's own
directory (the same filesystem, so `rename(2)` is atomic), then swapped onto the
live `ironcache` path. We borrow the self-update atomic-rename pattern from #81:
the new file is placed next to the current executable and an atomic rename swap
is performed, which on Unix you can do even though you cannot write into a
running executable [self-replace-atomic-rename]. The self-update crate ecosystem
replaces the running binary via this same atomic rename but ships no built-in
rollback [self-update-crate-version-backends]; the rollback contract below is the
part IronCache adds on top.

The swap uses a NEVER-ABSENT single-rename idiom so the `ironcache` path is never
momentarily missing (a crash / power-loss in an absent window would leave systemd
with no `ExecStart` binary and no auto-recovery). The earlier two-rename sequence
(`rename(ironcache -> ironcache.old)` then `rename(ironcache.new -> ironcache)`)
had exactly that absent window between the two renames and is rejected. Instead:
(1) stage the verified new bytes to `ironcache.new` on the same filesystem,
fsync'd; (2) while `ironcache` STILL exists, create the `ironcache.old` rollback
slot by hard-linking the current `ironcache` inode into it (falling back to a
durable byte copy when hard-linking is unavailable, e.g. `EXDEV`/`EMLINK`); (3)
ONE atomic `rename(ironcache.new -> ironcache)`, which replaces the destination
atomically, so `ironcache` transitions directly from the old inode to the new one
with no absent window, and the old inode survives because `ironcache.old` still
names it. An interruption therefore always leaves `ironcache` pointing at either
the old inode or the new one, never absent and never torn. A symlink `ironcache`
is refused (it would be clobbered into a real file and `.old` would dangle); the
operator points the swap at the real binary.

Exactly one prior binary is retained, as `ironcache.old`, alongside the live
binary, not in the state directory. One slot is the Simple choice (ADR-0017),
bounds disk to a single extra copy, and keeps rollback independent of
state-directory health. A versioned archive of old binaries is explicitly
rejected; one recoverable predecessor is the whole contract. Rollback restores
`ironcache.old` onto `ironcache` while PRESERVING the `.old` slot (a fresh
hard-link/copy + atomic rename), so `.old` always still holds the
last-known-good binary and a subsequent failed upgrade can still roll back.

### Post-swap stabilization probe

After the swap, the new process must clear a health bar before `ironcache.old`
is retired. The probe is the sole arbiter of "good"; assuming success is
rejected. The new process must, within a bounded `--health-timeout`:

1. Start and stay up past a stabilization window (it must not crash-loop
   immediately after exec).
2. Report a `--version` string exactly equal to the requested upgrade target.
3. Answer a readiness check confirming it has reattached the handed-off working
   set and is accepting connections.

Resolved decision (`--version` match): exact equality to the requested target,
not a compat range. An exact match keeps the probe unambiguous: the operator
asked for version X, and the running process must report exactly X or the
upgrade is treated as failed. A compat-range allowance would let a silently
wrong build pass the probe.

Resolved decision (readiness semantics): readiness requires both that the
process is accepting connections and that it has reattached the handed-off
working set (the #62 readiness signal), not merely that the socket is open.
Accepting connections on a cold, empty cache is not a successful upgrade of a
warm node.

Resolved decision (stabilization window and `--health-timeout` defaults): a 5s
stabilization window inside a 30s total `--health-timeout` by default, both
overridable on the command line. The stabilization window guards against an
immediate crash-loop; the outer timeout bounds how long the operator waits
before auto-rollback fires. These are defaults, not invariants, and are tuned
against the recovery-time measurements from #62 once they exist.

### Auto-rollback on any miss

Any missed probe condition (process down, `--version` mismatch, readiness not
reached within `--health-timeout`) triggers automatic rollback with no operator
action: restore `ironcache.old` onto `ironcache` (the same atomic rename, in
reverse), re-exec, and re-probe the restored binary. Auto-rollback keeps the
node serving rather than leaving a dead binary and paging a human, so a failed
upgrade never strands the node. A successful probe instead promotes the new
binary and discards the `.old` slot. After a rollback the failed new artifact is
discarded (not retained as `.old`); the `.old` slot always holds the
last-known-good binary.

### State directory is untouched

Rollback never writes to or depends on the state directory. The `.old` slot
lives alongside the binary, the swap and its reverse are pure binary-path rename
operations, and the working-set continuity comes from the warm-restart handoff
(#62), not from rollback rewriting persisted data. A failed upgrade therefore
cannot corrupt persisted state: the worst case is a reverted binary and a
working set reloaded through the normal #62 path.

### Data continuity via the warm-restart handoff (#62)

The swap is driven through the #62 warm-restart handoff so the working set
survives the binary swap rather than cold-flushing: the new binary attaches the
existing mmap before the old process releases it (the swap-sequence coordination
called out as an open decision in #62). This is what makes the readiness check's
"working set reattached" condition meaningful.

## Open questions

- Behavior when the warm-restart handoff (#62) fails but the new binary is
  otherwise healthy (up, correct `--version`, accepting connections on a cold
  cache). Options: treat a cold reattach as a probe failure and roll back, or
  accept the upgrade and log a cold-start warning. This is the open cross-ref to
  #62; resolving it requires the #62 fingerprint/clock invalidation rules and
  the recovery-time measurements, and it is deferred to that issue.
- Whether the stabilization window and `--health-timeout` defaults (5s/30s)
  hold once #62 publishes recovery-time-vs-dataset-size numbers; a 100 GB
  reattach may need a larger readiness allowance than 30s.
- Whether `upgrade` should refuse to run if the on-disk binary it is replacing
  is not the currently running one (detecting an out-of-band swap), or treat
  that as the operator's responsibility.

## Acceptance and test hooks

- `ironcache upgrade` verifies the detached minisign signature against the
  pinned public key and refuses (nonzero exit, no on-disk change) on a mismatch,
  a missing signature, or an SBOM mismatch
  [minisign-offline-ed25519-detached-verify][cargo-auditable-version-reproducible].
- The swap is atomic AND never-absent: an upgrade interrupted at any point leaves
  the `ironcache` path pointing at a runnable binary (the old inode before the
  single final rename, the new one after it), never absent and never torn
  [self-replace-atomic-rename].
- Exactly one `ironcache.old` is retained alongside the binary across an
  upgrade; a second upgrade replaces it (no versioned archive accumulates).
- A failed health probe (process down, `--version` not exactly the target, or
  readiness not reached within `--health-timeout`) auto-restores the prior
  binary with no operator action, and the node keeps serving.
- A successful probe (up past the stabilization window, exact `--version` match,
  readiness confirming working-set reattach and accepting connections) promotes
  the new binary and discards `.old`.
- No code path updates a live server implicitly; upgrade is operator-initiated
  via the `upgrade` verb (#82, ADR-0020).
- The working set is preserved across the swap via the #62 handoff; a rollback
  neither writes to nor reads from the state directory (verified by running an
  upgrade and a forced rollback against a read-only state directory).

## References

- ADR-0020 (CLI dispatch + minisign signing), ADR-0017 (Simple gate);
  issues #83, #81, #82, #84, #62, #1; specs CLI_BINARY.md, SUPPLY_CHAIN.md;
  packaging/distribution and signature publication are #84.
- Claims: [minisign-offline-ed25519-detached-verify],
  [self-replace-atomic-rename], [self-update-crate-version-backends],
  [cargo-auditable-version-reproducible].

<!-- Mechanism + lossless write-freeze implemented in #393 / #395 (epic #385). -->
