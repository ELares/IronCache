# Design: At-rest encryption of snapshots, warm-restart state, and tiered-store SSD files

Issue: #143. Decisions: ADR-0014 (ephemeral default, opt-in persistence, so at-rest
encryption is an opt-in over an already-opt-in feature), ADR-0017 (Simple gate: musl
static, kernel-only, no sidecar, so key handling and AEAD live inside the process),
ADR-0009 (behavioral equivalence: the on-disk RDB/AOF-shaped formats are kept;
encryption is an outer framing, not a format change). Related: #60/SNAPSHOT.md
(forkless snapshot base records), #62/WARM_RESTART.md (mmap `.meta` state file and
pointer fixup), #63/DURABLE_LOG.md (segment + atomic manifest), #66/TIERED_STORE.md
(cold values spilled to flash pages), #58/PERSISTENCE.md (the durability umbrella and
the shared io_uring write path), #145/SECRETS.md (the in-memory key-handling sibling),
#142/THREAT_MODEL.md (the asset and accepted-risk row this discharges), #105/TLS.md
(in-transit counterpart and key-material handling), #85/CONFIG.md (the opt-in knobs),
#59 (the ephemeral-by-default stance), #22 (the parent security epic), #5 (the
open-gap record), #1 (the vision EPIC).

## Goal and scope

IronCache writes plaintext keyspace bytes to disk in three places: forkless snapshot
and diskless-sync base records (#60), the mmap warm-restart `.meta` state file (#62),
and cold values spilled to flash in the RAM->SSD tier (#66), with the durable log
framing those records as segments under an atomic manifest (#63). All persist on
media that can be stolen, hypervisor-snapshotted, or read host-locally, yet the
security work to date covers only in-transit TLS (#105) and in-memory secret
handling (#145); the threat model lists plaintext-at-rest as an accepted-for-now risk
to be revisited when this lands (#142). This spec decides the at-rest posture:
optional envelope encryption with a configured or KMS-provided key, an AEAD applied
over segment, page, and `.meta` records, key rotation that crosses the manifest (#63)
and the warm-restart pointer fixup (#62), a fail-closed posture when the key is
unavailable, and a cost budget that keeps encryption off the hot path and opt-in to
match the ephemeral-by-default stance (#59, ADR-0014).

In scope: the AEAD choice and per-record framing, the envelope key hierarchy and KMS
seam, where encryption sits relative to compression and the io_uring writer, rotation
mechanics against the manifest and the warm-restart file, and the fail-closed and
test contract. Out of scope: in-memory secret handling (owned by SECRETS.md, #145),
the in-transit channel (TLS.md, #105), the on-disk formats themselves (#60/#62/#63/
#66 own those; this spec is an outer framing over them), process sandboxing and
privilege reduction (#84, which is isolation, not encryption), and the aclfile, which
is a plaintext-hash file owned by #106 and not part of the keyspace at-rest surface.

## Design

### Opt-in envelope encryption, off by default

- At-rest encryption is off by default and enabled per persistence surface through a
  hardening config knob (CONFIG.md, #85), the same posture the durability menu itself
  takes (opt-in over an ephemeral default, ADR-0014). When off there is zero added
  cost and the on-disk bytes are exactly the #60/#62/#63/#66 formats. When on, every
  durable record this binary writes (snapshot base, durable-log segment/incremental,
  cold-tier flash page, warm-restart `.meta`) is sealed before it reaches disk and
  opened after it is read back, and a manifest flag records that the live segment set
  is encrypted so a reader cannot silently misinterpret ciphertext as plaintext.

### AEAD over segment, page, and .meta records

- Records are protected with an Authenticated Encryption with Associated Data (AEAD)
  construction, so each sealed record carries both confidentiality and an integrity
  tag that detects tampering or bit-rot; the record header (segment id, offset, record
  type, length, and the manifest-bound key/version identifiers) is bound as the
  Associated Data so a ciphertext cannot be replayed into a different position or a
  different segment without failing the tag check. This composes with, and does not
  replace, the existing CRC placement in the durable-log format (#63): the CRC catches
  accidental corruption cheaply on the read path, the AEAD tag catches adversarial
  tampering; both must pass.
- The AEAD primitive is a maintained Rust AEAD crate from the RustCrypto `aeads`
  family, version-pinned in the lockfile the same way the TLS and zeroize crates are
  [rustcrypto-aeads-crate-versions-pinned], keeping the no-C-dependency posture that
  TLS already holds with rustls [rustls-pure-rust-tls12-tls13] and the Simple gate
  (ADR-0017). The candidate primitives are AES-256-GCM and XChaCha20-Poly1305; the
  tradeoff is hardware acceleration and a 96-bit nonce that forces strict
  per-key nonce-uniqueness discipline (AES-GCM) versus a 192-bit extended nonce that
  tolerates random nonce generation without a practical collision risk, at the cost of
  no dedicated CPU instruction on most cores (XChaCha20)
  [aead-aes-gcm-vs-xchacha20-nonce-tradeoff]. The default leans XChaCha20-Poly1305 for
  its nonce-misuse margin given that segments and flash pages are written from many
  shards over a long-lived data key, with AES-256-GCM selectable where AES-NI makes it
  the throughput winner; the final default is an open question pending the cost
  measurement below.
- Nonce management is a per-record scheme: each sealed record gets a nonce that is
  unique under its data key for the life of that key, derived from a per-segment or
  per-page deterministic counter component combined with the data-key generation so
  that no `(key, nonce)` pair is ever reused even across a warm restart that resumes
  appending to a segment. This is stated as a design choice; the durability of the
  counter (where the highest-issued nonce component is recorded so a crash cannot
  rewind it) ties into the manifest durable cut (#63) and is an open question below.

### Envelope key hierarchy: data key wrapped by a KEK or KMS

- Keys follow an envelope model: bulk records are sealed under a data encryption key
  (DEK) that lives only in process memory, and the DEK is itself wrapped (encrypted)
  under a key-encryption key (KEK) that the operator supplies directly or that a KMS
  holds, so the cleartext DEK is never written to disk and rotating the protection of
  the whole dataset is a cheap rewrap of the wrapped-DEK blob rather than a re-encrypt
  of every record [envelope-encryption-dek-wrapped-by-kek-kms]. The wrapped DEK and
  its key-version id are recorded in the manifest (#63) alongside the segment list, so
  loading the dataset means: read the manifest, ask the KEK/KMS to unwrap the DEK,
  then open records; the plaintext DEK is held in a zeroize-on-drop type (SECRETS.md,
  #145, [zeroize-crate-on-drop]) and kept out of swap and coredumps by the same
  mlock/no-coredump hardening SECRETS.md specifies.
- The KEK source is pluggable behind a small trait: a locally configured key (file or
  CONFIG, #85), or an external KMS that performs the unwrap (and optionally the wrap)
  out of process so the KEK itself never enters IronCache memory. The KMS seam is a
  trait, not a named vendor, consistent with the Simple gate (ADR-0017): the in-tree
  default is the local-KEK provider; a KMS provider is an additive implementation.

### Off the hot path

- All sealing and opening happen on the persistence path only, never on a RESP
  GET/SET serving a request from the in-RAM hot tier. Snapshot serialization, durable
  log appends, cold-tier spill, and warm-restart write are already off the request
  core (PERSISTENCE.md routes them through the shared io_uring streaming writer, #28);
  encryption is one more transform in that same streaming pipeline. Because the value
  codec (COMPRESSION.md, #52) is an in-RAM transparent compression of values, the
  bytes reaching the persistence path may already be compressed, so the framing is
  naturally compress-then-encrypt (ciphertext does not compress); encryption is
  applied after any compression step and before the io_uring submission, so it
  inherits the existing back-pressure and `durable_offset` accounting and adds no
  allocation on the read serving path. The only hot-tier interaction is on a cold-tier
  read miss that must open a flash page, which is already a disk-latency event, so the
  AEAD open is in the noise of the SSD read it accompanies.

### Fail closed on key-unavailable

- If the KEK or KMS is unavailable at load (KMS unreachable, wrong or missing local
  KEK, unwrap failure) or an AEAD tag check fails on read, IronCache fails closed: it
  refuses to start serving from, or to continue writing to, an encrypted surface it
  cannot authenticate, rather than degrading to plaintext or serving unverified bytes.
  This reuses the persistence fail-closed contract (PERSISTENCE.md) and the defined
  OOM-write-style error surface (ADMISSION.md, #137) so the operator sees an explicit
  error, not a stall or a silent plaintext fallback. A tag failure on a single record
  is a per-segment corruption-recovery event in the #63 sense (quarantine that
  segment, surface it), not a whole-process abort, but an unwrap failure for the DEK
  is fatal to opening the dataset because nothing can be read without it.

### Key rotation crossed with the manifest and warm-restart fixup

- Two rotation granularities are supported. KEK rotation is cheap: rewrap the existing
  DEK under the new KEK and atomically rewrite the manifest's wrapped-DEK blob and
  key-version id (#63 already rewrites the manifest atomically), touching no record.
  DEK rotation is the expensive case: a new DEK seals new segments while old segments
  stay readable under the retained old DEK, and old data migrates to the new DEK only
  through the normal compaction/rewrite the durable log already performs (#63 rewrite
  triggers), so rotation is amortized into compaction rather than a stop-the-world
  re-encrypt. The manifest therefore may reference more than one DEK version during a
  rotation window, each segment tagged with the DEK version that sealed it.
- The warm-restart `.meta` file (#62) carries the same key/version identifiers so the
  pointer-fixup and index-regeneration pass on the next boot can open the mmap item
  data under the correct DEK; a warm restart that finds the key unavailable fails
  closed and falls back to a cold start (re-warm from the backing store) rather than
  loading unverifiable item bytes. Because warm restart is explicitly a convenience
  and not a durability guarantee (#62), this fallback is acceptable.

## Open questions

- The default AEAD primitive (AES-256-GCM vs XChaCha20-Poly1305), decided by the
  cost measurement below and the deployment's AES-NI availability
  [aead-aes-gcm-vs-xchacha20-nonce-tradeoff].
- Where the highest-issued nonce counter component is durably recorded so a crash or
  warm restart cannot rewind it and reuse a `(key, nonce)` pair, and whether it rides
  the manifest durable cut (#63) or a per-segment header.
- Whether the warm-restart `.meta` mmap and the snapshot share one encrypted framing
  or two, inheriting the same open question PERSISTENCE.md raises for the unencrypted
  formats (#62 vs #60).
- The DEK-rotation completion policy: rely solely on natural compaction to migrate old
  segments, or offer an explicit background re-encrypt to retire an old DEK on a
  bounded schedule.
- Whether the KMS provider may also perform the wrap (so the KEK never enters the
  process even at provisioning) or only the unwrap, decided with the KMS-seam trait.

## Acceptance and test hooks

- With encryption off, the on-disk bytes are byte-identical to the unencrypted
  #60/#62/#63/#66 formats and there is no measurable added cost (a no-op-path
  assertion).
- With encryption on, a snapshot file, a durable-log segment, a cold-tier flash page,
  and a warm-restart `.meta` file each contain no plaintext keyspace bytes (a scan
  asserts no known key/value plaintext appears), and each record opens to the original
  plaintext through the AEAD [rustcrypto-aeads-crate-versions-pinned].
- A flipped ciphertext bit or a record moved to a different segment offset fails the
  AEAD tag check (Associated Data binds the position), and the failure is surfaced as
  a per-segment corruption-recovery event (#63), not a silent read.
- Key-unavailable on load (missing/wrong local KEK, KMS unreachable) makes the
  encrypted surface fail closed with a defined error (ADMISSION.md #137 surface), with
  no plaintext fallback and no partial serve; a warm restart in the same condition
  falls back to a cold start.
- KEK rotation rewraps the DEK and rewrites the manifest atomically with zero record
  re-encryption [envelope-encryption-dek-wrapped-by-kek-kms]; a DEK rotation leaves
  old segments readable under the retained old DEK and migrates them only via
  compaction, with the manifest correctly referencing both DEK versions during the
  window.
- A `(key, nonce)` uniqueness assertion holds across a crash-and-resume that continues
  appending to an open segment (no nonce is reissued after restart).
- Encryption runs only on the persistence path: a hot-path lint/assertion shows no
  AEAD seal/open call reachable from RESP GET/SET on the in-RAM tier, and the seal
  step sits after any compression (#52) and before the shared io_uring submission (#28).
- The plaintext DEK is held in a zeroize-on-drop type and is locked out of swap and
  coredumps (SECRETS.md #145, [zeroize-crate-on-drop]); a drop test asserts the DEK
  buffer reads as zeroed after use.

## References

- ADR-0014, ADR-0017, ADR-0009; issues #143, #60, #62, #63, #66, #58, #145, #142,
  #105, #85, #59, #22, #137, #52, #28, #84, #106, #5, #1; specs SNAPSHOT.md,
  WARM_RESTART.md, DURABLE_LOG.md, TIERED_STORE.md, PERSISTENCE.md, SECRETS.md,
  THREAT_MODEL.md, TLS.md, CONFIG.md, ADMISSION.md, COMPRESSION.md.
- Claims: [aead-aes-gcm-vs-xchacha20-nonce-tradeoff],
  [envelope-encryption-dek-wrapped-by-kek-kms],
  [rustcrypto-aeads-crate-versions-pinned], [rustls-pure-rust-tls12-tls13],
  [zeroize-crate-on-drop].
