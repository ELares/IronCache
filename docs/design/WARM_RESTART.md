# Design: mmap warm restart (graceful drain, state sidecar, pointer fixup)

Issue: #62. Decisions: ADR-0014 (ephemeral default, warm restart is opt-in tier 2,
not a durability guarantee), ADR-0023 (cold engine the flash index reloads
against). Related: #58 (persistence umbrella, PERSISTENCE.md), #139 (graceful
shutdown contract, SHUTDOWN.md), #66 (cold tier), #64 (hybrid-log engine), #60
(forkless snapshot, the separate durable path), #83 (binary-upgrade swap), #28
(io_uring write path).

## Goal and scope

IronCache is a single static binary that is redeployed often, and a cold cache on
every binary swap is a real steady-state cost. This spec realizes the warm-restart
tier promised in ADR-0014: a graceful stop writes the working set as an mmap heap
image plus a small `.meta` sidecar, and the next boot reattaches the mapping,
fixes up pointers, and regenerates the index in seconds with zero re-warm. We
adapt the Memcached model, which mmaps item memory to a tmpfs or DAX file, shuts
down on SIGUSR1 writing a `.meta` file, and on restart fixes internal pointers and
regenerates the hash table, earliest from 1.5.18
[memcached-warm-restart-mmap-sigusr1], but make it first class rather than
EXPERIMENTAL [warm-restart].

In scope: the mmap data region, the SIGUSR1 graceful drain, the `.meta` sidecar
contents, relative-offset pointer fixup, O(n) index regeneration, the
tmpfs-default / DAX-opt-in mount, the invalidation conditions that abort to a cold
start, and coexistence with the flash tier (#66). Out of scope and owned
elsewhere: durable persistence across crashes and the durability menu stance
(ADR-0014, #58), the broader SHUTDOWN SAVE/NOSAVE and connection-drain contract
(#139) that this drain composes with, and the forkless versioned snapshot
[dragonfly-forkless-versioned-snapshot] which stays the separate durable path
(#60). Warm restart is a restart convenience, not a durability guarantee.
Conflicts resolve Compatible and Efficient before Simple.

## Design

### SIGUSR1 graceful drain then write the sidecar

The restart trigger is a SIGUSR1 graceful drain that quiesces the data plane,
stops accepting writes, lets in-flight commands complete, and then writes the
`.meta` sidecar before exiting cleanly, matching the Memcached signal and ops
tooling [memcached-warm-restart-mmap-sigusr1] [warm-restart]. A RESP admin
command is the rejected alternative because it needs an authed live connection;
the signal does not. This drain is the warm-restart-specific arm of the broader
graceful-shutdown contract (#139, SHUTDOWN.md): #139 owns SIGTERM/SIGINT, the
SAVE/NOSAVE choice, connection drain, and the orchestrator exit-code and
grace-timeout contract, and warm restart slots in as one drain outcome that
leaves a reattachable mapping behind. SHUTDOWN.md names #62 as the owner of this
SIGUSR1 trigger boundary, so the split is explicit on both sides.

### The .meta sidecar: fingerprint, clock stamp, index seeds

The data region stays a pure heap image; all restart metadata lives in a `.meta`
sidecar next to the mmap file. The sidecar holds a layout header (heap item layout
version and the mapping base length), a settings fingerprint (size classes, max
item size, shard count, CAS on/off, and the like), a clock stamp (monotonic and
wall reference at shutdown, since TTLs are wall-clock relative), and the index
seeds needed to deterministically rebuild the shard/hash index. Keeping the
metadata out of band rather than inline in the mmap region keeps the data region a
plain heap image and makes the format easy to version.

### Relative-offset pointer fixup on boot

All intra-mapping pointers are stored as relative offsets within the mmap region
and rebased against the actual mapping base on boot. The rejected alternative is
re-mapping at a fixed address with MAP_FIXED, which is brittle under ASLR and
unsafe; relative offsets are portable across address-space layout changes between
the old and new process. This is the same internal-pointer-fixup step Memcached
performs on restart [memcached-warm-restart-mmap-sigusr1], expressed as a rebase
rather than an address assumption.

### O(n) index regeneration, not a persisted index

The boot path regenerates the hash/shard index by a single O(n) scan over the
mmapped item heap rather than persisting the index, which is exactly Memcached
regenerating the hash table on restart [memcached-warm-restart-mmap-sigusr1].
Regenerating avoids coupling the on-disk format to an index layout and pays only
one scan. Because tmpfs pages are already resident, recovery time is dominated by
this scan and not by I/O, so recovery scales with item count rather than dataset
bytes on the wire.

### tmpfs by default, DAX opt-in

The mmap file lives on tmpfs by default so warm restart runs anywhere and keeps
the single-binary promise, with DAX or pmem as an opt-in mount where present,
matching Memcached putting the memory file on a RAM disk or DAX mount
[memcached-warm-restart-mmap-sigusr1] [warm-restart]. Requiring DAX is the
rejected alternative because it would break portability. Whether DAX additionally
buys documented crash survival is an open question below; the default tmpfs path
does not survive a crash, only a graceful restart. The sidecar write goes through
the shared io_uring path (#28, also used by the snapshot and cold tier), with
SQPOLL keeping the submit syscall off the hot cores [io-uring-sqpoll-registered-buffers],
and a blocking fallback for older kernels and macOS dev.

### Invalidation aborts to a cold start

If the settings fingerprint does not match the running binary, or the clock stamp
fails its validity rule (for example wall clock moved backwards), or the heap item
layout version differs, the warm restart aborts to a clean cold start and logs the
reason. The fingerprint captures the settings that change item or slab layout
(size classes, max item size, shard count, CAS on/off, and slab reassignment), so
a change to any of them forces a cold start rather than a silently mis-rebased
load. Memcached likewise requires the system clock not to jump while the process
is down [warm-restart] [memcached-warmrestart-incompatible-extstore]; we keep that
requirement and make it explicit in the clock stamp rather than inheriting it
implicitly. Aborting to cold start is chosen over best-effort partial load,
because partial load risks silently serving corrupt or mis-rebased items.

### Coexistence with the flash tier

Memcached's warm restart is presently incompatible with extstore, and writes
during the restart window are missed [memcached-warmrestart-incompatible-extstore].
We reject that XOR limitation. On warm restart IronCache re-maps the RAM tier and
reloads the flash location index, which is the keys plus the tiny in-RAM location
pointer [memcached-extstore-keys-in-ram-12b-pointer], so the cold tier (#66) and
warm restart coexist. The flash-resident values themselves are not re-read on
boot; only the in-RAM pointer table is rebuilt, against the hybrid-log cold engine
of a hot log in RAM and a cold log on disk with a read cache
[f2-hot-cold-log-two-tier] decided in ADR-0023. The flash tier's own durable
recovery (fsync'd pages plus a recovered pointer table) is #66's contract; warm
restart only restores the in-RAM half quickly.

## Open questions

- Exact settings-fingerprint contents and which changes force a cold start versus
  a transparent migration (#66 size classes and shard count interact here).
- Clock invalidation rule: abort on monotonic-vs-wall skew over a threshold, or
  only when wall clock moved backwards, given TTLs are wall-clock relative.
- Whether DAX buys documented crash survival worth advertising, or stays a pure
  performance and portability note.
- The swap sequence with #83 so the new binary attaches the existing mapping
  before the old process releases it.
- Behavior when `.meta` is present but the binary's on-heap item layout version
  differs (abort versus a bounded migration).
- Whether warm-restart mmap and the forkless snapshot share one on-disk format
  (inherited open question from PERSISTENCE.md / #58).

## Acceptance and test hooks

- SIGUSR1 drains, refuses new writes, completes in-flight commands, writes `.meta`,
  and exits cleanly; the next boot reattaches the mmap and regenerates the index
  with zero re-warm, asserted end to end.
- A binary upgrade with the same item-layout version and settings fingerprint
  keeps the cache warm across the swap, verified with #83.
- Each invalidation condition (settings-fingerprint mismatch, clock-stamp
  failure, item-layout-version mismatch) aborts to a clean cold start and logs the
  reason; a fault-injection test toggles each condition.
- Pointers are stored and verified as relative offsets; a test rebases the heap at
  a different base address and the index regenerates correctly, with no MAP_FIXED
  dependency asserted structurally.
- Warm restart and the flash tier coexist: the RAM tier re-maps and the flash
  location index reloads with no XOR limitation
  [memcached-warmrestart-incompatible-extstore].
- Published recovery-time-versus-dataset-size measurements (for example 1, 10,
  100 GB) show seconds-scale reload with the O(n) index scan as the dominant cost,
  not I/O.
- The `.meta` sidecar write flows through the shared io_uring path (#28); the
  no-blocking-pwrite lint covers it.

## References

- ADR-0014, ADR-0023; issues #62, #58, #139, #66, #64, #60, #83, #28.
- Specs: PERSISTENCE.md (#58), SHUTDOWN.md (#139).
- Claims: [memcached-warm-restart-mmap-sigusr1], [warm-restart],
  [memcached-warmrestart-incompatible-extstore],
  [memcached-extstore-keys-in-ram-12b-pointer],
  [dragonfly-forkless-versioned-snapshot], [io-uring-sqpoll-registered-buffers],
  [f2-hot-cold-log-two-tier].
