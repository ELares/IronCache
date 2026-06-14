# Experiment: Cache value-size and compressibility distribution survey

Issue: #57. Provisional decision: docs/research/compression-encoding.md (area
compression) provisionally REJECTS the inherited 16384-byte fixed compression
threshold and provisionally pins the size/entropy gate as workload-derived, with
COMPRESSION.md (#52) and DICTIONARIES.md (#55) consuming those numbers as
PROVISIONAL defaults until this survey replaces them.

## Provisional decision (already pinned)

The compression-encoding research doc adapts threshold-gated compression but
rejects the spymemcached default: "Default threshold likely lower than 16 KB
given zstd dictionaries help small values." The reasoning is twofold. First, the
16384-byte floor is a client-library GZIP heuristic
[spymemcached-default-compression-threshold], not an IronCache server policy.
Second, trained zstd dictionaries lift sub-1 KB structurally similar values from
about 2.8x to about 6.9x [zstd-dictionary-small-data-6.9x], so a fixed 16 KB
floor would exclude exactly the values dictionaries make profitable. The doc
therefore pins the GATE SHAPE (a size floor plus a cheap entropy probe plus an
incompressible flag) but leaves the actual threshold, probe parameters, and
per-prefix dictionary eligibility to be set by measurement. COMPRESSION.md (#52)
and DICTIONARIES.md (#55) both cite #57 as the source of those provisional
numbers. This experiment does not re-decide the gate shape; it produces the
numbers the gate is parameterized with.

## Why this is harness-blocked

The decision rule is "compress only where it net-saves bytes per CPU
millisecond," which is a property of real value bytes, not of a citation. No
public claim states the IronCache value-size or entropy distribution, because it
depends on this project's own representative workloads. Resolving it requires
running codecs over a representative corpus, building size histograms, and
probing entropy. That corpus, the codec build, and the measurement harness do
not exist yet, so the issue is blocked on running the experiment.

## Experiment to run

Corpus: assemble at least three value classes, each a separate sample set.
(1) session blobs (serialized session/auth state), (2) serialized application
objects (JSON and protobuf records), (3) ID sets (membership bitmaps and
ordered-id payloads). Each class must carry a key-prefix label so per-prefix
homogeneity can be measured. Draw enough values per class to populate stable
upper-tail percentiles; document the exact sample count and source per class.

Fixed parameters: codec = zstd at the ADR-0015 default low level; dictionary
training = ZDICT_trainFromBuffer (fastCover); a single fixed entropy-probe
definition (compressed length of a fixed-size head sample of each value).

Varied parameters: value class; per-class with-dictionary versus
without-dictionary; candidate size thresholds swept across the observed range;
probe head-sample size swept across a small set of candidate lengths.

Measured, per class: value-size histogram reporting p50, p90, p99, and max; for
each candidate threshold the net bytes saved after framing overhead and the
compress CPU cost; with-dict versus no-dict ratio; the entropy-probe value and
its correlation with realized ratio; per-prefix homogeneity (ratio variance
within a prefix). No numbers are recorded here; this doc fixes the procedure.

Decision rule: for each class pick the smallest threshold where net bytes saved
per compress CPU millisecond stays positive; report whether that threshold lands
below 16384 bytes (the survey's purpose is to replace that floor, not assume the
result). Mark a class dictionary-eligible only if it is high-volume AND its
per-prefix ratio variance is low (structurally homogeneous); exclude high-entropy
or one-off classes. Export the measured per-class distribution as the reference
working set for the memory-model harness.

## What would change the decision

If every class showed positive net savings only at or above 16384 bytes, the
inherited floor would survive on evidence rather than inheritance. If no class
showed meaningful with-dictionary gains over no-dictionary, the per-prefix
dictionary recommendation feeding #55 would be dropped. If the entropy probe did
not correlate with realized ratio, the probe would be removed in favor of a
size-only gate. If per-prefix ratio variance were high everywhere, per-prefix
dictionaries would be rejected in favor of a single global dictionary or none.

## References

- Issue #57: cache value-size and compressibility distribution survey (this experiment)
- docs/research/compression-encoding.md: provisional rejection of the 16 KB floor and the size-plus-entropy gate shape
- Issue #52 / COMPRESSION.md: consumes the size threshold and incompressible-flag gate as provisional defaults
- Issue #55 / DICTIONARIES.md: consumes per-prefix dictionary eligibility as provisional defaults
- Issue #53 / ADR-0015: default codec decision (zstd-low) that fixes the codec for this survey
- [spymemcached-default-compression-threshold]: inherited 16384-byte client default being rejected
- [zstd-dictionary-small-data-6.9x]: dictionary lift on small similar values motivating a lower threshold