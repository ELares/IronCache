# Design: RESP request-size and adversarial-input hardening

Issue: #138 (merged from the failure-ops and security-threat audit lenses).
Related: #15 (parser), #137 (connection admission), #18 (errors), #95 (fuzzing).

## Goal and scope

The parser is the first code an unauthenticated client reaches, so its limits are
a security surface, not fixed compatibility constants. This specifies the
configurable bounds that keep a crafted or dribbled request from exhausting
memory or CPU, each with a clean protocol-error-or-disconnect contract and a
metric. It complements connection-level admission (#137): this document bounds a
single frame, #137 bounds aggregate connections and output buffers.

## Design

Each limit is a config knob (#85) with a safe default and a defined failure mode:

- **`proto-max-bulk-len`** (default 512 MB [bulk-string-max-512mb]): max single
  bulk-string length. Exceeding it is an `ERR` protocol error and the connection
  is closed.
- **Multibulk element cap:** max elements in one request array. Bounds the
  `*<huge>` announcement vector. Exceeding closes the connection.
- **Accumulated incomplete-frame bound:** the total bytes buffered for an
  in-progress frame is capped, defeating the slow-loris / memory-amplification
  vector where a client announces a large multibulk and dribbles bytes. The
  buffer cannot grow past the bound waiting for completion.
- **RESP3 aggregate nesting depth cap:** bounds recursion/stack on nested
  aggregates so a deeply nested frame cannot blow the parser.
- **Inline-command length cap:** bounds a bare inline line.
- **Parser-work budget:** a per-frame work bound so a syntactically valid but
  pathological frame cannot burn unbounded CPU on one core (which, under
  shared-nothing, would stall that core's whole shard).

Every rejection increments a `rejected_oversize_frame` (and per-reason) metric
(#152) and emits a catalog protocol error (#18) before disconnecting where the
protocol still permits a reply, or disconnects immediately where it does not.

## Open questions

- Default numeric values for the multibulk cap, accumulated-frame bound, nesting
  depth, inline cap, and work budget (set against measured client behavior so no
  legitimate client trips them; tuned in #85).
- Whether the parser-work budget is measured in bytes scanned, elements, or a
  cycle proxy.

## Acceptance and test hooks

- cargo-fuzz targets on the RESP2/RESP3 parser cover multibulk-count overflow,
  bulk-length overflow, dribbled incomplete frames, and deep nesting (#95).
- A test asserts each limit triggers its protocol-error/disconnect contract and
  bumps the metric, and that no legitimate mainstream-client request trips a
  default limit.
- The accumulated-frame bound is verified to hold buffer memory flat under a
  slow-loris simulation.

## References

- Issues #15, #137, #18, #152, #85, #95.
- Claims: [bulk-string-max-512mb].
