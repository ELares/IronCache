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
- **Total query-buffer cap (`query-buffer-limit`, default 1 GiB, #528):** the
  total bytes buffered in a connection's inbound read buffer are capped across
  recvs, defeating the slow-loris / memory-amplification vector where a client
  announces a large multibulk (`*<huge>\r\n`) and then DRIBBLES the elements: the
  frame never completes, so the read buffer would otherwise grow without bound
  PRE-AUTH. This is enforced at the CONNECTION level (the accumulated read buffer,
  not a single in-progress frame): after each recv grows the buffer the serve loop
  compares its length against the live cap and CLOSES the connection when it is
  exceeded. `0` disables it (Redis `client-query-buffer-limit` parity). Runtime-
  settable via `CONFIG SET query-buffer-limit`.
  NOTE: this is the bound that is actually implemented. Earlier revisions of this
  document described a per-frame "accumulated incomplete-frame bound" that did not
  exist in the code; the real protection is this per-connection query-buffer cap
  (its inbound analog is the `output-buffer-limit` output cap below).
- **Output-buffer cap (`output-buffer-limit`, default 1 GiB, PROD-SAFETY #5 /
  #529):** the total pending reply bytes for a connection are capped, both at the
  end of a pipelined batch (pre-flush) AND intra-batch after each command's reply
  is appended (#529, so a single pipelined batch of large-reply commands cannot
  accumulate unbounded output and OOM the host before the post-batch check runs).
  Exceeding it CLOSES the connection with the oversized buffer dropped unsent. `0`
  disables it. Runtime-settable via `CONFIG SET output-buffer-limit`.
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

- Default numeric values for the multibulk cap, nesting depth, inline cap, and
  work budget (set against measured client behavior so no legitimate client trips
  them; tuned in #85). The query-buffer and output-buffer caps (#528/#529) default
  to a high 1 GiB ceiling so a legitimate large request / reply / deep pipeline is
  unaffected while a pathological accumulation is bounded.
- Whether the parser-work budget is measured in bytes scanned, elements, or a
  cycle proxy.

## Acceptance and test hooks

- cargo-fuzz targets on the RESP2/RESP3 parser cover multibulk-count overflow,
  bulk-length overflow, dribbled incomplete frames, and deep nesting (#95).
- A test asserts each limit triggers its protocol-error/disconnect contract and
  bumps the metric, and that no legitimate mainstream-client request trips a
  default limit.
- The query-buffer cap (#528) is verified over a real socket to CLOSE a connection
  that dribbles a never-completing multibulk once its inbound buffer crosses the
  cap, and the output-buffer cap (#529) is verified to cut a single pipelined batch
  of large-reply commands off mid-batch (`crates/ironcache/tests/conn_limits.rs`).

## References

- Issues #15, #137, #18, #152, #85, #95.
- Claims: [bulk-string-max-512mb].
