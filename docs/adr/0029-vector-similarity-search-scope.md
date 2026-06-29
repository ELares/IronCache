# ADR-0029: Off-path vector similarity search scope (decline a native type for now)

Status: Accepted
Issue: #415

## Context

Vector similarity search is the AI-era data surface clients increasingly expect:
Redis 8.x ships vector sets (VADD, VSIM) with an on-disk graph index, the Valkey
search module offers HNSW and KNN with hybrid filtering, Dragonfly serves vector
similarity through its FT family, and Garnet is previewing a DiskANN-backed
surface. The use cases are retrieval-augmented generation, recommendation, and
semantic caching of model responses.

The charter boundary here is sharp and worth restating, because it is favorable
rather than prohibitive. NON_GOALS entry 10 rejects per-request ML inference on
the hot path but explicitly notes that similarity SEARCH is not inference, and
entry 11 forbids the engine making external model calls. So storing
client-supplied vectors and answering off-path k-nearest-neighbor queries is
permitted by the charter; generating an embedding on the engine, or running a
forward pass per access, is not. The question this ADR settles is therefore not
"is vector search allowed" (it is) but "does v-next build a native vector-set
type into the single static binary now."

The tenet order is Compatible greater than Efficient greater than Simple greater
than Scalable greater than AI-Driven. A native vector type pulls on Compatible
and on AI-Driven, the lowest tenet, while pushing hard against Simple and
Efficient: a usable implementation needs an approximate-nearest-neighbor index
(HNSW or a DiskANN-style disk graph), quantization choices, a recall-versus-QPS
operating point, and the memory budget of a graph over every stored vector, all
inside a binary whose headline promise is one static artifact with a cheap,
branch-predictable data path.

## Decision

IronCache does **not** build a native vector-set type in the core binary now. The
stance has three parts:

- The charter permits it later. This ADR records that off-path similarity search
  over client-supplied vectors is inside the charter (NON_GOALS 10 allows search,
  11 bans on-engine embedding generation), so a future build is gated by cost and
  demand, not by a non-goal. Embeddings are always client-supplied; the engine
  never calls a model.
- It is declined for now on Simple and the tenet order. AI-Driven is the lowest
  tenet, and a graph ANN index is a large, ongoing complexity and memory
  commitment for the single-binary promise. The default until that tradeoff is
  justified by demonstrated demand is to not carry the index.
- If built, it is built off the hot path and behind an opt-in seam. A future
  design issue owns the index choice (HNSW versus a disk-backed graph), the
  quantization posture, the recall-and-QPS claim it must prove under the
  benchmark harness per the no-claim-without-its-test rule, and whether it ships
  in the core binary or as an optional, separately-compiled capability so the
  default artifact stays lean.

## Rejected Alternatives

- **Build a native HNSW vector-set type now (Redis vector-sets parity).** Add
  VADD, VSIM, and friends with an in-memory HNSW graph this milestone. Rejected on
  Simple and Efficient: the graph index, its quantization and recall tuning, and
  its per-vector memory overhead are a standing cost on the single-binary, cheap
  data-path promise, taken on for the lowest-ranked tenet without demonstrated
  cache demand. Per the tenet order, this loses to keeping the engine lean.
- **Declare vector search a permanent non-goal.** Rejected on honesty and on the
  charter itself: NON_GOALS 10 already carves out similarity search as permitted,
  so a permanent exclusion would contradict the standing charter boundary and
  overstate the decision. The evidence supports "not now," not "never."
- **Put embedding generation on the engine to match a turnkey story.** Rejected
  flatly on NON_GOALS 11 and the Efficient tenet: a forward pass per request, or
  any external model call from the engine, breaks the single-static-binary
  contract and the nanosecond-scale data path. Off-path search over
  client-supplied vectors is the only shape ever considered.

## Consequences

- The compatibility map records vector sets as permitted-by-charter but
  not-built, distinct from the hard non-goals (Lua, the memcached protocol),
  which can never be built. NON_GOALS is updated to cite this ADR for the
  off-path-search carve-out so the boundary is not mistaken for a blanket no.
- No engine work, memory budget, or benchmark gate is committed now; a future
  vector design issue owns all of it, including the recall-versus-QPS proof.
- Reopening is a new opt-in capability, not an engine-model change, so it can land
  later without superseding any decision here. The AI-Driven-versus-Simple
  tradeoff is now explicit rather than implicit, which is the point of recording
  it.
