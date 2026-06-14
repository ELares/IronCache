# Design: SWIM + Lifeguard data-plane membership

Issue: #74. Decisions: ADR-0025 (cluster partition map this health joins against),
ADR-0012 (scale-out targets the protocol must hold flat past), ADR-0002
(shared-nothing; membership state is per-node, off the data hot path), ADR-0011
(slot-ready layout the live view is rendered over). Related: #73/CONTROL_PLANE.md
(the Raft authority SWIM proposes to), #70 (client cluster view), #68 (umbrella).

## Goal and scope

A data-plane membership and failure-detection layer whose per-node cost is flat
as the cluster grows and that does not flap a healthy node out of the ring during
a GC pause. It answers, for every other subsystem, which nodes are alive right
now so replication routes around the dead and the client view never points at a
corpse. Scope: the failure-detection protocol, its LAN/WAN defaults, the
`Membership` trait, and how SWIM health joins the Raft-committed map. Out of
scope: slot rebalancing policy and the authoritative epoch (#73), and the
partition layout (ADR-0025).

## Design

### SWIM, behind a Membership trait

- Adopt SWIM: randomized round-robin direct ping plus k indirect pings, with
  infection-style dissemination, so expected detection time, false-positive rate,
  and per-member message load are independent of group size [swim-scalability].
  This is the constant-cost transport that lets ADR-0012's several-thousand-node
  target hold flat past the ~1000-node Redis full-mesh ceiling
  [redis-cluster-max-nodes-recommendation].
- The protocol sits behind a `Membership` trait so it is swappable (a full-mesh
  backend stays implementable behind the same interface). Whether the trait is
  public in v1 or internal until a second backend exists is an open question
  below (Simple over Scalable until earned).

### Lifeguard, non-optional

- Lifeguard local-health awareness is a non-optional extension, not a tuning
  knob: a node that suspects itself is slow (missed self-probes) dilates its own
  timeouts so it does not falsely accuse peers, cutting failure-detector false
  positives by ~50x under CPU/network stress in the memberlist deployment
  [memberlist-lifeguard]. This is what keeps a GC pause from evicting a healthy
  node.

### LAN and WAN profiles

- Two default profiles split the single dominant timeout by network class,
  adapting the intent of Redis's one global `cluster-node-timeout` (15000 ms
  default [redis-cluster-node-timeout-default]) rather than its single value.
  LAN: probe ~200 ms, suspicion ~1 s, for fast failover within the ADR-0012
  budget. Cloud/WAN: probe ~1 s, suspicion ~5 s, anchored under the 15 s ceiling
  [redis-cluster-node-timeout-default] to tolerate jitter. Exact suspicion
  multipliers and indirect-probe fan-out are open below.

### Health joined with the Raft-committed map

- The client-facing view is not SWIM-direct. CLUSTER SLOTS/SHARDS (#70) are
  rendered from the Raft-committed slot->node map (#73) joined with live SWIM
  health, so a polling client reads a stable, committed answer rather than raw
  gossip. SWIM only annotates liveness over an ownership decided by Raft.

### The SWIM-proposes / Raft-commits handshake (with #73)

- SWIM is fast and unauthoritative; Raft is authoritative and slower. A SWIM
  suspicion or confirmation is a *hint* delivered to the Raft leader as a proposal
  input (#73); it never directly mutates the roster or a slot owner. The leader
  commits the membership or promotion change, bumps the config epoch, and only
  then does the change appear in the client projection. This is the same contract
  CONTROL_PLANE.md states from the consensus side: SWIM proposes, Raft commits.
- The joined view is therefore monotonic under suspicion: a node SWIM marks
  suspect is demoted in the CLUSTER SLOTS/SHARDS reply only after Raft confirms,
  so the polled answer is stable and never regresses on a transient flap. A
  debounce window before a suspicion is surfaced is an open question below.

## Open questions

- Exact LAN/WAN suspicion multipliers and indirect-probe fan-out.
- Whether `Membership` is a public trait in v1 or internal until a second backend
  exists (Simple over Scalable until earned).
- The debounce window before a SWIM suspicion is surfaced toward CLUSTER SLOTS.
- Whether WAN mode is auto-detected or operator-set.

## Acceptance and test hooks

- Measured per-node message and CPU overhead stays constant as N grows
  [swim-scalability]; a scaling run shows it flat past the ~1000-node ceiling
  [redis-cluster-max-nodes-recommendation].
- A GC-pause / process-pause injection does not flap a healthy node out of the
  ring, validating the Lifeguard self-awareness path [memberlist-lifeguard].
- The `Membership` trait isolates the protocol: a full-mesh backend compiles and
  runs behind it unchanged.
- LAN and WAN default profiles are documented and benchmarked against
  pause-induced false positives.
- CLUSTER SLOTS/SHARDS replies derive from Raft-committed state and never regress
  under a transient SWIM suspicion (joint hook with #73).

## References

- ADR-0025, ADR-0012, ADR-0002, ADR-0011; issues #74, #73, #70, #68.
- Claims: [swim-scalability], [memberlist-lifeguard],
  [redis-cluster-node-timeout-default], [redis-cluster-max-nodes-recommendation].
