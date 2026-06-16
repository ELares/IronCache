<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache competitor matrix (dated 2026-06-16)

This is the living competitor matrix required by BENCHMARK.md ("a committed, dated
competitor table tracks each baseline's version and defaults ... Versions are bumped
only by explicit PR, never floating tags"). The versions and memory-overhead facts
below are PINNED: a number changes only through an explicit pull request that edits
this file. Nothing here is a floating "latest" tag.

These are the rows the A1 allocator-true memory model (`memmodel`, reporting
`object_bytes_per_key` per encoding) is compared against, and the rows A4's
head-to-head (#96) pins against. A4 installs the pinned `valkey-server` version named
here and measures IronCache and Valkey side by side on identical hardware.

## Baselines (verified 2026-06-16)

| Baseline | Pinned version | Released | License | Memory-overhead fact (what A1 compares against) |
| --- | --- | --- | --- | --- |
| **Valkey** (primary oracle + head-to-head bar) | **9.1.0** (latest 8.1.x line: 8.1.8) | 2026-05-19 | BSD-3-Clause (permissive; no SSPL/RSAL artifact) | **Embedded key/value** (landed in Valkey 8.0): the key SDS, and for short strings the value too, are embedded inline right after the `robj` header in a SINGLE allocation, with an `embeddedDictEntry` removing the separate key pointer. The combined `robj` embeds when it fits in ~128 bytes (PR #1726); the project quotes "save up to 15 bytes per entry" and 8-45% more pairs packed. A SEPARATE, later win in Valkey 8.1 (a redesigned hashtable) cuts a further ~20 bytes/key (no TTL) / ~30 bytes/key (with TTL). |
| **Redis** (the redesigned object header) | **8.8.0** (the kvobj line; latest 8.2.x patch: 8.2.7) | 2026-05-25 | Tri-licensed: AGPLv3 (OSI, added 2025-05-01) OR RSALv2 OR SSPLv1 | **kvobj** (the unified key-value object, shipped in Redis 8.2, NOT 8.0): tightly packs the key name, short values, and optional TTL into a SINGLE allocation, with one pointer shared by the values and TTL hashtables. Quoted 25-37% memory reduction for short strings; for single-key hash slots Redis can avoid a separate entry entirely. |
| **Dragonfly** (dashtable per-entry overhead) | **v1.39.0** | 2026-06-09 | BSL 1.1 (BUSL-1.1; converts to Apache-2.0 on 2030-07-01; not OSI) | **Dashtable**: ~6-16 bytes of overhead per entry (total ~22-32 bytes/item; theoretical ~19N bytes at 100% utilization). The directory cost is negligible (~8N/840 bytes). For reference Redis's classic `dict` runs ~16-24 bytes/item typical (up to ~40 worst case). |

## Allocator defaults that move the numbers

Per BENCHMARK.md ("allocator defaults that move the numbers (jemalloc
decay/background-thread settings)"). Redis and Valkey both ship jemalloc; these are the
defaults that affect resident memory and therefore any bytes-per-key comparison:

| Knob | jemalloc bundled default | Notes |
| --- | --- | --- |
| `background_thread` | `false` (off) at the bundled-library level | Redis/Valkey ENABLE the async purge thread at runtime via `CONFIG SET jemalloc-bg-thread yes` (or `MALLOC_CONF=background_thread:true`). IronCache sets `background_thread:true` at boot via `malloc_conf` (ADR-0006). |
| `dirty_decay_ms` | `10000` (10s) | IronCache lowers this to `5000` (sub-10s) at boot per ADR-0006 so dirty pages return to the OS faster under eviction churn. A like-for-like comparison should note the decay difference. |
| `muzzy_decay_ms` | `0` | Upstream jemalloc bundled value; no Valkey-specific override was found. |

Note: the decay numbers above are the UPSTREAM jemalloc bundled defaults, not a
Valkey-specific tune; no source was found showing Valkey overrides the decay values.

## How to refresh

1. WEB-VERIFY the current real release of each baseline (Valkey, Redis, Dragonfly) and
   the relevant allocator defaults.
2. Bump the pinned versions / dates / facts in the table above in a dedicated PR. Re-date
   the heading. Cite the source for each changed number in the PR description.
3. A4 (#96) then installs the newly pinned `valkey-server` version for the head-to-head;
   the bar moves only when this matrix moves.

## Sources (verified 2026-06-16)

- Valkey releases: https://valkey.io/download/releases/ ; https://endoflife.date/valkey
- Valkey license / fork-of-7.2.4: https://logz.io/blog/redis-no-longer-open-source-is-valkey-successor/
- Valkey RESP protocol parity: https://valkey.io/topics/protocol/
- Valkey embedded key (8.0): https://github.com/valkey-io/valkey/issues/394 ; https://github.com/valkey-io/valkey/pull/541 ; https://github.com/valkey-io/valkey/pull/1726
- Valkey 8.1 hashtable redesign (the separate ~20-30 byte/key win): https://valkey.io/blog/valkey-8-1-0-ga/
- Redis releases: https://github.com/redis/redis/releases ; https://endoflife.date/redis
- Redis 8 licensing (AGPLv3 added): https://redis.io/blog/redis-8-ga/ ; https://www.phoronix.com/news/Redis-8.0-Goes-AGPLv3
- Redis kvobj (8.2): https://redis.io/blog/redis-82-ga/
- Dragonfly releases: https://github.com/dragonflydb/dragonfly/releases
- Dragonfly license (BSL 1.1): https://github.com/dragonflydb/dragonfly/blob/main/LICENSE.md ; https://www.dragonflydb.io/docs/about/license
- Dragonfly dashtable overhead: https://github.com/dragonflydb/dragonfly/blob/main/docs/dashtable.md
- jemalloc decay / background-thread defaults: https://fossies.org/linux/redis/deps/jemalloc/TUNING.md
