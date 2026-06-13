#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Offline, deterministic check that the prior-art prose agrees with the pinned
# claims file. It does NOT re-fetch sources. It asserts:
#   1. every claim id in docs/prior-art/claims.yaml is unique
#   2. every "[id]" citation in the prose files exists in claims.yaml
#
# Upstream value drift is caught by accessed_date going stale, not by this script.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CLAIMS="$ROOT/docs/prior-art/claims.yaml"

# Prose files that cite claim ids. Add to this list as the docs grow.
PROSE_FILES=(
  "$ROOT/docs/PRIOR_ART.md"
  "$ROOT/docs/CHARTER.md"
  "$ROOT/docs/GLOSSARY.md"
  "$ROOT/docs/INVARIANTS.md"
  "$ROOT/docs/NON_GOALS.md"
)

fail=0

# --- 1. collect ids and check uniqueness ---
# Lines look like:  - id: some-kebab-id
ids_file="$(mktemp)"
grep -E '^[[:space:]]*-[[:space:]]+id:[[:space:]]*' "$CLAIMS" \
  | sed -E 's/^[[:space:]]*-[[:space:]]+id:[[:space:]]*//; s/[[:space:]]*$//; s/^"//; s/"$//' \
  | sort > "$ids_file"

dupes="$(uniq -d "$ids_file" || true)"
if [ -n "$dupes" ]; then
  echo "ERROR: duplicate claim ids in claims.yaml:" >&2
  echo "$dupes" >&2
  fail=1
fi
id_count="$(wc -l < "$ids_file" | tr -d ' ')"
echo "claims.yaml: $id_count unique ids"

# --- 2. check every [id] citation in prose exists ---
missing_total=0
for f in "${PROSE_FILES[@]}"; do
  [ -f "$f" ] || { echo "WARN: prose file not found: $f" >&2; continue; }
  # Citations are bracketed lowercase-kebab tokens, e.g. [redis-io-threads-default].
  # Markdown link texts contain slashes, dots, backticks, or capitals, so the
  # [a-z0-9][a-z0-9-]* class does not match them.
  cites="$(grep -oE '\[[a-z0-9][a-z0-9-]*\]' "$f" | sed -E 's/^\[//; s/\]$//' | sort -u || true)"
  n_missing=0
  while IFS= read -r c; do
    [ -z "$c" ] && continue
    if ! grep -qxF "$c" "$ids_file"; then
      echo "ERROR: $(basename "$f") cites [$c] which is not in claims.yaml" >&2
      n_missing=$((n_missing+1))
    fi
  done <<< "$cites"
  n_cites="$(printf '%s\n' "$cites" | grep -c . || true)"
  echo "$(basename "$f"): $n_cites distinct citations, $n_missing missing"
  missing_total=$((missing_total + n_missing))
done

if [ "$missing_total" -ne 0 ]; then
  echo "FAIL: $missing_total citation(s) reference unknown claim ids" >&2
  fail=1
fi

rm -f "$ids_file"
if [ "$fail" -ne 0 ]; then exit 1; fi
echo "OK: prior-art claims check passed"
