#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Offline, deterministic ADR record check (docs/adr). It asserts:
#   1. every ADR record (NNNN-*.md, excluding the 0000 template) has the four
#      required sections and a valid Status: line;
#   2. every "[claim-id]" cited in an ADR exists in docs/prior-art/claims.yaml;
#   3. every Superseded-by:/Supersedes: ADR-NNNN link resolves to a record file;
#   4. every ADR record is listed in INDEX.md.
# It does NOT query GitHub; binding closed [DECISION] issues to ADRs is a
# separate, non-blocking check.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ADR="$ROOT/docs/adr"
CLAIMS="$ROOT/docs/prior-art/claims.yaml"
INDEX="$ADR/INDEX.md"
fail=0

ids_file="$(mktemp)"
grep -E '^[[:space:]]*-[[:space:]]+id:[[:space:]]*' "$CLAIMS" \
  | sed -E 's/^[[:space:]]*-[[:space:]]+id:[[:space:]]*//; s/[[:space:]]*$//; s/^"//; s/"$//' \
  | sort -u > "$ids_file"

records=0
for f in "$ADR"/[0-9][0-9][0-9][0-9]-*.md; do
  [ -e "$f" ] || continue
  base="$(basename "$f")"
  [ "$base" = "0000-template.md" ] && continue
  records=$((records+1))
  num="${base%%-*}"

  # 1. required sections (line-anchored headings, not substrings) + status
  for sec in "## Context" "## Decision" "## Rejected Alternatives" "## Consequences"; do
    grep -qE "^${sec}[[:space:]]*$" "$f" || { echo "ERROR: $base missing section heading '$sec'" >&2; fail=1; }
  done
  grep -qE '^Status: (Proposed|Accepted|Superseded)[[:space:]]*(#.*)?$' "$f" \
    || { echo "ERROR: $base missing a valid 'Status:' line" >&2; fail=1; }

  # 2. claim-id citations exist (bracketed lowercase-kebab tokens, not md links).
  # Note: a bracketed kebab token anywhere (including a code fence) is treated as
  # a citation, matching the sibling check-prior-art-claims.sh convention; ADRs
  # write illustrative ids without brackets (see the 0000 template).
  while IFS= read -r c; do
    [ -z "$c" ] && continue
    grep -qxF "$c" "$ids_file" || { echo "ERROR: $base cites [$c] not in claims.yaml" >&2; fail=1; }
  done < <(grep -oE '\[[a-z0-9][a-z0-9-]{2,}\]' "$f" | sed -E 's/^\[//; s/\]$//' | grep -- '-' | sort -u)

  # 3. supersession links resolve
  while IFS= read -r ref; do
    [ -z "$ref" ] && continue
    ls "$ADR/${ref}-"*.md >/dev/null 2>&1 || { echo "ERROR: $base references ADR-$ref with no record file" >&2; fail=1; }
  done < <(grep -oE '(Superseded-by|Supersedes): ADR-[0-9]{4}' "$f" | grep -oE '[0-9]{4}' | sort -u)

  # 4. listed in INDEX.md as a real link to the record file (no bare-number match)
  grep -qE "\(${num}-[a-z0-9-]+\.md\)" "$INDEX" \
    || { echo "ERROR: $base (ADR $num) is not listed in INDEX.md (expected a (${num}-title.md) link)" >&2; fail=1; }
done

rm -f "$ids_file"
echo "ADR records checked: $records"
if [ "$fail" -ne 0 ]; then echo "FAIL: ADR index check failed" >&2; exit 1; fi
echo "OK: ADR index check passed"
