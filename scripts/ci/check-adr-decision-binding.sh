#!/usr/bin/env sh
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# ONLINE, NON-BLOCKING reconciliation of the decision trail (issue #4 rule 6,
# split out as #166). The offline sibling scripts/ci/check-adr-index.sh is the
# hard gate on ADR records, citations, supersession, and INDEX listing; it
# cannot bind a *closed* [DECISION] issue to the existence of its ADR without
# the GitHub API, so that binding lives here.
#
# This script lists CLOSED issues labeled 'decision-needed' and reconciles them
# against the 'Issue:' back-link headers of the ADR records, in BOTH directions:
#
#   A. a closed decision-needed issue with NO ADR whose 'Issue:' header names it
#      (a decision was made and closed but never recorded);
#   B. an ADR 'Issue:' header that names an issue which does not exist, is still
#      OPEN, or is not labeled 'decision-needed' (the ADR's back-link is stale).
#
# It REPORTS mismatches and exits 0 regardless: this is an advisory governance
# signal for human follow-up, not a merge gate. The only failure exits are
# environment problems (no gh, no token, API unreachable), so a silent
# misconfiguration does not read as "all clear".
#
# Requirements: POSIX sh, gh (authenticated via GH_TOKEN/GITHUB_TOKEN), and the
# repo checked out. No network beyond gh against this repository's own API.
set -eu

LABEL="decision-needed"

# Repo root = two levels up from scripts/ci. Resolve ADR dir from there.
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
ADR_DIR="$ROOT/docs/adr"

# GH_REPO lets gh target the right repo in Actions; fall back to the origin
# remote inferred by gh when run locally.
REPO_ARG=""
if [ "${GH_REPO:-}" != "" ]; then
  REPO_ARG="--repo $GH_REPO"
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh (GitHub CLI) not found; this online check needs it" >&2
  exit 2
fi
if [ "${GH_TOKEN:-}" = "" ] && [ "${GITHUB_TOKEN:-}" = "" ]; then
  echo "ERROR: no GH_TOKEN/GITHUB_TOKEN in the environment for gh" >&2
  exit 2
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT INT TERM

closed="$WORK/closed_decisions"      # closed decision-needed issue numbers
adr_issues="$WORK/adr_issues"        # issue numbers named by ADR Issue: headers
adr_map="$WORK/adr_map"              # "<issue> <adr-file>" pairs for messages

# --- 1. closed decision-needed issues (paginated) -------------------------
# --jq emits one number per line; --limit caps the page set (well above the
# current count). gh paginates internally to satisfy --limit.
if ! gh issue list $REPO_ARG --state closed --label "$LABEL" --limit 1000 \
      --json number --jq '.[].number' >"$closed" 2>"$WORK/err1"; then
  echo "ERROR: gh failed listing closed '$LABEL' issues:" >&2
  cat "$WORK/err1" >&2
  exit 2
fi
sort -n -u "$closed" -o "$closed"

# --- 2. issue numbers named by ADR 'Issue:' headers -----------------------
# Header forms seen in the tree: "Issue: #41" and "Issue: #82, #119". Skip the
# template (#N is not a number). Record both a flat number list and the
# number->file map for human-readable messages.
: >"$adr_issues"
: >"$adr_map"
for f in "$ADR_DIR"/[0-9][0-9][0-9][0-9]-*.md; do
  [ -e "$f" ] || continue
  base=$(basename "$f")
  [ "$base" = "0000-template.md" ] && continue
  # First 'Issue:' line of the record; pull every #NNN token out of it.
  hdr=$(grep -m1 -E '^Issue:' "$f" 2>/dev/null || true)
  [ -n "$hdr" ] || continue
  nums=$(printf '%s\n' "$hdr" | grep -oE '#[0-9]+' | tr -d '#')
  for n in $nums; do
    printf '%s\n' "$n" >>"$adr_issues"
    printf '%s %s\n' "$n" "$base" >>"$adr_map"
  done
done
sort -n -u "$adr_issues" -o "$adr_issues"

# --- 3. direction A: closed decision with no ADR -------------------------
# In $closed but not in $adr_issues.
a_miss="$WORK/a_miss"
comm -23 "$closed" "$adr_issues" >"$a_miss"

# --- 4. direction B: ADR header names a bad issue ------------------------
# For each issue named by an ADR, confirm it exists, is CLOSED, and carries the
# decision-needed label. Anything else is a stale ADR back-link.
b_miss="$WORK/b_miss"
: >"$b_miss"
while IFS= read -r n; do
  [ -n "$n" ] || continue
  # One API call per ADR-referenced issue; the set is small (one per ADR).
  meta=$(gh issue view "$n" $REPO_ARG --json state,labels \
           --jq '.state + "|" + ([.labels[].name] | join(","))' 2>/dev/null || true)
  files=$(awk -v k="$n" '$1==k{printf "%s ", $2}' "$adr_map")
  if [ -z "$meta" ]; then
    printf '%s|MISSING|%s\n' "$n" "$files" >>"$b_miss"
    continue
  fi
  state=${meta%%|*}
  labels=${meta#*|}
  if [ "$state" != "CLOSED" ] && [ "$state" != "closed" ]; then
    printf '%s|OPEN|%s\n' "$n" "$files" >>"$b_miss"
  elif ! printf '%s' "$labels" | tr ',' '\n' | grep -qx "$LABEL"; then
    printf '%s|UNLABELED|%s\n' "$n" "$files" >>"$b_miss"
  fi
done <"$adr_issues"

# --- 5. report (Step Summary if present, else stdout) ---------------------
out() {
  printf '%s\n' "$1"
  if [ "${GITHUB_STEP_SUMMARY:-}" != "" ]; then
    printf '%s\n' "$1" >>"$GITHUB_STEP_SUMMARY"
  fi
}

# Count non-empty lines. wc -l is avoided for empty-file edge cases under set -e.
n_closed=$(grep -c . "$closed" 2>/dev/null || true); n_closed=${n_closed:-0}
n_a=$(grep -c . "$a_miss" 2>/dev/null || true); n_a=${n_a:-0}
n_b=$(grep -c . "$b_miss" 2>/dev/null || true); n_b=${n_b:-0}

out "## ADR <-> decision binding (advisory, non-blocking)"
out ""
out "Closed \`$LABEL\` issues scanned: $n_closed"
out ""

if [ "$n_a" -eq 0 ]; then
  out "### A. Closed decisions with no ADR: none"
else
  out "### A. Closed \`$LABEL\` issues with no ADR \`Issue:\` header naming them ($n_a)"
  while IFS= read -r n; do
    [ -n "$n" ] && out "- #$n closed but no ADR records it"
  done <"$a_miss"
fi
out ""

if [ "$n_b" -eq 0 ]; then
  out "### B. ADR Issue: headers that are stale: none"
else
  out "### B. ADR \`Issue:\` headers pointing at a missing/open/unlabeled issue ($n_b)"
  while IFS='|' read -r n why files; do
    [ -n "$n" ] || continue
    case "$why" in
      MISSING)   out "- #$n does not exist (referenced by: ${files})" ;;
      OPEN)      out "- #$n is still OPEN (referenced by: ${files})" ;;
      UNLABELED) out "- #$n is not labeled \`$LABEL\` (referenced by: ${files})" ;;
      *)         out "- #$n ($why) (referenced by: ${files})" ;;
    esac
  done <"$b_miss"
fi
out ""

total=$((n_a + n_b))
if [ "$total" -eq 0 ]; then
  out "OK: every closed decision has an ADR and every ADR back-link is valid."
else
  out "NOTE: $total mismatch(es) above are advisory. The offline gate (check-adr-index.sh) still passed if it did; resolve these by adding the missing ADR or fixing the ADR \`Issue:\` header. This job does not fail the build."
fi

# Advisory: always succeed. Environment failures exited 2 earlier.
exit 0
