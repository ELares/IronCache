#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# IronCache per-PR perf-regression COMPARE step (PERF_REGRESSION_GATE.md #159, PR-A5 of
# the performance track). It ratchets a HEAD measurement against a BASE (merge-base)
# measurement - BOTH produced by perf_measure.sh on the SAME runner in the SAME job -
# and emits a Markdown verdict table. It is the half of the gate that DECIDES.
#
# The two headline metrics and their ratchet directions:
#
#   - bytes_per_key (per encoding class int/embstr/raw): DETERMINISTIC, so a TIGHT
#     budget. Direction: may NOT RISE past budget. A class that grows beyond
#     BYTES_RISE_BUDGET FAILs the PR.
#   - qps_median: NOISY on shared CI, so we compute a NOISE BAND from the base reps
#     spread and apply a GENEROUS budget. Direction: may NOT FALL past budget. qps that
#     drops beyond QPS_DROP_BUDGET FAILs the PR.
#
# Per-metric verdict (PERF_REGRESSION_GATE.md ratchet semantics):
#   PASS  delta is inside the noise band (within-noise; not a real move).
#   WARN  delta is outside the band but inside the budget (a real move, but tolerated;
#         does NOT fail the PR).
#   FAIL  delta is outside the budget in the BAD direction (qps fell > budget, or a
#         bytes_per_key class rose > budget).
#
# The script EXITS NON-ZERO iff any metric FAILed; WARN/PASS exit 0. CI never auto-commits
# anything: an intentional perf trade is landed by raising the relevant budget IN THE PR
# with a documented reason (see scripts/bench/README.md, Perf-regression gate (A5)).
#
# Open-loop tails / criterion micro-benches are REPORTED-not-failed elsewhere and are NOT
# part of this ratchet (the doc: tail noise on shared CI is high). Only bytes_per_key +
# qps gate.
#
# Usage:
#   scripts/bench/perf_compare.sh --base BASE.json --head HEAD.json [--report FILE]
#
# Budgets (env-overridable fractions):
#   QPS_DROP_BUDGET   max tolerated qps DROP, fraction (default 0.15 = 15%). GENEROUS.
#   BYTES_RISE_BUDGET max tolerated bytes_per_key RISE, fraction (default 0.05 = 5%). TIGHT.
#   QPS_BAND_FLOOR    minimum qps noise band as a fraction (default 0.05 = 5%). The band
#                     is max(base reps spread, this floor) so a single-rep base (spread 0)
#                     still gets a sane within-noise tolerance.
#
# All float math is awk (POSIX; no bc/python dependency).

set -euo pipefail

# ---------------------------------------------------------------------------
# Budgets / band floor (env-overridable).
# ---------------------------------------------------------------------------
QPS_DROP_BUDGET="${QPS_DROP_BUDGET:-0.15}"     # 15% qps drop tolerated (generous; noisy).
# TRANSITIONAL 7% (normally 5%): the #285 stage-4 default-index flip trades a one-time
# +5.4% on the memmodel INT class (the reserved-fill split artifact of the dash reserve,
# documented in DashIndex::reserve + the flip PR) for the uniform ORGANIC memory win the
# flip record proves (never worse than hashbrown; 3.5-4.8% of total bytes better at its
# doubling-trough keycounts -- the memmodel reserved fill is the one scenario that cannot
# show the trough). RESTORE 0.05 in the follow-up PR once the merge-base carries the dash
# baseline (the ratchet then re-tightens around the new numbers automatically).
BYTES_RISE_BUDGET="${BYTES_RISE_BUDGET:-0.07}"
QPS_BAND_FLOOR="${QPS_BAND_FLOOR:-0.05}"       # 5% minimum qps noise band.

# ---------------------------------------------------------------------------
# Flag parsing: --base FILE (required), --head FILE (required), --report FILE (optional).
# ---------------------------------------------------------------------------
BASE_FILE=""
HEAD_FILE=""
REPORT_FILE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)
      [[ $# -ge 2 ]] || { echo "error: --base needs an argument" >&2; exit 2; }
      BASE_FILE="$2"; shift 2 ;;
    --base=*)
      BASE_FILE="${1#*=}"; shift ;;
    --head)
      [[ $# -ge 2 ]] || { echo "error: --head needs an argument" >&2; exit 2; }
      HEAD_FILE="$2"; shift 2 ;;
    --head=*)
      HEAD_FILE="${1#*=}"; shift ;;
    --report)
      [[ $# -ge 2 ]] || { echo "error: --report needs an argument" >&2; exit 2; }
      REPORT_FILE="$2"; shift 2 ;;
    --report=*)
      REPORT_FILE="${1#*=}"; shift ;;
    -h|--help)
      echo "Usage: $0 --base BASE.json --head HEAD.json [--report FILE]" >&2
      echo "Ratchets HEAD vs BASE (both from perf_measure.sh). Exits non-zero iff any metric FAILs." >&2
      exit 0 ;;
    *)
      echo "error: unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "${BASE_FILE}" ]] || { echo "error: --base FILE is required" >&2; exit 2; }
[[ -n "${HEAD_FILE}" ]] || { echo "error: --head FILE is required" >&2; exit 2; }
[[ -f "${BASE_FILE}" ]] || { echo "error: base file not found: ${BASE_FILE}" >&2; exit 2; }
[[ -f "${HEAD_FILE}" ]] || { echo "error: head file not found: ${HEAD_FILE}" >&2; exit 2; }

# ---------------------------------------------------------------------------
# JSON readers. jq when present (clean), else an awk fallback over the flat JSON the
# measure step emits. get_scalar FILE KEY pulls a top-level numeric; get_nested FILE
# PARENT CHILD pulls bytes_per_key.<class>.
# ---------------------------------------------------------------------------
get_scalar() {
  local file="$1" key="$2"
  if command -v jq >/dev/null 2>&1; then
    # `// empty` so a MISSING key yields empty (not the literal string "null"),
    # which lets the caller's `|| ="0"` default fire. Without it, "null" is a
    # non-empty string that slips past the default and then poisons the awk
    # numeric guards below (a missing field would otherwise silently PASS the gate).
    jq -r --arg k "${key}" '.[$k] // empty' "${file}" 2>/dev/null
  else
    # First occurrence of "key": <number> at any depth (the measure JSON has these keys
    # only at top level, so a first-match is unambiguous).
    sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\\([0-9.]*\\).*/\\1/p" "${file}" | head -n1
  fi
}
get_nested() {
  local file="$1" parent="$2" child="$3"
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg p "${parent}" --arg c "${child}" '.[$p][$c] // empty' "${file}" 2>/dev/null
  else
    # Narrow to the parent object's region, then pull the child. The measure JSON nests
    # bytes_per_key as the last object; isolate from "bytes_per_key" to the end, then
    # match the child key.
    awk -v p="${parent}" -v c="${child}" '
      BEGIN { inblock = 0 }
      $0 ~ ("\"" p "\"[[:space:]]*:[[:space:]]*{") { inblock = 1 }
      inblock && match($0, ("\"" c "\"[[:space:]]*:[[:space:]]*[0-9.]+")) {
        s = substr($0, RSTART, RLENGTH)
        sub(/.*:[[:space:]]*/, "", s)
        print s
        exit
      }
    ' "${file}"
  fi
}

# Read the headline numbers.
BASE_QPS="$(get_scalar "${BASE_FILE}" qps_median)";  [[ -n "${BASE_QPS}" ]] || BASE_QPS="0"
HEAD_QPS="$(get_scalar "${HEAD_FILE}" qps_median)";  [[ -n "${HEAD_QPS}" ]] || HEAD_QPS="0"
BASE_QMIN="$(get_scalar "${BASE_FILE}" qps_min)";    [[ -n "${BASE_QMIN}" ]] || BASE_QMIN="0"
BASE_QMAX="$(get_scalar "${BASE_FILE}" qps_max)";    [[ -n "${BASE_QMAX}" ]] || BASE_QMAX="0"

# ---------------------------------------------------------------------------
# awk verdict helpers. Each prints space-separated fields the shell reads back.
# ---------------------------------------------------------------------------

# qps_verdict: a DROP is the bad direction. delta% = (head-base)/base*100 (negative is a
# drop). band% = max((qmax-qmin)/median, floor)*100. Verdict:
#   PASS  the drop is within the band (|drop| <= band) OR qps rose.
#   WARN  the drop exceeds the band but is within budget.
#   FAIL  the drop exceeds the budget.
# Prints: delta_pct band_pct verdict
qps_verdict() {
  awk -v base="${BASE_QPS}" -v head="${HEAD_QPS}" -v qmin="${BASE_QMIN}" -v qmax="${BASE_QMAX}" \
      -v budget="${QPS_DROP_BUDGET}" -v floor="${QPS_BAND_FLOOR}" '
  BEGIN {
    # Force numeric context (base + 0): a missing/non-numeric base reads as 0 here
    # instead of slipping past as a string and dividing by zero below. No usable
    # baseline -> WARN (visible, does not fail a PR over an infra/schema gap), never
    # a silent PASS.
    if (base + 0 <= 0) { printf "0.00 0.00 WARN\n"; exit }
    delta = (head - base) / base            # signed fraction; negative is a drop.
    spread = (base > 0 ? (qmax - qmin) / base : 0)
    band = (spread > floor ? spread : floor) # noise band fraction.
    drop = -delta                            # positive when qps fell.
    verdict = "PASS"
    if (drop > band)    verdict = "WARN"
    if (drop > budget)  verdict = "FAIL"
    printf "%.2f %.2f %s\n", delta * 100, band * 100, verdict
  }'
}

# bytes_verdict CLASS: a RISE is the bad direction; deterministic, so no per-metric band
# (band shown as the budget). delta% = (head-base)/base*100 (positive is a rise).
#   PASS  no rise (delta <= 0) within rounding, treated as within a tiny band.
#   WARN  a rise that is within budget (deterministic builds should not move; flag it).
#   FAIL  a rise beyond budget.
# We give bytes a small within-noise band (the same QPS_BAND_FLOOR is too big for a
# deterministic metric, so use a fixed tiny 0.5% to absorb memmodel float rounding).
# Prints: base head delta_pct verdict
bytes_verdict() {
  local cls="$1"
  local b h
  b="$(get_nested "${BASE_FILE}" bytes_per_key "${cls}")"; [[ -n "${b}" ]] || b="0"
  h="$(get_nested "${HEAD_FILE}" bytes_per_key "${cls}")"; [[ -n "${h}" ]] || h="0"
  awk -v base="${b}" -v head="${h}" -v budget="${BYTES_RISE_BUDGET}" '
  BEGIN {
    tiny = 0.005   # 0.5% within-noise band for the deterministic memmodel float.
    # Numeric context (base + 0): a missing/non-numeric base reads as 0 here, not a
    # string that divides by zero below. No usable baseline -> WARN, never silent PASS.
    if (base + 0 <= 0) { printf "%s %s 0.00 WARN\n", base, head; exit }
    delta = (head - base) / base          # signed; positive is a rise.
    verdict = "PASS"
    if (delta > tiny)    verdict = "WARN"
    if (delta > budget)  verdict = "FAIL"
    printf "%s %s %.2f %s\n", base, head, delta * 100, verdict
  }'
}

# ---------------------------------------------------------------------------
# Compute verdicts.
# ---------------------------------------------------------------------------
read -r QPS_DELTA QPS_BAND QPS_RESULT <<EOF
$(qps_verdict)
EOF

read -r BINT_BASE BINT_HEAD BINT_DELTA BINT_RESULT <<EOF
$(bytes_verdict int)
EOF
read -r BEMB_BASE BEMB_HEAD BEMB_DELTA BEMB_RESULT <<EOF
$(bytes_verdict embstr)
EOF
read -r BRAW_BASE BRAW_HEAD BRAW_DELTA BRAW_RESULT <<EOF
$(bytes_verdict raw)
EOF

# Defense in depth: if any verdict came back EMPTY (an awk crash, e.g. a future
# malformed input), treat it as a hard error rather than letting an absent verdict
# silently count as not-FAIL. A perf gate must never go green on a measurement it
# could not actually evaluate.
for v in "${QPS_RESULT}" "${BINT_RESULT}" "${BEMB_RESULT}" "${BRAW_RESULT}"; do
  if [[ -z "${v}" ]]; then
    echo "error: a perf-gate verdict was empty (compare could not evaluate a metric); failing closed." >&2
    exit 3
  fi
done

# Overall: FAIL iff any metric FAILed.
OVERALL="PASS"
for v in "${QPS_RESULT}" "${BINT_RESULT}" "${BEMB_RESULT}" "${BRAW_RESULT}"; do
  if [[ "${v}" == "FAIL" ]]; then OVERALL="FAIL"; fi
done
if [[ "${OVERALL}" != "FAIL" ]]; then
  for v in "${QPS_RESULT}" "${BINT_RESULT}" "${BEMB_RESULT}" "${BRAW_RESULT}"; do
    if [[ "${v}" == "WARN" ]]; then OVERALL="WARN"; fi
  done
fi

# Budget strings for the table.
QPS_BUDGET_PCT="$(awk -v b="${QPS_DROP_BUDGET}" 'BEGIN { printf "%.0f", b * 100 }')"
BYTES_BUDGET_PCT="$(awk -v b="${BYTES_RISE_BUDGET}" 'BEGIN { printf "%.0f", b * 100 }')"

# ---------------------------------------------------------------------------
# Emit the Markdown report. The HTML marker comment lets CI find-and-update a single
# sticky PR comment instead of stacking a new one each push.
# ---------------------------------------------------------------------------
emit_report() {
  cat <<EOF
<!-- ironcache-perf-gate -->
## perf-gate (A5)

Same-runner ratchet of HEAD against the merge-base (both rebuilt and measured in this job).
\`PASS\` = within the noise band, \`WARN\` = a real move inside budget (does not fail), \`FAIL\` = past budget in the bad direction.

| metric | base | head | delta% | band | budget | verdict |
| --- | ---: | ---: | ---: | ---: | ---: | :---: |
| qps_median (peak) | ${BASE_QPS} | ${HEAD_QPS} | ${QPS_DELTA}% | +/-${QPS_BAND}% | drop <= ${QPS_BUDGET_PCT}% | ${QPS_RESULT} |
| bytes_per_key int | ${BINT_BASE} | ${BINT_HEAD} | ${BINT_DELTA}% | det | rise <= ${BYTES_BUDGET_PCT}% | ${BINT_RESULT} |
| bytes_per_key embstr | ${BEMB_BASE} | ${BEMB_HEAD} | ${BEMB_DELTA}% | det | rise <= ${BYTES_BUDGET_PCT}% | ${BEMB_RESULT} |
| bytes_per_key raw | ${BRAW_BASE} | ${BRAW_HEAD} | ${BRAW_DELTA}% | det | rise <= ${BYTES_BUDGET_PCT}% | ${BRAW_RESULT} |

**Overall: ${OVERALL}**

- qps: noisy on shared CI, so the band comes from the base reps spread (floored at $(awk -v f="${QPS_BAND_FLOOR}" 'BEGIN{printf "%.0f", f*100}')%); a drop is only a regression past the ${QPS_BUDGET_PCT}% budget.
- bytes_per_key: deterministic (allocator-true memmodel), so a tight ${BYTES_BUDGET_PCT}% rise budget; any rise beyond it FAILs.
- Open-loop tails / criterion micro-benches are reported-not-failed (tail noise is high) and are not part of this ratchet.
- An intentional perf trade is landed by raising the relevant budget in this PR with a documented reason (CI never auto-commits a baseline).
EOF
}

REPORT="$(emit_report)"
printf '%s\n' "${REPORT}"
if [[ -n "${REPORT_FILE}" ]]; then
  printf '%s\n' "${REPORT}" >"${REPORT_FILE}"
fi

# ---------------------------------------------------------------------------
# Exit code: non-zero iff any metric FAILed. WARN/PASS exit 0.
# ---------------------------------------------------------------------------
if [[ "${OVERALL}" == "FAIL" ]]; then
  echo "perf-gate: FAIL (a headline metric regressed past budget)" >&2
  exit 1
fi
echo "perf-gate: ${OVERALL}" >&2
exit 0
