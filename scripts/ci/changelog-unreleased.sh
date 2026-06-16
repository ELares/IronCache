#!/bin/sh
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Release precondition: the CHANGELOG section for this release must not be empty.
#
# A tagged release that ships no changelog entries erodes the audit trail, so the
# formal release workflow (release.yml) runs this BEFORE it builds. It picks the
# section to check and FAILS if that section has no content beyond the
# Keep-a-Changelog sub-headings (`### Added` / `### Changed` / `### Fixed` /
# `### Security` / `### Removed` / `### Deprecated`) and blank lines. A real
# bullet (a `-` line) or any other prose makes it pass.
#
# Which section: the standard Keep-a-Changelog flow keeps entries under
# `## [Unreleased]` until the release PR moves them under a `## [X.Y.Z]` heading.
# So if a tag/version is given AND a matching `## [X.Y.Z]` heading exists, that
# versioned section is checked; otherwise `## [Unreleased]` is checked. This
# makes the gate correct whether the entries are still under Unreleased at tag
# time or already moved under the version heading.
#
# Usage:
#   scripts/ci/changelog-unreleased.sh [path-to-CHANGELOG.md] [version-or-tag]
#
# `version-or-tag` may be `vX.Y.Z`, `X.Y.Z`, or empty. Deterministic and
# history-free: it reads only the file. Needs only POSIX `awk`/`grep` (mawk-safe:
# matches headings by literal string, not an awk regex var, so there is no
# backslash-escaping pitfall).
set -eu

changelog="${1:-CHANGELOG.md}"
version="${2:-}"

if [ ! -f "$changelog" ]; then
	echo "::error::CHANGELOG check: $changelog not found" >&2
	exit 2
fi

# Normalize a tag to a bare version: strip a single leading `v` so `v1.2.3` and
# `1.2.3` both match a `## [1.2.3]` (or `## [v1.2.3]`) heading below.
bare_version=""
if [ -n "$version" ]; then
	bare_version="${version#v}"
fi

# extract_section <exact-heading-line> prints the lines between that exact `## `
# heading line and the next `## ` heading. The heading is compared as a LITERAL
# string (awk `==`), so no regex metacharacters in a version (the dots in
# `1.2.3`) or the brackets need escaping.
extract_section() {
	awk -v target="$1" '
    $0 == target { if (in_section) exit; in_section = 1; next }
    /^## /       { if (in_section) exit }
    in_section   { print }
  ' "$changelog"
}

# heading_present <exact-heading-line> is true if the file contains that exact
# heading line.
heading_present() {
	awk -v target="$1" '$0 == target { found = 1 } END { exit(found ? 0 : 1) }' "$changelog"
}

section=""
which_section=""

# Prefer the versioned heading when a version is given and that heading (with or
# without a leading `v`) exists.
if [ -n "$bare_version" ]; then
	for hdr in "## [${bare_version}]" "## [v${bare_version}]"; do
		if heading_present "$hdr"; then
			section="$(extract_section "$hdr")"
			which_section="$hdr"
			break
		fi
	done
fi

# Fall back to the Unreleased section (accept either capitalization of the word).
if [ -z "$which_section" ]; then
	for hdr in "## [Unreleased]" "## [unreleased]"; do
		if heading_present "$hdr"; then
			section="$(extract_section "$hdr")"
			which_section="$hdr"
			break
		fi
	done
fi

if [ -z "$which_section" ]; then
	echo "::error::CHANGELOG check: no '## [${bare_version:-Unreleased}]' or '## [Unreleased]' section found in $changelog" >&2
	exit 1
fi

# Strip the section sub-headings and blanks; whatever remains is real content. A
# non-empty remainder means there is at least one entry.
content="$(printf '%s\n' "$section" |
	grep -vE '^###[[:space:]]' |
	grep -vE '^[[:space:]]*$' || true)"

if [ -z "$content" ]; then
	echo "::error::CHANGELOG '${which_section}' is empty. Add at least one entry before tagging a release (see CONTRIBUTING.md)." >&2
	exit 1
fi

n="$(printf '%s\n' "$content" | wc -l | tr -d ' ')"
echo "ok: CHANGELOG '${which_section}' has $n line(s) of content"
