// SPDX-License-Identifier: MIT OR Apache-2.0
//! Layering contract test (STORAGE_API.md "Layering contract"): the command layer
//! (`ironcache-server`) depends ONLY on the storage WAIST (`ironcache-storage`),
//! never on the concrete store (`ironcache-store`). This is what lets the index /
//! object layout change without reopening the command layer.
//!
//! `ironcache-store` IS a dev-dependency here (the dispatch/e2e tests need a real
//! Store to drive the generic dispatch), so the check is specifically that it is
//! NOT a normal `[dependencies]` edge. We assert this structurally by parsing the
//! crate's own `Cargo.toml`: the `[dependencies]` section must not name
//! `ironcache-store`.

use std::path::Path;

#[test]
fn server_does_not_depend_on_concrete_store() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("read ironcache-server Cargo.toml");

    // Extract the [dependencies] section (up to the next top-level [section]).
    let deps = section(&text, "[dependencies]");

    // Look at actual dependency lines only: a key is a non-comment line whose
    // first token (before any '=' or whitespace) is the crate name. This ignores
    // the prose comment that explains WHY ironcache-store is absent here.
    let has_dep = |crate_name: &str| {
        deps.lines().any(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return false;
            }
            let key = line.split(['=', ' ', '\t']).next().unwrap_or("").trim();
            key == crate_name
        })
    };

    assert!(
        !has_dep("ironcache-store"),
        "ironcache-server [dependencies] must NOT include ironcache-store (layering \
         contract: the command layer names only the storage waist). Section:\n{deps}"
    );
    // And it MUST depend on the waist.
    assert!(
        has_dep("ironcache-storage"),
        "ironcache-server must depend on the storage waist (ironcache-storage)"
    );
}

/// Return the body of a TOML section starting at `header` up to the next line that
/// begins with `[` (a new section header), or end of file.
fn section<'a>(text: &'a str, header: &str) -> &'a str {
    let Some(start) = text.find(header) else {
        return "";
    };
    let after = start + header.len();
    let rest = &text[after..];
    // Find the next top-level section header on its own line.
    let mut end = rest.len();
    for (i, line) in rest.match_indices('\n') {
        let next = &rest[i + 1..];
        if next.starts_with('[') {
            end = i;
            break;
        }
        let _ = line;
    }
    &rest[..end]
}
