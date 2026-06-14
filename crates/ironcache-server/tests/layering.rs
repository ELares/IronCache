// SPDX-License-Identifier: MIT OR Apache-2.0
//! Layering contract test (STORAGE_API.md "Layering contract"): the command layer
//! (`ironcache-server`) depends ONLY on the storage WAIST (`ironcache-storage`),
//! never on the concrete store (`ironcache-store`). This is what lets the index /
//! object layout change without reopening the command layer.
//!
//! `ironcache-store` IS a dev-dependency here (the dispatch/e2e tests need a real
//! Store to drive the generic dispatch), so the check is specifically that it is
//! NOT a normal `[dependencies]` edge. We assert this by PARSING the crate's own
//! `Cargo.toml` with the `toml` crate and walking the `[dependencies]`,
//! `[target.*.dependencies]`, and `[build-dependencies]` tables. Parsing (rather
//! than string-scanning the section) makes the guard form-independent: a dotted
//! -table `[dependencies.ironcache-store]` is caught exactly like the inline
//! `ironcache-store = { ... }` form, because both deserialize into the same
//! `dependencies` table key.

use std::path::Path;
use toml::{Table, Value};

#[test]
fn server_does_not_depend_on_concrete_store() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("read ironcache-server Cargo.toml");
    let doc: Table = toml::from_str(&text).expect("parse ironcache-server Cargo.toml");

    // Collect dependency NAMES from every NON-dev dependency table: top-level
    // [dependencies] and [build-dependencies], plus any [target.<cfg>.dependencies]
    // / [target.<cfg>.build-dependencies]. Dev-dependencies are intentionally
    // excluded (ironcache-store is allowed there).
    let mut runtime_deps: Vec<String> = Vec::new();
    collect_table_keys(doc.get("dependencies"), &mut runtime_deps);
    collect_table_keys(doc.get("build-dependencies"), &mut runtime_deps);
    if let Some(targets) = doc.get("target").and_then(Value::as_table) {
        for (_cfg, t) in targets {
            collect_table_keys(t.get("dependencies"), &mut runtime_deps);
            collect_table_keys(t.get("build-dependencies"), &mut runtime_deps);
        }
    }

    let has_runtime_dep = |name: &str| runtime_deps.iter().any(|d| d == name);

    assert!(
        !has_runtime_dep("ironcache-store"),
        "ironcache-server must NOT have ironcache-store as a (non-dev) dependency \
         (layering contract: the command layer names only the storage waist). \
         Found runtime deps: {runtime_deps:?}"
    );
    // And it MUST depend on the waist.
    assert!(
        has_runtime_dep("ironcache-storage"),
        "ironcache-server must depend on the storage waist (ironcache-storage). \
         Found runtime deps: {runtime_deps:?}"
    );
}

/// Push every key of the given TOML dependency table (if present) into `out`. Both
/// the inline form (`name = { ... }` / `name = "1"`) and the dotted-table form
/// (`[dependencies.name]`) deserialize to a table whose keys are the crate names,
/// so this one walk is form-independent.
fn collect_table_keys(table: Option<&Value>, out: &mut Vec<String>) {
    if let Some(t) = table.and_then(Value::as_table) {
        out.extend(t.keys().cloned());
    }
}
