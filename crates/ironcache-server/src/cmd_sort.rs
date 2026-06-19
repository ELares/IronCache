// SPDX-License-Identifier: MIT OR Apache-2.0
//! `SORT` / `SORT_RO` (COMMANDS.md generic, Redis `sortCommand`): sort the elements of a
//! list, set, or sorted set and optionally project / store the result.
//!
//! `SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC]
//! [ALPHA] [STORE destination]`. `SORT_RO` is the read-only form: it accepts every option
//! EXCEPT `STORE` (so it can run on a replica / under a read-only ACL).
//!
//! ## What is implemented (full core + BY/GET dereferencing)
//!
//! - NUMERIC sort (default): each element is parsed as a double; a non-numeric element is the
//!   Redis error `One or more scalars selected for the SORT operation are not numbers`.
//! - ALPHA sort: byte-lexicographic ordering of the elements (or of the BY weights).
//! - LIMIT offset count, ASC/DESC, STORE destination (stored as a LIST), and the count
//!   reply for STORE.
//! - BY pattern: the sort WEIGHT comes from an external key built by substituting `*` in the
//!   pattern with each element. A `pattern->field` form reads a HASH FIELD instead of a
//!   string key. A BY pattern with NO `*` is the `nosort` shortcut (Redis: skip sorting,
//!   preserve the source order -- for a SET the order is unspecified but stable here).
//! - GET pattern (repeatable): the output is projected by dereferencing each GET pattern per
//!   element (`#` yields the element itself; `pattern`/`pattern->field` reads a string key /
//!   hash field, a missing key/field yields nil). Without GET the sorted elements are
//!   returned verbatim.
//!
//! ## Documented partial / divergences (honest)
//!
//! - SINGLE-SHARD scope: BY/GET/STORE dereference keys on the CONNECTION'S ACCEPT SHARD (the
//!   store is this connection's whole keyspace; no cross-shard fan-out, ADR-0011). A true
//!   cross-shard SORT (the source on one shard, BY/GET keys on others) is a coordinator
//!   follow-up. SORT is `KeyedSingle` (its source key routes it home), so this matches how
//!   the other multi-key reads already scope.
//! - The `nosort` (BY pattern with no `*`) order for a SET is the store's stable member
//!   iteration order, not Redis's hash-table order; both are unspecified by the contract.
//! - SORT does NOT support the cluster CROSSSLOT check for its BY/GET keys (it routes on the
//!   source key only); in the single-shard-per-connection model every key co-locates, so
//!   this is not observable yet.

use crate::cmd_util::{ascii_upper, parse_f64};
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{
    DataType, ExpireWrite, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

/// A single GET projection pattern.
#[derive(Debug, Clone)]
enum GetPattern {
    /// `GET #`: the element itself.
    Hash,
    /// `GET pattern`: a string key built by substituting `*` (Redis only substitutes the
    /// FIRST `*`). The stored bytes are the `*`-marked prefix/suffix around the substitution.
    Key { prefix: Vec<u8>, suffix: Vec<u8> },
    /// `GET pattern->field`: a hash-field read. `prefix`/`suffix` build the key (around `*`);
    /// `field` is the (constant) hash field to read.
    HashField {
        prefix: Vec<u8>,
        suffix: Vec<u8>,
        field: Vec<u8>,
    },
}

/// The parsed SORT option set.
struct SortOptions {
    /// The BY weight pattern, or `None` for "sort by the element value itself".
    by: Option<ByPattern>,
    /// `(offset, count)` if LIMIT was given. A negative count means "to the end".
    limit: Option<(i64, i64)>,
    /// The GET projections (empty -> return the sorted elements verbatim).
    gets: Vec<GetPattern>,
    /// `true` for DESC, `false` for ASC (default).
    desc: bool,
    /// `true` for ALPHA (lexicographic) sort.
    alpha: bool,
    /// The STORE destination, or `None` (return the result to the client).
    store: Option<Vec<u8>>,
}

/// The BY weight pattern: a string key or a hash field, built by substituting `*`. A pattern
/// with NO `*` is `nosort` (Redis skips sorting entirely).
#[derive(Debug, Clone)]
enum ByPattern {
    /// `BY pattern` with no `*`: the `nosort` shortcut (do not sort; preserve source order).
    NoSort,
    /// `BY pattern` (a string key built around the first `*`).
    Key { prefix: Vec<u8>, suffix: Vec<u8> },
    /// `BY pattern->field` (a hash-field weight).
    HashField {
        prefix: Vec<u8>,
        suffix: Vec<u8>,
        field: Vec<u8>,
    },
}

/// Split a pattern on `->` into `(key_pattern, Some(field))` for a hash-field form, or
/// `(pattern, None)` for a plain string-key form (matching Redis `lookupKeyByPattern`,
/// which treats the FIRST `->` as the key/field separator).
fn split_hash_field(pattern: &[u8]) -> (&[u8], Option<&[u8]>) {
    // Find the first "->" occurrence.
    if let Some(pos) = pattern.windows(2).position(|w| w == b"->") {
        (&pattern[..pos], Some(&pattern[pos + 2..]))
    } else {
        (pattern, None)
    }
}

/// Split a key-pattern around its FIRST `*` into `(prefix, suffix)`; `None` if the pattern
/// has no `*` (Redis substitutes only the first `*`).
fn split_star(pattern: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let star = pattern.iter().position(|&b| b == b'*')?;
    Some((pattern[..star].to_vec(), pattern[star + 1..].to_vec()))
}

/// Build the concrete key from a `*`-substituted prefix/suffix and one element.
fn subst_key(prefix: &[u8], suffix: &[u8], elem: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + elem.len() + suffix.len());
    k.extend_from_slice(prefix);
    k.extend_from_slice(elem);
    k.extend_from_slice(suffix);
    k
}

/// Parse the SORT option tail (the args after the source key). `allow_store` is false for
/// `SORT_RO` (a `STORE` token there is a syntax error). Returns the parsed options or the
/// Redis-canonical error.
fn parse_options(args: &[Bytes], allow_store: bool) -> Result<SortOptions, ErrorReply> {
    let mut opts = SortOptions {
        by: None,
        limit: None,
        gets: Vec::new(),
        desc: false,
        alpha: false,
        store: None,
    };
    let mut i = 0;
    while i < args.len() {
        let up = ascii_upper(&args[i]);
        match up.as_slice() {
            b"ASC" => {
                opts.desc = false;
                i += 1;
            }
            b"DESC" => {
                opts.desc = true;
                i += 1;
            }
            b"ALPHA" => {
                opts.alpha = true;
                i += 1;
            }
            b"LIMIT" => {
                // LIMIT offset count: both must be integers.
                if i + 2 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                let (Some(off), Some(cnt)) = (
                    crate::cmd_util::parse_i64(&args[i + 1]),
                    crate::cmd_util::parse_i64(&args[i + 2]),
                ) else {
                    return Err(ErrorReply::not_an_integer());
                };
                opts.limit = Some((off, cnt));
                i += 3;
            }
            b"BY" => {
                if i + 1 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                opts.by = Some(parse_by(&args[i + 1]));
                i += 2;
            }
            b"GET" => {
                if i + 1 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                opts.gets.push(parse_get(&args[i + 1]));
                i += 2;
            }
            b"STORE" if allow_store => {
                if i + 1 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                opts.store = Some(args[i + 1].to_vec());
                i += 2;
            }
            _ => return Err(ErrorReply::syntax_error()),
        }
    }
    Ok(opts)
}

/// Parse a `BY` pattern argument into a [`ByPattern`].
fn parse_by(pattern: &[u8]) -> ByPattern {
    let (key_pat, field) = split_hash_field(pattern);
    let Some((prefix, suffix)) = split_star(key_pat) else {
        // No `*` anywhere in the key portion: the `nosort` shortcut.
        return ByPattern::NoSort;
    };
    match field {
        Some(f) => ByPattern::HashField {
            prefix,
            suffix,
            field: f.to_vec(),
        },
        None => ByPattern::Key { prefix, suffix },
    }
}

/// Parse a `GET` pattern argument into a [`GetPattern`].
fn parse_get(pattern: &[u8]) -> GetPattern {
    if pattern == b"#" {
        return GetPattern::Hash;
    }
    let (key_pat, field) = split_hash_field(pattern);
    // A GET pattern with no `*` reads the SAME (constant) key for every element (Redis
    // permits it; the key has no substitution). Treat a missing `*` as an empty prefix/suffix
    // around the literal pattern by using the whole pattern as the prefix with an empty
    // substitution point -- but since there is no `*`, every element maps to the same key.
    let (prefix, suffix) = split_star(key_pat).unwrap_or_else(|| (key_pat.to_vec(), Vec::new()));
    match field {
        Some(f) => GetPattern::HashField {
            prefix,
            suffix,
            field: f.to_vec(),
        },
        None => GetPattern::Key { prefix, suffix },
    }
}

/// Read a STRING key's bytes on this shard (for BY/GET string-key dereferencing). Returns
/// `None` for a missing key OR a non-string value (Redis `lookupKeyByPattern` ignores a
/// non-string just like a missing key, yielding nil).
fn read_string<S: Store>(store: &mut S, db: u32, now: UnixMillis, key: &[u8]) -> Option<Vec<u8>> {
    match store.read(db, key, now) {
        Some(v) if v.data_type() == DataType::String => Some(v.as_bytes().to_vec()),
        _ => None,
    }
}

/// Read a HASH FIELD's bytes on this shard (for BY/GET `pattern->field` dereferencing).
/// Returns `None` for a missing key, a non-hash value, or a missing field.
fn read_hash_field<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
    field: &[u8],
) -> Option<Vec<u8>> {
    let key = Bytes::copy_from_slice(key);
    store.rmw_mut(db, &key, now, |entry| match entry {
        RmwEntry::Vacant => RmwStep {
            action: RmwAction::Keep,
            expire: ExpireWrite::Unchanged,
            reply: None,
        },
        RmwEntry::OccupiedMut(mut o) => {
            let val = o
                .as_hash_mut()
                .and_then(|h| h.get(field).map(<[u8]>::to_vec));
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: val,
            }
        }
        RmwEntry::Occupied(_) => {
            unreachable!("read_hash_field uses rmw_mut, not rmw_mut->Occupied")
        }
    })
}

/// Resolve the BY weight bytes for one element (`None` -> Redis treats a missing BY value as
/// nil, which sorts as 0 numerically / empty alphabetically).
fn by_weight<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    by: &ByPattern,
    elem: &[u8],
) -> Option<Vec<u8>> {
    match by {
        ByPattern::NoSort => None,
        ByPattern::Key { prefix, suffix } => {
            let key = subst_key(prefix, suffix, elem);
            read_string(store, db, now, &key)
        }
        ByPattern::HashField {
            prefix,
            suffix,
            field,
        } => {
            let key = subst_key(prefix, suffix, elem);
            read_hash_field(store, db, now, &key, field)
        }
    }
}

/// Project one GET pattern for one element into a reply [`Value`] (nil for a missing
/// key/field; the element itself for `#`).
fn project_get<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    get: &GetPattern,
    elem: &[u8],
) -> Value {
    match get {
        GetPattern::Hash => Value::bulk(Bytes::copy_from_slice(elem)),
        GetPattern::Key { prefix, suffix } => {
            let key = subst_key(prefix, suffix, elem);
            read_string(store, db, now, &key).map_or(Value::Null, |b| Value::bulk(Bytes::from(b)))
        }
        GetPattern::HashField {
            prefix,
            suffix,
            field,
        } => {
            let key = subst_key(prefix, suffix, elem);
            read_hash_field(store, db, now, &key, field)
                .map_or(Value::Null, |b| Value::bulk(Bytes::from(b)))
        }
    }
}

/// Snapshot the source key's elements as owned bytes, or an error/empty signal. Returns
/// `Ok(None)` for a MISSING key (Redis SORT of a missing key is an empty result), `Ok(Some(
/// elems))` for a list/set/zset, or `Err(WRONGTYPE)` for a string/other non-sortable type.
fn read_source<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    key: &[u8],
) -> Result<Option<Vec<Vec<u8>>>, ErrorReply> {
    // Determine the type first (TYPE never WRONGTYPEs).
    match store.type_of(db, key, now) {
        None => Ok(None),
        Some(DataType::String) => Err(ErrorReply::wrong_type()),
        Some(_) => {
            let key = Bytes::copy_from_slice(key);
            let elems = store.rmw_mut(db, &key, now, |entry| {
                let elems = match entry {
                    RmwEntry::Vacant => Vec::new(),
                    RmwEntry::OccupiedMut(mut o) => {
                        if let Some(list) = o.as_list_mut() {
                            list.range(0, -1)
                        } else if let Some(set) = o.as_set_mut() {
                            set.members()
                        } else if let Some(zset) = o.as_zset_mut() {
                            // SORT of a zset uses the MEMBER VALUES (the zset's own scores are
                            // ignored unless BY is given; Redis sorts the members like a set).
                            zset.members_with_scores()
                                .into_iter()
                                .map(|(m, _)| m)
                                .collect()
                        } else {
                            // A non-list/set/zset collection cannot occur (TYPE already
                            // excluded String); be defensive with an empty snapshot.
                            Vec::new()
                        }
                    }
                    RmwEntry::Occupied(_) => unreachable!("rmw_mut never yields Occupied"),
                };
                RmwStep {
                    action: RmwAction::Keep,
                    expire: ExpireWrite::Unchanged,
                    reply: elems,
                }
            });
            Ok(Some(elems))
        }
    }
}

/// A sortable element paired with its (resolved) sort weight.
struct Weighted {
    elem: Vec<u8>,
    /// The numeric weight (used in numeric sort).
    num: f64,
    /// The alpha weight bytes (used in ALPHA sort); the BY value or the element itself.
    alpha: Vec<u8>,
}

/// Apply the LIMIT `(offset, count)` to a sorted vector in place (offset clamped to the
/// length; a negative offset is clamped to 0; a negative count means "to the end").
fn apply_limit<T>(items: &mut Vec<T>, limit: Option<(i64, i64)>) {
    let Some((offset, count)) = limit else {
        return;
    };
    let len = items.len() as i64;
    let start = offset.max(0).min(len);
    let end = if count < 0 {
        len
    } else {
        start.saturating_add(count).min(len)
    };
    let (start, end) = (start as usize, end as usize);
    // Keep [start, end): drain the tail then the head.
    items.truncate(end);
    items.drain(..start);
}

/// The shared SORT body. `allow_store` distinguishes SORT (true) from SORT_RO (false).
#[allow(clippy::too_many_lines)]
fn sort_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    allow_store: bool,
    cmd_name: &str,
) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let opts = match parse_options(&req.args[2..], allow_store) {
        Ok(o) => o,
        Err(e) => return Value::error(e),
    };

    // Snapshot the source elements (missing -> empty; WRONGTYPE on a string).
    let elems = match read_source(store, db, now, &req.args[1]) {
        Ok(Some(e)) => e,
        Ok(None) => Vec::new(),
        Err(e) => return Value::error(e),
    };

    // Determine whether we actually sort. `BY` with no `*` (NoSort) skips sorting; ALPHA with
    // a NoSort BY also skips. (Redis: a `dontsort` pattern preserves the source order.)
    let dontsort = matches!(opts.by, Some(ByPattern::NoSort));

    // Build the weighted elements. For a numeric sort WITHOUT BY, parse each element as a
    // double (a non-number is the Redis error). For ALPHA, the weight is the BY value (or the
    // element). For BY (numeric), the weight is the BY-read value parsed as a double.
    let mut weighted: Vec<Weighted> = Vec::with_capacity(elems.len());
    for elem in elems {
        // Resolve the BY weight bytes (None when no BY, NoSort, or a missing BY key).
        let by_bytes = match &opts.by {
            Some(by) if !dontsort => by_weight(store, db, now, by, &elem),
            _ => None,
        };
        if dontsort {
            // No sorting: weight is irrelevant; keep the element with placeholder weights.
            weighted.push(Weighted {
                elem,
                num: 0.0,
                alpha: Vec::new(),
            });
            continue;
        }
        if opts.alpha {
            // ALPHA: the comparison weight is the BY value if BY was given, else the element.
            let alpha = by_bytes.unwrap_or_else(|| elem.clone());
            weighted.push(Weighted {
                elem,
                num: 0.0,
                alpha,
            });
        } else {
            // NUMERIC: parse the weight (the BY value, or the element when no BY). A
            // missing BY value sorts as 0 (Redis treats a nil BY weight as 0). A
            // non-numeric weight is the canonical SORT error.
            let weight_bytes = match &opts.by {
                Some(_) => by_bytes,
                None => Some(elem.clone()),
            };
            let num = match weight_bytes {
                None => 0.0,
                Some(b) => match parse_f64(&b) {
                    Some(n) => n,
                    None => return Value::error(ErrorReply::sort_not_numbers()),
                },
            };
            weighted.push(Weighted {
                elem,
                num,
                alpha: Vec::new(),
            });
        }
    }

    // Sort (a STABLE sort, so equal weights keep their relative source order, matching Redis
    // when BY weights tie). Skipped entirely for the nosort shortcut.
    if !dontsort {
        if opts.alpha {
            weighted.sort_by(|a, b| a.alpha.cmp(&b.alpha));
        } else {
            // total_cmp gives a total order over f64 (NaN-safe, though NaN cannot occur:
            // parse_f64 rejects NaN). Redis orders by the numeric value.
            weighted.sort_by(|a, b| a.num.total_cmp(&b.num));
        }
        if opts.desc {
            weighted.reverse();
        }
    }

    // The sorted element list.
    let mut sorted: Vec<Vec<u8>> = weighted.into_iter().map(|w| w.elem).collect();
    // LIMIT is applied AFTER sorting (Redis applies the LIMIT window post-sort).
    apply_limit(&mut sorted, opts.limit);

    // Project the output: GET patterns (if any) per element, else the elements verbatim.
    let output: Vec<Value> = if opts.gets.is_empty() {
        sorted
            .iter()
            .map(|e| Value::bulk(Bytes::copy_from_slice(e)))
            .collect()
    } else {
        let mut out = Vec::with_capacity(sorted.len() * opts.gets.len());
        for e in &sorted {
            for g in &opts.gets {
                out.push(project_get(store, db, now, g, e));
            }
        }
        out
    };

    // STORE: write the result as a LIST at the destination and reply with the element count.
    // An EMPTY result DELETES the destination (Redis SORT ... STORE deletes dest on empty).
    if let Some(dest) = opts.store {
        // Flatten the projected output into list element bytes. A nil projection is stored as
        // an EMPTY string element (Redis stores nil GETs as empty bulks in the dest list).
        let list_elems: Vec<Vec<u8>> = output
            .iter()
            .map(|v| match v {
                Value::BulkString(Some(b)) => b.to_vec(),
                _ => Vec::new(),
            })
            .collect();
        let count = list_elems.len() as i64;
        let dest = Bytes::copy_from_slice(&dest);
        if list_elems.is_empty() {
            // Empty result: delete the destination (no empty list is ever stored).
            store.delete(db, &dest, now);
        } else {
            // Blind overwrite as a fresh list, clearing any prior TTL (Redis SORT STORE
            // overwrites the dest as a new key with no TTL). `Replace` on a vacant entry
            // behaves like `Insert`, so this both creates and overwrites correctly.
            store.rmw(db, &dest, now, move |_entry| RmwStep {
                action: RmwAction::Replace(NewValueOwned::list(list_elems)),
                expire: ExpireWrite::Clear,
                reply: (),
            });
        }
        return Value::Integer(count);
    }

    Value::Array(Some(output))
}

/// `SORT key [BY ...] [LIMIT ...] [GET ...] [ASC|DESC] [ALPHA] [STORE dest]`.
pub fn cmd_sort<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    sort_generic(store, db, now, req, true, "sort")
}

/// `SORT_RO key [BY ...] [LIMIT ...] [GET ...] [ASC|DESC] [ALPHA]` (no STORE).
pub fn cmd_sort_ro<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    sort_generic(store, db, now, req, false, "sort_ro")
}
