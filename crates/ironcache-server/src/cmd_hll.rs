// SPDX-License-Identifier: MIT OR Apache-2.0
//! HyperLogLog command handlers (PFADD, PFCOUNT, PFMERGE) over the STRING type
//! (COMMANDS.md HLL semantics). PR-11: dense-only, string-backed.
//!
//! ## An HLL is the string type (no new ValueRepr)
//!
//! A HyperLogLog in Redis is NOT a distinct data type: it is an opaque STRING value
//! whose bytes are the dense (or sparse) HLL object. So TYPE returns `string`, OBJECT
//! ENCODING reports a string encoding, and these handlers operate on
//! [`DataType::String`] only and need NO new `ValueRepr`. A fresh HLL is created as a
//! RAW 12304-byte string and round-trips through GET/STRLEN/TYPE like any other string
//! value; the storage waist (`ironcache-storage::Store`) is untouched.
//!
//! ## Dense-only, Redis-interoperable bytes
//!
//! The in-memory bytes ARE the real Redis DENSE layout (the `HYLL` magic + 16384
//! 6-bit registers), the hash IS MurmurHash64A(seed 0xadc83b19), and the estimator IS
//! the modern Redis (Ertl 2017) cardinality estimator, so PFCOUNT matches modern
//! Redis cardinality estimates exactly for a deterministic element set.
//!
//! ## The cached cardinality is always left INVALID
//!
//! The 16-byte header carries an 8-byte cached cardinality with a cache-invalid flag
//! (the MSB of the last cache byte). On every write this PR leaves the cache marked
//! INVALID (0x80 in `card[7]`) and PFCOUNT ALWAYS recomputes; it never reads or
//! populates the cache. This is observably identical to Redis (the same count) and
//! avoids a readonly-write / WATCH hazard (a PFCOUNT that wrote back a freshly-computed
//! cache would dirty a watched key on a pure read).
//!
//! ## Deferred (tracked follow-up)
//!
//! The SPARSE opcode stream (XZERO/ZERO/VAL), sparse->dense promotion, DUMP/RESTORE
//! byte-interop, PFDEBUG, and PFSELFTEST are NOT implemented. A freshly created HLL is
//! dense (12304 bytes) from the start. Sparse encoding + DUMP/RESTORE byte-interop are
//! a tracked follow-up.

use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{
    DataType, ExpireWrite, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

// ---------------------------------------------------------------------------
// Dense HLL constants (the exact Redis values for P = 14).
// ---------------------------------------------------------------------------

/// The HLL precision: 2^14 = 16384 registers (Redis `HLL_P`).
const HLL_P: u32 = 14;
/// The number of registers, 2^P (Redis `HLL_REGISTERS`).
const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
/// Bits per dense register (Redis `HLL_BITS`).
const HLL_BITS: usize = 6;
/// The maximum value a 6-bit register can hold (Redis `HLL_REGISTER_MAX`).
const HLL_REGISTER_MAX: u8 = 63;
/// The fixed header size in bytes (Redis `HLL_HDR_SIZE`): magic[4] + encoding[1] +
/// notused[3] + card[8].
const HLL_HDR_SIZE: usize = 16;
/// The dense register-block size in bytes: `ceil(REGISTERS * BITS / 8)` = 12288.
const HLL_DENSE_REG_BYTES: usize = (HLL_REGISTERS * HLL_BITS).div_ceil(8); // 12288
/// The total dense object size in bytes: header + register block = 12304 (Redis
/// `HLL_DENSE_SIZE`).
const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + HLL_DENSE_REG_BYTES; // 12304
/// The dense encoding tag stored in header byte[4] (Redis `HLL_DENSE`).
const HLL_DENSE: u8 = 0;
/// The number of leading-zero bits the estimator histograms over: `64 - P` = 50
/// (Redis `HLL_Q`). `hllPatLen` produces a register value in `0..=HLL_Q + 1`.
const HLL_Q: usize = 64 - HLL_P as usize; // 50

// ---------------------------------------------------------------------------
// Dense register get/set (the EXACT Redis HLL_DENSE_GET/SET_REGISTER bit math).
//
// Registers are 6 bits each, packed little-endian, so a register straddles a byte
// boundary. Redis's macros read/write `p[byte]` and `p[byte + 1]`. The LAST register
// (regnum 16383) starts at bit 98298 = byte 12287, bit 2, and occupies bits 2..8 of
// byte 12287, ending exactly at the 12288-byte boundary: its high bits contribute
// nothing past the block. We index `p[byte + 1]` only when it is in bounds; for the
// final register we treat the (would-be) second byte as 0 on read and skip writing its
// (zero) high bits, so the block is exactly 12288 bytes with NO pad and NO panic under
// `#![forbid(unsafe_code)]`. A round-trip unit test covers regnum 16383 = 63.
// ---------------------------------------------------------------------------

/// Get dense register `regnum` from the 12288-byte register block `p`.
fn dense_get_register(p: &[u8], regnum: usize) -> u8 {
    let byte = regnum * HLL_BITS / 8;
    let fb = (regnum * HLL_BITS) & 7;
    let fb8 = 8 - fb;
    let b0 = u32::from(p[byte]);
    // The second byte is out of bounds only for the final register, whose high bits
    // land exactly at the block boundary and contribute nothing; treat it as 0.
    let b1 = p.get(byte + 1).map_or(0u32, |&b| u32::from(b));
    (((b0 >> fb) | (b1 << fb8)) & 63) as u8
}

/// Set dense register `regnum` in the 12288-byte register block `p` to `val` (0..=63).
fn dense_set_register(p: &mut [u8], regnum: usize, val: u8) {
    debug_assert!(val <= HLL_REGISTER_MAX, "register value exceeds 6 bits");
    let byte = regnum * HLL_BITS / 8;
    let fb = (regnum * HLL_BITS) & 7;
    let fb8 = 8 - fb;
    let v = u32::from(val);
    p[byte] &= !((63u32 << fb) as u8);
    p[byte] |= (v << fb) as u8;
    // The high bits spill into the next byte. For the final register the next byte is
    // out of bounds, but those spilled bits are all zero (the value fits within the
    // block), so skipping the write is exact.
    if let Some(next) = p.get_mut(byte + 1) {
        *next &= !((63u32 >> fb8) as u8);
        *next |= (v >> fb8) as u8;
    }
}

// ---------------------------------------------------------------------------
// MurmurHash64A (seed 0xadc83b19), the Redis HLL hash, in safe Rust.
// ---------------------------------------------------------------------------

/// MurmurHash64A over `data` with the Redis HLL seed `0xadc83b19`. A faithful safe-Rust
/// port of the reference algorithm (8-byte little-endian blocks + the big-to-small tail
/// cascade + the finalizer), so it produces the SAME 64-bit hash Redis feeds into
/// `hllPatLen`.
fn murmur64a(data: &[u8]) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    const SEED: u64 = 0xadc8_3b19;

    let len = data.len();
    let mut h: u64 = SEED ^ (len as u64).wrapping_mul(M);

    // Process each full 8-byte little-endian block.
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        // `chunk` is exactly 8 bytes (chunks_exact guarantees it).
        let mut k = u64::from_le_bytes(chunk.try_into().expect("chunk is 8 bytes"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Tail: the `switch (len & 7)` big-to-small fallthrough cascade from MurmurHash64A.
    let tail = chunks.remainder();
    let mut t: u64 = 0;
    // Each present tail byte k contributes `data[k] << (8*k)`; the reference shifts the
    // highest present byte first with fallthrough, which is the same as OR-ing each byte
    // into its little-endian position.
    for (k, &b) in tail.iter().enumerate() {
        t |= u64::from(b) << (8 * k);
    }
    if !tail.is_empty() {
        h ^= t;
        h = h.wrapping_mul(M);
    }

    // Finalizer.
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Map an element to its `(register index, pattern length)` exactly as Redis `hllPatLen`
/// does (P = 14): the low 14 bits of the hash select the register; the count is the
/// 1-based position of the lowest set bit in the remaining high bits (a sentinel bit at
/// position `64 - P` guarantees termination), i.e. `leading run of zeros + 1`.
fn hll_pat_len(data: &[u8]) -> (usize, u8) {
    let hash = murmur64a(data);
    let index = (hash & (HLL_REGISTERS as u64 - 1)) as usize;
    // Drop the index bits and force a set bit at position (64 - P) so the scan always
    // terminates (the maximum count is HLL_Q + 1 = 51).
    let mut bits = hash >> HLL_P;
    bits |= 1u64 << (64 - HLL_P);
    let mut count: u8 = 1;
    let mut bit: u64 = 1;
    while bits & bit == 0 {
        count += 1;
        bit <<= 1;
    }
    (index, count)
}

// ---------------------------------------------------------------------------
// The dense object: create / validate / register access / add / estimate.
// ---------------------------------------------------------------------------

/// Build a fresh, all-zero dense HLL object (12304 bytes): the `HYLL` magic, the dense
/// encoding tag, and a cache-invalid header; every register is 0.
fn new_dense() -> Vec<u8> {
    let mut buf = vec![0u8; HLL_DENSE_SIZE];
    buf[0] = b'H';
    buf[1] = b'Y';
    buf[2] = b'L';
    buf[3] = b'L';
    buf[4] = HLL_DENSE;
    // bytes [5..8) reserved zero; card[0..8) zero except the cache-invalid flag.
    mark_cache_invalid(&mut buf);
    buf
}

/// Mark the cached cardinality INVALID (set the MSB of the last cache byte, byte[15]).
/// This PR always leaves the cache invalid on a write and recomputes in PFCOUNT.
fn mark_cache_invalid(buf: &mut [u8]) {
    buf[15] |= 0x80;
}

/// Whether `bytes` is a valid DENSE HLL object: the exact dense length, the `HYLL`
/// magic, and the dense encoding tag. A string that is not a valid (dense) HLL is the
/// [`ErrorReply::hll_invalid_value`] error; a non-string is WRONGTYPE (checked by the
/// caller before this).
fn is_valid_dense(bytes: &[u8]) -> bool {
    bytes.len() == HLL_DENSE_SIZE && &bytes[0..4] == b"HYLL" && bytes[4] == HLL_DENSE
}

/// The register block (the bytes past the 16-byte header) of a dense object, read-only.
fn reg_block(obj: &[u8]) -> &[u8] {
    &obj[HLL_HDR_SIZE..]
}

/// The register block of a dense object, mutable.
fn reg_block_mut(obj: &mut [u8]) -> &mut [u8] {
    &mut obj[HLL_HDR_SIZE..]
}

/// Add one element to the dense object: compute `(index, count)` and bump the register
/// to `max(old, count)`. Returns whether the register actually changed (so PFADD can
/// decide whether to write back).
fn dense_add(obj: &mut [u8], element: &[u8]) -> bool {
    let (index, count) = hll_pat_len(element);
    let p = reg_block_mut(obj);
    let old = dense_get_register(p, index);
    if count > old {
        dense_set_register(p, index, count);
        true
    } else {
        false
    }
}

/// Merge the source dense object's registers into `max_regs` (per-register max), where
/// `max_regs` is a working array of 16384 register values. Used by PFCOUNT (multi-key
/// union) and PFMERGE.
fn merge_into(max_regs: &mut [u8; HLL_REGISTERS], src_obj: &[u8]) {
    let p = reg_block(src_obj);
    for (i, slot) in max_regs.iter_mut().enumerate() {
        let v = dense_get_register(p, i);
        if v > *slot {
            *slot = v;
        }
    }
}

/// Pack a working register array back into a fresh dense object (cache-invalid).
fn dense_from_regs(regs: &[u8; HLL_REGISTERS]) -> Vec<u8> {
    let mut obj = new_dense();
    let p = reg_block_mut(&mut obj);
    for (i, &v) in regs.iter().enumerate() {
        if v != 0 {
            dense_set_register(p, i, v);
        }
    }
    obj
}

// ---------------------------------------------------------------------------
// The estimator: the modern Redis (Ertl 2017) cardinality algorithm.
// ---------------------------------------------------------------------------

/// `hllSigma` (Redis src/hyperloglog.c): the series used for the "many empty registers"
/// correction. Converges by a fixed-point loop on `z`.
///
/// The exact float comparisons (`x == 1.0`, `zp == z`) are INTENTIONAL: they are the
/// EXACT Redis convergence test (`zPrime != z`) and the `x == 1.` boundary guard,
/// reproduced verbatim so the estimate matches Redis. They are not approximate-equality
/// bugs, so `clippy::float_cmp` is allowed here with that justification.
#[allow(clippy::float_cmp)]
fn hll_sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0_f64;
    let mut z = x;
    loop {
        x *= x;
        let zp = z;
        z += x * y;
        y += y;
        if zp == z {
            break;
        }
    }
    z
}

/// `hllTau` (Redis src/hyperloglog.c): the series used for the "many saturated
/// registers" correction. Converges by a fixed-point loop on `z`.
///
/// As with [`hll_sigma`], the exact float comparisons are the verbatim Redis convergence
/// test + boundary guards, so `clippy::float_cmp` is allowed here.
#[allow(clippy::float_cmp)]
fn hll_tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0_f64;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let zp = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if zp == z {
            break;
        }
    }
    z / 3.0
}

/// Estimate the cardinality from a register-value histogram (`reghisto[v]` = number of
/// registers with value `v`), using the modern Redis estimator. `reghisto` covers
/// indices `0..=HLL_Q + 1` (the register values `hllPatLen` can produce); the array is
/// sized 64 so every index is in range.
fn hll_estimate(reghisto: &[i32; 64]) -> u64 {
    // The Redis HLL constant `HLL_ALPHA_INF` (src/hyperloglog.c), exactly `0.5 / ln(2)`
    // = 0.7213475204444817. NOT the Euler-Mascheroni constant 0.577...: using that
    // under-counts by ~20%. The 0.5/ln(2) value is what makes PFCOUNT match real Redis
    // (cross-checked against redis-cli on a fixed corpus).
    const HLL_ALPHA_INF: f64 = 0.5 / core::f64::consts::LN_2;
    let m = HLL_REGISTERS as f64;

    let mut z = m * hll_tau((m - f64::from(reghisto[HLL_Q + 1])) / m);
    for j in (1..=HLL_Q).rev() {
        z += f64::from(reghisto[j]);
        z *= 0.5;
    }
    z += m * hll_sigma(f64::from(reghisto[0]) / m);
    let e = (HLL_ALPHA_INF * m * m / z).round();
    e as u64
}

/// The PFCOUNT reply integer for a histogram: the estimate as a NON-NEGATIVE RESP
/// integer, saturating to `i64::MAX`. A degenerate fully-saturated register block (every
/// register at its max, reachable only by injecting a crafted 12304-byte `HYLL` string)
/// drives the estimator denominator to 0, so `hll_estimate` returns `u64::MAX`; a naive
/// `as i64` cast would wrap that to -1 (a negative cardinality). Redis computes
/// `(uint64_t) llroundl(+inf)` = `LLONG_MAX` and replies that large POSITIVE value, so
/// saturating an out-of-`i64`-range estimate to `i64::MAX` matches Redis exactly while
/// guaranteeing PFCOUNT is never negative.
fn estimate_reply(reghisto: &[i32; 64]) -> i64 {
    i64::try_from(hll_estimate(reghisto)).unwrap_or(i64::MAX)
}

/// Build the register-value histogram for a dense object's register block.
fn dense_reghisto(obj: &[u8]) -> [i32; 64] {
    let mut reghisto = [0i32; 64];
    let p = reg_block(obj);
    for i in 0..HLL_REGISTERS {
        let v = dense_get_register(p, i) as usize;
        reghisto[v] += 1;
    }
    reghisto
}

/// Build the register-value histogram from a working register array (the union path).
fn regs_reghisto(regs: &[u8; HLL_REGISTERS]) -> [i32; 64] {
    let mut reghisto = [0i32; 64];
    for &v in regs {
        reghisto[v as usize] += 1;
    }
    reghisto
}

// ---------------------------------------------------------------------------
// The commands.
// ---------------------------------------------------------------------------

/// `PFADD key [element ...]` -> Integer 1 if the HLL was created OR any register
/// changed, 0 otherwise. A missing key is created as a fresh dense HLL (even with no
/// elements). A no-op (existing HLL, nothing changed) writes NOTHING (so a watched key
/// stays clean and no dirty fires). WRONGTYPE on a non-string; the
/// [`ErrorReply::hll_invalid_value`] error on a string that is not a valid dense HLL.
/// `denyoom` (it can allocate a 12304-byte value).
pub fn cmd_pfadd<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("pfadd"));
    }
    let elements: Vec<Bytes> = req.args[2..].to_vec();
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Vacant => {
            // Create a fresh dense HLL and add every element. The key was created, so
            // PFADD returns 1 even when no register changed (and even with no elements).
            let mut obj = new_dense();
            for e in &elements {
                dense_add(&mut obj, e);
            }
            RmwStep {
                action: RmwAction::Insert(NewValueOwned::Bytes(Bytes::from(obj))),
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(1),
            }
        }
        RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
            keep_err(ErrorReply::wrong_type())
        }
        RmwEntry::Occupied(o) => {
            if !is_valid_dense(o.as_bytes()) {
                return keep_err(ErrorReply::hll_invalid_value());
            }
            let mut obj = o.as_bytes().to_vec();
            let mut changed = false;
            for e in &elements {
                if dense_add(&mut obj, e) {
                    changed = true;
                }
            }
            if changed {
                // A register moved: the cache is already invalid in `obj` (it was a
                // valid dense object whose cache we never populate). Re-assert invalid
                // defensively, then write back.
                mark_cache_invalid(&mut obj);
                RmwStep {
                    action: RmwAction::Replace(NewValueOwned::Bytes(Bytes::from(obj))),
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(1),
                }
            } else {
                // No register changed: NO write (Keep + Unchanged), so a watched key is
                // not falsely invalidated and no dirty fires. Return 0.
                RmwStep {
                    action: RmwAction::Keep,
                    expire: ExpireWrite::Unchanged,
                    reply: Value::Integer(0),
                }
            }
        }
        // Unreachable: PFADD uses the read-only `rmw`, never `rmw_mut`.
        RmwEntry::OccupiedMut(_) => unreachable!("cmd_pfadd uses rmw, not rmw_mut"),
    })
}

/// `PFCOUNT key [key ...]` -> the (approximate) cardinality as a RESP Integer.
///
/// A single missing key is 0. With multiple keys the result is the cardinality of the
/// UNION (per-register max across all existing valid HLLs). Missing keys contribute
/// nothing. Any wrong-type or invalid-HLL input aborts with the matching error and NO
/// partial result. READ-ONLY: it never writes (the cache is always recomputed, never
/// written back). SINGLE-SHARD-PER-CONNECTION like the other multi-key commands.
pub fn cmd_pfcount<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("pfcount"));
    }

    // Single-key fast path: estimate directly off the stored object's histogram (no
    // working-array copy needed).
    if req.args.len() == 2 {
        return match store.read(db, &req.args[1], now) {
            Some(v) if v.data_type() == DataType::String => {
                if is_valid_dense(v.as_bytes()) {
                    let reghisto = dense_reghisto(v.as_bytes());
                    Value::Integer(estimate_reply(&reghisto))
                } else {
                    Value::error(ErrorReply::hll_invalid_value())
                }
            }
            Some(_) => Value::error(ErrorReply::wrong_type()),
            None => Value::Integer(0),
        };
    }

    // Multi-key union: merge per-register max across every existing valid HLL. A
    // wrong-type / invalid input aborts BEFORE producing any count.
    let mut max_regs = [0u8; HLL_REGISTERS];
    for key in &req.args[1..] {
        match store.read(db, key, now) {
            Some(v) if v.data_type() == DataType::String => {
                if !is_valid_dense(v.as_bytes()) {
                    return Value::error(ErrorReply::hll_invalid_value());
                }
                merge_into(&mut max_regs, v.as_bytes());
            }
            Some(_) => return Value::error(ErrorReply::wrong_type()),
            None => {}
        }
    }
    let reghisto = regs_reghisto(&max_regs);
    Value::Integer(estimate_reply(&reghisto))
}

/// `PFMERGE destkey [sourcekey ...]` -> `+OK`. Computes the per-register max across the
/// destination's current HLL (if any) and all source HLLs, and writes the result back
/// to `destkey` as a dense HLL. Missing sources contribute nothing; `PFMERGE destkey`
/// with no sources ensures `destkey` exists as a (possibly empty) dense HLL. Any
/// wrong-type / invalid input (dest or source) aborts with the matching error and NO
/// write (no partial merge). `denyoom`.
pub fn cmd_pfmerge<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity("pfmerge"));
    }

    // Gather the merged registers from the destination + every source FIRST, validating
    // each, so a WRONGTYPE / invalid-HLL on ANY input aborts before we write the dest
    // (no partial merge). The dest counts as both a source (its current registers
    // participate in the max) and the write target.
    let mut max_regs = [0u8; HLL_REGISTERS];
    for key in &req.args[1..] {
        match store.read(db, key, now) {
            Some(v) if v.data_type() == DataType::String => {
                if !is_valid_dense(v.as_bytes()) {
                    return Value::error(ErrorReply::hll_invalid_value());
                }
                merge_into(&mut max_regs, v.as_bytes());
            }
            Some(_) => return Value::error(ErrorReply::wrong_type()),
            None => {}
        }
    }

    // Build the merged dense object and write it to the destination through the store
    // write path (so accounting + WATCH notify fire). Redis PRESERVES an existing
    // destination's TTL: pfmergeCommand mutates the existing object in place
    // (dbUnshareStringValue -> dbSetValue with keepTTL=1) and never touches the expires
    // dict; a newly created destination simply has no TTL. ExpireWrite::Unchanged matches
    // both cases (keep an existing deadline; a vacant entry gets none), like cmd_pfadd.
    let merged = dense_from_regs(&max_regs);
    let dest = req.args[1].clone();
    store.rmw(db, &dest, now, move |_entry| RmwStep {
        action: RmwAction::Replace(NewValueOwned::Bytes(Bytes::from(merged))),
        expire: ExpireWrite::Unchanged,
        reply: Value::ok(),
    })
}

// ---------------------------------------------------------------------------
// Shared rmw abort helper.
// ---------------------------------------------------------------------------

/// A no-write rmw step that just returns an error reply (value + TTL untouched). The
/// shared abort path for the HLL mutators (WRONGTYPE / invalid-HLL).
fn keep_err(e: ErrorReply) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply: Value::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{CountingAccounting, Store};
    use ironcache_store::ShardStore;

    type TestStore = ShardStore<ironcache_eviction::Policy, CountingAccounting>;

    fn test_store() -> TestStore {
        ShardStore::with_hooks(
            1,
            ironcache_eviction::Policy::cache_default(),
            CountingAccounting::new(),
        )
    }

    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    const NOW: UnixMillis = UnixMillis(0);

    fn int(v: &Value) -> i64 {
        match v {
            Value::Integer(n) => *n,
            other => panic!("expected an integer, got {other:?}"),
        }
    }

    fn err_line(v: &Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    fn get_bytes(store: &mut TestStore, key: &[u8]) -> Option<Vec<u8>> {
        store.read(0, key, NOW).map(|v| v.as_bytes().to_vec())
    }

    // ---- Register bit-packing round trip (incl. the boundary register). ----

    #[test]
    fn dense_register_round_trip_including_boundary() {
        // A fresh register block (12288 bytes). Set/get a spread of registers, including
        // the boundary register 16383 (whose high bits land exactly at the block end).
        let mut block = vec![0u8; HLL_DENSE_REG_BYTES];
        for &regnum in &[0usize, 1, 2, 100, 8191, 8192, 16382, 16383] {
            for val in [0u8, 1, 31, 32, 63] {
                dense_set_register(&mut block, regnum, val);
                assert_eq!(
                    dense_get_register(&block, regnum),
                    val,
                    "regnum {regnum} val {val} round trip"
                );
            }
        }
        // Setting one register must not corrupt an adjacent one.
        dense_set_register(&mut block, 16382, 63);
        dense_set_register(&mut block, 16383, 63);
        assert_eq!(dense_get_register(&block, 16382), 63);
        assert_eq!(dense_get_register(&block, 16383), 63);
        // Clear the boundary register and confirm the neighbor survives.
        dense_set_register(&mut block, 16383, 0);
        assert_eq!(dense_get_register(&block, 16383), 0);
        assert_eq!(dense_get_register(&block, 16382), 63);
    }

    #[test]
    fn boundary_register_set_max_does_not_panic_or_oob() {
        // The whole-object path (header + block). Register 16383 = 63 then back to 0.
        let mut obj = new_dense();
        dense_set_register(reg_block_mut(&mut obj), 16383, 63);
        assert_eq!(dense_get_register(reg_block(&obj), 16383), 63);
        dense_set_register(reg_block_mut(&mut obj), 16383, 0);
        assert_eq!(dense_get_register(reg_block(&obj), 16383), 0);
        // The object stayed exactly the dense size (no pad byte snuck in).
        assert_eq!(obj.len(), HLL_DENSE_SIZE);
    }

    // ---- MurmurHash64A known-answer + consistency. ----

    #[test]
    fn murmur64a_known_answers_and_consistency() {
        // Pinned reference values for MurmurHash64A with the Redis HLL seed 0xadc83b19.
        // These are CROSS-CHECKED against real Redis (redis-cli PFADD + PFDEBUG GETREG):
        // `"hello"` sets register index 9216 (the low 14 bits of the hash below) to value
        // 1, exactly as Redis does, confirming the hash is Redis-faithful (not just
        // self-consistent). The empty-input value is the seed-only finalized hash.
        assert_eq!(murmur64a(b""), 0xd8df_ea65_85bc_9732);
        assert_eq!(murmur64a(b"hello"), 0x0f65_6f01_eecf_e400);
        // The low 14 bits of the "hello" hash select register 9216 (Redis cross-checked).
        assert_eq!(
            (murmur64a(b"hello") & (HLL_REGISTERS as u64 - 1)) as usize,
            9216
        );
        // Same input is stable; different inputs differ.
        assert_eq!(murmur64a(b"foobar"), murmur64a(b"foobar"));
        assert_ne!(murmur64a(b"a"), murmur64a(b"b"));
        // Tail handling: lengths 0..=8 all hash without panic and are distinct here.
        let mut seen = std::collections::BTreeSet::new();
        for s in [
            &b""[..],
            b"a",
            b"ab",
            b"abc",
            b"abcd",
            b"abcde",
            b"abcdef",
            b"abcdefg",
            b"abcdefgh",
        ] {
            seen.insert(murmur64a(s));
        }
        assert_eq!(seen.len(), 9, "all distinct short inputs hashed distinctly");
    }

    // ---- PFADD: create returns 1, idempotent re-add returns 0, no-op no write. ----

    #[test]
    fn pfadd_create_then_idempotent() {
        let mut s = test_store();
        // First add creates the HLL -> 1.
        assert_eq!(
            int(&cmd_pfadd(
                &mut s,
                0,
                NOW,
                &req(&[b"PFADD", b"hll", b"a", b"b", b"c"])
            )),
            1
        );
        // Re-adding the SAME elements changes nothing -> 0.
        assert_eq!(
            int(&cmd_pfadd(
                &mut s,
                0,
                NOW,
                &req(&[b"PFADD", b"hll", b"a", b"b", b"c"])
            )),
            0
        );
        // Adding a NEW element changes a register -> 1 (usually; "z" is new).
        // It may collide, so add several new ones to be robust.
        assert_eq!(
            int(&cmd_pfadd(
                &mut s,
                0,
                NOW,
                &req(&[b"PFADD", b"hll", b"x1", b"x2", b"x3", b"x4", b"x5"])
            )),
            1
        );
    }

    #[test]
    fn pfadd_missing_key_no_elements_creates_and_returns_1() {
        let mut s = test_store();
        // PFADD with no elements on a MISSING key still creates an empty HLL -> 1.
        assert_eq!(
            int(&cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"hll"]))),
            1
        );
        // The key now exists as a dense HLL.
        assert_eq!(get_bytes(&mut s, b"hll").unwrap().len(), HLL_DENSE_SIZE);
        // PFADD with no elements on the EXISTING valid HLL writes nothing -> 0.
        assert_eq!(
            int(&cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"hll"]))),
            0
        );
    }

    #[test]
    fn pfadd_no_op_does_not_write() {
        // A no-op PFADD (nothing changed) must NOT write back (Keep), so a watch is not
        // falsely invalidated. We assert this via the WATCH dirty-CAS surface.
        use ironcache_storage::Watch;
        let mut s = test_store();
        cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"hll", b"a", b"b", b"c"]));
        // Snapshot a watch on the key.
        let snap = s.watch_snapshot(0, b"hll", NOW);
        assert!(!s.watch_is_dirty(&snap, NOW), "fresh snapshot is clean");
        // A no-op re-add of the same elements must keep the key CLEAN.
        assert_eq!(
            int(&cmd_pfadd(
                &mut s,
                0,
                NOW,
                &req(&[b"PFADD", b"hll", b"a", b"b", b"c"])
            )),
            0
        );
        assert!(
            !s.watch_is_dirty(&snap, NOW),
            "a no-op PFADD must not dirty a watched key"
        );
        // A real add (new elements) DOES dirty the key.
        cmd_pfadd(
            &mut s,
            0,
            NOW,
            &req(&[b"PFADD", b"hll", b"q1", b"q2", b"q3", b"q4", b"q5"]),
        );
        assert!(
            s.watch_is_dirty(&snap, NOW),
            "a real PFADD must dirty a watched key"
        );
    }

    // ---- PFCOUNT: missing = 0, tiny exact, ~true count for 1000. ----

    #[test]
    fn pfcount_missing_is_zero() {
        let mut s = test_store();
        assert_eq!(
            int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"nope"]))),
            0
        );
    }

    #[test]
    fn pfcount_tiny_counts_are_accurate() {
        // For tiny N the estimator is essentially exact (+/-1 at most for these).
        let mut s = test_store();
        // 0 elements (empty HLL) -> 0.
        cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"e0"]));
        assert_eq!(
            int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"e0"]))),
            0
        );
        // 1, 3, 10 distinct.
        cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"e1", b"a"]));
        assert_eq!(
            int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"e1"]))),
            1
        );
        // 3 distinct: real Redis returns exactly 3 for `a b c` (verified via redis-cli);
        // allow +/-1 for the estimator floor.
        cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"e3", b"a", b"b", b"c"]));
        let c3 = int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"e3"])));
        assert!((2..=4).contains(&c3), "3 distinct estimated {c3}");
        let ten: Vec<Vec<u8>> = (0..10).map(|i| format!("v{i}").into_bytes()).collect();
        let mut parts: Vec<&[u8]> = vec![b"PFADD", b"e10"];
        for v in &ten {
            parts.push(v);
        }
        cmd_pfadd(&mut s, 0, NOW, &req(&parts));
        let c10 = int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"e10"])));
        assert!((9..=11).contains(&c10), "10 distinct estimated {c10}");
    }

    #[test]
    fn pfcount_thousand_within_tolerance() {
        let mut s = test_store();
        let elems: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("elem-{i}").into_bytes())
            .collect();
        let mut parts: Vec<&[u8]> = vec![b"PFADD", b"big"];
        for e in &elems {
            parts.push(e);
        }
        cmd_pfadd(&mut s, 0, NOW, &req(&parts));
        let c = int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"big"])));
        // Standard error at P=14 is ~0.81%; allow a generous +/-3% window.
        assert!(
            (970..=1030).contains(&c),
            "1000 distinct estimated {c}, outside [970, 1030]"
        );
    }

    // ---- PFCOUNT multi-key union (disjoint + overlapping). ----

    #[test]
    fn pfcount_union_disjoint_and_overlapping() {
        let mut store = test_store();
        // Disjoint sets of 500 each.
        let set_a: Vec<Vec<u8>> = (0..500).map(|i| format!("a-{i}").into_bytes()).collect();
        let set_b: Vec<Vec<u8>> = (0..500).map(|i| format!("b-{i}").into_bytes()).collect();
        let mut pa: Vec<&[u8]> = vec![b"PFADD", b"k1"];
        for e in &set_a {
            pa.push(e);
        }
        cmd_pfadd(&mut store, 0, NOW, &req(&pa));
        let mut pb: Vec<&[u8]> = vec![b"PFADD", b"k2"];
        for e in &set_b {
            pb.push(e);
        }
        cmd_pfadd(&mut store, 0, NOW, &req(&pb));
        // Union of two disjoint 500s ~= 1000.
        let union = int(&cmd_pfcount(
            &mut store,
            0,
            NOW,
            &req(&[b"PFCOUNT", b"k1", b"k2"]),
        ));
        assert!(
            (950..=1050).contains(&union),
            "disjoint union estimated {union}"
        );

        // Overlapping: k3 = a-* (same 500 as k1), so the union of k1 and k3 ~= 500.
        let mut pc: Vec<&[u8]> = vec![b"PFADD", b"k3"];
        for e in &set_a {
            pc.push(e);
        }
        cmd_pfadd(&mut store, 0, NOW, &req(&pc));
        let overlap = int(&cmd_pfcount(
            &mut store,
            0,
            NOW,
            &req(&[b"PFCOUNT", b"k1", b"k3"]),
        ));
        assert!(
            (480..=520).contains(&overlap),
            "fully-overlapping union estimated {overlap} (should dedupe to ~500)"
        );
    }

    // ---- PFMERGE: dest = union; no-source creates empty HLL. ----

    #[test]
    fn pfmerge_unions_sources() {
        let mut store = test_store();
        let set_a: Vec<Vec<u8>> = (0..300).map(|i| format!("a-{i}").into_bytes()).collect();
        let set_b: Vec<Vec<u8>> = (0..300).map(|i| format!("b-{i}").into_bytes()).collect();
        let mut pa: Vec<&[u8]> = vec![b"PFADD", b"s1"];
        for e in &set_a {
            pa.push(e);
        }
        cmd_pfadd(&mut store, 0, NOW, &req(&pa));
        let mut pb: Vec<&[u8]> = vec![b"PFADD", b"s2"];
        for e in &set_b {
            pb.push(e);
        }
        cmd_pfadd(&mut store, 0, NOW, &req(&pb));
        // Merge into a fresh dest.
        let reply = cmd_pfmerge(
            &mut store,
            0,
            NOW,
            &req(&[b"PFMERGE", b"dst", b"s1", b"s2"]),
        );
        assert_eq!(reply, Value::ok());
        let count = int(&cmd_pfcount(
            &mut store,
            0,
            NOW,
            &req(&[b"PFCOUNT", b"dst"]),
        ));
        assert!(
            (570..=630).contains(&count),
            "merged union estimated {count}"
        );
        // The dest is a valid dense object.
        assert_eq!(get_bytes(&mut store, b"dst").unwrap().len(), HLL_DENSE_SIZE);
    }

    #[test]
    fn pfmerge_no_sources_creates_empty_hll() {
        let mut store = test_store();
        let reply = cmd_pfmerge(&mut store, 0, NOW, &req(&[b"PFMERGE", b"dst"]));
        assert_eq!(reply, Value::ok());
        // The dest now exists as a dense HLL with count 0.
        assert_eq!(get_bytes(&mut store, b"dst").unwrap().len(), HLL_DENSE_SIZE);
        assert_eq!(
            int(&cmd_pfcount(
                &mut store,
                0,
                NOW,
                &req(&[b"PFCOUNT", b"dst"])
            )),
            0
        );
    }

    #[test]
    fn pfmerge_includes_existing_dest_registers() {
        let mut store = test_store();
        // dst already holds set A; merging in set B unions both.
        let set_a: Vec<Vec<u8>> = (0..300).map(|i| format!("a-{i}").into_bytes()).collect();
        let set_b: Vec<Vec<u8>> = (0..300).map(|i| format!("b-{i}").into_bytes()).collect();
        let mut pa: Vec<&[u8]> = vec![b"PFADD", b"dst"];
        for e in &set_a {
            pa.push(e);
        }
        cmd_pfadd(&mut store, 0, NOW, &req(&pa));
        let mut pb: Vec<&[u8]> = vec![b"PFADD", b"src"];
        for e in &set_b {
            pb.push(e);
        }
        cmd_pfadd(&mut store, 0, NOW, &req(&pb));
        cmd_pfmerge(&mut store, 0, NOW, &req(&[b"PFMERGE", b"dst", b"src"]));
        let count = int(&cmd_pfcount(
            &mut store,
            0,
            NOW,
            &req(&[b"PFCOUNT", b"dst"]),
        ));
        assert!(
            (570..=630).contains(&count),
            "merge must include the dest's own registers, estimated {count}"
        );
    }

    // ---- Stored-bytes layout + TYPE. ----

    #[test]
    fn stored_object_layout_and_type() {
        let mut s = test_store();
        cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"hll", b"a"]));
        let bytes = get_bytes(&mut s, b"hll").unwrap();
        assert_eq!(bytes.len(), HLL_DENSE_SIZE, "exactly 12304 bytes");
        assert_eq!(&bytes[0..4], b"HYLL", "magic");
        assert_eq!(bytes[4], HLL_DENSE, "encoding byte is dense (0)");
        // The cache-invalid flag is set (we never populate the cache).
        assert_eq!(bytes[15] & 0x80, 0x80, "cache marked invalid");
        // OBJECT-level TYPE is String (an HLL is the string type).
        assert_eq!(s.type_of(0, b"hll", NOW), Some(DataType::String));
    }

    // ---- Determinism: same elements -> identical stored bytes. ----

    #[test]
    fn deterministic_stored_bytes() {
        let mut s1 = test_store();
        let mut s2 = test_store();
        let r = req(&[b"PFADD", b"hll", b"alpha", b"beta", b"gamma", b"delta"]);
        cmd_pfadd(&mut s1, 0, NOW, &r);
        cmd_pfadd(&mut s2, 0, NOW, &r);
        assert_eq!(
            get_bytes(&mut s1, b"hll"),
            get_bytes(&mut s2, b"hll"),
            "the same element set yields byte-identical HLLs (no RNG/clock)"
        );
    }

    // ---- WRONGTYPE + invalid-HLL error lines (exact bytes). ----

    #[test]
    fn wrongtype_on_non_string() {
        let mut s = test_store();
        // A LIST at "lst" is a non-string type.
        let _ = crate::cmd_list::cmd_lpush(&mut s, 0, NOW, &req(&[b"LPUSH", b"lst", b"x"]));
        let wt = "-WRONGTYPE Operation against a key holding the wrong kind of value";
        assert_eq!(
            err_line(&cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"lst", b"a"]))),
            wt
        );
        assert_eq!(
            err_line(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"lst"]))),
            wt
        );
        assert_eq!(
            err_line(&cmd_pfmerge(
                &mut s,
                0,
                NOW,
                &req(&[b"PFMERGE", b"dst", b"lst"])
            )),
            wt
        );
    }

    #[test]
    fn invalid_hll_string_error_line_is_exact() {
        let mut s = test_store();
        // A plain (non-HLL) string at "k".
        store_string(&mut s, b"k", b"foo");
        let line = "-WRONGTYPE Key is not a valid HyperLogLog string value.";
        assert_eq!(
            err_line(&cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"k", b"a"]))),
            line
        );
        assert_eq!(
            err_line(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"k"]))),
            line
        );
        // Multi-key PFCOUNT with an invalid HLL among the keys also errors.
        assert_eq!(
            err_line(&cmd_pfcount(
                &mut s,
                0,
                NOW,
                &req(&[b"PFCOUNT", b"missing", b"k"])
            )),
            line
        );
        // PFMERGE with an invalid source errors and writes no dest.
        assert_eq!(
            err_line(&cmd_pfmerge(
                &mut s,
                0,
                NOW,
                &req(&[b"PFMERGE", b"dst", b"k"])
            )),
            line
        );
        assert_eq!(get_bytes(&mut s, b"dst"), None, "no partial merge on error");
    }

    #[test]
    fn pfmerge_preserves_existing_destination_ttl() {
        use ironcache_storage::{ExpireWrite, NewValue};
        let mut s = test_store();
        // Seed the destination as a valid dense HLL carrying an absolute deadline.
        let mut dst = new_dense();
        dense_add(&mut dst, b"x");
        let deadline = UnixMillis(NOW.0 + 100_000);
        s.upsert(
            0,
            b"dst",
            NewValue::Bytes(&dst),
            ExpireWrite::Set(deadline),
            NOW,
        );
        // A source HLL with other elements.
        assert_eq!(
            int(&cmd_pfadd(
                &mut s,
                0,
                NOW,
                &req(&[b"PFADD", b"src", b"y", b"z"])
            )),
            1
        );
        // Redis edits the existing dest in place (keepTTL): the deadline must survive.
        assert_eq!(
            cmd_pfmerge(&mut s, 0, NOW, &req(&[b"PFMERGE", b"dst", b"src"])),
            Value::ok()
        );
        let ttl = s.read(0, b"dst", NOW).and_then(|v| v.expire_at());
        assert_eq!(
            ttl,
            Some(deadline),
            "PFMERGE must preserve the destination TTL"
        );
        // The merge still happened: the union of {x, y, z} estimates to 3.
        assert_eq!(
            int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"dst"]))),
            3
        );
    }

    #[test]
    fn pfmerge_into_missing_destination_has_no_ttl() {
        let mut s = test_store();
        assert_eq!(
            int(&cmd_pfadd(&mut s, 0, NOW, &req(&[b"PFADD", b"src", b"a"]))),
            1
        );
        assert_eq!(
            cmd_pfmerge(&mut s, 0, NOW, &req(&[b"PFMERGE", b"newdst", b"src"])),
            Value::ok()
        );
        let ttl = s.read(0, b"newdst", NOW).and_then(|v| v.expire_at());
        assert_eq!(
            ttl, None,
            "a freshly created PFMERGE destination has no TTL"
        );
    }

    #[test]
    fn pfcount_on_saturated_injected_hll_is_non_negative() {
        let mut s = test_store();
        // A crafted 12304-byte dense HLL whose entire register block is 0xFF (every
        // register reads 63). is_valid_dense accepts it (length + magic + encoding byte
        // only, exactly like Redis isHLLObjectOrReply, which does NOT validate register
        // contents), so it is reachable by any client via a plain SET. Every register
        // saturated drives the estimator denominator to 0 -> +inf; a naive `as i64` cast
        // would wrap that to -1. PFCOUNT must instead saturate to a large NON-NEGATIVE
        // integer, matching Redis (llroundl(+inf) -> LLONG_MAX, replied as positive).
        let mut obj = vec![0xFFu8; HLL_DENSE_SIZE];
        obj[0] = b'H';
        obj[1] = b'Y';
        obj[2] = b'L';
        obj[3] = b'L';
        obj[4] = HLL_DENSE; // the encoding byte must be 0 (dense) to pass validation
        store_string(&mut s, b"sat", &obj);
        let n = int(&cmd_pfcount(&mut s, 0, NOW, &req(&[b"PFCOUNT", b"sat"])));
        assert!(n >= 0, "PFCOUNT must never be negative, got {n}");
        assert_eq!(
            n,
            i64::MAX,
            "a saturated HLL saturates to i64::MAX like Redis"
        );
    }

    /// Store a raw string value at `key` via the store's blind upsert (a test helper to
    /// seed arbitrary byte patterns without going through a string command).
    fn store_string(store: &mut TestStore, key: &[u8], bytes: &[u8]) {
        use ironcache_storage::{ExpireWrite, NewValue};
        store.upsert(0, key, NewValue::Bytes(bytes), ExpireWrite::Clear, NOW);
    }
}
