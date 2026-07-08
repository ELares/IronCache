// SPDX-License-Identifier: MIT OR Apache-2.0
//! Mechanism check for the #512 transparent-huge-page lever (ADR-0006,
//! docs/design/CONFIG.md "Transparent huge pages").
//!
//! The binary turns huge pages on by exporting `thp:always` in jemalloc's
//! `malloc_conf` (see the `_rjem_malloc_conf` static in `main.rs`), so jemalloc
//! backs its extents (the per-shard store tables + the value blobs) with 2 MiB
//! transparent huge pages via `madvise(MADV_HUGEPAGE)`. This test proves that the
//! seam actually WORKS: with `thp:always` in `malloc_conf`, jemalloc parses and
//! HONORS the option, reported by the read-only `opt.thp` mallctl.
//!
//! It installs jemalloc as THIS test binary's own `#[global_allocator]` and exports
//! its OWN `_rjem_malloc_conf` carrying `thp:always` (on Linux only), mirroring the
//! `ironcache` binary, because the binary's `main.rs` static is not linked into an
//! integration-test binary (which links the library half). The `thp:` token is
//! emitted ONLY on Linux, exactly like the binary, since jemalloc compiles THP
//! support on Linux and nowhere else; on macOS/other targets the option does not
//! exist, so the meaningful body is Linux-gated and the file is otherwise inert.
//!
//! Gated to non-MSVC (jemalloc is unavailable there) and out of miri (which cannot
//! execute jemalloc's foreign allocator / `mallctl` C functions).

#![cfg(all(not(target_env = "msvc"), not(miri)))]

// jemalloc as this test binary's global allocator so the `opt.thp` mallctl is live.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// This binary's `malloc_conf`, mirroring the `ironcache` binary's static with the
// #512 huge-page directive baked in on Linux. jemalloc reads this pointer at init.
// On non-Linux there is no THP support in jemalloc, so the string stays THP-free
// (emitting `thp:` there would draw an "Invalid conf pair" warning).
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    &*c"background_thread:true,dirty_decay_ms:5000,thp:always,metadata_thp:auto"
        .as_ptr()
        .cast::<libc::c_char>()
});

// On Linux, assert jemalloc HONORED `thp:always`: the read-only `opt.thp` mallctl
// reports the boot value jemalloc parsed from `malloc_conf`. This proves the option
// is applied (the malloc_conf seam is live); whether the kernel then collapses the
// mappings into physical huge pages is a separate, kernel-dependent smaps check that
// is not asserted here (it is `never` on a host with THP disabled system-wide, which
// would make such an assertion flaky in CI/containers).
#[cfg(target_os = "linux")]
#[test]
fn jemalloc_honors_thp_always_from_malloc_conf() {
    // `read_str` returns the value bytes INCLUDING the trailing NUL terminator.
    let raw = unsafe { tikv_jemalloc_ctl::raw::read_str(b"opt.thp\0") }
        .expect("opt.thp mallctl is available on a Linux jemalloc build");
    let value = std::str::from_utf8(raw.strip_suffix(b"\0").unwrap_or(raw))
        .expect("opt.thp is valid UTF-8");
    assert_eq!(
        value, "always",
        "jemalloc must honor thp:always from malloc_conf (got {value:?})"
    );
}

// PROBE (ignored by default): the direct #512 acceptance check that a large jemalloc
// allocation under `thp:always` is REALLY backed by 2 MiB huge pages, read from
// `/proc/self/smaps` (AnonHugePages). It is `#[ignore]`d because the outcome depends
// on the host kernel's THP mode: `always`/`madvise` back it, `never` yields zero, so
// it is a manual verification aid rather than a CI assertion (CI/containers may run
// THP-disabled). Run it explicitly on a THP-capable Linux host with:
//
//   cargo test -p ironcache --test hugepages -- --ignored --nocapture
#[cfg(target_os = "linux")]
#[test]
#[ignore = "manual smaps probe; huge-page population depends on the host THP mode"]
fn probe_anon_huge_pages_back_a_large_allocation() {
    // A large allocation goes through jemalloc's large-extent path, which under
    // thp:always is madvised MADV_HUGEPAGE. Fault in every 4 KiB page so the mapping
    // is resident and the kernel can collapse it to 2 MiB huge pages.
    let len = 256 * 1024 * 1024usize;
    let mut buf = vec![0u8; len];
    let mut i = 0;
    while i < len {
        buf[i] = 1;
        i += 4096;
    }
    std::hint::black_box(&buf);

    // Sum AnonHugePages (kB) across the process maps.
    let smaps = std::fs::read_to_string("/proc/self/smaps").expect("read /proc/self/smaps");
    let total_kb: u64 = smaps
        .lines()
        .filter_map(|l| l.strip_prefix("AnonHugePages:"))
        .filter_map(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .sum();
    println!(
        "AnonHugePages total: {total_kb} kB after a {} MiB thp:always allocation",
        len / (1024 * 1024)
    );
    assert!(
        total_kb > 0,
        "expected AnonHugePages > 0 under thp:always on a THP-capable kernel \
         (host THP mode may be 'never'; this probe is #[ignore]d for that reason)"
    );
}

// On non-Linux targets the file must still compile and the crate's test run must not
// be empty for this binary; jemalloc has no THP there, so there is nothing to assert
// beyond that the huge-page lever is correctly inert.
#[cfg(not(target_os = "linux"))]
#[test]
fn transparent_huge_pages_are_inert_off_linux() {
    // No `opt.thp` mallctl exists off Linux; the binary emits a THP-free malloc_conf
    // (asserted by the main.rs unit test). Nothing to do here but confirm the build.
}
