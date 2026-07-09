// SPDX-License-Identifier: MIT OR Apache-2.0
//! The `persist-cpu` knob: which CPU core(s) the dedicated persist thread pins to (#589).
//!
//! Follow-up to the per-slot Arc-COW snapshot (#588). That fix moved a save's O(N) encode+fsync onto
//! a dedicated `ic-persist-<shard>` thread so the datapath stops paying it inline, but the thread
//! still competes for a serving core: under the thread-per-core model (ADR-0002) the datapath threads
//! are confined to a pinned cpuset, and the persist thread is an EXTRA runnable thread that the
//! scheduler places on one of those same serving cores. This knob lets an operator DEDICATE a core to
//! persistence so the encode stops stealing serving time.
//!
//! This module is PURE (no syscalls, no `unsafe`): it only parses the knob string and, given the set
//! of CPUs the process may run on, decides WHICH cpu(s) to pin to. The binary performs the actual
//! `sched_setaffinity` via the `ironcache-runtime` affinity seam. Keeping the decision here makes it
//! unit-testable on every host and lets `Config::validate` reject a malformed value at boot.
//!
//! It is a SCHEDULING concern off the engine decision path (ADR-0003 determinism is untouched: no
//! clock, no entropy, no effect on any stored value or ordering), and follows the tunability tenet
//! (env-dependent tradeoff -> a config knob with a SAFE default = [`PersistCpu::Off`] = today's
//! behavior; the operator opts into a dedicated core).

/// The resolved persist-thread CPU pin policy, parsed from the `persist-cpu` knob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistCpu {
    /// No pinning (the DEFAULT, the safe/current behavior): the `ic-persist` thread floats across the
    /// datapath cores exactly as it does today. Selected by an empty value or `off`/`none`/`disabled`.
    Off,
    /// Reserve the HIGHEST cpu id in the process's current affinity mask for persistence and pin the
    /// persist thread there. Selected by `auto`. Meaningful when the datapath is confined (via
    /// `taskset`/cpuset) to the lower cores so the top core is genuinely free; without such a
    /// confinement it at least parks the persist thread on a single deterministic core instead of
    /// letting it float across every serving core.
    Auto,
    /// Pin the persist thread to this EXPLICIT set of logical cpu ids (deduplicated, ascending). A
    /// single id (`persist-cpu = "8"`), a comma list (`"6,7"`), a range (`"6-7"`), or a mix
    /// (`"6-7,10"`). The recommended deployment: `taskset -c 0-7` the datapath and set `persist-cpu`
    /// to a core OUTSIDE that mask (the persist thread escapes onto the reserved core).
    List(Vec<usize>),
}

/// Parse the raw `persist-cpu` knob value into a [`PersistCpu`].
///
/// Accepted forms (case-insensitive for the keywords):
/// - `""` (empty), `off`, `none`, `disabled` -> [`PersistCpu::Off`] (the default).
/// - `auto` -> [`PersistCpu::Auto`].
/// - a cpu LIST -> [`PersistCpu::List`]: comma-separated tokens, each a single id (`8`) or an
///   inclusive range (`6-7`). Whitespace around tokens is tolerated; ids are deduplicated and sorted.
///
/// Note `0` is NOT a disable sentinel here (unlike some scalar knobs): it means cpu 0. Use an empty
/// value or `off` to disable.
///
/// # Errors
///
/// Returns a human-readable message (for the boot-time `ConfigError::Invalid` reason) when a list
/// token is empty, non-numeric, an inverted range (`7-6`), or the whole value parses to no cpu.
pub fn parse_persist_cpu(raw: &str) -> Result<PersistCpu, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("none")
        || trimmed.eq_ignore_ascii_case("disabled")
    {
        return Ok(PersistCpu::Off);
    }
    if trimmed.eq_ignore_ascii_case("auto") {
        return Ok(PersistCpu::Auto);
    }
    // Otherwise an explicit cpu list: comma-separated single ids or `lo-hi` ranges.
    let mut cpus: Vec<usize> = Vec::new();
    for token in trimmed.split(',') {
        let tok = token.trim();
        if tok.is_empty() {
            return Err(format!(
                "'{raw}' has an empty cpu token (expected e.g. '8', '6,7', '6-7', or 'auto'/'off')"
            ));
        }
        if let Some((lo_s, hi_s)) = tok.split_once('-') {
            let lo = parse_cpu_id(lo_s.trim(), raw)?;
            let hi = parse_cpu_id(hi_s.trim(), raw)?;
            if lo > hi {
                return Err(format!(
                    "'{raw}' has an inverted cpu range '{tok}' (low {lo} > high {hi})"
                ));
            }
            for cpu in lo..=hi {
                cpus.push(cpu);
            }
        } else {
            cpus.push(parse_cpu_id(tok, raw)?);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    if cpus.is_empty() {
        return Err(format!("'{raw}' resolves to no cpu"));
    }
    Ok(PersistCpu::List(cpus))
}

/// Parse one cpu id token as a `usize`, mapping a bad token to a boot-friendly message.
fn parse_cpu_id(tok: &str, raw: &str) -> Result<usize, String> {
    tok.parse::<usize>()
        .map_err(|_| format!("'{raw}' contains a non-numeric cpu id '{tok}'"))
}

/// Decide the CPU ids the persist thread should pin to, given the policy and the CPUs the process is
/// currently allowed to run on (`online_cpus`, e.g. from `sched_getaffinity`). Returns an EMPTY vec
/// when no pin should happen (the caller then leaves the thread unpinned).
///
/// - [`PersistCpu::Off`] -> empty (no pin).
/// - [`PersistCpu::List`] -> the explicit ids AS GIVEN (NOT intersected with `online_cpus`: a
///   reserved persist core is deliberately OUTSIDE the `taskset`-confined datapath mask, so
///   intersecting would wrongly drop it; an id the kernel ultimately rejects is handled by the pin
///   call returning an error and the thread running unpinned).
/// - [`PersistCpu::Auto`] -> the single HIGHEST id in `online_cpus` (empty if `online_cpus` is empty,
///   e.g. on a non-Linux host where the mask is unknown -> no pin).
#[must_use]
pub fn select_persist_cpus(policy: &PersistCpu, online_cpus: &[usize]) -> Vec<usize> {
    match policy {
        PersistCpu::Off => Vec::new(),
        PersistCpu::List(cpus) => cpus.clone(),
        PersistCpu::Auto => online_cpus
            .iter()
            .copied()
            .max()
            .map_or_else(Vec::new, |c| vec![c]),
    }
}

/// The cpu ids left for the DATAPATH once the persist cpu(s) are reserved out of `online_cpus`: the
/// "shards get the rest" view. Informational (the shard threads are not individually pinned today, so
/// this documents intent / feeds a boot log line and the acceptance test), not a pin instruction.
#[must_use]
pub fn datapath_cpus_excluding(online_cpus: &[usize], persist_cpus: &[usize]) -> Vec<usize> {
    online_cpus
        .iter()
        .copied()
        .filter(|c| !persist_cpus.contains(c))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_keywords_parse_to_off() {
        for s in ["", "  ", "off", "OFF", "none", "None", "disabled"] {
            assert_eq!(parse_persist_cpu(s), Ok(PersistCpu::Off), "{s:?} -> Off");
        }
    }

    #[test]
    fn auto_parses() {
        assert_eq!(parse_persist_cpu("auto"), Ok(PersistCpu::Auto));
        assert_eq!(parse_persist_cpu("  AuTo "), Ok(PersistCpu::Auto));
    }

    #[test]
    fn single_id_and_lists_and_ranges_parse() {
        assert_eq!(parse_persist_cpu("8"), Ok(PersistCpu::List(vec![8])));
        assert_eq!(parse_persist_cpu("6,7"), Ok(PersistCpu::List(vec![6, 7])));
        assert_eq!(parse_persist_cpu("6-7"), Ok(PersistCpu::List(vec![6, 7])));
        assert_eq!(
            parse_persist_cpu("10-8"),
            Err("'10-8' has an inverted cpu range '10-8' (low 10 > high 8)".to_owned())
        );
        // A mix, with dedup + sort + tolerated whitespace.
        assert_eq!(
            parse_persist_cpu(" 6 - 7 , 10 , 6 "),
            Ok(PersistCpu::List(vec![6, 7, 10]))
        );
    }

    #[test]
    fn malformed_lists_are_rejected() {
        assert!(parse_persist_cpu("x").is_err());
        assert!(parse_persist_cpu("1,,2").is_err());
        assert!(parse_persist_cpu("-1").is_err());
        assert!(parse_persist_cpu("1-").is_err());
        assert!(parse_persist_cpu("1,two").is_err());
    }

    #[test]
    fn select_off_and_list() {
        assert_eq!(
            select_persist_cpus(&PersistCpu::Off, &[0, 1, 2, 3]),
            Vec::<usize>::new()
        );
        // A List is used AS GIVEN even when the id is OUTSIDE the current online mask (a reserved
        // core outside the taskset-confined datapath set): the pin call is what escapes onto it.
        assert_eq!(
            select_persist_cpus(&PersistCpu::List(vec![8]), &[0, 1, 2, 3]),
            vec![8]
        );
    }

    /// The #589 acceptance for the core-selection logic: `auto` reserves the LAST core, the shards
    /// get the rest. On an 8-cpu box (`0..=7`), auto pins persistence to cpu 7 and leaves `0..=6` for
    /// the datapath.
    #[test]
    fn auto_reserves_the_last_core_shards_get_the_rest() {
        let online: Vec<usize> = (0..8).collect();
        let persist = select_persist_cpus(&PersistCpu::Auto, &online);
        assert_eq!(persist, vec![7], "auto reserves the highest core");
        let datapath = datapath_cpus_excluding(&online, &persist);
        assert_eq!(datapath, vec![0, 1, 2, 3, 4, 5, 6], "shards get the rest");
    }

    #[test]
    fn auto_with_unknown_mask_makes_no_pin() {
        // Non-Linux / query-failed: the mask is empty, so auto selects nothing (graceful no-op).
        assert_eq!(
            select_persist_cpus(&PersistCpu::Auto, &[]),
            Vec::<usize>::new()
        );
    }
}
