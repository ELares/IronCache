// SPDX-License-Identifier: MIT OR Apache-2.0
//! Structured tracing setup (issue #353). One stderr `fmt` layer at the
//! requested level. Mirrors the engine's `install_tracing` so console and
//! engine logs read the same in an orchestrator.

/// Install the global tracing subscriber at the level parsed from `log_level`.
/// Best-effort: a second install in the same process (a re-entrant test harness
/// that already set a global subscriber) is ignored rather than panicking.
pub fn install_tracing(log_level: &str) {
    use tracing_subscriber::fmt;

    let (level, unknown) = parse_log_level(log_level);
    let subscriber = fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_target(true)
        .finish();
    if tracing::subscriber::set_global_default(subscriber).is_ok() && unknown {
        tracing::warn!(
            requested = log_level,
            "unknown --log-level; defaulting to info"
        );
    }
}

/// Map a `--log-level` string to a `LevelFilter` (case-insensitive). Returns the
/// filter and a `bool` that is `true` when the input was UNRECOGNIZED (the caller
/// falls back to `info` and logs a note). Pure, so it is unit-tested without
/// installing a global subscriber.
pub fn parse_log_level(log_level: &str) -> (tracing::level_filters::LevelFilter, bool) {
    use tracing::level_filters::LevelFilter;
    match log_level.to_ascii_lowercase().as_str() {
        "error" => (LevelFilter::ERROR, false),
        "warn" | "warning" => (LevelFilter::WARN, false),
        "info" => (LevelFilter::INFO, false),
        "debug" | "verbose" => (LevelFilter::DEBUG, false),
        "trace" => (LevelFilter::TRACE, false),
        _ => (LevelFilter::INFO, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::level_filters::LevelFilter;

    #[test]
    fn parse_log_level_maps_the_vocabulary() {
        assert_eq!(parse_log_level("error"), (LevelFilter::ERROR, false));
        assert_eq!(parse_log_level("warn"), (LevelFilter::WARN, false));
        assert_eq!(parse_log_level("warning"), (LevelFilter::WARN, false));
        assert_eq!(parse_log_level("info"), (LevelFilter::INFO, false));
        assert_eq!(parse_log_level("debug"), (LevelFilter::DEBUG, false));
        assert_eq!(parse_log_level("trace"), (LevelFilter::TRACE, false));
        // Case-insensitive.
        assert_eq!(parse_log_level("INFO"), (LevelFilter::INFO, false));
        assert_eq!(parse_log_level("Debug"), (LevelFilter::DEBUG, false));
    }

    #[test]
    fn parse_log_level_unknown_falls_back_to_info() {
        let (level, unknown) = parse_log_level("loud");
        assert_eq!(level, LevelFilter::INFO);
        assert!(unknown);
    }
}
