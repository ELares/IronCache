// SPDX-License-Identifier: MIT OR Apache-2.0
//! The systemd socket-activation protocol parser (`sd_listen_fds`, #389 Phase 2a).
//!
//! When systemd starts a service through a matching `.socket` unit it opens the listening
//! socket(s) ITSELF, then passes them into the service process as inherited file descriptors and
//! describes them with three environment variables (`sd_listen_fds(3)` / `systemd.socket`):
//!
//! - `LISTEN_PID`   the pid the fds were passed to. It MUST equal this process's pid; otherwise the
//!   fds are meant for a different process (e.g. a leftover in a re-exec'd child's environment) and
//!   must NOT be adopted.
//! - `LISTEN_FDS`   the COUNT of inherited fds. They are numbered consecutively upward from
//!   [`SD_LISTEN_FDS_START`] (= 3), so `LISTEN_FDS=3` means fds 3, 4, 5.
//! - `LISTEN_FDNAMES`   an OPTIONAL `:`-separated list of names, one per fd (from the unit's
//!   `FileDescriptorName=`), so a service can tell which socket is which (e.g. `resp` vs `repl`).
//!   When present its entry count must equal `LISTEN_FDS`.
//!
//! This module is the PURE gate that turns those strings into a typed list of inherited fds (or a
//! typed rejection). The actual `TcpListener::from_raw_fd` adoption is a thin, Linux-only layer
//! DOWNSTREAM of this parse (a bare fd integer is only a real listening socket when systemd actually
//! inherited one, which cannot be reproduced off a live systemd host). Keeping the parse pure --
//! [`parse_listen_fds`] takes `self_pid` as a PARAMETER rather than calling `getpid()` -- makes the
//! dangerous decisions deterministic and fully unit-testable on any host: adopting an fd meant for a
//! DIFFERENT pid, an off-by-one on the fd numbering, or a name->fd mis-map that would bind the RESP
//! listener to the replication socket all live entirely in this string->typed-result logic.

use std::os::fd::RawFd;
use std::sync::OnceLock;

/// The first inherited file descriptor systemd uses (`SD_LISTEN_FDS_START`). Inherited fds are
/// numbered consecutively upward from here (stdin/stdout/stderr are 0/1/2).
pub const SD_LISTEN_FDS_START: RawFd = 3;

/// The largest `LISTEN_FDS` count accepted. A real service is passed a handful of sockets; a count
/// beyond this is a malformed/hostile environment, rejected as [`ListenFdsError::MalformedCount`]
/// rather than allocating a huge vector (fail-closed, bounded).
const MAX_LISTEN_FDS: usize = 1024;

/// One inherited listening socket: its file descriptor plus the optional name systemd gave it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InheritedFd {
    /// The raw file descriptor (>= [`SD_LISTEN_FDS_START`]).
    pub fd: RawFd,
    /// The socket's name from `LISTEN_FDNAMES`, or `None` when the environment named no fds.
    pub name: Option<String>,
}

/// A typed reason the socket-activation environment was rejected. On ANY of these the process must
/// NOT adopt an inherited fd and should fall back to binding the socket itself (fail-closed): a
/// malformed or foreign environment is never trusted with a live listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenFdsError {
    /// `LISTEN_FDS` was set without `LISTEN_PID`; adopting fds without a pid match could steal
    /// another process's sockets.
    MissingPid,
    /// `LISTEN_PID` was not a base-10 `u32`.
    MalformedPid(String),
    /// `LISTEN_PID` named a DIFFERENT process, so the fds are not ours to adopt.
    PidMismatch {
        /// The pid the environment says the fds belong to.
        listen_pid: u32,
        /// This process's pid.
        self_pid: u32,
    },
    /// `LISTEN_FDS` was not a base-10 count in `0..=MAX_LISTEN_FDS` (non-numeric, signed, or too
    /// large).
    MalformedCount(String),
    /// `LISTEN_FDNAMES` was present but its entry count did not match `LISTEN_FDS`, so the name->fd
    /// mapping is ambiguous.
    NameCountMismatch {
        /// The `LISTEN_FDS` count.
        fds: usize,
        /// The number of `:`-separated `LISTEN_FDNAMES` entries.
        names: usize,
    },
}

impl std::fmt::Display for ListenFdsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListenFdsError::MissingPid => {
                write!(f, "LISTEN_FDS is set but LISTEN_PID is missing")
            }
            ListenFdsError::MalformedPid(s) => write!(f, "LISTEN_PID is not a valid pid: {s:?}"),
            ListenFdsError::PidMismatch {
                listen_pid,
                self_pid,
            } => write!(
                f,
                "LISTEN_PID {listen_pid} is not this process ({self_pid}); the fds are not ours"
            ),
            ListenFdsError::MalformedCount(s) => {
                write!(f, "LISTEN_FDS is not a valid count: {s:?}")
            }
            ListenFdsError::NameCountMismatch { fds, names } => write!(
                f,
                "LISTEN_FDNAMES has {names} entries but LISTEN_FDS is {fds}"
            ),
        }
    }
}

impl std::error::Error for ListenFdsError {}

/// Whether the boot ADOPTED the systemd-passed listening fds (socket activation) or FELL BACK to
/// self-binding its own listener, plus WHY. This is the classification behind the loud
/// operator-facing boot log (#562): without it an operator cannot tell from the logs which listener
/// path a socket-activated upgrade took, which is exactly what is needed to debug a failed one.
///
/// Derived PURELY from the parsed [`from_env`] result (no clock/rand, ADR-0003, boot/OS seam), so the
/// exact log line the binary emits is unit-testable off a live systemd host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Activation {
    /// Socket-activated: ADOPT the inherited listening fds systemd passed. The listen queue then
    /// survives an upgrade restart (systemd keeps it open), so clients queue in the kernel backlog
    /// instead of getting `ECONNREFUSED`.
    Adopted(Vec<InheritedFd>),
    /// FALL BACK to self-binding our own listener; `reason` records why.
    SelfBound(SelfBindReason),
}

/// Why the boot self-bound its own listener instead of adopting socket-activation fds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelfBindReason {
    /// Not socket-activated: `LISTEN_FDS` was unset (the normal, non-systemd launch) or zero
    /// (activation opened no sockets). There is nothing to adopt; self-binding is correct and the
    /// boot is byte-unchanged.
    NotActivated,
    /// A `LISTEN_*` environment WAS present but was rejected (a foreign/missing `LISTEN_PID`, a
    /// malformed count, or a `LISTEN_FDNAMES` mismatch), so the fds are not safely ours to adopt.
    /// Carries the typed reason so the log names it.
    Rejected(ListenFdsError),
}

/// Classify the parsed socket-activation environment into the boot's listener decision (#562): adopt
/// the inherited fds, or self-bind (and why). Pure over [`from_env`]'s result -- the SAME result
/// [`crate::tokio_rt::listener_for`] acts on, so the logged decision matches the one the runtime
/// takes.
#[must_use]
pub fn classify(parsed: &Result<Vec<InheritedFd>, ListenFdsError>) -> Activation {
    match parsed {
        Ok(fds) if !fds.is_empty() => Activation::Adopted(fds.clone()),
        Ok(_) => Activation::SelfBound(SelfBindReason::NotActivated),
        Err(e) => Activation::SelfBound(SelfBindReason::Rejected(e.clone())),
    }
}

impl Activation {
    /// The loud, operator-facing one-line boot summary (#562): exactly which listener path the boot
    /// took and why, so a failed socket-activated upgrade is diagnosable from the logs alone. Each
    /// variant carries a distinct marker (`ADOPTED` vs `FELL BACK`) the binary emits through
    /// `tracing`; this crate is the pure runtime seam and takes no logging dependency itself.
    #[must_use]
    pub fn boot_summary(&self) -> String {
        match self {
            Activation::Adopted(fds) => {
                // Name each fd via LISTEN_FDNAMES when systemd supplied names (`resp=fd3`), else the
                // bare number (`fd3`), so the operator can see which socket is which.
                let named = fds
                    .iter()
                    .map(|f| match &f.name {
                        Some(name) => format!("{name}=fd{}", f.fd),
                        None => format!("fd{}", f.fd),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "ADOPTED {} systemd socket-activation listening fd(s) [{named}]; systemd owns \
                     the listen queue, so it survives an upgrade restart with no connection-refused \
                     window",
                    fds.len()
                )
            }
            Activation::SelfBound(SelfBindReason::NotActivated) => {
                "FELL BACK to self-binding its own listener: not socket-activated (no LISTEN_FDS in \
                 the environment)"
                    .to_owned()
            }
            Activation::SelfBound(SelfBindReason::Rejected(reason)) => format!(
                "FELL BACK to self-binding its own listener: the socket-activation environment was \
                 REJECTED and not adopted ({reason})"
            ),
        }
    }
}

/// Parse the socket-activation environment into the inherited listening fds, or a typed rejection.
///
/// `self_pid` is THIS process's pid (a parameter, so the parse is pure + deterministic). Returns
/// `Ok(empty)` when `LISTEN_FDS` is absent (the normal, not-socket-activated path) and when the
/// count is zero.
///
/// CALLER CONTRACT: an `Err` means "do NOT adopt an inherited fd" -- the caller must FALL BACK to
/// binding the socket itself, NOT abort startup. Real `sd_listen_fds(3)` silently returns 0 (no
/// error) for a missing or foreign `LISTEN_PID`, because a stray `LISTEN_PID` leaked into a normally
/// launched process is benign; this function surfaces that as a typed `Err` instead so the reason is
/// visible in a log, but the correct reaction is identical: self-bind and continue. Treating these
/// errors as fatal would turn a harmless environment quirk into a spurious crash.
///
/// # Errors
///
/// Returns a [`ListenFdsError`] for a missing/foreign `LISTEN_PID`, a malformed count, or a
/// `LISTEN_FDNAMES` whose entry count does not match `LISTEN_FDS`.
pub fn parse_listen_fds(
    listen_pid: Option<&str>,
    listen_fds: Option<&str>,
    listen_fdnames: Option<&str>,
    self_pid: u32,
) -> Result<Vec<InheritedFd>, ListenFdsError> {
    // Not socket-activated: no LISTEN_FDS means no inherited fds (the common case). A present
    // LISTEN_FDNAMES with no LISTEN_FDS is ignored (nothing to name).
    let Some(count_str) = listen_fds else {
        return Ok(Vec::new());
    };

    // A present LISTEN_FDS REQUIRES a matching LISTEN_PID, or we could adopt another process's fds.
    let pid_str = listen_pid.ok_or(ListenFdsError::MissingPid)?;
    let listen_pid: u32 = pid_str
        .parse()
        .map_err(|_| ListenFdsError::MalformedPid(pid_str.to_owned()))?;
    if listen_pid != self_pid {
        return Err(ListenFdsError::PidMismatch {
            listen_pid,
            self_pid,
        });
    }

    let count = parse_count(count_str)
        .ok_or_else(|| ListenFdsError::MalformedCount(count_str.to_owned()))?;

    // The optional per-fd names. When present there must be exactly one per fd, so the name->fd map
    // is unambiguous. An empty LISTEN_FDNAMES is zero entries (valid only when the count is zero).
    let names: Option<Vec<&str>> = match listen_fdnames {
        Some(s) => {
            let parts: Vec<&str> = if s.is_empty() {
                Vec::new()
            } else {
                s.split(':').collect()
            };
            if parts.len() != count {
                return Err(ListenFdsError::NameCountMismatch {
                    fds: count,
                    names: parts.len(),
                });
            }
            Some(parts)
        }
        None => None,
    };

    // Number the fds consecutively from SD_LISTEN_FDS_START. `count <= MAX_LISTEN_FDS` (well under
    // RawFd::MAX), so the fd arithmetic cannot overflow.
    let fds = (0..count)
        .map(|i| InheritedFd {
            fd: SD_LISTEN_FDS_START + i as RawFd,
            name: names.as_ref().map(|n| n[i].to_owned()),
        })
        .collect();
    Ok(fds)
}

/// Parse a `LISTEN_FDS` count: a base-10 non-negative integer within `0..=MAX_LISTEN_FDS`. Rejects
/// an empty string, a sign, non-digits, and an over-large count (so a bogus value is a clean
/// rejection, not a giant allocation).
fn parse_count(s: &str) -> Option<usize> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: usize = s.parse().ok()?;
    (n <= MAX_LISTEN_FDS).then_some(n)
}

/// The inherited fd with the given `name` (the socket the caller wants, e.g. `resp`), or `None` when
/// the environment was not socket-activated, named no fds, or has no fd with that name. The first
/// match wins (a unit's `FileDescriptorName=` values are conventionally unique).
#[must_use]
pub fn select_named<'a>(fds: &'a [InheritedFd], name: &str) -> Option<&'a InheritedFd> {
    fds.iter().find(|f| f.name.as_deref() == Some(name))
}

/// The inherited fd to adopt as the RESP CLIENT listener (#389): the fd NAMED `resp` (from a unit's
/// `FileDescriptorName=resp`) when `LISTEN_FDNAMES` disambiguates a MULTI-socket activation, else the
/// FIRST inherited fd (`SD_LISTEN_FDS_START` = 3, the single-socket default). Returns `None` only for
/// an empty list (not socket-activated). Using the NAME rather than blindly taking fd 3 is what keeps
/// a future multi-socket unit from binding the RESP listener to the wrong socket (the replication /
/// cluster-bus fd): that second socket would be a distinct fd named e.g. `repl`, and ADOPTING it is a
/// deliberate follow-up -- this PR scopes socket activation to the CLIENT listener.
#[must_use]
pub fn resp_listener_fd(fds: &[InheritedFd]) -> Option<&InheritedFd> {
    select_named(fds, "resp").or_else(|| fds.first())
}

/// The socket-activation environment captured ONCE by [`prime_from_env_and_unset`] at boot, BEFORE
/// the `LISTEN_*` vars were removed. When set, [`from_env`] returns THIS snapshot instead of
/// re-reading the (now-cleared) environment, so every boot consumer -- the loud adopt-vs-fallback log
/// (#562), the RESP listener adoption ([`crate::tokio_rt::listener_for`]), and the shard-owner guard
/// -- sees the SAME activation decision even though the env was cleared after the first read.
static PRIMED: OnceLock<Result<Vec<InheritedFd>, ListenFdsError>> = OnceLock::new();

/// Parse THIS process's live `LISTEN_*` environment. The raw read behind both [`from_env`] (its
/// not-yet-primed path) and [`prime_from_env_and_unset`] (the one-shot capture).
fn read_env() -> Result<Vec<InheritedFd>, ListenFdsError> {
    let self_pid = std::process::id();
    let listen_pid = std::env::var("LISTEN_PID").ok();
    let listen_fds = std::env::var("LISTEN_FDS").ok();
    let listen_fdnames = std::env::var("LISTEN_FDNAMES").ok();
    parse_listen_fds(
        listen_pid.as_deref(),
        listen_fds.as_deref(),
        listen_fdnames.as_deref(),
        self_pid,
    )
}

/// The socket-activation environment for THIS process, parsed. Returns the [`prime_from_env_and_unset`]
/// snapshot once the binary has primed it (the boot path, so every consumer agrees AND a later read
/// survives the env being unset); otherwise reads the live `LISTEN_*` vars directly (the un-primed
/// path -- unit/integration tests, which set the env themselves and never prime). Returns `Ok(empty)`
/// when not socket-activated (so a normal, non-systemd launch takes the self-bind path).
///
/// # Errors
///
/// Propagates [`parse_listen_fds`]'s errors.
pub fn from_env() -> Result<Vec<InheritedFd>, ListenFdsError> {
    if let Some(primed) = PRIMED.get() {
        return primed.clone();
    }
    read_env()
}

/// Capture the socket-activation environment ONCE, then UNSET `LISTEN_PID` / `LISTEN_FDS` /
/// `LISTEN_FDNAMES` -- the `sd_listen_fds(3)` `unset_environment` convention (#389). Clearing the vars
/// after the single authoritative read means a later `exec`'d child or a subprocess does NOT inherit
/// this process's activation state and try to RE-ADOPT fds meant for THIS pid. (The `LISTEN_PID` check
/// in [`parse_listen_fds`] already fail-closes a foreign pid, so this is belt-and-suspenders; it also
/// keeps a leaked `LISTEN_FDS` from confusing a child that happens to share our pid namespace.) The
/// snapshot is stored FIRST, so every later [`from_env`] caller still sees the activation decision.
///
/// CALL EXACTLY ONCE at the very top of the server boot, before any listener binds. Idempotent: a
/// second call is a no-op (the snapshot is already set).
///
/// # Safety / threading
///
/// This mutates the process environment, which is only sound with no concurrent environ access. The
/// binary calls this as the FIRST action of `cmd_server`, before ANY IronCache thread (the acceptor,
/// the shards, the metrics server) is spawned; the sole other live thread is jemalloc's background
/// purge thread, which never reads or writes the process environment. So there is no reader to race
/// this unset.
pub fn prime_from_env_and_unset() {
    let _ = PRIMED.set(read_env());
    // SAFETY: see the "Safety / threading" note above -- called once at the top of `cmd_server`
    // before any IronCache thread exists, and jemalloc's purge thread does not touch the environment,
    // so no other thread reads or writes `environ` concurrently with this removal.
    unsafe {
        std::env::remove_var("LISTEN_PID");
        std::env::remove_var("LISTEN_FDS");
        std::env::remove_var("LISTEN_FDNAMES");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Activation, InheritedFd, ListenFdsError, SD_LISTEN_FDS_START, SelfBindReason, classify,
        parse_listen_fds, resp_listener_fd, select_named,
    };

    const PID: u32 = 4242;

    fn ok(pid: &str, fds: &str, names: Option<&str>) -> Vec<InheritedFd> {
        parse_listen_fds(Some(pid), Some(fds), names, PID).expect("should parse")
    }

    #[test]
    fn not_socket_activated_yields_no_fds() {
        // The normal launch: none of the vars set -> empty, no error (fall back to self-bind).
        assert_eq!(parse_listen_fds(None, None, None, PID), Ok(vec![]));
        // A stray LISTEN_PID / LISTEN_FDNAMES without LISTEN_FDS is ignored (nothing to adopt).
        assert_eq!(
            parse_listen_fds(Some("4242"), None, Some("resp"), PID),
            Ok(vec![])
        );
    }

    #[test]
    fn one_fd_numbers_from_sd_listen_fds_start() {
        assert_eq!(
            ok("4242", "1", None),
            vec![InheritedFd {
                fd: SD_LISTEN_FDS_START,
                name: None
            }]
        );
        assert_eq!(SD_LISTEN_FDS_START, 3);
    }

    #[test]
    fn multiple_fds_are_consecutive_from_3() {
        let got = ok("4242", "3", None);
        assert_eq!(got.iter().map(|f| f.fd).collect::<Vec<_>>(), vec![3, 4, 5]);
        assert!(got.iter().all(|f| f.name.is_none()));
    }

    #[test]
    fn fdnames_map_one_per_fd_in_order() {
        let got = ok("4242", "3", Some("resp:repl:metrics"));
        assert_eq!(
            got,
            vec![
                InheritedFd {
                    fd: 3,
                    name: Some("resp".to_owned())
                },
                InheritedFd {
                    fd: 4,
                    name: Some("repl".to_owned())
                },
                InheritedFd {
                    fd: 5,
                    name: Some("metrics".to_owned())
                },
            ]
        );
        // The name-selection helper picks the RIGHT fd (the mis-map that would bind RESP to the
        // replication socket is exactly the bug this guards).
        assert_eq!(select_named(&got, "resp").map(|f| f.fd), Some(3));
        assert_eq!(select_named(&got, "repl").map(|f| f.fd), Some(4));
        assert_eq!(select_named(&got, "metrics").map(|f| f.fd), Some(5));
        assert_eq!(select_named(&got, "nope"), None);
    }

    #[test]
    fn zero_count_is_empty_not_an_error() {
        assert_eq!(
            parse_listen_fds(Some("4242"), Some("0"), None, PID),
            Ok(vec![])
        );
        // Zero fds with an empty LISTEN_FDNAMES is consistent (0 names == 0 fds).
        assert_eq!(
            parse_listen_fds(Some("4242"), Some("0"), Some(""), PID),
            Ok(vec![])
        );
    }

    #[test]
    fn foreign_or_missing_pid_is_rejected_not_adopted() {
        // The fds belong to a DIFFERENT pid: never adopt them.
        assert_eq!(
            parse_listen_fds(Some("9999"), Some("1"), None, PID),
            Err(ListenFdsError::PidMismatch {
                listen_pid: 9999,
                self_pid: PID
            })
        );
        // LISTEN_FDS present but no LISTEN_PID: fail closed.
        assert_eq!(
            parse_listen_fds(None, Some("1"), None, PID),
            Err(ListenFdsError::MissingPid)
        );
        // A non-numeric pid is malformed.
        assert_eq!(
            parse_listen_fds(Some("nan"), Some("1"), None, PID),
            Err(ListenFdsError::MalformedPid("nan".to_owned()))
        );
    }

    #[test]
    fn malformed_count_is_rejected() {
        for bad in [
            "x", "-1", "+2", "1.5", "", "99999", /* > MAX_LISTEN_FDS */
        ] {
            assert!(
                matches!(
                    parse_listen_fds(Some("4242"), Some(bad), None, PID),
                    Err(ListenFdsError::MalformedCount(_))
                ),
                "LISTEN_FDS={bad:?} must be MalformedCount"
            );
        }
    }

    #[test]
    fn fdnames_count_must_match_fds_count() {
        // Too few names.
        assert_eq!(
            parse_listen_fds(Some("4242"), Some("3"), Some("resp:repl"), PID),
            Err(ListenFdsError::NameCountMismatch { fds: 3, names: 2 })
        );
        // Too many names.
        assert_eq!(
            parse_listen_fds(Some("4242"), Some("1"), Some("resp:repl"), PID),
            Err(ListenFdsError::NameCountMismatch { fds: 1, names: 2 })
        );
    }

    #[test]
    fn empty_name_entry_is_a_named_but_empty_fd() {
        // systemd's split is NON-coalescing, so "resp::metrics" is THREE names (the middle one
        // empty), one per fd -- not two. This pins that fidelity behavior.
        let got = ok("4242", "3", Some("resp::metrics"));
        assert_eq!(
            got.iter().map(|f| f.name.as_deref()).collect::<Vec<_>>(),
            vec![Some("resp"), Some(""), Some("metrics")]
        );
    }

    #[test]
    fn resp_listener_fd_prefers_the_named_resp_fd_else_the_first() {
        // Unnamed single-socket activation (the packaged default, no LISTEN_FDNAMES): the FIRST fd
        // (fd 3, SD_LISTEN_FDS_START) is the RESP client listener -- byte-identical to blindly taking
        // fds[0].
        assert_eq!(
            resp_listener_fd(&ok("4242", "1", None)).map(|f| f.fd),
            Some(3)
        );
        // Named MULTI-socket: pick the fd NAMED `resp` even when it is NOT first. This is the mis-map
        // guard -- without it, a `repl:resp` unit would bind the RESP listener to fd 3 (the repl
        // socket) instead of fd 4.
        assert_eq!(
            resp_listener_fd(&ok("4242", "2", Some("repl:resp"))).map(|f| f.fd),
            Some(4)
        );
        // Names present but none is `resp`: fall back to the first fd.
        assert_eq!(
            resp_listener_fd(&ok("4242", "2", Some("a:b"))).map(|f| f.fd),
            Some(3)
        );
        // Not socket-activated: nothing to adopt.
        assert_eq!(resp_listener_fd(&[]), None);
    }

    #[test]
    fn select_named_returns_the_first_match_on_duplicates() {
        let got = ok("4242", "2", Some("dup:dup"));
        // Documented "first match wins": fd 3, not fd 4.
        assert_eq!(select_named(&got, "dup").map(|f| f.fd), Some(3));
    }

    #[test]
    fn leading_zero_count_parses_as_decimal() {
        // "007" is all-digits and parses as 7 (matching systemd's decimal read), yielding 7 fds.
        assert_eq!(ok("4242", "007", None).len(), 7);
    }

    // --- #562: the loud adopt-vs-fallback classification the boot logs. The ADOPT branch and BOTH
    //     fall-back branches must be distinct in the enum AND carry a distinct log marker.

    #[test]
    fn classify_adopt_branch_names_the_fds() {
        // A valid socket-activation env parses to non-empty fds -> ADOPT, carrying the fds/names.
        let parsed = parse_listen_fds(Some("4242"), Some("3"), Some("resp:repl:metrics"), PID);
        let activation = classify(&parsed);
        assert_eq!(
            activation,
            Activation::Adopted(vec![
                InheritedFd {
                    fd: 3,
                    name: Some("resp".to_owned())
                },
                InheritedFd {
                    fd: 4,
                    name: Some("repl".to_owned())
                },
                InheritedFd {
                    fd: 5,
                    name: Some("metrics".to_owned())
                },
            ])
        );
        // The log marker distinguishes this branch and names each socket (LISTEN_FDNAMES).
        let summary = activation.boot_summary();
        assert!(
            summary.contains("ADOPTED 3"),
            "adopt marker + count: {summary:?}"
        );
        assert!(
            summary.contains("resp=fd3"),
            "names the RESP fd: {summary:?}"
        );
        assert!(
            summary.contains("repl=fd4"),
            "names the repl fd: {summary:?}"
        );
        assert!(
            !summary.contains("FELL BACK"),
            "adopt must not claim a fallback: {summary:?}"
        );
    }

    #[test]
    fn classify_fallback_not_activated_branch() {
        // No LISTEN_FDS -> the normal launch -> self-bind, reason NotActivated.
        let parsed = parse_listen_fds(None, None, None, PID);
        let activation = classify(&parsed);
        assert_eq!(
            activation,
            Activation::SelfBound(SelfBindReason::NotActivated)
        );
        let summary = activation.boot_summary();
        assert!(
            summary.contains("FELL BACK"),
            "fallback marker: {summary:?}"
        );
        assert!(
            summary.contains("no LISTEN_FDS"),
            "states WHY (not activated): {summary:?}"
        );
        assert!(
            !summary.contains("ADOPTED"),
            "fallback must not claim an adopt: {summary:?}"
        );
    }

    #[test]
    fn classify_fallback_rejected_branch_names_the_reason() {
        // A foreign LISTEN_PID -> rejected -> self-bind, reason Rejected(PidMismatch), and the log
        // names the mismatch so a failed socket-activated upgrade is diagnosable.
        let parsed = parse_listen_fds(Some("9999"), Some("1"), None, PID);
        let activation = classify(&parsed);
        assert_eq!(
            activation,
            Activation::SelfBound(SelfBindReason::Rejected(ListenFdsError::PidMismatch {
                listen_pid: 9999,
                self_pid: PID
            }))
        );
        let summary = activation.boot_summary();
        assert!(
            summary.contains("FELL BACK"),
            "fallback marker: {summary:?}"
        );
        assert!(
            summary.contains("REJECTED"),
            "states it was rejected: {summary:?}"
        );
        assert!(
            summary.contains("9999"),
            "names the foreign pid from the reason: {summary:?}"
        );
    }
}
