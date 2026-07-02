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

/// Parse the socket-activation environment into the inherited listening fds, or a typed rejection.
///
/// `self_pid` is THIS process's pid (a parameter, so the parse is pure + deterministic). Returns
/// `Ok(empty)` when `LISTEN_FDS` is absent (the normal, not-socket-activated path) and when the
/// count is zero.
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

/// Read the socket-activation environment from THIS process and parse it. A thin wrapper over
/// [`parse_listen_fds`] that supplies the real `LISTEN_*` env vars + this process's pid; all of the
/// parsing logic (and its tests) live in the pure function. Returns `Ok(empty)` when not
/// socket-activated (so a normal, non-systemd launch takes the self-bind path).
///
/// # Errors
///
/// Propagates [`parse_listen_fds`]'s errors.
pub fn from_env() -> Result<Vec<InheritedFd>, ListenFdsError> {
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

#[cfg(test)]
mod tests {
    use super::{InheritedFd, ListenFdsError, SD_LISTEN_FDS_START, parse_listen_fds, select_named};

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
}
