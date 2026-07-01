// SPDX-License-Identifier: MIT OR Apache-2.0
//! The HTTPS/GitHub AUTO-FETCH for `ironcache upgrade --to` (#394).
//!
//! The v1 mechanism (#387/#393) takes a LOCAL `--binary` already on disk. This adds the convenience
//! fetch: download the release TARBALL + its `SHA256SUMS` over HTTPS, verify the tarball against the
//! sums, extract the binary, and hand it to the SAME verify -> SAVE-first -> swap -> health-gate ->
//! rollback flow the local path runs (the orchestrator is unchanged; this just materializes the
//! binary + a sums locally, exactly as [`super::source`] describes).
//!
//! ## Why shell out to `curl` + `tar` (the posture)
//!
//! The documented upgrade posture (source.rs / verify.rs, the ADR-0017 rationale) is "static + musl +
//! cargo-deny": NO heavy HTTP framework (reqwest/hyper), NO new crypto crate. A from-scratch in-process
//! HTTPS client would need a public-root-cert crate (the tree has none; the console's client is
//! HTTP-only) plus ~300 lines of redirect/TLS/HTTP-1.1 parsing. Instead this uses the SYSTEM `curl`
//! (TLS against the OS trust store, redirects, and byte/time bounds, all battle-tested) and the SYSTEM
//! `tar` (gzip + untar) as BOUNDED subprocesses through [`super::proc::run_bounded`] -- the SAME
//! subprocess seam the `--version` probe already uses (its ETXTBSY retry was added anticipating exactly
//! this freshly-downloaded binary, proc.rs). Zero new Rust dependency; the "simple, kernel-only at
//! runtime" tenet is served, and curl/tar are on every server host the upgrade targets.
//!
//! ## The verify chain (what is trusted, and why)
//!
//! The release publishes `SHA256SUMS` over the TARBALLS (`sha256sum ironcache-*.tar.gz`, release.yml),
//! so the authenticity-relevant check is the TARBALL's sha256 against that manifest -- done HERE with
//! the SAME [`super::verify::Sha256Verifier`] the local flow uses. Once the tarball is verified, the
//! binary is extracted deterministically and its trust FORWARDED to the downstream flow as a derived
//! single-entry sums (its own sha256), so the standard verify -> probe -> swap runs unchanged. When the
//! minisign anchor lands (#386) the signature over `SHA256SUMS` will gate this fetch too -- the
//! signature is verified alongside the tarball sha256 here, behind the same [`super::verify::Verifier`].

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::proc::{BoundedError, run_bounded};
use super::verify::{Sha256Verifier, Verifier, VerifyError};

/// The default cap on a fetched artifact: a release tarball is tens of MB; 512 MiB is generous
/// headroom that still refuses a runaway/hostile download. Overridable in [`FetchBounds`].
const DEFAULT_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// The default per-fetch wall-clock bound (each of the tarball + the sums download). Generous for a
/// tens-of-MB artifact on a slow link, bounded so a stalled server cannot wedge the upgrade.
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(300);

/// The file name of the executable inside a release tarball (release.yml packages the bare binary).
const RELEASE_BINARY_NAME: &str = "ironcache";

/// Bounds + policy for a fetch: the byte cap, the per-download timeout, and whether to REQUIRE https.
/// The fields are PRIVATE and every public constructor sets `https_only = true`, so a caller can NEVER
/// build production bounds that permit a plaintext (downgradeable) fetch; only the in-crate tests
/// (which point `curl` at a loopback `http://` mock) build the http-permitting variant.
#[derive(Debug, Clone, Copy)]
pub struct FetchBounds {
    /// The maximum bytes any single download may produce (enforced by `curl --max-filesize` AND a
    /// post-download size check, so a chunked/lengthless response cannot smuggle past the cap).
    max_bytes: u64,
    /// The wall-clock bound on each download (`curl --max-time` AND the outer `run_bounded` deadline).
    timeout: Duration,
    /// Require the `https` scheme end to end (`curl --proto '=https'`), so no redirect downgrades to
    /// `http`. ALWAYS true for any public constructor; only a test builds the http-permitting form.
    https_only: bool,
}

impl FetchBounds {
    /// Production bounds with explicit caps; https is ALWAYS required (no plaintext downgrade).
    #[must_use]
    pub fn new(max_bytes: u64, timeout: Duration) -> Self {
        FetchBounds {
            max_bytes,
            timeout,
            https_only: true,
        }
    }
}

impl Default for FetchBounds {
    fn default() -> Self {
        FetchBounds::new(DEFAULT_MAX_BYTES, DEFAULT_FETCH_TIMEOUT)
    }
}

#[cfg(test)]
impl FetchBounds {
    /// TEST-ONLY bounds that PERMIT `http` (to reach the loopback mock server). Never compiled into a
    /// release build, so production genuinely cannot fetch over plaintext.
    fn insecure_for_test(max_bytes: u64, timeout: Duration) -> Self {
        FetchBounds {
            max_bytes,
            timeout,
            https_only: false,
        }
    }
}

/// A typed fetch failure (ERRORS.md: no stringly-typed errors). Every network / subprocess / parse
/// condition maps to a variant; none is a panic.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// `curl` (or `tar`) could not be run, timed out, or exited non-zero.
    #[error("running {tool} for {what}: {detail}")]
    Subprocess {
        /// The external tool (`curl` / `tar`).
        tool: String,
        /// What it was doing (the URL being fetched / the tarball being extracted).
        what: String,
        /// The failure detail (exit status + captured stderr, or the bounded-run error).
        detail: String,
    },
    /// A download exceeded [`FetchBounds::max_bytes`].
    #[error("the download of {url} exceeded the {max_bytes}-byte cap ({actual} bytes)")]
    TooLarge {
        /// The URL that was too large.
        url: String,
        /// The configured cap.
        max_bytes: u64,
        /// The actual size seen on disk.
        actual: u64,
    },
    /// A supplied URL is not `https://` while [`FetchBounds::https_only`] is set.
    #[error("refusing the non-https URL {url} (https is required)")]
    NotHttps {
        /// The offending URL.
        url: String,
    },
    /// The downloaded tarball did not verify against its `SHA256SUMS` entry (integrity / authenticity).
    #[error("verifying the downloaded tarball: {0}")]
    Verify(#[from] VerifyError),
    /// The extracted tarball did not contain the expected `ironcache` binary.
    #[error("no `{name}` executable found in the extracted tarball {tarball}")]
    NoBinary {
        /// The binary name we looked for.
        name: String,
        /// The tarball we extracted.
        tarball: String,
    },
    /// A local filesystem operation (temp dir, write, read-back) failed.
    #[error("filesystem error during fetch ({what}): {detail}")]
    Io {
        /// The operation that failed.
        what: String,
        /// The IO error.
        detail: String,
    },
}

/// A self-cleaning temporary directory: created under the system temp dir with a unique name, and
/// REMOVED on drop. No `tempfile` crate (the no-new-dep posture); the unique name uses the pid +
/// thread id, matching the pattern the upgrade tests already use.
#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a fresh temp dir tagged `tag`. Errors if it cannot be created.
    fn new(tag: &str) -> Result<Self, FetchError> {
        let path = std::env::temp_dir().join(format!(
            "ic-upgrade-fetch-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        // A stale dir from a previous run with the same pid/thread is removed first.
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).map_err(|e| FetchError::Io {
            what: format!("creating temp dir {}", path.display()),
            detail: e.to_string(),
        })?;
        Ok(TempDir { path })
    }

    /// The directory path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // Best-effort cleanup; a leaked temp dir on a crash is harmless and OS-reaped.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A materialized remote release: the extracted binary + a derived single-entry `SHA256SUMS` that
/// vouches for it, both living under `_dir` (kept alive so the paths stay valid until the caller
/// finishes the upgrade). Feed `binary` + `sha256sums` straight into the local-file upgrade flow.
#[derive(Debug)]
pub struct Fetched {
    /// The extracted `ironcache` binary on disk.
    pub binary: PathBuf,
    /// A derived `<sha256>  ironcache` manifest for [`binary`], so the downstream `Sha256Verifier`
    /// (which keys on the binary basename) passes -- the AUTHENTICITY check already happened against
    /// the published tarball manifest in [`fetch_release`].
    pub sha256sums: PathBuf,
    /// The owning temp dir; dropping it deletes `binary` + `sha256sums`, so hold the whole
    /// [`Fetched`] until the upgrade run completes. Private (a drop guard, not read directly).
    _dir: TempDir,
}

/// Fetch + verify + extract a release from explicit URLs: download `tarball_url` and `sums_url`,
/// verify the tarball's sha256 against the downloaded `SHA256SUMS`, extract the `ironcache` binary,
/// and return it plus a derived per-binary manifest ([`Fetched`]).
///
/// `tarball_url`'s last path segment is used as the artifact name for the `SHA256SUMS` lookup (it must
/// match the manifest entry, e.g. `ironcache-2026.0622.1-linux-amd64-musl.tar.gz`).
///
/// # Errors
///
/// Returns a [`FetchError`] on a non-https URL (when required), a curl/tar failure, an over-cap
/// download, a tarball sha256 mismatch, or a missing binary in the archive.
pub fn fetch_release(
    tarball_url: &str,
    sums_url: &str,
    bounds: FetchBounds,
) -> Result<Fetched, FetchError> {
    let dir = TempDir::new("release")?;
    let asset_name = url_basename(tarball_url).ok_or_else(|| FetchError::Io {
        what: "deriving the artifact name".to_owned(),
        detail: format!("the URL {tarball_url} has no path segment to name the artifact"),
    })?;

    // 1. Download the tarball + the manifest (bounded, https-enforced).
    let tarball = dir.path().join(&asset_name);
    fetch_url(tarball_url, &tarball, bounds)?;
    let sums = dir.path().join("SHA256SUMS");
    fetch_url(sums_url, &sums, bounds)?;

    // 2. Verify the TARBALL against the published manifest -- the authenticity-relevant check (the
    //    manifest lists the tarballs). An `ok` here means the bytes are the published release bytes.
    Sha256Verifier.verify(&tarball, &asset_name, &sums)?;

    // 3. Extract the binary from the (now-verified) tarball.
    let extract_dir = dir.path().join("extracted");
    std::fs::create_dir_all(&extract_dir).map_err(|e| FetchError::Io {
        what: format!("creating {}", extract_dir.display()),
        detail: e.to_string(),
    })?;
    extract_tarball(&tarball, &extract_dir, bounds)?;
    let binary =
        find_binary(&extract_dir, RELEASE_BINARY_NAME).ok_or_else(|| FetchError::NoBinary {
            name: RELEASE_BINARY_NAME.to_owned(),
            tarball: tarball.display().to_string(),
        })?;

    // 4. FORWARD the trust: write a single-entry manifest over the extracted binary so the downstream
    //    verify -> probe -> swap flow runs unchanged (the real authenticity was step 2).
    let bin_bytes = std::fs::read(&binary).map_err(|e| FetchError::Io {
        what: format!("reading the extracted binary {}", binary.display()),
        detail: e.to_string(),
    })?;
    let digest = ironcache_config::sha256_hex(&bin_bytes);
    let derived_sums = dir.path().join("BINARY_SHA256SUMS");
    std::fs::write(&derived_sums, format!("{digest}  {RELEASE_BINARY_NAME}\n")).map_err(|e| {
        FetchError::Io {
            what: format!("writing the derived manifest {}", derived_sums.display()),
            detail: e.to_string(),
        }
    })?;

    Ok(Fetched {
        binary,
        sha256sums: derived_sums,
        _dir: dir,
    })
}

/// Download `url` to `dest` with `curl`, bounded by size, time, and (optionally) the https scheme.
/// A non-zero curl exit, a timeout, or an over-cap file is a typed [`FetchError`].
fn fetch_url(url: &str, dest: &Path, bounds: FetchBounds) -> Result<(), FetchError> {
    if bounds.https_only && !is_https(url) {
        return Err(FetchError::NotHttps {
            url: url.to_owned(),
        });
    }
    let max_secs = bounds.timeout.as_secs().max(1).to_string();
    let max_filesize = bounds.max_bytes.to_string();
    let dest_str = dest.to_string_lossy().into_owned();
    let argv = build_curl_argv(url, &dest_str, &max_secs, &max_filesize, bounds.https_only);
    let arg_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    // The outer bound is generous over --max-time so curl reports its own (nicer) timeout first.
    let outer = bounds.timeout + Duration::from_secs(30);
    let out = run_bounded(Path::new("curl"), &arg_refs, outer)
        .map_err(|e| subprocess_err("curl", url, &e))?;
    if !out.status.success() {
        return Err(FetchError::Subprocess {
            tool: "curl".to_owned(),
            what: url.to_owned(),
            detail: format!(
                "exit {}: {}",
                out.status
                    .code()
                    .map_or_else(|| "signal".to_owned(), |c| c.to_string()),
                String::from_utf8_lossy(&out.stderr).trim()
            ),
        });
    }
    // Defense in depth: enforce the cap on the bytes actually written (a chunked/lengthless response
    // can slip past curl's Content-Length-based --max-filesize).
    let size = std::fs::metadata(dest)
        .map_err(|e| FetchError::Io {
            what: format!("stat {}", dest.display()),
            detail: e.to_string(),
        })?
        .len();
    if size > bounds.max_bytes {
        let _ = std::fs::remove_file(dest);
        return Err(FetchError::TooLarge {
            url: url.to_owned(),
            max_bytes: bounds.max_bytes,
            actual: size,
        });
    }
    Ok(())
}

/// Build the `curl` argument vector for a bounded download. Separated as a PURE function so the
/// SECURITY-relevant flags are unit-tested without spawning curl: `--fail` (non-2xx -> non-zero exit),
/// `--location` (follow the GitHub -> CDN redirect), `--max-time`/`--max-filesize` (bounds), and -- when
/// https_only -- BOTH `--proto '=https'` (initial scheme) AND `--proto-redir '=https'` (REDIRECT
/// targets). Pinning `--proto-redir` matters: its default varies across curl versions, and GitHub
/// downloads ALWAYS redirect, so without it a 3xx could downgrade the transfer to plaintext.
fn build_curl_argv(
    url: &str,
    dest: &str,
    max_secs: &str,
    max_filesize: &str,
    https_only: bool,
) -> Vec<String> {
    let mut a: Vec<String> = vec![
        "--fail".into(),
        "--location".into(),
        "--silent".into(),
        "--show-error".into(),
        "--max-time".into(),
        max_secs.into(),
        "--max-filesize".into(),
        max_filesize.into(),
    ];
    if https_only {
        a.push("--proto".into());
        a.push("=https".into());
        a.push("--proto-redir".into());
        a.push("=https".into());
    }
    a.push("--output".into());
    a.push(dest.into());
    a.push(url.into());
    a
}

/// Extract `tarball` (a `.tar.gz`) into `dest` with the system `tar`, bounded. A tar failure is a
/// typed [`FetchError`].
fn extract_tarball(tarball: &Path, dest: &Path, bounds: FetchBounds) -> Result<(), FetchError> {
    let tarball_str = tarball.to_string_lossy().into_owned();
    let dest_str = dest.to_string_lossy().into_owned();
    let args = ["-xzf", &tarball_str, "-C", &dest_str];
    let out = run_bounded(Path::new("tar"), &args, bounds.timeout)
        .map_err(|e| subprocess_err("tar", &tarball_str, &e))?;
    if !out.status.success() {
        return Err(FetchError::Subprocess {
            tool: "tar".to_owned(),
            what: tarball_str,
            detail: format!(
                "exit {}: {}",
                out.status
                    .code()
                    .map_or_else(|| "signal".to_owned(), |c| c.to_string()),
                String::from_utf8_lossy(&out.stderr).trim()
            ),
        });
    }
    Ok(())
}

/// Recursively find a regular file named `name` under `root` (the release tarball may place the
/// binary at the archive root or in a versioned subdir, so search rather than assume the layout).
fn find_binary(root: &Path, name: &str) -> Option<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            let ty = entry.file_type().ok()?;
            if ty.is_dir() {
                stack.push(path);
            } else if ty.is_file() && entry.file_name().to_str() == Some(name) {
                return Some(path);
            }
        }
    }
    None
}

/// The last path segment of a URL (its artifact name), ignoring any `?query`/`#fragment`. `None` for
/// a URL that ends in `/` or has no path.
fn url_basename(url: &str) -> Option<String> {
    let no_frag = url.split('#').next().unwrap_or(url);
    let no_query = no_frag.split('?').next().unwrap_or(no_frag);
    let seg = no_query.rsplit('/').next().unwrap_or("");
    if seg.is_empty() {
        None
    } else {
        Some(seg.to_owned())
    }
}

/// Whether `url` uses the `https` scheme (case-insensitive).
fn is_https(url: &str) -> bool {
    let lower = url
        .get(0..8)
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    lower == "https://"
}

/// Map a [`BoundedError`] (spawn/timeout/wait) into a [`FetchError::Subprocess`].
fn subprocess_err(tool: &str, what: &str, e: &BoundedError) -> FetchError {
    FetchError::Subprocess {
        tool: tool.to_owned(),
        what: what.to_owned(),
        detail: e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    // ---- A tiny loopback HTTP/1.1 server, so the fetch is tested end-to-end WITHOUT the network. ----

    /// One canned route: the response body for an exact request path, or a 302 redirect to another
    /// path. The server serves each connection once (Connection: close) from a fixed route table.
    struct Route {
        path: String,
        body: Vec<u8>,
        redirect_to: Option<String>,
    }

    /// Spawn a loopback HTTP server serving `routes`; returns its `host:port` base and the join
    /// handle. It answers `n = routes-worth` sequential connections then exits (each test drives a
    /// known number of GETs).
    fn spawn_http(routes: Vec<Route>, conns: usize) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().unwrap();
        let base = format!("127.0.0.1:{}", addr.port());
        let handle = thread::spawn(move || {
            for _ in 0..conns {
                let Ok((stream, _)) = listener.accept() else {
                    break;
                };
                serve_one(stream, &routes);
            }
        });
        (base, handle)
    }

    /// Read the request line, match its path against the route table, and write the canned response.
    fn serve_one(mut stream: TcpStream, routes: &[Route]) {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        let path = req
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_owned();
        let route = routes.iter().find(|r| r.path == path);
        let resp = match route {
            Some(r) if r.redirect_to.is_some() => {
                let loc = r.redirect_to.as_ref().unwrap();
                format!("HTTP/1.1 302 Found\r\nLocation: {loc}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .into_bytes()
            }
            Some(r) => {
                let mut head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    r.body.len()
                )
                .into_bytes();
                head.extend_from_slice(&r.body);
                head
            }
            None => {
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
            }
        };
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    }

    /// Test bounds: http allowed (loopback mock), small cap, short timeout.
    fn test_bounds() -> FetchBounds {
        FetchBounds::insecure_for_test(10 * 1024 * 1024, Duration::from_secs(20))
    }

    fn have_tool(tool: &str) -> bool {
        std::process::Command::new(tool)
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// Build a real `.tar.gz` containing an `ironcache` script, returning its path + the tempdir.
    fn make_release_tarball(tag: &str, binary_body: &[u8]) -> (PathBuf, TempDir) {
        let dir = TempDir::new(tag).unwrap();
        let stage = dir.path().join("stage");
        std::fs::create_dir_all(&stage).unwrap();
        std::fs::write(stage.join("ironcache"), binary_body).unwrap();
        let tarball = dir.path().join("ironcache-9.9.9-test.tar.gz");
        let out = std::process::Command::new("tar")
            .args([
                "-czf",
                tarball.to_str().unwrap(),
                "-C",
                stage.to_str().unwrap(),
                "ironcache",
            ])
            .output()
            .unwrap();
        assert!(out.status.success(), "tar -czf failed: {out:?}");
        (tarball, dir)
    }

    #[test]
    fn url_basename_extracts_the_asset_name() {
        assert_eq!(
            url_basename("https://x/releases/download/v1/ironcache-1-linux.tar.gz").as_deref(),
            Some("ironcache-1-linux.tar.gz")
        );
        assert_eq!(
            url_basename("https://x/a.tar.gz?token=abc#f").as_deref(),
            Some("a.tar.gz")
        );
        assert_eq!(url_basename("https://x/dir/"), None);
    }

    #[test]
    fn curl_argv_pins_https_on_the_initial_scheme_and_redirects() {
        // The SECURITY-critical flags: under https_only, BOTH --proto and --proto-redir must pin
        // `=https`, so a GitHub -> CDN 3xx cannot downgrade the transfer to plaintext.
        let argv = build_curl_argv("https://x/a.tar.gz", "/tmp/out", "300", "999", true);
        let joined = argv.join(" ");
        assert!(
            argv.iter().any(|a| a == "--fail"),
            "curl must fail on non-2xx: {joined}"
        );
        assert!(
            argv.iter().any(|a| a == "--location"),
            "curl must follow redirects: {joined}"
        );
        // Both proto guards present, each followed by `=https`.
        for flag in ["--proto", "--proto-redir"] {
            let idx = argv
                .iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("missing {flag}: {joined}"));
            assert_eq!(
                argv.get(idx + 1).map(String::as_str),
                Some("=https"),
                "{flag} must pin =https"
            );
        }
        // The byte + time caps are passed through.
        assert!(
            argv.iter().any(|a| a == "--max-filesize"),
            "byte cap: {joined}"
        );
        assert!(argv.iter().any(|a| a == "--max-time"), "time cap: {joined}");
        // Without https_only (tests only), the proto guards are absent so a loopback http mock works.
        let insecure = build_curl_argv("http://127.0.0.1/x", "/tmp/out", "20", "999", false);
        assert!(
            !insecure.iter().any(|a| a == "--proto"),
            "no proto guard when not https_only"
        );
    }

    #[test]
    fn https_only_rejects_http() {
        let dir = TempDir::new("https").unwrap();
        let err = fetch_url(
            "http://example.com/x",
            &dir.path().join("x"),
            FetchBounds::default(), // https_only = true
        )
        .expect_err("http must be rejected under https_only");
        assert!(matches!(err, FetchError::NotHttps { .. }), "{err:?}");
    }

    #[test]
    fn fetch_url_downloads_and_follows_redirects() {
        if !have_tool("curl") {
            eprintln!("skipping: curl not available");
            return;
        }
        let body = b"the-tarball-bytes".to_vec();
        let routes = vec![
            Route {
                path: "/redirect".to_owned(),
                body: vec![],
                redirect_to: Some("/final".to_owned()),
            },
            Route {
                path: "/final".to_owned(),
                body: body.clone(),
                redirect_to: None,
            },
        ];
        let (base, handle) = spawn_http(routes, 2);
        let dir = TempDir::new("dl").unwrap();
        let dest = dir.path().join("out");
        fetch_url(&format!("http://{base}/redirect"), &dest, test_bounds())
            .expect("fetch follows 302");
        assert_eq!(
            std::fs::read(&dest).unwrap(),
            body,
            "got the redirected body"
        );
        handle.join().unwrap();
    }

    #[test]
    fn fetch_url_over_cap_is_rejected() {
        if !have_tool("curl") {
            return;
        }
        let body = vec![b'x'; 4096];
        let routes = vec![Route {
            path: "/big".to_owned(),
            body,
            redirect_to: None,
        }];
        let (base, handle) = spawn_http(routes, 1);
        let dir = TempDir::new("big").unwrap();
        let dest = dir.path().join("out");
        // A cap (100) smaller than the 4096-byte body.
        let bounds = FetchBounds::insecure_for_test(100, Duration::from_secs(20));
        let err =
            fetch_url(&format!("http://{base}/big"), &dest, bounds).expect_err("over cap errors");
        assert!(
            matches!(
                err,
                FetchError::TooLarge { .. } | FetchError::Subprocess { .. }
            ),
            "{err:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn extract_finds_the_binary() {
        if !have_tool("tar") {
            return;
        }
        let (tarball, _dir) = make_release_tarball("extract", b"#!/bin/sh\necho hi\n");
        let out = TempDir::new("out").unwrap();
        extract_tarball(&tarball, out.path(), test_bounds()).expect("extract");
        let bin = find_binary(out.path(), "ironcache").expect("finds ironcache");
        assert_eq!(std::fs::read(&bin).unwrap(), b"#!/bin/sh\necho hi\n");
    }

    #[test]
    fn fetch_release_end_to_end_verifies_and_extracts() {
        if !have_tool("curl") || !have_tool("tar") {
            return;
        }
        // A real tarball + a SHA256SUMS that vouches for it.
        let (tarball, _dir) = make_release_tarball("e2e", b"#!/bin/sh\necho 'ironcache 9.9.9'\n");
        let tarball_bytes = std::fs::read(&tarball).unwrap();
        let asset = "ironcache-9.9.9-test.tar.gz";
        let digest = ironcache_config::sha256_hex(&tarball_bytes);
        let sums_body = format!("{digest}  {asset}\n").into_bytes();
        let routes = vec![
            Route {
                path: format!("/{asset}"),
                body: tarball_bytes,
                redirect_to: None,
            },
            Route {
                path: "/SHA256SUMS".to_owned(),
                body: sums_body,
                redirect_to: None,
            },
        ];
        let (base, handle) = spawn_http(routes, 2);
        let fetched = fetch_release(
            &format!("http://{base}/{asset}"),
            &format!("http://{base}/SHA256SUMS"),
            test_bounds(),
        )
        .expect("fetch + verify + extract");
        // The extracted binary is present, and the derived manifest vouches for it.
        let bin_bytes = std::fs::read(&fetched.binary).unwrap();
        assert_eq!(bin_bytes, b"#!/bin/sh\necho 'ironcache 9.9.9'\n");
        Sha256Verifier
            .verify(&fetched.binary, "ironcache", &fetched.sha256sums)
            .expect("the derived manifest vouches for the extracted binary");
        handle.join().unwrap();
    }

    #[test]
    fn fetch_release_rejects_a_tampered_tarball() {
        if !have_tool("curl") || !have_tool("tar") {
            return;
        }
        // The SHA256SUMS vouches for DIFFERENT bytes than the tarball actually served -> reject.
        let (tarball, _dir) = make_release_tarball("tamper", b"real-bytes");
        let tarball_bytes = std::fs::read(&tarball).unwrap();
        let asset = "ironcache-9.9.9-test.tar.gz";
        let wrong_digest = ironcache_config::sha256_hex(b"some-other-bytes");
        let sums_body = format!("{wrong_digest}  {asset}\n").into_bytes();
        let routes = vec![
            Route {
                path: format!("/{asset}"),
                body: tarball_bytes,
                redirect_to: None,
            },
            Route {
                path: "/SHA256SUMS".to_owned(),
                body: sums_body,
                redirect_to: None,
            },
        ];
        let (base, handle) = spawn_http(routes, 2);
        let err = fetch_release(
            &format!("http://{base}/{asset}"),
            &format!("http://{base}/SHA256SUMS"),
            test_bounds(),
        )
        .expect_err("a tarball whose sha256 does not match the manifest must be rejected");
        assert!(matches!(err, FetchError::Verify(_)), "{err:?}");
        handle.join().unwrap();
    }
}
