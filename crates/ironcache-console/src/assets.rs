// SPDX-License-Identifier: MIT OR Apache-2.0
//! The console's static UI assets (issue #359), embedded in the binary.
//!
//! The dashboard SPA is plain HTML + CSS + vanilla JS (no npm, no framework, no
//! build step, no CDN), embedded with [`include_str!`] so the static musl build
//! stays a single pure-Rust binary and serves the UI off its OWN HTTP responder.
//! Keeping CSS and JS in SEPARATE files (not inline) is what lets the served
//! pages run under a strict Content-Security-Policy (`default-src 'self'`) with
//! NO `unsafe-inline`: the only script/style sources are same-origin files.
//!
//! SECURITY: the dashboard reads the unauthenticated `/api/*` surface (node
//! addresses, slowlog argv = key names, client IPs). The UI is UNAUTHENTICATED
//! today and relies on the loopback default bind; it MUST move behind the
//! auth/RBAC tier (#360) and the VPN-locked exposure (#369) before the console
//! is exposed. The served HTML/JS/CSS carry strict response headers (CSP,
//! `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`); see
//! [`crate::http`].

/// The dashboard shell. References `/app.css` and `/app.js` (no inline
/// script/style, for the strict CSP).
pub const INDEX_HTML: &str = include_str!("../ui/index.html");

/// The dashboard stylesheet.
pub const APP_CSS: &str = include_str!("../ui/app.css");

/// The dashboard logic (vanilla JS). It renders the panels from the `/api/*`
/// JSON, escaping every server string into the DOM via `textContent` (no
/// `innerHTML` interpolation, so the attacker-influenceable slowlog/client
/// fields are not an XSS sink).
pub const APP_JS: &str = include_str!("../ui/app.js");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_references_the_separate_css_and_js() {
        // The HTML must link the external files (not inline them) so the strict
        // CSP (default-src 'self') can run with no 'unsafe-inline'.
        assert!(INDEX_HTML.contains("/app.css"), "index must link app.css");
        assert!(INDEX_HTML.contains("/app.js"), "index must link app.js");
        // And it must NOT contain an inline <script>...</script> body or a
        // <style> block (only the external <link>/<script src>).
        assert!(
            !INDEX_HTML.contains("<style"),
            "index must not carry an inline <style> block (CSP)"
        );
    }

    #[test]
    fn assets_are_non_empty() {
        assert!(!INDEX_HTML.is_empty());
        assert!(!APP_CSS.is_empty());
        assert!(!APP_JS.is_empty());
    }

    #[test]
    fn app_js_avoids_inner_html_sinks() {
        // The XSS-safety posture: no innerHTML/outerHTML/insertAdjacentHTML/
        // document.write sink anywhere in the dashboard script. Server strings
        // reach the DOM only via textContent / createTextNode.
        for sink in [
            ".innerHTML",
            ".outerHTML",
            "insertAdjacentHTML",
            "document.write",
        ] {
            assert!(
                !APP_JS.contains(sink),
                "app.js must not use the {sink} sink (XSS-safety)"
            );
        }
    }
}
