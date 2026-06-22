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

    #[test]
    fn index_carries_the_login_panel_elements() {
        // The sign-in affordance (UI auth, follow-up to #360) is STATIC markup in
        // index.html (app.js only wires it). Assert the element ids app.js drives
        // are present and the token field is a password input.
        for id in [
            "id=\"login-panel\"",
            "id=\"login-form\"",
            "id=\"login-token\"",
            "id=\"login-submit\"",
            "id=\"logout-submit\"",
            "id=\"login-status\"",
        ] {
            assert!(
                INDEX_HTML.contains(id),
                "index.html must contain the login element {id}"
            );
        }
        assert!(
            INDEX_HTML.contains("type=\"password\""),
            "the token field must be a password input"
        );
        // The panel is hidden by default (revealed by app.js on a 401).
        assert!(
            INDEX_HTML.contains("id=\"login-panel\"")
                && INDEX_HTML.contains("class=\"login-panel\""),
            "the login panel must be present"
        );
    }

    #[test]
    fn index_has_no_inline_handlers() {
        // CSP forbids inline event handlers: index.html must wire NOTHING via
        // onclick/onsubmit/etc.; app.js attaches the handlers via addEventListener.
        for attr in ["onclick", "onsubmit", "onload", "onerror", "javascript:"] {
            assert!(
                !INDEX_HTML.contains(attr),
                "index.html must not contain the inline handler `{attr}` (CSP)"
            );
        }
    }

    #[test]
    fn app_js_is_auth_aware() {
        // The auth integration: the script sends the Bearer token (read from
        // sessionStorage, NOT localStorage) and wires the controls via
        // addEventListener (NOT inline onclick).
        for needle in [
            "Authorization",
            "Bearer ",
            "sessionStorage",
            "addEventListener",
            "ic_console_token",
        ] {
            assert!(
                APP_JS.contains(needle),
                "app.js must reference `{needle}` for the auth flow"
            );
        }
        // The token must live in sessionStorage (tab-scoped), never localStorage.
        assert!(
            !APP_JS.contains("localStorage"),
            "app.js must use sessionStorage, never localStorage"
        );
        // No inline onclick handlers in the script (CSP-safety / convention).
        assert!(
            !APP_JS.contains("onclick"),
            "app.js must wire events via addEventListener, not onclick"
        );
    }

    #[test]
    fn app_js_never_logs_or_urls_the_token() {
        // Defense-in-depth: the token is sent ONLY as a header. It must not be
        // console.log'd and must not be concatenated into a query string. We
        // assert the obvious leak shapes are absent.
        for leak in [
            "console.log(token",
            "console.log(getToken",
            "?token=",
            "&token=",
        ] {
            assert!(
                !APP_JS.contains(leak),
                "app.js must not leak the token via `{leak}`"
            );
        }
    }
}
