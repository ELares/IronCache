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

/// The self-hosted `@font-face` declarations (SIL Open Font License 1.1). Served
/// at `/assets/fonts.css` and imported from `app.css` so the bespoke type faces
/// load with NO CDN under the strict CSP (`default-src 'self'`). It references the
/// two woff2 files below by relative URL (`./fonts/<name>.woff2`).
pub const FONTS_CSS: &str = include_str!("../ui/assets/fonts.css");

/// The Hanken Grotesk variable font (the design system's open substitute for
/// Aeonik), embedded as raw bytes and served at
/// `/assets/fonts/hanken-grotesk.woff2` with `Content-Type: font/woff2`.
pub const FONT_HANKEN_GROTESK_WOFF2: &[u8] =
    include_bytes!("../ui/assets/fonts/hanken-grotesk.woff2");

/// The JetBrains Mono variable font (the monospace face), embedded as raw bytes
/// and served at `/assets/fonts/jetbrains-mono.woff2` with `Content-Type:
/// font/woff2`.
pub const FONT_JETBRAINS_MONO_WOFF2: &[u8] =
    include_bytes!("../ui/assets/fonts/jetbrains-mono.woff2");

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
        // The bespoke re-skin still carries the IronCache name (the <title>) and
        // links the SEPARATE stylesheet that imports the self-hosted fonts.
        assert!(
            INDEX_HTML.contains("IronCache"),
            "index must carry the IronCache name"
        );
    }

    #[test]
    fn index_is_csp_clean_no_inline_style_or_handlers() {
        // The bespoke re-skin must stay CSP-clean: no inline `style="..."`
        // attribute (all styling is by class in app.css; dynamic values are
        // CSSOM custom properties set from app.js), and no inline `on*=`
        // event-handler attribute (all behavior is wired with addEventListener).
        assert!(
            !INDEX_HTML.contains(" style="),
            "index.html must not carry an inline style= attribute (CSP)"
        );
        // No inline event handler: an ` on...="` shape (e.g. ` onclick="`).
        // Scan for ` on` immediately followed (after the event name) by `=`.
        assert!(
            !has_inline_handler(INDEX_HTML),
            "index.html must not carry an inline on*= handler (CSP)"
        );
    }

    /// Whether the markup carries an inline event-handler attribute: a whitespace
    /// then `on`, an alphabetic event name, then `=` (e.g. ` onclick=`). SVG
    /// presentation attributes and ordinary attributes never match this shape.
    fn has_inline_handler(html: &str) -> bool {
        let bytes = html.as_bytes();
        let mut i = 0;
        while i + 3 < bytes.len() {
            // A whitespace boundary before `on`.
            if bytes[i].is_ascii_whitespace()
                && bytes[i + 1] == b'o'
                && bytes[i + 2] == b'n'
                && bytes[i + 3].is_ascii_alphabetic()
            {
                let mut j = i + 3;
                while j < bytes.len() && bytes[j].is_ascii_alphabetic() {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b'=' {
                    return true;
                }
            }
            i += 1;
        }
        false
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
        // The panel is hidden by default (revealed by app.js on a 401). It now
        // carries the `login-panel` class among others (the bespoke card style),
        // so assert the class token is present rather than an exact attribute.
        assert!(
            INDEX_HTML.contains("id=\"login-panel\"") && INDEX_HTML.contains("login-panel"),
            "the login panel must be present"
        );
    }

    #[test]
    fn font_assets_are_embedded_non_empty() {
        // The self-hosted fonts (OFL-1.1) are embedded for the strict CSP (no CDN):
        // the @font-face stylesheet plus the two woff2 binaries. The stylesheet
        // references the two relative font URLs that the HTTP layer serves.
        assert!(!FONTS_CSS.is_empty(), "fonts.css must be embedded");
        assert!(
            FONTS_CSS.contains("@font-face"),
            "fonts.css must declare faces"
        );
        assert!(
            FONTS_CSS.contains("hanken-grotesk.woff2"),
            "fonts.css must reference the sans woff2"
        );
        assert!(
            FONTS_CSS.contains("jetbrains-mono.woff2"),
            "fonts.css must reference the mono woff2"
        );
        // The woff2 are binary; check they embedded and start with the wOF2 magic.
        assert!(
            FONT_HANKEN_GROTESK_WOFF2.starts_with(b"wOF2"),
            "the sans font must be a woff2 (wOF2 magic)"
        );
        assert!(
            FONT_JETBRAINS_MONO_WOFF2.starts_with(b"wOF2"),
            "the mono font must be a woff2 (wOF2 magic)"
        );
    }

    #[test]
    fn app_css_imports_self_hosted_fonts() {
        // app.css must @import the self-hosted fonts at the same origin (no CDN),
        // and must NOT reference any external font host (Google Fonts etc.).
        assert!(
            APP_CSS.contains("@import url('/assets/fonts.css')"),
            "app.css must import the self-hosted fonts.css"
        );
        for cdn in [
            "fonts.googleapis.com",
            "fonts.gstatic.com",
            "https://",
            "http://",
        ] {
            assert!(
                !APP_CSS.contains(cdn),
                "app.css must not reference an external host `{cdn}` (CSP/no-CDN)"
            );
        }
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
    fn index_carries_the_management_page_elements() {
        // The node-level management pages (#361) are now FUNCTIONAL markup (not the
        // gated empty-states). Assert the element ids app.js drives are present.
        for id in [
            // Config.
            "id=\"config-rows\"",
            "id=\"config-filter\"",
            // Keyspace browser + inspector + actions + new-key form.
            "id=\"ks-scan-form\"",
            "id=\"ks-key-list\"",
            "id=\"ks-inspector\"",
            "id=\"ks-detail-value\"",
            "id=\"ks-expire-form\"",
            "id=\"ks-del-btn\"",
            "id=\"ks-new-form\"",
            // Console.
            "id=\"console-form\"",
            "id=\"console-input\"",
            "id=\"console-scrollback\"",
            "id=\"console-chips\"",
            // Pub/Sub.
            "id=\"pubsub-body\"",
            "id=\"pubsub-form\"",
            // ACL.
            "id=\"acl-users\"",
            "id=\"acl-form\"",
            // Persistence (new page + nav).
            "id=\"section-persistence\"",
            "id=\"persistence-save\"",
            "data-section=\"persistence\"",
            // Cluster rebalance dry-run card (#361).
            "id=\"rebalance-load\"",
            "id=\"rebalance-body\"",
            "id=\"rebalance-summary\"",
            // Cluster failover card (#361, destructive: a typed confirmation input).
            "id=\"failover-confirm\"",
            "id=\"failover-trigger\"",
            // Cluster node membership (#361): add (MEET) + remove (FORGET).
            "id=\"meet-host\"",
            "id=\"meet-port\"",
            "id=\"meet-add\"",
            "id=\"forget-node-id\"",
            "id=\"forget-remove\"",
            // Cluster slot migration / FLIP (#361, CLUSTER SETSLOT).
            "id=\"setslot-slot\"",
            "id=\"setslot-action\"",
            "id=\"setslot-node-id\"",
            "id=\"setslot-apply\"",
        ] {
            assert!(
                INDEX_HTML.contains(id),
                "index.html must contain the management element {id}"
            );
        }
    }

    #[test]
    fn cluster_replication_shards_stay_gated_empty_states() {
        // Replication and shards stay honest empty-states (NO fabricated data): they
        // must NOT have been turned into functional pages. The cluster view keeps its
        // topology empty-state too, but now ALSO carries the real, admin-gated
        // rebalance dry-run card (#361, a genuine engine endpoint, not fabricated).
        for marker in [
            "id=\"section-cluster\"",
            "id=\"section-replication\"",
            "id=\"section-shards\"",
        ] {
            assert!(INDEX_HTML.contains(marker), "{marker} must still exist");
        }
        // The gated empty-state card text is still present (cluster topology + shards).
        assert!(
            INDEX_HTML.contains("This node is standalone."),
            "the gated cluster/shards empty-states must remain"
        );
        assert!(
            INDEX_HTML.contains("Replication view appears when replicas are attached."),
            "the replication view stays a gated empty-state"
        );
        // The cluster view's rebalance card is the one functional addition there.
        assert!(
            INDEX_HTML.contains("id=\"rebalance-load\"")
                && INDEX_HTML.contains("id=\"rebalance-body\""),
            "the cluster view carries the rebalance dry-run card"
        );
    }

    #[test]
    fn app_js_wires_the_management_mutations() {
        // The management write path: app.js issues POST/DELETE to the new endpoints
        // through a method-aware fetch that sends the Bearer token in the header.
        for needle in [
            "/api/config",
            "/api/keys/",
            "/api/command",
            "/api/pubsub/publish",
            "/api/acl/user",
            "/api/persistence/save",
            "/api/cluster/failover",
            "triggerFailover",
            "/api/cluster/meet",
            "/api/cluster/forget",
            "/api/cluster/setslot",
            "addNode",
            "removeNode",
            "applySetslot",
            "fetchMethod",
            "tokenizeCommand",
        ] {
            assert!(
                APP_JS.contains(needle),
                "app.js must reference `{needle}` for the management layer"
            );
        }
        // The mutation fetch still carries the token ONLY as a header (no token in a
        // URL/query), and still avoids the innerHTML sink.
        assert!(
            APP_JS.contains("Authorization"),
            "mutations carry the auth header"
        );
        assert!(!APP_JS.contains(".innerHTML"), "no innerHTML sink");
        assert!(!APP_JS.contains("?token="), "no token in a query");
        assert!(!APP_JS.contains("&token="), "no token in a query");
    }

    #[test]
    fn app_js_wires_the_topology_staleness_banner() {
        // #354: app.js reads the server-reported topology age and raises the shared
        // banner past a threshold (so a stuck poll is not shown as live data).
        for needle in [
            "topology_age_seconds",
            "lastTopologyAgeSeconds",
            "STALE_AFTER_S",
        ] {
            assert!(
                APP_JS.contains(needle),
                "app.js must reference `{needle}` for the staleness banner"
            );
        }
    }

    #[test]
    fn app_js_wires_the_rebalance_plan_read() {
        // The cluster rebalance dry-run (#361) is an admin READ wired on the button:
        // app.js fetches the endpoint via the token-aware fetchJson and renders it.
        for needle in [
            "/api/cluster/rebalance-plan",
            "loadRebalancePlan",
            "renderRebalancePlan",
        ] {
            assert!(
                APP_JS.contains(needle),
                "app.js must reference `{needle}` for the rebalance plan read"
            );
        }
    }

    #[test]
    fn index_management_markup_stays_csp_clean() {
        // The new management markup must keep the strict-CSP posture: no inline
        // style= attribute and no inline on*= handler (the existing whole-file
        // guards also cover this; this is an explicit management-focused check).
        assert!(
            !INDEX_HTML.contains(" style="),
            "management markup must not carry an inline style="
        );
        assert!(
            !has_inline_handler(INDEX_HTML),
            "management markup must not carry an inline on*= handler"
        );
        // The console suggestion chips carry a data-cmd attribute (NOT an on*=
        // handler); app.js wires them via addEventListener.
        assert!(
            INDEX_HTML.contains("data-cmd=\"PING\""),
            "console chips use data-cmd, wired in app.js"
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
