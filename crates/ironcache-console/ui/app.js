// SPDX-License-Identifier: MIT OR Apache-2.0
//
// IronCache Console dashboard logic (issue #359). Vanilla JS, no framework, no
// build step, no external fetch (a strict CSP default-src 'self' must run this
// with no 'unsafe-inline' and no CDN).
//
// SECURITY: every server-supplied string is written to the DOM via textContent
// or document.createTextNode ONLY. There is NO innerHTML with interpolation
// anywhere, because the slowlog argv and the client fields are attacker-
// influenceable through a compromised node, so any HTML sink would be an XSS
// vector. The static panel markup lives in index.html; this script only fills
// text and builds elements.
//
// AUTH (follow-up to #360): when the console is exposed/authenticated the
// PRIVILEGED_READ endpoints (/api/nodes, /api/slowlog, /api/clients,
// /api/keyspace) return 401 without a Bearer token. This script reads an
// operator token from sessionStorage (tab-scoped, so it clears on tab close;
// never the persistent web-storage area) and sends it as 'Authorization:
// Bearer <token>' on EVERY /api/* fetch.
// The OPEN endpoints (/api/health, /api/cluster) need no token and always
// render, so the header and cluster overview show even when signed out. A 401
// on a privileged panel reveals the sign-in affordance and marks that panel
// "sign in to view"; a 403 marks it "insufficient privileges". The token is
// held only in sessionStorage and is sent only as a request header: it is
// NEVER inserted into the DOM/HTML, never put in a URL/query, and never logged.
// On the loopback dev default (no token configured) the privileged routes
// return 200 with no token, so everything renders without signing in.

"use strict";

(function () {
  // Poll every 5 seconds (the task's cadence).
  var POLL_MS = 5000;

  // sessionStorage key for the operator token. sessionStorage (the tab-scoped
  // web-storage area, NOT the persistent one) so the token is cleared when the
  // tab closes. The token is read back only to build the Authorization header;
  // it is never written to the DOM and never logged.
  var TOKEN_KEY = "ic_console_token";

  // The endpoints that need a Bearer token when the console is exposed
  // (PRIVILEGED_READ in auth.rs). Used to decide which panels show a "sign in
  // to view" / "insufficient privileges" placeholder on 401 / 403.
  var PRIVILEGED_KEYS = {
    nodes: true,
    slowlog: true,
    clients: true,
    keyspace: true,
  };

  // Read the operator token from sessionStorage, or "" when none is stored.
  // sessionStorage access can throw (e.g. a privacy mode that disables it), so
  // it is guarded; a failure just means "no token" (the dev path still works).
  function getToken() {
    try {
      return window.sessionStorage.getItem(TOKEN_KEY) || "";
    } catch (e) {
      return "";
    }
  }

  // Persist the operator token in sessionStorage. The raw value is never
  // logged. A storage failure is swallowed (the fetch path then runs as if
  // signed out).
  function setToken(token) {
    try {
      window.sessionStorage.setItem(TOKEN_KEY, token);
    } catch (e) {
      // No-op: cannot persist, so the session stays signed out.
    }
  }

  // Remove the operator token from sessionStorage (sign out).
  function clearToken() {
    try {
      window.sessionStorage.removeItem(TOKEN_KEY);
    } catch (e) {
      // No-op.
    }
  }

  // The last-good rendered data, kept so a transient fetch error shows a banner
  // but does NOT blank the dashboard.
  var lastGood = {
    cluster: null,
    nodes: null,
    slowlog: null,
    clients: null,
    keyspace: null,
    health: null,
  };

  // The most recent cluster last_poll_unixtime, used to tick the "last poll age"
  // against the browser clock between fetches.
  var lastPollUnixtime = null;
  var ageTimer = null;

  function byId(id) {
    return document.getElementById(id);
  }

  // Replace all children of a node with a single text node. Safe: textContent
  // assignment never parses HTML.
  function setText(el, text) {
    if (el) {
      el.textContent = text;
    }
  }

  // Build a <td> whose text is `text` (a string), optionally with a class. The
  // text goes through createTextNode, so it is never interpreted as markup.
  function td(text, className) {
    var cell = document.createElement("td");
    if (className) {
      cell.className = className;
    }
    cell.appendChild(document.createTextNode(text == null ? "" : String(text)));
    return cell;
  }

  // A <td> carrying a status pill (reachable / unreachable).
  function pillCell(ok) {
    var cell = document.createElement("td");
    var span = document.createElement("span");
    span.className = ok ? "pill pill-ok" : "pill pill-bad";
    span.appendChild(document.createTextNode(ok ? "yes" : "no"));
    cell.appendChild(span);
    return cell;
  }

  // Replace a <tbody>'s rows with the given row elements, or a single "empty"
  // placeholder row when there are none.
  function fillBody(tbody, rows, colspan, emptyText) {
    if (!tbody) {
      return;
    }
    while (tbody.firstChild) {
      tbody.removeChild(tbody.firstChild);
    }
    if (!rows || rows.length === 0) {
      var tr = document.createElement("tr");
      var cell = td(emptyText || "No data.", "empty");
      cell.colSpan = colspan;
      tr.appendChild(cell);
      tbody.appendChild(tr);
      return;
    }
    for (var i = 0; i < rows.length; i++) {
      tbody.appendChild(rows[i]);
    }
  }

  // Format a byte count into a short human string. Pure number formatting; no
  // server string is interpreted.
  function fmtBytes(n) {
    if (n == null || isNaN(n)) {
      return "-";
    }
    var units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    var v = Number(n);
    var i = 0;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i += 1;
    }
    var s = i === 0 ? String(v) : v.toFixed(1);
    return s + " " + units[i];
  }

  function fmtNum(n) {
    if (n == null || isNaN(n)) {
      return "-";
    }
    return Number(n).toLocaleString();
  }

  function fmtRatio(r) {
    if (r == null || isNaN(r)) {
      return "-";
    }
    return (Number(r) * 100).toFixed(1) + "%";
  }

  // Format a unix-seconds timestamp via the browser locale. The value is a
  // number; the result is plain text inserted with textContent.
  function fmtTime(unixSeconds) {
    if (unixSeconds == null || isNaN(unixSeconds)) {
      return "-";
    }
    var d = new Date(Number(unixSeconds) * 1000);
    return d.toLocaleString();
  }

  // Compute the "last poll N seconds ago" string from the server's
  // last_poll_unixtime against the browser clock.
  function pollAgeText() {
    if (lastPollUnixtime == null) {
      return "-";
    }
    var nowSec = Math.floor(Date.now() / 1000);
    var age = nowSec - lastPollUnixtime;
    if (age < 0) {
      age = 0;
    }
    if (age < 60) {
      return age + "s ago";
    }
    if (age < 3600) {
      return Math.floor(age / 60) + "m ago";
    }
    return Math.floor(age / 3600) + "h ago";
  }

  function updatePollAge() {
    var el = byId("hdr-poll");
    if (!el) {
      return;
    }
    setText(el, pollAgeText());
    // A poll older than 3x the interval is visibly stale.
    if (lastPollUnixtime != null) {
      var age = Math.floor(Date.now() / 1000) - lastPollUnixtime;
      if (age > (POLL_MS / 1000) * 3) {
        el.classList.add("stale");
      } else {
        el.classList.remove("stale");
      }
    }
  }

  function showBanner(message) {
    var b = byId("banner");
    if (!b) {
      return;
    }
    b.className = "banner banner-error";
    setText(b, message);
    b.hidden = false;
  }

  function clearBanner() {
    var b = byId("banner");
    if (b) {
      b.hidden = true;
      setText(b, "");
    }
  }

  function showWaiting(on) {
    var w = byId("waiting");
    if (w) {
      w.hidden = !on;
    }
  }

  // Reveal or hide the sign-in panel. Revealed when a privileged fetch 401s; the
  // operator can also keep it open to sign out. `statusText` is an optional
  // short, NON-secret message (e.g. "insufficient privileges") shown in the
  // panel; it never contains the token.
  function showLogin(on, statusText) {
    var panel = byId("login-panel");
    if (panel) {
      panel.hidden = !on;
    }
    var status = byId("login-status");
    if (status) {
      if (statusText) {
        setText(status, statusText);
        status.hidden = false;
      } else {
        setText(status, "");
        status.hidden = true;
      }
    }
  }

  // The tbody id and column count for each privileged panel, so a 401/403 can
  // render a single-row placeholder ("sign in to view" / "insufficient
  // privileges") instead of an error banner.
  var PRIVILEGED_PANELS = {
    nodes: { body: "nodes-body", cols: 8 },
    slowlog: { body: "slowlog-body", cols: 5 },
    clients: { body: "clients-body", cols: 7 },
    keyspace: { body: "keyspace-body", cols: 4 },
  };

  // Render a privileged panel's placeholder row with a fixed, NON-secret message.
  // Used when a privileged fetch returns 401 (sign in) or 403 (insufficient
  // privileges) so the panel reads as a prompt, not an error.
  function renderPanelPlaceholder(key, message) {
    var panel = PRIVILEGED_PANELS[key];
    if (!panel) {
      return;
    }
    if (key === "keyspace") {
      setText(byId("ks-total"), "-");
    }
    fillBody(byId(panel.body), [], panel.cols, message);
  }

  // Render the cluster overview header + totals cards.
  function renderCluster(data) {
    setText(byId("hdr-mode"), data.mode || "-");
    setText(
      byId("hdr-nodes"),
      (data.nodes_reachable != null ? data.nodes_reachable : "-") +
        " / " +
        (data.nodes_total != null ? data.nodes_total : "-")
    );
    lastPollUnixtime = data.last_poll_unixtime != null ? data.last_poll_unixtime : null;
    updatePollAge();

    var t = data.totals || {};
    setText(byId("t-keys"), fmtNum(t.keys));
    setText(byId("t-mem"), fmtBytes(t.used_memory));
    setText(byId("t-clients"), fmtNum(t.connected_clients));
    var hits = Number(t.keyspace_hits || 0);
    var misses = Number(t.keyspace_misses || 0);
    var ratio = hits + misses > 0 ? hits / (hits + misses) : null;
    setText(byId("t-hit"), fmtRatio(ratio));
    setText(byId("t-evict"), fmtNum(t.evicted_keys));
    setText(byId("t-expire"), fmtNum(t.expired_keys));
  }

  // Render the per-node table from the /api/nodes array.
  function renderNodes(nodes) {
    var rows = [];
    for (var i = 0; i < nodes.length; i++) {
      var n = nodes[i];
      var tr = document.createElement("tr");
      tr.appendChild(td(n.addr, "mono"));
      tr.appendChild(pillCell(!!n.reachable));
      tr.appendChild(td(n.version == null ? "-" : n.version));
      tr.appendChild(td(fmtBytes(n.used_memory), "num"));
      tr.appendChild(td(fmtNum(n.keys), "num"));
      tr.appendChild(td(fmtNum(n.connected_clients), "num"));
      tr.appendChild(td(fmtRatio(n.hit_ratio), "num"));
      tr.appendChild(td(n.error == null ? "" : n.error, "err"));
      rows.push(tr);
    }
    fillBody(byId("nodes-body"), rows, 8, "No nodes.");
  }

  // Render the slowlog table from the /api/slowlog per-node shape. The argv
  // contains KEY NAMES from a possibly-compromised node, so it is joined into a
  // plain string and inserted via textContent only.
  function renderSlowlog(payload) {
    var rows = [];
    var nodes = (payload && payload.nodes) || [];
    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];
      var addr = node.addr;
      if (node.error) {
        var er = document.createElement("tr");
        er.appendChild(td(addr, "mono"));
        var ec = td("error: " + node.error, "err");
        ec.colSpan = 4;
        er.appendChild(ec);
        rows.push(er);
        continue;
      }
      var entries = node.entries || [];
      for (var j = 0; j < entries.length; j++) {
        var e = entries[j];
        var argv = Array.isArray(e.argv) ? e.argv.join(" ") : "";
        var client = e.client_addr || "";
        if (e.client_name) {
          client += " (" + e.client_name + ")";
        }
        var tr = document.createElement("tr");
        tr.appendChild(td(addr, "mono"));
        tr.appendChild(td(fmtTime(e.timestamp)));
        tr.appendChild(td(fmtNum(e.micros), "num"));
        tr.appendChild(td(argv, "cmd"));
        tr.appendChild(td(client, "mono"));
        rows.push(tr);
      }
    }
    fillBody(byId("slowlog-body"), rows, 5, "No slow commands.");
  }

  // Render the clients table from the /api/clients per-node shape.
  function renderClients(payload) {
    var rows = [];
    var nodes = (payload && payload.nodes) || [];
    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];
      var addr = node.addr;
      if (node.error) {
        var er = document.createElement("tr");
        er.appendChild(td(addr, "mono"));
        var ec = td("error: " + node.error, "err");
        ec.colSpan = 6;
        er.appendChild(ec);
        rows.push(er);
        continue;
      }
      var clients = node.clients || [];
      for (var j = 0; j < clients.length; j++) {
        var c = clients[j];
        var tr = document.createElement("tr");
        tr.appendChild(td(addr, "mono"));
        tr.appendChild(td(c.addr == null ? "-" : c.addr, "mono"));
        tr.appendChild(td(c.name == null ? "-" : c.name));
        tr.appendChild(td(c.age == null ? "-" : fmtNum(c.age), "num"));
        tr.appendChild(td(c.idle == null ? "-" : fmtNum(c.idle), "num"));
        tr.appendChild(td(c.cmd == null ? "-" : c.cmd, "cmd"));
        tr.appendChild(td(c.db == null ? "-" : c.db, "num"));
        rows.push(tr);
      }
    }
    fillBody(byId("clients-body"), rows, 7, "No clients.");
  }

  // Render the keyspace panel from /api/keyspace.
  function renderKeyspace(data) {
    setText(byId("ks-total"), fmtNum(data.total_keys));
    var rows = [];
    var perDb = data.per_db || [];
    for (var i = 0; i < perDb.length; i++) {
      var r = perDb[i];
      var tr = document.createElement("tr");
      tr.appendChild(td(r.node, "mono"));
      tr.appendChild(td(r.db));
      tr.appendChild(td(fmtNum(r.keys), "num"));
      tr.appendChild(td(fmtNum(r.expires), "num"));
      rows.push(tr);
    }
    fillBody(byId("keyspace-body"), rows, 4, "No keyspace data.");
  }

  function renderHealth(data) {
    if (data && data.version) {
      setText(byId("hdr-version"), data.version);
    }
  }

  // Fetch one endpoint, returning { status, body } where body is the parsed JSON
  // (or null). Network failures reject so the caller can keep last-good data.
  //
  // When an operator token is stored, it is sent as 'Authorization: Bearer
  // <token>' on EVERY /api/* request. The token goes into a request header only:
  // it is never appended to `path` (no token in a URL/query) and never logged.
  function fetchJson(path) {
    var headers = { Accept: "application/json" };
    var token = getToken();
    if (token) {
      headers.Authorization = "Bearer " + token;
    }
    return fetch(path, {
      headers: headers,
      cache: "no-store",
    }).then(function (resp) {
      return resp
        .json()
        .catch(function () {
          return null;
        })
        .then(function (body) {
          return { status: resp.status, body: body };
        });
    });
  }

  // One full refresh cycle. Health does not depend on a poll; the data
  // endpoints return 503 until the first node poll completes, which is rendered
  // as the "waiting" state (and the last-good data is kept).
  function refresh() {
    fetchJson("/api/health")
      .then(function (r) {
        if (r.status === 200 && r.body) {
          lastGood.health = r.body;
          renderHealth(r.body);
        }
      })
      .catch(function () {
        // Health failing is reported by the data-endpoint banner below.
      });

    // `/api/cluster` is OPEN (no token needed), so it renders in every posture.
    // The other four are PRIVILEGED_READ: on an exposed/authed console they 401
    // without a token and 403 with an under-privileged one.
    var endpoints = [
      { path: "/api/cluster", key: "cluster", render: renderCluster },
      { path: "/api/nodes", key: "nodes", render: renderNodes },
      { path: "/api/slowlog", key: "slowlog", render: renderSlowlog },
      { path: "/api/clients", key: "clients", render: renderClients },
      { path: "/api/keyspace", key: "keyspace", render: renderKeyspace },
    ];

    Promise.all(
      endpoints.map(function (ep) {
        return fetchJson(ep.path)
          .then(function (r) {
            return { ep: ep, status: r.status, body: r.body, err: null };
          })
          .catch(function (e) {
            return { ep: ep, status: 0, body: null, err: e };
          });
      })
    ).then(function (results) {
      var anyNetworkError = false;
      var anyWaiting = false;
      var anyOk = false;
      var anyUnauthorized = false; // a privileged route returned 401
      var anyForbidden = false; // a privileged route returned 403

      for (var i = 0; i < results.length; i++) {
        var res = results[i];
        var key = res.ep.key;
        var privileged = PRIVILEGED_KEYS[key] === true;
        if (res.status === 0) {
          anyNetworkError = true;
          continue;
        }
        if (res.status === 401 && privileged) {
          // Exposed/authed console, no usable token: prompt for sign-in and
          // render the panel as "sign in to view" rather than an error.
          anyUnauthorized = true;
          renderPanelPlaceholder(key, "Sign in to view.");
          continue;
        }
        if (res.status === 403 && privileged) {
          // A valid token of an insufficient tier.
          anyForbidden = true;
          renderPanelPlaceholder(key, "Insufficient privileges.");
          continue;
        }
        if (res.status === 503) {
          anyWaiting = true;
          continue;
        }
        if (res.status === 200 && res.body != null) {
          anyOk = true;
          lastGood[key] = res.body;
          res.ep.render(res.body);
        }
      }

      // Reveal the sign-in panel whenever a privileged route 401'd. A 403 (token
      // present but under-privileged) also keeps it open so the operator can
      // sign out and try another token; show a short, non-secret reason.
      if (anyUnauthorized) {
        showLogin(true, getToken() ? "The stored token was not accepted." : "");
      } else if (anyForbidden) {
        showLogin(true, "The token does not grant the required tier.");
      } else if (anyOk) {
        // A privileged route succeeded, so the current credential works (or the
        // dev loopback path needs none): hide the sign-in prompt.
        showLogin(false);
      }

      // The "waiting for the first poll" state: data endpoints are 503 and we
      // have nothing good yet. Keep retrying (the interval timer continues).
      if (anyWaiting && !anyOk) {
        showWaiting(true);
      } else {
        showWaiting(false);
      }

      // A network error shows a banner but keeps the last-good panels intact. An
      // auth challenge (401/403) is NOT an error banner: it is surfaced through
      // the sign-in panel and the per-panel placeholders above.
      if (anyNetworkError) {
        showBanner(
          "Could not reach the console API; showing the last known data. Retrying every " +
            POLL_MS / 1000 +
            "s."
        );
      } else {
        clearBanner();
      }
    });
  }

  // Wire the sign-in / sign-out controls. The handlers are attached with
  // addEventListener (CSP forbids inline event-handler attributes), never log
  // the token, and never write it to the DOM. After updating sessionStorage they
  // trigger a refresh so the privileged panels re-fetch with (or without) the
  // credential.
  function wireAuthControls() {
    var form = byId("login-form");
    var input = byId("login-token");
    var logout = byId("logout-submit");

    if (form) {
      form.addEventListener("submit", function (ev) {
        ev.preventDefault();
        if (!input) {
          return;
        }
        var token = (input.value || "").trim();
        // Clear the input immediately so the token does not linger in the field.
        input.value = "";
        if (token) {
          setToken(token);
          showLogin(false);
        } else {
          // An empty submit signs out (treat as "no token").
          clearToken();
        }
        refresh();
      });
    }

    if (logout) {
      logout.addEventListener("click", function () {
        clearToken();
        if (input) {
          input.value = "";
        }
        // Keep the panel open with a confirmation; the next privileged 401 will
        // already be reflected by the panel placeholders on the coming refresh.
        showLogin(true, "Signed out.");
        refresh();
      });
    }
  }

  function start() {
    // Wire the sign-in / sign-out controls before the first fetch.
    wireAuthControls();
    // Tick the poll-age display every second so it counts up between fetches.
    if (ageTimer == null) {
      ageTimer = setInterval(updatePollAge, 1000);
    }
    refresh();
    setInterval(refresh, POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
