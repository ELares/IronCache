// SPDX-License-Identifier: MIT OR Apache-2.0
//
// IronCache Console dashboard logic (issue #359), re-skinned to the bespoke
// Butlr design system. Vanilla JS, no framework, no build step, no external
// fetch (a strict CSP default-src 'self' must run this with no 'unsafe-inline'
// and no CDN).
//
// SECURITY: every server-supplied string is written to the DOM via textContent
// or document.createTextNode ONLY, and every element (including the inline-SVG
// sparkline) is built with document.createElement / createElementNS. None of the
// raw-HTML sinks (the inner/outer-HTML setters, the insert-adjacent-HTML method,
// or the document writer) is used anywhere, because the slowlog argv and the
// client fields are attacker-influenceable through a compromised node, so any
// HTML sink would be an XSS vector.
//
// CSP-CLEAN DYNAMIC STYLING: no inline style="" attribute is ever written.
// Dynamic values (the per-node memory-bar fraction, the sparkline geometry) are
// driven through CSS custom properties via element.style.setProperty('--x', v)
// (CSSOM, which the CSP allows) referenced by app.css, or through classList
// toggling, or via SVG element nodes built with createElementNS. The theme is a
// data-theme attribute on <html>.
//
// AUTH (follow-up to #360): when the console is exposed/authenticated the
// PRIVILEGED_READ endpoints (/api/nodes, /api/slowlog, /api/clients,
// /api/keyspace) return 401 without a Bearer token. This script reads an
// operator token from sessionStorage (tab-scoped; never the persistent
// web-storage area) and sends it as 'Authorization: Bearer <token>' on EVERY
// /api/* fetch. The OPEN endpoints (/api/health, /api/cluster) need no token and
// always render. A 401 on a privileged view reveals the sign-in affordance and
// marks that view "sign in to view"; a 403 marks it "insufficient privileges".
// The token is held only in sessionStorage and is sent only as a request
// header: it is NEVER inserted into the DOM/HTML, never put in a URL/query, and
// never logged.

"use strict";

(function () {
  // Poll every 5 seconds (the task's cadence).
  var POLL_MS = 5000;

  // sessionStorage key for the operator token (tab-scoped; cleared on tab
  // close). Read back only to build the Authorization header; never DOM'd or
  // logged.
  var TOKEN_KEY = "ic_console_token";

  // sessionStorage key for the chosen theme ("light" / "dark"). A non-secret UI
  // preference; kept in the tab-scoped web-storage area (the same one the token
  // uses, never the persistent area) to keep the served script free of the
  // persistent web-storage API.
  var THEME_KEY = "ic_console_theme";

  var SVG_NS = "http://www.w3.org/2000/svg";

  // The endpoints that need a Bearer token when the console is exposed
  // (PRIVILEGED_READ in auth.rs).
  var PRIVILEGED_KEYS = {
    nodes: true,
    slowlog: true,
    clients: true,
    keyspace: true,
  };

  // The per-section page title + subtitle shown in the topbar.
  var SECTION_META = {
    overview: { title: "Overview", subtitle: "Live cluster metrics" },
    nodes: { title: "Nodes", subtitle: "Per-node health and capacity" },
    slowlog: { title: "Slowlog", subtitle: "Slowest recent commands" },
    clients: { title: "Clients", subtitle: "Connected clients" },
    keyspace: { title: "Keyspace", subtitle: "Keys per database" },
    cluster: { title: "Cluster", subtitle: "Slot map and roster" },
    replication: { title: "Replication", subtitle: "Replica streams" },
    shards: { title: "Shards", subtitle: "Per-shard breakdown" },
    console: { title: "Console", subtitle: "Interactive commands" },
    pubsub: { title: "Pub/Sub", subtitle: "Channels and subscribers" },
    config: { title: "Config", subtitle: "Runtime configuration" },
    acl: { title: "ACL", subtitle: "Users and permissions" },
  };

  // ----- token storage (auth) ----------------------------------------------
  function getToken() {
    try {
      return window.sessionStorage.getItem(TOKEN_KEY) || "";
    } catch (e) {
      return "";
    }
  }

  function setToken(token) {
    try {
      window.sessionStorage.setItem(TOKEN_KEY, token);
    } catch (e) {
      // No-op: cannot persist, so the session stays signed out.
    }
  }

  function clearToken() {
    try {
      window.sessionStorage.removeItem(TOKEN_KEY);
    } catch (e) {
      // No-op.
    }
  }

  // ----- runtime state ------------------------------------------------------
  var lastGood = {
    cluster: null,
    nodes: null,
    slowlog: null,
    clients: null,
    keyspace: null,
    health: null,
  };

  // The currently selected section (client-side nav).
  var activeSection = "overview";

  // Whether live polling is on (the Live toggle). When off, the interval still
  // exists but skips the network work.
  var liveOn = true;
  var pollTimer = null;

  // The rolling ops/second buffer for the sparkline (last ~60s => 12 samples at
  // the 5s cadence). Each entry is a derived ops/s rate.
  var SPARK_MAX = 12;
  var opsBuffer = [];
  // The previous commands_processed counter + the wall-clock ms it was read, so
  // the next poll can difference them into an ops/s rate (the honest derivation
  // the task calls for; the counter itself is the new backend field).
  var prevCommands = null;
  var prevCommandsAt = null;

  function byId(id) {
    return document.getElementById(id);
  }

  function setText(el, text) {
    if (el) {
      el.textContent = text;
    }
  }

  // ----- formatting (pure number/text; no server string is interpreted) -----
  function fmtBytes(n) {
    if (n == null || isNaN(n)) {
      return { value: "-", unit: "" };
    }
    var units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    var v = Number(n);
    var i = 0;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i += 1;
    }
    var s = i === 0 ? String(v) : v.toFixed(1);
    return { value: s, unit: units[i] };
  }

  function fmtBytesShort(n) {
    var b = fmtBytes(n);
    if (b.unit === "") {
      return b.value;
    }
    return b.value + " " + b.unit;
  }

  function fmtNum(n) {
    if (n == null || isNaN(n)) {
      return "-";
    }
    return Number(n).toLocaleString();
  }

  function fmtRatioPct(r) {
    if (r == null || isNaN(r)) {
      return "-";
    }
    return (Number(r) * 100).toFixed(1);
  }

  function fmtRatio(r) {
    if (r == null || isNaN(r)) {
      return "-";
    }
    return (Number(r) * 100).toFixed(1) + "%";
  }

  function fmtTime(unixSeconds) {
    if (unixSeconds == null || isNaN(unixSeconds)) {
      return "-";
    }
    var d = new Date(Number(unixSeconds) * 1000);
    return d.toLocaleString();
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

  // A <td> carrying a reachable/unreachable status pill.
  function pillCell(ok) {
    var cell = document.createElement("td");
    var span = document.createElement("span");
    span.className = ok ? "pill pill-ok" : "pill pill-bad";
    span.appendChild(document.createTextNode(ok ? "reachable" : "down"));
    cell.appendChild(span);
    return cell;
  }

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

  // ----- banners / waiting / login -----------------------------------------
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

  var PRIVILEGED_PANELS = {
    nodes: { body: "nodes-body", cols: 8 },
    slowlog: { body: "slowlog-body", cols: 5 },
    clients: { body: "clients-body", cols: 7 },
    keyspace: { body: "keyspace-body", cols: 4 },
  };

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

  // ----- overview: metric cards + nodes summary + sparkline -----------------
  function deriveOps(commands) {
    // Difference the cumulative commands_processed counter against the previous
    // poll to get an ops/second rate (the honest derivation: a per-second rate
    // from two cumulative samples and the elapsed wall time).
    if (commands == null || isNaN(commands)) {
      return null;
    }
    var nowMs = Date.now();
    var ops = null;
    if (prevCommands != null && prevCommandsAt != null) {
      var dt = (nowMs - prevCommandsAt) / 1000;
      var dc = Number(commands) - prevCommands;
      if (dt > 0 && dc >= 0) {
        ops = dc / dt;
      }
    }
    prevCommands = Number(commands);
    prevCommandsAt = nowMs;
    return ops;
  }

  function pushOps(ops) {
    if (ops == null || isNaN(ops)) {
      return;
    }
    opsBuffer.push(ops);
    while (opsBuffer.length > SPARK_MAX) {
      opsBuffer.shift();
    }
  }

  // Render the sparkline as inline SVG built with createElementNS (element nodes,
  // never a raw-HTML sink). The viewBox is 600x160 (from index.html); geometry
  // is computed in those units. A path is built only with two or more samples.
  function renderSparkline() {
    var svg = byId("spark");
    var empty = byId("spark-empty");
    if (!svg) {
      return;
    }
    while (svg.firstChild) {
      svg.removeChild(svg.firstChild);
    }
    if (opsBuffer.length < 2) {
      if (empty) {
        empty.hidden = false;
      }
      return;
    }
    if (empty) {
      empty.hidden = true;
    }
    var W = 600;
    var H = 160;
    var pad = 8;
    var max = 0;
    for (var i = 0; i < opsBuffer.length; i++) {
      if (opsBuffer[i] > max) {
        max = opsBuffer[i];
      }
    }
    if (max <= 0) {
      max = 1;
    }
    var n = opsBuffer.length;
    var stepX = (W - pad * 2) / (n - 1);
    var coords = [];
    for (var j = 0; j < n; j++) {
      var x = pad + stepX * j;
      var y = H - pad - (opsBuffer[j] / max) * (H - pad * 2);
      coords.push([x, y]);
    }
    var line = "";
    var fill = "M " + coords[0][0].toFixed(1) + " " + (H - pad).toFixed(1);
    for (var k = 0; k < coords.length; k++) {
      var cmd = k === 0 ? "M" : "L";
      line += (k === 0 ? "" : " ") + cmd + " " + coords[k][0].toFixed(1) + " " + coords[k][1].toFixed(1);
      fill += " L " + coords[k][0].toFixed(1) + " " + coords[k][1].toFixed(1);
    }
    fill += " L " + coords[coords.length - 1][0].toFixed(1) + " " + (H - pad).toFixed(1) + " Z";

    var fillPath = document.createElementNS(SVG_NS, "path");
    fillPath.setAttribute("d", fill);
    fillPath.setAttribute("class", "spark-fill");
    svg.appendChild(fillPath);

    var linePath = document.createElementNS(SVG_NS, "path");
    linePath.setAttribute("d", line);
    linePath.setAttribute("class", "spark-line");
    svg.appendChild(linePath);
  }

  // Build one node-summary row for the overview "Nodes" card. `maxMem` is the
  // largest used_memory across nodes, so the bar is relative within the cluster.
  function nodeSummaryRow(node, maxMem) {
    var row = document.createElement("div");
    row.className = "node-row";

    var chip = document.createElement("span");
    chip.className = "node-chip";
    if (!node.reachable) {
      chip.classList.add("node-chip-down");
    }
    row.appendChild(chip);

    var main = document.createElement("div");
    main.className = "node-row-main";
    var idLine = document.createElement("span");
    idLine.className = "node-row-id mono";
    idLine.appendChild(document.createTextNode(node.addr == null ? "-" : node.addr));
    main.appendChild(idLine);

    var meta = document.createElement("div");
    meta.className = "node-row-meta";
    var role = document.createElement("span");
    // Standalone today; the role is honest (no fabricated leader/replica).
    role.className = "role-pill role-standalone";
    role.appendChild(document.createTextNode("standalone"));
    meta.appendChild(role);

    var bar = document.createElement("span");
    bar.className = "mem-bar";
    var barFill = document.createElement("span");
    barFill.className = "mem-bar-fill";
    var frac = 0;
    if (maxMem > 0 && node.used_memory != null) {
      frac = Math.max(0, Math.min(1, Number(node.used_memory) / maxMem));
    }
    // CSP-clean dynamic styling: set the fraction as a CSS custom property; the
    // width is computed from it in app.css (no inline style attribute).
    barFill.style.setProperty("--mem-frac", String(frac));
    bar.appendChild(barFill);
    meta.appendChild(bar);
    main.appendChild(meta);
    row.appendChild(main);

    var side = document.createElement("div");
    side.className = "node-row-side";
    var status = document.createElement("span");
    status.className = node.reachable ? "pill pill-ok" : "pill pill-bad";
    status.appendChild(document.createTextNode(node.reachable ? "up" : "down"));
    side.appendChild(status);
    var memText = document.createElement("span");
    memText.className = "node-row-ops";
    memText.appendChild(
      document.createTextNode(node.used_memory == null ? "-" : fmtBytesShort(node.used_memory))
    );
    side.appendChild(memText);
    row.appendChild(side);
    return row;
  }

  function renderNodeSummary(nodes) {
    var container = byId("node-rows");
    if (!container) {
      return;
    }
    while (container.firstChild) {
      container.removeChild(container.firstChild);
    }
    setText(byId("nodes-summary-count"), nodes.length + (nodes.length === 1 ? " node" : " nodes"));
    if (!nodes || nodes.length === 0) {
      var p = document.createElement("p");
      p.className = "placeholder";
      p.appendChild(document.createTextNode("No nodes."));
      container.appendChild(p);
      return;
    }
    var maxMem = 0;
    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].used_memory != null && Number(nodes[i].used_memory) > maxMem) {
        maxMem = Number(nodes[i].used_memory);
      }
    }
    for (var j = 0; j < nodes.length; j++) {
      container.appendChild(nodeSummaryRow(nodes[j], maxMem));
    }
  }

  // ----- cluster pill + topbar state ---------------------------------------
  function renderClusterPill(data) {
    var dot = byId("cluster-dot");
    var label = byId("cluster-pill-label");
    var count = byId("cluster-pill-count");
    var reachable = data.nodes_reachable != null ? data.nodes_reachable : 0;
    var total = data.nodes_total != null ? data.nodes_total : 0;
    if (dot) {
      dot.className = "dot " + (reachable > 0 ? "dot-ok" : "dot-bad");
    }
    if (label) {
      setText(label, data.mode === "clustered" ? "Clustered" : "Standalone");
    }
    if (count) {
      setText(count, reachable + "/" + total);
    }
  }

  // ----- renderers (one per /api/* endpoint) -------------------------------
  function renderCluster(data) {
    renderClusterPill(data);

    var t = data.totals || {};

    // Throughput: derive ops/s from the cumulative commands_processed counter.
    // The first poll has no previous sample to difference, so it shows 0 until
    // the second poll establishes a rate.
    var ops = deriveOps(t.commands_processed);
    var throughputEl = byId("m-throughput");
    if (throughputEl) {
      if (ops != null) {
        setText(throughputEl, Math.round(ops).toLocaleString());
      } else if (throughputEl.textContent === "-") {
        setText(throughputEl, "0");
      }
    }
    pushOps(ops == null ? 0 : ops);
    renderSparkline();

    // Hit rate.
    var hits = Number(t.keyspace_hits || 0);
    var misses = Number(t.keyspace_misses || 0);
    var ratio = hits + misses > 0 ? hits / (hits + misses) : null;
    setText(byId("m-hitrate"), fmtRatioPct(ratio));

    // Memory.
    var mem = fmtBytes(t.used_memory);
    setText(byId("m-memory"), mem.value);
    setText(byId("m-memory-unit"), mem.unit);

    // Keys + clients.
    setText(byId("m-keys"), fmtNum(t.keys));
    setText(byId("m-clients"), fmtNum(t.connected_clients));
  }

  function renderNodes(nodes) {
    // The overview's node-summary card + the Nodes table both come from here.
    renderNodeSummary(nodes);

    var rows = [];
    for (var i = 0; i < nodes.length; i++) {
      var n = nodes[i];
      var tr = document.createElement("tr");
      tr.appendChild(td(n.addr, "mono"));
      tr.appendChild(pillCell(!!n.reachable));
      tr.appendChild(td(n.version == null ? "-" : n.version));
      tr.appendChild(td(n.used_memory == null ? "-" : fmtBytesShort(n.used_memory), "num"));
      tr.appendChild(td(fmtNum(n.keys), "num"));
      tr.appendChild(td(fmtNum(n.connected_clients), "num"));
      tr.appendChild(td(fmtRatio(n.hit_ratio), "num"));
      tr.appendChild(td(n.error == null ? "" : n.error, "err"));
      rows.push(tr);
    }
    fillBody(byId("nodes-body"), rows, 8, "No nodes.");
    populateNodeSelect(nodes);
  }

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
      setText(byId("sidebar-version"), data.version);
    }
  }

  // Populate the topbar node selector from the live node list (the addresses are
  // server strings, so each option's text is set via textContent through the
  // Option constructor's first arg, which is a text label, not markup).
  function populateNodeSelect(nodes) {
    var sel = byId("node-select");
    if (!sel) {
      return;
    }
    var current = sel.value;
    while (sel.firstChild) {
      sel.removeChild(sel.firstChild);
    }
    var all = document.createElement("option");
    all.value = "all";
    all.appendChild(document.createTextNode("All nodes"));
    sel.appendChild(all);
    for (var i = 0; i < nodes.length; i++) {
      var opt = document.createElement("option");
      opt.value = nodes[i].addr == null ? "" : nodes[i].addr;
      opt.appendChild(document.createTextNode(nodes[i].addr == null ? "-" : nodes[i].addr));
      sel.appendChild(opt);
    }
    // Keep the prior selection if it still exists.
    var keep = false;
    for (var j = 0; j < sel.options.length; j++) {
      if (sel.options[j].value === current) {
        keep = true;
        break;
      }
    }
    sel.value = keep ? current : "all";
  }

  // ----- fetch --------------------------------------------------------------
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

  function refresh() {
    if (!liveOn) {
      return;
    }
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
      var anyUnauthorized = false;
      var anyForbidden = false;

      for (var i = 0; i < results.length; i++) {
        var res = results[i];
        var key = res.ep.key;
        var privileged = PRIVILEGED_KEYS[key] === true;
        if (res.status === 0) {
          anyNetworkError = true;
          continue;
        }
        if (res.status === 401 && privileged) {
          anyUnauthorized = true;
          renderPanelPlaceholder(key, "Sign in to view.");
          continue;
        }
        if (res.status === 403 && privileged) {
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

      if (anyUnauthorized) {
        showLogin(true, getToken() ? "The stored token was not accepted." : "");
      } else if (anyForbidden) {
        showLogin(true, "The token does not grant the required tier.");
      } else if (anyOk) {
        showLogin(false);
      }

      if (anyWaiting && !anyOk) {
        showWaiting(true);
      } else {
        showWaiting(false);
      }

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

  // ----- navigation (client-side section switch) ---------------------------
  function setActiveSection(section) {
    if (!SECTION_META[section]) {
      section = "overview";
    }
    activeSection = section;

    var items = document.getElementsByClassName("nav-item");
    for (var i = 0; i < items.length; i++) {
      if (items[i].getAttribute("data-section") === section) {
        items[i].classList.add("active");
      } else {
        items[i].classList.remove("active");
      }
    }

    var panels = document.querySelectorAll("[data-section-panel]");
    for (var j = 0; j < panels.length; j++) {
      panels[j].hidden = panels[j].getAttribute("data-section-panel") !== section;
    }

    var meta = SECTION_META[section];
    setText(byId("page-title"), meta.title);
    setText(byId("page-subtitle"), meta.subtitle);
  }

  function wireNav() {
    var items = document.getElementsByClassName("nav-item");
    for (var i = 0; i < items.length; i++) {
      (function (el) {
        el.addEventListener("click", function () {
          setActiveSection(el.getAttribute("data-section"));
        });
      })(items[i]);
    }
  }

  // ----- theme --------------------------------------------------------------
  function applyTheme(theme) {
    var root = document.documentElement;
    if (theme === "dark") {
      root.setAttribute("data-theme", "dark");
    } else {
      root.removeAttribute("data-theme");
    }
  }

  function storedTheme() {
    try {
      return window.sessionStorage.getItem(THEME_KEY) || "";
    } catch (e) {
      return "";
    }
  }

  function storeTheme(theme) {
    try {
      window.sessionStorage.setItem(THEME_KEY, theme);
    } catch (e) {
      // No-op: a privacy mode may disable storage; the theme stays for the page
      // lifetime via the data-theme attribute regardless.
    }
  }

  function initTheme() {
    var theme = storedTheme();
    if (theme !== "dark" && theme !== "light") {
      // Default to the OS preference on first load.
      theme =
        window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
          ? "dark"
          : "light";
    }
    applyTheme(theme);
  }

  function wireThemeToggle() {
    var btn = byId("theme-toggle");
    if (!btn) {
      return;
    }
    btn.addEventListener("click", function () {
      var isDark = document.documentElement.getAttribute("data-theme") === "dark";
      var next = isDark ? "light" : "dark";
      applyTheme(next);
      storeTheme(next);
      // Re-stroke the sparkline so its theme-conditional color refreshes.
      renderSparkline();
    });
  }

  // ----- live toggle + manual refresh --------------------------------------
  function wireLiveToggle() {
    var btn = byId("live-toggle");
    if (!btn) {
      return;
    }
    btn.addEventListener("click", function () {
      liveOn = !liveOn;
      btn.setAttribute("aria-pressed", liveOn ? "true" : "false");
      if (liveOn) {
        refresh();
      }
    });
  }

  function wireRefresh() {
    var btn = byId("refresh-btn");
    if (!btn) {
      return;
    }
    btn.addEventListener("click", function () {
      // A manual refresh runs even when live is paused (a one-shot poll).
      var wasLive = liveOn;
      liveOn = true;
      refresh();
      liveOn = wasLive;
    });
  }

  // ----- auth controls ------------------------------------------------------
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
        input.value = "";
        if (token) {
          setToken(token);
          showLogin(false);
        } else {
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
        showLogin(true, "Signed out.");
        refresh();
      });
    }
  }

  function start() {
    initTheme();
    wireNav();
    wireThemeToggle();
    wireLiveToggle();
    wireRefresh();
    wireAuthControls();
    setActiveSection("overview");
    refresh();
    pollTimer = setInterval(refresh, POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
