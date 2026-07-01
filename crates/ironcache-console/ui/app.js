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

  // Raise the staleness banner once the server-reported topology age exceeds this
  // many seconds (#354): the console's own poll loop refreshes every ~5s, so 4 missed
  // cycles means it is likely stuck and the view should NOT be trusted as live. Tuned
  // for the default 5s server poll; a much slower configured poll could trip it.
  var STALE_AFTER_S = (POLL_MS / 1000) * 4;
  // The latest server-reported topology age, captured by renderCluster and consulted
  // by the banner decision in refresh().
  var lastTopologyAgeSeconds = 0;

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
    persistence: { title: "Persistence", subtitle: "Snapshots and saves" },
  };

  // The management sections (#361): loaded on demand when navigated to (not on the
  // 5s live poll), since each one opens an on-demand node connection. The active
  // management section is also refreshed by the manual Refresh button.
  var MANAGEMENT_SECTIONS = {
    cluster: true,
    config: true,
    keyspace: true,
    console: true,
    pubsub: true,
    acl: true,
    persistence: true,
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

  // Whether ANY token is held this tab. The console cannot tell read vs admin
  // client-side (the token is opaque), so admin-gated controls are revealed
  // optimistically when a token is present, and a 401/403 from a mutation surfaces
  // the precise reason (and the sign-in) afterward. With NO token on the loopback
  // dev default the server still serves every tier, so the controls work too.
  function haveToken() {
    return getToken().length > 0;
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
  };

  function renderPanelPlaceholder(key, message) {
    var panel = PRIVILEGED_PANELS[key];
    if (!panel) {
      return;
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

    // Capture the server-reported topology age for the staleness banner (#354). The
    // banner itself is raised in refresh() so it shows on EVERY tab, not just Cluster.
    lastTopologyAgeSeconds = Number((data && data.topology_age_seconds) || 0);

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

  // ======================================================================
  // Node-level MANAGEMENT (#361). Each page loads on demand when navigated to
  // (loadManagement), and the admin write controls post through fetchMethod.
  // Every server string reaches the DOM via textContent / createTextNode only.
  // ======================================================================

  // ----- Cluster: rebalance plan (admin, on-demand) ------------------------
  // GET /api/cluster/rebalance-plan returns the engine CLUSTER REBALANCE DRYRUN
  // (#361/#444): per-node current vs balanced-target slots + signed move. READ-ONLY
  // (no slots move) and Admin-tier, loaded explicitly on the button.

  function updateRebalanceGate() {
    var gate = byId("rebalance-gate");
    if (gate) {
      gate.hidden = haveToken();
    }
  }

  function loadRebalancePlan() {
    var status = byId("rebalance-status");
    if (status) {
      status.hidden = false;
      setText(status, "Loading plan...");
    }
    fetchJson("/api/cluster/rebalance-plan").then(function (r) {
      if (status) {
        status.hidden = true;
      }
      if (r.status === 401 || r.status === 403) {
        renderRebalancePlan(
          null,
          r.status === 401 ? "Sign in to load the rebalance plan." : "Insufficient privileges."
        );
        return;
      }
      if (r.status === 200 && r.body && Array.isArray(r.body.targets)) {
        renderRebalancePlan(r.body, null);
      } else {
        renderRebalancePlan(null, apiError(r, "Could not load the rebalance plan."));
      }
    });
  }

  function renderRebalancePlan(data, message) {
    var summary = byId("rebalance-summary");
    if (message) {
      setText(summary, "-");
      fillBody(byId("rebalance-body"), [], 4, message);
      return;
    }
    var targets = (data && data.targets) || [];
    if (data && data.balanced) {
      setText(summary, "Balanced (nothing to move)");
    } else {
      setText(summary, fmtNum(data ? data.total_slots_to_move : 0) + " slots to move");
    }
    var rows = [];
    for (var i = 0; i < targets.length; i++) {
      var tgt = targets[i];
      var tr = document.createElement("tr");
      tr.appendChild(td(tgt.node == null ? "" : String(tgt.node), "mono"));
      tr.appendChild(td(fmtNum(tgt.current_slots), "num"));
      tr.appendChild(td(fmtNum(tgt.target_slots), "num"));
      tr.appendChild(td(fmtMove(tgt.slots_to_move), "num"));
      rows.push(tr);
    }
    fillBody(byId("rebalance-body"), rows, 4, "No nodes.");
  }

  // Format a signed slot-move count: "+N" receives, "-N" sheds, "0" settled.
  function fmtMove(n) {
    var v = Number(n || 0);
    return (v > 0 ? "+" : "") + v.toLocaleString();
  }

  // ----- Cluster: failover (admin, destructive) ----------------------------
  // POST /api/cluster/failover (#361): trigger a bare CLUSTER FAILOVER, gated by a
  // typed confirmation. The engine refuses it unless this node is an in-sync replica,
  // so a 502 carries the node's reason; we surface it verbatim.

  function updateFailoverGate() {
    var gate = byId("failover-gate");
    if (gate) {
      gate.hidden = haveToken();
    }
  }

  function triggerFailover() {
    var input = byId("failover-confirm");
    var status = byId("failover-status");
    var confirm = input ? (input.value || "").trim() : "";
    if (status) {
      status.hidden = false;
      setText(status, "Requesting failover...");
    }
    fetchMethod("POST", "/api/cluster/failover", { confirm: confirm }).then(function (r) {
      if (input) {
        input.value = "";
      }
      if (!status) {
        return;
      }
      status.hidden = false;
      if (r.status === 200) {
        setText(status, "Failover proposed.");
      } else {
        // 400 = missing/wrong confirmation; 502 = the node refused (e.g. not in-sync).
        setText(status, apiError(r, "Failover failed."));
      }
    });
  }

  // POST /api/cluster/rebalance (#361 over engine #371): arm a planned rebalance
  // (CLUSTER REBALANCE APPLY), gated by a typed confirmation. It arms MIGRATING/IMPORTING
  // (the engine auto-copies via HA-6) but does NOT flip ownership, so it cannot lose a write.
  function triggerRebalanceApply() {
    var input = byId("rebalance-confirm");
    var status = byId("rebalance-status");
    var confirm = input ? (input.value || "").trim() : "";
    if (status) {
      status.hidden = false;
      setText(status, "Arming rebalance...");
    }
    fetchMethod("POST", "/api/cluster/rebalance", { confirm: confirm }).then(function (r) {
      if (input) {
        input.value = "";
      }
      if (!status) {
        return;
      }
      status.hidden = false;
      if (r.status === 200) {
        setText(status, "Rebalance armed. Finalize each slot with SETSLOT NODE once caught up.");
      } else {
        // 400 = missing/wrong confirmation; 502 = the node refused (e.g. cluster disabled).
        setText(status, apiError(r, "Rebalance failed."));
      }
    });
  }

  // ----- Cluster: node membership (admin) ----------------------------------
  // POST /api/cluster/meet (add, additive) and /api/cluster/forget (remove,
  // destructive: the operator types the EXACT node id, which the UI echoes as confirm).

  function updateMembershipGates() {
    var have = haveToken();
    var mg = byId("meet-gate");
    if (mg) {
      mg.hidden = have;
    }
    var fg = byId("forget-gate");
    if (fg) {
      fg.hidden = have;
    }
  }

  function addNode() {
    var host = byId("meet-host");
    var port = byId("meet-port");
    var status = byId("meet-status");
    var h = host ? (host.value || "").trim() : "";
    var p = port ? parseInt(port.value, 10) : 0;
    if (!(p >= 1 && p <= 65535)) {
      p = 0; // an empty / out-of-range port -> 0, which the server answers with a 400
    }
    if (status) {
      status.hidden = false;
      setText(status, "Adding node...");
    }
    fetchMethod("POST", "/api/cluster/meet", { host: h, port: p }).then(function (r) {
      if (!status) {
        return;
      }
      status.hidden = false;
      if (r.status === 200) {
        setText(status, "Node added.");
        if (host) {
          host.value = "";
        }
        if (port) {
          port.value = "";
        }
      } else {
        setText(status, apiError(r, "Add node failed."));
      }
    });
  }

  function removeNode() {
    var input = byId("forget-node-id");
    var status = byId("forget-status");
    var id = input ? (input.value || "").trim() : "";
    if (status) {
      status.hidden = false;
      setText(status, "Removing node...");
    }
    // Typing the EXACT node id IS the human confirmation; echo it as confirm so the
    // server's "confirm must match node_id" rail passes for a deliberate UI action.
    fetchMethod("POST", "/api/cluster/forget", { node_id: id, confirm: id }).then(function (r) {
      if (input) {
        input.value = "";
      }
      if (!status) {
        return;
      }
      status.hidden = false;
      if (r.status === 200) {
        setText(status, "Node forgotten.");
      } else {
        setText(status, apiError(r, "Remove node failed."));
      }
    });
  }

  // ----- Cluster: migrate slot (admin, destructive) ------------------------
  // POST /api/cluster/setslot (#361): the online-migration / FLIP control. The operator
  // entering the EXACT slot IS the confirmation; the UI echoes it as confirm.

  function updateSetslotGate() {
    var gate = byId("setslot-gate");
    if (gate) {
      gate.hidden = haveToken();
    }
  }

  function applySetslot() {
    var slotEl = byId("setslot-slot");
    var actionEl = byId("setslot-action");
    var nodeEl = byId("setslot-node-id");
    var status = byId("setslot-status");
    var slot = slotEl ? parseInt(slotEl.value, 10) : NaN;
    var valid = slot >= 0 && slot <= 16383;
    var action = actionEl ? actionEl.value || "" : "";
    var nodeId = nodeEl ? (nodeEl.value || "").trim() : "";
    if (status) {
      status.hidden = false;
      setText(status, "Applying slot transition...");
    }
    // An invalid slot is sent as null (a server 400); a valid one echoes itself as confirm.
    var body = { slot: valid ? slot : null, action: action, confirm: valid ? String(slot) : "" };
    if (action !== "STABLE") {
      body.node_id = nodeId;
    }
    fetchMethod("POST", "/api/cluster/setslot", body).then(function (r) {
      if (!status) {
        return;
      }
      status.hidden = false;
      if (r.status === 200) {
        setText(status, "Slot transition applied.");
      } else {
        setText(status, apiError(r, "Slot transition failed."));
      }
    });
  }

  // ----- Config -------------------------------------------------------------
  var configParams = [];

  function loadConfig() {
    fetchJson("/api/config").then(function (r) {
      if (r.status === 401 || r.status === 403) {
        renderConfigRows([], r.status === 401 ? "Sign in to view configuration." : "Insufficient privileges.");
        return;
      }
      if (r.status === 200 && r.body && Array.isArray(r.body.params)) {
        configParams = r.body.params;
        setText(byId("config-count"), configParams.length + " parameters");
        renderConfigRows(configParams, null);
      } else {
        renderConfigRows([], "Could not load configuration.");
      }
    });
  }

  function renderConfigRows(params, message) {
    var host = byId("config-rows");
    if (!host) {
      return;
    }
    while (host.firstChild) {
      host.removeChild(host.firstChild);
    }
    var filterEl = byId("config-filter");
    var filter = filterEl ? (filterEl.value || "").toLowerCase() : "";
    if (message) {
      var p = document.createElement("p");
      p.className = "placeholder";
      p.appendChild(document.createTextNode(message));
      host.appendChild(p);
      return;
    }
    var shown = 0;
    for (var i = 0; i < params.length; i++) {
      var param = params[i];
      var name = param.param == null ? "" : String(param.param);
      if (filter && name.toLowerCase().indexOf(filter) === -1) {
        continue;
      }
      host.appendChild(configRow(name, param.value == null ? "" : String(param.value)));
      shown += 1;
    }
    if (shown === 0) {
      var none = document.createElement("p");
      none.className = "placeholder";
      none.appendChild(document.createTextNode("No matching parameters."));
      host.appendChild(none);
    }
  }

  function configRow(name, value) {
    var row = document.createElement("div");
    row.className = "config-row";
    var label = document.createElement("span");
    label.className = "config-param mono";
    label.appendChild(document.createTextNode(name));
    row.appendChild(label);
    var input = document.createElement("input");
    input.className = "ks-input mono config-value-input";
    input.type = "text";
    input.value = value;
    input.setAttribute("aria-label", name);
    row.appendChild(input);
    var apply = document.createElement("button");
    apply.className = "btn config-apply";
    apply.type = "button";
    apply.appendChild(document.createTextNode("Apply"));
    apply.addEventListener("click", function () {
      applyConfig(name, input.value);
    });
    row.appendChild(apply);
    return row;
  }

  function applyConfig(param, value) {
    setStatus("config-status", "Applying " + param + "...", "ok");
    fetchMethod("POST", "/api/config", { param: param, value: value }).then(function (r) {
      if (handleAuthFailure("config-status", r.status)) {
        return;
      }
      if (r.status === 200 && r.body && r.body.ok) {
        setStatus("config-status", "Applied " + param + ".", "ok");
      } else {
        setStatus("config-status", apiError(r, "Could not apply " + param + "."), "err");
      }
    });
  }

  // ----- Keyspace (browser + inspector + actions) ---------------------------
  var ksCursor = "0";
  var ksPattern = "*";
  var ksSelectedKey = null;

  function runScan(reset) {
    var patternEl = byId("ks-pattern");
    if (reset) {
      ksPattern = patternEl && patternEl.value ? patternEl.value : "*";
      ksCursor = "0";
      clearKeyList();
    }
    var url =
      "/api/keys?pattern=" +
      encodeURIComponent(ksPattern) +
      "&cursor=" +
      encodeURIComponent(ksCursor) +
      "&count=100";
    fetchJson(url).then(function (r) {
      if (r.status === 401 || r.status === 403) {
        setStatus("ks-browse-status", r.status === 401 ? "Sign in to browse keys." : "Insufficient privileges.", "err");
        return;
      }
      if (r.status === 200 && r.body && Array.isArray(r.body.keys)) {
        setStatus("ks-browse-status", null);
        appendKeys(r.body.keys);
        ksCursor = r.body.cursor == null ? "0" : String(r.body.cursor);
        var more = byId("ks-scan-more");
        if (more) {
          more.hidden = ksCursor === "0";
        }
      } else {
        setStatus("ks-browse-status", apiError(r, "Scan failed."), "err");
      }
    });
  }

  function clearKeyList() {
    var host = byId("ks-key-list");
    if (host) {
      while (host.firstChild) {
        host.removeChild(host.firstChild);
      }
    }
  }

  function appendKeys(keys) {
    var host = byId("ks-key-list");
    if (!host) {
      return;
    }
    // Drop a leading placeholder if present.
    var placeholder = host.querySelector(".placeholder");
    if (placeholder) {
      host.removeChild(placeholder);
    }
    var count = host.getElementsByClassName("ks-key-item").length + keys.length;
    setText(byId("ks-scan-count"), count + (count === 1 ? " key" : " keys"));
    if (keys.length === 0 && host.children.length === 0) {
      var none = document.createElement("p");
      none.className = "placeholder";
      none.appendChild(document.createTextNode("No matching keys."));
      host.appendChild(none);
      return;
    }
    for (var i = 0; i < keys.length; i++) {
      host.appendChild(keyListItem(keys[i]));
    }
  }

  function keyListItem(key) {
    var item = document.createElement("button");
    item.type = "button";
    item.className = "ks-key-item";
    var name = key.key == null ? "" : String(key.key);
    var typePill = document.createElement("span");
    typePill.className = "type-pill type-" + (key.type == null ? "unknown" : String(key.type));
    typePill.appendChild(document.createTextNode(key.type == null ? "?" : String(key.type)));
    item.appendChild(typePill);
    var label = document.createElement("span");
    label.className = "ks-key-name mono";
    label.appendChild(document.createTextNode(name));
    item.appendChild(label);
    var ttl = document.createElement("span");
    ttl.className = "ks-key-ttl mono";
    ttl.appendChild(document.createTextNode(fmtTtl(key.ttl)));
    item.appendChild(ttl);
    item.addEventListener("click", function () {
      selectKey(name);
    });
    return item;
  }

  function fmtTtl(ttl) {
    if (ttl == null) {
      return "-";
    }
    var n = Number(ttl);
    if (n === -1) {
      return "no ttl";
    }
    if (n === -2) {
      return "gone";
    }
    return n + "s";
  }

  function selectKey(name) {
    ksSelectedKey = name;
    setStatus("ks-action-status", null);
    fetchJson("/api/keys/" + encodeURIComponent(name)).then(function (r) {
      if (r.status === 404) {
        showInspector(false);
        setStatus("ks-action-status", "Key no longer exists.", "err");
        return;
      }
      if (r.status === 401 || r.status === 403) {
        setStatus("ks-action-status", "Sign in to inspect keys.", "err");
        return;
      }
      if (r.status === 200 && r.body) {
        renderInspector(r.body);
      } else {
        setStatus("ks-action-status", apiError(r, "Could not load the key."), "err");
      }
    });
  }

  function showInspector(on) {
    var empty = byId("ks-inspector-empty");
    var panel = byId("ks-inspector");
    var actions = byId("ks-actions");
    if (empty) {
      empty.hidden = on;
    }
    if (panel) {
      panel.hidden = !on;
    }
    if (actions) {
      actions.hidden = !on || !haveToken();
    }
  }

  function renderInspector(detail) {
    showInspector(true);
    setText(byId("ks-detail-key"), detail.key == null ? "-" : String(detail.key));
    setText(byId("ks-detail-type"), detail.type == null ? "-" : String(detail.type));
    setText(byId("ks-inspector-type"), detail.type == null ? "" : String(detail.type));
    setText(byId("ks-detail-ttl"), fmtTtl(detail.ttl));
    var trunc = byId("ks-detail-truncated");
    if (trunc) {
      trunc.hidden = !detail.truncated;
    }
    renderKeyValue(detail.value);
  }

  function renderKeyValue(value) {
    var block = byId("ks-detail-value");
    if (!block) {
      return;
    }
    var text = "";
    if (value && value.kind === "string") {
      text = value.data == null ? "" : String(value.data);
    } else if (value && (value.kind === "elements" || value.kind === "pairs")) {
      var items = Array.isArray(value.items) ? value.items : [];
      text = items.join("\n");
    } else {
      text = "(no value)";
    }
    setText(block, text);
  }

  function deleteSelectedKey() {
    if (!ksSelectedKey) {
      return;
    }
    var key = ksSelectedKey;
    fetchMethod("DELETE", "/api/keys/" + encodeURIComponent(key)).then(function (r) {
      if (handleAuthFailure("ks-action-status", r.status)) {
        return;
      }
      if (r.status === 200) {
        setStatus("ks-action-status", "Deleted " + key + ".", "ok");
        showInspector(false);
        ksSelectedKey = null;
      } else {
        setStatus("ks-action-status", apiError(r, "Delete failed."), "err");
      }
    });
  }

  function expireSelectedKey() {
    if (!ksSelectedKey) {
      return;
    }
    var secsEl = byId("ks-expire-secs");
    var seconds = secsEl ? parseInt(secsEl.value, 10) : NaN;
    if (isNaN(seconds) || seconds < 0) {
      setStatus("ks-action-status", "Enter a non-negative number of seconds.", "err");
      return;
    }
    fetchMethod("POST", "/api/keys/" + encodeURIComponent(ksSelectedKey) + "/expire", {
      seconds: seconds,
    }).then(function (r) {
      if (handleAuthFailure("ks-action-status", r.status)) {
        return;
      }
      if (r.status === 200) {
        setStatus("ks-action-status", "TTL set.", "ok");
        selectKey(ksSelectedKey);
      } else {
        setStatus("ks-action-status", apiError(r, "Expire failed."), "err");
      }
    });
  }

  function persistSelectedKey() {
    if (!ksSelectedKey) {
      return;
    }
    fetchMethod("POST", "/api/keys/" + encodeURIComponent(ksSelectedKey) + "/persist").then(function (r) {
      if (handleAuthFailure("ks-action-status", r.status)) {
        return;
      }
      if (r.status === 200) {
        setStatus("ks-action-status", "TTL cleared.", "ok");
        selectKey(ksSelectedKey);
      } else {
        setStatus("ks-action-status", apiError(r, "Persist failed."), "err");
      }
    });
  }

  function createKey() {
    var keyEl = byId("ks-new-key");
    var valEl = byId("ks-new-value");
    var key = keyEl ? (keyEl.value || "").trim() : "";
    var value = valEl ? valEl.value || "" : "";
    if (!key) {
      setStatus("ks-action-status", "Enter a key name.", "err");
      return;
    }
    fetchMethod("POST", "/api/keys/" + encodeURIComponent(key), { value: value }).then(function (r) {
      if (handleAuthFailure("ks-action-status", r.status)) {
        return;
      }
      if (r.status === 200) {
        setStatus("ks-action-status", "Set " + key + ".", "ok");
        if (keyEl) keyEl.value = "";
        if (valEl) valEl.value = "";
        selectKey(key);
      } else {
        setStatus("ks-action-status", apiError(r, "Set failed."), "err");
      }
    });
  }

  // ----- Console (arbitrary command runner) ---------------------------------
  function appendScrollback(prefix, text) {
    var block = byId("console-scrollback");
    if (!block) {
      return;
    }
    var line = document.createElement("div");
    line.className = "console-line";
    var pre = document.createElement("span");
    pre.className = "console-line-prefix mono";
    pre.appendChild(document.createTextNode(prefix));
    line.appendChild(pre);
    var body = document.createElement("span");
    body.className = "console-line-body mono";
    body.appendChild(document.createTextNode(text));
    line.appendChild(body);
    block.appendChild(line);
    block.scrollTop = block.scrollHeight;
  }

  // Tokenize a command line respecting single and double quotes. Returns an array
  // of arg strings. Unterminated quotes consume to end of line.
  function tokenizeCommand(input) {
    var args = [];
    var cur = "";
    var inSingle = false;
    var inDouble = false;
    var started = false;
    for (var i = 0; i < input.length; i++) {
      var ch = input.charAt(i);
      if (inSingle) {
        if (ch === "'") {
          inSingle = false;
        } else {
          cur += ch;
        }
        continue;
      }
      if (inDouble) {
        if (ch === '"') {
          inDouble = false;
        } else {
          cur += ch;
        }
        continue;
      }
      if (ch === "'") {
        inSingle = true;
        started = true;
        continue;
      }
      if (ch === '"') {
        inDouble = true;
        started = true;
        continue;
      }
      if (ch === " " || ch === "\t") {
        if (started) {
          args.push(cur);
          cur = "";
          started = false;
        }
        continue;
      }
      cur += ch;
      started = true;
    }
    if (started) {
      args.push(cur);
    }
    return args;
  }

  // Flatten a rendered reply (the {kind, value, items} shape) into a text string
  // for the scrollback. Recursive for arrays; pure text (no markup).
  function renderReplyText(reply, depth) {
    if (!reply || !reply.kind) {
      return "(empty)";
    }
    if (reply.kind === "simple") {
      return reply.value == null ? "" : String(reply.value);
    }
    if (reply.kind === "error") {
      return "(error) " + (reply.value == null ? "" : String(reply.value));
    }
    if (reply.kind === "integer") {
      return "(integer) " + String(reply.value);
    }
    if (reply.kind === "bulk") {
      return reply.value == null ? "(nil)" : String(reply.value);
    }
    if (reply.kind === "array") {
      var items = Array.isArray(reply.items) ? reply.items : [];
      if (items.length === 0) {
        return "(empty array)";
      }
      var lines = [];
      for (var i = 0; i < items.length; i++) {
        lines.push(i + 1 + ") " + renderReplyText(items[i], (depth || 0) + 1));
      }
      return lines.join("\n");
    }
    return "(unknown)";
  }

  function runConsoleCommand() {
    var input = byId("console-input");
    if (!input) {
      return;
    }
    var raw = (input.value || "").trim();
    if (!raw) {
      return;
    }
    var args = tokenizeCommand(raw);
    appendScrollback("> ", raw);
    input.value = "";
    if (args.length === 0) {
      return;
    }
    fetchMethod("POST", "/api/command", { args: args }).then(function (r) {
      if (r.status === 401) {
        appendScrollback("! ", "sign in as admin to run commands");
        showLogin(true, "");
        return;
      }
      if (r.status === 403) {
        appendScrollback("! ", "the token does not grant admin");
        showLogin(true, "The token does not grant the required tier.");
        return;
      }
      if (r.status === 200 && r.body && r.body.reply) {
        appendScrollback("  ", renderReplyText(r.body.reply, 0));
      } else {
        appendScrollback("! ", apiError(r, "command failed"));
      }
    });
  }

  function updateConsoleGate() {
    var gate = byId("console-gate");
    var input = byId("console-input");
    var run = byId("console-run");
    var enabled = haveToken();
    // On the loopback dev default (no token configured) the server still serves
    // every tier, so do NOT hard-disable the input when no token is present; the
    // gate note is advisory and a 401/403 (when enforcing) shows the precise need.
    if (gate) {
      gate.hidden = enabled;
    }
    if (input) {
      input.setAttribute("placeholder", "type a command, e.g. GET mykey");
    }
    if (run) {
      run.disabled = false;
    }
  }

  // ----- Pub/Sub ------------------------------------------------------------
  var pubsubRecent = [];

  function loadPubsub() {
    fetchJson("/api/pubsub/channels").then(function (r) {
      if (r.status === 401 || r.status === 403) {
        renderChannels([], r.status === 401 ? "Sign in to view channels." : "Insufficient privileges.");
        return;
      }
      if (r.status === 200 && r.body && Array.isArray(r.body.channels)) {
        renderChannels(r.body.channels, null);
      } else {
        renderChannels([], "Could not load channels.");
      }
    });
    // The notify-keyspace-events config (best-effort; needs a config read tier).
    fetchJson("/api/config").then(function (r) {
      var note = byId("pubsub-notify-config");
      if (r.status === 200 && r.body && Array.isArray(r.body.params)) {
        var found = "";
        for (var i = 0; i < r.body.params.length; i++) {
          if (r.body.params[i].param === "notify-keyspace-events") {
            found = r.body.params[i].value == null ? "" : String(r.body.params[i].value);
            break;
          }
        }
        setText(note, found === "" ? "(disabled)" : found);
      } else {
        setText(note, "-");
      }
    });
    var gate = byId("pubsub-gate");
    if (gate) {
      gate.hidden = haveToken();
    }
  }

  function renderChannels(channels, message) {
    var rows = [];
    for (var i = 0; i < channels.length; i++) {
      var c = channels[i];
      var tr = document.createElement("tr");
      tr.appendChild(td(c.channel == null ? "-" : c.channel, "mono"));
      tr.appendChild(td(c.subs == null ? "-" : fmtNum(c.subs), "num"));
      rows.push(tr);
    }
    fillBody(byId("pubsub-body"), rows, 2, message || "No active channels.");
    setText(byId("pubsub-channel-count"), channels.length + (channels.length === 1 ? " channel" : " channels"));
  }

  function publishMessage() {
    var chEl = byId("pubsub-channel");
    var msgEl = byId("pubsub-message");
    var channel = chEl ? (chEl.value || "").trim() : "";
    var message = msgEl ? msgEl.value || "" : "";
    if (!channel) {
      setStatus("pubsub-status", "Enter a channel.", "err");
      return;
    }
    fetchMethod("POST", "/api/pubsub/publish", { channel: channel, message: message }).then(function (r) {
      if (handleAuthFailure("pubsub-status", r.status)) {
        return;
      }
      if (r.status === 200 && r.body) {
        var n = r.body.receivers == null ? 0 : r.body.receivers;
        setStatus("pubsub-status", "Published to " + n + (n === 1 ? " receiver." : " receivers."), "ok");
        addRecentPublish(channel, message);
        if (msgEl) {
          msgEl.value = "";
        }
        loadPubsub();
      } else {
        setStatus("pubsub-status", apiError(r, "Publish failed."), "err");
      }
    });
  }

  function addRecentPublish(channel, message) {
    pubsubRecent.unshift({ channel: channel, message: message });
    while (pubsubRecent.length > 10) {
      pubsubRecent.pop();
    }
    var wrap = byId("pubsub-recent");
    var list = byId("pubsub-recent-list");
    if (!list) {
      return;
    }
    if (wrap) {
      wrap.hidden = false;
    }
    while (list.firstChild) {
      list.removeChild(list.firstChild);
    }
    for (var i = 0; i < pubsubRecent.length; i++) {
      var li = document.createElement("li");
      li.appendChild(document.createTextNode(pubsubRecent[i].channel + ": " + pubsubRecent[i].message));
      list.appendChild(li);
    }
  }

  // ----- ACL ----------------------------------------------------------------
  function loadAcl() {
    fetchJson("/api/acl").then(function (r) {
      var gate = byId("acl-gate");
      if (r.status === 401 || r.status === 403) {
        if (gate) {
          gate.hidden = false;
        }
        renderAclUsers([], "(sign in as admin)");
        return;
      }
      if (gate) {
        gate.hidden = true;
      }
      if (r.status === 200 && r.body) {
        setText(byId("acl-whoami"), r.body.whoami == null ? "-" : String(r.body.whoami));
        renderAclUsers(Array.isArray(r.body.users) ? r.body.users : [], null);
      } else {
        renderAclUsers([], "Could not load users.");
      }
    });
  }

  // Parse an ACL LIST line ("user alice on >... ~* +@all") into a username + the
  // remaining rule tokens, for display chips.
  function parseAclLine(line) {
    var parts = String(line).split(/\s+/);
    var name = "(unknown)";
    var rules = [];
    var enabled = false;
    var start = 0;
    if (parts[0] === "user" && parts.length > 1) {
      name = parts[1];
      start = 2;
    } else if (parts.length > 0) {
      name = parts[0];
      start = 1;
    }
    for (var i = start; i < parts.length; i++) {
      if (parts[i] === "on") {
        enabled = true;
      }
      if (parts[i] === "off") {
        enabled = false;
      }
      if (parts[i].length > 0) {
        rules.push(parts[i]);
      }
    }
    return { name: name, rules: rules, enabled: enabled };
  }

  function renderAclUsers(users, message) {
    var host = byId("acl-users");
    if (!host) {
      return;
    }
    while (host.firstChild) {
      host.removeChild(host.firstChild);
    }
    if (message) {
      var p = document.createElement("p");
      p.className = "placeholder";
      p.appendChild(document.createTextNode(message));
      host.appendChild(p);
      return;
    }
    if (users.length === 0) {
      var none = document.createElement("p");
      none.className = "placeholder";
      none.appendChild(document.createTextNode("No users."));
      host.appendChild(none);
      return;
    }
    for (var i = 0; i < users.length; i++) {
      host.appendChild(aclUserRow(parseAclLine(users[i])));
    }
  }

  function aclUserRow(user) {
    var row = document.createElement("div");
    row.className = "acl-user-row";
    var avatar = document.createElement("span");
    avatar.className = "acl-avatar";
    var initial = user.name && user.name.length > 0 ? user.name.charAt(0).toUpperCase() : "?";
    avatar.appendChild(document.createTextNode(initial));
    row.appendChild(avatar);

    var main = document.createElement("div");
    main.className = "acl-user-main";
    var nameLine = document.createElement("div");
    nameLine.className = "acl-user-name mono";
    nameLine.appendChild(document.createTextNode(user.name));
    var status = document.createElement("span");
    status.className = user.enabled ? "pill pill-ok" : "pill pill-bad";
    status.appendChild(document.createTextNode(user.enabled ? "on" : "off"));
    nameLine.appendChild(status);
    main.appendChild(nameLine);

    var chips = document.createElement("div");
    chips.className = "acl-chips";
    for (var i = 0; i < user.rules.length && i < 24; i++) {
      var chip = document.createElement("span");
      chip.className = "acl-chip mono";
      chip.appendChild(document.createTextNode(user.rules[i]));
      chips.appendChild(chip);
    }
    main.appendChild(chips);
    row.appendChild(main);

    var del = document.createElement("button");
    del.type = "button";
    del.className = "btn btn-danger acl-del";
    del.appendChild(document.createTextNode("Delete"));
    var uname = user.name;
    del.addEventListener("click", function () {
      deleteAclUser(uname);
    });
    row.appendChild(del);
    return row;
  }

  function addAclUser() {
    var nameEl = byId("acl-username");
    var rulesEl = byId("acl-rules");
    var username = nameEl ? (nameEl.value || "").trim() : "";
    var rulesRaw = rulesEl ? (rulesEl.value || "").trim() : "";
    if (!username) {
      setStatus("acl-status", "Enter a username.", "err");
      return;
    }
    var rules = rulesRaw.length > 0 ? rulesRaw.split(/\s+/) : [];
    fetchMethod("POST", "/api/acl/user", { username: username, rules: rules }).then(function (r) {
      if (handleAuthFailure("acl-status", r.status)) {
        return;
      }
      if (r.status === 200) {
        setStatus("acl-status", "Saved user " + username + ".", "ok");
        if (nameEl) nameEl.value = "";
        if (rulesEl) rulesEl.value = "";
        loadAcl();
      } else {
        setStatus("acl-status", apiError(r, "Could not save user."), "err");
      }
    });
  }

  function deleteAclUser(name) {
    fetchMethod("DELETE", "/api/acl/user/" + encodeURIComponent(name)).then(function (r) {
      if (handleAuthFailure("acl-status", r.status)) {
        return;
      }
      if (r.status === 200) {
        setStatus("acl-status", "Deleted " + name + ".", "ok");
        loadAcl();
      } else {
        setStatus("acl-status", apiError(r, "Could not delete " + name + "."), "err");
      }
    });
  }

  // ----- Persistence --------------------------------------------------------
  function loadPersistence() {
    fetchJson("/api/persistence").then(function (r) {
      if (r.status === 401 || r.status === 403) {
        setText(byId("p-lastsave"), "-");
        setStatus("persistence-status", r.status === 401 ? "Sign in to view persistence." : "Insufficient privileges.", "err");
        return;
      }
      if (r.status === 200 && r.body) {
        renderPersistence(r.body);
      } else {
        setStatus("persistence-status", apiError(r, "Could not load persistence."), "err");
      }
    });
    var gate = byId("persistence-gate");
    if (gate) {
      gate.hidden = haveToken();
    }
  }

  function renderPersistence(p) {
    if (p.last_save_unixtime != null) {
      var ago = Math.max(0, Math.floor(Date.now() / 1000 - Number(p.last_save_unixtime)));
      setText(byId("p-lastsave"), fmtDuration(ago));
      setText(byId("p-lastsave-delta"), "at " + fmtTime(p.last_save_unixtime));
    } else {
      setText(byId("p-lastsave"), "never");
    }
    setText(byId("p-changes"), p.changes_since_save == null ? "-" : fmtNum(p.changes_since_save));
    setText(byId("p-rdb"), p.rdb_enabled ? "on" : "off");
    setText(byId("p-aof"), p.aof_enabled ? "on" : "off");
    if (p.last_bgsave_status) {
      setText(byId("p-bgsave-status"), "last bgsave: " + p.last_bgsave_status);
    }
  }

  function fmtDuration(secs) {
    var s = Number(secs);
    if (isNaN(s)) {
      return "-";
    }
    if (s < 60) {
      return s + "s ago";
    }
    if (s < 3600) {
      return Math.floor(s / 60) + "m ago";
    }
    if (s < 86400) {
      return Math.floor(s / 3600) + "h ago";
    }
    return Math.floor(s / 86400) + "d ago";
  }

  function saveNow() {
    setStatus("persistence-status", "Requesting BGSAVE...", "ok");
    fetchMethod("POST", "/api/persistence/save", { background: true }).then(function (r) {
      if (handleAuthFailure("persistence-status", r.status)) {
        return;
      }
      if (r.status === 200) {
        setStatus("persistence-status", "Background save started.", "ok");
        loadPersistence();
      } else {
        setStatus("persistence-status", apiError(r, "Save failed."), "err");
      }
    });
  }

  // Extract an error message from a non-200 management response, falling back to a
  // default. The server error body is {"error":"..."} (a string), so it is safe to
  // surface as text.
  function apiError(r, fallback) {
    if (r && r.body && typeof r.body.error === "string") {
      return r.body.error;
    }
    return fallback + " (HTTP " + (r ? r.status : "?") + ")";
  }

  // Load the data for a management section on demand (when navigated to / on a
  // manual refresh). The OPEN/PRIVILEGED monitoring sections keep using the 5s
  // poll in refresh().
  function loadManagement(section) {
    if (section === "cluster") {
      // The rebalance plan + failover are explicit operator actions (a CLUSTER command
      // per click), so they fire on their buttons, NOT on every Cluster-tab visit; here
      // we only refresh the admin-gate notes.
      updateRebalanceGate();
      updateFailoverGate();
      updateMembershipGates();
      updateSetslotGate();
    } else if (section === "config") {
      loadConfig();
    } else if (section === "keyspace") {
      // The keyspace browser is driven by the Scan button; nothing auto-loads.
      var actions = byId("ks-actions");
      if (actions && ksSelectedKey) {
        actions.hidden = !haveToken();
      }
    } else if (section === "console") {
      updateConsoleGate();
    } else if (section === "pubsub") {
      loadPubsub();
    } else if (section === "acl") {
      loadAcl();
    } else if (section === "persistence") {
      loadPersistence();
    }
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

  // Issue a non-GET request (POST/DELETE) with a JSON body and the Bearer token.
  // Returns { status, body } like fetchJson. The body is sent as JSON; the token
  // (if any) rides ONLY in the Authorization header, never the URL or the body.
  function fetchMethod(method, path, payload) {
    var headers = { Accept: "application/json" };
    var token = getToken();
    if (token) {
      headers.Authorization = "Bearer " + token;
    }
    var init = { method: method, headers: headers, cache: "no-store" };
    if (payload !== undefined && payload !== null) {
      headers["Content-Type"] = "application/json";
      init.body = JSON.stringify(payload);
    }
    return fetch(path, init).then(function (resp) {
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

  // Show a transient status line on a management form. `kind` is "ok" or "err".
  function setStatus(id, message, kind) {
    var el = byId(id);
    if (!el) {
      return;
    }
    if (!message) {
      el.hidden = true;
      setText(el, "");
      el.classList.remove("status-ok", "status-err");
      return;
    }
    setText(el, message);
    el.hidden = false;
    el.classList.remove("status-ok", "status-err");
    el.classList.add(kind === "ok" ? "status-ok" : "status-err");
  }

  // Map a non-200 management response to a human status string + reveal sign-in on
  // an auth failure. Returns true if it was an auth failure (so the caller stops).
  function handleAuthFailure(statusId, status) {
    if (status === 401) {
      setStatus(statusId, "Admin token required. Sign in below.", "err");
      showLogin(true, "");
      return true;
    }
    if (status === 403) {
      setStatus(statusId, "The token does not grant admin. Sign in with an admin token.", "err");
      showLogin(true, "The token does not grant the required tier.");
      return true;
    }
    return false;
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
      } else if (lastTopologyAgeSeconds > STALE_AFTER_S) {
        // The console API is reachable, but its node poll has not refreshed in a
        // while: warn that the topology may be stale before an operator trusts it.
        showBanner(
          "Topology data is " +
            lastTopologyAgeSeconds +
            "s old; the console poll may be stuck. Do not treat this view as live."
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

    // Management sections load on demand (each opens an on-demand node connection),
    // so fetch their data when navigated to rather than on the 5s monitoring poll.
    if (MANAGEMENT_SECTIONS[section]) {
      loadManagement(section);
    }
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
      // Also reload the active management section (it is not on the live poll).
      if (MANAGEMENT_SECTIONS[activeSection]) {
        loadManagement(activeSection);
      }
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
        // Re-evaluate the active management section's gates/data with the new token.
        if (MANAGEMENT_SECTIONS[activeSection]) {
          loadManagement(activeSection);
        }
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
        if (MANAGEMENT_SECTIONS[activeSection]) {
          loadManagement(activeSection);
        }
      });
    }
  }

  // ----- management form wiring (#361) --------------------------------------
  function wireManagement() {
    // Cluster: load the rebalance dry-run plan on demand (admin); trigger a failover;
    // add/remove a node.
    wireClick("rebalance-load", loadRebalancePlan);
    wireClick("rebalance-apply", triggerRebalanceApply);
    wireClick("failover-trigger", triggerFailover);
    wireClick("meet-add", addNode);
    wireClick("forget-remove", removeNode);
    wireClick("setslot-apply", applySetslot);

    // Config: filter + apply (per-row Apply wired in configRow).
    var configFilter = byId("config-filter");
    if (configFilter) {
      configFilter.addEventListener("input", function () {
        renderConfigRows(configParams, null);
      });
    }

    // Keyspace: scan, more, inspector actions, new key.
    var scanForm = byId("ks-scan-form");
    if (scanForm) {
      scanForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        runScan(true);
      });
    }
    var scanMore = byId("ks-scan-more");
    if (scanMore) {
      scanMore.addEventListener("click", function () {
        runScan(false);
      });
    }
    var expireForm = byId("ks-expire-form");
    if (expireForm) {
      expireForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        expireSelectedKey();
      });
    }
    wireClick("ks-persist-btn", persistSelectedKey);
    wireClick("ks-del-btn", deleteSelectedKey);
    var newForm = byId("ks-new-form");
    if (newForm) {
      newForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        createKey();
      });
    }

    // Console: form submit + suggestion chips.
    var consoleForm = byId("console-form");
    if (consoleForm) {
      consoleForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        runConsoleCommand();
      });
    }
    var chips = byId("console-chips");
    if (chips) {
      var chipBtns = chips.getElementsByClassName("chip");
      for (var i = 0; i < chipBtns.length; i++) {
        (function (btn) {
          btn.addEventListener("click", function () {
            var input = byId("console-input");
            if (input) {
              input.value = btn.getAttribute("data-cmd") || "";
              input.focus();
            }
          });
        })(chipBtns[i]);
      }
    }

    // Pub/Sub: publish form.
    var pubForm = byId("pubsub-form");
    if (pubForm) {
      pubForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        publishMessage();
      });
    }

    // ACL: add-user form (delete wired per-row in aclUserRow).
    var aclForm = byId("acl-form");
    if (aclForm) {
      aclForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        addAclUser();
      });
    }

    // Persistence: save now.
    wireClick("persistence-save", saveNow);
  }

  function wireClick(id, fn) {
    var el = byId(id);
    if (el) {
      el.addEventListener("click", fn);
    }
  }

  function start() {
    initTheme();
    wireNav();
    wireThemeToggle();
    wireLiveToggle();
    wireRefresh();
    wireAuthControls();
    wireManagement();
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
