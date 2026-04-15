(function () {
  "use strict";

  var statusEl = document.getElementById("nats-status");
  var logEl = document.getElementById("event-log");
  var providerCardsEl = document.getElementById("provider-cards");
  var scatterResultsEl = document.getElementById("scatter-results");
  var timelineEl = document.getElementById("scatter-timeline");

  var ws = null;
  var closing = false;
  var flashingIds = new Set();
  var providers = new Map();  // respId -> {id, name, subject, delayMs, replied}
  var currentScatter = null;  // {scatterId, subject, payload, timeoutMs, replies: [], done}
  var scatterHistory = [];    // last 5 scatters for reference

  window.addEventListener("beforeunload", function () {
    closing = true;
    if (ws) ws.close();
  });

  function makeEl(tag, className, text) {
    var el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  }

  // --- WebSocket ---
  function connect() {
    var url = "ws://" + location.host + "/demo/scatter/ws";
    statusEl.textContent = "connecting...";
    statusEl.classList.remove("connected");

    ws = new WebSocket(url);

    ws.onopen = function () {
      statusEl.textContent = "connected";
      statusEl.classList.add("connected");
      addLog("system", "Connected to server");
    };

    ws.onclose = function () {
      if (closing) return;
      statusEl.textContent = "disconnected";
      statusEl.classList.remove("connected");
      addLog("system", "Disconnected -- reconnecting in 2s...");
      setTimeout(connect, 2000);
    };

    ws.onerror = function () {};

    ws.onmessage = function (e) {
      var evt = JSON.parse(e.data);
      switch (evt.type) {
        case "responder_added":    onResponderAdded(evt.payload); break;
        case "responder_removed":  onResponderRemoved(evt.payload); break;
        case "responder_replied":  onResponderReplied(evt.payload); break;
        case "scatter_sent":       onScatterSent(evt.payload); break;
        case "scatter_reply":      onScatterReply(evt.payload); break;
        case "scatter_done":       onScatterDone(evt.payload); break;
        case "error":              onError(evt.payload); break;
      }
    };
  }

  // --- Event handlers ---
  function onResponderAdded(p) {
    providers.set(p.respId, {
      id: p.respId,
      name: p.name,
      subject: p.subject,
      delayMs: p.delayMs,
      replied: 0
    });
    flashingIds.add(p.respId);
    setTimeout(function () { flashingIds.delete(p.respId); renderProviders(); }, 400);
    renderProviders();
    addLog("subscribe", p.name + " ready (" + p.delayMs + "ms simulated delay)");
  }

  function onResponderRemoved(p) {
    var prov = providers.get(p.respId);
    var name = prov ? prov.name : p.respId;
    providers.delete(p.respId);
    renderProviders();
    addLog("unsubscribe", name + " removed");
  }

  function onResponderReplied(p) {
    var prov = providers.get(p.respId);
    if (prov) prov.replied++;
    flashingIds.add(p.respId);
    setTimeout(function () { flashingIds.delete(p.respId); renderProviders(); }, 400);
    renderProviders();
  }

  function onScatterSent(p) {
    currentScatter = {
      scatterId: p.scatterId,
      subject: p.subject,
      payload: p.payload,
      timeoutMs: p.timeoutMs,
      replies: [],
      done: false
    };
    renderResults();
    renderTimeline();
    addLog("publish", "Scatter \"" + p.subject + "\": " + p.payload + " (deadline: " + p.timeoutMs + "ms)");
  }

  function onScatterReply(p) {
    if (currentScatter && p.scatterId === currentScatter.scatterId) {
      currentScatter.replies.push({
        index: p.index,
        data: p.data,
        latencyMs: p.latencyMs
      });
      flashingIds.add("reply-" + p.index);
      setTimeout(function () { flashingIds.delete("reply-" + p.index); renderResults(); }, 400);
      renderResults();
      renderTimeline();
    }
    // Parse provider name from reply data
    var provName = "Provider";
    try {
      var parsed = JSON.parse(p.data);
      if (parsed.provider) provName = parsed.provider;
    } catch (e) {}
    addLog("receive", provName + " replied in " + p.latencyMs + "ms");
  }

  function onScatterDone(p) {
    if (currentScatter && p.scatterId === currentScatter.scatterId) {
      currentScatter.done = true;
      // Save to history
      scatterHistory.unshift(currentScatter);
      if (scatterHistory.length > 5) scatterHistory.pop();
    }
    renderResults();
    renderTimeline();
    addLog("system", "Scatter complete: " + p.replyCount + " of " + providers.size + " replied in " + p.totalMs + "ms");
  }

  function onError(p) {
    addLog("error", p.message);
  }

  // --- Render providers ---
  function renderProviders() {
    providerCardsEl.textContent = "";

    if (providers.size === 0) {
      providerCardsEl.appendChild(makeEl("p", "empty-state", "Click \"Setup: Price Search\" to add providers."));
      return;
    }

    providers.forEach(function (p) {
      var card = makeEl("div", "sg-provider-card");
      if (flashingIds.has(p.id)) card.classList.add("pulse");

      var header = makeEl("div", "sg-provider-header");
      header.appendChild(makeEl("span", "sg-provider-name", p.name));
      header.appendChild(makeEl("span", "sg-provider-delay", p.delayMs + "ms"));
      card.appendChild(header);

      var stats = makeEl("div", "sg-provider-stats");
      stats.appendChild(makeEl("span", "sg-provider-replied", p.replied + " replied"));

      var removeBtn = makeEl("button", "btn btn-small btn-danger", "Remove");
      removeBtn.addEventListener("click", (function (id) {
        return function () { send({ action: "remove_responder", respId: id }); };
      })(p.id));
      stats.appendChild(removeBtn);

      card.appendChild(stats);
      providerCardsEl.appendChild(card);
    });
  }

  // --- Render scatter results ---
  function renderResults() {
    scatterResultsEl.textContent = "";

    if (!currentScatter) {
      scatterResultsEl.appendChild(makeEl("p", "empty-state", "Send a scatter request to see replies stream in."));
      return;
    }

    // Status header
    var statusRow = makeEl("div", "sg-status-row");
    if (currentScatter.done) {
      statusRow.appendChild(makeEl("span", "sg-status-badge sg-status-done", "COMPLETE"));
      statusRow.appendChild(makeEl("span", "sg-status-text",
        currentScatter.replies.length + " of " + providers.size + " providers replied"));
    } else {
      statusRow.appendChild(makeEl("span", "sg-status-badge sg-status-waiting", "WAITING"));
      statusRow.appendChild(makeEl("span", "sg-status-text",
        currentScatter.replies.length + " replies so far..."));
    }
    scatterResultsEl.appendChild(statusRow);

    // Reply cards
    if (currentScatter.replies.length === 0) {
      scatterResultsEl.appendChild(makeEl("p", "empty-state", "Waiting for providers to respond..."));
      return;
    }

    // Sort by latency
    var sorted = currentScatter.replies.slice().sort(function (a, b) {
      return a.latencyMs - b.latencyMs;
    });

    for (var i = 0; i < sorted.length; i++) {
      var r = sorted[i];
      var row = makeEl("div", "sg-reply-row");
      if (flashingIds.has("reply-" + r.index)) row.classList.add("flash");
      if (i === 0 && currentScatter.done) row.classList.add("sg-reply-fastest");

      var provName = "Provider";
      try {
        var parsed = JSON.parse(r.data);
        if (parsed.provider) provName = parsed.provider;
      } catch (e) {}

      var left = makeEl("div", "sg-reply-left");
      if (i === 0 && currentScatter.done) {
        left.appendChild(makeEl("span", "sg-fastest-badge", "FASTEST"));
      }
      left.appendChild(makeEl("span", "sg-reply-name", provName));
      row.appendChild(left);

      row.appendChild(makeEl("span", "sg-reply-latency", r.latencyMs + "ms"));
      scatterResultsEl.appendChild(row);
    }
  }

  // --- Render timeline ---
  function renderTimeline() {
    timelineEl.textContent = "";

    if (!currentScatter || currentScatter.replies.length === 0) {
      timelineEl.appendChild(makeEl("p", "empty-state", "Results will appear on a timeline showing response latency."));
      return;
    }

    var maxMs = currentScatter.timeoutMs;
    // Find actual max for scaling
    var actualMax = 0;
    for (var i = 0; i < currentScatter.replies.length; i++) {
      if (currentScatter.replies[i].latencyMs > actualMax) {
        actualMax = currentScatter.replies[i].latencyMs;
      }
    }
    if (actualMax > maxMs) maxMs = actualMax;

    // Deadline bar
    var deadlineRow = makeEl("div", "sg-timeline-header");
    deadlineRow.appendChild(makeEl("span", "sg-timeline-label", "0ms"));
    deadlineRow.appendChild(makeEl("span", "sg-timeline-label", maxMs + "ms deadline"));
    timelineEl.appendChild(deadlineRow);

    // Sort by latency for display
    var sorted = currentScatter.replies.slice().sort(function (a, b) {
      return a.latencyMs - b.latencyMs;
    });

    var colors = ["var(--accent)", "var(--green)", "var(--cyan)", "var(--orange)", "var(--red)"];

    for (var j = 0; j < sorted.length; j++) {
      var r = sorted[j];
      var provName = "Provider";
      try {
        var parsed = JSON.parse(r.data);
        if (parsed.provider) provName = parsed.provider;
      } catch (e) {}

      var row = makeEl("div", "sg-timeline-row");
      row.appendChild(makeEl("span", "sg-timeline-name", provName));

      var barWrap = makeEl("div", "sg-timeline-bar-wrap");
      var bar = makeEl("div", "sg-timeline-bar");
      var pct = Math.min(100, Math.round((r.latencyMs / maxMs) * 100));
      bar.style.width = pct + "%";
      bar.style.background = colors[j % colors.length];
      barWrap.appendChild(bar);

      // Deadline marker
      if (currentScatter.timeoutMs < maxMs) {
        var marker = makeEl("div", "sg-deadline-marker");
        marker.style.left = Math.round((currentScatter.timeoutMs / maxMs) * 100) + "%";
        barWrap.appendChild(marker);
      }

      row.appendChild(barWrap);
      row.appendChild(makeEl("span", "sg-timeline-ms", r.latencyMs + "ms"));
      timelineEl.appendChild(row);
    }
  }

  // --- Log ---
  function addLog(type, text) {
    var time = new Date().toLocaleTimeString("en-US", { hour12: false });
    var div = makeEl("div", "log-entry log-" + type);
    div.appendChild(makeEl("span", "log-time", time));
    div.appendChild(document.createTextNode(" "));
    div.appendChild(makeEl("span", "log-text", text));
    logEl.appendChild(div);
    logEl.scrollTop = logEl.scrollHeight;
  }

  function send(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(obj));
    } else {
      addLog("error", "Not connected -- message dropped");
    }
  }

  // --- Button handlers ---
  document.getElementById("btn-setup-providers").addEventListener("click", function () {
    var steps = [
      { action: "add_responder", name: "Amazon",   subject: "demo.scatter.search", delayMs: 80 },
      { action: "add_responder", name: "BestBuy",  subject: "demo.scatter.search", delayMs: 250 },
      { action: "add_responder", name: "Walmart",  subject: "demo.scatter.search", delayMs: 150 },
      { action: "add_responder", name: "Newegg",   subject: "demo.scatter.search", delayMs: 400 }
    ];
    steps.forEach(function (step, i) {
      setTimeout(function () { send(step); }, i * 100);
    });
  });

  document.getElementById("btn-scatter-preset").addEventListener("click", function () {
    send({
      action: "scatter",
      subject: "demo.scatter.search",
      payload: '"MacBook Pro 16"',
      timeoutMs: 2000
    });
  });

  document.getElementById("btn-add-responder").addEventListener("click", function () {
    var name = document.getElementById("resp-name").value.trim();
    var delayMs = parseInt(document.getElementById("resp-delay").value, 10) || 0;
    if (!name) return;
    send({
      action: "add_responder",
      name: name,
      subject: "demo.scatter.search",
      delayMs: delayMs
    });
  });

  document.getElementById("btn-scatter").addEventListener("click", function () {
    var payload = document.getElementById("scatter-payload").value.trim();
    var timeoutMs = parseInt(document.getElementById("scatter-timeout").value.trim(), 10) || 2000;
    send({
      action: "scatter",
      subject: "demo.scatter.search",
      payload: payload,
      timeoutMs: timeoutMs
    });
  });

  document.getElementById("btn-clear-log").addEventListener("click", function () {
    logEl.textContent = "";
  });

  // Enter key
  document.getElementById("resp-name").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-add-responder").click();
  });
  document.getElementById("resp-delay").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-add-responder").click();
  });
  document.getElementById("scatter-payload").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-scatter").click();
  });
  document.getElementById("scatter-timeout").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-scatter").click();
  });

  // Initial render
  renderProviders();
  renderResults();
  renderTimeline();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
