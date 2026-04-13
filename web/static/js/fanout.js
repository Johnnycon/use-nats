(function () {
  "use strict";

  const statusEl = document.getElementById("nats-status");
  const logEl = document.getElementById("event-log");
  const subsEl = document.getElementById("subscribers");
  const flowEl = document.getElementById("flow-diagram");

  let ws = null;
  let closing = false;
  let pubCount = 0;
  let publishFlash = false;
  const flashingIds = new Set();
  const activeSubs = new Map(); // id -> {subject, name, recvCount}

  // Clean up WebSocket on page unload to free Chrome connection slots
  window.addEventListener("beforeunload", () => {
    closing = true;
    if (ws) ws.close();
  });

  // --- helpers ---
  function makeEl(tag, className, text) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  }

  // --- WebSocket with auto-reconnect ---
  function connect() {
    const url = `ws://${location.host}/demo/fanout/ws`;
    statusEl.textContent = "connecting...";
    statusEl.classList.remove("connected");

    ws = new WebSocket(url);

    ws.onopen = () => {
      statusEl.textContent = "connected";
      statusEl.classList.add("connected");
      addLog("system", "Connected to server");
    };

    ws.onclose = () => {
      if (closing) return;
      statusEl.textContent = "disconnected";
      statusEl.classList.remove("connected");
      addLog("system", "Disconnected -- reconnecting in 2s...");
      setTimeout(connect, 2000);
    };

    ws.onerror = () => {};

    ws.onmessage = (e) => {
      const evt = JSON.parse(e.data);
      switch (evt.type) {
        case "subscribed":
          onSubscribed(evt.payload);
          break;
        case "unsubscribed":
          onUnsubscribed(evt.payload);
          break;
        case "published":
          onPublished(evt.payload);
          break;
        case "message_received":
          onMessageReceived(evt.payload);
          break;
      }
    };
  }

  // --- Event handlers ---
  function onSubscribed(p) {
    activeSubs.set(p.subId, { subject: p.subject, name: p.name, recvCount: 0 });
    renderSubscribers();
    renderFlow();
    addLog("subscribe", `${p.name} subscribed to "${p.subject}"`);
  }

  function onUnsubscribed(p) {
    const name = activeSubs.get(p.subId)?.name || p.subId;
    activeSubs.delete(p.subId);
    renderSubscribers();
    renderFlow();
    addLog("unsubscribe", `${name} unsubscribed`);
  }

  function onPublished(p) {
    pubCount++;
    publishFlash = true;
    setTimeout(() => { publishFlash = false; }, 300);
    renderFlow();
    addLog("publish", `Broadcast to "${p.subject}": ${p.data}`);
  }

  function onMessageReceived(p) {
    const sub = activeSubs.get(p.subId);
    if (sub) sub.recvCount++;
    var name = p.name || p.subId;
    flashingIds.add(p.subId);
    setTimeout(() => { flashingIds.delete(p.subId); }, 400);
    renderSubscribers();
    renderFlow();
    addLog("receive", `${name} received on "${p.subject}": ${p.data}`);
  }

  // --- Group subscriptions by subscriber name ---
  function groupedSubs() {
    const groups = new Map(); // name -> [{id, subject, recvCount}]
    for (const [id, s] of activeSubs) {
      const name = s.name || id;
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name).push({ id, subject: s.subject, recvCount: s.recvCount });
    }
    return groups;
  }

  // --- Flow diagram rendering ---
  function renderFlow() {
    flowEl.textContent = "";

    if (activeSubs.size === 0) {
      flowEl.appendChild(makeEl("p", "empty-state", "Subscribe to see message flow."));
      return;
    }

    // Publisher column
    const pubCol = makeEl("div", "flow-col flow-col-pub");
    const pubNode = makeEl("div", "flow-node flow-pub publisher-node");
    if (publishFlash) pubNode.classList.add("pulse");
    pubNode.appendChild(makeEl("div", "node-label", "Broadcaster"));
    pubNode.appendChild(makeEl("div", "node-counter", String(pubCount)));
    pubCol.appendChild(pubNode);

    // NATS column
    const natsCol = makeEl("div", "flow-col flow-col-nats");
    const natsNode = makeEl("div", "flow-node flow-nats nats-node");
    if (publishFlash) natsNode.classList.add("pulse");
    natsNode.appendChild(makeEl("div", "node-label", "NATS"));
    natsCol.appendChild(natsNode);

    // Subscribers column -- grouped by name
    const subCol = makeEl("div", "flow-col flow-col-subs");
    for (const [name, subs] of groupedSubs()) {
      const totalRecv = subs.reduce(function (sum, s) { return sum + s.recvCount; }, 0);
      const node = makeEl("div", "flow-node flow-sub subscriber-node");
      node.id = `flow-group-${name}`;
      if (subs.some(s => flashingIds.has(s.id))) node.classList.add("pulse");
      node.appendChild(makeEl("div", "node-name", name));
      for (var i = 0; i < subs.length; i++) {
        node.appendChild(makeEl("div", "node-subject", subs[i].subject));
      }
      node.appendChild(makeEl("div", "node-counter", String(totalRecv)));
      subCol.appendChild(node);
    }

    // Arrows
    const arrow1 = makeEl("div", "flow-arrow-col");
    arrow1.appendChild(makeEl("div", "arrow-line-h"));
    const arrow2 = makeEl("div", "flow-arrow-col");
    arrow2.appendChild(makeEl("div", "arrow-line-h"));

    flowEl.appendChild(pubCol);
    flowEl.appendChild(arrow1);
    flowEl.appendChild(natsCol);
    flowEl.appendChild(arrow2);
    flowEl.appendChild(subCol);
  }

  // --- UI rendering ---
  function renderSubscribers() {
    subsEl.textContent = "";
    if (activeSubs.size === 0) {
      subsEl.appendChild(makeEl("p", "empty-state", "No subscribers yet. Create one above."));
      return;
    }
    for (const [name, subs] of groupedSubs()) {
      const card = makeEl("div", "sub-card-group");
      const totalRecv = subs.reduce(function (sum, s) { return sum + s.recvCount; }, 0);

      const header = makeEl("div", "sub-group-header");
      header.appendChild(makeEl("span", "sub-id", name));
      const headerRight = makeEl("span", "sub-header-stats");
      headerRight.appendChild(makeEl("span", "sub-recv-total", String(totalRecv)));
      headerRight.appendChild(makeEl("span", "sub-count", `${subs.length} sub${subs.length > 1 ? "s" : ""}`));
      header.appendChild(headerRight);
      card.appendChild(header);

      for (var i = 0; i < subs.length; i++) {
        (function (s) {
          const row = makeEl("div", "sub-row");
          row.id = `card-${s.id}`;
          if (flashingIds.has(s.id)) row.classList.add("flash");
          row.appendChild(makeEl("span", "sub-subject", s.subject));
          const rowRight = makeEl("span", "sub-row-stats");
          rowRight.appendChild(makeEl("span", "sub-recv-count", String(s.recvCount)));
          const btn = makeEl("button", "btn btn-small btn-danger", "Unsub");
          btn.addEventListener("click", function () {
            send({ action: "unsubscribe", subId: s.id });
          });
          rowRight.appendChild(btn);
          row.appendChild(rowRight);
          card.appendChild(row);
        })(subs[i]);
      }

      subsEl.appendChild(card);
    }
  }

  function highlightSub(subId) {
    const card = document.getElementById(`card-${subId}`);
    if (!card) return;
    card.classList.add("flash");
    setTimeout(function () { card.classList.remove("flash"); }, 400);
  }

  function pulseNode(selector) {
    const el = document.querySelector(selector);
    if (!el) return;
    el.classList.add("pulse");
    setTimeout(function () { el.classList.remove("pulse"); }, 300);
  }

  function addLog(type, text) {
    const time = new Date().toLocaleTimeString("en-US", { hour12: false });
    const div = makeEl("div", `log-entry log-${type}`);
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
  document.getElementById("btn-subscribe").addEventListener("click", function () {
    const subject = document.getElementById("sub-subject").value.trim();
    const name = document.getElementById("sub-name").value.trim();
    if (subject) send({ action: "subscribe", subject: subject, name: name });
  });

  document.getElementById("btn-publish").addEventListener("click", function () {
    const subject = document.getElementById("pub-subject").value.trim();
    const message = document.getElementById("pub-message").value.trim();
    if (subject) send({ action: "publish", subject: subject, message: message });
  });

  document.getElementById("btn-burst").addEventListener("click", function () {
    const subject = document.getElementById("pub-subject").value.trim();
    if (!subject) return;
    for (var i = 1; i <= 10; i++) {
      send({ action: "publish", subject: subject, message: "Broadcast burst #" + i });
    }
  });

  document.getElementById("btn-setup-fleet").addEventListener("click", function () {
    var steps = [
      { action: "subscribe", subject: "system.cache.invalidate", name: "WebServer-1" },
      { action: "subscribe", subject: "system.cache.invalidate", name: "WebServer-2" },
      { action: "subscribe", subject: "system.cache.invalidate", name: "WebServer-3" },
      { action: "subscribe", subject: "system.config.update", name: "APIGateway-1" },
      { action: "subscribe", subject: "system.config.update", name: "APIGateway-2" },
      { action: "subscribe", subject: "system.cache.invalidate", name: "MonitorDashboard" },
      { action: "subscribe", subject: "system.config.update", name: "MonitorDashboard" },
    ];
    steps.forEach(function (step, i) {
      setTimeout(function () { send(step); }, i * 100);
    });
  });

  document.getElementById("btn-broadcast-cache").addEventListener("click", function () {
    send({
      action: "publish",
      subject: "system.cache.invalidate",
      message: '{"action": "invalidate", "keys": ["users:*", "sessions:*"], "reason": "schema migration"}',
    });
  });

  document.getElementById("btn-broadcast-config").addEventListener("click", function () {
    send({
      action: "publish",
      subject: "system.config.update",
      message: '{"key": "rate_limit", "value": 1000, "previous": 500}',
    });
  });

  document.getElementById("btn-clear-log").addEventListener("click", function () {
    logEl.textContent = "";
  });

  // Allow Enter key in inputs
  document.getElementById("sub-name").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-subscribe").click();
  });
  document.getElementById("sub-subject").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-subscribe").click();
  });
  document.getElementById("pub-message").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish").click();
  });

  // Initial render, then connect after page resources have loaded
  renderFlow();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
