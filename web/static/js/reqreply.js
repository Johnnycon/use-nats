(function () {
  "use strict";

  const statusEl = document.getElementById("nats-status");
  const logEl = document.getElementById("event-log");
  const respondersEl = document.getElementById("responders");
  const flowEl = document.getElementById("flow-diagram");

  let ws = null;
  let closing = false;
  let reqCount = 0;
  let publishFlash = false;
  const flashingIds = new Set();
  const activeResponders = new Map(); // id -> {name, subject, delayMs, repliedCount}

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
    const url = `ws://${location.host}/demo/reqreply/ws`;
    statusEl.textContent = "connecting...";
    statusEl.classList.remove("connected");

    ws = new WebSocket(url);

    ws.onopen = () => {
      statusEl.textContent = "connected";
      statusEl.classList.add("connected");
      addLog("system", "Connected to server");
    };

    ws.onclose = (e) => {
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
        case "responder_added":
          onResponderAdded(evt.payload);
          break;
        case "responder_removed":
          onResponderRemoved(evt.payload);
          break;
        case "request_sent":
          onRequestSent(evt.payload);
          break;
        case "responder_replied":
          onResponderReplied(evt.payload);
          break;
        case "reply_received":
          onReplyReceived(evt.payload);
          break;
        case "request_timeout":
          onRequestTimeout(evt.payload);
          break;
      }
    };
  }

  // --- Event handlers ---
  function onResponderAdded(p) {
    activeResponders.set(p.respId, {
      name: p.name,
      subject: p.subject,
      delayMs: p.delayMs,
      repliedCount: 0,
    });
    renderResponders();
    renderFlow();
    const delayLabel = p.delayMs > 0 ? ` (${p.delayMs}ms delay)` : "";
    addLog("subscribe", `${p.name} listening on "${p.subject}"${delayLabel}`);
  }

  function onResponderRemoved(p) {
    const name = activeResponders.get(p.respId)?.name || p.respId;
    activeResponders.delete(p.respId);
    renderResponders();
    renderFlow();
    addLog("unsubscribe", `${name} removed`);
  }

  function onRequestSent(p) {
    reqCount++;
    publishFlash = true;
    setTimeout(() => { publishFlash = false; }, 300);
    renderFlow();
    addLog("publish", `Request to "${p.subject}": ${p.data} (timeout ${p.timeoutMs}ms)`);
  }

  function onResponderReplied(p) {
    const resp = activeResponders.get(p.respId);
    if (resp) resp.repliedCount++;
    flashingIds.add(p.respId);
    setTimeout(() => { flashingIds.delete(p.respId); }, 400);
    renderResponders();
    renderFlow();
    const delayLabel = p.delayMs > 0 ? ` (${p.delayMs}ms processing)` : "";
    addLog("receive", `${p.name} replied to "${p.subject}"${delayLabel}`);
  }

  function onReplyReceived(p) {
    pulseNode(".flow-req");
    addLog("reply", `Reply received on "${p.subject}" in ${p.latencyMs}ms: ${p.reply}`);
  }

  function onRequestTimeout(p) {
    addLog("error", `Timeout on "${p.subject}" after ${p.latencyMs}ms: ${p.error}`);
  }

  // --- Group responders by name ---
  function groupedResponders() {
    const groups = new Map(); // name -> [{id, subject, delayMs, repliedCount}]
    for (const [id, r] of activeResponders) {
      const name = r.name || id;
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name).push({
        id,
        subject: r.subject,
        delayMs: r.delayMs,
        repliedCount: r.repliedCount,
      });
    }
    return groups;
  }

  // --- Flow diagram rendering ---
  function renderFlow() {
    flowEl.textContent = "";

    if (activeResponders.size === 0) {
      flowEl.appendChild(
        makeEl("p", "empty-state", "Add a responder to see message flow.")
      );
      return;
    }

    // Requester column
    const reqCol = makeEl("div", "flow-col flow-col-pub");
    const reqNode = makeEl("div", "flow-node flow-req publisher-node");
    if (publishFlash) reqNode.classList.add("pulse");
    reqNode.appendChild(makeEl("div", "node-label", "Requester"));
    reqNode.appendChild(makeEl("div", "node-counter", String(reqCount)));
    reqCol.appendChild(reqNode);

    // NATS column
    const natsCol = makeEl("div", "flow-col flow-col-nats");
    const natsNode = makeEl("div", "flow-node flow-nats nats-node");
    if (publishFlash) natsNode.classList.add("pulse");
    natsNode.appendChild(makeEl("div", "node-label", "NATS"));
    natsCol.appendChild(natsNode);

    // Responders column -- grouped by name
    const respCol = makeEl("div", "flow-col flow-col-subs");
    for (const [name, resps] of groupedResponders()) {
      const totalReplied = resps.reduce((sum, r) => sum + r.repliedCount, 0);
      const node = makeEl("div", "flow-node flow-sub subscriber-node");
      node.id = `flow-group-${name}`;
      if (resps.some(r => flashingIds.has(r.id))) node.classList.add("pulse");
      node.appendChild(makeEl("div", "node-name", name));
      for (const r of resps) {
        const detail = makeEl("div", "node-subject");
        detail.textContent =
          r.subject + (r.delayMs > 0 ? ` (${r.delayMs}ms)` : "");
        node.appendChild(detail);
      }
      node.appendChild(makeEl("div", "node-counter", String(totalReplied)));
      respCol.appendChild(node);
    }

    // Arrows (bidirectional to represent request/reply)
    const arrow1 = makeEl("div", "flow-arrow-col flow-arrow-bidi");
    const arrowRight1 = makeEl("div", "arrow-line-h");
    arrow1.appendChild(arrowRight1);

    const arrow2 = makeEl("div", "flow-arrow-col flow-arrow-bidi");
    const arrowRight2 = makeEl("div", "arrow-line-h");
    arrow2.appendChild(arrowRight2);

    flowEl.appendChild(reqCol);
    flowEl.appendChild(arrow1);
    flowEl.appendChild(natsCol);
    flowEl.appendChild(arrow2);
    flowEl.appendChild(respCol);
  }

  // --- UI rendering ---
  function renderResponders() {
    respondersEl.textContent = "";
    if (activeResponders.size === 0) {
      respondersEl.appendChild(
        makeEl("p", "empty-state", "No responders yet. Add one above.")
      );
      return;
    }
    for (const [name, resps] of groupedResponders()) {
      const card = makeEl("div", "sub-card-group");
      const totalReplied = resps.reduce((sum, r) => sum + r.repliedCount, 0);

      const header = makeEl("div", "sub-group-header");
      header.appendChild(makeEl("span", "sub-id", name));
      const headerRight = makeEl("span", "sub-header-stats");
      headerRight.appendChild(
        makeEl("span", "sub-recv-total", String(totalReplied))
      );
      headerRight.appendChild(
        makeEl(
          "span",
          "sub-count",
          `${resps.length} resp${resps.length > 1 ? "s" : ""}`
        )
      );
      header.appendChild(headerRight);
      card.appendChild(header);

      for (const r of resps) {
        const row = makeEl("div", "sub-row");
        row.id = `card-${r.id}`;
        if (flashingIds.has(r.id)) row.classList.add("flash");

        const subjectLabel = makeEl("span", "sub-subject");
        subjectLabel.textContent =
          r.subject + (r.delayMs > 0 ? ` (${r.delayMs}ms delay)` : "");
        row.appendChild(subjectLabel);

        const rowRight = makeEl("span", "sub-row-stats");
        rowRight.appendChild(
          makeEl("span", "sub-recv-count", String(r.repliedCount))
        );
        const btn = makeEl("button", "btn btn-small btn-danger", "Remove");
        btn.addEventListener("click", () => {
          send({ action: "remove_responder", respId: r.id });
        });
        rowRight.appendChild(btn);
        row.appendChild(rowRight);
        card.appendChild(row);
      }

      respondersEl.appendChild(card);
    }
  }

  function highlightResp(respId) {
    const card = document.getElementById(`card-${respId}`);
    if (!card) return;
    card.classList.add("flash");
    setTimeout(() => card.classList.remove("flash"), 400);
  }

  function pulseNode(selector) {
    const el = document.querySelector(selector);
    if (!el) return;
    el.classList.add("pulse");
    setTimeout(() => el.classList.remove("pulse"), 300);
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
  document.getElementById("btn-add-responder").addEventListener("click", () => {
    const name = document.getElementById("resp-name").value.trim();
    const subject = document.getElementById("resp-subject").value.trim();
    const delayMs = parseInt(document.getElementById("resp-delay").value, 10) || 0;
    if (subject) send({ action: "add_responder", subject, name, delayMs });
  });

  document.getElementById("btn-request").addEventListener("click", () => {
    const subject = document.getElementById("req-subject").value.trim();
    const payload = document.getElementById("req-payload").value.trim();
    const timeoutMs =
      parseInt(document.getElementById("req-timeout").value, 10) || 3000;
    if (subject) send({ action: "request", subject, payload, timeoutMs });
  });

  document.getElementById("btn-burst").addEventListener("click", () => {
    const subject = document.getElementById("req-subject").value.trim();
    const timeoutMs =
      parseInt(document.getElementById("req-timeout").value, 10) || 3000;
    if (!subject) return;
    for (let i = 1; i <= 5; i++) {
      send({
        action: "request",
        subject,
        payload: `Burst request #${i}`,
        timeoutMs,
      });
    }
  });

  document.getElementById("btn-setup-microservices").addEventListener("click", () => {
    var steps = [
      { action: "add_responder", subject: "auth.validate", name: "AuthService", delayMs: 50 },
      { action: "add_responder", subject: "users.lookup", name: "UserService", delayMs: 120 },
      { action: "add_responder", subject: "payments.charge", name: "PaymentService", delayMs: 200 },
    ];
    steps.forEach(function (step, i) {
      setTimeout(function () { send(step); }, i * 100);
    });
  });

  document.getElementById("btn-preset-validate").addEventListener("click", () => {
    send({
      action: "request",
      subject: "auth.validate",
      payload: '{"token": "eyJhbGciOi...","userId": "usr_8291"}',
      timeoutMs: 3000,
    });
  });

  document.getElementById("btn-preset-charge").addEventListener("click", () => {
    send({
      action: "request",
      subject: "payments.charge",
      payload: '{"orderId": "ORD-1042", "amount": 79.99, "currency": "USD"}',
      timeoutMs: 3000,
    });
  });

  document.getElementById("btn-clear-log").addEventListener("click", () => {
    logEl.textContent = "";
  });

  // Allow Enter key in inputs
  document.getElementById("resp-name").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-add-responder").click();
  });
  document.getElementById("resp-subject").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-add-responder").click();
  });
  document.getElementById("resp-delay").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-add-responder").click();
  });
  document.getElementById("req-payload").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-request").click();
  });
  document.getElementById("req-subject").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-request").click();
  });

  // Initial render, then connect after page resources have loaded
  renderFlow();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
