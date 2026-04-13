(function () {
  "use strict";

  const statusEl = document.getElementById("nats-status");
  const logEl = document.getElementById("event-log");
  const workersEl = document.getElementById("workers");
  const flowEl = document.getElementById("flow-diagram");

  let ws = null;
  let closing = false;
  let pubCount = 0;
  let publishFlash = false;
  const flashingIds = new Set();
  // id -> {name, subject, queueGroup, recvCount}
  const activeWorkers = new Map();

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
    const url = `ws://${location.host}/demo/queuegroup/ws`;
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
        case "worker_added":
          onWorkerAdded(evt.payload);
          break;
        case "worker_removed":
          onWorkerRemoved(evt.payload);
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
  function onWorkerAdded(p) {
    activeWorkers.set(p.workerId, {
      name: p.name,
      subject: p.subject,
      queueGroup: p.queueGroup,
      recvCount: 0,
    });
    renderWorkers();
    renderFlow();
    const qLabel = p.queueGroup
      ? ` in queue "${p.queueGroup}"`
      : " (no queue group)";
    addLog("subscribe", `${p.name} subscribed to "${p.subject}"${qLabel}`);
  }

  function onWorkerRemoved(p) {
    const name = activeWorkers.get(p.workerId)?.name || p.workerId;
    activeWorkers.delete(p.workerId);
    renderWorkers();
    renderFlow();
    addLog("unsubscribe", `${name} removed`);
  }

  function onPublished(p) {
    pubCount++;
    publishFlash = true;
    setTimeout(() => { publishFlash = false; }, 300);
    renderFlow();
    addLog("publish", `Published to "${p.subject}": ${p.data}`);
  }

  function onMessageReceived(p) {
    const w = activeWorkers.get(p.workerId);
    if (w) w.recvCount++;
    flashingIds.add(p.workerId);
    setTimeout(() => { flashingIds.delete(p.workerId); }, 400);
    renderWorkers();
    renderFlow();
    const qLabel = p.queueGroup ? ` [${p.queueGroup}]` : " [no group]";
    addLog(
      "receive",
      `${p.name}${qLabel} received on "${p.subject}": ${p.data}`
    );
  }

  // --- Group workers by queue group for the flow diagram ---
  // Returns: Map<groupLabel, [{id, name, subject, queueGroup, recvCount}]>
  // Non-grouped workers each get their own "group" keyed by "(none):<id>"
  function workersByQueueGroup() {
    const groups = new Map();
    for (const [id, w] of activeWorkers) {
      const key = w.queueGroup || `__none__:${id}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push({
        id,
        name: w.name,
        subject: w.subject,
        queueGroup: w.queueGroup,
        recvCount: w.recvCount,
      });
    }
    return groups;
  }

  // --- Group workers by name for the worker cards ---
  function workersByName() {
    const groups = new Map();
    for (const [id, w] of activeWorkers) {
      const name = w.name || id;
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name).push({
        id,
        subject: w.subject,
        queueGroup: w.queueGroup,
        recvCount: w.recvCount,
      });
    }
    return groups;
  }

  // --- Flow diagram rendering ---
  function renderFlow() {
    flowEl.textContent = "";

    if (activeWorkers.size === 0) {
      flowEl.appendChild(
        makeEl("p", "empty-state", "Add a worker to see message flow.")
      );
      return;
    }

    // Publisher column
    const pubCol = makeEl("div", "flow-col flow-col-pub");
    const pubNode = makeEl("div", "flow-node flow-pub publisher-node");
    if (publishFlash) pubNode.classList.add("pulse");
    pubNode.appendChild(makeEl("div", "node-label", "Publisher"));
    pubNode.appendChild(makeEl("div", "node-counter", String(pubCount)));
    pubCol.appendChild(pubNode);

    // NATS column
    const natsCol = makeEl("div", "flow-col flow-col-nats");
    const natsNode = makeEl("div", "flow-node flow-nats nats-node");
    if (publishFlash) natsNode.classList.add("pulse");
    natsNode.appendChild(makeEl("div", "node-label", "NATS"));
    natsCol.appendChild(natsNode);

    // Workers column -- grouped by queue group
    const workerCol = makeEl("div", "flow-col flow-col-subs");
    const qGroups = workersByQueueGroup();

    for (const [key, members] of qGroups) {
      const isQueued = !key.startsWith("__none__:");

      if (isQueued) {
        // Render a queue group container with all members inside
        const groupBox = makeEl("div", "qg-group-box");
        const groupLabel = makeEl("div", "qg-group-label", `queue: ${key}`);
        groupBox.appendChild(groupLabel);

        for (const m of members) {
          const node = makeEl("div", "flow-node flow-sub subscriber-node");
          node.id = `flow-worker-${m.id}`;
          if (flashingIds.has(m.id)) node.classList.add("pulse");
          node.appendChild(makeEl("div", "node-name", m.name));
          node.appendChild(makeEl("div", "node-subject", m.subject));
          node.appendChild(
            makeEl("div", "node-counter", String(m.recvCount))
          );
          groupBox.appendChild(node);
        }

        workerCol.appendChild(groupBox);
      } else {
        // Non-grouped: render as standalone node
        const m = members[0];
        const node = makeEl("div", "flow-node flow-sub subscriber-node");
        node.id = `flow-worker-${m.id}`;
        if (flashingIds.has(m.id)) node.classList.add("pulse");
        node.appendChild(makeEl("div", "node-name", m.name));
        node.appendChild(makeEl("div", "node-subject", m.subject));
        const noGroupTag = makeEl("div", "qg-no-group-tag", "no group");
        node.appendChild(noGroupTag);
        node.appendChild(
          makeEl("div", "node-counter", String(m.recvCount))
        );
        workerCol.appendChild(node);
      }
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
    flowEl.appendChild(workerCol);
  }

  // --- Worker cards rendering ---
  function renderWorkers() {
    workersEl.textContent = "";
    if (activeWorkers.size === 0) {
      workersEl.appendChild(
        makeEl("p", "empty-state", "No workers yet. Add one above.")
      );
      return;
    }
    for (const [name, members] of workersByName()) {
      const card = makeEl("div", "sub-card-group");
      const totalRecv = members.reduce((sum, m) => sum + m.recvCount, 0);

      const header = makeEl("div", "sub-group-header");
      header.appendChild(makeEl("span", "sub-id", name));
      const headerRight = makeEl("span", "sub-header-stats");
      headerRight.appendChild(
        makeEl("span", "sub-recv-total", String(totalRecv))
      );
      headerRight.appendChild(
        makeEl(
          "span",
          "sub-count",
          `${members.length} worker${members.length > 1 ? "s" : ""}`
        )
      );
      header.appendChild(headerRight);
      card.appendChild(header);

      for (const m of members) {
        const row = makeEl("div", "sub-row");
        row.id = `card-${m.id}`;
        if (flashingIds.has(m.id)) row.classList.add("flash");

        const leftSpan = makeEl("span", "sub-subject");
        const qLabel = m.queueGroup
          ? `${m.subject}  [${m.queueGroup}]`
          : `${m.subject}  (no group)`;
        leftSpan.textContent = qLabel;
        row.appendChild(leftSpan);

        const rowRight = makeEl("span", "sub-row-stats");
        rowRight.appendChild(
          makeEl("span", "sub-recv-count", String(m.recvCount))
        );
        const btn = makeEl("button", "btn btn-small btn-danger", "Remove");
        btn.addEventListener("click", () => {
          send({ action: "remove_worker", workerId: m.id });
        });
        rowRight.appendChild(btn);
        row.appendChild(rowRight);
        card.appendChild(row);
      }

      workersEl.appendChild(card);
    }
  }

  function highlightWorker(workerId) {
    const card = document.getElementById(`card-${workerId}`);
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
  document.getElementById("btn-add-worker").addEventListener("click", () => {
    const name = document.getElementById("worker-name").value.trim();
    const subject = document.getElementById("worker-subject").value.trim();
    const queueGroup = document.getElementById("worker-queue").value.trim();
    if (subject)
      send({ action: "add_worker", subject, name, queueGroup });
  });

  document.getElementById("btn-publish").addEventListener("click", () => {
    const subject = document.getElementById("pub-subject").value.trim();
    const message = document.getElementById("pub-message").value.trim();
    if (subject) send({ action: "publish", subject, message });
  });

  document.getElementById("btn-burst").addEventListener("click", () => {
    const subject = document.getElementById("pub-subject").value.trim();
    if (!subject) return;
    for (let i = 1; i <= 10; i++) {
      send({ action: "publish", subject, message: `Burst task #${i}` });
    }
  });

  document.getElementById("btn-setup-pipeline").addEventListener("click", () => {
    var steps = [
      { action: "add_worker", subject: "images.process", name: "ResizeWorker-1", queueGroup: "image-processors" },
      { action: "add_worker", subject: "images.process", name: "ResizeWorker-2", queueGroup: "image-processors" },
      { action: "add_worker", subject: "images.process", name: "ResizeWorker-3", queueGroup: "image-processors" },
      { action: "add_worker", subject: "images.process", name: "AuditLogger", queueGroup: "" },
    ];
    steps.forEach(function (step, i) {
      setTimeout(function () { send(step); }, i * 100);
    });
  });

  document.getElementById("btn-upload-5").addEventListener("click", () => {
    var images = [
      '{"file": "photo_001.jpg", "size": "4.2MB"}',
      '{"file": "photo_002.jpg", "size": "3.8MB"}',
      '{"file": "photo_003.png", "size": "6.1MB"}',
      '{"file": "photo_004.jpg", "size": "2.9MB"}',
      '{"file": "photo_005.png", "size": "5.5MB"}',
    ];
    images.forEach(function (payload, i) {
      setTimeout(function () {
        send({ action: "publish", subject: "images.process", message: payload });
      }, i * 100);
    });
  });

  document.getElementById("btn-clear-log").addEventListener("click", () => {
    logEl.textContent = "";
  });

  // Allow Enter key in inputs
  document.getElementById("worker-name").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-add-worker").click();
  });
  document.getElementById("worker-subject").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-add-worker").click();
  });
  document.getElementById("worker-queue").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("btn-add-worker").click();
  });
  document.getElementById("pub-message").addEventListener("keydown", (e) => {
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
