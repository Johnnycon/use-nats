(function () {
  "use strict";

  var statusEl = document.getElementById("nats-status");
  var logEl = document.getElementById("event-log");
  var streamInfoEl = document.getElementById("stream-info");
  var messagesEl = document.getElementById("messages-table");
  var batchEl = document.getElementById("batch-results");
  var replayEl = document.getElementById("replay-results");

  var ws = null;
  var closing = false;
  var streamReady = false;
  var messages = [];      // {msgId, subject, data, sequence, duplicate, timestamp}
  var lastBatch = null;   // {count, originals, duplicates}
  var replayMsgs = [];    // from replay
  var flashSet = new Set();

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
    var url = "ws://" + location.host + "/demo/dedup/ws";
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
        case "stream_ready":   onStreamReady(evt.payload); break;
        case "published":      onPublished(evt.payload); break;
        case "batch_result":   onBatchResult(evt.payload); break;
        case "stream_info":    onStreamInfo(evt.payload); break;
        case "replay":         onReplay(evt.payload); break;
        case "error":          onError(evt.payload); break;
      }
    };
  }

  // --- Event handlers ---
  function onStreamReady(p) {
    streamReady = true;
    renderStreamInfo(p);
    addLog("system", "Stream " + p.name + " ready (dedup window: " + p.dedupWindow + "s)");
    checkPendingEvent("stream_ready");
  }

  function onPublished(p) {
    messages.push({
      msgId: p.msgId,
      subject: p.subject,
      data: p.data,
      sequence: p.sequence,
      duplicate: p.duplicate,
      timestamp: p.timestamp
    });

    // Auto-fill the dedup ID field with the last published ID
    if (!p.duplicate && p.msgId !== "(none)") {
      document.getElementById("dedup-id").value = p.msgId;
    }

    var flashKey = "msg-" + messages.length;
    flashSet.add(flashKey);
    setTimeout(function () { flashSet.delete(flashKey); renderMessages(); }, 400);
    renderMessages();

    if (p.duplicate) {
      addLog("error", "DUPLICATE rejected: " + p.msgId + " (seq #" + p.sequence + ")");
    } else {
      addLog("publish", "Stored: " + p.msgId + " -> seq #" + p.sequence);
    }
    checkPendingEvent("published");
  }

  function onBatchResult(p) {
    lastBatch = p;
    renderBatch();

    var dupes = 0;
    for (var i = 0; i < p.duplicates.length; i++) {
      if (p.duplicates[i].duplicate) dupes++;
    }
    addLog("system", "Batch: " + p.count + " originals stored, " + dupes + "/" + p.count + " duplicates rejected");
  }

  function onStreamInfo(p) {
    renderStreamInfo(p);
    addLog("system", "Stream: " + p.messages + " msgs, " + p.bytes + " bytes (dedup window: " + p.dedupWindow + "s)");
  }

  function onReplay(p) {
    replayMsgs = p.messages || [];
    renderReplay();
    addLog("system", "Replay: " + p.count + " messages in stream");
  }

  function onError(p) {
    addLog("error", p.message);
  }

  // --- Render stream info ---
  function renderStreamInfo(p) {
    streamInfoEl.textContent = "";

    var card = makeEl("div", "dd-stream-card");

    var header = makeEl("div", "dd-stream-header");
    header.appendChild(makeEl("span", "dd-stream-name", p.name));
    var dedupBadge = makeEl("span", "dd-dedup-badge", "DEDUP " + p.dedupWindow + "s");
    header.appendChild(dedupBadge);
    card.appendChild(header);

    var stats = makeEl("div", "dd-stats-grid");
    var items = [
      { label: "Messages", value: p.messages, cls: "dd-stat-cyan" },
      { label: "Subjects", value: (p.subjects || []).join(", "), cls: "" }
    ];
    if (p.bytes !== undefined) {
      items.push({ label: "Bytes", value: p.bytes, cls: "" });
    }
    if (p.firstSeq !== undefined) {
      items.push({ label: "Seq Range", value: p.firstSeq + " - " + p.lastSeq, cls: "" });
    }

    for (var i = 0; i < items.length; i++) {
      var stat = makeEl("div", "dd-stat");
      stat.appendChild(makeEl("span", "dd-stat-value " + items[i].cls, String(items[i].value)));
      stat.appendChild(makeEl("span", "dd-stat-label", items[i].label));
      stats.appendChild(stat);
    }
    card.appendChild(stats);
    streamInfoEl.appendChild(card);
  }

  // --- Render messages table ---
  function renderMessages() {
    messagesEl.textContent = "";

    if (messages.length === 0) {
      messagesEl.appendChild(makeEl("p", "empty-state", "Publish messages to see dedup in action."));
      return;
    }

    var table = makeEl("table", "dd-table");
    var thead = makeEl("thead");
    var headerRow = makeEl("tr");
    var cols = ["#", "Msg ID", "Seq", "Status", "Data"];
    for (var c = 0; c < cols.length; c++) {
      headerRow.appendChild(makeEl("th", "", cols[c]));
    }
    thead.appendChild(headerRow);
    table.appendChild(thead);

    var tbody = makeEl("tbody");
    for (var i = messages.length - 1; i >= 0; i--) {
      var m = messages[i];
      var tr = makeEl("tr");
      var flashKey = "msg-" + (i + 1);
      if (flashSet.has(flashKey)) tr.classList.add("dd-row-flash");
      if (m.duplicate) tr.classList.add("dd-row-dup");

      tr.appendChild(makeEl("td", "dd-cell-idx", String(i + 1)));
      tr.appendChild(makeEl("td", "dd-cell-id", m.msgId));

      var seqTd = makeEl("td", "dd-cell-seq", String(m.sequence));
      tr.appendChild(seqTd);

      var statusTd = makeEl("td");
      if (m.duplicate) {
        statusTd.appendChild(makeEl("span", "dd-badge dd-badge-dup", "DUPLICATE"));
      } else if (m.msgId === "(none)") {
        statusTd.appendChild(makeEl("span", "dd-badge dd-badge-none", "NO ID"));
      } else {
        statusTd.appendChild(makeEl("span", "dd-badge dd-badge-ok", "STORED"));
      }
      tr.appendChild(statusTd);

      var dataTd = makeEl("td", "dd-cell-data");
      var dataStr = m.data;
      if (dataStr.length > 60) dataStr = dataStr.substring(0, 60) + "...";
      dataTd.textContent = dataStr;
      tr.appendChild(dataTd);

      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    messagesEl.appendChild(table);
  }

  // --- Render batch results ---
  function renderBatch() {
    batchEl.textContent = "";

    if (!lastBatch) {
      batchEl.appendChild(makeEl("p", "empty-state", "Run \"Batch Test\" to see originals vs. duplicates side by side."));
      return;
    }

    var wrap = makeEl("div", "dd-batch-grid");

    // Originals column
    var origCol = makeEl("div", "dd-batch-col");
    origCol.appendChild(makeEl("h4", "dd-batch-heading dd-batch-heading-ok", "Originals (" + lastBatch.originals.length + ")"));
    for (var i = 0; i < lastBatch.originals.length; i++) {
      var o = lastBatch.originals[i];
      var row = makeEl("div", "dd-batch-row");
      row.appendChild(makeEl("span", "dd-batch-id", o.msgId));
      row.appendChild(makeEl("span", "dd-cell-seq", "#" + o.sequence));
      row.appendChild(makeEl("span", "dd-badge dd-badge-ok", o.duplicate ? "DUP" : "STORED"));
      origCol.appendChild(row);
    }
    wrap.appendChild(origCol);

    // Arrow
    var arrowCol = makeEl("div", "dd-batch-arrow");
    arrowCol.textContent = "re-send";
    wrap.appendChild(arrowCol);

    // Duplicates column
    var dupCol = makeEl("div", "dd-batch-col");
    dupCol.appendChild(makeEl("h4", "dd-batch-heading dd-batch-heading-dup", "Duplicates (" + lastBatch.duplicates.length + ")"));
    for (var j = 0; j < lastBatch.duplicates.length; j++) {
      var d = lastBatch.duplicates[j];
      var drow = makeEl("div", "dd-batch-row");
      drow.appendChild(makeEl("span", "dd-batch-id", d.msgId));
      drow.appendChild(makeEl("span", "dd-cell-seq", "#" + d.sequence));
      drow.appendChild(makeEl("span", "dd-badge " + (d.duplicate ? "dd-badge-dup" : "dd-badge-ok"),
        d.duplicate ? "REJECTED" : "STORED"));
      dupCol.appendChild(drow);
    }
    wrap.appendChild(dupCol);

    batchEl.appendChild(wrap);
  }

  // --- Render replay ---
  function renderReplay() {
    replayEl.textContent = "";

    if (replayMsgs.length === 0) {
      replayEl.appendChild(makeEl("p", "empty-state", "Click \"Replay Stream\" to read all stored messages."));
      return;
    }

    var summary = makeEl("div", "dd-replay-summary");
    summary.textContent = replayMsgs.length + " messages stored (duplicates were never persisted)";
    replayEl.appendChild(summary);

    var list = makeEl("div", "dd-replay-list");
    for (var i = 0; i < replayMsgs.length; i++) {
      var m = replayMsgs[i];
      var row = makeEl("div", "dd-replay-row");

      row.appendChild(makeEl("span", "dd-cell-seq", "#" + m.sequence));
      row.appendChild(makeEl("span", "dd-cell-id", m.msgId || "(none)"));

      var dataStr = m.data;
      if (dataStr.length > 50) dataStr = dataStr.substring(0, 50) + "...";
      row.appendChild(makeEl("span", "dd-cell-data", dataStr));

      list.appendChild(row);
    }
    replayEl.appendChild(list);
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

  // --- Sequenced demo runner ---
  // Waits for a specific event type before continuing, with a timeout fallback.
  var pendingResolve = null;
  var pendingType = null;

  function waitForEvent(type, timeoutMs) {
    return new Promise(function (resolve) {
      pendingType = type;
      pendingResolve = resolve;
      setTimeout(function () {
        if (pendingResolve === resolve) {
          pendingType = null;
          pendingResolve = null;
          resolve();
        }
      }, timeoutMs || 2000);
    });
  }

  function checkPendingEvent(type) {
    if (pendingType === type && pendingResolve) {
      var fn = pendingResolve;
      pendingType = null;
      pendingResolve = null;
      fn();
    }
  }

  // --- Button handlers ---

  // "Run Demo": setup stream -> publish msg -> re-send same ID as duplicate
  document.getElementById("btn-run-demo").addEventListener("click", function () {
    (async function () {
      // Step 1: setup stream
      send({ action: "setup" });
      await waitForEvent("stream_ready", 3000);

      // Step 2: publish a message (auto-generated ID)
      send({ action: "publish", msgId: "order-123", payload: '{"order":"ORD-123","item":"Widget","qty":1}' });
      await waitForEvent("published", 2000);

      // Step 3: small delay so user can see the first message land
      await new Promise(function (r) { setTimeout(r, 600); });

      // Step 4: re-send the same ID -- this will be rejected as duplicate
      send({ action: "publish_duplicate", dedupId: "order-123" });
    })();
  });

  // "Batch Demo": setup stream (if needed) -> run batch test
  document.getElementById("btn-batch-demo").addEventListener("click", function () {
    (async function () {
      if (!streamReady) {
        send({ action: "setup" });
        await waitForEvent("stream_ready", 3000);
      }
      send({ action: "publish_batch", count: 5 });
    })();
  });

  document.getElementById("btn-stream-info").addEventListener("click", function () {
    send({ action: "stream_info" });
  });

  document.getElementById("btn-replay").addEventListener("click", function () {
    if (!streamReady) {
      addLog("error", "Run the demo first to create the stream");
      return;
    }
    send({ action: "replay" });
  });

  document.getElementById("btn-publish").addEventListener("click", function () {
    if (!streamReady) {
      addLog("error", "Run the demo first to create the stream");
      return;
    }
    var msgId = document.getElementById("msg-id").value.trim();
    var payload = document.getElementById("msg-payload").value.trim();
    send({ action: "publish", msgId: msgId, payload: payload });
  });

  document.getElementById("btn-duplicate").addEventListener("click", function () {
    var dedupId = document.getElementById("dedup-id").value.trim();
    if (!dedupId) {
      addLog("error", "Enter a Message ID to re-send");
      return;
    }
    send({ action: "publish_duplicate", dedupId: dedupId });
  });

  document.getElementById("btn-no-dedup").addEventListener("click", function () {
    if (!streamReady) {
      addLog("error", "Run the demo first to create the stream");
      return;
    }
    send({ action: "publish_no_dedup" });
  });

  document.getElementById("btn-clear-log").addEventListener("click", function () {
    logEl.textContent = "";
  });

  // Enter key support
  document.getElementById("msg-id").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish").click();
  });
  document.getElementById("msg-payload").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish").click();
  });
  document.getElementById("dedup-id").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-duplicate").click();
  });

  // Initial render
  renderMessages();
  renderBatch();
  renderReplay();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
