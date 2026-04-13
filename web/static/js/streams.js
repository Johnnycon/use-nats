(function () {
  "use strict";

  var statusEl = document.getElementById("nats-status");
  var logEl = document.getElementById("event-log");
  var streamInfoEl = document.getElementById("stream-info-card");
  var storedMsgsEl = document.getElementById("stored-messages");

  var ws = null;
  var closing = false;
  var publishFlash = false;
  var currentStream = null; // {name, subjects, maxMsgs, storage, messages, bytes, firstSeq, lastSeq, consumers, created}
  var replayedMessages = []; // [{sequence, subject, data, timestamp}]

  // Clean up WebSocket on page unload to free Chrome connection slots
  window.addEventListener("beforeunload", function () {
    closing = true;
    if (ws) ws.close();
  });

  // --- helpers ---
  function makeEl(tag, className, text) {
    var el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  }

  function formatBytes(bytes) {
    if (bytes === 0) return "0 B";
    var units = ["B", "KB", "MB", "GB"];
    var i = 0;
    var val = bytes;
    while (val >= 1024 && i < units.length - 1) {
      val /= 1024;
      i++;
    }
    return (i === 0 ? val : val.toFixed(1)) + " " + units[i];
  }

  function formatTime(isoStr) {
    try {
      var d = new Date(isoStr);
      return d.toLocaleTimeString("en-US", { hour12: false }) + " " + d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    } catch (e) {
      return isoStr;
    }
  }

  // --- WebSocket with auto-reconnect ---
  function connect() {
    var url = "ws://" + location.host + "/demo/streams/ws";
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
        case "stream_created":
          onStreamCreated(evt.payload);
          break;
        case "published":
          onPublished(evt.payload);
          break;
        case "stream_info":
          onStreamInfo(evt.payload);
          break;
        case "replay_start":
          onReplayStart(evt.payload);
          break;
        case "replay_message":
          onReplayMessage(evt.payload);
          break;
        case "replay_end":
          onReplayEnd(evt.payload);
          break;
        case "stream_purged":
          onStreamPurged(evt.payload);
          break;
        case "stream_deleted":
          onStreamDeleted(evt.payload);
          break;
        case "error":
          onError(evt.payload);
          break;
      }
    };
  }

  // --- Event handlers ---
  function onStreamCreated(p) {
    currentStream = {
      name: p.name,
      subjects: p.subjects,
      maxMsgs: p.maxMsgs,
      storage: p.storage,
      messages: 0,
      bytes: 0,
      firstSeq: 0,
      lastSeq: 0,
      consumers: 0,
      created: p.created
    };
    replayedMessages = [];
    renderStreamInfo();
    renderStoredMessages();
    addLog("subscribe", "Stream \"" + p.name + "\" created -- subjects: [" + p.subjects.join(", ") + "], max: " + p.maxMsgs + " msgs, storage: " + p.storage);
  }

  function onPublished(p) {
    publishFlash = true;
    setTimeout(function () { publishFlash = false; }, 300);

    // Update local stream info if we have it
    if (currentStream && currentStream.name === p.stream) {
      currentStream.lastSeq = p.sequence;
      if (currentStream.firstSeq === 0) currentStream.firstSeq = p.sequence;
      currentStream.messages++;
      renderStreamInfo();
    }

    addLog("publish", "Published to \"" + p.subject + "\" -> seq #" + p.sequence + " on stream " + p.stream);
  }

  function onStreamInfo(p) {
    currentStream = {
      name: p.name,
      subjects: p.subjects,
      maxMsgs: p.maxMsgs,
      storage: p.storage,
      messages: p.messages,
      bytes: p.bytes,
      firstSeq: p.firstSeq,
      lastSeq: p.lastSeq,
      consumers: p.consumers,
      created: p.created
    };
    renderStreamInfo();
    addLog("receive", "Stream \"" + p.name + "\": " + p.messages + " msgs, " + formatBytes(p.bytes) + ", seq " + p.firstSeq + "-" + p.lastSeq);
  }

  function onReplayStart(p) {
    replayedMessages = [];
    renderStoredMessages();
    addLog("system", "Replaying " + p.messages + " messages from stream \"" + p.stream + "\"...");
  }

  function onReplayMessage(p) {
    replayedMessages.push({
      sequence: p.sequence,
      subject: p.subject,
      data: p.data,
      timestamp: p.timestamp
    });
    renderStoredMessages();
    addLog("receive", "Replay seq #" + p.sequence + " [" + p.subject + "]: " + p.data);
  }

  function onReplayEnd(p) {
    addLog("system", "Replay complete: " + p.replayed + " messages from stream \"" + p.stream + "\"");
  }

  function onStreamPurged(p) {
    if (currentStream && currentStream.name === p.stream) {
      currentStream.messages = 0;
      currentStream.bytes = 0;
      currentStream.firstSeq = 0;
      currentStream.lastSeq = 0;
      renderStreamInfo();
    }
    replayedMessages = [];
    renderStoredMessages();
    addLog("unsubscribe", "Stream \"" + p.stream + "\" purged -- all messages removed");
  }

  function onStreamDeleted(p) {
    if (currentStream && currentStream.name === p.stream) {
      currentStream = null;
      renderStreamInfo();
    }
    replayedMessages = [];
    renderStoredMessages();
    addLog("unsubscribe", "Stream \"" + p.stream + "\" deleted");
  }

  function onError(p) {
    addLog("error", p.message);
  }

  // --- Render stream info card ---
  function renderStreamInfo() {
    streamInfoEl.textContent = "";

    if (!currentStream) {
      streamInfoEl.appendChild(makeEl("p", "empty-state", "No stream created yet. Create one to see stats."));
      return;
    }

    var card = makeEl("div", "si-card");

    // Stream name row
    var nameRow = makeEl("div", "si-row si-name-row");
    nameRow.appendChild(makeEl("span", "si-label", "Name"));
    nameRow.appendChild(makeEl("span", "si-value si-name", currentStream.name));
    card.appendChild(nameRow);

    // Subjects row
    var subjRow = makeEl("div", "si-row");
    subjRow.appendChild(makeEl("span", "si-label", "Subjects"));
    subjRow.appendChild(makeEl("span", "si-value", currentStream.subjects.join(", ")));
    card.appendChild(subjRow);

    // Stats grid
    var grid = makeEl("div", "si-stats-grid");

    var msgStat = makeEl("div", "si-stat");
    msgStat.appendChild(makeEl("div", "si-stat-value", String(currentStream.messages)));
    msgStat.appendChild(makeEl("div", "si-stat-label", "Messages"));
    grid.appendChild(msgStat);

    var bytesStat = makeEl("div", "si-stat");
    bytesStat.appendChild(makeEl("div", "si-stat-value", formatBytes(currentStream.bytes)));
    bytesStat.appendChild(makeEl("div", "si-stat-label", "Bytes"));
    grid.appendChild(bytesStat);

    var firstStat = makeEl("div", "si-stat");
    firstStat.appendChild(makeEl("div", "si-stat-value", currentStream.firstSeq ? String(currentStream.firstSeq) : "--"));
    firstStat.appendChild(makeEl("div", "si-stat-label", "First Seq"));
    grid.appendChild(firstStat);

    var lastStat = makeEl("div", "si-stat");
    lastStat.appendChild(makeEl("div", "si-stat-value", currentStream.lastSeq ? String(currentStream.lastSeq) : "--"));
    lastStat.appendChild(makeEl("div", "si-stat-label", "Last Seq"));
    grid.appendChild(lastStat);

    card.appendChild(grid);

    // Storage + created row
    var metaRow = makeEl("div", "si-row si-meta-row");
    metaRow.appendChild(makeEl("span", "si-label", "Storage"));
    metaRow.appendChild(makeEl("span", "si-value", currentStream.storage));
    card.appendChild(metaRow);

    var maxRow = makeEl("div", "si-row");
    maxRow.appendChild(makeEl("span", "si-label", "Max Msgs"));
    maxRow.appendChild(makeEl("span", "si-value", String(currentStream.maxMsgs)));
    card.appendChild(maxRow);

    var createdRow = makeEl("div", "si-row");
    createdRow.appendChild(makeEl("span", "si-label", "Created"));
    createdRow.appendChild(makeEl("span", "si-value", formatTime(currentStream.created)));
    card.appendChild(createdRow);

    if (publishFlash) {
      card.classList.add("pulse");
    }

    streamInfoEl.appendChild(card);
  }

  // --- Render stored messages ---
  function renderStoredMessages() {
    storedMsgsEl.textContent = "";

    if (replayedMessages.length === 0) {
      storedMsgsEl.appendChild(makeEl("p", "empty-state", "Replay messages to see stored data."));
      return;
    }

    for (var i = 0; i < replayedMessages.length; i++) {
      var msg = replayedMessages[i];
      var row = makeEl("div", "sm-row");

      var seqBadge = makeEl("span", "sm-seq", "#" + msg.sequence);
      row.appendChild(seqBadge);

      var subjSpan = makeEl("span", "sm-subject", msg.subject);
      row.appendChild(subjSpan);

      var tsSpan = makeEl("span", "sm-timestamp", formatTime(msg.timestamp));
      row.appendChild(tsSpan);

      var dataDiv = makeEl("div", "sm-data", msg.data);
      row.appendChild(dataDiv);

      storedMsgsEl.appendChild(row);
    }

    storedMsgsEl.scrollTop = storedMsgsEl.scrollHeight;
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
  document.getElementById("btn-create-stream").addEventListener("click", function () {
    var name = document.getElementById("stream-name").value.trim();
    var subjects = document.getElementById("stream-subjects").value.trim();
    var maxMsgs = parseInt(document.getElementById("stream-max-msgs").value.trim(), 10);
    if (!name) return;
    send({ action: "create_stream", streamName: name, subjects: subjects, maxMsgs: maxMsgs || 1000 });
  });

  document.getElementById("btn-publish").addEventListener("click", function () {
    var subject = document.getElementById("pub-subject").value.trim();
    var message = document.getElementById("pub-message").value.trim();
    if (subject) send({ action: "publish", subject: subject, message: message });
  });

  document.getElementById("btn-stream-info").addEventListener("click", function () {
    var name = document.getElementById("stream-name").value.trim() || "ORDERS";
    send({ action: "stream_info", streamName: name });
  });

  document.getElementById("btn-replay").addEventListener("click", function () {
    var name = document.getElementById("stream-name").value.trim() || "ORDERS";
    send({ action: "replay", streamName: name });
  });

  document.getElementById("btn-purge").addEventListener("click", function () {
    var name = document.getElementById("stream-name").value.trim() || "ORDERS";
    send({ action: "purge", streamName: name });
  });

  document.getElementById("btn-delete").addEventListener("click", function () {
    var name = document.getElementById("stream-name").value.trim() || "ORDERS";
    send({ action: "delete_stream", streamName: name });
  });

  // Preset buttons
  document.getElementById("btn-setup-orders").addEventListener("click", function () {
    send({ action: "create_stream", streamName: "ORDERS", subjects: "orders.>", maxMsgs: 1000 });
  });

  document.getElementById("btn-preset-publish").addEventListener("click", function () {
    var events = [
      { subject: "orders.created", message: '{"orderId":"ORD-1001","customer":"Alice","item":"Wireless Headphones","amount":79.99}' },
      { subject: "orders.paid", message: '{"orderId":"ORD-1001","method":"credit_card","amount":79.99}' },
      { subject: "orders.created", message: '{"orderId":"ORD-1002","customer":"Bob","item":"Mechanical Keyboard","amount":149.00}' },
      { subject: "orders.shipped", message: '{"orderId":"ORD-1001","carrier":"FedEx","tracking":"FX-98765"}' },
      { subject: "orders.delivered", message: '{"orderId":"ORD-1001","signature":"Alice S.","time":"14:32"}' }
    ];
    events.forEach(function (evt, i) {
      setTimeout(function () {
        send({ action: "publish", subject: evt.subject, message: evt.message });
      }, i * 150);
    });
  });

  document.getElementById("btn-preset-replay").addEventListener("click", function () {
    send({ action: "replay", streamName: "ORDERS" });
  });

  document.getElementById("btn-clear-log").addEventListener("click", function () {
    logEl.textContent = "";
  });

  // Allow Enter key in inputs
  document.getElementById("stream-name").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-create-stream").click();
  });
  document.getElementById("stream-subjects").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-create-stream").click();
  });
  document.getElementById("stream-max-msgs").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-create-stream").click();
  });
  document.getElementById("pub-subject").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish").click();
  });
  document.getElementById("pub-message").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish").click();
  });

  // Initial render, then connect after page resources have loaded
  renderStreamInfo();
  renderStoredMessages();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
