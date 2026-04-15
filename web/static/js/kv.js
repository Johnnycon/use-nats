(function () {
  "use strict";

  var statusEl = document.getElementById("nats-status");
  var logEl = document.getElementById("event-log");
  var bucketInfoEl = document.getElementById("bucket-info-card");
  var tableBody = document.getElementById("kv-table-body");
  var watcherStatusEl = document.getElementById("kv-watcher-status");
  var kvHistoryEl = document.getElementById("kv-history");

  var ws = null;
  var closing = false;
  var flashingKeys = new Set();
  var currentBucket = null;
  var currentBucketName = "";
  var entries = new Map();        // key -> {key, value, revision, operation, created}
  var watcherActive = false;
  var watcherId = null;
  var historyEntries = [];
  var historyKey = "";

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

  function formatTime(isoStr) {
    try {
      return new Date(isoStr).toLocaleTimeString("en-US", { hour12: false });
    } catch (e) {
      return isoStr;
    }
  }

  function truncate(str, max) {
    if (!str) return "";
    return str.length > max ? str.substring(0, max) + "..." : str;
  }

  // --- WebSocket ---
  function connect() {
    var url = "ws://" + location.host + "/demo/kv/ws";
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
        case "bucket_created":     onBucketCreated(evt.payload); break;
        case "kv_put":             onKvPut(evt.payload); break;
        case "kv_get":             onKvGet(evt.payload); break;
        case "kv_deleted":         onKvDeleted(evt.payload); break;
        case "kv_keys":            onKvKeys(evt.payload); break;
        case "kv_history":         onKvHistory(evt.payload); break;
        case "watcher_created":    onWatcherCreated(evt.payload); break;
        case "kv_watch_init_done": onWatchInitDone(evt.payload); break;
        case "kv_watch_update":    onWatchUpdate(evt.payload); break;
        case "watcher_stopped":    onWatcherStopped(evt.payload); break;
        case "kv_cas_success":     onCASSuccess(evt.payload); break;
        case "kv_cas_conflict":    onCASConflict(evt.payload); break;
        case "bucket_info":        onBucketInfo(evt.payload); break;
        case "error":              onError(evt.payload); break;
      }
    };
  }

  // --- Event handlers ---
  function onBucketCreated(p) {
    currentBucket = p;
    currentBucketName = p.bucket;
    entries.clear();
    renderBucketInfo();
    renderTable();
    addLog("subscribe", "Bucket \"" + p.bucket + "\" created (history: " + p.history + ", backed by stream: " + p.backingStream + ")");
  }

  function onKvPut(p) {
    entries.set(p.key, {
      key: p.key, value: p.value, revision: p.revision,
      operation: "PUT", created: new Date().toISOString()
    });
    flash(p.key);
    addLog("publish", "PUT \"" + p.key + "\" -> rev #" + p.revision);
  }

  function onKvGet(p) {
    if (!p.found) {
      addLog("error", "Key \"" + p.key + "\" not found");
      return;
    }
    entries.set(p.key, {
      key: p.key, value: p.value, revision: p.revision,
      operation: p.operation, created: p.created
    });
    flash(p.key);
    addLog("receive", "GET \"" + p.key + "\" = " + truncate(p.value, 50) + " (rev #" + p.revision + ")");
  }

  function onKvDeleted(p) {
    var existing = entries.get(p.key);
    if (existing) {
      existing.operation = "DEL";
      existing.value = "";
    }
    flash(p.key);
    addLog("unsubscribe", "DEL \"" + p.key + "\"");
  }

  function onKvKeys(p) {
    var keys = p.keys || [];
    addLog("receive", keys.length === 0
      ? "Bucket is empty"
      : "Keys: " + keys.join(", "));
  }

  function onKvHistory(p) {
    historyKey = p.key;
    historyEntries = p.entries || [];
    renderHistory();
    addLog("receive", "History for \"" + p.key + "\": " + historyEntries.length + " revision(s)");
  }

  function onWatcherCreated(p) {
    watcherId = p.watcherId;
    renderWatcherStatus();
    addLog("subscribe", "Watcher started on " + (p.watchKey === ">" ? "all keys" : "\"" + p.watchKey + "\""));
  }

  function onWatchInitDone(p) {
    watcherActive = true;
    renderWatcherStatus();
    addLog("system", "Watcher live -- table will update in real-time");
  }

  function onWatchUpdate(p) {
    if (p.operation === "PUT") {
      entries.set(p.key, {
        key: p.key, value: p.value, revision: p.revision,
        operation: "PUT", created: p.created
      });
    } else if (p.operation === "DEL" || p.operation === "PURGE") {
      var existing = entries.get(p.key);
      if (existing) {
        existing.operation = p.operation;
        existing.value = "";
        existing.revision = p.revision;
      }
    }
    flash(p.key);
    if (watcherActive) {
      addLog("receive", "WATCH " + p.operation + " \"" + p.key + "\" rev #" + p.revision);
    }
  }

  function onWatcherStopped(p) {
    watcherActive = false;
    watcherId = null;
    renderWatcherStatus();
    addLog("unsubscribe", "Watcher stopped");
  }

  function onCASSuccess(p) {
    entries.set(p.key, {
      key: p.key, value: p.value, revision: p.newRevision,
      operation: "PUT", created: new Date().toISOString()
    });
    flash(p.key);
    addLog("publish", "CAS OK: \"" + p.key + "\" rev #" + p.oldRevision + " -> #" + p.newRevision);
  }

  function onCASConflict(p) {
    addLog("error", "CAS FAILED on \"" + p.key + "\": expected rev #" + p.expectedRevision + " but key has been updated. Get the latest revision and retry.");
  }

  function onBucketInfo(p) {
    currentBucket = p;
    renderBucketInfo();
    addLog("system", "Bucket: " + p.values + " keys, " + p.bytes + " bytes, history depth: " + p.history);
  }

  function onError(p) {
    addLog("error", p.message);
  }

  // --- Flash a table row ---
  function flash(key) {
    flashingKeys.add(key);
    renderTable();
    setTimeout(function () {
      flashingKeys.delete(key);
      renderTable();
    }, 500);
  }

  // --- Render bucket info (compact) ---
  function renderBucketInfo() {
    bucketInfoEl.textContent = "";
    if (!currentBucket) return;

    var card = makeEl("div", "si-card");
    var grid = makeEl("div", "si-stats-grid");
    var stats = [
      { val: String(currentBucket.values || 0), label: "Keys" },
      { val: String(currentBucket.history || 0), label: "History" },
      { val: currentBucket.ttl ? currentBucket.ttl + "s" : "none", label: "TTL" },
      { val: currentBucket.backingStream || currentBucket.bucket, label: "Bucket" }
    ];
    for (var i = 0; i < stats.length; i++) {
      var stat = makeEl("div", "si-stat");
      stat.appendChild(makeEl("div", "si-stat-value", stats[i].val));
      stat.appendChild(makeEl("div", "si-stat-label", stats[i].label));
      grid.appendChild(stat);
    }
    card.appendChild(grid);
    bucketInfoEl.appendChild(card);
  }

  // --- Render watcher status badge ---
  function renderWatcherStatus() {
    watcherStatusEl.textContent = "";
    if (!watcherId) return;

    if (watcherActive) {
      var badge = makeEl("span", "kv-watcher-badge kv-watcher-badge-live", "WATCHING");
      watcherStatusEl.appendChild(badge);
    } else {
      watcherStatusEl.appendChild(makeEl("span", "kv-watcher-badge kv-watcher-badge-loading", "LOADING"));
    }
  }

  // --- Render the key-value table ---
  function renderTable() {
    tableBody.textContent = "";

    if (entries.size === 0) {
      var emptyRow = document.createElement("tr");
      var emptyCell = document.createElement("td");
      emptyCell.colSpan = 4;
      emptyCell.className = "empty-state";
      emptyCell.textContent = "Click \"Setup: User Profiles\" to get started.";
      emptyRow.appendChild(emptyCell);
      tableBody.appendChild(emptyRow);
      return;
    }

    entries.forEach(function (entry) {
      var tr = document.createElement("tr");
      var isDel = entry.operation === "DEL" || entry.operation === "PURGE";

      if (flashingKeys.has(entry.key)) tr.classList.add("kv-row-flash");
      if (isDel) tr.classList.add("kv-row-deleted");

      // Key cell (clickable -- loads into get-key input)
      var keyCell = document.createElement("td");
      keyCell.className = "kv-cell-key";
      keyCell.textContent = entry.key;
      keyCell.title = "Click to select this key";
      keyCell.addEventListener("click", (function (k) {
        return function () {
          document.getElementById("get-key").value = k;
          document.getElementById("put-key").value = k;
          document.getElementById("cas-key").value = k;
          document.getElementById("cas-revision").value = String(entry.revision || "");
        };
      })(entry.key));
      tr.appendChild(keyCell);

      // Value cell
      var valCell = document.createElement("td");
      valCell.className = "kv-cell-value";
      valCell.textContent = isDel ? "(deleted)" : truncate(entry.value, 60);
      valCell.title = entry.value || "";
      tr.appendChild(valCell);

      // Revision cell
      var revCell = document.createElement("td");
      revCell.className = "kv-cell-rev";
      revCell.textContent = entry.revision ? "#" + entry.revision : "--";
      tr.appendChild(revCell);

      // Status cell
      var statusCell = document.createElement("td");
      if (isDel) {
        statusCell.appendChild(makeEl("span", "kv-entry-op kv-entry-op-del", "DEL"));
      } else {
        statusCell.appendChild(makeEl("span", "kv-entry-op kv-entry-op-put", "PUT"));
      }
      tr.appendChild(statusCell);

      tableBody.appendChild(tr);
    });
  }

  // --- Render history ---
  function renderHistory() {
    kvHistoryEl.textContent = "";

    if (historyEntries.length === 0) {
      kvHistoryEl.style.display = "none";
      return;
    }

    kvHistoryEl.style.display = "";

    var hdr = makeEl("h3", "panel-heading", "History: " + historyKey);
    kvHistoryEl.appendChild(hdr);

    var list = makeEl("div", "kv-history-list");
    for (var i = historyEntries.length - 1; i >= 0; i--) {
      var e = historyEntries[i];
      var row = makeEl("div", "kv-history-entry");
      row.appendChild(makeEl("span", "kv-history-rev", "#" + e.revision));
      var opCls = (e.operation === "PUT") ? "kv-history-op kv-entry-op-put" : "kv-history-op kv-entry-op-del";
      row.appendChild(makeEl("span", opCls, e.operation));
      row.appendChild(makeEl("span", "kv-history-val", e.value || "(deleted)"));
      row.appendChild(makeEl("span", "kv-history-time", formatTime(e.created)));
      list.appendChild(row);
    }
    kvHistoryEl.appendChild(list);
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
  document.getElementById("btn-quick-setup").addEventListener("click", function () {
    var bucket = "PROFILES";
    currentBucketName = bucket;

    // 1. Create bucket
    send({ action: "create_bucket", bucket: bucket, history: 5, ttl: 0 });

    // 2. Start a watcher so the table is live
    setTimeout(function () {
      send({ action: "watch", bucket: bucket, watchKey: "" });
    }, 200);

    // 3. Seed some profiles (watcher will pick these up automatically)
    var profiles = [
      { key: "user.alice", value: '{"name":"Alice","role":"admin","email":"alice@example.com"}' },
      { key: "user.bob",   value: '{"name":"Bob","role":"developer","email":"bob@example.com"}' },
      { key: "user.carol", value: '{"name":"Carol","role":"manager","email":"carol@example.com"}' }
    ];
    profiles.forEach(function (p, i) {
      setTimeout(function () {
        send({ action: "put", bucket: bucket, key: p.key, value: p.value });
      }, 500 + i * 200);
    });
  });

  document.getElementById("btn-put").addEventListener("click", function () {
    if (!currentBucketName) { addLog("error", "Run Setup first"); return; }
    var key = document.getElementById("put-key").value.trim();
    var value = document.getElementById("put-value").value.trim();
    if (!key) return;
    send({ action: "put", bucket: currentBucketName, key: key, value: value });
  });

  document.getElementById("btn-get").addEventListener("click", function () {
    if (!currentBucketName) { addLog("error", "Run Setup first"); return; }
    var key = document.getElementById("get-key").value.trim();
    if (!key) return;
    send({ action: "get", bucket: currentBucketName, key: key });
  });

  document.getElementById("btn-delete-key").addEventListener("click", function () {
    if (!currentBucketName) { addLog("error", "Run Setup first"); return; }
    var key = document.getElementById("get-key").value.trim();
    if (!key) return;
    send({ action: "delete", bucket: currentBucketName, key: key });
  });

  document.getElementById("btn-history").addEventListener("click", function () {
    if (!currentBucketName) { addLog("error", "Run Setup first"); return; }
    var key = document.getElementById("get-key").value.trim();
    if (!key) return;
    send({ action: "history", bucket: currentBucketName, key: key });
  });

  document.getElementById("btn-cas").addEventListener("click", function () {
    if (!currentBucketName) { addLog("error", "Run Setup first"); return; }
    var key = document.getElementById("cas-key").value.trim();
    var value = document.getElementById("cas-value").value.trim();
    var rev = parseInt(document.getElementById("cas-revision").value.trim(), 10);
    if (!key || !rev) { addLog("error", "Key and revision # are required for CAS"); return; }
    send({ action: "cas_update", bucket: currentBucketName, key: key, value: value, revision: rev });
  });

  document.getElementById("btn-clear-log").addEventListener("click", function () {
    logEl.textContent = "";
  });

  // Enter key on inputs
  document.getElementById("put-key").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-put").click();
  });
  document.getElementById("put-value").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-put").click();
  });
  document.getElementById("get-key").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-get").click();
  });
  document.getElementById("cas-revision").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-cas").click();
  });

  // Initial render, then connect
  renderTable();
  renderWatcherStatus();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
