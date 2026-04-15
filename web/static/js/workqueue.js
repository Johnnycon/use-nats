(function () {
  "use strict";

  var statusEl = document.getElementById("nats-status");
  var logEl = document.getElementById("event-log");
  var streamInfoEl = document.getElementById("stream-info-card");
  var workerCardsEl = document.getElementById("worker-cards");
  var distributionEl = document.getElementById("task-distribution");

  var ws = null;
  var closing = false;
  var flashingIds = new Set();
  var workers = new Map();       // workerID -> {id, name, completed, failed, processing}
  var pendingTasks = new Map();  // msgId -> {msgId, sequence, subject, data, workerId, timestamp}
  var currentStream = null;
  var totalCompleted = 0;
  var totalFailed = 0;

  var workerNames = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta"];
  var workerNameIdx = 0;

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

  // --- WebSocket ---
  function connect() {
    var url = "ws://" + location.host + "/demo/workqueue/ws";
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
        case "stream_ready":
          onStreamReady(evt.payload);
          break;
        case "consumer_ready":
          onConsumerReady(evt.payload);
          break;
        case "worker_added":
          onWorkerAdded(evt.payload);
          break;
        case "worker_removed":
          onWorkerRemoved(evt.payload);
          break;
        case "task_published":
          onTaskPublished(evt.payload);
          break;
        case "batch_published":
          onBatchPublished(evt.payload);
          break;
        case "tasks_fetched":
          onTasksFetched(evt.payload);
          break;
        case "fetch_complete":
          onFetchComplete(evt.payload);
          break;
        case "task_acked":
          onTaskAcked(evt.payload);
          break;
        case "task_naked":
          onTaskNaked(evt.payload);
          break;
        case "stream_info":
          onStreamInfo(evt.payload);
          break;
        case "error":
          onError(evt.payload);
          break;
      }
    };
  }

  // --- Event handlers ---
  function onStreamReady(p) {
    currentStream = p;
    renderStreamInfo();
    addLog("subscribe", "Work queue stream \"" + p.name + "\" ready -- retention: " + p.retention + ", subjects: [" + p.subjects.join(", ") + "]");
  }

  function onConsumerReady(p) {
    addLog("subscribe", "Shared consumer \"" + p.name + "\" ready -- all workers will pull from this consumer");
  }

  function onWorkerAdded(p) {
    workers.set(p.id, {
      id: p.id,
      name: p.name,
      completed: 0,
      failed: 0,
      processing: 0
    });
    flashingIds.add("worker-" + p.id);
    setTimeout(function () { flashingIds.delete("worker-" + p.id); renderWorkerCards(); }, 400);
    renderWorkerCards();
    renderDistribution();
    addLog("subscribe", "Worker \"" + p.name + "\" (" + p.id + ") added");
  }

  function onWorkerRemoved(p) {
    var w = workers.get(p.workerId);
    var name = w ? w.name : p.workerId;
    workers.delete(p.workerId);
    // Remove pending tasks for this worker
    pendingTasks.forEach(function (task, msgId) {
      if (task.workerId === p.workerId) pendingTasks.delete(msgId);
    });
    renderWorkerCards();
    renderDistribution();
    addLog("unsubscribe", "Worker \"" + name + "\" removed");
  }

  function onTaskPublished(p) {
    flashingIds.add("stream-card");
    setTimeout(function () { flashingIds.delete("stream-card"); renderStreamInfo(); }, 300);
    if (currentStream) currentStream.messages = (currentStream.messages || 0) + 1;
    renderStreamInfo();
    addLog("publish", "Task " + p.taskId + " published to \"" + p.subject + "\" -> seq #" + p.sequence);
  }

  function onBatchPublished(p) {
    flashingIds.add("stream-card");
    setTimeout(function () { flashingIds.delete("stream-card"); renderStreamInfo(); }, 300);
    if (currentStream) currentStream.messages = (currentStream.messages || 0) + p.count;
    renderStreamInfo();
    addLog("publish", "Published " + p.count + " tasks (seq #" + p.firstSeq + " - #" + p.lastSeq + ")");
  }

  function onTasksFetched(p) {
    var msgs = p.messages;
    for (var i = 0; i < msgs.length; i++) {
      pendingTasks.set(msgs[i].msgId, {
        msgId: msgs[i].msgId,
        sequence: msgs[i].sequence,
        subject: msgs[i].subject,
        data: msgs[i].data,
        workerId: msgs[i].workerId,
        timestamp: msgs[i].timestamp
      });
      flashingIds.add(msgs[i].msgId);
    }
    // Update local worker processing count
    var w = workers.get(p.workerId);
    if (w) w.processing += p.count;

    setTimeout(function () {
      for (var j = 0; j < msgs.length; j++) flashingIds.delete(msgs[j].msgId);
      renderWorkerCards();
    }, 400);
    renderWorkerCards();
    addLog("receive", "Worker \"" + p.workerName + "\" fetched " + p.count + " task(s)");
  }

  function onFetchComplete(p) {
    addLog("system", "Worker \"" + p.workerName + "\": " + p.message);
  }

  function onTaskAcked(p) {
    pendingTasks.delete(p.msgId);
    var w = workers.get(p.workerId);
    if (w) {
      w.processing--;
      w.completed++;
    }
    totalCompleted++;
    renderWorkerCards();
    renderDistribution();
    addLog("publish", "Acked seq #" + p.sequence + " by worker \"" + p.workerName + "\"");
  }

  function onTaskNaked(p) {
    pendingTasks.delete(p.msgId);
    var w = workers.get(p.workerId);
    if (w) {
      w.processing--;
      w.failed++;
    }
    totalFailed++;
    renderWorkerCards();
    renderDistribution();
    addLog("error", "Naked seq #" + p.sequence + " by worker \"" + p.workerName + "\" -- will be redelivered");
  }

  function onStreamInfo(p) {
    currentStream = p;
    renderStreamInfo();
    addLog("system", "Stream: " + p.messages + " msgs, " + p.bytes + " bytes, seq " + p.firstSeq + "-" + p.lastSeq + ", " + p.consumers + " consumer(s)");
  }

  function onError(p) {
    addLog("error", p.message);
  }

  // --- Render stream info ---
  function renderStreamInfo() {
    streamInfoEl.textContent = "";

    if (!currentStream) {
      streamInfoEl.appendChild(makeEl("p", "empty-state", "Run \"Setup: Work Queue\" to create the stream."));
      return;
    }

    var card = makeEl("div", "si-card");
    if (flashingIds.has("stream-card")) card.classList.add("pulse");

    var nameRow = makeEl("div", "si-row si-name-row");
    nameRow.appendChild(makeEl("span", "si-name", currentStream.name));
    nameRow.appendChild(makeEl("span", "si-value", currentStream.retention || "WorkQueue"));
    card.appendChild(nameRow);

    var grid = makeEl("div", "si-stats-grid");
    var stats = [
      { val: String(currentStream.messages || 0), label: "Messages" },
      { val: String(currentStream.bytes || 0), label: "Bytes" },
      { val: currentStream.firstSeq ? String(currentStream.firstSeq) : "--", label: "First Seq" },
      { val: currentStream.lastSeq ? String(currentStream.lastSeq) : "--", label: "Last Seq" }
    ];
    for (var i = 0; i < stats.length; i++) {
      var stat = makeEl("div", "si-stat");
      stat.appendChild(makeEl("div", "si-stat-value", stats[i].val));
      stat.appendChild(makeEl("div", "si-stat-label", stats[i].label));
      grid.appendChild(stat);
    }
    card.appendChild(grid);

    if (currentStream.subjects) {
      var subRow = makeEl("div", "si-row");
      subRow.appendChild(makeEl("span", "si-label", "Subjects"));
      subRow.appendChild(makeEl("span", "si-value", currentStream.subjects.join(", ")));
      card.appendChild(subRow);
    }

    streamInfoEl.appendChild(card);
  }

  // --- Render worker cards ---
  function renderWorkerCards() {
    workerCardsEl.textContent = "";

    if (workers.size === 0) {
      workerCardsEl.appendChild(makeEl("p", "empty-state", "No workers yet. Add workers to process tasks."));
      return;
    }

    workers.forEach(function (w) {
      var card = makeEl("div", "wq-worker-card");
      if (flashingIds.has("worker-" + w.id)) card.classList.add("pulse");

      // Header
      var header = makeEl("div", "wq-worker-header");
      header.appendChild(makeEl("span", "wq-worker-name", w.name));
      var busy = w.processing > 0;
      var badge = makeEl("span", busy ? "wq-badge wq-badge-busy" : "wq-badge wq-badge-idle", busy ? "BUSY" : "IDLE");
      header.appendChild(badge);
      card.appendChild(header);

      // Stats grid
      var grid = makeEl("div", "wq-stats-grid");
      var stats = [
        { val: String(w.completed), label: "Completed", cls: "wq-stat-green" },
        { val: String(w.failed), label: "Failed", cls: "wq-stat-red" },
        { val: String(w.processing), label: "Processing", cls: "wq-stat-cyan" }
      ];
      for (var i = 0; i < stats.length; i++) {
        var stat = makeEl("div", "wq-stat");
        var valEl = makeEl("div", "wq-stat-value", stats[i].val);
        valEl.classList.add(stats[i].cls);
        stat.appendChild(valEl);
        stat.appendChild(makeEl("div", "wq-stat-label", stats[i].label));
        grid.appendChild(stat);
      }
      card.appendChild(grid);

      // Pending tasks for this worker
      var workerTasks = [];
      pendingTasks.forEach(function (task) {
        if (task.workerId === w.id) workerTasks.push(task);
      });

      if (workerTasks.length > 0) {
        var taskList = makeEl("div", "wq-task-list");
        for (var j = 0; j < workerTasks.length; j++) {
          var task = workerTasks[j];
          var row = makeEl("div", "wq-task-row");
          if (flashingIds.has(task.msgId)) row.classList.add("flash");

          var info = makeEl("div", "wq-task-info");
          info.appendChild(makeEl("span", "pm-seq", "#" + task.sequence));
          info.appendChild(makeEl("span", "pm-subject", task.subject));
          row.appendChild(info);

          row.appendChild(makeEl("div", "wq-task-data", task.data));

          var btns = makeEl("div", "wq-task-btns");
          var ackBtn = makeEl("button", "btn btn-small btn-accent", "Ack");
          ackBtn.addEventListener("click", (function (id) {
            return function () { send({ action: "ack", msgId: id }); };
          })(task.msgId));
          btns.appendChild(ackBtn);

          var nakBtn = makeEl("button", "btn btn-small btn-danger", "Nak");
          nakBtn.addEventListener("click", (function (id) {
            return function () { send({ action: "nak", msgId: id }); };
          })(task.msgId));
          btns.appendChild(nakBtn);

          row.appendChild(btns);
          taskList.appendChild(row);
        }
        card.appendChild(taskList);
      }

      // Action buttons
      var actions = makeEl("div", "wq-actions");

      var fetch1 = makeEl("button", "btn btn-small btn-primary", "Fetch 1");
      fetch1.addEventListener("click", (function (id) {
        return function () { send({ action: "fetch_tasks", workerId: id, batchSize: 1 }); };
      })(w.id));
      actions.appendChild(fetch1);

      var fetch5 = makeEl("button", "btn btn-small btn-primary", "Fetch 5");
      fetch5.addEventListener("click", (function (id) {
        return function () { send({ action: "fetch_tasks", workerId: id, batchSize: 5 }); };
      })(w.id));
      actions.appendChild(fetch5);

      var removeBtn = makeEl("button", "btn btn-small btn-danger", "Remove");
      removeBtn.addEventListener("click", (function (id) {
        return function () { send({ action: "remove_worker", workerId: id }); };
      })(w.id));
      actions.appendChild(removeBtn);

      card.appendChild(actions);
      workerCardsEl.appendChild(card);
    });
  }

  // --- Render distribution ---
  function renderDistribution() {
    distributionEl.textContent = "";

    var total = totalCompleted + totalFailed;
    if (total === 0 || workers.size === 0) {
      distributionEl.appendChild(makeEl("p", "empty-state", "Process tasks to see distribution across workers."));
      return;
    }

    // Summary
    var summary = makeEl("div", "wq-dist-summary");
    summary.appendChild(makeEl("span", "wq-dist-total", "Completed: " + totalCompleted));
    summary.appendChild(makeEl("span", "wq-dist-total wq-dist-failed", "Failed: " + totalFailed));
    distributionEl.appendChild(summary);

    // Bars
    var maxCompleted = 0;
    workers.forEach(function (w) {
      if (w.completed > maxCompleted) maxCompleted = w.completed;
    });
    if (maxCompleted === 0) maxCompleted = 1;

    var colors = ["var(--accent)", "var(--green)", "var(--cyan)", "var(--orange)"];
    var idx = 0;
    workers.forEach(function (w) {
      var row = makeEl("div", "wq-dist-row");

      row.appendChild(makeEl("span", "wq-dist-label", w.name));

      var barWrap = makeEl("div", "wq-dist-bar-wrap");
      var bar = makeEl("div", "wq-dist-bar");
      var pct = Math.round((w.completed / maxCompleted) * 100);
      bar.style.width = pct + "%";
      bar.style.background = colors[idx % colors.length];
      barWrap.appendChild(bar);
      row.appendChild(barWrap);

      row.appendChild(makeEl("span", "wq-dist-count", String(w.completed)));
      distributionEl.appendChild(row);
      idx++;
    });
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
  document.getElementById("btn-setup-queue").addEventListener("click", function () {
    send({ action: "setup" });
  });

  document.getElementById("btn-add-worker").addEventListener("click", function () {
    var nameInput = document.getElementById("worker-name");
    var name = nameInput.value.trim();
    if (!name) return;
    send({ action: "add_worker", workerName: name });
    // Advance to next default name
    workerNameIdx++;
    if (workerNameIdx < workerNames.length) {
      nameInput.value = workerNames[workerNameIdx];
    } else {
      nameInput.value = "Worker-" + (workerNameIdx + 1);
    }
  });

  document.getElementById("btn-publish-task").addEventListener("click", function () {
    var taskType = document.getElementById("task-type").value;
    var taskData = document.getElementById("task-data").value.trim();
    send({ action: "publish_task", taskType: taskType, taskData: taskData });
  });

  document.getElementById("btn-publish-batch").addEventListener("click", function () {
    send({ action: "publish_batch", count: 10 });
  });

  document.getElementById("btn-fetch-all").addEventListener("click", function () {
    var delay = 0;
    workers.forEach(function (w) {
      setTimeout(function () {
        send({ action: "fetch_tasks", workerId: w.id, batchSize: 1 });
      }, delay);
      delay += 100;
    });
  });

  document.getElementById("btn-stream-info").addEventListener("click", function () {
    send({ action: "stream_info" });
  });

  document.getElementById("btn-clear-log").addEventListener("click", function () {
    logEl.textContent = "";
  });

  // Enter key on inputs
  document.getElementById("worker-name").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-add-worker").click();
  });
  document.getElementById("task-data").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish-task").click();
  });

  // Initial render, then connect after page load
  renderStreamInfo();
  renderWorkerCards();
  renderDistribution();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
