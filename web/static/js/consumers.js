(function () {
  "use strict";

  var statusEl = document.getElementById("nats-status");
  var logEl = document.getElementById("event-log");
  var consumerCardsEl = document.getElementById("consumer-cards");
  var pendingMsgsEl = document.getElementById("pending-messages");

  var ws = null;
  var closing = false;
  var publishFlash = false;
  var flashingIds = new Set();
  var consumers = new Map(); // name -> {name, durable, deliverPolicy, ackWait, filterSubject, numPending, numAckPending, delivered, ackFloor}
  var pendingMessages = []; // [{msgId, sequence, subject, data, consumer, timestamp}]

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

  function formatTime(isoStr) {
    try {
      var d = new Date(isoStr);
      return d.toLocaleTimeString("en-US", { hour12: false });
    } catch (e) {
      return isoStr;
    }
  }

  // --- WebSocket with auto-reconnect ---
  function connect() {
    var url = "ws://" + location.host + "/demo/consumers/ws";
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
        case "consumer_created":
          onConsumerCreated(evt.payload);
          break;
        case "consumer_deleted":
          onConsumerDeleted(evt.payload);
          break;
        case "published":
          onPublished(evt.payload);
          break;
        case "messages_pulled":
          onMessagesPulled(evt.payload);
          break;
        case "pull_complete":
          onPullComplete(evt.payload);
          break;
        case "message_acked":
          onMessageAcked(evt.payload);
          break;
        case "message_naked":
          onMessageNaked(evt.payload);
          break;
        case "consumer_info":
          onConsumerInfoReceived(evt.payload);
          break;
        case "error":
          onError(evt.payload);
          break;
      }
    };
  }

  // --- Event handlers ---
  function onStreamReady(p) {
    addLog("subscribe", "Stream \"" + p.name + "\" ready -- subjects: [" + p.subjects.join(", ") + "], " + p.messages + " existing messages");
  }

  function onConsumerCreated(p) {
    consumers.set(p.name, {
      name: p.name,
      durable: p.durable,
      deliverPolicy: p.deliverPolicy,
      ackWait: p.ackWait,
      filterSubject: p.filterSubject,
      numPending: p.numPending,
      numAckPending: p.numAckPending,
      delivered: p.delivered,
      ackFloor: p.ackFloor
    });
    flashingIds.add("consumer-" + p.name);
    setTimeout(function () { flashingIds.delete("consumer-" + p.name); }, 400);
    renderConsumerCards();
    addLog("subscribe", "Consumer \"" + p.name + "\" created -- " + (p.durable ? "durable" : "ephemeral") + ", policy: " + p.deliverPolicy + ", ack wait: " + p.ackWait + "s");
  }

  function onConsumerDeleted(p) {
    consumers.delete(p.name);
    // Remove pending messages for this consumer
    pendingMessages = pendingMessages.filter(function (m) { return m.consumer !== p.name; });
    renderConsumerCards();
    renderPendingMessages();
    addLog("unsubscribe", "Consumer \"" + p.name + "\" deleted");
  }

  function onPublished(p) {
    publishFlash = true;
    setTimeout(function () { publishFlash = false; }, 300);
    addLog("publish", "Published to \"" + p.subject + "\" -> seq #" + p.sequence + " on " + p.stream);
  }

  function onMessagesPulled(p) {
    var msgs = p.messages;
    for (var i = 0; i < msgs.length; i++) {
      pendingMessages.push({
        msgId: msgs[i].msgId,
        sequence: msgs[i].sequence,
        subject: msgs[i].subject,
        data: msgs[i].data,
        consumer: msgs[i].consumer,
        timestamp: msgs[i].timestamp
      });
      flashingIds.add(msgs[i].msgId);
    }
    setTimeout(function () {
      for (var j = 0; j < msgs.length; j++) {
        flashingIds.delete(msgs[j].msgId);
      }
      renderPendingMessages();
    }, 400);
    renderPendingMessages();
    addLog("receive", "Pulled " + p.count + " message(s) from consumer \"" + p.consumer + "\"");
  }

  function onPullComplete(p) {
    addLog("system", "Pull from \"" + p.consumer + "\": " + p.message);
  }

  function onMessageAcked(p) {
    pendingMessages = pendingMessages.filter(function (m) { return m.msgId !== p.msgId; });
    renderPendingMessages();
    // Refresh consumer info
    send({ action: "consumer_info", consumerName: p.consumer });
    addLog("publish", "Acked seq #" + p.sequence + " on consumer \"" + p.consumer + "\"");
  }

  function onMessageNaked(p) {
    pendingMessages = pendingMessages.filter(function (m) { return m.msgId !== p.msgId; });
    renderPendingMessages();
    // Refresh consumer info
    send({ action: "consumer_info", consumerName: p.consumer });
    addLog("error", "Naked seq #" + p.sequence + " on consumer \"" + p.consumer + "\" -- will be redelivered");
  }

  function onConsumerInfoReceived(p) {
    var existing = consumers.get(p.name);
    if (existing) {
      existing.numPending = p.numPending;
      existing.numAckPending = p.numAckPending;
      existing.delivered = p.delivered;
      existing.ackFloor = p.ackFloor;
      existing.durable = p.durable;
      existing.deliverPolicy = p.deliverPolicy;
      existing.ackWait = p.ackWait;
      existing.filterSubject = p.filterSubject;
    } else {
      consumers.set(p.name, {
        name: p.name,
        durable: p.durable,
        deliverPolicy: p.deliverPolicy,
        ackWait: p.ackWait,
        filterSubject: p.filterSubject,
        numPending: p.numPending,
        numAckPending: p.numAckPending,
        delivered: p.delivered,
        ackFloor: p.ackFloor
      });
    }
    renderConsumerCards();
  }

  function onError(p) {
    addLog("error", p.message);
  }

  // --- Render consumer cards ---
  function renderConsumerCards() {
    consumerCardsEl.textContent = "";

    if (consumers.size === 0) {
      consumerCardsEl.appendChild(makeEl("p", "empty-state", "No consumers yet. Create one to see details."));
      return;
    }

    consumers.forEach(function (c) {
      var card = makeEl("div", "ci-card");
      if (flashingIds.has("consumer-" + c.name)) card.classList.add("pulse");

      // Header with name and type badge
      var header = makeEl("div", "ci-header");
      header.appendChild(makeEl("span", "ci-name", c.name));
      var badge = makeEl("span", c.durable ? "ci-badge ci-badge-durable" : "ci-badge ci-badge-ephemeral", c.durable ? "DURABLE" : "EPHEMERAL");
      header.appendChild(badge);
      card.appendChild(header);

      // Stats grid
      var grid = makeEl("div", "ci-stats-grid");

      var pendStat = makeEl("div", "ci-stat");
      pendStat.appendChild(makeEl("div", "ci-stat-value", String(c.numPending)));
      pendStat.appendChild(makeEl("div", "ci-stat-label", "Pending"));
      grid.appendChild(pendStat);

      var ackPendStat = makeEl("div", "ci-stat");
      ackPendStat.appendChild(makeEl("div", "ci-stat-value", String(c.numAckPending)));
      ackPendStat.appendChild(makeEl("div", "ci-stat-label", "Ack Pending"));
      grid.appendChild(ackPendStat);

      var delivStat = makeEl("div", "ci-stat");
      delivStat.appendChild(makeEl("div", "ci-stat-value", c.delivered ? String(c.delivered) : "--"));
      delivStat.appendChild(makeEl("div", "ci-stat-label", "Last Delivered"));
      grid.appendChild(delivStat);

      var ackFloorStat = makeEl("div", "ci-stat");
      ackFloorStat.appendChild(makeEl("div", "ci-stat-value", c.ackFloor ? String(c.ackFloor) : "--"));
      ackFloorStat.appendChild(makeEl("div", "ci-stat-label", "Ack Floor"));
      grid.appendChild(ackFloorStat);

      card.appendChild(grid);

      // Details rows
      var policyRow = makeEl("div", "ci-row");
      policyRow.appendChild(makeEl("span", "ci-label", "Policy"));
      policyRow.appendChild(makeEl("span", "ci-value", c.deliverPolicy));
      card.appendChild(policyRow);

      var ackRow = makeEl("div", "ci-row");
      ackRow.appendChild(makeEl("span", "ci-label", "Ack Wait"));
      ackRow.appendChild(makeEl("span", "ci-value", c.ackWait + "s"));
      card.appendChild(ackRow);

      var filterRow = makeEl("div", "ci-row");
      filterRow.appendChild(makeEl("span", "ci-label", "Filter"));
      filterRow.appendChild(makeEl("span", "ci-value", c.filterSubject));
      card.appendChild(filterRow);

      // Action buttons row
      var actions = makeEl("div", "ci-actions");

      var pullBtn = makeEl("button", "btn btn-small btn-primary", "Pull");
      pullBtn.addEventListener("click", (function (name) {
        return function () {
          send({ action: "pull", consumerName: name, batchSize: 5 });
        };
      })(c.name));
      actions.appendChild(pullBtn);

      var infoBtn = makeEl("button", "btn btn-small btn-outline", "Refresh");
      infoBtn.addEventListener("click", (function (name) {
        return function () {
          send({ action: "consumer_info", consumerName: name });
        };
      })(c.name));
      actions.appendChild(infoBtn);

      var delBtn = makeEl("button", "btn btn-small btn-danger", "Delete");
      delBtn.addEventListener("click", (function (name) {
        return function () {
          send({ action: "delete_consumer", consumerName: name });
        };
      })(c.name));
      actions.appendChild(delBtn);

      card.appendChild(actions);
      consumerCardsEl.appendChild(card);
    });
  }

  // --- Render pending messages ---
  function renderPendingMessages() {
    pendingMsgsEl.textContent = "";

    if (pendingMessages.length === 0) {
      pendingMsgsEl.appendChild(makeEl("p", "empty-state", "Pull messages to see them here. Use Ack/Nak to process."));
      return;
    }

    for (var i = 0; i < pendingMessages.length; i++) {
      var msg = pendingMessages[i];
      var row = makeEl("div", "pm-row");
      if (flashingIds.has(msg.msgId)) row.classList.add("flash");

      // Top line: seq badge, subject, consumer
      var topLine = makeEl("div", "pm-top");
      topLine.appendChild(makeEl("span", "pm-seq", "#" + msg.sequence));
      topLine.appendChild(makeEl("span", "pm-subject", msg.subject));
      topLine.appendChild(makeEl("span", "pm-consumer", msg.consumer));

      row.appendChild(topLine);

      // Data
      row.appendChild(makeEl("div", "pm-data", msg.data));

      // Buttons
      var btnRow = makeEl("div", "pm-buttons");

      var ackBtn = makeEl("button", "btn btn-small btn-accent", "Ack");
      ackBtn.addEventListener("click", (function (id) {
        return function () {
          send({ action: "ack", msgId: id });
        };
      })(msg.msgId));
      btnRow.appendChild(ackBtn);

      var nakBtn = makeEl("button", "btn btn-small btn-danger", "Nak");
      nakBtn.addEventListener("click", (function (id) {
        return function () {
          send({ action: "nak", msgId: id });
        };
      })(msg.msgId));
      btnRow.appendChild(nakBtn);

      row.appendChild(btnRow);
      pendingMsgsEl.appendChild(row);
    }

    pendingMsgsEl.scrollTop = pendingMsgsEl.scrollHeight;
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

  // --- Show/hide start sequence input based on deliver policy ---
  document.getElementById("consumer-policy").addEventListener("change", function () {
    var seqInput = document.getElementById("consumer-start-seq");
    if (this.value === "by_start_sequence") {
      seqInput.style.display = "";
    } else {
      seqInput.style.display = "none";
    }
  });

  // --- Button handlers ---
  document.getElementById("btn-setup-pipeline").addEventListener("click", function () {
    // Step 1: Create stream
    send({ action: "setup" });

    // Step 2: Create durable consumer (with delay to ensure stream is ready)
    setTimeout(function () {
      send({
        action: "create_consumer",
        consumerName: "PaymentProcessor",
        durable: true,
        deliverPolicy: "all",
        ackWait: 30,
        filterSubject: "demo.consumers.>"
      });
    }, 300);

    // Step 3: Publish 5 order events
    var events = [
      { subject: "demo.consumers.orders.created", message: '{"orderId":"ORD-3001","customer":"Alice","item":"Wireless Headphones","amount":79.99}' },
      { subject: "demo.consumers.orders.created", message: '{"orderId":"ORD-3002","customer":"Bob","item":"Mechanical Keyboard","amount":149.00}' },
      { subject: "demo.consumers.orders.paid",    message: '{"orderId":"ORD-3001","method":"credit_card","amount":79.99}' },
      { subject: "demo.consumers.orders.created", message: '{"orderId":"ORD-3003","customer":"Carol","item":"USB-C Hub","amount":49.99}' },
      { subject: "demo.consumers.orders.shipped",  message: '{"orderId":"ORD-3001","carrier":"FedEx","tracking":"FX-98765"}' }
    ];
    events.forEach(function (evt, i) {
      setTimeout(function () {
        send({ action: "publish", subject: evt.subject, message: evt.message });
      }, 600 + i * 150);
    });
  });

  document.getElementById("btn-publish-batch").addEventListener("click", function () {
    var events = [
      { subject: "demo.consumers.orders.created", message: '{"orderId":"ORD-4001","customer":"Dave","item":"Monitor Stand","amount":34.99}' },
      { subject: "demo.consumers.orders.created", message: '{"orderId":"ORD-4002","customer":"Eve","item":"Webcam","amount":89.99}' },
      { subject: "demo.consumers.orders.paid",    message: '{"orderId":"ORD-4001","method":"paypal","amount":34.99}' },
      { subject: "demo.consumers.orders.created", message: '{"orderId":"ORD-4003","customer":"Frank","item":"Desk Lamp","amount":24.99}' },
      { subject: "demo.consumers.orders.shipped",  message: '{"orderId":"ORD-4001","carrier":"UPS","tracking":"UPS-11223"}' }
    ];
    events.forEach(function (evt, i) {
      setTimeout(function () {
        send({ action: "publish", subject: evt.subject, message: evt.message });
      }, i * 150);
    });
  });

  document.getElementById("btn-create-consumer").addEventListener("click", function () {
    var name = document.getElementById("consumer-name").value.trim();
    var durable = document.getElementById("consumer-durable").checked;
    var policy = document.getElementById("consumer-policy").value;
    var filter = document.getElementById("consumer-filter").value.trim();
    var ackWait = parseInt(document.getElementById("consumer-ack-wait").value.trim(), 10);
    var startSeq = parseInt(document.getElementById("consumer-start-seq").value.trim(), 10);
    if (!name) return;

    // Ensure stream exists first
    send({ action: "setup" });
    setTimeout(function () {
      var cmd = {
        action: "create_consumer",
        consumerName: name,
        durable: durable,
        deliverPolicy: policy,
        filterSubject: filter || "demo.consumers.>",
        ackWait: ackWait || 30
      };
      if (policy === "by_start_sequence" && startSeq > 0) {
        cmd.startSeq = startSeq;
      }
      send(cmd);
    }, 200);
  });

  document.getElementById("btn-publish").addEventListener("click", function () {
    var subject = document.getElementById("pub-subject").value.trim();
    var message = document.getElementById("pub-message").value.trim();
    if (subject) send({ action: "publish", subject: subject, message: message });
  });

  document.getElementById("btn-pull").addEventListener("click", function () {
    var consumer = document.getElementById("pull-consumer").value.trim();
    var batch = parseInt(document.getElementById("pull-batch").value.trim(), 10);
    if (consumer) send({ action: "pull", consumerName: consumer, batchSize: batch || 5 });
  });

  document.getElementById("btn-clear-log").addEventListener("click", function () {
    logEl.textContent = "";
  });

  // Allow Enter key in inputs
  document.getElementById("consumer-name").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-create-consumer").click();
  });
  document.getElementById("consumer-ack-wait").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-create-consumer").click();
  });
  document.getElementById("pub-subject").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish").click();
  });
  document.getElementById("pub-message").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-publish").click();
  });
  document.getElementById("pull-consumer").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-pull").click();
  });
  document.getElementById("pull-batch").addEventListener("keydown", function (e) {
    if (e.key === "Enter") document.getElementById("btn-pull").click();
  });

  // Initial render, then connect after page resources have loaded
  renderConsumerCards();
  renderPendingMessages();
  if (document.readyState === "complete") {
    connect();
  } else {
    window.addEventListener("load", connect);
  }
})();
