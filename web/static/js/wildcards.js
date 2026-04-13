(function () {
  "use strict";

  var statusEl = document.getElementById("nats-status");
  var logEl = document.getElementById("event-log");
  var subsEl = document.getElementById("subscribers");
  var flowEl = document.getElementById("flow-diagram");

  var ws = null;
  var closing = false;
  var pubCount = 0;
  var publishFlash = false;
  var flashingIds = new Set();
  var activeSubs = new Map(); // id -> {subject, name, recvCount}

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

  // --- WebSocket with auto-reconnect ---
  function connect() {
    var url = "ws://" + location.host + "/demo/wildcards/ws";
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
    addLog("subscribe", p.name + ' subscribed to "' + p.subject + '"');
  }

  function onUnsubscribed(p) {
    var name = activeSubs.get(p.subId)?.name || p.subId;
    activeSubs.delete(p.subId);
    renderSubscribers();
    renderFlow();
    addLog("unsubscribe", name + " unsubscribed");
  }

  function onPublished(p) {
    pubCount++;
    publishFlash = true;
    setTimeout(function () { publishFlash = false; }, 300);
    renderFlow();
    addLog("publish", 'Published to "' + p.subject + '": ' + p.data);
  }

  function onMessageReceived(p) {
    var sub = activeSubs.get(p.subId);
    if (sub) sub.recvCount++;
    var name = p.name || p.subId;
    flashingIds.add(p.subId);
    setTimeout(function () { flashingIds.delete(p.subId); }, 400);
    renderSubscribers();
    renderFlow();
    addLog("receive", name + ' received on "' + p.subject + '": ' + p.data);
  }

  // --- Group subscriptions by subscriber name ---
  function groupedSubs() {
    var groups = new Map(); // name -> [{id, subject, recvCount}]
    for (var entry of activeSubs) {
      var id = entry[0];
      var s = entry[1];
      var name = s.name || id;
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name).push({ id: id, subject: s.subject, recvCount: s.recvCount });
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
    var pubCol = makeEl("div", "flow-col flow-col-pub");
    var pubNode = makeEl("div", "flow-node flow-pub publisher-node");
    if (publishFlash) pubNode.classList.add("pulse");
    pubNode.appendChild(makeEl("div", "node-label", "Publisher"));
    pubNode.appendChild(makeEl("div", "node-counter", String(pubCount)));
    pubCol.appendChild(pubNode);

    // NATS column
    var natsCol = makeEl("div", "flow-col flow-col-nats");
    var natsNode = makeEl("div", "flow-node flow-nats nats-node");
    if (publishFlash) natsNode.classList.add("pulse");
    natsNode.appendChild(makeEl("div", "node-label", "NATS"));
    natsCol.appendChild(natsNode);

    // Subscribers column -- grouped by name
    var subCol = makeEl("div", "flow-col flow-col-subs");
    for (var entry of groupedSubs()) {
      var name = entry[0];
      var subs = entry[1];
      var totalRecv = subs.reduce(function (sum, s) { return sum + s.recvCount; }, 0);
      var node = makeEl("div", "flow-node flow-sub subscriber-node");
      node.id = "flow-group-" + name;
      if (subs.some(function (s) { return flashingIds.has(s.id); })) node.classList.add("pulse");
      node.appendChild(makeEl("div", "node-name", name));
      for (var i = 0; i < subs.length; i++) {
        node.appendChild(makeEl("div", "node-subject", subs[i].subject));
      }
      node.appendChild(makeEl("div", "node-counter", String(totalRecv)));
      subCol.appendChild(node);
    }

    // Arrows
    var arrow1 = makeEl("div", "flow-arrow-col");
    arrow1.appendChild(makeEl("div", "arrow-line-h"));
    var arrow2 = makeEl("div", "flow-arrow-col");
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
    for (var entry of groupedSubs()) {
      var name = entry[0];
      var subs = entry[1];
      var card = makeEl("div", "sub-card-group");
      var totalRecv = subs.reduce(function (sum, s) { return sum + s.recvCount; }, 0);

      var header = makeEl("div", "sub-group-header");
      header.appendChild(makeEl("span", "sub-id", name));
      var headerRight = makeEl("span", "sub-header-stats");
      headerRight.appendChild(makeEl("span", "sub-recv-total", String(totalRecv)));
      headerRight.appendChild(makeEl("span", "sub-count", subs.length + " sub" + (subs.length > 1 ? "s" : "")));
      header.appendChild(headerRight);
      card.appendChild(header);

      for (var i = 0; i < subs.length; i++) {
        (function (s) {
          var row = makeEl("div", "sub-row");
          row.id = "card-" + s.id;
          if (flashingIds.has(s.id)) row.classList.add("flash");
          row.appendChild(makeEl("span", "sub-subject", s.subject));
          var rowRight = makeEl("span", "sub-row-stats");
          rowRight.appendChild(makeEl("span", "sub-recv-count", String(s.recvCount)));
          var btn = makeEl("button", "btn btn-small btn-danger", "Unsub");
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
    var card = document.getElementById("card-" + subId);
    if (!card) return;
    card.classList.add("flash");
    setTimeout(function () { card.classList.remove("flash"); }, 400);
  }

  function pulseNode(selector) {
    var el = document.querySelector(selector);
    if (!el) return;
    el.classList.add("pulse");
    setTimeout(function () { el.classList.remove("pulse"); }, 300);
  }

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
  document.getElementById("btn-subscribe").addEventListener("click", function () {
    var subject = document.getElementById("sub-subject").value.trim();
    var name = document.getElementById("sub-name").value.trim();
    if (subject) send({ action: "subscribe", subject: subject, name: name });
  });

  document.getElementById("btn-publish").addEventListener("click", function () {
    var subject = document.getElementById("pub-subject").value.trim();
    var message = document.getElementById("pub-message").value.trim();
    if (subject) send({ action: "publish", subject: subject, message: message });
  });

  document.getElementById("btn-burst").addEventListener("click", function () {
    var subject = document.getElementById("pub-subject").value.trim();
    if (!subject) return;
    for (var i = 1; i <= 10; i++) {
      send({ action: "publish", subject: subject, message: "Burst message #" + i });
    }
  });

  document.getElementById("btn-setup-tracking").addEventListener("click", function () {
    var steps = [
      { action: "subscribe", subject: "orders.us.*", name: "USTeam" },
      { action: "subscribe", subject: "orders.*.shipped", name: "GlobalShipping" },
      { action: "subscribe", subject: "orders.>", name: "Analytics" },
      { action: "subscribe", subject: "orders.eu.paid", name: "EUPayments" },
    ];
    steps.forEach(function (step, i) {
      setTimeout(function () { send(step); }, i * 100);
    });
  });

  document.getElementById("btn-pub-us-created").addEventListener("click", function () {
    send({
      action: "publish",
      subject: "orders.us.created",
      message: '{"orderId":"ORD-2001","region":"us","status":"created","item":"Wireless Headphones","amount":79.99}',
    });
  });

  document.getElementById("btn-pub-eu-shipped").addEventListener("click", function () {
    send({
      action: "publish",
      subject: "orders.eu.shipped",
      message: '{"orderId":"ORD-2002","region":"eu","status":"shipped","item":"Mechanical Keyboard","amount":149.00}',
    });
  });

  document.getElementById("btn-pub-asia-paid").addEventListener("click", function () {
    send({
      action: "publish",
      subject: "orders.asia.paid",
      message: '{"orderId":"ORD-2003","region":"asia","status":"paid","item":"USB-C Hub","amount":34.99}',
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
