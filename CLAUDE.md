# NATS Learning Lab

## What This Is

A hands-on learning project for NATS and JetStream, written in Go. The app is a single binary that runs an embedded NATS server, an HTTP server on `:8092`, and a WebSocket hub for real-time UI updates. No Docker required for most demos.

The goal is 11 isolated demo pages, each showcasing a different NATS messaging pattern with a real interactive UI — controls, live event logs, and visual diagrams showing messages flowing in real-time.

## Running

```bash
make run          # builds then runs (recommended)
# or
make build        # just compile → ./nats-lab
./nats-lab        # instant startup, no recompile
# Open http://localhost:8092
```

Note: `go run ./cmd/server/` works but is slow (~30s) because it recompiles the large nats-server dependency every time. Prefer `make run`.

## Architecture

- **Embedded NATS server** with JetStream on `:4222` (in-process, real TCP)
- **HTTP server** on `:8092` — dashboard + per-demo pages
- **WebSocket hub** — pushes real-time events to the browser
- **Demo registry** — each demo is a self-contained package under `internal/demos/` that self-registers via `init()`
- **Frontend** — vanilla HTML/JS/CSS, no framework. Each demo has its own template and JS file.
- Each demo uses its own NATS subject namespace (e.g. `demo.pubsub.*`) so they don't interfere with each other.

## Project Structure

```
cmd/server/main.go          # Entry point, starts NATS + HTTP, registers demos
internal/
  hub/hub.go                # WebSocket hub (shared infra)
  demos/
    registry.go             # Demo registration system
    pubsub/pubsub.go        # Demo 1: Pub/Sub
    reqreply/reqreply.go    # Demo 2: Request/Reply
    queuegroup/             # Demo 3: Queue Groups
    fanout/                 # Demo 4: Broadcast
    wildcards/              # Demo 5: Wildcard Routing
    streams/                # Demo 6: JetStream Streams
    consumers/              # Demo 7: Durable Consumers
    workqueue/              # Demo 8: Work Queue
    kv/                     # Demo 9: Key-Value Store
    scatter/                # Demo 10: Scatter-Gather
    dedup/                  # Demo 11: Exactly-Once / Dedup
web/
  static/css/style.css      # Shared dark-theme styles
  static/js/pubsub.js       # Per-demo JS
  templates/
    layout.html             # Shared layout
    dashboard.html           # Dashboard listing all demos
    pubsub.html             # Per-demo HTML
```

## Demo Roadmap

| # | ID | Title | Category | Status |
|---|----|-------|----------|--------|
| 1 | `pubsub` | Pub/Sub Basic | Core NATS | Done |
| 2 | `reqreply` | Request/Reply | Core NATS | Done |
| 3 | `queuegroup` | Queue Groups | Core NATS | Done |
| 4 | `fanout` | Broadcast | Core NATS | Done |
| 5 | `wildcards` | Wildcard Routing | Core NATS | Done |
| 6 | `streams` | JetStream Streams | JetStream | Done |
| 7 | `consumers` | Durable Consumers | JetStream | Done |
| 8 | `workqueue` | Work Queue | JetStream | Done |
| 9 | `kv` | Key-Value Store | JetStream | Done |
| 10 | `scatter` | Scatter-Gather | Core NATS | Done |
| 11 | `dedup` | Exactly-Once / Dedup | JetStream | Done |

### What Each Demo Includes
- **Controls panel** — buttons to publish, subscribe, add workers, etc.
- **Live event log** — scrolling timestamped feed of events
- **Visual diagram** — nodes and messages flowing between them
- **Explanation sidebar** — what the pattern is, when to use it, key concepts

## Adding a New Demo

1. Create `internal/demos/<id>/<id>.go` — implement the demo, call `demos.Register()` in `init()`
2. Add a blank import in `cmd/server/main.go`: `_ "nats-lab/internal/demos/<id>"`
3. Create `web/templates/<id>.html` and `web/static/js/<id>.js`
4. Use NATS subjects namespaced as `demo.<id>.*`

## Conventions

- Go module name: `nats-lab`
- Dark theme UI with monospace font
- WebSocket messages are JSON with a `type` field
- Event log entries are color-coded: green = publish, cyan = receive, blue = subscribe
- Each demo is fully isolated — no cross-demo dependencies
- Subscribers have user-provided names (e.g. "OrderService") — group subscriptions by name in UI
- WebSocket connections must be deferred until after `window.load` to avoid Chrome connection-limit delays
- WebSocket connections must close on `beforeunload` to free Chrome connection slots (use a `closing` flag to suppress auto-reconnect during unload)
- WebSocket connections should auto-reconnect on disconnect
- The server prints a step-by-step startup log with a clear "Ready!" indicator
