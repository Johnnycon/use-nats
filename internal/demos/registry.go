package demos

import (
	"net/http"
	"sort"

	"github.com/nats-io/nats.go"
)

// Demo describes a single demo that can be registered with the server.
type Demo struct {
	ID          string // URL-safe identifier, e.g. "pubsub"
	Title       string // Human-readable title
	Description string // One-line description shown on the dashboard
	Category    string // "Core NATS" or "JetStream"
}

// RegisterFunc sets up routes for a demo.
type RegisterFunc func(mux *http.ServeMux, nc *nats.Conn)

var registry []entry

type entry struct {
	demo Demo
	fn   RegisterFunc
}

// Register adds a demo to the global registry.
func Register(d Demo, fn RegisterFunc) {
	registry = append(registry, entry{demo: d, fn: fn})
}

// SetupAll calls every registered demo's RegisterFunc.
func SetupAll(mux *http.ServeMux, nc *nats.Conn) {
	for _, e := range registry {
		e.fn(mux, nc)
	}
}

// All returns all registered demos (for the dashboard), sorted by title.
func All() []Demo {
	out := make([]Demo, len(registry))
	for i, e := range registry {
		out[i] = e.demo
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Title < out[j].Title
	})
	return out
}
