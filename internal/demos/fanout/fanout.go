package fanout

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"nats-lab/internal/demos"
	"nats-lab/internal/hub"

	"github.com/nats-io/nats.go"
)

func init() {
	demos.Register(demos.Demo{
		ID:          "fanout",
		Title:       "4. Broadcast",
		Description: "One message, every subscriber receives it — fire-and-forget to all instances",
		Category:    "Core NATS",
		Order:       4,
	}, Setup)
}

// subscriber tracks a live NATS subscription for the UI.
type subscriber struct {
	ID      string             `json:"id"`
	Name    string             `json:"name"`
	Subject string             `json:"subject"`
	Sub     *nats.Subscription `json:"-"`
}

type state struct {
	mu   sync.Mutex
	subs map[string]*subscriber
	seq  int
}

// command is what the browser sends us over WebSocket.
type command struct {
	Action  string `json:"action"`
	Subject string `json:"subject,omitempty"`
	Message string `json:"message,omitempty"`
	SubID   string `json:"subId,omitempty"`
	Name    string `json:"name,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{subs: make(map[string]*subscriber)}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("fanout: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "subscribe":
			handleSubscribe(st, h, nc, cmd)
		case "unsubscribe":
			handleUnsubscribe(st, h, cmd)
		case "publish":
			handlePublish(st, h, nc, cmd)
		}
	}

	mux.HandleFunc("/demo/fanout/ws", h.ServeWS)
}

func handleSubscribe(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.fanout.default"
	}

	st.mu.Lock()
	st.seq++
	id := fmt.Sprintf("sub-%d", st.seq)
	st.mu.Unlock()

	name := cmd.Name
	if name == "" {
		name = id
	}

	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		evt := map[string]any{
			"type": "message_received",
			"payload": map[string]any{
				"subId":     id,
				"name":      name,
				"subject":   msg.Subject,
				"data":      string(msg.Data),
				"timestamp": time.Now().UnixMilli(),
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	})
	if err != nil {
		log.Printf("fanout: subscribe error: %v", err)
		return
	}

	st.mu.Lock()
	st.subs[id] = &subscriber{ID: id, Name: name, Subject: subject, Sub: sub}
	st.mu.Unlock()

	evt := map[string]any{
		"type": "subscribed",
		"payload": map[string]any{
			"subId":   id,
			"name":    name,
			"subject": subject,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleUnsubscribe(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	s, ok := st.subs[cmd.SubID]
	if ok {
		s.Sub.Unsubscribe()
		delete(st.subs, cmd.SubID)
	}
	st.mu.Unlock()

	if ok {
		evt := map[string]any{
			"type": "unsubscribed",
			"payload": map[string]any{
				"subId": cmd.SubID,
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	}
}

func handlePublish(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.fanout.default"
	}
	message := cmd.Message
	if message == "" {
		message = "broadcast payload"
	}

	err := nc.Publish(subject, []byte(message))
	if err != nil {
		log.Printf("fanout: publish error: %v", err)
		return
	}

	evt := map[string]any{
		"type": "published",
		"payload": map[string]any{
			"subject":   subject,
			"data":      message,
			"timestamp": time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}
