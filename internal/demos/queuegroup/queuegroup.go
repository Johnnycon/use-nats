package queuegroup

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
		ID:          "queuegroup",
		Title:       "3. Queue Groups",
		Description: "Load-balanced message delivery — each message goes to one group member",
		Category:    "Core NATS",
		Order:       3,
	}, Setup)
}

// worker tracks a live NATS subscription (possibly queue-subscribed).
type worker struct {
	ID         string             `json:"id"`
	Name       string             `json:"name"`
	Subject    string             `json:"subject"`
	QueueGroup string             `json:"queueGroup"` // empty = regular subscriber
	Sub        *nats.Subscription `json:"-"`
	RecvCount  int                `json:"recvCount"`
}

type state struct {
	mu      sync.Mutex
	workers map[string]*worker
	seq     int
}

// command is what the browser sends us over WebSocket.
type command struct {
	Action     string `json:"action"`
	Subject    string `json:"subject,omitempty"`
	Message    string `json:"message,omitempty"`
	Name       string `json:"name,omitempty"`
	QueueGroup string `json:"queueGroup,omitempty"`
	WorkerID   string `json:"workerId,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{workers: make(map[string]*worker)}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("queuegroup: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "add_worker":
			handleAddWorker(st, h, nc, cmd)
		case "remove_worker":
			handleRemoveWorker(st, h, cmd)
		case "publish":
			handlePublish(st, h, nc, cmd)
		}
	}

	mux.HandleFunc("/demo/queuegroup/ws", h.ServeWS)
}

func handleAddWorker(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.queuegroup.tasks"
	}

	st.mu.Lock()
	st.seq++
	id := fmt.Sprintf("wkr-%d", st.seq)
	st.mu.Unlock()

	name := cmd.Name
	if name == "" {
		name = id
	}

	queueGroup := cmd.QueueGroup

	handler := func(msg *nats.Msg) {
		st.mu.Lock()
		w, ok := st.workers[id]
		if ok {
			w.RecvCount++
		}
		st.mu.Unlock()

		evt := map[string]any{
			"type": "message_received",
			"payload": map[string]any{
				"workerId":   id,
				"name":       name,
				"subject":    msg.Subject,
				"queueGroup": queueGroup,
				"data":       string(msg.Data),
				"timestamp":  time.Now().UnixMilli(),
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	}

	var sub *nats.Subscription
	var err error
	if queueGroup != "" {
		sub, err = nc.QueueSubscribe(subject, queueGroup, handler)
	} else {
		sub, err = nc.Subscribe(subject, handler)
	}
	if err != nil {
		log.Printf("queuegroup: subscribe error: %v", err)
		return
	}

	st.mu.Lock()
	st.workers[id] = &worker{
		ID:         id,
		Name:       name,
		Subject:    subject,
		QueueGroup: queueGroup,
		Sub:        sub,
	}
	st.mu.Unlock()

	evt := map[string]any{
		"type": "worker_added",
		"payload": map[string]any{
			"workerId":   id,
			"name":       name,
			"subject":    subject,
			"queueGroup": queueGroup,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleRemoveWorker(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	w, ok := st.workers[cmd.WorkerID]
	if ok {
		w.Sub.Unsubscribe()
		delete(st.workers, cmd.WorkerID)
	}
	st.mu.Unlock()

	if ok {
		evt := map[string]any{
			"type": "worker_removed",
			"payload": map[string]any{
				"workerId": cmd.WorkerID,
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	}
}

func handlePublish(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.queuegroup.tasks"
	}
	message := cmd.Message
	if message == "" {
		message = "task payload"
	}

	err := nc.Publish(subject, []byte(message))
	if err != nil {
		log.Printf("queuegroup: publish error: %v", err)
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
