package reqreply

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
		ID:          "reqreply",
		Title:       "2. Request/Reply",
		Description: "Synchronous RPC-style messaging — send a request and get a response",
		Category:    "Core NATS",
		Order:       2,
	}, Setup)
}

// responder tracks a live NATS subscription that replies to requests.
type responder struct {
	ID       string             `json:"id"`
	Name     string             `json:"name"`
	Subject  string             `json:"subject"`
	DelayMs  int                `json:"delayMs"`
	Sub      *nats.Subscription `json:"-"`
	Replied  int                `json:"replied"`
}

type state struct {
	mu         sync.Mutex
	responders map[string]*responder
	seq        int
}

// command is what the browser sends us over WebSocket.
type command struct {
	Action    string `json:"action"`
	Subject   string `json:"subject,omitempty"`
	Payload   string `json:"payload,omitempty"`
	Name      string `json:"name,omitempty"`
	DelayMs   int    `json:"delayMs,omitempty"`
	TimeoutMs int    `json:"timeoutMs,omitempty"`
	RespID    string `json:"respId,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{responders: make(map[string]*responder)}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("reqreply: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "add_responder":
			handleAddResponder(st, h, nc, cmd)
		case "remove_responder":
			handleRemoveResponder(st, h, cmd)
		case "request":
			go handleRequest(st, h, nc, cmd)
		}
	}

	mux.HandleFunc("/demo/reqreply/ws", h.ServeWS)
}

func handleAddResponder(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.reqreply.default"
	}

	st.mu.Lock()
	st.seq++
	id := fmt.Sprintf("resp-%d", st.seq)
	st.mu.Unlock()

	name := cmd.Name
	if name == "" {
		name = id
	}

	delayMs := cmd.DelayMs
	if delayMs < 0 {
		delayMs = 0
	}

	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}

		receiveTime := time.Now()

		// Simulate processing delay
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}

		replyData := fmt.Sprintf("Reply from %s: processed %q", name, string(msg.Data))
		if err := msg.Respond([]byte(replyData)); err != nil {
			log.Printf("reqreply: respond error: %v", err)
			return
		}

		st.mu.Lock()
		r, ok := st.responders[id]
		if ok {
			r.Replied++
		}
		st.mu.Unlock()

		evt := map[string]any{
			"type": "responder_replied",
			"payload": map[string]any{
				"respId":    id,
				"name":      name,
				"subject":   msg.Subject,
				"request":   string(msg.Data),
				"reply":     replyData,
				"delayMs":   delayMs,
				"timestamp": receiveTime.UnixMilli(),
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	})
	if err != nil {
		log.Printf("reqreply: subscribe error: %v", err)
		return
	}

	st.mu.Lock()
	st.responders[id] = &responder{
		ID:      id,
		Name:    name,
		Subject: subject,
		DelayMs: delayMs,
		Sub:     sub,
	}
	st.mu.Unlock()

	evt := map[string]any{
		"type": "responder_added",
		"payload": map[string]any{
			"respId":  id,
			"name":    name,
			"subject": subject,
			"delayMs": delayMs,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleRemoveResponder(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	r, ok := st.responders[cmd.RespID]
	if ok {
		r.Sub.Unsubscribe()
		delete(st.responders, cmd.RespID)
	}
	st.mu.Unlock()

	if ok {
		evt := map[string]any{
			"type": "responder_removed",
			"payload": map[string]any{
				"respId": cmd.RespID,
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	}
}

func handleRequest(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.reqreply.default"
	}
	payload := cmd.Payload
	if payload == "" {
		payload = "Hello from requester!"
	}
	timeoutMs := cmd.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 3000
	}

	sentTime := time.Now()

	// Notify UI that request was sent
	sentEvt := map[string]any{
		"type": "request_sent",
		"payload": map[string]any{
			"subject":   subject,
			"data":      payload,
			"timeoutMs": timeoutMs,
			"timestamp": sentTime.UnixMilli(),
		},
	}
	sentData, _ := json.Marshal(sentEvt)
	h.BroadcastRaw(sentData)

	// Send the NATS request with timeout
	msg, err := nc.Request(subject, []byte(payload), time.Duration(timeoutMs)*time.Millisecond)
	latency := time.Since(sentTime)

	if err != nil {
		errEvt := map[string]any{
			"type": "request_timeout",
			"payload": map[string]any{
				"subject":   subject,
				"data":      payload,
				"error":     err.Error(),
				"latencyMs": latency.Milliseconds(),
				"timestamp": time.Now().UnixMilli(),
			},
		}
		errData, _ := json.Marshal(errEvt)
		h.BroadcastRaw(errData)
		return
	}

	replyEvt := map[string]any{
		"type": "reply_received",
		"payload": map[string]any{
			"subject":   subject,
			"request":   payload,
			"reply":     string(msg.Data),
			"latencyMs": latency.Milliseconds(),
			"timestamp": time.Now().UnixMilli(),
		},
	}
	replyData, _ := json.Marshal(replyEvt)
	h.BroadcastRaw(replyData)
}
