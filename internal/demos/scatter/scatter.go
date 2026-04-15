package scatter

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
		ID:          "scatter",
		Title:       "10. Scatter-Gather",
		Description: "Broadcast a request to multiple services and collect all replies within a deadline",
		Category:    "Core NATS",
		Order:       10,
	}, Setup)
}

type responder struct {
	ID      string             `json:"id"`
	Name    string             `json:"name"`
	Subject string             `json:"subject"`
	DelayMs int                `json:"delayMs"`
	Sub     *nats.Subscription `json:"-"`
	Replied int                `json:"replied"`
}

type state struct {
	mu         sync.Mutex
	responders map[string]*responder
	seq        int
	scatterSeq int
}

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
			log.Printf("scatter: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "add_responder":
			handleAddResponder(st, h, nc, cmd)
		case "remove_responder":
			handleRemoveResponder(st, h, cmd)
		case "scatter":
			go handleScatter(st, h, nc, cmd)
		}
	}

	mux.HandleFunc("/demo/scatter/ws", h.ServeWS)
}

func handleAddResponder(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.scatter.search"
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

		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}

		replyData := fmt.Sprintf(`{"provider":"%s","query":%s,"delayMs":%d}`, name, string(msg.Data), delayMs)
		if err := msg.Respond([]byte(replyData)); err != nil {
			log.Printf("scatter: respond error: %v", err)
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
				"reply":     replyData,
				"delayMs":   delayMs,
				"timestamp": time.Now().UnixMilli(),
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Subscribe failed: %v", err))
		return
	}

	st.mu.Lock()
	st.responders[id] = &responder{
		ID: id, Name: name, Subject: subject, DelayMs: delayMs, Sub: sub,
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

func handleScatter(st *state, h *hub.Hub, nc *nats.Conn, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.scatter.search"
	}
	payload := cmd.Payload
	if payload == "" {
		payload = `"search query"`
	}
	timeoutMs := cmd.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 3000
	}

	st.mu.Lock()
	st.scatterSeq++
	scatterID := fmt.Sprintf("scatter-%d", st.scatterSeq)
	st.mu.Unlock()

	// Create unique reply inbox
	inbox := nc.NewInbox()

	// Subscribe to the inbox to collect replies
	var mu sync.Mutex
	var replies []map[string]any
	startTime := time.Now()

	sub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		elapsed := time.Since(startTime).Milliseconds()
		mu.Lock()
		idx := len(replies)
		reply := map[string]any{
			"index":     idx,
			"data":      string(msg.Data),
			"latencyMs": elapsed,
		}
		replies = append(replies, reply)
		mu.Unlock()

		// Broadcast each reply as it arrives
		evt := map[string]any{
			"type": "scatter_reply",
			"payload": map[string]any{
				"scatterId": scatterID,
				"index":     idx,
				"data":      string(msg.Data),
				"latencyMs": elapsed,
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Subscribe to inbox failed: %v", err))
		return
	}
	defer sub.Unsubscribe()

	// Tell the UI the scatter request was sent
	sentEvt := map[string]any{
		"type": "scatter_sent",
		"payload": map[string]any{
			"scatterId": scatterID,
			"subject":   subject,
			"payload":   payload,
			"timeoutMs": timeoutMs,
			"timestamp": startTime.UnixMilli(),
		},
	}
	sentData, _ := json.Marshal(sentEvt)
	h.BroadcastRaw(sentData)

	// Publish the request with our inbox as the reply subject
	err = nc.PublishRequest(subject, inbox, []byte(payload))
	if err != nil {
		broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
		return
	}

	// Wait for the timeout to collect all replies
	time.Sleep(time.Duration(timeoutMs) * time.Millisecond)

	// Gather final results
	mu.Lock()
	totalReplies := len(replies)
	finalReplies := make([]map[string]any, len(replies))
	copy(finalReplies, replies)
	mu.Unlock()

	doneEvt := map[string]any{
		"type": "scatter_done",
		"payload": map[string]any{
			"scatterId":  scatterID,
			"subject":    subject,
			"totalMs":    time.Since(startTime).Milliseconds(),
			"replyCount": totalReplies,
			"replies":    finalReplies,
		},
	}
	doneData, _ := json.Marshal(doneEvt)
	h.BroadcastRaw(doneData)
}

func broadcastError(h *hub.Hub, msg string) {
	evt := map[string]any{
		"type": "error",
		"payload": map[string]any{
			"message": msg,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}
