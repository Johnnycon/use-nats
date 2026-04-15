package streams

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
		ID:          "streams",
		Title:       "6. JetStream Streams",
		Description: "Persistent message storage with JetStream streams, replay, and retention limits",
		Category:    "JetStream",
		Order:       6,
	}, Setup)
}

type state struct {
	mu          sync.Mutex
	streamNames map[string]bool
}

// command is what the browser sends over WebSocket.
type command struct {
	Action      string `json:"action"`
	StreamName  string `json:"streamName,omitempty"`
	Subjects    string `json:"subjects,omitempty"`
	MaxMsgs     int64  `json:"maxMsgs,omitempty"`
	Subject     string `json:"subject,omitempty"`
	Message     string `json:"message,omitempty"`
	StartSeq    uint64 `json:"startSeq,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{streamNames: make(map[string]bool)}

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("streams: failed to get JetStream context: %v", err)
		return
	}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("streams: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "create_stream":
			handleCreateStream(st, h, js, cmd)
		case "publish":
			handlePublish(h, js, cmd)
		case "stream_info":
			handleStreamInfo(st, h, js, cmd)
		case "replay":
			handleReplay(st, h, js, cmd)
		case "purge":
			handlePurge(st, h, js, cmd)
		case "delete_stream":
			handleDeleteStream(st, h, js, cmd)
		}
	}

	mux.HandleFunc("/demo/streams/ws", h.ServeWS)
}

func handleCreateStream(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.StreamName
	if name == "" {
		name = "ORDERS"
	}
	subjects := cmd.Subjects
	if subjects == "" {
		subjects = "orders.>"
	}
	maxMsgs := cmd.MaxMsgs
	if maxMsgs <= 0 {
		maxMsgs = 1000
	}

	// Parse subjects (comma-separated)
	subjectList := splitSubjects(subjects)

	info, err := js.AddStream(&nats.StreamConfig{
		Name:     name,
		Subjects: subjectList,
		MaxMsgs:  maxMsgs,
		Storage:  nats.FileStorage,
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create stream: %v", err))
		return
	}

	st.mu.Lock()
	st.streamNames[name] = true
	st.mu.Unlock()

	evt := map[string]any{
		"type": "stream_created",
		"payload": map[string]any{
			"name":     info.Config.Name,
			"subjects": info.Config.Subjects,
			"maxMsgs":  info.Config.MaxMsgs,
			"storage":  info.Config.Storage.String(),
			"created":  info.Created.Format(time.RFC3339),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublish(h *hub.Hub, js nats.JetStreamContext, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "orders.created"
	}
	message := cmd.Message
	if message == "" {
		message = "{}"
	}

	ack, err := js.Publish(subject, []byte(message))
	if err != nil {
		broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "published",
		"payload": map[string]any{
			"subject":   subject,
			"data":      message,
			"stream":    ack.Stream,
			"sequence":  ack.Sequence,
			"timestamp": time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleStreamInfo(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.StreamName
	if name == "" {
		name = "ORDERS"
	}

	info, err := js.StreamInfo(name)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Stream info failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "stream_info",
		"payload": map[string]any{
			"name":      info.Config.Name,
			"subjects":  info.Config.Subjects,
			"maxMsgs":   info.Config.MaxMsgs,
			"storage":   info.Config.Storage.String(),
			"messages":  info.State.Msgs,
			"bytes":     info.State.Bytes,
			"firstSeq":  info.State.FirstSeq,
			"lastSeq":   info.State.LastSeq,
			"consumers": info.State.Consumers,
			"created":   info.Created.Format(time.RFC3339),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleReplay(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.StreamName
	if name == "" {
		name = "ORDERS"
	}

	// Get stream info first to know how many messages
	info, err := js.StreamInfo(name)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Replay failed: %v", err))
		return
	}

	if info.State.Msgs == 0 {
		broadcastError(h, "Stream is empty -- nothing to replay")
		return
	}

	// Signal replay start
	startEvt := map[string]any{
		"type": "replay_start",
		"payload": map[string]any{
			"stream":   name,
			"messages": info.State.Msgs,
		},
	}
	startData, _ := json.Marshal(startEvt)
	h.BroadcastRaw(startData)

	// Create an ephemeral ordered consumer to read all messages
	sub, err := js.SubscribeSync(
		">",
		nats.BindStream(name),
		nats.DeliverAll(),
		nats.OrderedConsumer(),
	)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Replay subscribe failed: %v", err))
		return
	}
	defer sub.Unsubscribe()

	replayed := uint64(0)
	total := info.State.Msgs

	for replayed < total {
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			break
		}

		meta, _ := msg.Metadata()
		seq := uint64(0)
		ts := time.Now()
		if meta != nil {
			seq = meta.Sequence.Stream
			ts = meta.Timestamp
		}

		evt := map[string]any{
			"type": "replay_message",
			"payload": map[string]any{
				"sequence":  seq,
				"subject":   msg.Subject,
				"data":      string(msg.Data),
				"timestamp": ts.Format(time.RFC3339),
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
		replayed++
	}

	// Signal replay end
	endEvt := map[string]any{
		"type": "replay_end",
		"payload": map[string]any{
			"stream":   name,
			"replayed": replayed,
		},
	}
	endData, _ := json.Marshal(endEvt)
	h.BroadcastRaw(endData)
}

func handlePurge(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.StreamName
	if name == "" {
		name = "ORDERS"
	}

	err := js.PurgeStream(name)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Purge failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "stream_purged",
		"payload": map[string]any{
			"stream": name,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleDeleteStream(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.StreamName
	if name == "" {
		name = "ORDERS"
	}

	err := js.DeleteStream(name)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Delete stream failed: %v", err))
		return
	}

	st.mu.Lock()
	delete(st.streamNames, name)
	st.mu.Unlock()

	evt := map[string]any{
		"type": "stream_deleted",
		"payload": map[string]any{
			"stream": name,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
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

func splitSubjects(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			tok := trimSpace(s[start:i])
			if tok != "" {
				out = append(out, tok)
			}
			start = i + 1
		}
	}
	tok := trimSpace(s[start:])
	if tok != "" {
		out = append(out, tok)
	}
	return out
}

func trimSpace(s string) string {
	i := 0
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	j := len(s)
	for j > i && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	return s[i:j]
}
