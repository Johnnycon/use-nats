package dedup

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
		ID:          "dedup",
		Title:       "11. Exactly-Once / Dedup",
		Description: "JetStream message deduplication using Nats-Msg-Id headers",
		Category:    "JetStream",
		Order:       11,
	}, Setup)
}

const streamName = "DEMO_DEDUP"

type state struct {
	mu      sync.Mutex
	msgSeq  int
	ready   bool
}

type command struct {
	Action    string `json:"action"`
	MsgID     string `json:"msgId,omitempty"`
	Payload   string `json:"payload,omitempty"`
	AutoID    bool   `json:"autoId,omitempty"`
	Count     int    `json:"count,omitempty"`
	DedupID   string `json:"dedupId,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{}

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("dedup: failed to get JetStream context: %v", err)
		return
	}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("dedup: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "setup":
			handleSetup(st, h, js)
		case "publish":
			handlePublish(st, h, js, nc, cmd)
		case "publish_duplicate":
			handlePublishDuplicate(st, h, js, nc, cmd)
		case "publish_batch":
			handlePublishBatch(st, h, js, nc, cmd)
		case "publish_no_dedup":
			handlePublishNoDedup(st, h, js, cmd)
		case "stream_info":
			handleStreamInfo(h, js)
		case "replay":
			handleReplay(h, js)
		}
	}

	mux.HandleFunc("/demo/dedup/ws", h.ServeWS)
}

func handleSetup(st *state, h *hub.Hub, js nats.JetStreamContext) {
	// Create stream with a 2-minute dedup window
	info, err := js.AddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{"demo.dedup.>"},
		MaxMsgs:    1000,
		Storage:    nats.FileStorage,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create stream: %v", err))
		return
	}

	st.mu.Lock()
	st.ready = true
	st.mu.Unlock()

	evt := map[string]any{
		"type": "stream_ready",
		"payload": map[string]any{
			"name":       info.Config.Name,
			"subjects":   info.Config.Subjects,
			"messages":   info.State.Msgs,
			"dedupWindow": int(info.Config.Duplicates.Seconds()),
			"created":    info.Created.Format(time.RFC3339),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublish(st *state, h *hub.Hub, js nats.JetStreamContext, nc *nats.Conn, cmd command) {
	st.mu.Lock()
	st.msgSeq++
	seq := st.msgSeq
	st.mu.Unlock()

	msgID := cmd.MsgID
	if msgID == "" {
		msgID = fmt.Sprintf("msg-%d", seq)
	}

	payload := cmd.Payload
	if payload == "" {
		payload = fmt.Sprintf(`{"order":"ORD-%03d","item":"Widget"}`, seq)
	}

	// Publish with Nats-Msg-Id header for dedup
	msg := &nats.Msg{
		Subject: "demo.dedup.orders",
		Data:    []byte(payload),
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", msgID)

	ack, err := js.PublishMsg(msg)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "published",
		"payload": map[string]any{
			"msgId":     msgID,
			"subject":   "demo.dedup.orders",
			"data":      payload,
			"stream":    ack.Stream,
			"sequence":  ack.Sequence,
			"duplicate": ack.Duplicate,
			"timestamp": time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublishDuplicate(st *state, h *hub.Hub, js nats.JetStreamContext, nc *nats.Conn, cmd command) {
	msgID := cmd.DedupID
	if msgID == "" {
		broadcastError(h, "Dedup ID is required")
		return
	}

	payload := cmd.Payload
	if payload == "" {
		payload = fmt.Sprintf(`{"order":"RETRY","msgId":"%s","note":"duplicate attempt"}`, msgID)
	}

	msg := &nats.Msg{
		Subject: "demo.dedup.orders",
		Data:    []byte(payload),
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", msgID)

	ack, err := js.PublishMsg(msg)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "published",
		"payload": map[string]any{
			"msgId":     msgID,
			"subject":   "demo.dedup.orders",
			"data":      payload,
			"stream":    ack.Stream,
			"sequence":  ack.Sequence,
			"duplicate": ack.Duplicate,
			"timestamp": time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublishBatch(st *state, h *hub.Hub, js nats.JetStreamContext, nc *nats.Conn, cmd command) {
	count := cmd.Count
	if count <= 0 {
		count = 5
	}

	// Publish N unique messages, then re-publish them all again
	type result struct {
		MsgID     string `json:"msgId"`
		Sequence  uint64 `json:"sequence"`
		Duplicate bool   `json:"duplicate"`
	}

	var originals []result
	var duplicates []result

	// First pass: unique messages
	for i := 0; i < count; i++ {
		st.mu.Lock()
		st.msgSeq++
		seq := st.msgSeq
		st.mu.Unlock()

		msgID := fmt.Sprintf("batch-%d", seq)
		payload := fmt.Sprintf(`{"order":"ORD-%03d","pass":"original"}`, seq)

		msg := &nats.Msg{
			Subject: "demo.dedup.orders",
			Data:    []byte(payload),
			Header:  nats.Header{},
		}
		msg.Header.Set("Nats-Msg-Id", msgID)

		ack, err := js.PublishMsg(msg)
		if err != nil {
			broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
			return
		}
		originals = append(originals, result{MsgID: msgID, Sequence: ack.Sequence, Duplicate: ack.Duplicate})
	}

	// Second pass: same IDs again (these should be deduped)
	for _, orig := range originals {
		payload := fmt.Sprintf(`{"order":"RETRY","msgId":"%s","pass":"duplicate"}`, orig.MsgID)

		msg := &nats.Msg{
			Subject: "demo.dedup.orders",
			Data:    []byte(payload),
			Header:  nats.Header{},
		}
		msg.Header.Set("Nats-Msg-Id", orig.MsgID)

		ack, err := js.PublishMsg(msg)
		if err != nil {
			broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
			return
		}
		duplicates = append(duplicates, result{MsgID: orig.MsgID, Sequence: ack.Sequence, Duplicate: ack.Duplicate})
	}

	evt := map[string]any{
		"type": "batch_result",
		"payload": map[string]any{
			"count":      count,
			"originals":  originals,
			"duplicates": duplicates,
			"timestamp":  time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublishNoDedup(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	st.mu.Lock()
	st.msgSeq++
	seq := st.msgSeq
	st.mu.Unlock()

	payload := cmd.Payload
	if payload == "" {
		payload = fmt.Sprintf(`{"order":"ORD-%03d","note":"no dedup header"}`, seq)
	}

	// Publish WITHOUT Nats-Msg-Id -- no dedup protection
	ack, err := js.Publish("demo.dedup.orders", []byte(payload))
	if err != nil {
		broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "published",
		"payload": map[string]any{
			"msgId":     "(none)",
			"subject":   "demo.dedup.orders",
			"data":      payload,
			"stream":    ack.Stream,
			"sequence":  ack.Sequence,
			"duplicate": false,
			"timestamp": time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleStreamInfo(h *hub.Hub, js nats.JetStreamContext) {
	info, err := js.StreamInfo(streamName)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Stream info failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "stream_info",
		"payload": map[string]any{
			"name":        info.Config.Name,
			"messages":    info.State.Msgs,
			"bytes":       info.State.Bytes,
			"firstSeq":    info.State.FirstSeq,
			"lastSeq":     info.State.LastSeq,
			"dedupWindow": int(info.Config.Duplicates.Seconds()),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleReplay(h *hub.Hub, js nats.JetStreamContext) {
	info, err := js.StreamInfo(streamName)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Stream info failed: %v", err))
		return
	}

	if info.State.Msgs == 0 {
		broadcastError(h, "Stream is empty")
		return
	}

	// Read all messages from the stream
	sub, err := js.SubscribeSync("demo.dedup.>",
		nats.BindStream(streamName),
		nats.DeliverAll(),
		nats.AckNone(),
	)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Subscribe failed: %v", err))
		return
	}
	defer sub.Unsubscribe()

	var messages []map[string]any
	for i := uint64(0); i < info.State.Msgs; i++ {
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			break
		}
		meta, _ := msg.Metadata()
		seq := uint64(0)
		ts := ""
		if meta != nil {
			seq = meta.Sequence.Stream
			ts = meta.Timestamp.Format(time.RFC3339)
		}
		msgID := msg.Header.Get("Nats-Msg-Id")
		messages = append(messages, map[string]any{
			"sequence":  seq,
			"subject":   msg.Subject,
			"data":      string(msg.Data),
			"msgId":     msgID,
			"timestamp": ts,
		})
	}

	evt := map[string]any{
		"type": "replay",
		"payload": map[string]any{
			"count":    len(messages),
			"messages": messages,
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
