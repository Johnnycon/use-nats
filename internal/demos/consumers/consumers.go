package consumers

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
		ID:          "consumers",
		Title:       "7. Durable Consumers",
		Description: "Durable and ephemeral consumers with ack/nak, redelivery, and deliver policies",
		Category:    "JetStream",
		Order:       7,
	}, Setup)
}

const streamName = "DEMO_CONSUMERS"

type pendingMsg struct {
	Msg      *nats.Msg `json:"-"`
	ID       string    `json:"id"`
	Sequence uint64    `json:"sequence"`
	Subject  string    `json:"subject"`
	Data     string    `json:"data"`
	Consumer string    `json:"consumer"`
}

type consumerInfo struct {
	Name     string `json:"name"`
	Durable  bool   `json:"durable"`
	Policy   string `json:"policy"`
	AckWait  int    `json:"ackWait"`
	FilterSu string `json:"filterSubject"`
}

type state struct {
	mu          sync.Mutex
	consumers   map[string]*consumerInfo
	pending     map[string]*pendingMsg // msgID -> pendingMsg
	pendingSeq  int
	subs        map[string]*nats.Subscription // consumerName -> pull subscription
}

type command struct {
	Action        string `json:"action"`
	ConsumerName  string `json:"consumerName,omitempty"`
	Durable       bool   `json:"durable,omitempty"`
	DeliverPolicy string `json:"deliverPolicy,omitempty"`
	AckWait       int    `json:"ackWait,omitempty"`
	FilterSubject string `json:"filterSubject,omitempty"`
	Subject       string `json:"subject,omitempty"`
	Message       string `json:"message,omitempty"`
	BatchSize     int    `json:"batchSize,omitempty"`
	MsgID         string `json:"msgId,omitempty"`
	StartSeq      uint64 `json:"startSeq,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{
		consumers: make(map[string]*consumerInfo),
		pending:   make(map[string]*pendingMsg),
		subs:      make(map[string]*nats.Subscription),
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("consumers: failed to get JetStream context: %v", err)
		return
	}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("consumers: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "setup":
			handleSetup(st, h, js)
		case "create_consumer":
			handleCreateConsumer(st, h, js, cmd)
		case "delete_consumer":
			handleDeleteConsumer(st, h, js, cmd)
		case "publish":
			handlePublish(h, js, cmd)
		case "pull":
			handlePull(st, h, js, cmd)
		case "ack":
			handleAck(st, h, cmd)
		case "nak":
			handleNak(st, h, cmd)
		case "consumer_info":
			handleConsumerInfo(st, h, js, cmd)
		}
	}

	mux.HandleFunc("/demo/consumers/ws", h.ServeWS)
}

func handleSetup(st *state, h *hub.Hub, js nats.JetStreamContext) {
	info, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"demo.consumers.>"},
		MaxMsgs:  1000,
		Storage:  nats.FileStorage,
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create stream: %v", err))
		return
	}

	evt := map[string]any{
		"type": "stream_ready",
		"payload": map[string]any{
			"name":     info.Config.Name,
			"subjects": info.Config.Subjects,
			"messages": info.State.Msgs,
			"created":  info.Created.Format(time.RFC3339),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleCreateConsumer(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.ConsumerName
	if name == "" {
		broadcastError(h, "Consumer name is required")
		return
	}

	ackWait := time.Duration(cmd.AckWait) * time.Second
	if ackWait <= 0 {
		ackWait = 30 * time.Second
	}

	filterSubject := cmd.FilterSubject
	if filterSubject == "" {
		filterSubject = "demo.consumers.>"
	}

	cfg := &nats.ConsumerConfig{
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       ackWait,
		FilterSubject: filterSubject,
	}

	if cmd.Durable {
		cfg.Durable = name
	} else {
		cfg.Name = name
	}

	switch cmd.DeliverPolicy {
	case "new":
		cfg.DeliverPolicy = nats.DeliverNewPolicy
	case "by_start_sequence":
		cfg.DeliverPolicy = nats.DeliverByStartSequencePolicy
		cfg.OptStartSeq = cmd.StartSeq
	default:
		cfg.DeliverPolicy = nats.DeliverAllPolicy
	}

	ci, err := js.AddConsumer(streamName, cfg)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create consumer: %v", err))
		return
	}

	isDurable := ci.Config.Durable != ""
	policyStr := deliverPolicyString(ci.Config.DeliverPolicy)

	st.mu.Lock()
	st.consumers[name] = &consumerInfo{
		Name:     name,
		Durable:  isDurable,
		Policy:   policyStr,
		AckWait:  int(ci.Config.AckWait.Seconds()),
		FilterSu: ci.Config.FilterSubject,
	}
	st.mu.Unlock()

	evt := map[string]any{
		"type": "consumer_created",
		"payload": map[string]any{
			"name":            name,
			"durable":         isDurable,
			"deliverPolicy":   policyStr,
			"ackWait":         int(ci.Config.AckWait.Seconds()),
			"filterSubject":   ci.Config.FilterSubject,
			"numPending":      ci.NumPending,
			"numAckPending":   ci.NumAckPending,
			"delivered":       ci.Delivered.Stream,
			"ackFloor":        ci.AckFloor.Stream,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleDeleteConsumer(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.ConsumerName
	if name == "" {
		broadcastError(h, "Consumer name is required")
		return
	}

	err := js.DeleteConsumer(streamName, name)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to delete consumer: %v", err))
		return
	}

	st.mu.Lock()
	delete(st.consumers, name)
	// Clean up any pull subscription
	if sub, ok := st.subs[name]; ok {
		sub.Unsubscribe()
		delete(st.subs, name)
	}
	// Remove pending messages for this consumer
	for id, pm := range st.pending {
		if pm.Consumer == name {
			delete(st.pending, id)
		}
	}
	st.mu.Unlock()

	evt := map[string]any{
		"type": "consumer_deleted",
		"payload": map[string]any{
			"name": name,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublish(h *hub.Hub, js nats.JetStreamContext, cmd command) {
	subject := cmd.Subject
	if subject == "" {
		subject = "demo.consumers.orders.created"
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

func handlePull(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.ConsumerName
	if name == "" {
		broadcastError(h, "Consumer name is required")
		return
	}

	batchSize := cmd.BatchSize
	if batchSize <= 0 {
		batchSize = 5
	}

	filterSubject := ""
	st.mu.Lock()
	ci, exists := st.consumers[name]
	if exists {
		filterSubject = ci.FilterSu
	}
	st.mu.Unlock()

	if filterSubject == "" {
		filterSubject = "demo.consumers.>"
	}

	st.mu.Lock()
	sub, hasSub := st.subs[name]
	st.mu.Unlock()

	if !hasSub {
		var err error
		sub, err = js.PullSubscribe(filterSubject, name, nats.BindStream(streamName))
		if err != nil {
			broadcastError(h, fmt.Sprintf("Pull subscribe failed: %v", err))
			return
		}
		st.mu.Lock()
		st.subs[name] = sub
		st.mu.Unlock()
	}

	msgs, err := sub.Fetch(batchSize, nats.MaxWait(2*time.Second))
	if err != nil && len(msgs) == 0 {
		if err == nats.ErrTimeout {
			evt := map[string]any{
				"type": "pull_complete",
				"payload": map[string]any{
					"consumer": name,
					"count":    0,
					"message":  "No messages available",
				},
			}
			data, _ := json.Marshal(evt)
			h.BroadcastRaw(data)
			return
		}
		broadcastError(h, fmt.Sprintf("Fetch failed: %v", err))
		return
	}

	pulled := make([]map[string]any, 0, len(msgs))
	st.mu.Lock()
	for _, msg := range msgs {
		st.pendingSeq++
		msgID := fmt.Sprintf("msg-%d", st.pendingSeq)

		meta, _ := msg.Metadata()
		seq := uint64(0)
		ts := time.Now()
		if meta != nil {
			seq = meta.Sequence.Stream
			ts = meta.Timestamp
		}

		pm := &pendingMsg{
			Msg:      msg,
			ID:       msgID,
			Sequence: seq,
			Subject:  msg.Subject,
			Data:     string(msg.Data),
			Consumer: name,
		}
		st.pending[msgID] = pm

		pulled = append(pulled, map[string]any{
			"msgId":     msgID,
			"sequence":  seq,
			"subject":   msg.Subject,
			"data":      string(msg.Data),
			"consumer":  name,
			"timestamp": ts.Format(time.RFC3339),
		})
	}
	st.mu.Unlock()

	evt := map[string]any{
		"type": "messages_pulled",
		"payload": map[string]any{
			"consumer": name,
			"count":    len(pulled),
			"messages": pulled,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleAck(st *state, h *hub.Hub, cmd command) {
	msgID := cmd.MsgID
	if msgID == "" {
		broadcastError(h, "Message ID is required")
		return
	}

	st.mu.Lock()
	pm, ok := st.pending[msgID]
	if !ok {
		st.mu.Unlock()
		broadcastError(h, fmt.Sprintf("Message %s not found or already handled", msgID))
		return
	}
	msg := pm.Msg
	consumer := pm.Consumer
	seq := pm.Sequence
	delete(st.pending, msgID)
	st.mu.Unlock()

	err := msg.Ack()
	if err != nil {
		broadcastError(h, fmt.Sprintf("Ack failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "message_acked",
		"payload": map[string]any{
			"msgId":    msgID,
			"sequence": seq,
			"consumer": consumer,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleNak(st *state, h *hub.Hub, cmd command) {
	msgID := cmd.MsgID
	if msgID == "" {
		broadcastError(h, "Message ID is required")
		return
	}

	st.mu.Lock()
	pm, ok := st.pending[msgID]
	if !ok {
		st.mu.Unlock()
		broadcastError(h, fmt.Sprintf("Message %s not found or already handled", msgID))
		return
	}
	msg := pm.Msg
	consumer := pm.Consumer
	seq := pm.Sequence
	delete(st.pending, msgID)
	st.mu.Unlock()

	err := msg.Nak()
	if err != nil {
		broadcastError(h, fmt.Sprintf("Nak failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "message_naked",
		"payload": map[string]any{
			"msgId":    msgID,
			"sequence": seq,
			"consumer": consumer,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleConsumerInfo(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.ConsumerName
	if name == "" {
		broadcastError(h, "Consumer name is required")
		return
	}

	ci, err := js.ConsumerInfo(streamName, name)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Consumer info failed: %v", err))
		return
	}

	isDurable := ci.Config.Durable != ""
	policyStr := deliverPolicyString(ci.Config.DeliverPolicy)

	evt := map[string]any{
		"type": "consumer_info",
		"payload": map[string]any{
			"name":          name,
			"durable":       isDurable,
			"deliverPolicy": policyStr,
			"ackWait":       int(ci.Config.AckWait.Seconds()),
			"filterSubject": ci.Config.FilterSubject,
			"numPending":    ci.NumPending,
			"numAckPending": ci.NumAckPending,
			"delivered":     ci.Delivered.Stream,
			"ackFloor":      ci.AckFloor.Stream,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func deliverPolicyString(p nats.DeliverPolicy) string {
	switch p {
	case nats.DeliverAllPolicy:
		return "all"
	case nats.DeliverNewPolicy:
		return "new"
	case nats.DeliverByStartSequencePolicy:
		return "by_start_sequence"
	case nats.DeliverLastPolicy:
		return "last"
	case nats.DeliverLastPerSubjectPolicy:
		return "last_per_subject"
	default:
		return "all"
	}
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
