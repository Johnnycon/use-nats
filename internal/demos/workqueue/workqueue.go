package workqueue

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
		ID:          "workqueue",
		Title:       "8. Work Queue",
		Description: "JetStream work queue with competing pull consumers and exactly-once task delivery",
		Category:    "JetStream",
		Order:       8,
	}, Setup)
}

const streamName = "DEMO_WORKQUEUE"
const consumerName = "TASK_WORKERS"

type workerInfo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Completed  int    `json:"completed"`
	Failed     int    `json:"failed"`
	Processing int    `json:"processing"`
}

type pendingMsg struct {
	Msg      *nats.Msg `json:"-"`
	ID       string    `json:"id"`
	Sequence uint64    `json:"sequence"`
	Subject  string    `json:"subject"`
	Data     string    `json:"data"`
	WorkerID string    `json:"workerId"`
}

type state struct {
	mu         sync.Mutex
	workers    map[string]*workerInfo
	pending    map[string]*pendingMsg
	subs       map[string]*nats.Subscription
	pendingSeq int
	workerSeq  int
	taskSeq    int
}

type command struct {
	Action     string `json:"action"`
	WorkerName string `json:"workerName,omitempty"`
	WorkerID   string `json:"workerId,omitempty"`
	TaskType   string `json:"taskType,omitempty"`
	TaskData   string `json:"taskData,omitempty"`
	BatchSize  int    `json:"batchSize,omitempty"`
	MsgID      string `json:"msgId,omitempty"`
	Count      int    `json:"count,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{
		workers: make(map[string]*workerInfo),
		pending: make(map[string]*pendingMsg),
		subs:    make(map[string]*nats.Subscription),
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("workqueue: failed to get JetStream context: %v", err)
		return
	}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("workqueue: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "setup":
			handleSetup(st, h, js)
		case "add_worker":
			handleAddWorker(st, h, js, cmd)
		case "remove_worker":
			handleRemoveWorker(st, h, cmd)
		case "publish_task":
			handlePublishTask(h, js, st, cmd)
		case "publish_batch":
			handlePublishBatch(h, js, st, cmd)
		case "fetch_tasks":
			handleFetchTasks(st, h, cmd)
		case "ack":
			handleAck(st, h, cmd)
		case "nak":
			handleNak(st, h, cmd)
		case "stream_info":
			handleStreamInfo(h, js)
		}
	}

	mux.HandleFunc("/demo/workqueue/ws", h.ServeWS)
}

func handleSetup(st *state, h *hub.Hub, js nats.JetStreamContext) {
	info, err := js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"demo.workqueue.>"},
		MaxMsgs:   10000,
		Storage:   nats.FileStorage,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create stream: %v", err))
		return
	}

	evt := map[string]any{
		"type": "stream_ready",
		"payload": map[string]any{
			"name":      info.Config.Name,
			"subjects":  info.Config.Subjects,
			"messages":  info.State.Msgs,
			"retention": "WorkQueue",
			"created":   info.Created.Format(time.RFC3339),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)

	// Create the shared durable consumer
	ci, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		FilterSubject: "demo.workqueue.>",
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create consumer: %v", err))
		return
	}

	evt2 := map[string]any{
		"type": "consumer_ready",
		"payload": map[string]any{
			"name":       ci.Config.Durable,
			"numPending": ci.NumPending,
		},
	}
	data2, _ := json.Marshal(evt2)
	h.BroadcastRaw(data2)
}

func handleAddWorker(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	st.mu.Lock()
	st.workerSeq++
	id := fmt.Sprintf("worker-%d", st.workerSeq)
	st.mu.Unlock()

	name := cmd.WorkerName
	if name == "" {
		name = id
	}

	sub, err := js.PullSubscribe("demo.workqueue.>", consumerName, nats.BindStream(streamName))
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create pull subscription: %v", err))
		return
	}

	w := &workerInfo{ID: id, Name: name}

	st.mu.Lock()
	st.workers[id] = w
	st.subs[id] = sub
	st.mu.Unlock()

	evt := map[string]any{
		"type": "worker_added",
		"payload": map[string]any{
			"id":         id,
			"name":       name,
			"completed":  0,
			"failed":     0,
			"processing": 0,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleRemoveWorker(st *state, h *hub.Hub, cmd command) {
	id := cmd.WorkerID
	if id == "" {
		broadcastError(h, "Worker ID is required")
		return
	}

	st.mu.Lock()
	_, exists := st.workers[id]
	if !exists {
		st.mu.Unlock()
		broadcastError(h, fmt.Sprintf("Worker %s not found", id))
		return
	}
	if sub, ok := st.subs[id]; ok {
		sub.Unsubscribe()
		delete(st.subs, id)
	}
	delete(st.workers, id)
	for msgID, pm := range st.pending {
		if pm.WorkerID == id {
			delete(st.pending, msgID)
		}
	}
	st.mu.Unlock()

	evt := map[string]any{
		"type": "worker_removed",
		"payload": map[string]any{
			"workerId": id,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublishTask(h *hub.Hub, js nats.JetStreamContext, st *state, cmd command) {
	taskType := cmd.TaskType
	if taskType == "" {
		taskType = "email"
	}
	subject := "demo.workqueue.tasks." + taskType

	st.mu.Lock()
	st.taskSeq++
	taskID := fmt.Sprintf("TASK-%03d", st.taskSeq)
	st.mu.Unlock()

	payload := cmd.TaskData
	if payload == "" {
		payload = fmt.Sprintf(`{"taskId":"%s","type":"%s"}`, taskID, taskType)
	}

	ack, err := js.Publish(subject, []byte(payload))
	if err != nil {
		broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "task_published",
		"payload": map[string]any{
			"taskId":    taskID,
			"subject":   subject,
			"data":      payload,
			"stream":    ack.Stream,
			"sequence":  ack.Sequence,
			"timestamp": time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePublishBatch(h *hub.Hub, js nats.JetStreamContext, st *state, cmd command) {
	count := cmd.Count
	if count <= 0 {
		count = 10
	}

	types := []string{"email", "resize", "report", "notify"}
	var firstSeq, lastSeq uint64

	for i := 0; i < count; i++ {
		taskType := types[i%len(types)]
		subject := "demo.workqueue.tasks." + taskType

		st.mu.Lock()
		st.taskSeq++
		taskID := fmt.Sprintf("TASK-%03d", st.taskSeq)
		st.mu.Unlock()

		payload := fmt.Sprintf(`{"taskId":"%s","type":"%s"}`, taskID, taskType)
		ack, err := js.Publish(subject, []byte(payload))
		if err != nil {
			broadcastError(h, fmt.Sprintf("Publish failed: %v", err))
			return
		}
		if i == 0 {
			firstSeq = ack.Sequence
		}
		lastSeq = ack.Sequence
	}

	evt := map[string]any{
		"type": "batch_published",
		"payload": map[string]any{
			"count":     count,
			"firstSeq":  firstSeq,
			"lastSeq":   lastSeq,
			"timestamp": time.Now().UnixMilli(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleFetchTasks(st *state, h *hub.Hub, cmd command) {
	id := cmd.WorkerID
	if id == "" {
		broadcastError(h, "Worker ID is required")
		return
	}

	batchSize := cmd.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	st.mu.Lock()
	worker, wOk := st.workers[id]
	sub, sOk := st.subs[id]
	st.mu.Unlock()

	if !wOk || !sOk {
		broadcastError(h, fmt.Sprintf("Worker %s not found", id))
		return
	}

	msgs, err := sub.Fetch(batchSize, nats.MaxWait(2*time.Second))
	if err != nil && len(msgs) == 0 {
		if err == nats.ErrTimeout {
			evt := map[string]any{
				"type": "fetch_complete",
				"payload": map[string]any{
					"workerId":   id,
					"workerName": worker.Name,
					"count":      0,
					"message":    "No tasks available",
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
		if meta != nil {
			seq = meta.Sequence.Stream
		}

		pm := &pendingMsg{
			Msg:      msg,
			ID:       msgID,
			Sequence: seq,
			Subject:  msg.Subject,
			Data:     string(msg.Data),
			WorkerID: id,
		}
		st.pending[msgID] = pm

		pulled = append(pulled, map[string]any{
			"msgId":     msgID,
			"sequence":  seq,
			"subject":   msg.Subject,
			"data":      string(msg.Data),
			"workerId":  id,
			"timestamp": time.Now().UnixMilli(),
		})
	}
	worker.Processing += len(msgs)
	st.mu.Unlock()

	evt := map[string]any{
		"type": "tasks_fetched",
		"payload": map[string]any{
			"workerId":   id,
			"workerName": worker.Name,
			"count":      len(pulled),
			"messages":   pulled,
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
	workerID := pm.WorkerID
	seq := pm.Sequence
	delete(st.pending, msgID)
	worker := st.workers[workerID]
	if worker != nil {
		worker.Processing--
		worker.Completed++
	}
	workerName := ""
	if worker != nil {
		workerName = worker.Name
	}
	st.mu.Unlock()

	if err := msg.Ack(); err != nil {
		broadcastError(h, fmt.Sprintf("Ack failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "task_acked",
		"payload": map[string]any{
			"msgId":      msgID,
			"sequence":   seq,
			"workerId":   workerID,
			"workerName": workerName,
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
	workerID := pm.WorkerID
	seq := pm.Sequence
	delete(st.pending, msgID)
	worker := st.workers[workerID]
	if worker != nil {
		worker.Processing--
		worker.Failed++
	}
	workerName := ""
	if worker != nil {
		workerName = worker.Name
	}
	st.mu.Unlock()

	if err := msg.Nak(); err != nil {
		broadcastError(h, fmt.Sprintf("Nak failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "task_naked",
		"payload": map[string]any{
			"msgId":      msgID,
			"sequence":   seq,
			"workerId":   workerID,
			"workerName": workerName,
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
			"name":      info.Config.Name,
			"subjects":  info.Config.Subjects,
			"retention": "WorkQueue",
			"messages":  info.State.Msgs,
			"bytes":     info.State.Bytes,
			"firstSeq":  info.State.FirstSeq,
			"lastSeq":   info.State.LastSeq,
			"consumers": info.State.Consumers,
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
