package kv

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
		ID:          "kv",
		Title:       "9. Key-Value Store",
		Description: "JetStream-backed key-value store with watchers, history, and compare-and-swap",
		Category:    "JetStream",
		Order:       9,
	}, Setup)
}

type state struct {
	mu       sync.Mutex
	buckets  map[string]nats.KeyValue
	watchers map[string]nats.KeyWatcher
	watchSeq int
}

type command struct {
	Action    string `json:"action"`
	Bucket    string `json:"bucket,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Revision  uint64 `json:"revision,omitempty"`
	History   int64  `json:"history,omitempty"`
	TTL       int    `json:"ttl,omitempty"`
	WatcherID string `json:"watcherId,omitempty"`
	WatchKey  string `json:"watchKey,omitempty"`
}

func Setup(mux *http.ServeMux, nc *nats.Conn) {
	h := hub.New()
	st := &state{
		buckets:  make(map[string]nats.KeyValue),
		watchers: make(map[string]nats.KeyWatcher),
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("kv: failed to get JetStream context: %v", err)
		return
	}

	h.OnMessage = func(raw []byte) {
		var cmd command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			log.Printf("kv: bad command: %v", err)
			return
		}
		switch cmd.Action {
		case "create_bucket":
			handleCreateBucket(st, h, js, cmd)
		case "put":
			handlePut(st, h, cmd)
		case "get":
			handleGet(st, h, cmd)
		case "delete":
			handleDelete(st, h, cmd)
		case "keys":
			handleKeys(st, h, cmd)
		case "history":
			handleHistory(st, h, cmd)
		case "watch":
			handleWatch(st, h, cmd)
		case "stop_watch":
			handleStopWatch(st, h, cmd)
		case "cas_update":
			handleCASUpdate(st, h, cmd)
		case "bucket_info":
			handleBucketInfo(st, h, cmd)
		}
	}

	mux.HandleFunc("/demo/kv/ws", h.ServeWS)
}

func handleCreateBucket(st *state, h *hub.Hub, js nats.JetStreamContext, cmd command) {
	name := cmd.Bucket
	if name == "" {
		name = "DEMO_KV"
	}
	history := cmd.History
	if history <= 0 {
		history = 5
	}
	var ttl time.Duration
	if cmd.TTL > 0 {
		ttl = time.Duration(cmd.TTL) * time.Second
	}

	kvBucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  name,
		History: uint8(history),
		TTL:     ttl,
	})
	if err != nil {
		broadcastError(h, fmt.Sprintf("Failed to create bucket: %v", err))
		return
	}

	st.mu.Lock()
	st.buckets[name] = kvBucket
	st.mu.Unlock()

	status, _ := kvBucket.Status()
	backingStream := ""
	if status != nil {
		backingStream = status.BackingStore()
	}

	evt := map[string]any{
		"type": "bucket_created",
		"payload": map[string]any{
			"bucket":        name,
			"history":       history,
			"ttl":           cmd.TTL,
			"backingStream": backingStream,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handlePut(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	rev, err := kvBucket.Put(cmd.Key, []byte(cmd.Value))
	if err != nil {
		broadcastError(h, fmt.Sprintf("Put failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "kv_put",
		"payload": map[string]any{
			"bucket":   cmd.Bucket,
			"key":      cmd.Key,
			"value":    cmd.Value,
			"revision": rev,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleGet(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	entry, err := kvBucket.Get(cmd.Key)
	if err != nil {
		evt := map[string]any{
			"type": "kv_get",
			"payload": map[string]any{
				"bucket": cmd.Bucket,
				"key":    cmd.Key,
				"found":  false,
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
		return
	}

	evt := map[string]any{
		"type": "kv_get",
		"payload": map[string]any{
			"bucket":    cmd.Bucket,
			"key":       entry.Key(),
			"value":     string(entry.Value()),
			"revision":  entry.Revision(),
			"operation": kvOpString(entry.Operation()),
			"created":   entry.Created().Format(time.RFC3339),
			"found":     true,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleDelete(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	err := kvBucket.Delete(cmd.Key)
	if err != nil {
		broadcastError(h, fmt.Sprintf("Delete failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "kv_deleted",
		"payload": map[string]any{
			"bucket": cmd.Bucket,
			"key":    cmd.Key,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleKeys(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	keys, err := kvBucket.Keys()
	if err != nil {
		// ErrNoKeysFound means empty bucket, not a real error
		keys = []string{}
	}

	evt := map[string]any{
		"type": "kv_keys",
		"payload": map[string]any{
			"bucket": cmd.Bucket,
			"keys":   keys,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleHistory(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	entries, err := kvBucket.History(cmd.Key)
	if err != nil {
		broadcastError(h, fmt.Sprintf("History failed: %v", err))
		return
	}

	result := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		result = append(result, map[string]any{
			"revision":  e.Revision(),
			"value":     string(e.Value()),
			"operation": kvOpString(e.Operation()),
			"created":   e.Created().Format(time.RFC3339),
		})
	}

	evt := map[string]any{
		"type": "kv_history",
		"payload": map[string]any{
			"bucket":  cmd.Bucket,
			"key":     cmd.Key,
			"entries": result,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleWatch(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	var watcher nats.KeyWatcher
	var err error
	watchKey := cmd.WatchKey
	if watchKey == "" || watchKey == ">" {
		watcher, err = kvBucket.WatchAll()
		watchKey = ">"
	} else {
		watcher, err = kvBucket.Watch(watchKey)
	}
	if err != nil {
		broadcastError(h, fmt.Sprintf("Watch failed: %v", err))
		return
	}

	st.mu.Lock()
	st.watchSeq++
	watcherID := fmt.Sprintf("watcher-%d", st.watchSeq)
	st.watchers[watcherID] = watcher
	st.mu.Unlock()

	evt := map[string]any{
		"type": "watcher_created",
		"payload": map[string]any{
			"watcherId": watcherID,
			"bucket":    cmd.Bucket,
			"watchKey":  watchKey,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)

	go func() {
		for entry := range watcher.Updates() {
			if entry == nil {
				evt := map[string]any{
					"type": "kv_watch_init_done",
					"payload": map[string]any{
						"watcherId": watcherID,
					},
				}
				data, _ := json.Marshal(evt)
				h.BroadcastRaw(data)
				continue
			}
			evt := map[string]any{
				"type": "kv_watch_update",
				"payload": map[string]any{
					"watcherId": watcherID,
					"bucket":    cmd.Bucket,
					"key":       entry.Key(),
					"value":     string(entry.Value()),
					"revision":  entry.Revision(),
					"operation": kvOpString(entry.Operation()),
					"created":   entry.Created().Format(time.RFC3339),
				},
			}
			data, _ := json.Marshal(evt)
			h.BroadcastRaw(data)
		}
	}()
}

func handleStopWatch(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	w, ok := st.watchers[cmd.WatcherID]
	if ok {
		w.Stop()
		delete(st.watchers, cmd.WatcherID)
	}
	st.mu.Unlock()

	if ok {
		evt := map[string]any{
			"type": "watcher_stopped",
			"payload": map[string]any{
				"watcherId": cmd.WatcherID,
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
	}
}

func handleCASUpdate(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	newRev, err := kvBucket.Update(cmd.Key, []byte(cmd.Value), cmd.Revision)
	if err != nil {
		evt := map[string]any{
			"type": "kv_cas_conflict",
			"payload": map[string]any{
				"bucket":           cmd.Bucket,
				"key":              cmd.Key,
				"expectedRevision": cmd.Revision,
				"message":          fmt.Sprintf("CAS conflict: expected rev #%d but update failed -- %v", cmd.Revision, err),
			},
		}
		data, _ := json.Marshal(evt)
		h.BroadcastRaw(data)
		return
	}

	evt := map[string]any{
		"type": "kv_cas_success",
		"payload": map[string]any{
			"bucket":      cmd.Bucket,
			"key":         cmd.Key,
			"value":       cmd.Value,
			"oldRevision": cmd.Revision,
			"newRevision": newRev,
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func handleBucketInfo(st *state, h *hub.Hub, cmd command) {
	st.mu.Lock()
	kvBucket, ok := st.buckets[cmd.Bucket]
	st.mu.Unlock()
	if !ok {
		broadcastError(h, fmt.Sprintf("Bucket %q not found", cmd.Bucket))
		return
	}

	status, err := kvBucket.Status()
	if err != nil {
		broadcastError(h, fmt.Sprintf("Bucket info failed: %v", err))
		return
	}

	evt := map[string]any{
		"type": "bucket_info",
		"payload": map[string]any{
			"bucket":        status.Bucket(),
			"history":       status.History(),
			"ttl":           int(status.TTL().Seconds()),
			"values":        status.Values(),
			"bytes":         status.Bytes(),
			"backingStream": status.BackingStore(),
		},
	}
	data, _ := json.Marshal(evt)
	h.BroadcastRaw(data)
}

func kvOpString(op nats.KeyValueOp) string {
	switch op {
	case nats.KeyValuePut:
		return "PUT"
	case nats.KeyValueDelete:
		return "DEL"
	case nats.KeyValuePurge:
		return "PURGE"
	default:
		return "UNKNOWN"
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
