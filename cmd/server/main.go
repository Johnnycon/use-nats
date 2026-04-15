package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"nats-lab/internal/demos"

	// Demo auto-registration
	_ "nats-lab/internal/demos/consumers"
	_ "nats-lab/internal/demos/dedup"
	_ "nats-lab/internal/demos/fanout"
	_ "nats-lab/internal/demos/kv"
	_ "nats-lab/internal/demos/pubsub"
	_ "nats-lab/internal/demos/queuegroup"
	_ "nats-lab/internal/demos/reqreply"
	_ "nats-lab/internal/demos/scatter"
	_ "nats-lab/internal/demos/streams"
	_ "nats-lab/internal/demos/wildcards"
	_ "nats-lab/internal/demos/workqueue"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func main() {
	log.SetFlags(0)

	// Step 1: Start embedded NATS server with JetStream
	fmt.Println("  [1/4] Starting embedded NATS server...")
	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      4222,
		JetStream: true,
		StoreDir:  filepath.Join(os.TempDir(), "nats-lab-js"),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		log.Fatalf("Failed to create NATS server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		log.Fatal("NATS server not ready after 5s")
	}
	fmt.Println("         NATS listening on :4222 (JetStream enabled)")

	// Step 2: Connect to NATS
	fmt.Println("  [2/4] Connecting to NATS...")
	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	fmt.Println("         Connected")

	// Step 3: Verify templates exist
	fmt.Println("  [3/4] Loading templates...")
	if _, err := template.ParseFiles("web/templates/layout.html", "web/templates/dashboard.html"); err != nil {
		log.Fatalf("Failed to parse templates: %v", err)
	}
	fmt.Println("         OK")

	// Step 4: Set up HTTP routes
	fmt.Println("  [4/4] Setting up HTTP routes...")
	mux := http.NewServeMux()

	// Dashboard
	mux.HandleFunc("/", handleDashboard)

	// Demo pages
	mux.HandleFunc("/demo/{id}", handleDemoPage)

	// Static files
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))

	// Register all demo WebSocket + API routes
	demos.SetupAll(mux, nc)

	fmt.Println("         Routes registered")
	fmt.Println()
	fmt.Println("  Ready! Open http://localhost:8092")
	fmt.Println()

	if err := http.ListenAndServe(":8092", mux); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// renderTemplate parses layout.html together with a page-specific template
// and executes the "layout" block. This avoids template cloning issues and
// ensures each page gets a clean "content" definition.
func renderTemplate(w http.ResponseWriter, page string, data any) {
	t, err := template.ParseFiles("web/templates/layout.html", "web/templates/"+page+".html")
	if err != nil {
		log.Printf("template parse error for %s: %v", page, err)
		http.Error(w, "Internal Server Error", 500)
		return
	}
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		log.Printf("template render error for %s: %v", page, err)
		http.Error(w, "Internal Server Error", 500)
	}
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data := struct {
		Title string
		Demos []demos.Demo
	}{
		Title: "NATS Learning Lab",
		Demos: demos.All(),
	}
	renderTemplate(w, "dashboard", data)
}

func handleDemoPage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Find the demo
	var found *demos.Demo
	for _, d := range demos.All() {
		if d.ID == id {
			d := d
			found = &d
			break
		}
	}
	if found == nil {
		http.NotFound(w, r)
		return
	}

	data := struct {
		Title string
		Demo  demos.Demo
	}{
		Title: found.Title + " — NATS Lab",
		Demo:  *found,
	}
	renderTemplate(w, id, data)
}
