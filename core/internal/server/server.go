package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tey-sun/suntey/core/internal/dispatcher"
	"github.com/tey-sun/suntey/core/internal/download/event"
)

// New builds the loopback HTTP handler: REST endpoints proxy the in-process
// dispatcher, /events streams Bus events as SSE. Bound to 127.0.0.1 by the
// caller; every request must carry X-SunTey-Token (Tauri shell / extension).
func New(d *dispatcher.Dispatcher, bus *event.Bus, token string) http.Handler {
	mux := http.NewServeMux()

	proxy := func(method, path string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !auth(r, token) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			var body []byte
			if r.Body != nil {
				var err error
				body, err = readAll(r)
				if err != nil {
					writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
					return
				}
			}
			out, err := d.Call(method, path, body)
			if err != nil {
				writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
				return
			}
			writeJSON(w, 200, map[string]any{"ok": true, "data": out})
		}
	}

	mux.HandleFunc("POST /api/v1/resolve", proxy("POST", "/resolve"))
	mux.HandleFunc("POST /api/v1/tasks", proxy("POST", "/tasks"))
	mux.HandleFunc("POST /api/v1/tasks/start", proxy("POST", "/tasks/start"))
	mux.HandleFunc("POST /api/v1/tasks/stop", proxy("POST", "/tasks/stop"))
	mux.HandleFunc("GET /api/v1/tasks", proxy("GET", "/tasks"))
	mux.HandleFunc("DELETE /api/v1/tasks", proxy("DELETE", "/tasks"))
	mux.HandleFunc("GET /api/v1/version", proxy("GET", "/version"))

	// Handshake is the token-free probe for the browser extension: the popup
	// can tell "core is running, token missing" from "core not on this port"
	// without ever seeing the token itself.
	mux.HandleFunc("GET /api/v1/handshake", func(w http.ResponseWriter, r *http.Request) {
		out, err := d.Call("GET", "/version", nil)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "data": out})
	})
	mux.HandleFunc("GET /api/v1/bt/stats", proxy("GET", "/bt/stats"))
	mux.HandleFunc("POST /api/v1/bt/torrent", proxy("POST", "/bt/torrent"))
	mux.HandleFunc("POST /api/v1/bt/selection", proxy("POST", "/bt/selection"))
	// Query-string routes fold their params into the dispatcher payload here so
	// the handler bodies stay transport-agnostic.
	mux.HandleFunc("GET /api/v1/bt/files", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r, token) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := json.Marshal(map[string]any{"id": r.URL.Query().Get("id")})
		out, err := d.Call("GET", "/bt/files", body)
		if err != nil {
			writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "data": out})
	})
	mux.HandleFunc("GET /api/v1/config", proxy("GET", "/config"))
	mux.HandleFunc("PUT /api/v1/config", proxy("PUT", "/config"))
	mux.HandleFunc("POST /api/v1/shutdown", proxy("POST", "/shutdown"))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// SSE event stream for the front-end (task progress/status/done).
	mux.HandleFunc("GET /api/v1/events", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r, token) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch, cancel := bus.Subscribe(256)
		defer cancel()

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		w.Write([]byte(":suntey\n\n"))
		flusher.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case e := <-ch:
				b, _ := json.Marshal(e)
				fmt.Fprintf(w, "data: %s\n\n", b)
				flusher.Flush()
			case <-ticker.C:
				w.Write([]byte(":ka\n\n"))
				flusher.Flush()
			}
		}
	})

	return cors(mux)
}

func auth(r *http.Request, token string) bool {
	if token == "" {
		return true // dev mode without token
	}
	if r.Header.Get("X-SunTey-Token") == token {
		return true
	}
	// SSE (EventSource) cannot set headers; token via query is the standard escape.
	return r.URL.Query().Get("token") == token
}

// cors lets the loopback serve the Tauri webview (tauri:// scheme) and the MV3
// extension (chrome-extension:// ids) without credentials — token gates access.
func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (strings.HasPrefix(origin, "http://127.0.0.1") ||
			strings.HasPrefix(origin, "http://localhost") ||
			strings.HasPrefix(origin, "tauri://") ||
			strings.HasPrefix(origin, "chrome-extension://") ||
			strings.HasPrefix(origin, "http://tauri.localhost")) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-SunTey-Token")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func readAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}
