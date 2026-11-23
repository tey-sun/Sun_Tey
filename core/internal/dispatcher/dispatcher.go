package dispatcher

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// Handler processes a request body and returns a result (JSON-marshalled by the
// transport). ctx is carried via closures by the registrant.
type Handler func(body []byte) (any, error)

// Dispatcher is an in-process method registry keyed by "METHOD path", mirroring
// GoPeed's single in-process rest.Dispatch: the desktop front-end (Tauri FFI)
// and the loopback HTTP server (browser extension) share ONE dispatcher, so the
// engine has no network boundary of its own.
type Dispatcher struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func New() *Dispatcher {
	return &Dispatcher{handlers: map[string]Handler{}}
}

func (d *Dispatcher) Register(method, path string, h Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[method+" "+path] = h
}

// Routes lists registered routes (sorted) — handy for /api/v1 introspection.
func (d *Dispatcher) Routes() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.handlers))
	for k := range d.handlers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Has reports whether method+path is registered (transport-side probes for
// optional endpoints).
func (d *Dispatcher) Has(method, path string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.handlers[method+" "+path]
	return ok
}

// Call dispatches by method+path. Unknown routes yield a 404-style error.
func (d *Dispatcher) Call(method, path string, body []byte) (any, error) {
	d.mu.RLock()
	h, ok := d.handlers[method+" "+path]
	d.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no route: %s %s", method, path)
	}
	return h(body)
}

// CallJSON unmarshals a typed request, dispatches, returns the typed result.
func (d *Dispatcher) CallJSON(method, path string, req any, out any) error {
	var body []byte
	if req != nil {
		b, err := json.Marshal(req)
		if err != nil {
			return err
		}
		body = b
	}
	res, err := d.Call(method, path, body)
	if err != nil {
		return err
	}
	if out != nil && res != nil {
		b, err := json.Marshal(res)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, out)
	}
	return nil
}
