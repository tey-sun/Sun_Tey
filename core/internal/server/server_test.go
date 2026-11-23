package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tey-sun/suntey/core/internal/dispatcher"
	"github.com/tey-sun/suntey/core/internal/download/event"
)

func newTestHandler(t *testing.T) *httptest.Server {
	t.Helper()
	d := dispatcher.New()
	d.Register("GET", "/version", func(body []byte) (any, error) {
		return map[string]any{"version": "0.2.0", "features": []string{"http", "bt", "hls"}}, nil
	})
	srv := httptest.NewServer(New(d, event.NewBus(), "s3cret"))
	t.Cleanup(srv.Close)
	return srv
}

// The browser extension popup must be able to distinguish "core running, token
// missing" from "nothing on this port" before the user pastes a token, so
// /handshake must answer without the token.
func TestHandshakeIsTokenFree(t *testing.T) {
	srv := newTestHandler(t)

	res, err := http.Get(srv.URL + "/api/v1/handshake")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("handshake without token: status %d, want 200", res.StatusCode)
	}
	var payload struct {
		OK   bool `json:"ok"`
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if !payload.OK || payload.Data.Version != "0.2.0" {
		t.Fatalf("handshake payload = %+v, want ok + version 0.2.0", payload)
	}
}

// Control: the authed surface must still reject the same token-free request.
func TestAuthedRoutesStillRejectNoToken(t *testing.T) {
	srv := newTestHandler(t)

	res, err := http.Get(srv.URL + "/api/v1/version")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /version without token: status %d, want 401", res.StatusCode)
	}

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/version", nil)
	req.Header.Set("X-SunTey-Token", "s3cret")
	res2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusOK {
		t.Fatalf("GET /version with token: status %d, want 200", res2.StatusCode)
	}
}
