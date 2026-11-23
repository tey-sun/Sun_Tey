package httpx

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Process-wide download bandwidth limit (IDM-style "限速"). The token bucket
// is global on purpose: every leg of every task draws from the same budget,
// so the aggregate line rate is what the user configured, not per-connection.
// 0 (default) means unlimited. BT traffic is engineered separately and is
// not throttled by this knob.
var globalRate atomic.Int64 // bytes per second

// SetRate updates the global cap (0 = unlimited). Safe to call at any time;
// in-flight reads pick the new rate up on their next chunk.
func SetRate(bytesPerSec int64) {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	globalRate.Store(bytesPerSec)
}

// Rate returns the current global cap in bytes/s (0 = unlimited).
func Rate() int64 { return globalRate.Load() }

var (
	limMu    sync.Mutex
	limToks  float64
	limLast  time.Time
	limReset = false // bucket was emptied on the last rate change
)

// consume blocks until n bytes' worth of budget is available, then takes it.
func consume(n int64, done <-chan struct{}) {
	if n <= 0 {
		return
	}
	for {
		r := globalRate.Load()
		if r <= 0 {
			return
		}
		limMu.Lock()
		now := time.Now()
		if limLast.IsZero() || limReset {
			limLast = now
			limToks = 0
			limReset = false
		} else {
			limToks += now.Sub(limLast).Seconds() * float64(r)
			limLast = now
			// burst cap: half a second of the current rate, floored at one
			// transport bufio round (16 KiB) so tiny low-rate configs still
			// let the first 4 KiB chunk through.
			burst := float64(r) / 2
			if floor := float64(16 << 10); burst < floor {
				burst = floor
			}
			if limToks > burst {
				limToks = burst
			}
		}
		if limToks >= float64(n) {
			limToks -= float64(n)
			limMu.Unlock()
			return
		}
		wait := time.Duration(float64(time.Second) * (float64(n) - limToks) / float64(r))
		limMu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		for wait > 0 {
			d := wait
			if d > 150*time.Millisecond {
				d = 150 * time.Millisecond
			}
			if done != nil {
				select {
				case <-done:
					return
				case <-time.After(d):
				}
			} else {
				time.Sleep(d)
			}
			wait -= d
			_ = d
		}
	}
}

// SetRate-adjacent: empty the bucket so an old, high rate cannot lend a huge
// burst to the new one (and vice versa). Called from the manager on config PUT.
func ResetRateBucket() {
	limMu.Lock()
	limToks = 0
	limLast = time.Time{}
	limReset = true
	limMu.Unlock()
}

// throttle wraps a response-body reader and draws from the global bucket
// after every chunk that actually arrived. Wired via the client transport,
// so it covers range legs, sequential streams and HLS segment fetches with
// a single injection point and no changes in the read loops themselves.
// done is closed when the owning request's context is done: token-bucket
// sleeps must be interruptible, or pause/stop under a rate cap hangs until
// every leg finished sleeping its whole wait (measured 5-8s, IDM stops
// instantly). nil done = plain sleep (unit tests, uncancellable reads).
type throttle struct {
	r    io.Reader
	done <-chan struct{}
}

func (t *throttle) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		consume(int64(n), t.done)
	}
	return n, err
}

// Close keeps the response-body contract: the wrapped reader owns the
// connection's lifecycle, so pass the close through.
func (t *throttle) Close() error {
	if c, ok := t.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// newThrottle rate-limits r, interrupting its bucket sleeps when ctx is
// done. It returns an io.ReadCloser so it can replace resp.Body directly.
func newThrottle(ctx context.Context, r io.Reader) *throttle {
	t := &throttle{r: r}
	if ctx != nil && ctx.Done() != nil {
		done := make(chan struct{})
		context.AfterFunc(ctx, func() { close(done) })
		t.done = done
	}
	return t
}
