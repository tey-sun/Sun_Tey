package httpx

import "testing"

// skipLong keeps the wall-clock-bound tests out of `go test -short`.
//
// Blueprint §7 budgets the whole -short suite at 90 s; four httpx tests
// (idle watchdog, bounded give-up, rate-limited reads, small-drop churn) burned
// 41 s of the 56 s package time just waiting on real timers [V] - and that wait
// IS the behaviour under test, so shortening it would only make the test lie.
// They still run on every plain `go test ./...` and on CI (the windows job runs
// the full suite); -short stays the fast inner loop and -short -race stays the
// CI race detector.
func skipLong(t *testing.T, why string) {
	t.Helper()
	if testing.Short() {
		t.Skip("long: " + why)
	}
}
