package testhelper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// WaitTimeout returns a duration broadly appropriate for waiting on an expected
// event in a test.
func WaitTimeout() time.Duration {
	return 3 * time.Second
}

// WaitOrTimeout tries to wait on the given channel for a value to come through,
// and returns it if one does, but times out after a reasonable amount of time.
// Useful to guarantee that test cases don't hang forever, even in the event of
// something wrong.
func WaitOrTimeout[T any](tb testing.TB, waitChan <-chan T) T {
	tb.Helper()

	timeout := WaitTimeout()

	select {
	case value := <-waitChan:
		return value
	case <-time.After(timeout):
		require.FailNowf(tb, "WaitOrTimeout timed out",
			"WaitOrTimeout timed out after waiting %s", timeout)
	}
	return *new(T) // unreachable
}
