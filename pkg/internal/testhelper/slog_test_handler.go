package testhelper

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
)

// NewLogger returns a new slog text logger that outputs to `t.Log`.
// This keeps test output well-formatted and helps differentiate it during parallel test runs.
func NewLogger(tb testing.TB, opts *slog.HandlerOptions) *slog.Logger {
	tb.Helper()

	var buf bytes.Buffer

	textHandler := slog.NewTextHandler(&buf, opts)

	return slog.New(&slogTestHandler{
		buf:   &buf,
		inner: textHandler,
		mu:    &sync.Mutex{},
		tb:    tb,
	})
}

type slogTestHandler struct {
	buf   *bytes.Buffer
	inner slog.Handler
	mu    *sync.Mutex
	tb    testing.TB
}

func (b *slogTestHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return b.inner.Enabled(ctx, level)
}

func (b *slogTestHandler) Handle(ctx context.Context, rec slog.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err := b.inner.Handle(ctx, rec)
	if err != nil {
		return err
	}

	output, err := io.ReadAll(b.buf)
	if err != nil {
		return err
	}

	// t.Log adds its own newline, so trim the one from slog.
	output = bytes.TrimSuffix(output, []byte("\n"))

	// Register as a helper, but it won't fix the log line callsite issue.
	// The log line will still show `logger.go` from slog's internals.
	// See explanation and discussion here: https://github.com/neilotoole/slogt#deficiency
	b.tb.Helper()

	b.tb.Log(string(output))

	return nil
}

func (b *slogTestHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &slogTestHandler{
		buf:   b.buf,
		inner: b.inner.WithAttrs(attrs),
		mu:    b.mu,
		tb:    b.tb,
	}
}

func (b *slogTestHandler) WithGroup(name string) slog.Handler {
	return &slogTestHandler{
		buf:   b.buf,
		inner: b.inner.WithGroup(name),
		mu:    b.mu,
		tb:    b.tb,
	}
}
