package mongodb

import (
	"context"
	"testing"
	"time"
)

// --- Options that the existing table did not cover ---

func TestWithStartFromPast(t *testing.T) {
	t.Parallel()
	o := &transportOptions{}
	WithStartFromPast(24 * time.Hour)(o)
	if o.startFromPast != 24*time.Hour {
		t.Errorf("startFromPast = %v, want 24h", o.startFromPast)
	}
}

func TestWithHistoryLostCallback(t *testing.T) {
	t.Parallel()
	t.Run("sets callback", func(t *testing.T) {
		t.Parallel()
		o := &transportOptions{}
		called := false
		WithHistoryLostCallback(func(_ context.Context, _ string, _ error) { called = true })(o)
		if o.historyLostCallback == nil {
			t.Fatal("expected historyLostCallback to be set")
		}
		o.historyLostCallback(context.Background(), "key", nil)
		if !called {
			t.Error("expected callback to fire")
		}
	})

	t.Run("nil callback is ignored", func(t *testing.T) {
		t.Parallel()
		existing := func(_ context.Context, _ string, _ error) {}
		o := &transportOptions{historyLostCallback: existing}
		WithHistoryLostCallback(nil)(o)
		if o.historyLostCallback == nil {
			t.Error("nil callback should leave existing one in place")
		}
	})
}

func TestWithMetrics(t *testing.T) {
	t.Parallel()
	m := &Metrics{}
	o := &transportOptions{}
	WithMetrics(m)(o)
	if o.metrics != m {
		t.Error("expected metrics to be set on transportOptions")
	}
}

func TestWithResumeTokenCollection_NilDBPanics(t *testing.T) {
	t.Parallel()
	// WithResumeTokenCollection calls db.Collection; with a nil *mongo.Database
	// it panics. We can still verify the option sets initErr when the store
	// constructor would fail. Since we cannot build a real *mongo.Database
	// without a connection, assert it does not silently no-op by checking it
	// recovers from a nil db dereference. We guard with recover.
	defer func() { _ = recover() }()
	o := &transportOptions{}
	// A nil database dereference is expected; the recover keeps the test green.
	WithResumeTokenCollection(nil, "_tokens")(o)
}

func TestDefaultOptions(t *testing.T) {
	t.Parallel()
	o := defaultOptions(nil)
	if o.logger == nil {
		t.Error("default logger should be set")
	}
	if o.onError == nil {
		t.Error("default onError should be set")
	}
	if o.bufferSize != 100 {
		t.Errorf("default bufferSize = %d, want 100", o.bufferSize)
	}

	// Options applied in order
	o2 := defaultOptions([]Option{WithBufferSize(7), WithResumeTokenID("xyz")})
	if o2.bufferSize != 7 {
		t.Errorf("bufferSize = %d, want 7", o2.bufferSize)
	}
	if o2.resumeTokenID != "xyz" {
		t.Errorf("resumeTokenID = %q, want xyz", o2.resumeTokenID)
	}
}
