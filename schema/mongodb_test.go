package schema

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	evtschema "github.com/rbaliyan/event/v3/schema"
)

// lazyDatabase returns a *mongo.Database backed by a lazily-connected client.
// No server round-trips occur, so this is safe for pure unit tests.
func lazyDatabase(t *testing.T) *mongo.Database {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Fatalf("lazy connect: %v", err)
	}
	return client.Database("unit_test_db")
}

func TestNewMongoProvider_NilDB(t *testing.T) {
	p, err := NewMongoProvider(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil db, got nil")
	}
	if p != nil {
		t.Fatalf("expected nil provider on error, got %v", p)
	}
}

func TestNewMongoProvider_Defaults(t *testing.T) {
	db := lazyDatabase(t)
	p, err := NewMongoProvider(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
	if p.Collection() == nil {
		t.Fatal("expected collection to be set")
	}
	if p.Collection().Name() != "event_schemas" {
		t.Fatalf("expected default collection event_schemas, got %q", p.Collection().Name())
	}
	if p.closeChan == nil {
		t.Fatal("expected closeChan to be initialized")
	}
}

func TestWithCollection(t *testing.T) {
	db := lazyDatabase(t)
	p, err := NewMongoProvider(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ret := p.WithCollection("custom_schemas")
	if ret != p {
		t.Fatal("WithCollection should return the same provider for chaining")
	}
	if p.Collection().Name() != "custom_schemas" {
		t.Fatalf("expected collection custom_schemas, got %q", p.Collection().Name())
	}
}

func TestIndexes(t *testing.T) {
	db := lazyDatabase(t)
	p, err := NewMongoProvider(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	idx := p.Indexes()
	if len(idx) != 1 {
		t.Fatalf("expected 1 index, got %d", len(idx))
	}
	keys, ok := idx[0].Keys.(bson.D)
	if !ok {
		t.Fatalf("expected bson.D keys, got %T", idx[0].Keys)
	}
	if len(keys) != 1 || keys[0].Key != "updated_at" || keys[0].Value != 1 {
		t.Fatalf("unexpected index keys: %#v", keys)
	}
}

func TestClose_Idempotent(t *testing.T) {
	db := lazyDatabase(t)
	p, err := NewMongoProvider(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if !p.closed {
		t.Fatal("expected provider marked closed")
	}
	// Second close must be a no-op (must not panic on double channel close).
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestOperationsAfterClose verifies the pure guard logic: once closed, the
// in-memory checks short-circuit with ErrProviderClosed before any server I/O.
func TestOperationsAfterClose(t *testing.T) {
	db := lazyDatabase(t)
	p, err := NewMongoProvider(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx := context.Background()

	if _, err := p.Get(ctx, "x"); err != evtschema.ErrProviderClosed {
		t.Fatalf("Get after close: want ErrProviderClosed, got %v", err)
	}
	if _, err := p.List(ctx); err != evtschema.ErrProviderClosed {
		t.Fatalf("List after close: want ErrProviderClosed, got %v", err)
	}
	if err := p.Delete(ctx, "x"); err != evtschema.ErrProviderClosed {
		t.Fatalf("Delete after close: want ErrProviderClosed, got %v", err)
	}
	if _, err := p.Watch(ctx); err != evtschema.ErrProviderClosed {
		t.Fatalf("Watch after close: want ErrProviderClosed, got %v", err)
	}
	// Set first validates the schema, so use a valid one to reach the guard.
	err = p.Set(ctx, &evtschema.EventSchema{Name: "x", Version: 1})
	if err != evtschema.ErrProviderClosed {
		t.Fatalf("Set after close: want ErrProviderClosed, got %v", err)
	}
}

// TestSetValidationError verifies Set rejects an invalid schema before any
// server interaction (pure validation path).
func TestSetValidationError(t *testing.T) {
	db := lazyDatabase(t)
	p, err := NewMongoProvider(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Empty name -> ErrEmptyName from EventSchema.Validate.
	if err := p.Set(context.Background(), &evtschema.EventSchema{Version: 1}); err != evtschema.ErrEmptyName {
		t.Fatalf("expected ErrEmptyName, got %v", err)
	}
}

func TestSchemaRoundTripConversion(t *testing.T) {
	src := &evtschema.EventSchema{
		Name:              "orders.created",
		Version:           3,
		Description:       "order events",
		SubTimeout:        1500 * 1e6, // 1.5s in ns
		MaxRetries:        5,
		RetryBackoff:      250 * 1e6, // 250ms in ns
		EnableMonitor:     true,
		EnableIdempotency: true,
		EnablePoison:      true,
		Metadata:          map[string]string{"team": "payments"},
	}

	m := fromEventSchema(src)
	if m.Name != src.Name || m.Version != src.Version || m.Description != src.Description {
		t.Fatalf("identity fields not copied: %+v", m)
	}
	if m.SubTimeoutMs == nil || *m.SubTimeoutMs != 1500 {
		t.Fatalf("SubTimeoutMs = %v, want 1500", m.SubTimeoutMs)
	}
	if m.MaxRetries == nil || *m.MaxRetries != 5 {
		t.Fatalf("MaxRetries = %v, want 5", m.MaxRetries)
	}
	if m.RetryBackoffMs == nil || *m.RetryBackoffMs != 250 {
		t.Fatalf("RetryBackoffMs = %v, want 250", m.RetryBackoffMs)
	}

	back := m.toEventSchema()
	if back.Name != src.Name || back.Version != src.Version {
		t.Fatalf("round-trip identity mismatch: %+v", back)
	}
	if back.SubTimeout != src.SubTimeout {
		t.Fatalf("SubTimeout = %v, want %v", back.SubTimeout, src.SubTimeout)
	}
	if back.MaxRetries != src.MaxRetries {
		t.Fatalf("MaxRetries = %v, want %v", back.MaxRetries, src.MaxRetries)
	}
	if back.RetryBackoff != src.RetryBackoff {
		t.Fatalf("RetryBackoff = %v, want %v", back.RetryBackoff, src.RetryBackoff)
	}
	if !back.EnableMonitor || !back.EnableIdempotency || !back.EnablePoison {
		t.Fatalf("feature flags not preserved: %+v", back)
	}
	if back.Metadata["team"] != "payments" {
		t.Fatalf("metadata not preserved: %v", back.Metadata)
	}
}

func TestSchemaConversion_ZeroDurations(t *testing.T) {
	// Zero/negative durations and retries should produce nil pointers (omitted).
	src := &evtschema.EventSchema{Name: "x", Version: 1}
	m := fromEventSchema(src)
	if m.SubTimeoutMs != nil {
		t.Fatalf("expected nil SubTimeoutMs, got %v", *m.SubTimeoutMs)
	}
	if m.MaxRetries != nil {
		t.Fatalf("expected nil MaxRetries, got %v", *m.MaxRetries)
	}
	if m.RetryBackoffMs != nil {
		t.Fatalf("expected nil RetryBackoffMs, got %v", *m.RetryBackoffMs)
	}

	// And the reverse: nil pointers decode to zero values.
	back := m.toEventSchema()
	if back.SubTimeout != 0 || back.MaxRetries != 0 || back.RetryBackoff != 0 {
		t.Fatalf("expected zero values from nil pointers, got %+v", back)
	}
}
