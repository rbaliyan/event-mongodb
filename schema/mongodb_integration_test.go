package schema

import (
	"context"
	"errors"
	"testing"
	"time"

	evtschema "github.com/rbaliyan/event/v3/schema"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupSchemaIntegrationTest connects to MongoDB and returns a MongoProvider
// over a unique per-test database. Tests skip when MongoDB is unavailable or
// when -short is set. Teardown (provider close + db drop) is registered via
// t.Cleanup so it runs even if a test panics.
func setupSchemaIntegrationTest(t *testing.T, publisher func(context.Context, evtschema.SchemaChangeEvent) error) (*MongoProvider, func()) {
	t.Helper()

	client := mongotest.Connect(t)

	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_schema"))

	p, err := NewMongoProvider(db, publisher)
	if err != nil {
		_ = db.Drop(context.Background())
		t.Fatalf("NewMongoProvider: %v", err)
	}

	cleanup := func() {
		_ = p.Close()
		_ = db.Drop(context.Background())
	}
	t.Cleanup(cleanup)

	return p, cleanup
}

func TestIntegrationSetGetDelete(t *testing.T) {
	p, _ := setupSchemaIntegrationTest(t, nil)

	ctx := context.Background()

	// Missing schema returns (nil, nil).
	got, err := p.Get(ctx, "orders.created")
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing schema, got %v", got)
	}

	schema := &evtschema.EventSchema{
		Name:         "orders.created",
		Version:      1,
		Description:  "order events",
		SubTimeout:   2 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 500 * time.Millisecond,
		Metadata:     map[string]string{"team": "payments"},
	}
	if err := p.Set(ctx, schema); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err = p.Get(ctx, "orders.created")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected schema after Set")
	}
	if got.Version != 1 || got.SubTimeout != 2*time.Second || got.MaxRetries != 3 {
		t.Fatalf("schema fields mismatch: %+v", got)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Fatal("expected timestamps to be set")
	}

	if err := p.Delete(ctx, "orders.created"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = p.Get(ctx, "orders.created")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got %v", got)
	}
}

func TestIntegrationSetVersionDowngrade(t *testing.T) {
	p, _ := setupSchemaIntegrationTest(t, nil)

	ctx := context.Background()

	if err := p.Set(ctx, &evtschema.EventSchema{Name: "evt", Version: 5}); err != nil {
		t.Fatalf("Set v5: %v", err)
	}

	err := p.Set(ctx, &evtschema.EventSchema{Name: "evt", Version: 3})
	if !errors.Is(err, evtschema.ErrVersionDowngrade) {
		t.Fatalf("expected ErrVersionDowngrade, got %v", err)
	}

	// Same version is allowed (upsert).
	if err := p.Set(ctx, &evtschema.EventSchema{Name: "evt", Version: 5}); err != nil {
		t.Fatalf("Set same version: %v", err)
	}
	// Higher version is allowed.
	if err := p.Set(ctx, &evtschema.EventSchema{Name: "evt", Version: 6}); err != nil {
		t.Fatalf("Set higher version: %v", err)
	}
}

func TestIntegrationSetPreservesCreatedAt(t *testing.T) {
	p, _ := setupSchemaIntegrationTest(t, nil)

	ctx := context.Background()

	if err := p.Set(ctx, &evtschema.EventSchema{Name: "evt", Version: 1}); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	first, err := p.Get(ctx, "evt")
	if err != nil {
		t.Fatalf("Get v1: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	if err := p.Set(ctx, &evtschema.EventSchema{Name: "evt", Version: 2}); err != nil {
		t.Fatalf("Set v2: %v", err)
	}
	second, err := p.Get(ctx, "evt")
	if err != nil {
		t.Fatalf("Get v2: %v", err)
	}

	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("CreatedAt should be preserved: first=%v second=%v", first.CreatedAt, second.CreatedAt)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) {
		t.Fatalf("UpdatedAt should advance: first=%v second=%v", first.UpdatedAt, second.UpdatedAt)
	}
}

func TestIntegrationList(t *testing.T) {
	p, _ := setupSchemaIntegrationTest(t, nil)

	ctx := context.Background()

	empty, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty list, got %d", len(empty))
	}

	for _, name := range []string{"c.evt", "a.evt", "b.evt"} {
		if err := p.Set(ctx, &evtschema.EventSchema{Name: name, Version: 1}); err != nil {
			t.Fatalf("Set %s: %v", name, err)
		}
	}

	list, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 schemas, got %d", len(list))
	}
	// List sorts by _id ascending.
	want := []string{"a.evt", "b.evt", "c.evt"}
	for i, w := range want {
		if list[i].Name != w {
			t.Fatalf("list[%d] = %q, want %q", i, list[i].Name, w)
		}
	}
}

func TestIntegrationPublisherInvokedOnSet(t *testing.T) {
	var got evtschema.SchemaChangeEvent
	called := 0
	publisher := func(_ context.Context, ev evtschema.SchemaChangeEvent) error {
		got = ev
		called++
		return nil
	}

	p, _ := setupSchemaIntegrationTest(t, publisher)

	ctx := context.Background()
	if err := p.Set(ctx, &evtschema.EventSchema{Name: "evt", Version: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if called != 1 {
		t.Fatalf("expected publisher called once, got %d", called)
	}
	if got.EventName != "evt" || got.Version != 2 {
		t.Fatalf("publisher event mismatch: %+v", got)
	}
}

func TestIntegrationWatchReceivesChange(t *testing.T) {
	p, _ := setupSchemaIntegrationTest(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := p.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := p.Set(ctx, &evtschema.EventSchema{Name: "watched", Version: 7}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventName != "watched" || ev.Version != 7 {
			t.Fatalf("unexpected change event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch notification")
	}

	// Cancelling the context removes the watcher and closes the channel.
	cancel()
	select {
	case _, open := <-ch:
		if open {
			// May receive a buffered value first; drain then expect close.
			select {
			case _, open2 := <-ch:
				if open2 {
					t.Fatal("expected channel to close after context cancel")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for channel close")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestIntegrationEnsureIndexes(t *testing.T) {
	p, _ := setupSchemaIntegrationTest(t, nil)

	if err := p.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
}
