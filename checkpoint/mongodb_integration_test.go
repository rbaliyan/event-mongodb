package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupCheckpointIntegrationTest connects to MongoDB and returns a MongoStore
// over a unique per-test database. Tests skip when MongoDB is unavailable or
// when -short is set. Teardown is registered via t.Cleanup so it runs even if a
// test panics.
func setupCheckpointIntegrationTest(t *testing.T, opts ...MongoOption) (*MongoStore, func()) {
	t.Helper()

	client := mongotest.Connect(t)

	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_checkpoint"))
	coll := db.Collection("checkpoints")

	store, err := NewMongoStore(coll, opts...)
	if err != nil {
		_ = db.Drop(context.Background())
		t.Fatalf("NewMongoStore: %v", err)
	}

	cleanup := func() { _ = db.Drop(context.Background()) }
	t.Cleanup(cleanup)

	return store, cleanup
}

func TestIntegrationSaveLoadRoundTrip(t *testing.T) {
	s, _ := setupCheckpointIntegrationTest(t)

	ctx := context.Background()
	pos := time.Now().UTC().Truncate(time.Millisecond)

	if err := s.Save(ctx, "sub-1", pos); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load(ctx, "sub-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !got.Equal(pos) {
		t.Fatalf("position mismatch: want %v got %v", pos, got)
	}
}

func TestIntegrationLoadMissing(t *testing.T) {
	s, _ := setupCheckpointIntegrationTest(t)

	got, err := s.Load(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("Load missing should not error: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero time for missing checkpoint, got %v", got)
	}
}

func TestIntegrationSaveOverwrites(t *testing.T) {
	s, _ := setupCheckpointIntegrationTest(t)

	ctx := context.Background()
	first := time.Now().UTC().Truncate(time.Millisecond)
	second := first.Add(time.Hour)

	if err := s.Save(ctx, "sub-1", first); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	if err := s.Save(ctx, "sub-1", second); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	got, err := s.Load(ctx, "sub-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !got.Equal(second) {
		t.Fatalf("expected overwritten position %v, got %v", second, got)
	}
}

func TestIntegrationDeleteAndDeleteAll(t *testing.T) {
	s, _ := setupCheckpointIntegrationTest(t)

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	for _, id := range []string{"a", "b", "c"} {
		if err := s.Save(ctx, id, now); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}

	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := s.Load(ctx, "a")
	if err != nil {
		t.Fatalf("Load after delete: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero after delete, got %v", got)
	}

	if err := s.DeleteAll(ctx); err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	ids, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty list after DeleteAll, got %v", ids)
	}
}

func TestIntegrationListAndGetAll(t *testing.T) {
	s, _ := setupCheckpointIntegrationTest(t)

	ctx := context.Background()
	want := map[string]time.Time{
		"x": time.Now().UTC().Truncate(time.Millisecond),
		"y": time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond),
	}
	for id, pos := range want {
		if err := s.Save(ctx, id, pos); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}

	ids, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %v", ids)
	}

	all, err := s.GetAll(ctx)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 checkpoints, got %d", len(all))
	}
	for id, pos := range want {
		if got, ok := all[id]; !ok || !got.Equal(pos) {
			t.Fatalf("GetAll[%s]=%v ok=%v want %v", id, got, ok, pos)
		}
	}
}

func TestIntegrationGetCheckpointInfo(t *testing.T) {
	s, _ := setupCheckpointIntegrationTest(t)

	ctx := context.Background()

	missing, err := s.GetCheckpointInfo(ctx, "nope")
	if err != nil {
		t.Fatalf("GetCheckpointInfo missing: %v", err)
	}
	if missing != nil {
		t.Fatalf("expected nil info for missing checkpoint, got %v", missing)
	}

	pos := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.Save(ctx, "sub-info", pos); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := s.GetCheckpointInfo(ctx, "sub-info")
	if err != nil {
		t.Fatalf("GetCheckpointInfo: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil info")
	}
	if info.SubscriberID != "sub-info" {
		t.Fatalf("SubscriberID = %q", info.SubscriberID)
	}
	if !info.Position.Equal(pos) {
		t.Fatalf("Position mismatch: want %v got %v", pos, info.Position)
	}
	if info.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero UpdatedAt")
	}
}

func TestIntegrationEnsureIndexes(t *testing.T) {
	s, _ := setupCheckpointIntegrationTest(t, WithMongoTTL(time.Hour))

	if err := s.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
}
