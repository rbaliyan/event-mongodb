package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupAckIntegrationTest connects to MongoDB (no replica-set requirement) and
// returns a per-test database. Tests are skipped when MongoDB is unavailable
// or when -short is set. Teardown is registered via t.Cleanup so it runs even
// if a test panics.
func setupAckIntegrationTest(t *testing.T) (*mongo.Database, func()) {
	t.Helper()

	client := mongotest.Connect(t)

	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_ack"))

	cleanup := func() { _ = db.Drop(context.Background()) }
	t.Cleanup(cleanup)

	return db, cleanup
}

// newAckStoreForTest constructs a MongoAckStore and synchronously creates its
// indexes so tests can rely on them being present before exercising queries.
// The NewMongoAckStore constructor creates indexes asynchronously which would
// race with subsequent operations in tests.
func newAckStoreForTest(t *testing.T, db *mongo.Database, ttl time.Duration) *MongoAckStore {
	t.Helper()

	collName := fmt.Sprintf("_event_acks_%d", time.Now().UnixNano())
	s := &MongoAckStore{
		collection: db.Collection(collName),
		ttl:        ttl,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return s
}

// TestIntegration_AckStore_PendingCount_StoreAck exercises the full Store→Ack
// lifecycle and confirms the in-memory counter mirrors collection state.
func TestIntegration_AckStore_PendingCount_StoreAck(t *testing.T) {
	db, _ := setupAckIntegrationTest(t)

	store := newAckStoreForTest(t, db, time.Hour)
	ctx := context.Background()

	if got := store.PendingCount(); got != 0 {
		t.Fatalf("initial PendingCount = %d, want 0", got)
	}

	for i := 0; i < 5; i++ {
		if err := store.Store(ctx, fmt.Sprintf("evt-%d", i)); err != nil {
			t.Fatalf("Store(evt-%d): %v", i, err)
		}
	}
	if got := store.PendingCount(); got != 5 {
		t.Errorf("after 5 Store: PendingCount = %d, want 5", got)
	}

	if err := store.Ack(ctx, "evt-0"); err != nil {
		t.Fatalf("Ack(evt-0): %v", err)
	}
	if err := store.Ack(ctx, "evt-1"); err != nil {
		t.Fatalf("Ack(evt-1): %v", err)
	}
	if got := store.PendingCount(); got != 3 {
		t.Errorf("after 2 Ack: PendingCount = %d, want 3", got)
	}

	// DB-backed count must agree with the in-memory counter for a single
	// process. This also exercises the pending partial index.
	dbCount, err := store.Count(ctx, AckFilter{Status: AckStatusPending})
	if err != nil {
		t.Fatalf("Count(pending): %v", err)
	}
	if dbCount != int64(store.PendingCount()) {
		t.Errorf("DB count = %d, PendingCount = %d (should match for single process)",
			dbCount, store.PendingCount())
	}
}

// TestIntegration_AckStore_PendingCount_DuplicateStore verifies that a Store
// call for an already-stored event is a no-op for the counter, matching the
// duplicate-key behavior of the underlying InsertOne.
func TestIntegration_AckStore_PendingCount_DuplicateStore(t *testing.T) {
	db, _ := setupAckIntegrationTest(t)

	store := newAckStoreForTest(t, db, time.Hour)
	ctx := context.Background()

	if err := store.Store(ctx, "evt-dup"); err != nil {
		t.Fatalf("Store(evt-dup) first call: %v", err)
	}
	if err := store.Store(ctx, "evt-dup"); err != nil {
		t.Fatalf("Store(evt-dup) second call: %v", err)
	}
	if got := store.PendingCount(); got != 1 {
		t.Errorf("after duplicate Store: PendingCount = %d, want 1", got)
	}
}

// TestIntegration_AckStore_PendingCount_AckIdempotent verifies that a second
// Ack for the same event does not drive the counter negative. Ack is
// idempotent at the document level (filter requires acked_at == epoch) so a
// re-ack is a silent no-op.
func TestIntegration_AckStore_PendingCount_AckIdempotent(t *testing.T) {
	db, _ := setupAckIntegrationTest(t)

	store := newAckStoreForTest(t, db, time.Hour)
	ctx := context.Background()

	if err := store.Store(ctx, "evt-idem"); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := store.Ack(ctx, "evt-idem"); err != nil {
		t.Fatalf("Ack 1st: %v", err)
	}
	if err := store.Ack(ctx, "evt-idem"); err != nil {
		t.Fatalf("Ack 2nd: %v", err)
	}
	if err := store.Ack(ctx, "evt-never-stored"); err != nil {
		t.Fatalf("Ack non-existent: %v", err)
	}
	if got := store.PendingCount(); got != 0 {
		t.Errorf("after idempotent acks: PendingCount = %d, want 0", got)
	}
}

// TestIntegration_AckStore_PendingPartialIndex_ServesCount verifies the
// pending partial index is selected by the query planner for the pending
// Count query. Without the partial index the query falls back to a full
// collection scan, which is the regression this PR addresses.
func TestIntegration_AckStore_PendingPartialIndex_ServesCount(t *testing.T) {
	db, _ := setupAckIntegrationTest(t)

	store := newAckStoreForTest(t, db, time.Hour)
	ctx := context.Background()

	// Insert a representative working set: mostly acked, a few pending. The
	// targeting-ratio alert in production showed exactly this distribution.
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("evt-%d", i)
		if err := store.Store(ctx, id); err != nil {
			t.Fatalf("Store(%s): %v", id, err)
		}
		if i >= 5 {
			if err := store.Ack(ctx, id); err != nil {
				t.Fatalf("Ack(%s): %v", id, err)
			}
		}
	}

	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "count", Value: store.collection.Name()},
			{Key: "query", Value: buildAckFilter(AckFilter{Status: AckStatusPending})},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}

	var result bson.M
	if err := db.RunCommand(ctx, cmd).Decode(&result); err != nil {
		t.Fatalf("explain: %v", err)
	}

	if findIndexScan(result) == "" {
		t.Errorf("expected pending Count to be served by an index-backed plan (IXSCAN or COUNT_SCAN), got plan: %v", result["queryPlanner"])
	}
}

// findIndexScan walks an explain result and returns the indexName of the first
// index-backed stage encountered, or "" if the plan is not index-backed. A
// covered pending count is served by a COUNT -> COUNT_SCAN plan over the
// partial index rather than an IXSCAN, so any stage that carries an indexName
// (IXSCAN, COUNT_SCAN, DISTINCT_SCAN, ...) counts as index-served. Walks both
// queryPlanner.winningPlan and any nested rejectedPlans/inputStage trees.
func findIndexScan(v any) string {
	switch m := v.(type) {
	case bson.M:
		// Any stage carrying a non-empty indexName is index-backed. This covers
		// IXSCAN as well as COUNT_SCAN (the covered-count plan over the partial
		// index) without requiring a specific literal stage name.
		if name, _ := m["indexName"].(string); name != "" {
			return name
		}
		for _, child := range m {
			if name := findIndexScan(child); name != "" {
				return name
			}
		}
	case bson.A:
		for _, child := range m {
			if name := findIndexScan(child); name != "" {
				return name
			}
		}
	case []any:
		for _, child := range m {
			if name := findIndexScan(child); name != "" {
				return name
			}
		}
	}
	return ""
}
