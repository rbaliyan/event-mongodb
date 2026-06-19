package outbox

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	evtoutbox "github.com/rbaliyan/event/v3/outbox"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupOutboxStore connects to MongoDB and returns a MongoStore backed by a
// unique per-test database. It skips the test (rather than failing) when no
// MongoDB instance is reachable. Teardown (a bounded db drop) is registered via
// t.Cleanup so it runs even if a test panics.
func setupOutboxStore(t *testing.T) (*MongoStore, func()) {
	t.Helper()

	client := mongotest.Connect(t)

	db := client.Database(mongotest.UniqueDBName("test_outbox"))

	store, err := NewMongoStore(db)
	if err != nil {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = db.Drop(dropCtx)
		dropCancel()
		t.Fatalf("NewMongoStore: %v", err)
	}

	cleanup := func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		_ = db.Drop(dropCtx)
	}
	t.Cleanup(cleanup)

	return store, cleanup
}

func insertPending(t *testing.T, ctx context.Context, store *MongoStore, eventID string, priority int) {
	t.Helper()
	msg := &mongoMessage{
		EventName: "order.created",
		EventID:   eventID,
		Payload:   []byte(`{"id":1}`),
		Priority:  priority,
	}
	if err := store.Insert(ctx, msg); err != nil {
		t.Fatalf("Insert(%s): %v", eventID, err)
	}
}

func TestIntegrationInsertAndGetPending(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	insertPending(t, ctx, store, "evt-1", 0)
	insertPending(t, ctx, store, "evt-2", 5)

	pending, err := store.Count(ctx, evtoutbox.StatusPending)
	if err != nil {
		t.Fatalf("Count pending: %v", err)
	}
	if pending != 2 {
		t.Fatalf("pending count = %d, want 2", pending)
	}

	msgs, err := store.GetPending(ctx, 10)
	if err != nil {
		t.Fatalf("GetPending: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("GetPending returned %d, want 2", len(msgs))
	}

	// Higher priority should come first.
	if msgs[0].EventID != "evt-2" {
		t.Errorf("expected highest-priority evt-2 first, got %s", msgs[0].EventID)
	}

	// Claiming moves messages to processing.
	processing, err := store.Count(ctx, evtoutbox.StatusProcessing)
	if err != nil {
		t.Fatalf("Count processing: %v", err)
	}
	if processing != 2 {
		t.Fatalf("processing count = %d, want 2", processing)
	}
}

func TestIntegrationClaimBatchConcurrencySafe(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	insertPending(t, ctx, store, "evt-1", 0)
	insertPending(t, ctx, store, "evt-2", 0)
	insertPending(t, ctx, store, "evt-3", 0)

	first, err := store.claimBatch(ctx, 10)
	if err != nil {
		t.Fatalf("first claimBatch: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("first claim got %d, want 3", len(first))
	}

	// A second claim must not re-claim the already-processing messages.
	second, err := store.claimBatch(ctx, 10)
	if err != nil {
		t.Fatalf("second claimBatch: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second claim got %d, want 0 (already claimed)", len(second))
	}
}

func TestIntegrationConcurrentClaimNoDuplicates(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	const total = 20
	for i := 0; i < total; i++ {
		insertPending(t, ctx, store, "evt-"+strconv.Itoa(i), 0)
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	var claimErrs []error
	var wg sync.WaitGroup
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs, err := store.claimBatch(ctx, total)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				claimErrs = append(claimErrs, err)
				return
			}
			for _, m := range msgs {
				seen[m.EventID]++
			}
		}()
	}
	wg.Wait()

	for _, err := range claimErrs {
		t.Errorf("concurrent claimBatch error: %v", err)
	}

	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %s claimed %d times, want exactly 1", id, n)
		}
	}
	if len(seen) != total {
		t.Errorf("claimed %d distinct events, want %d", len(seen), total)
	}
}

func TestIntegrationMarkPublished(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	insertPending(t, ctx, store, "evt-1", 0)
	claimed, err := store.getPendingMongo(ctx, 10)
	if err != nil {
		t.Fatalf("getPendingMongo: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1", len(claimed))
	}

	if err := store.MarkPublished(ctx, claimed[0].ID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	published, err := store.Count(ctx, evtoutbox.StatusPublished)
	if err != nil {
		t.Fatalf("Count published: %v", err)
	}
	if published != 1 {
		t.Fatalf("published count = %d, want 1", published)
	}
}

func TestIntegrationMarkFailed(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	insertPending(t, ctx, store, "evt-1", 0)
	claimed, err := store.getPendingMongo(ctx, 10)
	if err != nil {
		t.Fatalf("getPendingMongo: %v", err)
	}

	if err := store.MarkFailed(ctx, claimed[0].ID, errors.New("publish boom")); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	failed, err := store.Count(ctx, evtoutbox.StatusFailed)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if failed != 1 {
		t.Fatalf("failed count = %d, want 1", failed)
	}

	// retry_count should have been incremented and last_error recorded.
	var doc mongoMessage
	if err := store.Collection().FindOne(ctx, bson.M{"_id": claimed[0].ID}).Decode(&doc); err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if doc.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", doc.RetryCount)
	}
	if doc.LastError != "publish boom" {
		t.Errorf("LastError = %q, want %q", doc.LastError, "publish boom")
	}
}

func TestIntegrationCountByStatus(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	insertPending(t, ctx, store, "evt-1", 0)
	insertPending(t, ctx, store, "evt-2", 0)
	insertPending(t, ctx, store, "evt-3", 0)

	if c, err := store.Count(ctx, evtoutbox.StatusPending); err != nil || c != 3 {
		t.Fatalf("pending count = %d (err=%v), want 3", c, err)
	}
	if c, err := store.Count(ctx, evtoutbox.StatusPublished); err != nil || c != 0 {
		t.Fatalf("published count = %d (err=%v), want 0", c, err)
	}
}

func TestIntegrationRecoverStuck(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	// Insert a message stuck in processing with an old claimed_at.
	stale := time.Now().Add(-30 * time.Minute)
	_, err := store.Collection().InsertOne(ctx, mongoMessage{
		ID:        bson.NewObjectID(),
		EventName: "order.created",
		EventID:   "stuck-1",
		Status:    evtoutbox.StatusProcessing,
		ClaimedAt: &stale,
		ClaimedBy: "dead-relay",
		CreatedAt: stale,
	})
	if err != nil {
		t.Fatalf("InsertOne stuck: %v", err)
	}

	// A freshly-claimed message must NOT be recovered.
	fresh := time.Now()
	_, err = store.Collection().InsertOne(ctx, mongoMessage{
		ID:        bson.NewObjectID(),
		EventName: "order.created",
		EventID:   "fresh-1",
		Status:    evtoutbox.StatusProcessing,
		ClaimedAt: &fresh,
		ClaimedBy: "live-relay",
		CreatedAt: fresh,
	})
	if err != nil {
		t.Fatalf("InsertOne fresh: %v", err)
	}

	recovered, err := store.RecoverStuck(ctx, 5*time.Minute)
	if err != nil {
		t.Fatalf("RecoverStuck: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}

	if c, err := store.Count(ctx, evtoutbox.StatusPending); err != nil || c != 1 {
		t.Fatalf("pending count = %d (err=%v), want 1", c, err)
	}
	if c, err := store.Count(ctx, evtoutbox.StatusProcessing); err != nil || c != 1 {
		t.Fatalf("processing count = %d (err=%v), want 1", c, err)
	}
}

func TestIntegrationRetryFailed(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	// One failed message below the retry limit, one at/above it.
	_, err := store.Collection().InsertMany(ctx, []any{
		mongoMessage{
			ID:         bson.NewObjectID(),
			EventName:  "e",
			EventID:    "retryable",
			Status:     evtoutbox.StatusFailed,
			RetryCount: 1,
			CreatedAt:  time.Now(),
		},
		mongoMessage{
			ID:         bson.NewObjectID(),
			EventName:  "e",
			EventID:    "exhausted",
			Status:     evtoutbox.StatusFailed,
			RetryCount: 5,
			CreatedAt:  time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	retried, err := store.RetryFailed(ctx, 3)
	if err != nil {
		t.Fatalf("RetryFailed: %v", err)
	}
	if retried != 1 {
		t.Fatalf("retried = %d, want 1", retried)
	}

	if c, err := store.Count(ctx, evtoutbox.StatusPending); err != nil || c != 1 {
		t.Fatalf("pending count = %d (err=%v), want 1", c, err)
	}
	if c, err := store.Count(ctx, evtoutbox.StatusFailed); err != nil || c != 1 {
		t.Fatalf("failed count = %d (err=%v), want 1", c, err)
	}
}

func TestIntegrationDeleteOldPublished(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	oldTime := time.Now().Add(-48 * time.Hour)
	recentTime := time.Now()

	_, err := store.Collection().InsertMany(ctx, []any{
		mongoMessage{
			ID:          bson.NewObjectID(),
			EventName:   "e",
			EventID:     "old",
			Status:      evtoutbox.StatusPublished,
			PublishedAt: &oldTime,
			CreatedAt:   oldTime,
		},
		mongoMessage{
			ID:          bson.NewObjectID(),
			EventName:   "e",
			EventID:     "recent",
			Status:      evtoutbox.StatusPublished,
			PublishedAt: &recentTime,
			CreatedAt:   recentTime,
		},
	})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	deleted, err := store.Delete(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	if c, err := store.Count(ctx, evtoutbox.StatusPublished); err != nil || c != 1 {
		t.Fatalf("remaining published = %d (err=%v), want 1", c, err)
	}
}

func TestIntegrationEnsureIndexes(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	cursor, err := store.Collection().Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	var got []bson.M
	if err := cursor.All(ctx, &got); err != nil {
		t.Fatalf("read indexes: %v", err)
	}

	// 4 declared indexes + the implicit _id index = 5.
	if len(got) != 5 {
		t.Errorf("index count = %d, want 5 (4 declared + _id)", len(got))
	}

	// Idempotent: calling again must not error.
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes (second call): %v", err)
	}
}

func TestIntegrationIsCappedFalse(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	// Create the collection via a normal insert.
	insertPending(t, ctx, store, "evt-1", 0)

	capped, err := store.IsCapped(ctx)
	if err != nil {
		t.Fatalf("IsCapped: %v", err)
	}
	if capped {
		t.Error("IsCapped = true, want false for a normal collection")
	}
}
