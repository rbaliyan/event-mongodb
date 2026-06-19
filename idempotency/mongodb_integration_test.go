package idempotency

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func getMongoURI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27018/?directConnection=true"
}

// setupIdempotencyIntegrationTest connects to MongoDB and returns a per-test
// MongoStore over a unique database. The background cleanup loop is disabled
// (interval 0) to avoid leaking goroutines. Tests are skipped when MongoDB is
// unavailable or when -short is set.
func setupIdempotencyIntegrationTest(t *testing.T) (*MongoStore, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	uri := getMongoURI()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		cancel()
		t.Skipf("MongoDB not available: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		cancel()
		_ = client.Disconnect(ctx)
		t.Skipf("MongoDB not available: %v", err)
	}

	dbName := fmt.Sprintf("test_event_mongodb_idem_%d_%s",
		os.Getpid(), time.Now().Format("20060102150405.000000"))
	db := client.Database(dbName)

	store, err := NewMongoStore(db, WithMongoCleanupInterval(0))
	if err != nil {
		cancel()
		_ = client.Disconnect(ctx)
		t.Fatalf("NewMongoStore: %v", err)
	}

	cleanup := func() {
		_ = store.Close()
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
		cancel()
	}

	return store, cleanup
}

// TestIntegration_IsDuplicate_FirstThenSecond verifies the core semantics of
// isDuplicateWithCollection: the first IsDuplicate for a fresh id returns
// false (and atomically marks it via the upsert), and the second returns true.
func TestIntegration_IsDuplicate_FirstThenSecond(t *testing.T) {
	store, cleanup := setupIdempotencyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	const id = "msg-1"

	dup, err := store.IsDuplicate(ctx, id)
	if err != nil {
		t.Fatalf("first IsDuplicate: %v", err)
	}
	if dup {
		t.Fatal("first IsDuplicate = true, want false (id was never seen)")
	}

	dup, err = store.IsDuplicate(ctx, id)
	if err != nil {
		t.Fatalf("second IsDuplicate: %v", err)
	}
	if !dup {
		t.Fatal("second IsDuplicate = false, want true (id already marked)")
	}
}

// TestIntegration_ConcurrentIsDuplicate races N goroutines calling IsDuplicate
// for the same fresh id. Per isDuplicateWithCollection's semantics, the upsert
// filter never matches a non-existent document, so all callers contend on an
// upsert insert keyed by the unique _id: exactly one wins (returns false,
// "first-seen") and the rest hit a duplicate-key error mapped to true
// ("already seen"). Any other error fails the test.
func TestIntegration_ConcurrentIsDuplicate(t *testing.T) {
	store, cleanup := setupIdempotencyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	const (
		id         = "concurrent-dup"
		goroutines = 20
	)

	var wg sync.WaitGroup
	dups := make([]bool, goroutines)
	errs := make([]error, goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // release simultaneously to maximize contention
			dup, err := store.IsDuplicate(ctx, id)
			dups[idx] = dup
			errs[idx] = err
		}(i)
	}

	close(start)
	wg.Wait()

	firstSeen := 0
	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: unexpected IsDuplicate error: %v", i, errs[i])
		}
		if !dups[i] { // dup==false means this caller observed the first-seen result
			firstSeen++
		}
	}
	if firstSeen != 1 {
		t.Fatalf("expected exactly 1 first-seen (dup==false) observation, got %d", firstSeen)
	}
}

// TestIntegration_MarkProcessedThenIsDuplicate verifies that an explicitly
// marked id is reported as a duplicate on the next IsDuplicate check.
func TestIntegration_MarkProcessedThenIsDuplicate(t *testing.T) {
	store, cleanup := setupIdempotencyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	const id = "msg-marked"

	if err := store.MarkProcessed(ctx, id); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	dup, err := store.IsDuplicate(ctx, id)
	if err != nil {
		t.Fatalf("IsDuplicate after MarkProcessed: %v", err)
	}
	if !dup {
		t.Fatal("IsDuplicate = false after MarkProcessed, want true")
	}

	// Remove clears the entry, so it should no longer be a duplicate.
	if err := store.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	dup, err = store.IsDuplicate(ctx, id)
	if err != nil {
		t.Fatalf("IsDuplicate after Remove: %v", err)
	}
	if dup {
		t.Fatal("IsDuplicate = true after Remove, want false")
	}
}

// TestIntegration_MarkProcessedWithTTL_Expired verifies that once an entry's
// TTL has elapsed, IsDuplicate treats the id as not-yet-processed again.
func TestIntegration_MarkProcessedWithTTL_Expired(t *testing.T) {
	store, cleanup := setupIdempotencyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	const id = "msg-expiring"

	// Mark with a TTL already in the past so expires_at < now.
	if err := store.MarkProcessedWithTTL(ctx, id, -time.Minute); err != nil {
		t.Fatalf("MarkProcessedWithTTL: %v", err)
	}

	// The stored entry is expired, so isDuplicateWithCollection's filter
	// (expires_at < now) matches and the upsert refreshes it: not a duplicate.
	dup, err := store.IsDuplicate(ctx, id)
	if err != nil {
		t.Fatalf("IsDuplicate on expired entry: %v", err)
	}
	if dup {
		t.Fatal("IsDuplicate = true on expired entry, want false")
	}

	// Now it has been refreshed with the default (future) TTL: duplicate.
	dup, err = store.IsDuplicate(ctx, id)
	if err != nil {
		t.Fatalf("second IsDuplicate after refresh: %v", err)
	}
	if !dup {
		t.Fatal("IsDuplicate = false after refresh, want true")
	}
}

// TestIntegration_EnsureIndexes verifies the TTL index on expires_at is created.
func TestIntegration_EnsureIndexes(t *testing.T) {
	store, cleanup := setupIdempotencyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	cur, err := store.Collection().Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	var indexes []bson.M
	if err := cur.All(ctx, &indexes); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}

	found := false
	for _, idx := range indexes {
		key, _ := idx["key"].(bson.M)
		if key == nil {
			// bson.M decodes nested documents as bson.M; also handle bson.D-ish.
			continue
		}
		if _, ok := key["expires_at"]; ok {
			found = true
			if _, ok := idx["expireAfterSeconds"]; !ok {
				t.Errorf("expires_at index is not a TTL index: %v", idx)
			}
		}
	}
	if !found {
		t.Fatalf("TTL index on expires_at not found; indexes=%v", indexes)
	}
}
