package distributed

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	evtdistributed "github.com/rbaliyan/event/v3/distributed"
)

func getMongoURI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27018/?directConnection=true"
}

// setupDistributedIntegrationTest connects to MongoDB and returns a
// MongoStateManager over a unique per-test database. Tests skip when MongoDB
// is unavailable or when -short is set.
func setupDistributedIntegrationTest(t *testing.T, opts ...Option) (*MongoStateManager, func()) {
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

	dbName := fmt.Sprintf("test_event_mongodb_distributed_%d_%s",
		os.Getpid(), time.Now().Format("20060102150405.000000"))
	db := client.Database(dbName)

	mgr, err := NewMongoStateManager(db, opts...)
	if err != nil {
		cancel()
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
		t.Fatalf("NewMongoStateManager: %v", err)
	}

	cleanup := func() {
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
		cancel()
	}

	return mgr, cleanup
}

func TestIntegrationAcquireAndReacquire(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	ok, err := mgr.Acquire(ctx, "msg-1", time.Minute)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected first Acquire to succeed")
	}

	// Still held (not expired): second acquire must fail.
	ok, err = mgr.Acquire(ctx, "msg-1", time.Minute)
	if err != nil {
		t.Fatalf("Acquire (held): %v", err)
	}
	if ok {
		t.Fatal("expected acquire to fail while lease is held")
	}
}

// TestIntegration_ConcurrentAcquire races N goroutines to Acquire the SAME
// message ID. The Acquire upsert is keyed on a unique _id, so exactly one
// goroutine must win the lease (return true); the rest must observe it as
// already held (return false) via the duplicate-key / held-lease paths. Any
// unexpected error fails the test.
func TestIntegration_ConcurrentAcquire(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	const (
		id         = "concurrent-acquire"
		goroutines = 20
		leaseTTL   = time.Minute
	)

	var wg sync.WaitGroup
	results := make([]bool, goroutines)
	errs := make([]error, goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // release all goroutines at once to maximize contention
			ok, err := mgr.Acquire(ctx, id, leaseTTL)
			results[idx] = ok
			errs[idx] = err
		}(i)
	}

	close(start)
	wg.Wait()

	successes := 0
	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: unexpected Acquire error: %v", i, errs[i])
		}
		if results[i] {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 successful Acquire, got %d", successes)
	}
}

func TestIntegrationAcquireExpiredLease(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	ok, err := mgr.Acquire(ctx, "msg-exp", 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("first Acquire: ok=%v err=%v", ok, err)
	}

	time.Sleep(120 * time.Millisecond)

	ok, err = mgr.Acquire(ctx, "msg-exp", time.Minute)
	if err != nil {
		t.Fatalf("Acquire after expiry: %v", err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed after lease expired")
	}
}

func TestIntegrationMarkProcessedBlocksReacquire(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t, WithCompletedTTL(time.Hour))
	defer cleanup()

	ctx := context.Background()

	ok, err := mgr.Acquire(ctx, "msg-done", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}

	if err := mgr.MarkProcessed(ctx, "msg-done"); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	// Completed with a long TTL: acquire must still fail.
	ok, err = mgr.Acquire(ctx, "msg-done", time.Minute)
	if err != nil {
		t.Fatalf("Acquire after MarkProcessed: %v", err)
	}
	if ok {
		t.Fatal("expected acquire to fail for completed message")
	}
}

func TestIntegrationResetAllowsReacquire(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	ok, err := mgr.Acquire(ctx, "msg-reset", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}

	if err := mgr.Reset(ctx, "msg-reset"); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	ok, err = mgr.Acquire(ctx, "msg-reset", time.Minute)
	if err != nil {
		t.Fatalf("Acquire after Reset: %v", err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed after Reset")
	}
}

func TestIntegrationListAndResetStale(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	for _, id := range []string{"stale-1", "stale-2"} {
		ok, err := mgr.Acquire(ctx, id, time.Minute)
		if err != nil || !ok {
			t.Fatalf("Acquire %s: ok=%v err=%v", id, ok, err)
		}
	}

	// updated_at was just now; with a tiny stale timeout they appear stale.
	time.Sleep(20 * time.Millisecond)

	stale, err := mgr.ListStale(ctx, 10*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ListStale: %v", err)
	}
	if len(stale) != 2 {
		t.Fatalf("expected 2 stale entries, got %v", stale)
	}

	n, err := mgr.ResetStale(ctx, 10*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ResetStale: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected ResetStale to reset 2, got %d", n)
	}

	// After reset (delete in regular mode), they can be reacquired.
	ok, err := mgr.Acquire(ctx, "stale-1", time.Minute)
	if err != nil {
		t.Fatalf("Acquire after ResetStale: %v", err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed after ResetStale")
	}
}

func TestIntegrationPayloadLifecycle(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	ok, err := mgr.Acquire(ctx, "msg-pay", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}

	data := &evtdistributed.MessageData{
		Payload:   []byte("hello world"),
		Metadata:  map[string]string{"k": "v"},
		EventName: "orders.created",
	}
	if err := mgr.StorePayload(ctx, "msg-pay", data); err != nil {
		t.Fatalf("StorePayload: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	stale, err := mgr.LoadStalePayloads(ctx, 10*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("LoadStalePayloads: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale payload, got %d", len(stale))
	}
	got := stale[0]
	if got.MessageID != "msg-pay" {
		t.Fatalf("MessageID = %q", got.MessageID)
	}
	if string(got.Data.Payload) != "hello world" {
		t.Fatalf("Payload = %q", got.Data.Payload)
	}
	if got.Data.EventName != "orders.created" {
		t.Fatalf("EventName = %q", got.Data.EventName)
	}
	if got.Data.Metadata["k"] != "v" {
		t.Fatalf("Metadata = %v", got.Data.Metadata)
	}

	// ClearPayload removes the stored payload from stale results.
	if err := mgr.ClearPayload(ctx, "msg-pay"); err != nil {
		t.Fatalf("ClearPayload: %v", err)
	}
	stale, err = mgr.LoadStalePayloads(ctx, 10*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("LoadStalePayloads after clear: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("expected 0 stale payloads after clear, got %d", len(stale))
	}
}

func TestIntegrationStorePayloadNilIgnored(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	if err := mgr.StorePayload(ctx, "msg-nil", nil); err != nil {
		t.Fatalf("StorePayload(nil): %v", err)
	}
	if err := mgr.StorePayload(ctx, "msg-empty", &evtdistributed.MessageData{}); err != nil {
		t.Fatalf("StorePayload(empty): %v", err)
	}
}

func TestIntegrationEnsureIndexes(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t)
	defer cleanup()

	if err := mgr.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
}

func TestIntegrationCappedCollectionLifecycle(t *testing.T) {
	mgr, cleanup := setupDistributedIntegrationTest(t,
		WithCapped(64*1024, 0))
	defer cleanup()

	ctx := context.Background()

	if err := mgr.CreateCollection(ctx); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	// Idempotent: creating again must not error (namespace-exists tolerated).
	if err := mgr.CreateCollection(ctx); err != nil {
		t.Fatalf("CreateCollection (second): %v", err)
	}
	if err := mgr.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes (capped): %v", err)
	}

	ok, err := mgr.Acquire(ctx, "capped-1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire (capped): ok=%v err=%v", ok, err)
	}

	// In capped mode Reset is a status update (no delete); acquire should
	// still succeed afterwards because expires_at is set to now.
	if err := mgr.Reset(ctx, "capped-1"); err != nil {
		t.Fatalf("Reset (capped): %v", err)
	}
	ok, err = mgr.Acquire(ctx, "capped-1", time.Minute)
	if err != nil {
		t.Fatalf("Acquire after Reset (capped): %v", err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed after capped Reset")
	}
}
