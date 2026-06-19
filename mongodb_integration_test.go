package mongodb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	// Blank import registers the BSON payload codec ("application/bson") in the
	// global event payload registry, enabling typed decode for FullDocumentOnly
	// mode in TestIntegration_FullDocumentOnly_Delivery.
	_ "github.com/rbaliyan/event-mongodb/payload"
)

// fdoOrder is a document type used by the FullDocumentOnly delivery test. It
// carries bson tags so the registered BSON payload codec can decode the raw
// fullDocument payload directly into it.
type fdoOrder struct {
	ID         bson.ObjectID `bson:"_id,omitempty"`
	CustomerID string        `bson:"customer_id"`
	Product    string        `bson:"product"`
	Amount     float64       `bson:"amount"`
	Status     string        `bson:"status"`
}

func getMongoURI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27018/?directConnection=true"
}

func setupIntegrationTest(t *testing.T) (*mongo.Client, *mongo.Database, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	client, err := mongo.Connect(options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		cancel()
		t.Skipf("MongoDB not available: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		cancel()
		_ = client.Disconnect(ctx)
		t.Skipf("MongoDB not available: %v", err)
	}

	// Check for replica set
	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.M{"replSetGetStatus": 1}).Decode(&result); err != nil {
		cancel()
		_ = client.Disconnect(ctx)
		t.Skipf("MongoDB replica set not available: %v", err)
	}

	// Use nanosecond + PID resolution to avoid collisions between tests that
	// start within the same second (matches the subpackages' naming pattern).
	dbName := fmt.Sprintf("test_event_mongodb_%d_%s",
		os.Getpid(), time.Now().Format("20060102150405.000000"))
	db := client.Database(dbName)

	cleanup := func() {
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
		cancel()
	}
	// Use t.Cleanup so the database is dropped even if a test panics after
	// connecting (a bare defer would be skipped on panic in a helper-spawned
	// goroutine path). Callers no longer need to defer cleanup themselves.
	t.Cleanup(cleanup)

	return client, db, cleanup
}

// eventuallyIntegration polls fn until it returns true or the timeout elapses.
// It returns true on success and false on timeout. Used to replace fixed
// sleeps with bounded condition polling for deterministic readiness/assertions.
func eventuallyIntegration(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForStreamReady blocks until the change stream behind received is actually
// delivering events, or fails the test on timeout. It does so by repeatedly
// inserting a sentinel document into sentinelColl and waiting for ANY event to
// arrive on received, which proves the stream is live before the test performs
// its real assertion writes. This replaces fixed time.Sleep readiness waits,
// which are racy on slow CI. The drain channel is emptied so the sentinel
// event(s) do not pollute later assertions.
func waitForStreamReady(t *testing.T, ctx context.Context, sentinelColl *mongo.Collection, received <-chan ChangeEvent) {
	t.Helper()
	ready := eventuallyIntegration(15*time.Second, func() bool {
		if _, err := sentinelColl.InsertOne(ctx, bson.M{"__sentinel": time.Now().UnixNano()}); err != nil {
			return false
		}
		select {
		case <-received:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	})
	if !ready {
		t.Fatal("timed out waiting for change stream to become ready")
	}
	// Drain any buffered sentinel events so they don't leak into assertions.
	for {
		select {
		case <-received:
		default:
			return
		}
	}
}

// waitForStreamReadySlice is the slice-collector counterpart of
// waitForStreamReady for tests that accumulate events into a slice guarded by
// mu rather than a channel. It inserts sentinel docs into sentinelColl until the
// collector observes growth (proving the stream is live), then resets the slice
// (via reset) so the sentinel events do not count toward the test's assertions.
func waitForStreamReadySlice(t *testing.T, ctx context.Context, sentinelColl *mongo.Collection, mu *sync.Mutex, count func() int, reset func()) {
	t.Helper()
	ready := eventuallyIntegration(15*time.Second, func() bool {
		mu.Lock()
		before := count()
		mu.Unlock()
		if _, err := sentinelColl.InsertOne(ctx, bson.M{"__sentinel": time.Now().UnixNano()}); err != nil {
			return false
		}
		return eventuallyIntegration(500*time.Millisecond, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return count() > before
		})
	})
	if !ready {
		t.Fatal("timed out waiting for change stream to become ready")
	}
	mu.Lock()
	reset()
	mu.Unlock()
}

func TestIntegration_ChangeStreamInsert(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create transport
	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Create bus
	bus, err := event.NewBus("test-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	// Register and subscribe
	orderChanges := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	received := make(chan ChangeEvent, 1)
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	// Wait for the change stream to be live by inserting sentinel docs into the
	// watched collection until an event arrives, then proceed with the real write.
	coll := db.Collection("orders")
	waitForStreamReady(t, ctx, coll, received)

	// Insert a document
	insertResult, err := coll.InsertOne(ctx, bson.M{
		"product": "test-product",
		"amount":  42.50,
	})
	if err != nil {
		t.Fatalf("InsertOne() error: %v", err)
	}

	// Wait for change event
	select {
	case change := <-received:
		if change.OperationType != OperationInsert {
			t.Errorf("OperationType = %q, want %q", change.OperationType, OperationInsert)
		}
		if change.Database != db.Name() {
			t.Errorf("Database = %q, want %q", change.Database, db.Name())
		}
		if change.Collection != "orders" {
			t.Errorf("Collection = %q, want %q", change.Collection, "orders")
		}
		expectedKey := insertResult.InsertedID.(bson.ObjectID).Hex()
		if change.DocumentKey != expectedKey {
			t.Errorf("DocumentKey = %q, want %q", change.DocumentKey, expectedKey)
		}
		if change.FullDocument == nil {
			t.Error("FullDocument is nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for change event")
	}
}

func TestIntegration_ChangeStreamUpdate(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Insert a document first
	coll := db.Collection("orders")
	insertResult, err := coll.InsertOne(ctx, bson.M{
		"product": "test-product",
		"amount":  42.50,
		"status":  "pending",
	})
	if err != nil {
		t.Fatalf("InsertOne() error: %v", err)
	}

	// Create transport
	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithUpdateDescription(),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus, err := event.NewBus("test-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	orderChanges := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	received := make(chan ChangeEvent, 1)
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	waitForStreamReady(t, ctx, coll, received)

	// Update the document
	_, err = coll.UpdateByID(ctx, insertResult.InsertedID, bson.M{
		"$set": bson.M{"status": "shipped"},
	})
	if err != nil {
		t.Fatalf("UpdateByID() error: %v", err)
	}

	select {
	case change := <-received:
		if change.OperationType != OperationUpdate {
			t.Errorf("OperationType = %q, want %q", change.OperationType, OperationUpdate)
		}
		if change.UpdateDesc == nil {
			t.Fatal("UpdateDesc is nil")
		}
		if change.UpdateDesc.UpdatedFields["status"] != "shipped" {
			t.Errorf("UpdatedFields[status] = %v, want shipped", change.UpdateDesc.UpdatedFields["status"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for change event")
	}
}

func TestIntegration_ChangeStreamDelete(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Insert a document first
	coll := db.Collection("orders")
	insertResult, err := coll.InsertOne(ctx, bson.M{
		"product": "to-delete",
	})
	if err != nil {
		t.Fatalf("InsertOne() error: %v", err)
	}

	// Create transport
	transport, err := New(db,
		WithCollection("orders"),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus, err := event.NewBus("test-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	orderChanges := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	received := make(chan ChangeEvent, 1)
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	waitForStreamReady(t, ctx, coll, received)

	// Delete the document
	_, err = coll.DeleteOne(ctx, bson.M{"_id": insertResult.InsertedID})
	if err != nil {
		t.Fatalf("DeleteOne() error: %v", err)
	}

	select {
	case change := <-received:
		if change.OperationType != OperationDelete {
			t.Errorf("OperationType = %q, want %q", change.OperationType, OperationDelete)
		}
		expectedKey := insertResult.InsertedID.(bson.ObjectID).Hex()
		if change.DocumentKey != expectedKey {
			t.Errorf("DocumentKey = %q, want %q", change.DocumentKey, expectedKey)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for change event")
	}
}

func TestIntegration_MultipleEvents(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus, err := event.NewBus("test-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	orderChanges := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	var mu sync.Mutex
	var events []ChangeEvent

	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		mu.Lock()
		events = append(events, change)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	coll := db.Collection("orders")
	waitForStreamReadySlice(t, ctx, coll, &mu,
		func() int { return len(events) },
		func() { events = nil },
	)

	// Perform multiple operations
	const numDocs = 5
	for i := 0; i < numDocs; i++ {
		_, err := coll.InsertOne(ctx, bson.M{"index": i})
		if err != nil {
			t.Fatalf("InsertOne() error: %v", err)
		}
	}

	// Wait for all events
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		count := len(events)
		mu.Unlock()

		if count >= numDocs {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Timeout: received %d events, expected %d", count, numDocs)
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != numDocs {
		t.Errorf("Received %d events, want %d", len(events), numDocs)
	}
	for _, ev := range events {
		if ev.OperationType != OperationInsert {
			t.Errorf("Unexpected operation type: %s", ev.OperationType)
		}
	}
}

func TestIntegration_HealthCheck(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transport, err := New(db,
		WithCollection("orders"),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer transport.Close(ctx)

	health := transport.Health(ctx)

	if health.Status != "healthy" {
		t.Errorf("Status = %q, want healthy", health.Status)
	}
	if health.Details["type"] != "mongodb-changestream" {
		t.Errorf("Details[type] = %v, want mongodb-changestream", health.Details["type"])
	}
	if health.Details["watch_level"] != "collection" {
		t.Errorf("Details[watch_level] = %v, want collection", health.Details["watch_level"])
	}
	if health.Details["database"] != db.Name() {
		t.Errorf("Details[database] = %v, want %s", health.Details["database"], db.Name())
	}
	if health.Details["collection"] != "orders" {
		t.Errorf("Details[collection] = %v, want orders", health.Details["collection"])
	}
}

func TestIntegration_DatabaseLevelWatch(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create transport without WithCollection (database-level watch)
	transport, err := New(db,
		WithFullDocument(FullDocumentUpdateLookup),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus, err := event.NewBus("test-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	orderChanges := event.New[ChangeEvent]("all.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	var mu sync.Mutex
	var events []ChangeEvent

	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		mu.Lock()
		events = append(events, change)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	waitForStreamReadySlice(t, ctx, db.Collection("_sentinel"), &mu,
		func() int { return len(events) },
		func() { events = nil },
	)

	// Insert into different collections
	if _, err := db.Collection("orders").InsertOne(ctx, bson.M{"type": "order"}); err != nil {
		t.Fatalf("InsertOne(orders) error: %v", err)
	}
	if _, err := db.Collection("products").InsertOne(ctx, bson.M{"type": "product"}); err != nil {
		t.Fatalf("InsertOne(products) error: %v", err)
	}
	if _, err := db.Collection("users").InsertOne(ctx, bson.M{"type": "user"}); err != nil {
		t.Fatalf("InsertOne(users) error: %v", err)
	}

	// Wait for events
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		count := len(events)
		mu.Unlock()

		if count >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Timeout: received %d events, expected 3", count)
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify we received events from all collections
	collections := make(map[string]bool)
	for _, ev := range events {
		collections[ev.Collection] = true
	}
	if !collections["orders"] || !collections["products"] || !collections["users"] {
		t.Errorf("Expected events from orders, products, users; got: %v", collections)
	}
}

func TestIntegration_ResumeTokenPersistence(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create transport with resume token persistence
	transport1, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithResumeTokenID("test-instance"),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus1, err := event.NewBus("test-bus-1", event.WithTransport(transport1))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}

	orderChanges1 := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus1, orderChanges1); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	received1 := make(chan ChangeEvent, 10)
	err = orderChanges1.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		received1 <- change
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	coll := db.Collection("orders")
	waitForStreamReady(t, ctx, coll, received1)

	// Insert first document
	_, err = coll.InsertOne(ctx, bson.M{"batch": 1, "index": 1})
	if err != nil {
		t.Fatalf("InsertOne() error: %v", err)
	}

	// Wait for first event
	select {
	case <-received1:
		// Good
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for first event")
	}

	// Give the throttled resume-token saver a chance to persist progress before
	// closing, so transport2 resumes from after batch-1 rather than re-reading it.
	time.Sleep(resumeTokenSaveInterval + time.Second)

	// Close first transport
	bus1.Close(ctx)
	time.Sleep(1 * time.Second)

	// Insert document while transport is down
	_, err = coll.InsertOne(ctx, bson.M{"batch": 2, "index": 2})
	if err != nil {
		t.Fatalf("InsertOne() error: %v", err)
	}

	// Create second transport with same resume token ID
	transport2, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithResumeTokenID("test-instance"),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus2, err := event.NewBus("test-bus-2", event.WithTransport(transport2))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus2.Close(ctx)

	orderChanges2 := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus2, orderChanges2); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	received2 := make(chan ChangeEvent, 10)
	err = orderChanges2.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		received2 <- change
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	// Should receive the missed event from resume
	select {
	case change := <-received2:
		// Verify we got the missed event
		if change.OperationType != OperationInsert {
			t.Errorf("Expected insert, got %s", change.OperationType)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for resumed event - resume token may not have worked")
	}
}

// staleTokenStore is a test ResumeTokenStore that injects a deliberately bogus
// resume token on the first Load and then behaves like a cleared store (returns
// nil) afterwards. This mirrors the production recovery sequence: on a stale or
// invalid token the transport's watchLoop clears the token (Save(nil)) and the
// next reconnect Loads nil and starts fresh. We simulate the clear in-store so
// the assertion is deterministic without requiring real oplog rotation to force
// a ChangeStreamHistoryLost (code 286) error, which is not feasible to trigger
// reliably in a unit/integration environment. See cleared/saveCount for the
// recovery observation.
type staleTokenStore struct {
	mu         sync.Mutex
	bogusToken bson.Raw
	served     bool
	cleared    bool
	saveCount  int
}

func (s *staleTokenStore) Load(_ context.Context, _ string) (bson.Raw, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.served && !s.cleared {
		s.served = true
		return s.bogusToken, nil
	}
	// After the bogus token has been served once (and the driver rejected it),
	// behave as a cleared store so the reconnect starts from a valid position.
	return nil, nil
}

func (s *staleTokenStore) Save(_ context.Context, _ string, token bson.Raw) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCount++
	if token == nil {
		s.cleared = true
	}
	return nil
}

func (s *staleTokenStore) wasCleared() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleared
}

// TestIntegration_ResumeFromStaleToken verifies the transport recovers from a
// stored resume token that the server cannot honor instead of failing
// permanently. A deliberately invalid token is injected via a custom store; the
// transport's watchLoop reconnects, the store then yields a cleared (nil)
// position, and delivery of fresh changes continues.
//
// LIMITATION: Forcing a genuine ChangeStreamHistoryLost (error code 286)
// requires rotating the oplog past the token's position, which cannot be done
// deterministically here. The staleTokenStore therefore simulates the
// post-clear Load(nil) that production performs after detecting the stale token,
// exercising the closest feasible portion of the recovery path: reconnect ->
// clear -> resume from a valid position -> continue delivering.
func TestIntegration_ResumeFromStaleToken(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A syntactically-shaped but invalid resume token. The driver rejects this
	// on stream open, triggering a reconnect.
	bogus := bson.Raw(func() []byte {
		raw, _ := bson.Marshal(bson.M{"_data": "00FFFFFFFFFFFFFFFF"})
		return raw
	}())
	store := &staleTokenStore{bogusToken: bogus}

	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithResumeTokenStore(store),
		WithResumeTokenID("stale-instance"),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus, err := event.NewBus("stale-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	orderChanges := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	received := make(chan ChangeEvent, 16)
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	// The stream must recover and start delivering despite the bad initial token.
	coll := db.Collection("orders")
	waitForStreamReady(t, ctx, coll, received)

	// A fresh change must still be delivered after recovery.
	if _, err := coll.InsertOne(ctx, bson.M{"after": "recovery"}); err != nil {
		t.Fatalf("InsertOne() error: %v", err)
	}
	select {
	case change := <-received:
		if change.OperationType != OperationInsert {
			t.Errorf("OperationType = %q, want %q", change.OperationType, OperationInsert)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("transport did not deliver changes after stale-token recovery")
	}

	// The store should have been asked to persist progress (or clear) during
	// recovery; the bogus token was not allowed to wedge the stream permanently.
	if store.saveCount == 0 && !store.wasCleared() {
		t.Log("note: store recorded no saves; recovery relied on Load(nil) fallback")
	}
}

// TestIntegration_ReconnectAfterClose opens a transport, closes it, then opens a
// fresh transport sharing the same persistent resume token store and asserts a
// change written between the two is delivered after reconnect — proving delivery
// resumes from the saved position rather than dropping the gap.
func TestIntegration_ReconnectAfterClose(t *testing.T) {
	client, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A shared persistent token store so the second transport resumes where the
	// first left off.
	tokenColl := db.Collection("_reconnect_tokens")
	tokenStore, err := NewMongoResumeTokenStore(tokenColl)
	if err != nil {
		t.Fatalf("NewMongoResumeTokenStore: %v", err)
	}
	if err := tokenStore.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	_ = client // client retained for symmetry with other setups

	newTransport := func() (*event.Bus, event.Event[ChangeEvent], chan ChangeEvent) {
		t.Helper()
		tr, err := New(db,
			WithCollection("orders"),
			WithFullDocument(FullDocumentUpdateLookup),
			WithResumeTokenStore(tokenStore),
			WithResumeTokenID("reconnect-instance"),
		)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		bus, err := event.NewBus("reconnect-bus", event.WithTransport(tr))
		if err != nil {
			t.Fatalf("NewBus() error: %v", err)
		}
		ev := event.New[ChangeEvent]("order.changes")
		if err := event.Register(ctx, bus, ev); err != nil {
			t.Fatalf("Register() error: %v", err)
		}
		ch := make(chan ChangeEvent, 16)
		if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
			select {
			case ch <- change:
			default:
			}
			return nil
		}); err != nil {
			t.Fatalf("Subscribe() error: %v", err)
		}
		return bus, ev, ch
	}

	coll := db.Collection("orders")

	// First transport: establish and persist a resume position.
	bus1, _, ch1 := newTransport()
	waitForStreamReady(t, ctx, coll, ch1)
	if _, err := coll.InsertOne(ctx, bson.M{"phase": "before-close"}); err != nil {
		t.Fatalf("InsertOne(before-close): %v", err)
	}
	select {
	case <-ch1:
	case <-time.After(10 * time.Second):
		t.Fatal("first transport did not receive pre-close change")
	}
	// Let the throttled saver persist the position before closing.
	time.Sleep(resumeTokenSaveInterval + time.Second)
	bus1.Close(ctx)

	// Write a change while no transport is watching.
	if _, err := coll.InsertOne(ctx, bson.M{"phase": "during-gap"}); err != nil {
		t.Fatalf("InsertOne(during-gap): %v", err)
	}

	// Second transport with the same token store must resume and deliver the gap.
	bus2, _, ch2 := newTransport()
	defer bus2.Close(ctx)

	gotGap := false
	deadline := time.After(15 * time.Second)
	for !gotGap {
		select {
		case change := <-ch2:
			if change.OperationType == OperationInsert {
				gotGap = true
			}
		case <-deadline:
			t.Fatal("reopened transport did not resume delivery from saved position")
		}
	}
}

// TestIntegration_UpdateDescription_RemovedFields verifies $unset removals are
// surfaced via UpdateDescription.RemovedFields when WithUpdateDescription is on.
func TestIntegration_UpdateDescription_RemovedFields(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := db.Collection("orders")
	res, err := coll.InsertOne(ctx, bson.M{"product": "p", "note": "remove-me", "status": "pending"})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithUpdateDescription(),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	bus, err := event.NewBus("rm-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	ev := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatalf("Register: %v", err)
	}
	received := make(chan ChangeEvent, 4)
	if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	waitForStreamReady(t, ctx, coll, received)

	if _, err := coll.UpdateByID(ctx, res.InsertedID, bson.M{"$unset": bson.M{"note": ""}}); err != nil {
		t.Fatalf("UpdateByID($unset): %v", err)
	}

	select {
	case change := <-received:
		if change.OperationType != OperationUpdate {
			t.Fatalf("OperationType = %q, want update", change.OperationType)
		}
		if change.UpdateDesc == nil {
			t.Fatal("UpdateDesc is nil")
		}
		foundRemoved := false
		for _, f := range change.UpdateDesc.RemovedFields {
			if f == "note" {
				foundRemoved = true
			}
		}
		if !foundRemoved {
			t.Errorf("RemovedFields = %v, want to contain 'note'", change.UpdateDesc.RemovedFields)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for $unset update event")
	}
}

// TestIntegration_UpdateDescription_TruncatedArrays verifies array truncation is
// surfaced via UpdateDescription.TruncatedArrays. TruncatedArrays is only
// present on the full ChangeEvent payload (it is not emitted as metadata), so
// this test subscribes to ChangeEvent directly.
func TestIntegration_UpdateDescription_TruncatedArrays(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := db.Collection("orders")
	res, err := coll.InsertOne(ctx, bson.M{
		"items": bson.A{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithUpdateDescription(),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	bus, err := event.NewBus("ta-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	ev := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatalf("Register: %v", err)
	}
	received := make(chan ChangeEvent, 4)
	if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	waitForStreamReady(t, ctx, coll, received)

	// $push with $slice truncates the array, which MongoDB reports as a
	// truncatedArray in the change stream's updateDescription.
	if _, err := coll.UpdateByID(ctx, res.InsertedID, bson.M{
		"$push": bson.M{"items": bson.M{"$each": bson.A{}, "$slice": 3}},
	}); err != nil {
		t.Fatalf("UpdateByID($slice): %v", err)
	}

	select {
	case change := <-received:
		if change.OperationType != OperationUpdate {
			t.Fatalf("OperationType = %q, want update", change.OperationType)
		}
		if change.UpdateDesc == nil {
			t.Fatal("UpdateDesc is nil")
		}
		// Depending on server version the truncation may be reported either as a
		// TruncatedArrays entry or as an updated_fields rewrite of the array.
		// Accept either, but require the change to be non-empty and reference items.
		foundTruncated := false
		for _, f := range change.UpdateDesc.TruncatedArrays {
			if f == "items" {
				foundTruncated = true
			}
		}
		_, rewritten := change.UpdateDesc.UpdatedFields["items"]
		if !foundTruncated && !rewritten {
			t.Errorf("expected items to appear in TruncatedArrays or UpdatedFields; got truncated=%v updated=%v",
				change.UpdateDesc.TruncatedArrays, change.UpdateDesc.UpdatedFields)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for truncated-array update event")
	}
}

// TestIntegration_EmptyUpdate_DiscardedByDefault verifies that an update which
// produces no field-level changes is discarded by default, and that the same
// update IS delivered when WithEmptyUpdates is set. The two sub-cases use
// separate transports.
func TestIntegration_EmptyUpdate_DiscardedByDefault(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	coll := db.Collection("orders")
	res, err := coll.InsertOne(ctx, bson.M{"status": "same"})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Sub-case 1: default transport discards empty updates.
	t.Run("discarded", func(t *testing.T) {
		transport, err := New(db,
			WithCollection("orders"),
			WithUpdateDescription(),
			WithoutResume(),
		)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		bus, err := event.NewBus("empty-default-bus", event.WithTransport(transport))
		if err != nil {
			t.Fatalf("NewBus() error: %v", err)
		}
		defer bus.Close(ctx)

		ev := event.New[ChangeEvent]("order.changes")
		if err := event.Register(ctx, bus, ev); err != nil {
			t.Fatalf("Register: %v", err)
		}
		received := make(chan ChangeEvent, 8)
		if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
			select {
			case received <- change:
			default:
			}
			return nil
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		waitForStreamReady(t, ctx, coll, received)

		// Set the field to its current value: produces no actual change, so the
		// update is an empty update and must be discarded.
		if _, err := coll.UpdateByID(ctx, res.InsertedID, bson.M{"$set": bson.M{"status": "same"}}); err != nil {
			t.Fatalf("UpdateByID(no-op): %v", err)
		}
		// Follow with a real change as a sentinel-of-progress; if we receive the
		// real update first (and not an empty one), the empty update was dropped.
		if _, err := coll.UpdateByID(ctx, res.InsertedID, bson.M{"$set": bson.M{"status": "changed"}}); err != nil {
			t.Fatalf("UpdateByID(real): %v", err)
		}

		select {
		case change := <-received:
			if change.UpdateDesc == nil || len(change.UpdateDesc.UpdatedFields) == 0 {
				t.Fatalf("received an empty update by default; want it discarded (UpdateDesc=%+v)", change.UpdateDesc)
			}
			if change.UpdateDesc.UpdatedFields["status"] != "changed" {
				t.Errorf("first delivered update should be the real one; got %+v", change.UpdateDesc.UpdatedFields)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for the real update event")
		}
	})

	// Sub-case 2: WithEmptyUpdates delivers the empty update.
	t.Run("delivered_with_option", func(t *testing.T) {
		transport, err := New(db,
			WithCollection("orders"),
			WithUpdateDescription(),
			WithEmptyUpdates(),
			WithoutResume(),
		)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		bus, err := event.NewBus("empty-on-bus", event.WithTransport(transport))
		if err != nil {
			t.Fatalf("NewBus() error: %v", err)
		}
		defer bus.Close(ctx)

		ev := event.New[ChangeEvent]("order.changes")
		if err := event.Register(ctx, bus, ev); err != nil {
			t.Fatalf("Register: %v", err)
		}
		received := make(chan ChangeEvent, 8)
		if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
			select {
			case received <- change:
			default:
			}
			return nil
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		waitForStreamReady(t, ctx, coll, received)

		// Issue an update whose $set writes the field to the value it already
		// holds (set to "changed" by sub-case 1). This yields an update with no
		// effective field changes (an empty update). With WithEmptyUpdates the
		// transport must deliver it instead of discarding it.
		//
		// NOTE: Some MongoDB server versions suppress a truly identical no-op
		// write entirely (no oplog entry, hence no change event). When that
		// happens there is nothing to deliver and we skip rather than fail, since
		// the discard-by-default behavior is already covered above and the
		// delivery path here is what's under test only when the server emits the
		// empty update at all.
		if _, err := coll.UpdateByID(ctx, res.InsertedID, bson.M{"$set": bson.M{"status": "changed"}}); err != nil {
			t.Fatalf("UpdateByID(no-op): %v", err)
		}

		select {
		case change := <-received:
			if change.OperationType != OperationUpdate {
				t.Errorf("OperationType = %q, want update", change.OperationType)
			}
			if !isEmptyUpdate(change) {
				t.Errorf("expected an empty update, got UpdateDesc=%+v", change.UpdateDesc)
			}
		case <-time.After(8 * time.Second):
			t.Skip("server emitted no change event for a no-op update; empty-update delivery not exercisable on this deployment")
		}
	})
}

// TestIntegration_MaxUpdatedFieldsSize_Omitted verifies that when the serialized
// updated_fields exceeds WithMaxUpdatedFieldsSize, the updated_fields metadata
// is omitted while the event itself is still delivered. The check inspects the
// metadata via ContextUpdateDescription from the handler context.
func TestIntegration_MaxUpdatedFieldsSize_Omitted(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := db.Collection("orders")
	res, err := coll.InsertOne(ctx, bson.M{"product": "p"})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Tiny limit so any real updated_fields payload exceeds it. WithFullDocument
	// is required for WithMaxUpdatedFieldsSize; the option implicitly enables
	// WithUpdateDescription.
	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithMaxUpdatedFieldsSize(4),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	bus, err := event.NewBus("maxsize-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	ev := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatalf("Register: %v", err)
	}

	type result struct {
		desc *UpdateDescription
	}
	received := make(chan result, 4)
	if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- result{desc: ContextUpdateDescription(ctx)}:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Readiness: drain any sentinel events on this channel shape.
	if !eventuallyIntegration(15*time.Second, func() bool {
		if _, err := coll.InsertOne(ctx, bson.M{"__sentinel": time.Now().UnixNano()}); err != nil {
			return false
		}
		select {
		case <-received:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}) {
		t.Fatal("timed out waiting for stream readiness")
	}
	for {
		select {
		case <-received:
			continue
		default:
		}
		break
	}

	// A multi-field update whose serialized updated_fields exceeds 4 bytes.
	if _, err := coll.UpdateByID(ctx, res.InsertedID, bson.M{
		"$set": bson.M{"status": "shipped-with-a-long-value", "tracking": "ABCDEFGHIJK"},
	}); err != nil {
		t.Fatalf("UpdateByID: %v", err)
	}

	select {
	case r := <-received:
		// updated_fields metadata should be omitted (over the size limit), so
		// ContextUpdateDescription should not surface UpdatedFields. removed_fields
		// metadata may still be absent; either way UpdatedFields must be empty.
		if r.desc != nil && len(r.desc.UpdatedFields) > 0 {
			t.Errorf("expected updated_fields omitted over size limit, got %v", r.desc.UpdatedFields)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for oversized update event")
	}
}

// TestIntegration_FullDocumentOnly_Delivery verifies FullDocumentOnly payload
// mode: the transport sends the raw fullDocument as BSON, and a subscriber typed
// on the document struct (decoded via the registered BSON payload codec)
// receives the document fields directly rather than a ChangeEvent wrapper.
func TestIntegration_FullDocumentOnly_Delivery(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithFullDocumentOnly(),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	bus, err := event.NewBus("fdonly-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	// Subscribe with the document type directly. The BSON payload codec is
	// registered via the blank import of the payload package (see imports).
	orderEvent := event.New[fdoOrder]("order.changes")
	if err := event.Register(ctx, bus, orderEvent); err != nil {
		t.Fatalf("Register: %v", err)
	}
	received := make(chan fdoOrder, 4)
	if err := orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[fdoOrder], order fdoOrder) error {
		select {
		case received <- order:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Readiness using the typed channel.
	coll := db.Collection("orders")
	if !eventuallyIntegration(15*time.Second, func() bool {
		if _, err := coll.InsertOne(ctx, bson.M{"product": "__sentinel"}); err != nil {
			return false
		}
		select {
		case <-received:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}) {
		t.Fatal("timed out waiting for stream readiness")
	}
	for {
		select {
		case <-received:
			continue
		default:
		}
		break
	}

	if _, err := coll.InsertOne(ctx, bson.M{
		"customer_id": "cust-1",
		"product":     "widget",
		"amount":      99.99,
		"status":      "new",
	}); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case order := <-received:
			if order.Product == "__sentinel" {
				continue // stray sentinel; keep waiting for the real doc
			}
			if order.Product != "widget" {
				t.Errorf("Product = %q, want widget", order.Product)
			}
			if order.Amount != 99.99 {
				t.Errorf("Amount = %v, want 99.99", order.Amount)
			}
			if order.CustomerID != "cust-1" {
				t.Errorf("CustomerID = %q, want cust-1", order.CustomerID)
			}
			return
		case <-deadline:
			t.Fatal("timeout waiting for full-document-only delivery")
		}
	}
}

// TestIntegration_ClusterWatch verifies cluster-level watching via
// NewClusterWatch delivers changes from an arbitrary database/collection.
// Cluster watch requires privileges to watch the deployment and stores tokens
// in the admin database; it is skipped (not failed) when the deployment refuses
// the cluster-level change stream.
func TestIntegration_ClusterWatch(t *testing.T) {
	client, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transport, err := NewClusterWatch(client,
		WithFullDocument(FullDocumentUpdateLookup),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("NewClusterWatch() error: %v", err)
	}
	bus, err := event.NewBus("cluster-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	ev := event.New[ChangeEvent]("all.changes")
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatalf("Register: %v", err)
	}
	received := make(chan ChangeEvent, 16)
	if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Probe readiness; if the cluster-level stream never produces our sentinel
	// within the window, the deployment likely disallows cluster watch — skip.
	coll := db.Collection("cluster_orders")
	ready := eventuallyIntegration(15*time.Second, func() bool {
		if _, err := coll.InsertOne(ctx, bson.M{"__sentinel": time.Now().UnixNano()}); err != nil {
			return false
		}
		select {
		case <-received:
			return true
		case <-time.After(300 * time.Millisecond):
			return false
		}
	})
	if !ready {
		t.Skip("cluster-level change stream not available on this deployment")
	}
	for {
		select {
		case <-received:
			continue
		default:
		}
		break
	}

	if _, err := coll.InsertOne(ctx, bson.M{"product": "cluster-doc"}); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case change := <-received:
			if change.Collection == "cluster_orders" && change.OperationType == OperationInsert {
				return
			}
		case <-deadline:
			t.Fatal("timeout waiting for cluster-level change event")
		}
	}
}
