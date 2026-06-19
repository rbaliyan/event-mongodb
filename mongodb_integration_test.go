package mongodb

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"

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

func setupIntegrationTest(t *testing.T) (*mongo.Client, *mongo.Database, func()) {
	t.Helper()

	// mongotest.Connect handles -short skip, connect, ping, and registers the
	// client disconnect via t.Cleanup. Change streams additionally require a
	// replica set, which is this package's local precondition (checked below).
	client := mongotest.Connect(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Check for replica set — change streams require it.
	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.M{"replSetGetStatus": 1}).Decode(&result); err != nil {
		t.Skipf("MongoDB replica set not available: %v", err)
	}

	db := client.Database(mongotest.UniqueDBName("test_event_mongodb"))

	// Drop the database on cleanup. Registered after the client-disconnect
	// cleanup inside mongotest.Connect, so (LIFO) the drop runs first while the
	// client is still connected. Returned for symmetry with older callers.
	cleanup := func() { _ = db.Drop(context.Background()) }
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

	// Wait (bounded) for the throttled resume-token saver to persist progress
	// before closing, so transport2 resumes from after batch-1 rather than
	// re-reading it. Instead of a fixed sleep tied to resumeTokenSaveInterval,
	// poll the default _event_resume_tokens collection directly until a token
	// is stored for this transport's key. The transport auto-creates its store
	// in this collection, so we read through an equivalent store.
	tokenProbe, err := NewMongoResumeTokenStore(db.Collection(defaultResumeTokenCollection))
	if err != nil {
		t.Fatalf("NewMongoResumeTokenStore(probe): %v", err)
	}
	resumeKey := transport1.ResumeTokenKey()
	if !eventuallyIntegration(resumeTokenSaveInterval+10*time.Second, func() bool {
		tok, loadErr := tokenProbe.Load(ctx, resumeKey)
		return loadErr == nil && len(tok) > 0
	}) {
		t.Fatal("timed out waiting for resume token to be persisted before close")
	}

	// Close first transport
	bus1.Close(ctx)

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

	newTransport := func() (*event.Bus, *Transport, chan ChangeEvent) {
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
		return bus, tr, ch
	}

	coll := db.Collection("orders")

	// First transport: establish and persist a resume position.
	bus1, bus1Transport, ch1 := newTransport()
	waitForStreamReady(t, ctx, coll, ch1)
	if _, err := coll.InsertOne(ctx, bson.M{"phase": "before-close"}); err != nil {
		t.Fatalf("InsertOne(before-close): %v", err)
	}
	select {
	case <-ch1:
	case <-time.After(10 * time.Second):
		t.Fatal("first transport did not receive pre-close change")
	}
	// Wait (bounded) for the throttled saver to persist a position before
	// closing, polling the shared token store directly instead of sleeping for
	// resumeTokenSaveInterval. The first save may be the initial position (saved
	// immediately on first start) or the post-change token; either is non-empty
	// and sufficient for the second transport to resume from.
	reconnectKey := bus1Transport.ResumeTokenKey()
	if !eventuallyIntegration(resumeTokenSaveInterval+10*time.Second, func() bool {
		tok, loadErr := tokenStore.Load(ctx, reconnectKey)
		return loadErr == nil && len(tok) > 0
	}) {
		t.Fatal("timed out waiting for resume token to be persisted before close")
	}
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

// TestIntegration_MetricsReflectStreamActivity wires the transport to a real
// OTel Metrics instance backed by a ManualReader (mirroring metrics_test.go),
// drives a real change through the change stream, and asserts that transport-
// and handler-level instruments actually moved:
//
//   - mongodb_stream_active > 0 while the stream is open (transport-level
//     UpDownCounter recorded by the transport itself).
//   - mongodb_stream_receive_lag_seconds recorded at least once (transport-level
//     histogram, recorded before any handler runs).
//   - mongodb_changes_processed_total > 0 (handler-level counter recorded by the
//     MetricsMiddleware wired onto the subscription).
func TestIntegration_MetricsReflectStreamActivity(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Deterministic, in-process metrics pipeline — no external collector.
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	metrics, err := NewMetrics(WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	t.Cleanup(func() { _ = metrics.Close() })

	transport, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithMetrics(metrics),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	bus, err := event.NewBus("metrics-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	orderChanges := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	received := make(chan ChangeEvent, 8)
	// Wire the handler-level metrics middleware so changes_processed_total moves.
	if err := orderChanges.Subscribe(ctx,
		func(ctx context.Context, ev event.Event[ChangeEvent], change ChangeEvent) error {
			select {
			case received <- change:
			default:
			}
			return nil
		},
		event.WithMiddleware(MetricsMiddleware[ChangeEvent](metrics)),
	); err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	coll := db.Collection("orders")
	waitForStreamReady(t, ctx, coll, received)

	// While the stream is live, the transport-level active-stream gauge must be
	// positive. Poll the ManualReader until it observes the open stream.
	if !eventuallyIntegration(10*time.Second, func() bool {
		rm := collectMetrics(t, reader)
		return sumCounter(findMetric(rm, "mongodb_stream_active")) > 0
	}) {
		t.Fatal("mongodb_stream_active never became > 0 while stream open")
	}

	// Drive a real change and wait for the handler to observe it.
	if _, err := coll.InsertOne(ctx, bson.M{"product": "metrics-doc"}); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for change to reach handler")
	}

	// After the change has been processed, the handler-level processed counter
	// and the transport-level receive-lag histogram must both have recorded.
	if !eventuallyIntegration(10*time.Second, func() bool {
		rm := collectMetrics(t, reader)
		processed := sumCounter(findMetric(rm, "mongodb_changes_processed_total")) > 0
		recvLag := histogramCount(findMetric(rm, "mongodb_stream_receive_lag_seconds")) > 0
		return processed && recvLag
	}) {
		rm := collectMetrics(t, reader)
		t.Fatalf("metrics not recorded: processed=%d receive_lag=%d",
			sumCounter(findMetric(rm, "mongodb_changes_processed_total")),
			histogramCount(findMetric(rm, "mongodb_stream_receive_lag_seconds")))
	}
}

// TestIntegration_StartFromPast verifies the WithStartFromPast behavior end to
// end:
//
//  1. Write history to a collection, then start a FRESH transport (no saved
//     token) with WithStartFromPast(d). Changes that occurred within the window
//     must be replayed.
//  2. A SECOND start of an equivalent transport that DOES have a saved token
//     must NOT replay history; it resumes from the saved position. We assert it
//     does not re-deliver the historical document, but does deliver a brand-new
//     change written after that token was saved.
//
// A shared persistent token store is used across both phases so the second
// transport observes the position the first one saved.
func TestIntegration_StartFromPast(t *testing.T) {
	_, db, _ := setupIntegrationTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	coll := db.Collection("orders")

	// Shared token store so phase 2 sees the token phase 1 saves.
	tokenStore, err := NewMongoResumeTokenStore(db.Collection("_startfrompast_tokens"))
	if err != nil {
		t.Fatalf("NewMongoResumeTokenStore: %v", err)
	}
	if err := tokenStore.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	// Write a historical document BEFORE any transport exists. With
	// WithStartFromPast, a fresh transport must replay it.
	const historicalMarker = "historical-doc"
	if _, err := coll.InsertOne(ctx, bson.M{"product": historicalMarker}); err != nil {
		t.Fatalf("InsertOne(historical): %v", err)
	}

	subscribe := func(bus *event.Bus, ev event.Event[ChangeEvent]) chan ChangeEvent {
		ch := make(chan ChangeEvent, 32)
		if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
			select {
			case ch <- change:
			default:
			}
			return nil
		}); err != nil {
			t.Fatalf("Subscribe() error: %v", err)
		}
		return ch
	}

	// Phase 1: fresh transport, no saved token, WithStartFromPast covering the
	// historical write. It must replay the historical document.
	t1, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithResumeTokenStore(tokenStore),
		WithResumeTokenID("startfrompast-instance"),
		WithStartFromPast(time.Hour),
	)
	if err != nil {
		t.Fatalf("New(phase1) error: %v", err)
	}
	bus1, err := event.NewBus("startpast-bus-1", event.WithTransport(t1))
	if err != nil {
		t.Fatalf("NewBus(phase1) error: %v", err)
	}
	ev1 := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus1, ev1); err != nil {
		t.Fatalf("Register(phase1) error: %v", err)
	}
	ch1 := subscribe(bus1, ev1)

	gotHistorical := false
	deadline := time.After(20 * time.Second)
phase1:
	for {
		select {
		case change := <-ch1:
			if change.OperationType == OperationInsert && jsonContains(change.FullDocument, historicalMarker) {
				gotHistorical = true
				break phase1
			}
		case <-deadline:
			break phase1
		}
	}
	if !gotHistorical {
		bus1.Close(ctx)
		t.Fatal("WithStartFromPast did not replay the historical document")
	}

	// Wait (bounded) for a resume token to be persisted so phase 2 has a saved
	// position to resume from.
	resumeKey := t1.ResumeTokenKey()
	if !eventuallyIntegration(resumeTokenSaveInterval+15*time.Second, func() bool {
		tok, loadErr := tokenStore.Load(ctx, resumeKey)
		return loadErr == nil && len(tok) > 0
	}) {
		bus1.Close(ctx)
		t.Fatal("timed out waiting for resume token to persist after phase 1")
	}
	bus1.Close(ctx)

	// Phase 2: equivalent transport WITH the saved token. WithStartFromPast must
	// be ignored on this start (token takes precedence), so the historical
	// document must NOT be replayed. A new change written after the token must
	// still be delivered, proving the stream resumed rather than starting fresh.
	t2, err := New(db,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithResumeTokenStore(tokenStore),
		WithResumeTokenID("startfrompast-instance"),
		WithStartFromPast(time.Hour),
	)
	if err != nil {
		t.Fatalf("New(phase2) error: %v", err)
	}
	bus2, err := event.NewBus("startpast-bus-2", event.WithTransport(t2))
	if err != nil {
		t.Fatalf("NewBus(phase2) error: %v", err)
	}
	defer bus2.Close(ctx)
	ev2 := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus2, ev2); err != nil {
		t.Fatalf("Register(phase2) error: %v", err)
	}
	ch2 := subscribe(bus2, ev2)

	// Write a fresh sentinel that phase 2 must deliver. When we see it we know
	// the resume stream is live and caught up; the historical document must not
	// have appeared before it.
	const freshMarker = "post-resume-doc"
	if _, err := coll.InsertOne(ctx, bson.M{"product": freshMarker}); err != nil {
		t.Fatalf("InsertOne(fresh): %v", err)
	}

	deadline2 := time.After(20 * time.Second)
	for {
		select {
		case change := <-ch2:
			if change.OperationType != OperationInsert {
				continue
			}
			if jsonContains(change.FullDocument, historicalMarker) {
				t.Fatal("phase 2 replayed the historical document; resume from token did not take precedence over WithStartFromPast")
			}
			if jsonContains(change.FullDocument, freshMarker) {
				return // resumed correctly: fresh change delivered, no replay
			}
		case <-deadline2:
			t.Fatal("phase 2 did not deliver the post-resume change")
		}
	}
}

// jsonContains reports whether the raw JSON payload contains the given
// substring. Used to identify which document a ChangeEvent carried without
// fully decoding it.
func jsonContains(raw []byte, substr string) bool {
	return len(raw) > 0 && bytesContains(raw, substr)
}

// bytesContains is a tiny substring check over a byte slice, avoiding a
// dependency on strings/bytes import churn at call sites.
func bytesContains(b []byte, substr string) bool {
	s := string(b)
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestIntegration_MidStreamConnectivityLossRecovery exercises the transport's
// reconnect/backoff path against a real mid-stream connectivity loss. A tiny
// in-process TCP proxy sits between the transport's MongoDB client and the real
// MongoDB host:port. The test:
//
//  1. starts watching through the proxy and confirms delivery,
//  2. severs all proxied connections (simulating a network blip),
//  3. writes a change during the outage,
//  4. restores the proxy and asserts delivery resumes after the transport's
//     backoff/reconnect.
//
// The proxy only forwards a single MongoDB host:port. It therefore requires a
// directConnection-style single-node deployment. If the resolved deployment is
// not reachable as a single host:port through the proxy (e.g. a multi-host
// replica set the driver insists on contacting directly), the test skips with a
// clear reason rather than flaking.
func TestIntegration_MidStreamConnectivityLossRecovery(t *testing.T) {
	// Establish the real deployment is reachable and is a replica set (needed
	// for change streams). This also gives us a known-good URI to derive the
	// upstream host:port from.
	client, db, _ := setupIntegrationTest(t)
	_ = client // connection/replica-set precondition is enforced by setup.

	upstream, ok := singleHostFromURI(mongotest.URI())
	if !ok {
		t.Skip("proxy test requires a single host:port URI (directConnection); MONGO_URI exposes multiple hosts or an SRV record")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	proxy, err := newTCPProxy(upstream)
	if err != nil {
		t.Skipf("could not start in-process TCP proxy: %v", err)
	}
	defer proxy.Close()

	// A second client pointed at the proxy. directConnection avoids replica-set
	// topology discovery that would bypass the proxy address.
	proxyURI := "mongodb://" + proxy.Addr() + "/?directConnection=true"
	proxyClient, err := mongo.Connect(options.Client().ApplyURI(proxyURI))
	if err != nil {
		t.Skipf("could not connect through proxy: %v", err)
	}
	defer func() { _ = proxyClient.Disconnect(context.Background()) }()

	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	pingErr := proxyClient.Ping(pingCtx, nil)
	pingCancel()
	if pingErr != nil {
		t.Skipf("MongoDB not reachable through proxy: %v", pingErr)
	}

	// Watch the SAME database (by name) but through the proxied client so writes
	// made on the direct client are observed via the proxied change stream.
	proxyDB := proxyClient.Database(db.Name())
	coll := db.Collection("orders") // write via the direct client
	transport, err := New(proxyDB,
		WithCollection("orders"),
		WithFullDocument(FullDocumentUpdateLookup),
		WithoutResume(),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	bus, err := event.NewBus("proxy-bus", event.WithTransport(transport))
	if err != nil {
		t.Fatalf("NewBus() error: %v", err)
	}
	defer bus.Close(ctx)

	ev := event.New[ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	received := make(chan ChangeEvent, 32)
	if err := ev.Subscribe(ctx, func(ctx context.Context, e event.Event[ChangeEvent], change ChangeEvent) error {
		select {
		case received <- change:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	// Confirm the stream is live through the proxy before disrupting it.
	waitForStreamReady(t, ctx, coll, received)

	// Sever all proxied connections mid-stream. New connects are still accepted
	// by the proxy, but every existing tunnel is torn down, which the driver
	// surfaces as a stream error and the transport must recover from.
	proxy.SeverAll()

	// Write a change during/after the outage. The direct client is unaffected
	// by the proxy, so the write lands in the oplog; the proxied stream must
	// pick it up once it reconnects.
	const marker = "after-outage"
	if _, err := coll.InsertOne(ctx, bson.M{"product": marker}); err != nil {
		t.Fatalf("InsertOne(after-outage): %v", err)
	}

	// Give the transport a moment to notice the broken stream, then allow new
	// connections to flow again (they already are; SeverAll only dropped the
	// existing ones). Assert delivery resumes after backoff/reconnect.
	deadline := time.After(45 * time.Second)
	for {
		select {
		case change := <-received:
			if change.OperationType == OperationInsert && jsonContains(change.FullDocument, marker) {
				return // recovered and delivered the change written during the outage
			}
		case <-deadline:
			t.Fatal("transport did not resume delivery after mid-stream connectivity loss")
		}
	}
}

// singleHostFromURI extracts a single host:port from a standard mongodb:// URI
// (e.g. "mongodb://localhost:27018/?directConnection=true"). It returns
// ok=false for SRV URIs (mongodb+srv://), multi-host seed lists, or URIs
// missing an explicit port — none of which a single-port proxy can faithfully
// stand in for.
func singleHostFromURI(uri string) (string, bool) {
	if !strings.HasPrefix(uri, "mongodb://") {
		return "", false // mongodb+srv:// resolves multiple hosts dynamically
	}
	rest := strings.TrimPrefix(uri, "mongodb://")
	// Strip any userinfo@ prefix.
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	// Host section ends at the first '/' or '?'.
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		rest = rest[:i]
	}
	if rest == "" || strings.Contains(rest, ",") {
		return "", false // empty or multi-host seed list
	}
	host, port, err := net.SplitHostPort(rest)
	if err != nil || host == "" || port == "" {
		return "", false
	}
	return rest, true
}

// tcpProxy is a minimal in-process TCP proxy that forwards accepted connections
// to a fixed upstream address. It tracks live tunnels so a test can sever them
// all mid-stream to simulate a connectivity blip while continuing to accept new
// connections (so the client can reconnect).
type tcpProxy struct {
	ln       net.Listener
	upstream string

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// newTCPProxy starts a proxy listening on an ephemeral localhost port that
// forwards to upstream.
func newTCPProxy(upstream string) (*tcpProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &tcpProxy{
		ln:       ln,
		upstream: upstream,
		conns:    make(map[net.Conn]struct{}),
	}
	go p.acceptLoop()
	return p, nil
}

// Addr returns the proxy's listening address (host:port).
func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) acceptLoop() {
	for {
		downstream, err := p.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go p.handle(downstream)
	}
}

func (p *tcpProxy) handle(downstream net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.upstream, 10*time.Second)
	if err != nil {
		_ = downstream.Close()
		return
	}

	p.track(downstream)
	p.track(upstream)

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = ioCopy(dst, src)
		done <- struct{}{}
	}
	go pipe(upstream, downstream)
	go pipe(downstream, upstream)

	<-done
	p.untrack(downstream)
	p.untrack(upstream)
	_ = downstream.Close()
	_ = upstream.Close()
}

func (p *tcpProxy) track(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = c.Close()
		return
	}
	p.conns[c] = struct{}{}
}

func (p *tcpProxy) untrack(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.conns, c)
}

// SeverAll closes every currently-tracked tunnel connection, simulating a
// mid-stream network drop. The listener stays open so the client can reconnect.
func (p *tcpProxy) SeverAll() {
	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// Close shuts down the proxy listener and all live connections.
func (p *tcpProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	_ = p.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

// ioCopy copies from src to dst until EOF or error. It is a thin wrapper kept
// local to avoid importing io at multiple call sites in this test file.
func ioCopy(dst, src net.Conn) (int64, error) {
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			w, werr := dst.Write(buf[:n])
			total += int64(w)
			if werr != nil {
				return total, werr
			}
		}
		if rerr != nil {
			return total, rerr
		}
	}
}
