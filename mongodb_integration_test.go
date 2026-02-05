package mongodb

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3"
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
		client.Disconnect(ctx)
		t.Skipf("MongoDB not available: %v", err)
	}

	// Check for replica set
	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.M{"replSetGetStatus": 1}).Decode(&result); err != nil {
		cancel()
		client.Disconnect(ctx)
		t.Skipf("MongoDB replica set not available: %v", err)
	}

	dbName := "test_event_mongodb_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)

	cleanup := func() {
		db.Drop(context.Background())
		client.Disconnect(context.Background())
		cancel()
	}

	return client, db, cleanup
}

func TestIntegration_ChangeStreamInsert(t *testing.T) {
	_, db, cleanup := setupIntegrationTest(t)
	defer cleanup()

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

	// Wait for change stream to be ready
	time.Sleep(500 * time.Millisecond)

	// Insert a document
	coll := db.Collection("orders")
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
	_, db, cleanup := setupIntegrationTest(t)
	defer cleanup()

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

	time.Sleep(500 * time.Millisecond)

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
	_, db, cleanup := setupIntegrationTest(t)
	defer cleanup()

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

	time.Sleep(500 * time.Millisecond)

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
	_, db, cleanup := setupIntegrationTest(t)
	defer cleanup()

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

	time.Sleep(500 * time.Millisecond)

	// Perform multiple operations
	coll := db.Collection("orders")
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
	_, db, cleanup := setupIntegrationTest(t)
	defer cleanup()

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
	_, db, cleanup := setupIntegrationTest(t)
	defer cleanup()

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

	time.Sleep(500 * time.Millisecond)

	// Insert into different collections
	db.Collection("orders").InsertOne(ctx, bson.M{"type": "order"})
	db.Collection("products").InsertOne(ctx, bson.M{"type": "product"})
	db.Collection("users").InsertOne(ctx, bson.M{"type": "user"})

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
	_, db, cleanup := setupIntegrationTest(t)
	defer cleanup()

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

	time.Sleep(500 * time.Millisecond)

	// Insert first document
	coll := db.Collection("orders")
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
