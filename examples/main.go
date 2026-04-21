// Package main provides a comprehensive example and integration test app for event-mongodb.
//
// # MongoDB Requirements
//
// Most examples require a MongoDB replica set (for transactions and change streams):
//
//	docker run -d -p 27017:27017 --name mongo mongo:8.0 --replSet rs0
//	docker exec mongo mongosh --eval "rs.initiate()"
//
// # Running Examples
//
//	EXAMPLE=all               Run all integration tests
//	EXAMPLE=transaction       MongoDB transaction manager
//	EXAMPLE=checkpoint        MongoDB checkpoint store
//	EXAMPLE=schema            MongoDB schema provider
//	EXAMPLE=store             Generic MongoDB CRUD store
//	EXAMPLE=idempotency-mongo MongoDB idempotency store
//	EXAMPLE=monitor-mongo     MongoDB monitor store
//	EXAMPLE=outbox            MongoDB outbox pattern
//	EXAMPLE=codec             BSON transport codec (no MongoDB)
//	EXAMPLE=payload           BSON payload codec (no MongoDB)
//	EXAMPLE=basic             Change stream basics
//	EXAMPLE=workerpool        WorkerPool with MongoDB coordinator
//	EXAMPLE=idempotency       In-memory idempotency
//	EXAMPLE=full              Full production setup
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	mongodb "github.com/rbaliyan/event-mongodb"
	"github.com/rbaliyan/event-mongodb/checkpoint"
	mongocodec "github.com/rbaliyan/event-mongodb/codec"
	mongodist "github.com/rbaliyan/event-mongodb/distributed"
	mongoidempotency "github.com/rbaliyan/event-mongodb/idempotency"
	mongomonitor "github.com/rbaliyan/event-mongodb/monitor"
	"github.com/rbaliyan/event-mongodb/outbox"
	mongopayload "github.com/rbaliyan/event-mongodb/payload"
	mongoschema "github.com/rbaliyan/event-mongodb/schema"
	mongostore "github.com/rbaliyan/event-mongodb/store"
	mongotx "github.com/rbaliyan/event-mongodb/transaction"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/distributed"
	"github.com/rbaliyan/event/v3/idempotency"
	evtmonitor "github.com/rbaliyan/event/v3/monitor"
	evtoutbox "github.com/rbaliyan/event/v3/outbox"
	evtschema "github.com/rbaliyan/event/v3/schema"
	evtstore "github.com/rbaliyan/event/v3/store"
	channeltransport "github.com/rbaliyan/event/v3/transport/channel"
	"github.com/rbaliyan/event/v3/transport/message"
)

// Order is the example domain type used across several examples.
type Order struct {
	ID         bson.ObjectID `bson:"_id,omitempty" json:"id"`
	CustomerID string        `bson:"customer_id" json:"customer_id"`
	Product    string        `bson:"product" json:"product"`
	Amount     float64       `bson:"amount" json:"amount"`
	Status     string        `bson:"status" json:"status"`
	CreatedAt  time.Time     `bson:"created_at" json:"created_at"`
}

// ProductDoc is a simple document that satisfies the store.MongoDocument interface.
type ProductDoc struct {
	ID        string    `bson:"_id"`
	Name      string    `bson:"name"`
	Price     float64   `bson:"price"`
	CreatedAt time.Time `bson:"created_at"`
}

func (d ProductDoc) GetID() string           { return d.ID }
func (d ProductDoc) GetCreatedAt() time.Time { return d.CreatedAt }

var _ mongostore.MongoDocument = ProductDoc{} // compile-time: GetID() + GetCreatedAt()

func main() {
	example := os.Getenv("EXAMPLE")
	if example == "" {
		example = "basic"
	}

	switch example {
	// Integration tests (need MongoDB)
	case "all":
		runAllTests()
	case "transaction":
		runTransactionExample()
	case "checkpoint":
		runCheckpointExample()
	case "schema":
		runSchemaExample()
	case "store":
		runStoreExample()
	case "idempotency-mongo":
		runMongoIdempotencyExample()
	case "monitor-mongo":
		runMonitorExample()
	case "outbox":
		runOutboxExample()

	// Pure codec tests (no MongoDB needed)
	case "codec":
		runCodecExample()
	case "payload":
		runPayloadExample()

	// Change stream examples (need MongoDB replica set)
	case "basic":
		runBasicExample()
	case "workerpool":
		runWorkerPoolExample()
	case "idempotency":
		runIdempotencyExample()
	case "full":
		runFullSetupExample()

	default:
		log.Fatalf("unknown example %q (see package comment for valid values)", example)
	}
}

// mongoURI returns the MongoDB URI from the environment, defaulting to localhost.
func mongoURI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27017"
}

// connectMongo creates a MongoDB client and returns the client and a test database.
func connectMongo(ctx context.Context) (*mongo.Client, *mongo.Database) {
	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI()))
	if err != nil {
		log.Fatal("connect:", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("ping MongoDB (%s): %v\nEnsure MongoDB is running with a replica set.", mongoURI(), err)
	}
	return client, client.Database("event_mongodb_examples")
}

func check(label string, err error) {
	if err != nil {
		log.Fatalf("[FAIL] %s: %v", label, err)
	}
}

func pass(label string) {
	fmt.Printf("[PASS] %s\n", label)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// =============================================================================
// EXAMPLE=all — runs every integration test in sequence
// =============================================================================

func runAllTests() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, db := connectMongo(ctx)
	defer func() { _ = client.Disconnect(context.Background()) }()

	fmt.Printf("Connected to MongoDB: %s\n\n", mongoURI())

	runTransactionTest(ctx, client, db)
	runCheckpointTest(ctx, db)
	runSchemaTest(ctx, db)
	runStoreTest(ctx, db)
	runMongoIdempotencyTest(ctx, db)
	runMonitorTest(ctx, db)
	runOutboxTest(ctx, client, db)

	fmt.Println("\nAll integration tests passed.")
}

// =============================================================================
// EXAMPLE=transaction — transaction.MongoManager
// =============================================================================

func runTransactionExample() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, db := connectMongo(ctx)
	defer func() { _ = client.Disconnect(context.Background()) }()
	runTransactionTest(ctx, client, db)
}

func runTransactionTest(ctx context.Context, client *mongo.Client, db *mongo.Database) {
	const label = "transaction"
	fmt.Printf("[TEST] %s\n", label)

	mgr, err := mongotx.NewMongoManager(client)
	check(label+": NewMongoManager", err)

	col := db.Collection("tx_test_orders")
	defer func() { _ = col.Drop(context.Background()) }()

	// ExecuteWithContext runs a function inside a MongoDB transaction.
	// The context passed to fn has the session embedded (MongoDB v2 driver pattern).
	err = mgr.ExecuteWithContext(ctx, func(txCtx context.Context) error {
		order := bson.M{"_id": bson.NewObjectID(), "product": "widget", "amount": 9.99}
		_, err := col.InsertOne(txCtx, order)
		return err
	})
	check(label+": Execute insert", err)

	// Verify document persisted after commit.
	n, err := col.CountDocuments(ctx, bson.M{})
	check(label+": count", err)
	if n != 1 {
		log.Fatalf("[FAIL] %s: expected 1 document, got %d", label, n)
	}

	// Manual Begin / Commit flow.
	tx, err := mgr.Begin(ctx)
	check(label+": Begin", err)
	provider, ok := tx.(mongotx.MongoSessionProvider)
	if !ok {
		log.Fatalf("[FAIL] %s: tx does not implement MongoSessionProvider", label)
	}
	_, err = col.InsertOne(provider.Context(), bson.M{"_id": bson.NewObjectID(), "product": "gadget"})
	check(label+": insert in manual tx", err)
	check(label+": Commit", tx.Commit())

	n, err = col.CountDocuments(ctx, bson.M{})
	check(label+": count after commit", err)
	if n != 2 {
		log.Fatalf("[FAIL] %s: expected 2 documents after commit, got %d", label, n)
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=checkpoint — checkpoint.MongoStore
// =============================================================================

func runCheckpointExample() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, db := connectMongo(ctx)
	runCheckpointTest(ctx, db)
}

func runCheckpointTest(ctx context.Context, db *mongo.Database) {
	const label = "checkpoint"
	fmt.Printf("[TEST] %s\n", label)

	col := db.Collection("event_checkpoints")
	defer func() { _ = col.Drop(context.Background()) }()

	store, err := checkpoint.NewMongoStore(col, checkpoint.WithMongoTTL(24*time.Hour))
	check(label+": NewMongoStore", err)
	check(label+": EnsureIndexes", store.EnsureIndexes(ctx))

	// Save a checkpoint.
	subID := "orders-processor-v1"
	pos := time.Now().Truncate(time.Millisecond)
	check(label+": Save", store.Save(ctx, subID, pos))

	// Load it back and verify.
	got, err := store.Load(ctx, subID)
	check(label+": Load", err)
	if !got.Equal(pos) {
		log.Fatalf("[FAIL] %s: loaded %v, want %v", label, got, pos)
	}

	// Load a non-existent subscriber returns zero time.
	zero, err := store.Load(ctx, "nonexistent")
	check(label+": Load nonexistent", err)
	if !zero.IsZero() {
		log.Fatalf("[FAIL] %s: expected zero time for nonexistent, got %v", label, zero)
	}

	// Update the checkpoint (upsert).
	pos2 := pos.Add(10 * time.Second)
	check(label+": Save update", store.Save(ctx, subID, pos2))
	got2, err := store.Load(ctx, subID)
	check(label+": Load after update", err)
	if !got2.Equal(pos2) {
		log.Fatalf("[FAIL] %s: expected %v after update, got %v", label, pos2, got2)
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=schema — schema.MongoProvider
// =============================================================================

func runSchemaExample() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, db := connectMongo(ctx)
	runSchemaTest(ctx, db)
}

func runSchemaTest(ctx context.Context, db *mongo.Database) {
	const label = "schema"
	fmt.Printf("[TEST] %s\n", label)

	// publisher can be nil — schema changes are tracked in MongoDB only.
	provider, err := mongoschema.NewMongoProvider(db, nil)
	check(label+": NewMongoProvider", err)

	// Switch to test collection before EnsureIndexes to avoid touching production-named collection.
	provider = provider.WithCollection("test_event_schemas")
	defer func() { _ = provider.Collection().Drop(context.Background()) }()
	check(label+": EnsureIndexes", provider.EnsureIndexes(ctx))

	schema := &evtschema.EventSchema{
		Name:              "order.created",
		Version:           1,
		SubTimeout:        30 * time.Second,
		MaxRetries:        3,
		EnableMonitor:     true,
		EnableIdempotency: true,
	}

	// Set stores the schema.
	check(label+": Set v1", provider.Set(ctx, schema))

	// Get retrieves it.
	got, err := provider.Get(ctx, "order.created")
	check(label+": Get", err)
	if got == nil {
		log.Fatalf("[FAIL] %s: Get returned nil", label)
	}
	if got.Version != 1 || got.MaxRetries != 3 {
		log.Fatalf("[FAIL] %s: schema mismatch: %+v", label, got)
	}

	// Get nonexistent returns nil, nil.
	missing, err := provider.Get(ctx, "nonexistent.event")
	check(label+": Get nonexistent", err)
	if missing != nil {
		log.Fatalf("[FAIL] %s: expected nil for missing schema", label)
	}

	// Update to v2.
	schema.Version = 2
	schema.MaxRetries = 5
	check(label+": Set v2", provider.Set(ctx, schema))
	got2, err := provider.Get(ctx, "order.created")
	check(label+": Get v2", err)
	if got2.Version != 2 || got2.MaxRetries != 5 {
		log.Fatalf("[FAIL] %s: expected v2 schema, got %+v", label, got2)
	}

	// List returns all schemas.
	all, err := provider.List(ctx)
	check(label+": List", err)
	if len(all) != 1 {
		log.Fatalf("[FAIL] %s: List returned %d schemas, want 1", label, len(all))
	}

	// Publisher callback is invoked on Set when not nil.
	published := make([]evtschema.SchemaChangeEvent, 0)
	providerWithPub, err := mongoschema.NewMongoProvider(db,
		func(_ context.Context, ev evtschema.SchemaChangeEvent) error {
			published = append(published, ev)
			return nil
		},
	)
	check(label+": NewMongoProvider with publisher", err)
	providerWithPub = providerWithPub.WithCollection("test_event_schemas")
	// Use a fresh schema object to avoid sharing state with the earlier assertions.
	schema3 := &evtschema.EventSchema{
		Name:    "order.created",
		Version: 3,
	}
	check(label+": Set with publisher", providerWithPub.Set(ctx, schema3))
	if len(published) != 1 || published[0].Version != 3 {
		log.Fatalf("[FAIL] %s: expected 1 publish event at v3, got %v", label, published)
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=store — store.MongoStore[T] (generic CRUD)
// =============================================================================

func runStoreExample() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, db := connectMongo(ctx)
	runStoreTest(ctx, db)
}

func runStoreTest(ctx context.Context, db *mongo.Database) {
	const label = "store"
	fmt.Printf("[TEST] %s\n", label)

	col := db.Collection("test_products")
	defer func() { _ = col.Drop(context.Background()) }()

	s := mongostore.NewMongoStore[ProductDoc](col,
		mongostore.WithMongoDefaultLimit[ProductDoc](50),
	)
	check(label+": EnsureIndexes", s.EnsureIndexes(ctx))

	now := time.Now().Truncate(time.Millisecond)
	p1 := ProductDoc{ID: "prod-1", Name: "Widget", Price: 9.99, CreatedAt: now}
	p2 := ProductDoc{ID: "prod-2", Name: "Gadget", Price: 24.99, CreatedAt: now.Add(time.Second)}
	p3 := ProductDoc{ID: "prod-3", Name: "Doohickey", Price: 4.99, CreatedAt: now.Add(2 * time.Second)}

	check(label+": Create p1", s.Create(ctx, p1))
	check(label+": Create p2", s.Create(ctx, p2))
	check(label+": Create p3", s.Create(ctx, p3))

	// Get by ID.
	got, err := s.Get(ctx, "prod-2")
	check(label+": Get", err)
	if got.Name != "Gadget" || got.Price != 24.99 {
		log.Fatalf("[FAIL] %s: Get returned %+v", label, got)
	}

	// List all — default order is ascending by created_at.
	page, err := s.List(ctx, evtstore.Filter{Limit: 10})
	check(label+": List", err)
	if len(page.Items) != 3 {
		log.Fatalf("[FAIL] %s: List returned %d items, want 3", label, len(page.Items))
	}
	if page.Items[0].ID != "prod-1" {
		log.Fatalf("[FAIL] %s: first item is %s, want prod-1", label, page.Items[0].ID)
	}

	// Count.
	n, err := s.Count(ctx, evtstore.Filter{})
	check(label+": Count", err)
	if n != 3 {
		log.Fatalf("[FAIL] %s: Count returned %d, want 3", label, n)
	}

	// Update.
	p2.Price = 19.99
	check(label+": Update", s.Update(ctx, p2))
	updated, err := s.Get(ctx, "prod-2")
	check(label+": Get after update", err)
	if updated.Price != 19.99 {
		log.Fatalf("[FAIL] %s: price after update is %f, want 19.99", label, updated.Price)
	}

	// Upsert (insert new).
	p4 := ProductDoc{ID: "prod-4", Name: "Thingamajig", Price: 14.99, CreatedAt: now.Add(3 * time.Second)}
	check(label+": Upsert", s.Upsert(ctx, p4))
	n2, err := s.Count(ctx, evtstore.Filter{})
	check(label+": Count after upsert", err)
	if n2 != 4 {
		log.Fatalf("[FAIL] %s: Count after upsert returned %d, want 4", label, n2)
	}

	// Delete.
	check(label+": Delete", s.Delete(ctx, "prod-3"))
	n3, err := s.Count(ctx, evtstore.Filter{})
	check(label+": Count after delete", err)
	if n3 != 3 {
		log.Fatalf("[FAIL] %s: Count after delete returned %d, want 3", label, n3)
	}

	// Pagination.
	page1, err := s.List(ctx, evtstore.Filter{Limit: 2})
	check(label+": List page 1", err)
	if len(page1.Items) != 2 || !page1.HasMore() {
		log.Fatalf("[FAIL] %s: expected 2 items and more pages, got %d items, hasMore=%v",
			label, len(page1.Items), page1.HasMore())
	}
	page2, err := s.List(ctx, evtstore.Filter{Limit: 2, Cursor: page1.NextCursor})
	check(label+": List page 2", err)
	if len(page2.Items) != 1 {
		log.Fatalf("[FAIL] %s: expected 1 item on page 2, got %d", label, len(page2.Items))
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=idempotency-mongo — idempotency.MongoStore
// =============================================================================

func runMongoIdempotencyExample() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, db := connectMongo(ctx)
	runMongoIdempotencyTest(ctx, db)
}

func runMongoIdempotencyTest(ctx context.Context, db *mongo.Database) {
	const label = "idempotency-mongo"
	fmt.Printf("[TEST] %s\n", label)

	store, err := mongoidempotency.NewMongoStore(db,
		mongoidempotency.WithMongoTTL(1*time.Hour),
		mongoidempotency.WithMongoCollection("test_idempotency"),
		mongoidempotency.WithMongoCleanupInterval(0), // disable background cleanup
	)
	check(label+": NewMongoStore", err)
	defer func() { _ = db.Collection("test_idempotency").Drop(context.Background()) }()
	check(label+": EnsureIndexes", store.EnsureIndexes(ctx))

	msgID := "evt-abc-123"

	// First check: not a duplicate.
	dup, err := store.IsDuplicate(ctx, msgID)
	check(label+": IsDuplicate (first)", err)
	if dup {
		log.Fatalf("[FAIL] %s: expected false on first check", label)
	}

	// Mark as processed.
	check(label+": MarkProcessed", store.MarkProcessed(ctx, msgID))

	// Second check: now a duplicate.
	dup2, err := store.IsDuplicate(ctx, msgID)
	check(label+": IsDuplicate (second)", err)
	if !dup2 {
		log.Fatalf("[FAIL] %s: expected true after MarkProcessed", label)
	}

	// NewMongoStoreWithCollection — custom collection name.
	store2, err := mongoidempotency.NewMongoStoreWithCollection(db, "test_idempotency_custom",
		mongoidempotency.WithMongoCleanupInterval(0),
	)
	check(label+": NewMongoStoreWithCollection", err)
	defer func() { _ = db.Collection("test_idempotency_custom").Drop(context.Background()) }()

	dup3, err := store2.IsDuplicate(ctx, "fresh-msg")
	check(label+": IsDuplicate on custom store", err)
	if dup3 {
		log.Fatalf("[FAIL] %s: separate store should not see msg from first store", label)
	}

	// IsDuplicateTx works the same as IsDuplicate outside a session context.
	dup4, err := store.IsDuplicateTx(ctx, msgID)
	check(label+": IsDuplicateTx", err)
	if !dup4 {
		log.Fatalf("[FAIL] %s: IsDuplicateTx should report duplicate", label)
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=monitor-mongo — monitor.MongoStore
// =============================================================================

func runMonitorExample() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, db := connectMongo(ctx)
	runMonitorTest(ctx, db)
}

func runMonitorTest(ctx context.Context, db *mongo.Database) {
	const label = "monitor-mongo"
	fmt.Printf("[TEST] %s\n", label)

	ms, err := mongomonitor.NewMongoStore(db)
	check(label+": NewMongoStore", err)
	ms = ms.WithCollection("test_monitor_entries")
	defer func() { _ = ms.Collection().Drop(context.Background()) }()
	check(label+": EnsureIndexes", ms.EnsureIndexes(ctx))

	eventID := "evt-" + bson.NewObjectID().Hex()
	subID := "sub-broadcast-1"

	// RecordStart — record beginning of event processing.
	check(label+": RecordStart", ms.RecordStart(ctx, event.RecordStartParams{
		EventID:        eventID,
		SubscriptionID: subID,
		EventName:      "order.created",
		BusID:          "test-bus",
		SubscriberName: "order-processor",
	}))

	// Get the entry back.
	entry, err := ms.Get(ctx, eventID, subID)
	check(label+": Get", err)
	if entry == nil {
		log.Fatalf("[FAIL] %s: Get returned nil entry", label)
	}
	if entry.Status != evtmonitor.StatusPending {
		log.Fatalf("[FAIL] %s: expected StatusPending, got %s", label, entry.Status)
	}

	// UpdateStatus to completed.
	check(label+": UpdateStatus completed", ms.UpdateStatus(ctx, eventID, subID,
		evtmonitor.StatusCompleted, nil, 42*time.Millisecond))

	// Verify status change.
	updated, err := ms.Get(ctx, eventID, subID)
	check(label+": Get after UpdateStatus", err)
	if updated.Status != evtmonitor.StatusCompleted {
		log.Fatalf("[FAIL] %s: expected StatusCompleted, got %s", label, updated.Status)
	}

	// List with filter.
	page, err := ms.List(ctx, evtmonitor.Filter{EventName: "order.created"})
	check(label+": List", err)
	if len(page.Entries) != 1 {
		log.Fatalf("[FAIL] %s: List returned %d entries, want 1", label, len(page.Entries))
	}

	// Count.
	n, err := ms.Count(ctx, evtmonitor.Filter{Status: []evtmonitor.Status{evtmonitor.StatusCompleted}})
	check(label+": Count completed", err)
	if n != 1 {
		log.Fatalf("[FAIL] %s: Count returned %d, want 1", label, n)
	}

	// GetByEventID — returns all entries for an event (Broadcast has one per sub).
	entries, err := ms.GetByEventID(ctx, eventID)
	check(label+": GetByEventID", err)
	if len(entries) != 1 {
		log.Fatalf("[FAIL] %s: GetByEventID returned %d entries, want 1", label, len(entries))
	}

	// Summary — aggregates pending/completed/failed counts per event.
	summary, err := ms.Summary(ctx, evtmonitor.Filter{})
	check(label+": Summary", err)
	if summary == nil {
		log.Fatalf("[FAIL] %s: Summary returned nil", label)
	}
	if summary.TotalEntries != 1 {
		log.Fatalf("[FAIL] %s: Summary.TotalEntries = %d, want 1", label, summary.TotalEntries)
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=outbox — outbox.MongoStore + relay + publisher
// =============================================================================

func runOutboxExample() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, db := connectMongo(ctx)
	defer func() { _ = client.Disconnect(context.Background()) }()
	runOutboxTest(ctx, client, db)
}

func runOutboxTest(ctx context.Context, client *mongo.Client, db *mongo.Database) {
	const label = "outbox"
	fmt.Printf("[TEST] %s\n", label)

	store, err := outbox.NewMongoStore(db, outbox.WithCollection("test_outbox"))
	check(label+": NewMongoStore", err)
	defer func() { _ = store.Collection().Drop(context.Background()) }()
	check(label+": EnsureIndexes", store.EnsureIndexes(ctx))

	// Store a message in the outbox.
	payload, _ := json.Marshal(map[string]string{"product": "widget", "qty": "3"})
	check(label+": Store", store.Store(ctx, "order.created", "evt-001", payload, map[string]string{
		"source": "checkout-service",
	}))

	// Verify pending count.
	n, err := store.Count(ctx, evtoutbox.StatusPending)
	check(label+": Count pending", err)
	if n != 1 {
		log.Fatalf("[FAIL] %s: expected 1 pending message, got %d", label, n)
	}

	// GetPending retrieves claimed messages (transitions them to processing state).
	pending, err := store.GetPending(ctx, 10)
	check(label+": GetPending", err)
	if len(pending) != 1 {
		log.Fatalf("[FAIL] %s: expected 1 pending message, got %d", label, len(pending))
	}
	if pending[0].EventName != "order.created" {
		log.Fatalf("[FAIL] %s: expected order.created, got %s", label, pending[0].EventName)
	}

	// MarkPublished transitions the message to published state.
	// In normal operation the relay does this automatically.
	check(label+": MarkPublishedByEventID", store.MarkPublishedByEventID(ctx, "evt-001"))

	// Pending count should now be zero.
	n2, err := store.Count(ctx, evtoutbox.StatusPending)
	check(label+": Count pending after publish", err)
	if n2 != 0 {
		log.Fatalf("[FAIL] %s: expected 0 pending after publish, got %d", label, n2)
	}

	// ─── MongoPublisher: transactional outbox write ───────────────────────────
	publisher, err := outbox.NewMongoPublisher(client, db, outbox.WithCollection("test_outbox"))
	check(label+": NewMongoPublisher", err)

	// PublishWithTransaction atomically inserts the business doc + outbox message.
	ordersCol := db.Collection("test_orders")
	defer func() { _ = ordersCol.Drop(context.Background()) }()

	// PublishWithTransaction takes (ctx, eventName, payload any, metadata, fn).
	// payload is marshalled to JSON internally; eventID is auto-generated.
	err = publisher.PublishWithTransaction(ctx, "order.shipped",
		map[string]string{"orderId": "ord-99", "status": "shipped"},
		map[string]string{"trace": "xyz"},
		func(txCtx context.Context) error {
			_, err := ordersCol.InsertOne(txCtx, bson.M{"_id": "ord-99", "status": "shipped"})
			return err
		},
	)
	check(label+": PublishWithTransaction", err)

	n3, err := store.Count(ctx, evtoutbox.StatusPending)
	check(label+": Count after PublishWithTransaction", err)
	if n3 != 1 {
		log.Fatalf("[FAIL] %s: expected 1 pending after PublishWithTransaction, got %d", label, n3)
	}

	// ─── MongoRelay: polling relay publishes to in-memory transport ──────────
	chanT := channeltransport.New()
	relay := outbox.NewMongoRelay(store, chanT).
		WithPollDelay(0).
		WithBatchSize(10).
		WithCleanupAge(1 * time.Hour)

	// PublishOnce picks up the pending message and publishes it to the transport.
	check(label+": relay.PublishOnce", relay.PublishOnce(ctx))

	n4, err := store.Count(ctx, evtoutbox.StatusPending)
	check(label+": Count after relay PublishOnce", err)
	if n4 != 0 {
		log.Fatalf("[FAIL] %s: expected 0 pending after relay, got %d", label, n4)
	}

	// ─── ChangeStreamRelay: real-time relay (requires replica set) ───────────
	// NewChangeStreamRelay creates a relay that watches the outbox collection via
	// a MongoDB Change Stream instead of polling. Call csRelay.Start(ctx) to run.
	outbox.NewChangeStreamRelay(store, chanT).
		WithBatchSize(10).
		WithCleanupAge(24 * time.Hour).
		WithStuckDuration(5 * time.Minute)

	// ─── MongoPublisher with BSON codec ────────────────────────────────────
	// WithCodec selects the encoding used to store and relay payloads.
	// mongocodec.BSON is preferred when payloads are MongoDB documents.
	bsonPublisher, err := outbox.NewMongoPublisher(client, db, outbox.WithCollection("test_outbox"))
	check(label+": NewMongoPublisher for BSON", err)
	if bsonPublisher.WithCodec(mongocodec.BSON{}) == nil {
		log.Fatalf("[FAIL] %s: WithCodec returned nil", label)
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=codec — codec.BSON transport codec (no MongoDB needed)
// =============================================================================

func runCodecExample() {
	const label = "codec"
	fmt.Printf("[TEST] %s\n", label)

	c := mongocodec.BSON{}

	if c.ContentType() != "application/bson" {
		log.Fatalf("[FAIL] %s: ContentType = %q, want application/bson", label, c.ContentType())
	}
	if c.Name() != "bson" {
		log.Fatalf("[FAIL] %s: Name = %q, want bson", label, c.Name())
	}

	// The BSON codec stores the message payload as a bson.Raw subdocument,
	// so the payload must be valid BSON bytes (not JSON).
	payloadDoc, err := bson.Marshal(bson.M{"order": "ord-1"})
	check(label+": marshal payload", err)
	meta := map[string]string{"trace-id": "abc-123", "source": "checkout"}
	ts := time.Now().Truncate(time.Millisecond)
	orig := message.New("msg-id-1", "order-bus", payloadDoc, meta,
		message.WithTimestamp(ts),
		message.WithRetryCount(2),
	)

	// Encode.
	data, err := c.Encode(orig)
	check(label+": Encode", err)
	if len(data) == 0 {
		log.Fatalf("[FAIL] %s: Encode returned empty bytes", label)
	}

	// Decode round-trip.
	decoded, err := c.Decode(data)
	check(label+": Decode", err)

	if decoded.ID() != orig.ID() {
		log.Fatalf("[FAIL] %s: ID mismatch: %q != %q", label, decoded.ID(), orig.ID())
	}
	if decoded.Source() != orig.Source() {
		log.Fatalf("[FAIL] %s: Source mismatch", label)
	}
	if !bytesEqual(decoded.Payload(), orig.Payload()) {
		log.Fatalf("[FAIL] %s: Payload mismatch: %q != %q", label, decoded.Payload(), orig.Payload())
	}
	if decoded.Metadata()["trace-id"] != "abc-123" {
		log.Fatalf("[FAIL] %s: Metadata not round-tripped", label)
	}
	if decoded.RetryCount() != 2 {
		log.Fatalf("[FAIL] %s: RetryCount mismatch: %d != 2", label, decoded.RetryCount())
	}
	if !decoded.Timestamp().Equal(ts) {
		log.Fatalf("[FAIL] %s: Timestamp mismatch: %v != %v", label, decoded.Timestamp(), ts)
	}

	pass(label)
}

// =============================================================================
// EXAMPLE=payload — payload.BSON payload codec (no MongoDB needed)
// =============================================================================

func runPayloadExample() {
	const label = "payload"
	fmt.Printf("[TEST] %s\n", label)

	c := mongopayload.BSON{}

	if c.ContentType() != "application/bson" {
		log.Fatalf("[FAIL] %s: ContentType = %q, want application/bson", label, c.ContentType())
	}

	// Encode a struct to BSON bytes.
	type Item struct {
		ID    string  `bson:"_id"`
		Price float64 `bson:"price"`
	}
	orig := Item{ID: "item-1", Price: 19.99}

	data, err := c.Encode(orig)
	check(label+": Encode", err)
	if len(data) == 0 {
		log.Fatalf("[FAIL] %s: Encode returned empty bytes", label)
	}

	// Decode back.
	var got Item
	check(label+": Decode", c.Decode(data, &got))
	if got.ID != orig.ID || got.Price != orig.Price {
		log.Fatalf("[FAIL] %s: round-trip mismatch: %+v != %+v", label, got, orig)
	}

	// WithPayloadCodec shows how to wire the BSON codec into an event.
	orderEvent := event.New[Order]("order.created", event.WithPayloadCodec(mongopayload.BSON{}))
	_ = orderEvent

	pass(label)
}

// =============================================================================
// EXAMPLE=basic — Change stream basics
// =============================================================================

func runBasicExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; cancel() }()

	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI()))
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
		mongodb.WithLogger(slog.Default()),
	)
	if err != nil {
		log.Fatal("transport:", err)
	}

	bus, err := event.NewBus("orders-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("bus:", err)
	}
	defer func() { _ = bus.Close(ctx) }()

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("register:", err)
	}

	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		fmt.Printf("[%s] %s.%s: %s (key: %s)\n",
			change.Timestamp.Format(time.RFC3339),
			change.Database, change.Collection,
			change.OperationType, change.DocumentKey,
		)
		switch change.OperationType {
		case mongodb.OperationInsert:
			if change.FullDocument != nil {
				fmt.Printf("  Document: %s\n", string(change.FullDocument))
			}
		case mongodb.OperationUpdate:
			if change.UpdateDesc != nil {
				fmt.Printf("  Updated fields: %v\n", change.UpdateDesc.UpdatedFields)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal("subscribe:", err)
	}

	fmt.Println("Watching for order changes. Press Ctrl+C to stop.")
	<-ctx.Done()
}

// =============================================================================
// EXAMPLE=workerpool — WorkerPool with MongoDB coordinator
// =============================================================================

func runWorkerPoolExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; cancel() }()

	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI()))
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal")

	// MongoDB coordinator lives in event-mongodb/distributed.
	claimer, err := mongodist.NewMongoStateManager(internalDB,
		mongodist.WithCollection("_order_worker_claims"),
		mongodist.WithCompletedTTL(24*time.Hour),
	)
	if err != nil {
		log.Fatal("claimer:", err)
	}
	if err := claimer.EnsureIndexes(ctx); err != nil {
		log.Fatal("claimer indexes:", err)
	}

	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
	)
	if err != nil {
		log.Fatal("transport:", err)
	}

	bus, err := event.NewBus("orders-worker-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("bus:", err)
	}
	defer func() { _ = bus.Close(ctx) }()

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("register:", err)
	}

	instanceID, _ := os.Hostname()
	if id := os.Getenv("INSTANCE_ID"); id != "" {
		instanceID = id
	}

	claimTTL := 5 * time.Minute
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		fmt.Printf("[%s] processing %s: %s\n", instanceID, change.OperationType, change.DocumentKey)
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("[%s] done: %s\n", instanceID, change.DocumentKey)
		return nil
	}, event.WithMiddleware(
		distributed.WorkerPoolMiddleware[mongodb.ChangeEvent](claimer, claimTTL),
	))
	if err != nil {
		log.Fatal("subscribe:", err)
	}

	recoveryRunner, err := distributed.NewRecoveryRunner(claimer,
		distributed.WithStaleTimeout(2*time.Minute),
		distributed.WithCheckInterval(30*time.Second),
	)
	if err != nil {
		log.Fatal("recovery:", err)
	}
	go recoveryRunner.Run(ctx)

	fmt.Printf("Worker %s ready. Press Ctrl+C to stop.\n", instanceID)
	<-ctx.Done()
}

// =============================================================================
// EXAMPLE=idempotency — in-memory idempotency store
// =============================================================================

func runIdempotencyExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; cancel() }()

	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI()))
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	idStore := idempotency.NewMemoryStore(24 * time.Hour)
	defer idStore.Close()

	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
	)
	if err != nil {
		log.Fatal("transport:", err)
	}

	bus, err := event.NewBus("orders-dedup-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("bus:", err)
	}
	defer func() { _ = bus.Close(ctx) }()

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("register:", err)
	}

	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		msgID := change.ID

		dup, err := idStore.IsDuplicate(ctx, msgID)
		if err != nil {
			return fmt.Errorf("idempotency check: %w", err)
		}
		if dup {
			fmt.Printf("skipping duplicate: %.20s…\n", msgID)
			return nil
		}

		fmt.Printf("processing: %s %s\n", change.OperationType, change.DocumentKey)
		time.Sleep(50 * time.Millisecond)

		if err := idStore.MarkProcessed(ctx, msgID); err != nil {
			fmt.Printf("warn: mark processed: %v\n", err)
		}
		return nil
	})
	if err != nil {
		log.Fatal("subscribe:", err)
	}

	fmt.Println("Watching with deduplication. Press Ctrl+C to stop.")
	<-ctx.Done()
}

// =============================================================================
// EXAMPLE=full — production setup with all features
// =============================================================================

func runFullSetupExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; cancel() }()

	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI()))
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal")

	// Ack store for at-least-once delivery.
	ackStore, err := mongodb.NewMongoAckStore(
		internalDB.Collection("_event_acks"),
		24*time.Hour,
	)
	if err != nil {
		log.Fatal("ack store:", err)
	}
	if err := ackStore.EnsureIndexes(ctx); err != nil {
		log.Fatal("ack indexes:", err)
	}

	// MongoDB coordinator (from event-mongodb/distributed).
	claimer, err := mongodist.NewMongoStateManager(internalDB,
		mongodist.WithCollection("_order_worker_claims"),
		mongodist.WithCompletedTTL(24*time.Hour),
	)
	if err != nil {
		log.Fatal("claimer:", err)
	}
	if err := claimer.EnsureIndexes(ctx); err != nil {
		log.Fatal("claimer indexes:", err)
	}

	idStore := idempotency.NewMemoryStore(7 * 24 * time.Hour)
	defer idStore.Close()

	instanceID, _ := os.Hostname()
	if id := os.Getenv("INSTANCE_ID"); id != "" {
		instanceID = id
	}

	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
		mongodb.WithResumeTokenCollection(internalDB, "_resume_tokens"),
		mongodb.WithResumeTokenID(instanceID),
		mongodb.WithAckStore(ackStore),
		mongodb.WithPipeline(mongo.Pipeline{
			{{Key: "$match", Value: bson.M{
				"operationType": bson.M{"$in": []string{"insert", "update"}},
			}}},
		}),
		mongodb.WithLogger(slog.Default()),
	)
	if err != nil {
		log.Fatal("transport:", err)
	}

	bus, err := event.NewBus("orders-full-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("bus:", err)
	}
	defer func() { _ = bus.Close(ctx) }()

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("register:", err)
	}

	claimTTL := 5 * time.Minute
	handler := func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		dup, err := idStore.IsDuplicate(ctx, change.ID)
		if err != nil {
			fmt.Printf("warn: idempotency: %v\n", err)
		} else if dup {
			fmt.Printf("[%s] duplicate skipped: %s\n", instanceID, change.DocumentKey)
			return nil
		}

		fmt.Printf("[%s] processing %s: %s\n", instanceID, change.OperationType, change.DocumentKey)
		time.Sleep(100 * time.Millisecond)

		if err := idStore.MarkProcessed(ctx, change.ID); err != nil {
			fmt.Printf("warn: mark processed: %v\n", err)
		}
		return nil
	}

	err = orderChanges.Subscribe(ctx, handler,
		event.WithMiddleware(distributed.WorkerPoolMiddleware[mongodb.ChangeEvent](claimer, claimTTL)),
	)
	if err != nil {
		log.Fatal("subscribe:", err)
	}

	recoveryRunner, err := distributed.NewRecoveryRunner(claimer,
		distributed.WithStaleTimeout(2*time.Minute),
		distributed.WithCheckInterval(30*time.Second),
		distributed.WithBatchLimit(100),
	)
	if err != nil {
		log.Fatal("recovery:", err)
	}
	go recoveryRunner.Run(ctx)

	fmt.Println("=== Full production setup ready ===")
	fmt.Printf("Instance: %s\n", instanceID)
	fmt.Println("Features: resume tokens, ack tracking, worker pool, idempotency, orphan recovery")
	fmt.Println("Press Ctrl+C to stop.")

	<-ctx.Done()
}
