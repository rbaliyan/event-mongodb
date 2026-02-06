package mongodb_test

import (
	"context"
	"fmt"
	"time"

	"github.com/rbaliyan/event-mongodb"
	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/distributed"
	"github.com/rbaliyan/event/v3/idempotency"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Order represents an order document.
type Order struct {
	ID         bson.ObjectID `bson:"_id,omitempty" json:"id"`
	CustomerID string             `bson:"customer_id" json:"customer_id"`
	Product    string             `bson:"product" json:"product"`
	Amount     float64            `bson:"amount" json:"amount"`
	Status     string             `bson:"status" json:"status"`
}

// Example demonstrates basic usage of the MongoDB change stream transport.
//
// The MongoDB transport watches for changes in MongoDB collections and delivers
// them as events. Unlike traditional transports, publishing happens implicitly
// when documents are inserted, updated, or deleted in MongoDB.
//
// This package is designed to work with the event pub-sub library and integrates
// seamlessly with other packages like distributed and idempotency for
// production-ready deployments.
func Example() {
	ctx := context.Background()

	// Connect to MongoDB (requires a replica set)
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		fmt.Println("Connect error:", err)
		return
	}
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	// Create a change stream transport watching the "orders" collection
	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
	)
	if err != nil {
		fmt.Println("Transport error:", err)
		return
	}

	// Create event bus with the MongoDB transport
	bus, err := event.NewBus("orders-bus", event.WithTransport(transport))
	if err != nil {
		fmt.Println("Bus error:", err)
		return
	}
	defer bus.Close(ctx)

	// Define and register an event
	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		fmt.Println("Register error:", err)
		return
	}

	// Subscribe to order changes
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		fmt.Printf("%s on %s.%s: %s\n",
			change.OperationType,
			change.Database,
			change.Collection,
			change.DocumentKey,
		)
		return nil
	})
	if err != nil {
		fmt.Println("Subscribe error:", err)
		return
	}

	fmt.Println("Watching for changes...")
	// In a real application, you would block here until shutdown
}

// Example_basic demonstrates the simplest change stream watching setup.
//
// This example shows how to watch a single MongoDB collection for changes.
// The transport automatically handles reconnection with exponential backoff
// and resume token persistence.
func Example_basic() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	// Watch the "orders" collection
	// - Default resume token storage in "_event_resume_tokens" collection
	// - Automatic reconnection on connection errors
	transport, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
	)

	bus, _ := event.NewBus("orders", event.WithTransport(transport))
	defer bus.Close(ctx)

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	_ = event.Register(ctx, bus, orderChanges)

	_ = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		fmt.Printf("Change: %s %s\n", change.OperationType, change.DocumentKey)
		return nil
	})

	fmt.Println("Basic change stream watching configured")
}

// Example_withResumeToken demonstrates resume token persistence options.
//
// Resume tokens allow the change stream to resume from where it left off
// after a restart. This ensures no changes are missed during downtime
// (as long as they're within MongoDB's oplog window).
func Example_withResumeToken() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal")

	// Option 1: Default (uses "_event_resume_tokens" in same database)
	transport1, _ := mongodb.New(db, mongodb.WithCollection("orders"))

	// Option 2: Custom collection for resume tokens
	// Useful for separating operational data from application data
	transport2, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithResumeTokenCollection(internalDB, "_resume_tokens"),
	)

	// Option 3: Per-instance resume tokens
	// Each application instance maintains its own resume position
	// Useful for independent processing across instances
	transport3, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithResumeTokenID("instance-1"),
	)

	// Option 4: Disable resume tokens entirely
	// Always start from current position on restart
	// Use only when missing changes is acceptable
	transport4, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithoutResume(),
	)

	_ = transport1
	_ = transport2
	_ = transport3
	_ = transport4

	fmt.Println("Resume token options configured")
}

// Example_withAckStore demonstrates acknowledgment tracking for at-least-once delivery.
//
// The acknowledgment store tracks which events have been successfully processed.
// Combined with resume tokens, this enables reliable at-least-once delivery:
//   - Events are stored as "pending" when received
//   - Events are marked "acknowledged" when processing completes
//   - On restart, pending events can be reprocessed
func Example_withAckStore() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal")

	// Create an acknowledgment store
	// TTL controls how long acknowledged events are remembered
	ackStore := mongodb.NewMongoAckStore(
		internalDB.Collection("_event_acks"),
		24*time.Hour, // Keep acknowledged events for 24 hours
	)

	// Create indexes for efficient queries and TTL-based cleanup
	_ = ackStore.CreateIndexes(ctx)

	// Create transport with ack store
	transport, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithAckStore(ackStore),
	)

	_ = transport

	fmt.Println("Acknowledgment store configured")
}

// Example_withPipeline demonstrates pipeline filtering for change events.
//
// Aggregation pipelines allow you to filter which changes are delivered.
// This is useful for:
//   - Watching only specific operation types (insert, update, delete)
//   - Filtering by document fields
//   - Projecting only needed fields
func Example_withPipeline() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	// Only watch insert and update operations
	insertUpdatePipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"operationType": bson.M{"$in": []string{"insert", "update"}},
		}}},
	}

	transport1, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithPipeline(insertUpdatePipeline),
	)

	// Watch only orders with amount > 100
	highValuePipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"fullDocument.amount": bson.M{"$gt": 100},
		}}},
	}

	transport2, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
		mongodb.WithPipeline(highValuePipeline),
	)

	// Watch only status changes
	statusChangePipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"updateDescription.updatedFields.status": bson.M{"$exists": true},
		}}},
	}

	transport3, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithPipeline(statusChangePipeline),
	)

	_ = transport1
	_ = transport2
	_ = transport3

	fmt.Println("Pipeline filtering configured")
}

// Example_fullDocumentOnly demonstrates receiving documents directly instead of ChangeEvent.
//
// When WithFullDocumentOnly is enabled, the transport sends only the fullDocument
// as the payload instead of the entire ChangeEvent. This allows you to subscribe
// with your document type directly rather than mongodb.ChangeEvent.
func Example_fullDocumentOnly() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	// Enable full document only mode
	// IMPORTANT: Requires FullDocumentUpdateLookup to get documents on updates
	transport, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
		mongodb.WithFullDocumentOnly(), // Send just the document, not ChangeEvent
	)

	bus, _ := event.NewBus("orders", event.WithTransport(transport))
	defer bus.Close(ctx)

	// Subscribe with your document type directly
	// No need for mongodb.ChangeEvent wrapper
	orderEvent := event.New[Order]("order.changes")
	_ = event.Register(ctx, bus, orderEvent)

	_ = orderEvent.Subscribe(ctx, func(ctx context.Context, ev event.Event[Order], order Order) error {
		// Access order fields directly
		fmt.Printf("Order %s: %s ($%.2f)\n",
			order.ID.Hex(),
			order.Product,
			order.Amount,
		)
		return nil
	})

	fmt.Println("Full document only mode configured")
}

// Example_watchLevels demonstrates different watch levels (collection, database, cluster).
//
// The MongoDB transport supports three levels of change stream watching:
//   - Collection-level: Watch a specific collection
//   - Database-level: Watch all collections in a database
//   - Cluster-level: Watch all databases in the cluster
func Example_watchLevels() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	// Collection-level: Watch a specific collection
	// Use when you only care about changes to one collection
	collectionTransport, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
	)

	// Database-level: Watch all collections in the database
	// Use when you want to react to changes across multiple collections
	// Note: Don't specify WithCollection for database-level
	databaseTransport, _ := mongodb.New(db)

	// Cluster-level: Watch all databases
	// Use when you need to monitor the entire MongoDB deployment
	clusterTransport, _ := mongodb.NewClusterWatch(client)

	_ = collectionTransport
	_ = databaseTransport
	_ = clusterTransport

	fmt.Println("Watch levels configured")
}

// Example_withDistributed demonstrates integration with distributed.MongoStateManager
// for emulating WorkerPool semantics.
//
// MongoDB change streams are Broadcast-only (all subscribers receive all changes).
// The distributed package provides WorkerPoolMiddleware that uses atomic database
// claiming to ensure each message is processed by exactly one worker.
//
// This is useful for:
//   - Load balancing across multiple application instances
//   - Automatic failover when workers crash
//   - Preventing duplicate processing in distributed deployments
func Example_withDistributed() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal")

	// Create claimer for worker coordination
	// Uses MongoDB's atomic findOneAndUpdate for race-condition-free coordination
	claimer := distributed.NewMongoStateManager(internalDB).
		WithCollection("_order_worker_claims"). // Custom collection name
		WithCompletedTTL(24 * time.Hour)       // Remember completed messages for 24h

	// Create TTL index for automatic cleanup
	_ = claimer.EnsureIndexes(ctx)

	// Create transport and bus
	transport, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
	)

	bus, _ := event.NewBus("orders", event.WithTransport(transport))
	defer bus.Close(ctx)

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	_ = event.Register(ctx, bus, orderChanges)

	// Claim TTL should exceed your handler's maximum processing time
	// If a worker holds a message longer than this, it's considered orphaned
	claimTTL := 5 * time.Minute

	// Subscribe with WorkerPoolMiddleware
	// The middleware handles:
	// 1. Atomic claiming (only one worker processes each message)
	// 2. Completion tracking (prevents reprocessing)
	// 3. Failure handling (releases claim for retry on error)
	_ = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		fmt.Printf("Processing: %s %s\n", change.OperationType, change.DocumentKey)

		// Your business logic here
		// If this returns an error, the claim is released
		// and another worker can pick it up

		return nil
	}, event.WithMiddleware(
		distributed.WorkerPoolMiddleware[mongodb.ChangeEvent](claimer, claimTTL),
	))

	// Optional: Set up orphan recovery
	// Detects workers that crashed and releases their claims
	recoveryRunner := distributed.NewRecoveryRunner(claimer,
		distributed.WithStaleTimeout(2*time.Minute),
		distributed.WithCheckInterval(30*time.Second),
	)
	go recoveryRunner.Run(ctx)

	fmt.Println("WorkerPool emulation with distributed claimer configured")
}

// Example_withIdempotency demonstrates integration with idempotency store
// for deduplication.
//
// Even with WorkerPool emulation, messages might be delivered more than once
// (at-least-once delivery). The idempotency package provides stores to track
// processed messages and skip duplicates.
//
// This is useful for:
//   - Ensuring exactly-once processing semantics
//   - Handling redelivery after crashes
//   - Non-idempotent operations (like sending emails, charging payments)
func Example_withIdempotency() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")

	// Create idempotency store
	// Using in-memory store for this example
	// For production distributed deployments, use Redis: idempotency.NewRedisStore()
	idempotencyStore := idempotency.NewMemoryStore(7 * 24 * time.Hour) // Remember processed messages for 7 days
	defer idempotencyStore.Close()

	// Create transport and bus
	transport, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
	)

	bus, _ := event.NewBus("orders", event.WithTransport(transport))
	defer bus.Close(ctx)

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	_ = event.Register(ctx, bus, orderChanges)

	// Subscribe with idempotency check in the handler
	_ = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		// Use the change event ID as the idempotency key
		// This ID is unique per change event
		messageID := change.ID

		// Atomic check: returns false if new, true if already processed
		isDuplicate, err := idempotencyStore.IsDuplicate(ctx, messageID)
		if err != nil {
			return fmt.Errorf("idempotency check: %w", err)
		}
		if isDuplicate {
			fmt.Printf("Skipping duplicate: %s\n", change.DocumentKey)
			return nil
		}

		// Process the change
		fmt.Printf("Processing: %s %s\n", change.OperationType, change.DocumentKey)

		// Mark as processed after successful processing
		if err := idempotencyStore.MarkProcessed(ctx, messageID); err != nil {
			fmt.Printf("Warning: failed to mark processed: %v\n", err)
		}

		return nil
	})

	fmt.Println("Deduplication with idempotency store configured")
}

// Example_completeSetup demonstrates a production-ready configuration
// combining all features.
//
// This example shows how to combine:
//   - Resume token persistence (survive restarts)
//   - Acknowledgment tracking (at-least-once delivery)
//   - WorkerPool emulation (load balancing)
//   - Idempotency (deduplication)
//   - Orphan recovery (handle crashed workers)
func Example_completeSetup() {
	ctx := context.Background()

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal")

	// 1. Acknowledgment store for at-least-once delivery
	ackStore := mongodb.NewMongoAckStore(
		internalDB.Collection("_event_acks"),
		24*time.Hour,
	)
	_ = ackStore.CreateIndexes(ctx)

	// 2. Claimer for WorkerPool emulation
	claimer := distributed.NewMongoStateManager(internalDB).
		WithCollection("_order_worker_claims").
		WithCompletedTTL(24 * time.Hour)
	_ = claimer.EnsureIndexes(ctx)

	// 3. Idempotency store for deduplication
	idempotencyStore := idempotency.NewMemoryStore(7 * 24 * time.Hour)
	defer idempotencyStore.Close()

	// 4. Transport with all features
	transport, _ := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
		mongodb.WithResumeTokenCollection(internalDB, "_resume_tokens"),
		mongodb.WithResumeTokenID("worker-1"),
		mongodb.WithAckStore(ackStore),
		mongodb.WithPipeline(mongo.Pipeline{
			{{Key: "$match", Value: bson.M{
				"operationType": bson.M{"$in": []string{"insert", "update"}},
			}}},
		}),
	)

	// 5. Bus and event
	bus, _ := event.NewBus("orders", event.WithTransport(transport))
	defer bus.Close(ctx)

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	_ = event.Register(ctx, bus, orderChanges)

	// 6. Handler with full middleware stack
	claimTTL := 5 * time.Minute

	_ = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		// Additional idempotency check
		messageID := change.ID
		isDuplicate, _ := idempotencyStore.IsDuplicate(ctx, messageID)
		if isDuplicate {
			return nil
		}

		// Process
		fmt.Printf("Processing: %s %s\n", change.OperationType, change.DocumentKey)

		// Mark processed
		_ = idempotencyStore.MarkProcessed(ctx, messageID)
		return nil
	}, event.WithMiddleware(
		distributed.WorkerPoolMiddleware[mongodb.ChangeEvent](claimer, claimTTL),
	))

	// 7. Orphan recovery
	recoveryRunner := distributed.NewRecoveryRunner(claimer,
		distributed.WithStaleTimeout(2*time.Minute),
		distributed.WithCheckInterval(30*time.Second),
	)
	go recoveryRunner.Run(ctx)

	fmt.Println("Production-ready setup configured")
}
