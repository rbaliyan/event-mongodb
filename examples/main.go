// Package main demonstrates comprehensive usage of the event-mongodb package.
//
// This example shows:
//  1. Basic change stream subscription
//  2. Integration with distributed.MongoClaimer for WorkerPool emulation
//  3. Integration with idempotency (in-memory store) for deduplication
//  4. Complete at-least-once delivery setup
//
// IMPORTANT: This example requires a running MongoDB replica set.
// Run with: docker run -d -p 27017:27017 --name mongo mongo:6.0 --replSet rs0
// Then initialize: docker exec -it mongo mongosh --eval "rs.initiate()"
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rbaliyan/event-mongodb"
	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/distributed"
	"github.com/rbaliyan/event/v3/idempotency"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Order represents an order document in the orders collection.
type Order struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	CustomerID string             `bson:"customer_id" json:"customer_id"`
	Product    string             `bson:"product" json:"product"`
	Amount     float64            `bson:"amount" json:"amount"`
	Status     string             `bson:"status" json:"status"`
	CreatedAt  time.Time          `bson:"created_at" json:"created_at"`
}

func main() {
	// Choose which example to run
	example := os.Getenv("EXAMPLE")
	if example == "" {
		example = "basic"
	}

	switch example {
	case "basic":
		runBasicExample()
	case "workerpool":
		runWorkerPoolExample()
	case "idempotency":
		runIdempotencyExample()
	case "full":
		runFullSetupExample()
	default:
		log.Fatalf("Unknown example: %s (use: basic, workerpool, idempotency, full)", example)
	}
}

// =============================================================================
// Example 1: Basic Change Stream Subscription
// =============================================================================

// runBasicExample demonstrates the simplest way to watch MongoDB changes.
// This example shows:
//   - Creating a MongoDB transport
//   - Subscribing to change events
//   - Processing insert, update, and delete operations
func runBasicExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("myapp")

	// Create a change stream transport watching the "orders" collection
	// - WithCollection: Watch specific collection (vs database or cluster level)
	// - WithFullDocument: Include full document in update events
	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
		mongodb.WithLogger(slog.Default()),
	)
	if err != nil {
		log.Fatal("Failed to create transport:", err)
	}

	// Create event bus with the MongoDB transport
	bus, err := event.NewBus("orders-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("Failed to create bus:", err)
	}
	defer bus.Close(ctx)

	// Define and register an event for order changes
	// Using mongodb.ChangeEvent gives you full change event details
	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("Failed to register event:", err)
	}

	// Subscribe to order changes
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		// Log the change event
		fmt.Printf("[%s] %s.%s: %s (key: %s)\n",
			change.Timestamp.Format(time.RFC3339),
			change.Database,
			change.Collection,
			change.OperationType,
			change.DocumentKey,
		)

		// Handle different operation types
		switch change.OperationType {
		case mongodb.OperationInsert:
			fmt.Printf("  New order created\n")
			if change.FullDocument != nil {
				fmt.Printf("  Document: %s\n", string(change.FullDocument))
			}

		case mongodb.OperationUpdate:
			fmt.Printf("  Order updated\n")
			if change.UpdateDesc != nil {
				fmt.Printf("  Updated fields: %v\n", change.UpdateDesc.UpdatedFields)
			}

		case mongodb.OperationDelete:
			fmt.Printf("  Order deleted\n")
		}

		return nil
	})
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}

	fmt.Println("Watching for order changes...")
	fmt.Println("Insert/update/delete documents in the 'orders' collection to see events.")
	fmt.Println("Press Ctrl+C to stop.")

	// Wait for shutdown
	<-ctx.Done()
}

// =============================================================================
// Example 2: WorkerPool Emulation with Distributed Claimer
// =============================================================================

// runWorkerPoolExample demonstrates using distributed.MongoClaimer
// to emulate WorkerPool semantics on the Broadcast-only MongoDB transport.
//
// This is useful when:
//   - You have multiple application instances watching the same collection
//   - You want each change to be processed by only ONE instance (load balancing)
//   - You want automatic failover if an instance crashes during processing
func runWorkerPoolExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal")

	// Create a claimer for worker coordination
	// The claimer uses MongoDB for atomic claim acquisition,
	// ensuring only one worker processes each message.
	claimer := distributed.NewMongoClaimer(internalDB).
		WithCollection("_order_worker_claims"). // Custom collection for this worker group
		WithCompletionTTL(24 * time.Hour)       // Remember completed messages for 24h

	// Create necessary indexes for TTL-based cleanup
	if err := claimer.EnsureIndexes(ctx); err != nil {
		log.Fatal("Failed to create indexes:", err)
	}

	// Create transport and bus
	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
	)
	if err != nil {
		log.Fatal("Failed to create transport:", err)
	}

	bus, err := event.NewBus("orders-worker-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("Failed to create bus:", err)
	}
	defer bus.Close(ctx)

	// Register event
	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("Failed to register event:", err)
	}

	// Get instance ID for logging (helps identify which worker processed the message)
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}

	// Subscribe with DistributedWorkerMiddleware
	// This middleware ensures each message is processed by exactly one worker:
	// 1. Worker calls Claim() to try to claim the message
	// 2. If successful, worker processes and marks as completed
	// 3. If another worker already claimed it, skip silently
	// 4. If processing fails, claim is released for another worker to retry
	claimTTL := 5 * time.Minute // How long a worker can hold a message before it's considered orphaned

	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		fmt.Printf("[%s] Processing %s order %s\n",
			instanceID,
			change.OperationType,
			change.DocumentKey,
		)

		// Simulate some processing time
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("[%s] Completed order %s\n", instanceID, change.DocumentKey)
		return nil
	}, event.WithMiddleware(
		distributed.DistributedWorkerMiddleware[mongodb.ChangeEvent](claimer, claimTTL),
	))
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}

	// Optional: Set up orphan recovery
	// This detects workers that crashed while processing and releases their claims
	recoveryRunner := distributed.NewOrphanRecoveryRunner(claimer,
		distributed.WithStaleTimeout(2*time.Minute),   // Message is orphaned if processing > 2min
		distributed.WithCheckInterval(30*time.Second), // Check for orphans every 30s
	)
	go recoveryRunner.Run(ctx)

	fmt.Printf("Worker %s ready. Watching for order changes...\n", instanceID)
	fmt.Println("Start multiple instances with different INSTANCE_ID to see load balancing.")
	fmt.Println("Press Ctrl+C to stop.")

	<-ctx.Done()
}

// =============================================================================
// Example 3: Deduplication with Idempotency Store
// =============================================================================

// runIdempotencyExample demonstrates using idempotency.MemoryStore
// to prevent duplicate processing of messages.
//
// This is useful when:
//   - Messages might be delivered more than once (at-least-once delivery)
//   - Your handler is not naturally idempotent
//   - You need to track which messages have been processed
//
// Note: For production distributed deployments, use Redis-based idempotency store
// or implement a MongoDB-based one using a similar pattern.
func runIdempotencyExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("myapp")

	// Create an idempotency store
	// This tracks which messages have been processed to prevent duplicates
	// Using in-memory store for this example - for production use Redis or implement MongoDB store
	idempotencyStore := idempotency.NewMemoryStore(24 * time.Hour) // Remember processed messages for 24h
	defer idempotencyStore.Close()

	// Create transport and bus
	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
	)
	if err != nil {
		log.Fatal("Failed to create transport:", err)
	}

	bus, err := event.NewBus("orders-dedup-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("Failed to create bus:", err)
	}
	defer bus.Close(ctx)

	// Register event
	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("Failed to register event:", err)
	}

	// Subscribe with idempotency check in the handler
	err = orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		// Use the change event ID as the idempotency key
		// This ID is unique and derived from the resume token
		messageID := change.ID

		// Check if already processed
		isDuplicate, err := idempotencyStore.IsDuplicate(ctx, messageID)
		if err != nil {
			return fmt.Errorf("idempotency check failed: %w", err)
		}
		if isDuplicate {
			fmt.Printf("Skipping duplicate: %s\n", messageID[:20]+"...")
			return nil
		}

		// Process the change
		fmt.Printf("Processing new change: %s %s\n",
			change.OperationType,
			change.DocumentKey,
		)

		// Simulate processing
		time.Sleep(50 * time.Millisecond)

		// Mark as processed
		if err := idempotencyStore.MarkProcessed(ctx, messageID); err != nil {
			// Log but don't fail - message might be reprocessed but that's ok
			fmt.Printf("Warning: failed to mark as processed: %v\n", err)
		}

		fmt.Printf("Completed: %s\n", change.DocumentKey)
		return nil
	})
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}

	fmt.Println("Watching for order changes with deduplication...")
	fmt.Println("Press Ctrl+C to stop.")

	<-ctx.Done()
}

// =============================================================================
// Example 4: Complete At-Least-Once Delivery Setup
// =============================================================================

// runFullSetupExample demonstrates a production-ready setup combining:
//   - Resume token persistence (survive restarts)
//   - Acknowledgment tracking (at-least-once delivery)
//   - WorkerPool emulation (load balancing)
//   - Idempotency (deduplication)
//   - Orphan recovery (handle crashed workers)
func runFullSetupExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("myapp")
	internalDB := client.Database("myapp_internal") // Separate DB for internal state

	// -------------------------------------------------------------------------
	// Step 1: Set up acknowledgment store for at-least-once delivery
	// -------------------------------------------------------------------------
	// The ack store tracks which events are pending acknowledgment.
	// This enables redelivery of failed events on restart.
	ackStore := mongodb.NewMongoAckStore(
		internalDB.Collection("_event_acks"),
		24*time.Hour, // TTL for acknowledged events
	)
	if err := ackStore.CreateIndexes(ctx); err != nil {
		log.Fatal("Failed to create ack indexes:", err)
	}

	// -------------------------------------------------------------------------
	// Step 2: Set up claimer for WorkerPool emulation
	// -------------------------------------------------------------------------
	claimer := distributed.NewMongoClaimer(internalDB).
		WithCollection("_order_worker_claims").
		WithCompletionTTL(24 * time.Hour)
	if err := claimer.EnsureIndexes(ctx); err != nil {
		log.Fatal("Failed to create claim indexes:", err)
	}

	// -------------------------------------------------------------------------
	// Step 3: Set up idempotency store for deduplication
	// -------------------------------------------------------------------------
	// Using in-memory store - for production distributed setup, use Redis
	idempotencyStore := idempotency.NewMemoryStore(7 * 24 * time.Hour) // 7 days
	defer idempotencyStore.Close()

	// -------------------------------------------------------------------------
	// Step 4: Create transport with all features enabled
	// -------------------------------------------------------------------------
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}

	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),

		// Resume tokens: Store in separate DB to survive collection drops
		mongodb.WithResumeTokenCollection(internalDB, "_resume_tokens"),

		// Resume token ID: Per-instance tokens for independent resumption
		// Use "shared" for all instances to resume from same position
		mongodb.WithResumeTokenID(instanceID),

		// Acknowledgment tracking: Track pending/completed events
		mongodb.WithAckStore(ackStore),

		// Pipeline: Only watch insert and update operations
		mongodb.WithPipeline(mongo.Pipeline{
			{{Key: "$match", Value: bson.M{
				"operationType": bson.M{"$in": []string{"insert", "update"}},
			}}},
		}),

		// Logging
		mongodb.WithLogger(slog.Default()),
	)
	if err != nil {
		log.Fatal("Failed to create transport:", err)
	}

	// -------------------------------------------------------------------------
	// Step 5: Create bus and register event
	// -------------------------------------------------------------------------
	bus, err := event.NewBus("orders-full-bus", event.WithTransport(transport))
	if err != nil {
		log.Fatal("Failed to create bus:", err)
	}
	defer bus.Close(ctx)

	orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
	if err := event.Register(ctx, bus, orderChanges); err != nil {
		log.Fatal("Failed to register event:", err)
	}

	// -------------------------------------------------------------------------
	// Step 6: Subscribe with full middleware stack
	// -------------------------------------------------------------------------
	claimTTL := 5 * time.Minute

	// Create the order processing handler
	orderHandler := func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
		// Additional idempotency check (belt-and-suspenders approach)
		// The DistributedWorkerMiddleware handles claiming, but this catches
		// cases where the same message is delivered to the same worker twice
		messageID := change.ID
		isDuplicate, err := idempotencyStore.IsDuplicate(ctx, messageID)
		if err != nil {
			fmt.Printf("Warning: idempotency check failed: %v\n", err)
			// Continue processing - better to process twice than not at all
		} else if isDuplicate {
			fmt.Printf("[%s] Skipping duplicate (idempotency): %s\n", instanceID, change.DocumentKey)
			return nil
		}

		// Process the order change
		fmt.Printf("[%s] Processing %s: %s\n", instanceID, change.OperationType, change.DocumentKey)

		// Simulate business logic
		switch change.OperationType {
		case mongodb.OperationInsert:
			// Handle new order
			fmt.Printf("[%s] New order - sending confirmation email...\n", instanceID)
			time.Sleep(100 * time.Millisecond)

		case mongodb.OperationUpdate:
			// Handle order update
			fmt.Printf("[%s] Order updated - checking if status changed...\n", instanceID)
			time.Sleep(50 * time.Millisecond)
		}

		// Mark as processed in idempotency store
		if err := idempotencyStore.MarkProcessed(ctx, messageID); err != nil {
			fmt.Printf("Warning: failed to mark as processed: %v\n", err)
		}

		fmt.Printf("[%s] Completed: %s\n", instanceID, change.DocumentKey)
		return nil
	}

	// Subscribe with DistributedWorkerMiddleware for load balancing
	err = orderChanges.Subscribe(ctx, orderHandler,
		event.WithMiddleware(
			distributed.DistributedWorkerMiddleware[mongodb.ChangeEvent](claimer, claimTTL),
		),
	)
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}

	// -------------------------------------------------------------------------
	// Step 7: Start orphan recovery
	// -------------------------------------------------------------------------
	recoveryRunner := distributed.NewOrphanRecoveryRunner(claimer,
		distributed.WithStaleTimeout(2*time.Minute),
		distributed.WithCheckInterval(30*time.Second),
		distributed.WithBatchLimit(100),
	)
	go recoveryRunner.Run(ctx)

	// -------------------------------------------------------------------------
	// Ready!
	// -------------------------------------------------------------------------
	fmt.Println("=== Full Production Setup Ready ===")
	fmt.Printf("Instance ID: %s\n", instanceID)
	fmt.Println("Features enabled:")
	fmt.Println("  - Resume token persistence (survive restarts)")
	fmt.Println("  - Acknowledgment tracking (at-least-once delivery)")
	fmt.Println("  - WorkerPool emulation (load balancing)")
	fmt.Println("  - Idempotency (deduplication)")
	fmt.Println("  - Orphan recovery (handle crashed workers)")
	fmt.Println("")
	fmt.Println("Watching for order changes...")
	fmt.Println("Press Ctrl+C to stop.")

	<-ctx.Done()
}
