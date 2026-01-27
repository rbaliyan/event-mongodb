# event-mongodb

MongoDB Change Stream transport for the [event](https://github.com/rbaliyan/event) library.

## Installation

```bash
go get github.com/rbaliyan/event-mongodb
```

## Features

- Watch MongoDB change streams at collection, database, or cluster level
- Resume token persistence for reliable resumption after restarts
- Acknowledgment tracking for at-least-once delivery
- Automatic reconnection with exponential backoff
- Full document lookup for update events
- Flexible payload options (full ChangeEvent or document only)
- Update description metadata (updated/removed fields) via context
- Empty update filtering and updated fields size limits

## Usage

### Basic Usage

```go
import (
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event-mongodb"
)

// Watch a specific collection
transport, err := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
)

bus, err := event.NewBus("orders", event.WithTransport(transport))

// Define and register event
orderEvent := event.New[Order]("order.changes")
event.Register(ctx, bus, orderEvent)

// Subscribe to changes
orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
    fmt.Printf("Order changed: %s\n", order.ID)
    return nil
})
```

### Watch Levels

```go
// Collection-level: watch specific collection
t, _ := mongodb.New(db, mongodb.WithCollection("orders"))

// Database-level: watch all collections in database
t, _ := mongodb.New(db)

// Cluster-level: watch all databases
t, _ := mongodb.NewClusterWatch(client)
```

### Resume Tokens

Resume tokens are automatically persisted to enable reliable resumption:

```go
// Default: stores in "_event_resume_tokens" collection
t, _ := mongodb.New(db, mongodb.WithCollection("orders"))

// Custom collection
t, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithResumeTokenCollection(internalDB, "_resume_tokens"),
)

// Per-instance resume tokens (for multiple workers)
t, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithResumeTokenID(os.Getenv("INSTANCE_ID")),
)

// Disable resume (start from current position on restart)
t, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithoutResume(),
)
```

### Full Document Options

```go
// Default: only full document for insert/replace
mongodb.WithFullDocument(mongodb.FullDocumentDefault)

// Lookup current document for updates (most common)
mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup)

// Post-image when available (MongoDB 6.0+)
mongodb.WithFullDocument(mongodb.FullDocumentWhenAvailable)

// Require post-image or fail (MongoDB 6.0+)
mongodb.WithFullDocument(mongodb.FullDocumentRequired)
```

### Document-Only Payload

Send just the document instead of the full ChangeEvent:

```go
transport, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    mongodb.WithFullDocumentOnly(), // Send just the document
)

// Subscribe with your type directly
orderEvent := event.New[Order]("order.changes")
orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
    // order is your Order struct, not mongodb.ChangeEvent
    return nil
})
```

### Acknowledgment Store

Track which events have been processed:

```go
ackStore := mongodb.NewMongoAckStore(
    internalDB.Collection("_event_acks"),
    24*time.Hour, // TTL for acknowledged events
)
ackStore.CreateIndexes(ctx)

transport, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithAckStore(ackStore),
)
```

### Update Description Metadata

Include field-level change details in event context metadata:

```go
transport, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    mongodb.WithUpdateDescription(), // Include updated/removed fields in metadata
)

// In your subscriber handler:
orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
    desc := mongodb.ContextUpdateDescription(ctx)
    if desc != nil {
        fmt.Printf("Updated fields: %v\n", desc.UpdatedFields)
        fmt.Printf("Removed fields: %v\n", desc.RemovedFields)
    }
    return nil
})
```

Control update behavior:

```go
// Include empty updates (no-op updates that MongoDB produces internally)
mongodb.WithEmptyUpdates()

// Limit updated_fields metadata size (falls back to full document)
// Requires WithFullDocument to be set
mongodb.WithMaxUpdatedFieldsSize(4096) // 4KB limit; implicitly enables WithUpdateDescription()
```

### Pipeline Filtering

Filter change events with aggregation pipeline:

```go
// Only watch insert and update operations
pipeline := mongo.Pipeline{
    {{Key: "$match", Value: bson.M{
        "operationType": bson.M{"$in": []string{"insert", "update"}},
    }}},
}

transport, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithPipeline(pipeline),
)
```

## Integration with distributed package

MongoDB change streams are Broadcast-only (all subscribers receive all changes). The [distributed](https://github.com/rbaliyan/event/tree/main/distributed) package provides `DistributedWorkerMiddleware` that uses atomic database claiming to emulate WorkerPool semantics.

This enables:
- Load balancing across multiple application instances
- Automatic failover when workers crash
- Preventing duplicate processing in distributed deployments

### Setup

```go
import (
    "github.com/rbaliyan/event/v3/distributed"
)

// Create claimer for worker coordination
// Uses MongoDB's atomic findOneAndUpdate for race-condition-free coordination
claimer := distributed.NewMongoClaimer(internalDB).
    WithCollection("_order_worker_claims"). // Custom collection name
    WithCompletionTTL(24 * time.Hour)       // Remember completed messages for 24h

// Create TTL index for automatic cleanup
claimer.EnsureIndexes(ctx)
```

### Subscribe with WorkerPool Emulation

```go
// Claim TTL should exceed your handler's maximum processing time
claimTTL := 5 * time.Minute

orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
    fmt.Printf("Processing: %s %s\n", change.OperationType, change.DocumentKey)

    // Your business logic here
    // If this returns an error, the claim is released
    // and another worker can pick it up

    return nil
}, event.WithMiddleware(
    distributed.DistributedWorkerMiddleware[mongodb.ChangeEvent](claimer, claimTTL),
))
```

### Orphan Recovery

Detect crashed workers and release their claims:

```go
recoveryRunner := distributed.NewOrphanRecoveryRunner(claimer,
    distributed.WithStaleTimeout(2*time.Minute),   // Message is orphaned if processing > 2min
    distributed.WithCheckInterval(30*time.Second), // Check every 30s
)
go recoveryRunner.Run(ctx)
```

## Integration with idempotency package

Even with WorkerPool emulation, messages might be delivered more than once (at-least-once delivery). The [idempotency](https://github.com/rbaliyan/event/tree/main/idempotency) package provides stores to track processed messages and skip duplicates.

### Setup

```go
import (
    "github.com/rbaliyan/event/v3/idempotency"
)

// Create idempotency store
// For single-instance deployments, use in-memory store
idempotencyStore := idempotency.NewMemoryStore(7 * 24 * time.Hour) // Remember processed messages for 7 days
defer idempotencyStore.Close()

// For distributed deployments, use Redis store
// redisStore := idempotency.NewRedisStore(redisClient, 7 * 24 * time.Hour)
```

### Use in Handler

```go
orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
    // Use the change event ID as the idempotency key
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
```

## Complete At-Least-Once Delivery

Combine all features for production-ready setup:

```go
import (
    mongodb "github.com/rbaliyan/event-mongodb"
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/distributed"
    "github.com/rbaliyan/event/v3/idempotency"
)

// 1. Acknowledgment store for at-least-once delivery
ackStore := mongodb.NewMongoAckStore(
    internalDB.Collection("_event_acks"),
    24*time.Hour,
)
ackStore.CreateIndexes(ctx)

// 2. Claimer for WorkerPool emulation
claimer := distributed.NewMongoClaimer(internalDB).
    WithCollection("_order_worker_claims").
    WithCompletionTTL(24 * time.Hour)
claimer.EnsureIndexes(ctx)

// 3. Idempotency store for deduplication
// Using in-memory store - for distributed deployments, use Redis
idempotencyStore := idempotency.NewMemoryStore(7 * 24 * time.Hour)
defer idempotencyStore.Close()

// 4. Transport with all features
transport, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    mongodb.WithResumeTokenCollection(internalDB, "_resume_tokens"),
    mongodb.WithResumeTokenID("worker-1"),
    mongodb.WithAckStore(ackStore),
)

// 5. Bus and event
bus, _ := event.NewBus("orders", event.WithTransport(transport))
defer bus.Close(ctx)

orderChanges := event.New[mongodb.ChangeEvent]("order.changes")
event.Register(ctx, bus, orderChanges)

// 6. Handler with full middleware stack
claimTTL := 5 * time.Minute

orderChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
    // Idempotency check
    messageID := change.ID
    isDuplicate, _ := idempotencyStore.IsDuplicate(ctx, messageID)
    if isDuplicate {
        return nil
    }

    // Process
    fmt.Printf("Processing: %s %s\n", change.OperationType, change.DocumentKey)

    // Mark processed
    idempotencyStore.MarkProcessed(ctx, messageID)
    return nil
}, event.WithMiddleware(
    distributed.DistributedWorkerMiddleware[mongodb.ChangeEvent](claimer, claimTTL),
))

// 7. Orphan recovery
recoveryRunner := distributed.NewOrphanRecoveryRunner(claimer,
    distributed.WithStaleTimeout(2*time.Minute),
    distributed.WithCheckInterval(30*time.Second),
)
go recoveryRunner.Run(ctx)
```

This setup provides:
- **Resume tokens**: Survive restarts without missing changes
- **Acknowledgment tracking**: Track pending/completed events
- **WorkerPool emulation**: Load balance across multiple instances
- **Idempotency**: Prevent duplicate processing
- **Orphan recovery**: Handle crashed workers

## Examples

See the [examples](examples/) directory for complete runnable examples:

```bash
# Basic change stream watching
EXAMPLE=basic go run ./examples

# WorkerPool emulation with distributed state
EXAMPLE=workerpool go run ./examples

# Deduplication with idempotency
EXAMPLE=idempotency go run ./examples

# Full production setup
EXAMPLE=full go run ./examples
```

## Limitations

- `Publish()` is not supported - changes are triggered by database writes
- Only Broadcast delivery mode (all subscribers receive all changes)
- Requires MongoDB replica set or sharded cluster

## License

MIT License - see [LICENSE](LICENSE) for details.
