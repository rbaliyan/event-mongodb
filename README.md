# event-mongodb

[![CI](https://github.com/rbaliyan/event-mongodb/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event-mongodb/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event-mongodb.svg)](https://pkg.go.dev/github.com/rbaliyan/event-mongodb)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event-mongodb)](https://goreportcard.com/report/github.com/rbaliyan/event-mongodb)

MongoDB Change Stream transport for the [event](https://github.com/rbaliyan/event) library.

## Installation

```bash
go get github.com/rbaliyan/event-mongodb
```

## Features

- Watch MongoDB change streams at collection, database, or cluster level
- Resume token persistence for reliable resumption after restarts
- Resume token reset and start-from-beginning options for recovery
- Acknowledgment tracking for at-least-once delivery
- Automatic reconnection with exponential backoff
- Full document lookup for update events
- Flexible payload options (full ChangeEvent or document only)
- Update description metadata (updated/removed fields) via context
- Empty update filtering and updated fields size limits
- OpenTelemetry metrics with subscriber middleware (oplog lag, handler duration, throughput, pending gauge)

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

### Bus Architecture: One Bus per Collection

The MongoDB transport fans out **every change to every registered event** on the bus with no per-event routing. This means architecture choice matters.

**Use one transport and one bus per collection** (recommended):

```go
// Orders: own transport → own bus → own events
ordersTransport, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    mongodb.WithAckStore(ordersAckStore),
)
ordersBus, _ := event.NewBus("orders", event.WithTransport(ordersTransport))

// Customers: own transport → own bus → own events
customersTransport, _ := mongodb.New(db,
    mongodb.WithCollection("customers"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    mongodb.WithAckStore(customersAckStore),
)
customersBus, _ := event.NewBus("customers", event.WithTransport(customersTransport))
```

**Why one bus per collection:**

- **No wasted work.** A database-level transport delivers every change from every collection to every registered event. Subscribers discard irrelevant changes.
- **Codec decode safety.** The event bus decodes payloads before the handler runs. With `event.New[Order]("orders")` and `event.New[Customer]("customers")` on the same bus, a customer change decoded as `Order` will fail. If DLQ is enabled, these decode failures are sent to the dead-letter queue — flooding it with false positives and preventing handler-level collection routing.
- **Independent resume tokens.** Each transport tracks its own resume position. If orders processing falls behind, customers are unaffected.
- **Independent ack stores.** Pending event counts are per-collection, making monitoring meaningful.
- **Independent DLQ, metrics, and middleware.** Different retry policies, metrics namespaces, and claim TTLs per collection.

**When a single database-level bus is appropriate:**

A single bus watching all collections makes sense only when a single handler processes changes from all collections uniformly — for example, an audit log, CDC replication sink, or a router using `mongodb.ChangeEvent` (not a typed struct):

```go
// Database-level watch — single handler for all collections
transport, _ := mongodb.New(db) // No WithCollection = all collections
bus, _ := event.NewBus("all-changes", event.WithTransport(transport))

allChanges := event.New[mongodb.ChangeEvent]("db.changes")
event.Register(ctx, bus, allChanges)

allChanges.Subscribe(ctx, func(ctx context.Context, ev event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
    // Route by collection — works because ChangeEvent is the wire type
    switch change.Collection {
    case "orders":
        return handleOrder(ctx, change)
    case "customers":
        return handleCustomer(ctx, change)
    default:
        return nil
    }
})
```

This pattern only works with `event.New[mongodb.ChangeEvent]` because every change decodes successfully into `ChangeEvent`. Using typed structs like `event.New[Order]` on a database-level bus will cause decode failures for non-order collections.

**Avoid multiple events on one MongoDB bus:**

```go
// BAD: both events receive ALL changes, decode failures flood DLQ
orderEvent := event.New[Order]("order.changes")
customerEvent := event.New[Customer]("customer.changes")
event.Register(ctx, bus, orderEvent)    // customer changes → decode error → DLQ
event.Register(ctx, bus, customerEvent) // order changes → decode error → DLQ
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

// Start from beginning of oplog on first start (process all available history)
t, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithStartFromBeginning(),
)
```

#### Resetting Resume Tokens

When a resume token becomes stale (oplog has rotated past the stored position), you can reset it:

```go
// Clear the stored resume token
if err := transport.ResetResumeToken(ctx); err != nil {
    log.Printf("failed to reset token: %v", err)
}

// Get the token key for monitoring/debugging
key := transport.ResumeTokenKey()
fmt.Printf("Token key: %s\n", key)
```

After resetting, the next restart will:
- Start from the beginning of oplog (if `WithStartFromBeginning` was set)
- Start from the current position (default behavior)

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

### Metrics & Monitoring

Track change stream processing with OpenTelemetry metrics:

```go
// Create metrics (uses global OTel provider by default)
metrics, err := mongodb.NewMetrics()

// Or with a custom provider and namespace
metrics, err := mongodb.NewMetrics(
    mongodb.WithMeterProvider(myProvider),
    mongodb.WithMetricsNamespace("orders"),
)
defer metrics.Close()

// Wire pending gauge to your ack store
metrics.SetPendingCallback(func() int64 {
    count, _ := db.Collection("_event_acks").CountDocuments(ctx,
        bson.M{"acked_at": bson.M{"$eq": time.Time{}}})
    return count
})

// Use as subscriber middleware
orderEvent.Subscribe(ctx, handler,
    event.WithMiddleware(mongodb.MetricsMiddleware[mongodb.ChangeEvent](metrics)),
)
```

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `mongodb_changes_processed_total` | Counter | Events processed successfully (by event, operation, namespace) |
| `mongodb_changes_failed_total` | Counter | Handler errors (by event, operation, namespace) |
| `mongodb_oplog_lag_seconds` | Histogram | Delay from MongoDB clusterTime to handler execution |
| `mongodb_handler_duration_seconds` | Histogram | Handler processing time |
| `mongodb_changes_pending` | Gauge | Pending unacked events (callback-based) |

### Event Lifecycle Observability

For production change stream processing, combine four complementary layers to achieve full event lifecycle observability:

| Layer | Purpose | Answers |
|-------|---------|---------|
| **Monitor middleware** | Per-event lifecycle tracking | What happened to event X? Which events are failing? |
| **Metrics middleware** | Aggregate throughput and latency | What's our throughput? How far behind are we? |
| **AckStore** | Delivery guarantee tracking | Is event X still in-flight? How many events are stuck? |
| **DLQ** | Permanently failed event capture | Did event X get rejected? What errors are recurring? |

```
Change Stream
    │
    ▼
AckStore.Store(pending)
    │
    ▼
Monitor middleware ──► pending → completed
    │                         → failed
    │                         → retrying
    ▼
Metrics middleware ──► counters, histograms, oplog lag
    │
    ▼
Handler
    │
    ├── success ──► AckStore.Ack()
    │
    └── failure ──► retries exhausted? ──► DLQ.Store()
```

#### Full Setup

```go
import (
    mongodb "github.com/rbaliyan/event-mongodb"
    "github.com/rbaliyan/event-dlq"
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/monitor"
    "github.com/rbaliyan/event/v3/transport/message"
    monitorhttp "github.com/rbaliyan/event/v3/monitor/http"
)

// ── 1. Storage layer ──────────────────────────────────

// Monitor: per-event lifecycle (pending/completed/failed/retrying)
monitorStore := monitor.NewMongoStore(internalDB)

// AckStore: delivery guarantee tracking
ackStore := mongodb.NewMongoAckStore(
    internalDB.Collection("_event_acks"),
    24*time.Hour,
)
ackStore.CreateIndexes(ctx)

// DLQ: permanently failed events
dlqStore := dlq.NewMongoStore(internalDB)
dlqStore.EnsureIndexes(ctx)
dlqManager := dlq.NewManager(dlqStore, transport)

// Metrics: aggregate counters and histograms
metrics, _ := mongodb.NewMetrics(
    mongodb.WithMetricsNamespace("orders"),
)
defer metrics.Close()

// Wire pending gauge to ack store
metrics.SetPendingCallback(func() int64 {
    if qs, ok := any(ackStore).(mongodb.AckQueryStore); ok {
        count, _ := qs.Count(ctx, mongodb.AckFilter{
            Status: mongodb.AckStatusPending,
        })
        return count
    }
    return 0
})

// ── 2. Transport with ack tracking ────────────────────

transport, _ := mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    mongodb.WithAckStore(ackStore),
)

bus, _ := event.NewBus("orders", event.WithTransport(transport))
defer bus.Close(ctx)

// ── 3. Event with DLQ hook ────────────────────────────

orderChanges := event.New[mongodb.ChangeEvent]("order.changes",
    event.WithMaxRetries(3),
    event.WithDeadLetterQueue(func(ctx context.Context, msg message.Message, err error) error {
        return dlqManager.Store(ctx,
            "order.changes",
            msg.ID(),
            msg.Payload(),
            msg.Metadata(),
            err,
            msg.RetryCount(),
            "order-service",
        )
    }),
)
event.Register(ctx, bus, orderChanges)

// ── 4. Subscribe with middleware stack ─────────────────

orderChanges.Subscribe(ctx, handleOrder,
    event.WithMiddleware(
        monitor.Middleware[mongodb.ChangeEvent](monitorStore),
        mongodb.MetricsMiddleware[mongodb.ChangeEvent](metrics),
    ),
)

// ── 5. HTTP API for dashboards ────────────────────────

mux := http.NewServeMux()
mux.Handle("/v1/monitor/", monitorhttp.New(monitorStore))
go http.ListenAndServe(":8081", mux)

// ── 6. Background maintenance ─────────────────────────

go func() {
    ticker := time.NewTicker(1 * time.Hour)
    for range ticker.C {
        monitorStore.DeleteOlderThan(ctx, 7*24*time.Hour)
        dlqManager.Cleanup(ctx, 30*24*time.Hour)
    }
}()
```

#### Diagnosing a Specific Event

Query across all four layers to get the complete picture for any event:

```go
func diagnoseEvent(ctx context.Context, eventID string) {
    // 1. Monitor: lifecycle status with timing and trace IDs
    entries, _ := monitorStore.GetByEventID(ctx, eventID)
    for _, e := range entries {
        fmt.Printf("  subscriber=%s status=%s duration=%v trace=%s\n",
            e.SubscriptionID, e.Status, e.Duration, e.TraceID)
    }

    // 2. AckStore: is it still pending delivery?
    pending, _ := ackStore.IsPending(ctx, eventID)
    fmt.Printf("  pending_ack=%v\n", pending)

    // 3. DLQ: did it get rejected after exhausting retries?
    dlqMsg, err := dlqStore.GetByOriginalID(ctx, eventID)
    if err == nil {
        fmt.Printf("  DLQ: error=%s retries=%d created=%v\n",
            dlqMsg.Error, dlqMsg.RetryCount, dlqMsg.CreatedAt)
    }

    // 4. AckStore: list all pending events for dashboards
    if qs, ok := any(ackStore).(mongodb.AckQueryStore); ok {
        pending, _ := qs.Count(ctx, mongodb.AckFilter{
            Status: mongodb.AckStatusPending,
        })
        fmt.Printf("  total_pending=%d\n", pending)
    }
}
```

#### Monitor HTTP API

The monitor exposes a REST API for operational dashboards:

```bash
# Failed events in the last hour
GET /v1/monitor/entries?status=failed&start_time=2025-01-15T09:00:00Z&order_desc=true

# Events stuck in retry loop
GET /v1/monitor/entries?status=retrying&min_retries=3

# Slow handlers (> 5s)
GET /v1/monitor/entries?min_duration=5s&order_desc=true

# Count pending by bus
GET /v1/monitor/entries/count?status=pending&bus_id=orders

# Full lifecycle for a specific event
GET /v1/monitor/entries/{eventID}
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

## Buffered Transport (Redis + MongoDB)

The `persistent` subpackage provides a MongoDB implementation of `persistent.Store` for use with the composite transport. This enables **buffered event delivery** combining:
- **MongoDB** for durable message storage (at-least-once delivery)
- **Redis/NATS** for low-latency real-time notifications

### Quick Start

```go
import (
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/transport/composite"
    "github.com/rbaliyan/event/v3/transport/redis"
    mongopersistent "github.com/rbaliyan/event-mongodb/persistent"
)

func main() {
    // MongoDB for durable storage
    store, err := mongopersistent.NewStore(
        mongoClient.Database("events").Collection("messages"),
        mongopersistent.WithTTL(7*24*time.Hour), // Auto-delete acked messages
    )
    if err != nil {
        log.Fatal(err)
    }
    store.EnsureIndexes(ctx)

    // Optional: checkpoint store for resume after restart
    checkpointStore, err := mongopersistent.NewCheckpointStore(
        mongoClient.Database("events").Collection("checkpoints"),
    )
    if err != nil {
        log.Fatal(err)
    }
    checkpointStore.EnsureIndexes(ctx)

    // Redis for real-time signals
    signal := redis.New(redisClient)

    // Composite transport: durability + low latency
    transport, _ := composite.New(store, signal,
        composite.WithCheckpointStore(checkpointStore),
        composite.WithPollInterval(5*time.Second),
    )

    // Create bus with the composite transport
    bus, _ := event.NewBus("orders", event.WithTransport(transport))
    defer bus.Close(ctx)

    // Use normally - messages are durable!
    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)
    orderEvent.Publish(ctx, order)
}
```

### How It Works

```
Publish:  App -> MongoDB (durable) -> Redis signal (fast notification)
                     |
Subscribe: Consumer <- poll MongoDB <- triggered by Redis signal
```

1. **Publish**: Messages are written to MongoDB first (source of truth), then a lightweight signal is sent via Redis
2. **Subscribe**: Consumers wait for Redis signals (fast path) or poll MongoDB (fallback)
3. **Acknowledgment**: Messages are marked "acked" and auto-deleted via TTL

### Message Lifecycle

```
pending -> inflight -> acked
    ^          |
    +---nack---+
```

- **pending**: Stored and waiting to be processed
- **inflight**: Fetched and being processed (visibility timeout applies)
- **acked**: Successfully processed (will be auto-deleted if TTL configured)

### Store Options

```go
mongopersistent.NewStore(collection,
    mongopersistent.WithTTL(7*24*time.Hour),           // Auto-delete acked messages
    mongopersistent.WithVisibilityTimeout(10*time.Minute), // Redelivery timeout
)
```

### Monitoring

```go
stats, _ := store.GetStats(ctx, "orders")
fmt.Printf("Pending: %d, Inflight: %d, Acked: %d\n",
    stats.Pending, stats.Inflight, stats.Acked)
```

## Limitations

- `Publish()` is not supported - changes are triggered by database writes
- Only Broadcast delivery mode (all subscribers receive all changes)
- Requires MongoDB replica set or sharded cluster

## License

MIT License - see [LICENSE](LICENSE) for details.
