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

## Limitations

- `Publish()` is not supported - changes are triggered by database writes
- Only Broadcast delivery mode (all subscribers receive all changes)
- Requires MongoDB replica set or sharded cluster

## License

MIT License - see [LICENSE](LICENSE) for details.
