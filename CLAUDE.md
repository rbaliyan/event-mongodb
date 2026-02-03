# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Build Commands

```bash
go test ./...          # Run all tests
go test -run TestName  # Run a specific test
go build ./...         # Build all packages
go mod tidy            # Clean up dependencies
```

## Project Overview

MongoDB Change Stream transport (`github.com/rbaliyan/event-mongodb`) for the event pub-sub library. Watches MongoDB for changes and delivers them as events.

## Architecture

### Core Components

**Transport (mongodb.go)** - Main transport implementation:
- Implements `transport.Transport` interface from event/v3
- Uses internal `channel.Transport` for fan-out to subscribers
- Supports collection, database, and cluster-level watching
- Automatic reconnection with exponential backoff

**Context (context.go)** - Metadata constants and extraction:
- Exported metadata key constants (`MetadataOperation`, `MetadataCollection`, etc.)
- `ContextUpdateDescription(ctx)`: Extracts `UpdateDescription` from handler context metadata

**Stores (stores.go)** - Persistence implementations:
- `MongoResumeTokenStore`: Persists resume tokens for crash recovery
- `MongoAckStore`: Tracks acknowledged events for at-least-once delivery

**Metrics (metrics.go)** - OpenTelemetry metrics and subscriber middleware:
- `Metrics` struct with nil-safe methods
- `NewMetrics(opts...)` constructor with `WithMeterProvider()` and `WithMetricsNamespace()` options
- `MetricsMiddleware[T](*Metrics)` generic subscriber middleware
- Counters: `mongodb_changes_processed_total`, `mongodb_changes_failed_total`
- Histograms: `mongodb_oplog_lag_seconds`, `mongodb_handler_duration_seconds`
- Observable gauge: `mongodb_changes_pending` (callback-based via `SetPendingCallback`)
- Middleware extracts `cluster_time` from context metadata to compute oplog lag
- Uses `event.ClassifyError()` for proper error classification (ErrAck = processed, all others = failed)
- `Close()` unregisters gauge callbacks

**Persistent Store (persistent/)** - Composite transport support:
- `Store`: Implements `persistent.Store` for durable message storage
- `CheckpointStore`: Implements `persistent.CheckpointStore` for consumer resume
- Used with `composite.New(store, signal)` for Redis+MongoDB buffered transport

### Persistent Store Usage

```go
import (
    "github.com/rbaliyan/event/v3/transport/composite"
    "github.com/rbaliyan/event/v3/transport/redis"
    mongopersistent "github.com/rbaliyan/event-mongodb/persistent"
)

// MongoDB for durable storage
store := mongopersistent.NewStore(collection,
    mongopersistent.WithTTL(7*24*time.Hour),
    mongopersistent.WithVisibilityTimeout(10*time.Minute),
)

// Checkpoint store for consumer resume
checkpointStore := mongopersistent.NewCheckpointStore(checkpointCollection)

// Combine with Redis for low-latency signals
transport, _ := composite.New(store, redis.New(redisClient),
    composite.WithCheckpointStore(checkpointStore),
)
```

### Watch Levels

Watch level is determined by the constructor and options (unexported internally):

```
Collection  - Watch single collection (New + WithCollection)
Database    - Watch all collections in database (New without WithCollection)
Cluster     - Watch all databases in cluster (NewClusterWatch)
```

### Key Interfaces

```go
// ResumeTokenStore persists resume tokens
type ResumeTokenStore interface {
    Load(ctx context.Context, collection string) (bson.Raw, error)
    Save(ctx context.Context, collection string, token bson.Raw) error
}

// AckStore tracks acknowledgments
type AckStore interface {
    Store(ctx context.Context, eventID string) error
    Ack(ctx context.Context, eventID string) error
}
```

### Data Flow

```
MongoDB Change Stream
    │
    ▼
watchLoop() ─── reconnection with backoff
    │
    ▼
processChange() ─── extract ChangeEvent, build payload
    │
    ▼
channelTransport.Publish() ─── fan-out to all registered events
    │
    ▼
Subscribers receive via Messages() channel
```

### ChangeEvent Structure

```go
type ChangeEvent struct {
    ID            string              // Resume token data
    OperationType OperationType       // insert, update, replace, delete
    Database      string
    Collection    string
    DocumentKey   string              // String representation of _id
    FullDocument  json.RawMessage     // Raw JSON of document
    UpdateDesc    *UpdateDescription  // Field-level changes (update ops only)
    Timestamp     time.Time
    Namespace     string              // "database.collection"
}

type UpdateDescription struct {
    UpdatedFields   map[string]any  // Fields that changed with new values
    RemovedFields   []string        // Fields that were removed
    TruncatedArrays []string        // Arrays that were truncated
}
```

### Payload Modes

1. **Default (ChangeEvent)**: Full `ChangeEvent` as JSON payload
2. **FullDocumentOnly**: Just the document as BSON payload (for direct type subscription)

### Update Description Options

- `WithUpdateDescription()`: Adds `updated_fields` and `removed_fields` to event metadata
- `WithEmptyUpdates()`: Delivers update events with no field changes (default: discarded)
- `WithMaxUpdatedFieldsSize(bytes)`: Limits `updated_fields` metadata size; omits when exceeded (implicitly enables `WithUpdateDescription()`)
- `ContextUpdateDescription(ctx)`: Extracts `*UpdateDescription` from handler context

Metadata keys are exported constants: `MetadataUpdatedFields`, `MetadataRemovedFields`, etc.

### Resume Token Handling

- Key format: `"namespace:resumeTokenID"` (e.g., `"mydb.orders:hostname"`)
- Auto-saves after each processed change
- Clears stale tokens on `ChangeStreamHistoryLost` error
- First start saves initial position to avoid processing historical oplog

### Resume Token Saves

Resume token saves use a detached context (`context.WithTimeout(context.Background(), 10s)`) via the `saveResumeToken()` helper. This ensures tokens are persisted even during shutdown when the watcher context is cancelled.

### Error Handling

- `ChangeStreamHistoryLost`: Clears resume token and restarts from current position
- Connection errors: Exponential backoff with reconnection
- Processing errors: Logged but continues to next change

## Design Patterns

### Functional Options

```go
type Option func(*transportOptions)  // unexported options struct

mongodb.New(db,
    mongodb.WithCollection("orders"),
    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    mongodb.WithResumeTokenStore(store),
    mongodb.WithUpdateDescription(),
    mongodb.WithMaxUpdatedFieldsSize(4096),
)
```

### Compile-Time Interface Checks

```go
var (
    _ transport.Transport     = (*Transport)(nil)
    _ transport.HealthChecker = (*Transport)(nil)
)
```

### Atomic Status Management

```go
status int32  // 0=closed, 1=open
atomic.CompareAndSwapInt32(&t.status, 1, 0)  // Close
atomic.LoadInt32(&t.status) == 1             // IsOpen
```

## Dependencies

- `github.com/rbaliyan/event/v3` - Transport interface and utilities
- `go.mongodb.org/mongo-driver` - MongoDB driver
- `go.opentelemetry.io/otel` - OpenTelemetry API (metrics)
- `go.opentelemetry.io/otel/metric` - OpenTelemetry metric instruments
- `go.opentelemetry.io/otel/sdk/metric` - OpenTelemetry SDK (test only)

## Testing

Metrics tests use `sdkmetric.NewManualReader()` for deterministic reads — no external services needed.
Integration tests (transport, stores) require a MongoDB replica set. Run with:

```bash
go test -v ./...
```

## Limitations

- `Publish()` returns `ErrPublishNotSupported` - changes come from database writes
- Only Broadcast mode - WorkerPool mode is not supported (all subscribers receive all changes)
- Requires MongoDB replica set or sharded cluster for change streams
