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

The root package is the change stream transport. The module also ships MongoDB implementations of several `event/v3` abstractions as independent subpackages.

## Subpackages

| Package | Purpose | Canonical constructor |
|---------|---------|-----------------------|
| (root) | Change Stream transport | `mongodb.New(db, ...)` |
| `persistent` | Durable `persistent.Store` + `CheckpointStore` for composite transport | `persistent.NewStore(collection, ...)` |
| `checkpoint` | Standalone `CheckpointStore` (subscriber resume positions) | `checkpoint.NewMongoStore(collection, ...)` |
| `store` | Generic typed CRUD store with cursor pagination | `store.NewMongoStore[T](collection, ...)` |
| `outbox` | Transactional outbox store, publisher, and relays | `outbox.NewMongoStore(db, ...)` |
| `transaction` | `transaction.Manager` for MongoDB sessions | `transaction.NewMongoManager(client)` |
| `idempotency` | Shared, TTL-based message deduplication store | `idempotency.NewMongoStore(db, ...)` |
| `distributed` | Atomic claim state manager for WorkerPool emulation | `distributed.NewMongoStateManager(db, ...)` |
| `monitor` | Per-event lifecycle store for the monitor handler | `monitor.NewMongoStore(db)` |
| `schema` | Schema registry provider with change watching | `schema.NewMongoProvider(db, publisher)` |
| `codec` | BSON transport `Codec` (envelope) | `codec.BSON{}` |
| `payload` | BSON payload `Codec` (document body) | `payload.BSON{}` |

Note: index creation is no longer performed in store constructors (which do no I/O). The root transport calls `EnsureIndexes` on its resume-token and ack stores during `Start`; outbox relays call it during `Start`. Standalone store users should call `EnsureIndexes` once at startup.

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
- `MetricsMiddleware[T](*Metrics)` generic subscriber middleware (handler-level metrics)
- `WithMetrics(m *Metrics)` transport option (transport-level metrics)
- Handler-level counters: `mongodb_changes_processed_total`, `mongodb_changes_failed_total`
- Handler-level histograms: `mongodb_oplog_lag_seconds` (clusterTime → handler), `mongodb_handler_duration_seconds`
- Observable gauge: `mongodb_changes_pending` (callback-based via `SetPendingCallback`)
- Transport-level UpDownCounter: `mongodb_stream_active` (open streams, attr: namespace)
- Transport-level counter: `mongodb_stream_reconnections_total` (attrs: namespace, reason=error|history_lost)
- Transport-level histogram: `mongodb_stream_receive_lag_seconds` (clusterTime → transport receive, before handler)
- Middleware extracts `cluster_time` from context metadata to compute oplog lag
- Uses `event.ClassifyError()` for proper error classification (ErrAck = processed, all others = failed)
- `Close()` unregisters gauge callbacks

**Observability Stack** - Three-layer event lifecycle observability:
- Monitor middleware (`event/v3/monitor`): per-event lifecycle tracking (pending/completed/failed/retrying)
- Metrics middleware (`MetricsMiddleware`): aggregate throughput, latency, error rates via OpenTelemetry
- DLQ (`event-dlq`): capture permanently failed events for replay
- `AckQueryStore` interface extends `AckStore` with `List`/`Count` for monitoring pending events
- `monitor.MongoStore` implements `evtmonitor.StuckPendingProvider` (`StuckPendingCount` and `StuckPendingEntries`), enabling stuck-pending detection from the monitor HTTP handler via `WithStuckPendingProvider`
- `monitor.MongoStore` implements `evtmonitor.SummaryProvider` (`Summary`), enabling aggregate status counts and per-event statistics from the monitor HTTP handler

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
store, err := mongopersistent.NewStore(collection,
    mongopersistent.WithTTL(7*24*time.Hour),
    mongopersistent.WithVisibilityTimeout(10*time.Minute),
)

// Checkpoint store for consumer resume
checkpointStore, err := mongopersistent.NewCheckpointStore(checkpointCollection)

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
- Auto-saves after each processed change (throttled to every 5 seconds)
- Clears stale tokens on `ChangeStreamHistoryLost` error
- Default: First start saves initial position to skip historical oplog

**Resume Token Options:**
- `WithStartFromPast(d time.Duration)`: On first start (no token), start from the specified duration in the past to process available oplog history instead of the current position
- `WithoutResume()`: Disable resume token persistence entirely
- `WithResumeTokenID(id)`: Set custom ID for token key (default: hostname)
- `WithResumeTokenStore(store)`: Use custom storage backend
- `WithResumeTokenCollection(db, name)`: Store tokens in specific collection

**Resume Token Methods:**
- `ResetResumeToken(ctx)`: Clear stored token so next restart starts fresh
- `ResumeTokenKey()`: Get the storage key for this transport's token

### Resume Token Saves

Resume token saves use a detached context (`context.WithTimeout(context.Background(), 10s)`) via the `saveResumeToken()` helper. This ensures tokens are persisted even during shutdown when the watcher context is cancelled.

### Error Handling

- `ChangeStreamHistoryLost`: Clears resume token and restarts from current position
- Connection errors: Exponential backoff with reconnection
- Processing errors: Logged but continues to next change

### Resume Token Recovery

Resume tokens allow the transport to resume watching from where it left off after a restart. However, if the oplog no longer contains the position referenced by the token, a `ChangeStreamHistoryLost` error occurs and the token is automatically cleared.

**When tokens become invalid:**
- MongoDB's oplog window expires (typically 24-72 hours)
- Service was offline longer than the oplog retention period
- Token was saved but event processing failed before oplog was consumed

**Automatic recovery:**
The transport automatically handles `ChangeStreamHistoryLost` by clearing the stored token and restarting from the current oplog position. Some events may be missed in this scenario.

**Manual token reset:**
Use the `ResetResumeToken(ctx)` method to clear the stored token:
```go
if err := transport.ResetResumeToken(ctx); err != nil {
    log.Error("failed to reset token", "error", err)
}
```

**Direct MongoDB token operations:**

Resume tokens are stored in the `_event_resume_tokens` collection in the same database passed to `mongodb.New()` (or the `admin` database for cluster-level watches). The key format is `{namespace}:{resumeTokenID}`, for example `mydb.orders:hostname` for a collection-level watch.

```javascript
// Check stored tokens
db.getCollection("_event_resume_tokens").find({});

// Delete token for a specific collection
db.getCollection("_event_resume_tokens").deleteMany({_id: /orders/});

// Delete all tokens
db.getCollection("_event_resume_tokens").deleteMany({});
```

**WithStartFromPast behavior:**
The `WithStartFromPast(duration)` option only applies on FIRST start (when no resume token exists). On subsequent restarts, the transport resumes from the saved token position. To reprocess historical events:
1. Call `ResetResumeToken(ctx)` or delete the token from MongoDB
2. Restart the service

**Recommended monitoring:**
- Monitor for `ChangeStreamHistoryLost` errors in logs (`mongodb_stream_reconnections_total{reason="history_lost"}`)
- Alert when oplog lag approaches retention period
- Track `mongodb_stream_receive_lag_seconds` for network/oplog propagation lag
- Track `mongodb_oplog_lag_seconds` for total end-to-end lag (includes handler queuing)
- Alert on `mongodb_stream_active` drops to 0 (all streams disconnected)

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

The suite has five layers; CI runs each in a dedicated job (`.github/workflows/ci.yml`).

| Layer | How to run | MongoDB needed? |
|-------|------------|-----------------|
| Smoke | `just smoke` (build + `go test -short` + examples) | No |
| Unit | `go test -short -race ./...` | No |
| Integration | `just test-integration` (spins up a replica set) | Yes (replica set) |
| Benchmarks | `just bench` / `just bench-compare main` | No (pure hot paths) |
| Fuzz | `just fuzz <Target> <pkg> <dur>`; ClusterFuzzLite in CI | No |

- Unit/smoke tests are hermetic: integration tests gate on `testing.Short()` and on a MongoDB ping (`getMongoURI` → connect → `t.Skipf` when unavailable), so `go test -short ./...` needs no services.
- Metrics tests use `sdkmetric.NewManualReader()` for deterministic reads.
- Integration tests require a MongoDB replica set (change streams + transactions). The CI `integration` job starts `mongo:6.0 --replSet rs0`, sets `MONGO_URI`, runs the full suite with `-race`, and enforces a coverage floor.

### Benchmarks

Benchmarks live in `*_bench_test.go` / `bench_test.go` files and cover the per-event hot paths (BSON→JSON conversion, change-event extraction, payload/metadata building, codec round-trips, the `Field[T]` accessor, and store query builders). All use `b.ReportAllocs()` and `b.Run` sub-benchmarks. The CI `bench` job runs them and posts a `benchstat` base-vs-HEAD comparison to the job summary.

### Fuzzing

Native Go fuzz targets live in `fuzz_test.go` files; targets are registered for ClusterFuzzLite in `.clusterfuzzlite/build.sh`. Targets cover the untrusted-input boundary and use real oracles (decode→encode→decode round-trip fixpoints, cross-type coercion agreement, JSON-validity of converted output) rather than don't-panic only. The byte-oriented targets feed RAW bytes through the production driver decoder (`bson.Unmarshal`) so the real parser — not a JSON re-encode — is exercised. Seed corpora are committed under `testdata/fuzz/<Target>/` (real change-stream-shaped insert/update/replace/delete documents, docs with ObjectID/Decimal128/Binary/DateTime/Timestamp, and deeply nested / array-heavy shapes; codec/payload seeds are produced by their real `Encode`). CI runs ClusterFuzzLite in `code-change` mode per PR (`cflite_pr.yml`) and a weekly hour-long ASan batch (`cflite_batch.yml`).

**Targets and packages:**

| Target | Package | What it fuzzes |
|--------|---------|----------------|
| `FuzzFormatDocumentKey` | `.` (root) | `formatDocumentKey` over arbitrary `_id` strings |
| `FuzzBsonDToJSON` | `.` (root) | raw bytes → `bson.Unmarshal` → `bsonDToJSON`; oracle: output is valid JSON |
| `FuzzConvertBSONTypes` | `.` (root) | raw bytes → `bson.Unmarshal` → `convertBSONTypes`; oracle: output is JSON-encodable |
| `FuzzChangeEventJSON` | `.` (root) | JSON → `ChangeEvent` unmarshal (JSON by design) |
| `FuzzConvertBSOND` | `.` (root) | hand-built `bson.D` with special types → `bsonDToJSON` |
| `FuzzFieldCoerce` | `.` (root) | `Field[T]` / `coerceNumeric` cross-type agreement |
| `FuzzContextUpdateDescription` | `.` (root) | metadata JSON → `ContextUpdateDescription`; oracle: nil-iff-no-keys, internal consistency |
| `FuzzFullDocumentUnmarshal` | `.` (root) | raw bytes as `bson.Raw` FullDocument → production decode path |
| `FuzzDecodeMongoCursor` | `monitor` | `decodeMongoCursor` round-trip stability |
| `FuzzWorkerCursor` | `distributed` | `decodeWorkerCursor` round-trip stability |
| `FuzzBSONCodecDecode` / `FuzzBSONCodecRoundTrip` | `codec` | message envelope BSON decode / round-trip |
| `FuzzPayloadBSONDecode` / `FuzzPayloadBSONRoundTrip` | `payload` | payload BSON decode / round-trip |

**Dictionary:** `.clusterfuzzlite/fuzz.dict` is a libFuzzer dictionary of BSON element type bytes, the codec envelope field names (`id`, `source`, `payload`, `metadata`, `timestamp`, `retry`), the change-stream schema tokens (`operationType`, `ns`, `fullDocument`, `updateDescription`, `updatedFields`, `removedFields`, `_id`, …), and JSON structural tokens. `build.sh` copies it into `$OUT` and writes per-fuzzer `$OUT/<name>.dict` + `$OUT/<name>.options` (`[libfuzzer]\ndict = <name>.dict`) for the byte-oriented targets only (scalar-argument targets like `FuzzFieldCoerce` are deliberately omitted). This is the OSS-Fuzz/ClusterFuzzLite convention for structure-aware mutation.

**Crash → regression-seed triage loop:**
1. A ClusterFuzzLite run surfaces a crash as a SARIF alert (PR Security tab) or a build artifact containing the reproducer testcase.
2. Reproduce locally with an **anchored** run: `go test -run='^$' -fuzz='^Name$' <pkg>` (e.g. `go test -run='^$' -fuzz='^FuzzWorkerCursor$' ./distributed/`). When `go test` finds a failing input it writes it to `testdata/fuzz/<Target>/<hash>`.
3. Commit that failing input under `testdata/fuzz/<Target>/` as a **permanent regression seed** — it then replays on every `go test -run=Fuzz` and guards against reintroduction.
4. Fix the code; the committed seed turns the crash into a standing regression test.

**Anchored-pattern caveat:** always anchor the `-fuzz` pattern with `^…$`. The pattern is a regexp matched against target names, so an unanchored `-fuzz=FuzzBsonDToJSON` would also match any future target whose name *contains* `FuzzBsonDToJSON` (it is a prefix of such names), running more than intended. Use `-fuzz='^FuzzBsonDToJSON$'`. The same applies to `-run`; pair `-run='^$'` with `-fuzz` so only the fuzz engine runs and unit tests are skipped.

**Rule:** new code that parses or decodes untrusted input must ship a corresponding fuzz target registered in `build.sh` with a unique `fuzz_*` binary name, plus committed seeds under `testdata/fuzz/<Target>/`.

## Limitations

- `Publish()` returns `ErrPublishNotSupported` - changes come from database writes
- Only Broadcast mode - WorkerPool mode is not supported (all subscribers receive all changes)
- Requires MongoDB replica set or sharded cluster for change streams
