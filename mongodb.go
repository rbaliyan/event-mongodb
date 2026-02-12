// Package mongodb provides a MongoDB change stream transport implementation.
//
// This transport watches MongoDB for changes and delivers them as events.
// Publishing is implicit - writing to MongoDB triggers events automatically.
//
// Watch Levels:
//
// The transport supports three levels of change stream watching:
//
//   - Collection-level: Watch a specific collection
//   - Database-level: Watch all collections in a database
//   - Cluster-level: Watch all databases in a cluster
//
// Features:
//   - Subscribe to changes (insert, update, delete, replace)
//   - Resume tokens for reliable change stream resumption
//   - Optional acknowledgment tracking via MongoDB collection
//   - Automatic reconnection with exponential backoff
//
// Limitations:
//   - Publish() is not supported - changes are triggered by database writes
//   - Only Broadcast delivery mode is supported (WorkerPool mode is ignored)
//   - All subscribers receive all changes
//
// Usage:
//
//	// Watch a specific collection
//	t, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	)
//
//	// Watch all collections in a database
//	t, _ := mongodb.New(db) // No WithCollection = database-level
//
//	// Watch all databases in a cluster
//	t, _ := mongodb.NewClusterWatch(client)
//
//	// Register and subscribe
//	t.RegisterEvent(ctx, "order-changes")
//	sub, _ := t.Subscribe(ctx, "order-changes")
//
//	// Process changes
//	for msg := range sub.Messages() {
//	    var change mongodb.ChangeEvent
//	    bson.Unmarshal(msg.Payload(), &change)
//	    fmt.Printf("Change in %s.%s: %s\n",
//	        change.Database, change.Collection, change.OperationType)
//	    msg.Ack(nil)
//	}
//
// The transport automatically watches and delivers change events.
// "Publishing" happens implicitly when documents are inserted/updated/deleted.
package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/base"
	"github.com/rbaliyan/event/v3/transport/channel"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Transport status constants
const (
	statusClosed int32 = 0
	statusOpen   int32 = 1
)

// watchLevel indicates what level of MongoDB hierarchy to watch.
// This is an internal detail determined by the constructor (New vs NewClusterWatch)
// and the WithCollection option.
type watchLevel int

const (
	watchLevelCollection watchLevel = iota
	watchLevelDatabase
	watchLevelCluster
)

// defaultResumeTokenCollection is the default collection name for storing resume tokens.
const defaultResumeTokenCollection = "_event_resume_tokens"

// resumeTokenSaveInterval is the minimum interval between resume token saves.
// Tokens are buffered between saves and flushed on stream close to avoid
// losing progress while reducing write amplification.
const resumeTokenSaveInterval = 5 * time.Second

// detachedTimeout is the timeout for operations that use a detached context
// (context.Background) to survive parent cancellation — resume token saves,
// ack store writes, change stream close, and fan-out publishes.
const detachedTimeout = 10 * time.Second

// options holds configuration values set via Option functions.
// This struct is unexported to prevent direct access — only Option functions
// can modify it.
type transportOptions struct {
	collectionName           string
	logger                   *slog.Logger
	onError                  func(error)
	bufferSize               int
	resumeTokenStore         ResumeTokenStore
	resumeTokenID            string
	ackStore                 AckStore
	disableResume            bool
	startFromPast            time.Duration // If > 0, start from this duration in the past when no resume token exists
	pipeline                 mongo.Pipeline
	fullDocument             FullDocumentOption
	batchSize                *int32
	maxAwaitTime             *time.Duration
	fullDocumentOnly         bool
	includeUpdateDescription bool
	emptyUpdates             bool
	maxUpdatedFieldsSize     int
}

// Transport implements transport.Transport using MongoDB change streams.
type Transport struct {
	transportOptions // Embedded configuration from Option functions

	status           int32
	client           *mongo.Client   // For cluster-level watch
	db               *mongo.Database // For database/collection-level watch
	level            watchLevel
	channelTransport *channel.Transport // Internal channel transport for fan-out
	registeredEvents sync.Map           // map[string]struct{} - tracks registered event names

	// Watcher state
	watcherOnce        sync.Once
	watcherCtx         context.Context
	watcherCancel      context.CancelFunc
	watcherWg          sync.WaitGroup
	startFromPastTried int32 // Atomic flag: 1 if startFromPast was tried (prevents infinite retry on oplog too short)
}

// Option configures the MongoDB transport.
type Option func(*transportOptions)

// WithCollection sets the collection to watch for changes.
// When provided, the transport watches only this collection (collection-level).
// When omitted (or empty), the transport watches all collections in the
// database (database-level). For cluster-level watching, use NewClusterWatch.
func WithCollection(name string) Option {
	return func(o *transportOptions) {
		if name != "" {
			o.collectionName = name
		}
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(o *transportOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithErrorHandler sets the error callback.
func WithErrorHandler(fn func(error)) Option {
	return func(o *transportOptions) {
		if fn != nil {
			o.onError = fn
		}
	}
}

// WithBufferSize sets the default buffer size for subscriptions.
func WithBufferSize(size int) Option {
	return func(o *transportOptions) {
		if size > 0 {
			o.bufferSize = size
		}
	}
}

// WithResumeTokenStore sets a custom store for persisting resume tokens.
// By default, the transport automatically stores resume tokens in the
// "_event_resume_tokens" collection. Use this option to override the default.
func WithResumeTokenStore(store ResumeTokenStore) Option {
	return func(o *transportOptions) {
		o.resumeTokenStore = store
	}
}

// WithResumeTokenCollection sets a custom database and collection for resume tokens.
// This is a convenience wrapper around WithResumeTokenStore.
//
// Example:
//
//	// Store resume tokens in a different database
//	transport, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithResumeTokenCollection(client.Database("internal"), "_resume_tokens"),
//	)
func WithResumeTokenCollection(db *mongo.Database, collectionName string) Option {
	return func(o *transportOptions) {
		o.resumeTokenStore = NewMongoResumeTokenStore(db.Collection(collectionName))
	}
}

// WithoutResume disables resume token persistence.
//
// Without resume tokens:
//   - On restart, the change stream starts from the CURRENT position (latest)
//   - Any changes that occurred while the service was down are MISSED
//
// With resume tokens (default):
//   - On restart, the change stream resumes from where it left off
//   - No changes are missed (as long as they're within MongoDB's oplog window)
//
// Use this only for scenarios where missing changes during restarts is acceptable,
// such as real-time dashboards that don't need historical accuracy.
func WithoutResume() Option {
	return func(o *transportOptions) {
		o.disableResume = true
	}
}

// WithResumeTokenID sets a unique identifier for resume token storage.
// This allows multiple instances to maintain their own resume positions.
//
// By default, the hostname is used. Each instance will:
//   - Store its own resume token with key: "namespace:id"
//   - Resume from its own last position on restart
//   - Process events independently (may cause duplicates across instances)
//
// Use cases:
//   - Multiple instances processing the same collection independently
//   - Instance-specific recovery after crashes
//   - Development/testing with multiple local instances
//
// For shared resume tokens (all instances resume from the same position),
// set the same ID across all instances or use an empty string.
//
// Example:
//
//	// Each instance uses its own resume token
//	mongodb.New(db, mongodb.WithResumeTokenID(os.Getenv("INSTANCE_ID")))
//
//	// Shared resume token across all instances
//	mongodb.New(db, mongodb.WithResumeTokenID("shared"))
func WithResumeTokenID(id string) Option {
	return func(o *transportOptions) {
		o.resumeTokenID = id
	}
}

// WithStartFromPast configures the transport to start from a specified duration
// in the past when no resume token exists (first start).
//
// Default behavior (without this option):
//   - On first start, the change stream starts from the CURRENT position
//   - Only new changes (after startup) are processed
//   - Historical changes in the oplog are skipped
//
// With this option:
//   - On first start, the change stream starts from the specified duration ago
//   - Historical changes within that window are processed first
//   - Useful for processing existing data or recovering missed events
//
// This option only affects the first start (when no resume token exists).
// Subsequent restarts resume from the stored token as normal.
//
// The duration should be within the oplog retention window (typically 24-72 hours
// on MongoDB Atlas). If the specified time is before the oplog window, MongoDB
// will return ChangeStreamHistoryLost and the transport will fall back to
// starting from the current position.
//
// WARNING: Starting from the past may process a large number of events.
// Ensure your handlers are idempotent as events may be reprocessed on restart.
//
// Example:
//
//	// Start from 24 hours ago on first start
//	mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithStartFromPast(24 * time.Hour),
//	)
func WithStartFromPast(d time.Duration) Option {
	return func(o *transportOptions) {
		o.startFromPast = d
	}
}

// WithAckStore sets the store for tracking acknowledgments.
// This enables at-least-once delivery semantics.
func WithAckStore(store AckStore) Option {
	return func(o *transportOptions) {
		o.ackStore = store
	}
}

// WithPipeline sets an aggregation pipeline to filter change events.
//
// Example - only watch insert and update operations:
//
//	pipeline := mongo.Pipeline{
//	    {{Key: "$match", Value: bson.M{
//	        "operationType": bson.M{"$in": []string{"insert", "update"}},
//	    }}},
//	}
//	mongodb.WithPipeline(pipeline)
func WithPipeline(pipeline mongo.Pipeline) Option {
	return func(o *transportOptions) {
		o.pipeline = pipeline
	}
}

// WithFullDocument configures full document lookup for update events.
//
// Use one of the FullDocument* constants:
//   - FullDocumentDefault: Only include full document for insert/replace
//   - FullDocumentUpdateLookup: Lookup current document for updates (most common)
//   - FullDocumentWhenAvailable: Return post-image if available (MongoDB 6.0+)
//   - FullDocumentRequired: Require post-image or fail (MongoDB 6.0+)
//
// Example:
//
//	mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
func WithFullDocument(option FullDocumentOption) Option {
	return func(o *transportOptions) {
		o.fullDocument = option
	}
}

// WithBatchSize sets the batch size for change stream operations.
func WithBatchSize(size int32) Option {
	return func(o *transportOptions) {
		o.batchSize = &size
	}
}

// WithMaxAwaitTime sets the maximum time to wait for new changes.
func WithMaxAwaitTime(d time.Duration) Option {
	return func(o *transportOptions) {
		o.maxAwaitTime = &d
	}
}

// WithFullDocumentOnly configures the transport to send only the fullDocument
// as the message payload instead of the entire ChangeEvent.
//
// This is useful when you want to subscribe with your document type directly:
//
//	// Instead of subscribing to mongodb.ChangeEvent:
//	event.New[mongodb.ChangeEvent]("order.created")
//
//	// You can subscribe to your document type directly:
//	event.New[Order]("order.created")
//
// IMPORTANT: This requires WithFullDocument(FullDocumentUpdateLookup) or similar
// to ensure fullDocument is populated. Without it:
//   - Insert: fullDocument is always present
//   - Update: fullDocument is empty unless FullDocumentUpdateLookup is set
//   - Delete: fullDocument is always empty (event will be skipped)
//
// Events where fullDocument is empty will be skipped (not delivered to subscribers).
// The operation type, database, and collection are still available in message metadata.
//
// Example:
//
//	transport, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	    mongodb.WithFullDocumentOnly(), // Send just the document
//	)
//
//	// Now subscribe with your type directly
//	orderEvent := event.New[Order]("order.created")
//	orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
//	    fmt.Println("Order:", order.ID)
//	    return nil
//	})
func WithFullDocumentOnly() Option {
	return func(o *transportOptions) {
		o.fullDocumentOnly = true
	}
}

// WithUpdateDescription includes UpdateDescription (updated and removed fields)
// from change stream events in the message metadata. This is useful for audit
// trails or tracking which fields changed in update operations.
//
// When enabled, two metadata keys are added for update/replace operations:
//   - "updated_fields": JSON-encoded map of fields that were updated
//   - "removed_fields": JSON-encoded array of fields that were removed
//
// These keys are omitted for non-update operations (insert, delete) or when
// the change event has no UpdateDescription.
//
// Use ContextUpdateDescription(ctx) to extract the UpdateDescription from
// the handler context.
func WithUpdateDescription() Option {
	return func(o *transportOptions) {
		o.includeUpdateDescription = true
	}
}

// WithEmptyUpdates delivers update events even when UpdateDescription has
// no field changes (no updated or removed fields). By default, such empty updates
// are silently discarded since they carry no meaningful change information.
//
// MongoDB can produce empty updates in scenarios like:
//   - $set to the same value (no-op update)
//   - Internal replication events
//   - Updates that only affect internal fields
func WithEmptyUpdates() Option {
	return func(o *transportOptions) {
		o.emptyUpdates = true
	}
}

// WithMaxUpdatedFieldsSize sets the maximum size in bytes for the serialized
// updated_fields metadata value. When the JSON-encoded updated fields exceed
// this limit, updated_fields is omitted from metadata and the subscriber
// should rely on the full document instead. The removed_fields metadata is
// always included regardless of this limit since it only contains field names.
//
// This requires WithFullDocument to be set so there is a fallback when the
// updated fields are too large. Returns an error from New() if fullDocument
// is not configured.
//
// A value of 0 or negative means unlimited (no size limit).
// This option implicitly enables WithUpdateDescription().
func WithMaxUpdatedFieldsSize(bytes int) Option {
	return func(o *transportOptions) {
		if bytes > 0 {
			o.maxUpdatedFieldsSize = bytes
		}
		o.includeUpdateDescription = true
	}
}

// New creates a new MongoDB change stream transport.
//
// Watch levels:
//   - With WithCollection("name"): watches a specific collection
//   - Without WithCollection: watches all collections in the database
//
// For cluster-level watching (all databases), use NewClusterWatch instead.
//
// Resume tokens are automatically persisted to enable reliable resumption
// after restarts. Use WithoutResume() to disable this behavior.
//
// Example:
//
//	// Watch a specific collection
//	t, err := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
//
//	// Watch all collections in the database
//	t, err := mongodb.New(db)
//
//	// Watch all collections with options
//	t, err := mongodb.New(db,
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
func New(db *mongo.Database, opts ...Option) (*Transport, error) {
	if db == nil {
		return nil, ErrDatabaseRequired
	}

	o := transportOptions{
		logger:     transport.Logger("transport>mongodb"),
		onError:    func(error) {},
		bufferSize: 100,
	}
	for _, opt := range opts {
		opt(&o)
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())

	t := &Transport{
		transportOptions: o,
		status:           statusOpen,
		db:               db,
		client:           db.Client(),
		watcherCtx:       watchCtx,
		watcherCancel:    watchCancel,
	}

	// Determine watch level based on collection name
	if t.collectionName != "" {
		t.level = watchLevelCollection
	} else {
		t.level = watchLevelDatabase
	}

	if err := t.init(); err != nil {
		watchCancel()
		return nil, err
	}

	return t, nil
}

// NewClusterWatch creates a MongoDB transport that watches all databases.
//
// This watches the entire MongoDB cluster/deployment for changes across
// all databases and collections. Use this when you need to monitor
// changes across your entire MongoDB deployment.
//
// Example:
//
//	// Watch all databases in the cluster
//	t, err := mongodb.NewClusterWatch(client)
//
//	// With options
//	t, err := mongodb.NewClusterWatch(client,
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
//
//	// Subscribe to changes
//	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
//	    fmt.Printf("Change in %s.%s: %s\n",
//	        change.Database, change.Collection, change.OperationType)
//	    return nil
//	})
func NewClusterWatch(client *mongo.Client, opts ...Option) (*Transport, error) {
	if client == nil {
		return nil, ErrClientRequired
	}

	o := transportOptions{
		logger:     transport.Logger("transport>mongodb"),
		onError:    func(error) {},
		bufferSize: 100,
	}
	for _, opt := range opts {
		opt(&o)
	}

	// Use "admin" database for storing resume tokens
	adminDB := client.Database("admin")

	watchCtx, watchCancel := context.WithCancel(context.Background())

	t := &Transport{
		transportOptions: o,
		status:           statusOpen,
		client:           client,
		db:               adminDB,
		level:            watchLevelCluster,
		watcherCtx:       watchCtx,
		watcherCancel:    watchCancel,
	}

	if err := t.init(); err != nil {
		watchCancel()
		return nil, err
	}

	return t, nil
}

func (t *Transport) validate() error {
	hasFullDocument := t.fullDocument != "" && t.fullDocument != FullDocumentDefault

	// maxUpdatedFieldsSize requires fullDocument as fallback when updated fields are omitted
	if t.maxUpdatedFieldsSize > 0 && !hasFullDocument {
		return ErrMaxUpdatedFieldsSizeRequiresFull
	}

	// fullDocumentOnly mode requires fullDocument to populate the payload
	if t.fullDocumentOnly && !hasFullDocument {
		return ErrFullDocumentRequired
	}

	return nil
}

// init performs shared initialization after options are applied.
// Called by both New() and NewClusterWatch().
func (t *Transport) init() error {
	if err := t.validate(); err != nil {
		return err
	}

	// Create internal channel transport for fan-out
	t.channelTransport = channel.New(
		channel.WithBufferSize(uint(t.bufferSize)),
		channel.WithLogger(t.logger),
	)

	// Default resume token ID to hostname
	if t.resumeTokenID == "" {
		if hostname, err := os.Hostname(); err == nil {
			t.resumeTokenID = hostname
		} else {
			t.resumeTokenID = "default"
		}
	}

	// Auto-create resume token store if not disabled and not provided
	if !t.disableResume && t.resumeTokenStore == nil {
		t.resumeTokenStore = NewMongoResumeTokenStore(t.db.Collection(defaultResumeTokenCollection))
		t.logger.Debug("using default resume token collection",
			"database", t.db.Name(),
			"collection", defaultResumeTokenCollection)
	}

	return nil
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == statusOpen
}

// RegisterEvent creates resources for an event.
// The change stream watcher is started lazily when the first subscriber is added.
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	// Delegate to channel transport
	if err := t.channelTransport.RegisterEvent(ctx, name); err != nil {
		return err
	}

	// Track registered event name
	t.registeredEvents.Store(name, struct{}{})

	// Note: watcher is started in Subscribe() to avoid race condition where
	// historical events (from WithStartFromPast) are dropped before subscribers exist.

	t.logger.Debug("registered event", "event", name, "collection", t.collectionName)
	return nil
}

// UnregisterEvent cleans up event resources.
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	// Delegate to channel transport
	if err := t.channelTransport.UnregisterEvent(ctx, name); err != nil {
		return err
	}

	// Remove from tracked events
	t.registeredEvents.Delete(name)

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish always returns ErrPublishNotSupported because MongoDB change stream
// events are triggered by database writes, not by explicit publishing.
//
// Unlike traditional transports (Redis, NATS, Kafka) where Publish sends a
// message to subscribers, the MongoDB transport watches for changes that occur
// when your application (or any other client) inserts, updates, or deletes
// documents. There is no need to call Publish -- writing to MongoDB IS the
// publish action.
//
// If you are writing generic transport code that calls Publish, you should
// check for this error and handle it appropriately:
//
//	err := transport.Publish(ctx, name, msg)
//	if errors.Is(err, mongodb.ErrPublishNotSupported) {
//	    // MongoDB transport: writes to the database trigger events automatically
//	    return nil
//	}
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	return ErrPublishNotSupported
}

// Subscribe creates a subscription to receive change events.
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	// Delegate to channel transport - it handles all the fan-out logic
	sub, err := t.channelTransport.Subscribe(ctx, name, opts...)
	if err != nil {
		return nil, err
	}

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.ID())
	return sub, nil
}

// Start begins watching the MongoDB change stream.
// Call this method AFTER all subscribers have been registered to ensure
// no events are dropped. This is especially important when using
// WithStartFromPast, which replays historical events on startup.
//
// If Start is not called explicitly, the watcher will not start automatically.
// This gives the application explicit control over when to begin processing events.
//
// Example:
//
//	// Create transport
//	t, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithStartFromPast(8 * time.Hour),
//	)
//
//	// Register events and subscribers on bus
//	bus, _ := event.NewBus("app", event.WithTransport(t))
//	orderEvent := event.New[Order]("order.created")
//	bus.Register(ctx, orderEvent)
//	orderEvent.Subscribe(ctx, handler)
//
//	// NOW start watching - all subscribers are ready
//	t.Start(ctx)
func (t *Transport) Start(ctx context.Context) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}
	t.startWatcher()
	t.logger.Info("change stream watcher started")
	return nil
}

// Close shuts down the transport.
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, statusOpen, statusClosed) {
		return nil
	}

	// Stop the watcher
	t.watcherCancel()
	t.watcherWg.Wait()

	// Close the channel transport (closes all subscriptions)
	if err := t.channelTransport.Close(ctx); err != nil {
		t.logger.Warn("failed to close channel transport", "error", err)
	}

	t.logger.Debug("transport closed")
	return nil
}

// ResetResumeToken clears the stored resume token for this transport.
// After calling this method, the next restart will start from:
//   - The beginning of the oplog (if WithStartFromBeginning was set)
//   - The current position (default behavior)
//
// This is useful when:
//   - The resume token has become stale (oplog has rotated past it)
//   - You want to reprocess events from a fresh position
//   - Troubleshooting change stream issues
//
// Note: This only clears the token in storage. The currently running
// change stream continues from its current position until restarted.
// To apply the reset, restart the transport after calling this method.
//
// Example:
//
//	// Clear stale token and restart
//	if err := transport.ResetResumeToken(ctx); err != nil {
//	    log.Printf("failed to reset token: %v", err)
//	}
//	// Restart the application to start fresh
func (t *Transport) ResetResumeToken(ctx context.Context) error {
	if t.resumeTokenStore == nil {
		return nil // No token store configured
	}

	resumeKey := t.resumeTokenKey()
	if err := t.resumeTokenStore.Save(ctx, resumeKey, nil); err != nil {
		return fmt.Errorf("failed to clear resume token %s: %w", resumeKey, err)
	}

	t.logger.Info("resume token cleared", "key", resumeKey)
	return nil
}

// ResumeTokenKey returns the key used for storing this transport's resume token.
// This can be useful for external monitoring or manual token management.
func (t *Transport) ResumeTokenKey() string {
	return t.resumeTokenKey()
}

// Health performs a health check.
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()

	result := &transport.HealthCheckResult{
		CheckedAt: start,
		Details:   make(map[string]any),
	}

	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	// Ping MongoDB
	pingStart := time.Now()
	err := t.db.Client().Ping(ctx, nil)
	pingLatency := time.Since(pingStart)

	if err != nil {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "mongodb ping failed"
		result.Latency = time.Since(start)
		result.Details["ping_error"] = err.Error()
		return result
	}

	// Get stats from channel transport
	channelHealth := t.channelTransport.Health(ctx)

	result.Status = transport.HealthStatusHealthy
	result.Message = "mongodb transport is healthy"
	result.Latency = time.Since(start)
	result.Details["type"] = "mongodb-changestream"
	result.Details["watch_level"] = t.watchLevelString()
	if t.db != nil {
		result.Details["database"] = t.db.Name()
	}
	if t.collectionName != "" {
		result.Details["collection"] = t.collectionName
	}
	result.Details["events"] = channelHealth.Details["events"]
	result.Details["subscribers"] = channelHealth.Details["subscribers"]
	result.Details["ping_latency_ms"] = pingLatency.Milliseconds()

	return result
}

// startWatcher starts the change stream watcher goroutine.
func (t *Transport) startWatcher() {
	t.watcherOnce.Do(func() {
		t.watcherWg.Add(1)
		go func() {
			defer t.watcherWg.Done()
			t.watchLoop(t.watcherCtx)
		}()
	})
}

// watchLoop continuously watches the change stream with reconnection.
func (t *Transport) watchLoop(ctx context.Context) {
	t.logger.Info("watchLoop started")
	defer t.logger.Info("watchLoop exited")

	backoff := base.NewBackoff()
	iteration := 0

	for {
		iteration++
		t.logger.Debug("watchLoop iteration starting", "iteration", iteration)

		select {
		case <-ctx.Done():
			t.logger.Info("watchLoop context done, exiting", "error", ctx.Err())
			return
		default:
		}

		err := t.watchOnce(ctx, backoff.Reset)
		t.logger.Debug("watchOnce returned", "iteration", iteration, "error", err)

		if err != nil {
			// Only exit if the parent context is done.
			// Don't use errors.Is(err, context.DeadlineExceeded) because MongoDB driver
			// wraps context.DeadlineExceeded for per-operation timeouts, which should
			// be retried, not cause exit.
			if ctx.Err() != nil {
				t.logger.Info("watchLoop parent context done, exiting", "ctx_error", ctx.Err())
				return
			}

			// Notify error handler
			t.onError(err)

			backoffDuration := backoff.Next()
			t.logger.Error("change stream error, reconnecting",
				"error", err, "backoff", backoffDuration)

			// Check if this is a ChangeStreamHistoryLost error
			// This means the resume token or start time points to a position no longer in the oplog
			if isChangeStreamHistoryLost(err) {
				t.logger.Warn("oplog does not contain requested position, clearing and starting fresh")
				if t.resumeTokenStore != nil {
					resumeKey := t.resumeTokenKey()
					clearCtx, clearCancel := context.WithTimeout(context.Background(), detachedTimeout)
					if clearErr := t.resumeTokenStore.Save(clearCtx, resumeKey, nil); clearErr != nil {
						t.logger.Error("failed to clear stale resume token", "error", clearErr)
					}
					clearCancel()
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoffDuration):
			}
			continue
		}

		// Reset backoff on clean exit (shouldn't happen normally)
		backoff.Reset()
	}
}

// isChangeStreamHistoryLost checks if the error indicates the resume token
// is no longer valid because the oplog has rolled past that position.
// Uses the MongoDB driver's typed error API (error code 286) with a
// string-based fallback for wrapped or non-standard errors.
func isChangeStreamHistoryLost(err error) bool {
	if err == nil {
		return false
	}
	// Prefer typed error check via mongo.ServerError interface (error code 286)
	var se mongo.ServerError
	if errors.As(err, &se) {
		return se.HasErrorCode(286)
	}
	// Fallback: string matching for wrapped or non-standard errors
	errStr := err.Error()
	return strings.Contains(errStr, "ChangeStreamHistoryLost") ||
		strings.Contains(errStr, "resume point may no longer be in the oplog")
}

// watchOnce creates and processes a single change stream.
// onConnected is called after the first successfully processed change event,
// allowing the caller to reset backoff state on successful reconnection.
func (t *Transport) watchOnce(ctx context.Context, onConnected func()) error {
	// Build change stream options
	csOpts := options.ChangeStream()

	if t.fullDocument != "" {
		csOpts.SetFullDocument(t.fullDocument.toDriverOption())
	}
	if t.batchSize != nil {
		csOpts.SetBatchSize(*t.batchSize)
	}
	if t.maxAwaitTime != nil {
		csOpts.SetMaxAwaitTime(*t.maxAwaitTime)
	}

	// Resume token key depends on watch level
	resumeKey := t.resumeTokenKey()

	// Try to resume from stored token
	var hasExistingToken bool
	if t.resumeTokenStore != nil {
		token, err := t.resumeTokenStore.Load(ctx, resumeKey)
		if err != nil {
			t.logger.Warn("failed to load resume token", "error", err)
		} else if token != nil {
			csOpts.SetResumeAfter(token)
			hasExistingToken = true
			t.logger.Debug("resuming from stored token", "key", resumeKey)
		}
	}

	// When startFromPast is configured and no existing token, set startAtOperationTime
	// to start from the specified duration in the past.
	// Use atomic flag to ensure we only try once - if oplog doesn't go back far enough,
	// we fall back to current position on retry.
	if !hasExistingToken && t.startFromPast > 0 && atomic.CompareAndSwapInt32(&t.startFromPastTried, 0, 1) {
		startTime := time.Now().Add(-t.startFromPast)
		// Convert to MongoDB Timestamp (seconds since Unix epoch in T field)
		csOpts.SetStartAtOperationTime(&bson.Timestamp{T: uint32(startTime.Unix()), I: 0})
		t.logger.Info("starting from past (no resume token)",
			"key", resumeKey,
			"start_time", startTime,
			"duration_ago", t.startFromPast)
	}

	// Open change stream based on watch level
	var cs *mongo.ChangeStream
	var err error

	switch t.level {
	case watchLevelCollection:
		cs, err = t.db.Collection(t.collectionName).Watch(ctx, t.pipeline, csOpts)
		t.logger.Info("change stream opened",
			"watch_level", "collection",
			"database", t.db.Name(),
			"target_collection", t.collectionName)
	case watchLevelDatabase:
		cs, err = t.db.Watch(ctx, t.pipeline, csOpts)
		t.logger.Info("change stream opened", "watch_level", "database",
			"database", t.db.Name())
	case watchLevelCluster:
		cs, err = t.client.Watch(ctx, t.pipeline, csOpts)
		t.logger.Info("change stream opened", "watch_level", "cluster")
	}

	if err != nil {
		return err
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), detachedTimeout)
		defer closeCancel()
		_ = cs.Close(closeCtx)
	}()

	// Handle first-time start (no existing token).
	// Default: save current position to skip historical oplog events.
	// With startFromPast: process historical events, don't save initial position.
	startedFromPast := t.startFromPast > 0 && atomic.LoadInt32(&t.startFromPastTried) == 1
	if !hasExistingToken && t.resumeTokenStore != nil && !startedFromPast {
		// Get a single event to establish our position, or use current token.
		// We need to call TryNext to get a resume token without blocking.
		if cs.TryNext(ctx) {
			// Got an event — process it first, then save token only on success.
			if err := t.processChange(ctx, cs); err != nil {
				t.logger.Error("failed to process first change", "error", err)
				t.onError(err)
				// Don't save resume token — allows reprocessing on next restart
			} else if err := t.saveResumeToken(resumeKey, cs.ResumeToken()); err != nil {
				t.logger.Warn("failed to save initial resume token", "error", err)
			} else {
				t.logger.Info("saved initial resume token", "key", resumeKey)
			}
		} else if cs.ResumeToken() != nil {
			// No event yet, but we have a resume token from the cursor
			if err := t.saveResumeToken(resumeKey, cs.ResumeToken()); err != nil {
				t.logger.Warn("failed to save initial resume token", "error", err)
			} else {
				t.logger.Info("saved initial resume token, starting from current position", "key", resumeKey)
			}
		}
	}
	// Note: startFromPast case is handled above before opening the change stream

	// Process changes
	connected := false
	lastTokenSave := time.Now()
	var pendingToken bson.Raw
	eventCount := 0
	t.logger.Debug("entering change stream loop")
	for cs.Next(ctx) {
		eventCount++
		if err := t.processChange(ctx, cs); err != nil {
			t.logger.Error("failed to process change", "event_count", eventCount, "error", err)
			t.onError(err)
			// Skip resume token save on failure to allow reprocessing
			continue
		}

		// Log periodic progress (every 10000 events to reduce log noise at high throughput)
		if eventCount%10000 == 0 {
			t.logger.Info("change stream progress", "events_processed", eventCount)
		}

		// Signal successful processing so caller can reset backoff
		if !connected {
			connected = true
			onConnected()
		}

		// Throttle resume token saves to reduce write amplification.
		// Save at most once per resumeTokenSaveInterval, buffering the
		// latest token between saves.
		if t.resumeTokenStore != nil {
			pendingToken = cs.ResumeToken()
			if time.Since(lastTokenSave) >= resumeTokenSaveInterval {
				if err := t.saveResumeToken(resumeKey, pendingToken); err != nil {
					t.logger.Warn("failed to save resume token", "error", err)
				} else {
					pendingToken = nil
					lastTokenSave = time.Now()
				}
			}
		}
	}

	// Log why we exited the loop
	csErr := cs.Err()
	t.logger.Info("change stream loop exited",
		"events_processed", eventCount,
		"cs_error", csErr,
		"ctx_error", ctx.Err())

	// Flush any pending resume token before exiting so we don't lose
	// progress when the change stream closes cleanly or on error.
	if t.resumeTokenStore != nil && pendingToken != nil {
		if err := t.saveResumeToken(resumeKey, pendingToken); err != nil {
			t.logger.Warn("failed to flush resume token on exit", "error", err)
		}
	}

	return csErr
}

// saveResumeToken persists a resume token using a detached context so the
// save succeeds even when the watcher context is being cancelled during shutdown.
func (t *Transport) saveResumeToken(resumeKey string, token bson.Raw) error {
	saveCtx, cancel := context.WithTimeout(context.Background(), detachedTimeout)
	defer cancel()
	return t.resumeTokenStore.Save(saveCtx, resumeKey, token)
}

// resumeTokenKey returns the key used for storing resume tokens.
// Format: "namespace:id" where namespace is based on watch level
// and id is the resumeTokenID (defaults to hostname).
func (t *Transport) resumeTokenKey() string {
	var namespace string
	switch t.level {
	case watchLevelCollection:
		namespace = t.db.Name() + "." + t.collectionName
	case watchLevelDatabase:
		namespace = t.db.Name() + ".*"
	case watchLevelCluster:
		namespace = "*.*"
	default:
		namespace = "default"
	}
	return namespace + ":" + t.resumeTokenID
}

// watchLevelString returns a human-readable string for the watch level.
func (t *Transport) watchLevelString() string {
	switch t.level {
	case watchLevelCollection:
		return "collection"
	case watchLevelDatabase:
		return "database"
	case watchLevelCluster:
		return "cluster"
	default:
		return "unknown"
	}
}

// changeStream abstracts the methods used from *mongo.ChangeStream,
// enabling unit testing of the processing pipeline without a live MongoDB connection.
type changeStream interface {
	Decode(val interface{}) error
	Next(ctx context.Context) bool
	TryNext(ctx context.Context) bool
	ResumeToken() bson.Raw
	Err() error
	Close(ctx context.Context) error
}

// Compile-time check that *mongo.ChangeStream satisfies changeStream.
var _ changeStream = (*mongo.ChangeStream)(nil)

// processChange processes a single change event from the change stream.
func (t *Transport) processChange(_ context.Context, cs changeStream) error {
	// Decode change document into typed struct for type safety.
	var doc changeStreamDoc
	if err := cs.Decode(&doc); err != nil {
		return err
	}

	// Extract change event data
	changeEvent := t.extractChangeEvent(doc)

	t.logger.Debug("change event received",
		"operation", changeEvent.OperationType,
		"database", changeEvent.Database,
		"collection", changeEvent.Collection,
		"document_key", changeEvent.DocumentKey)

	// Discard empty updates unless explicitly included
	if !t.emptyUpdates && isEmptyUpdate(changeEvent) {
		t.logger.Debug("skipping empty update event",
			"operation", changeEvent.OperationType,
			"document_key", changeEvent.DocumentKey)
		return nil
	}

	// Build payload and content type
	payload, contentType, err := t.buildPayload(doc, changeEvent)
	if err != nil {
		return err
	}
	if payload == nil {
		t.logger.Debug("skipping event with nil payload",
			"operation", changeEvent.OperationType,
			"database", changeEvent.Database,
			"collection", changeEvent.Collection)
		return nil // Event was skipped (e.g., no fullDocument)
	}

	// Build metadata and message
	metadata := t.buildMetadata(changeEvent, contentType)
	msg := t.buildMessage(changeEvent, payload, metadata)

	// Store as pending and publish
	t.logger.Debug("publishing change event",
		"operation", changeEvent.OperationType,
		"database", changeEvent.Database,
		"collection", changeEvent.Collection,
		"document_key", changeEvent.DocumentKey)

	err = t.storeAndPublish(changeEvent, msg)
	if err != nil {
		t.logger.Error("storeAndPublish failed",
			"operation", changeEvent.OperationType,
			"document_key", changeEvent.DocumentKey,
			"error", err)
	}
	return err
}

// buildPayload creates the event payload based on transport mode.
// Returns nil payload if the event should be skipped.
func (t *Transport) buildPayload(doc changeStreamDoc, event ChangeEvent) ([]byte, string, error) {
	if t.fullDocumentOnly {
		return t.buildFullDocumentPayload(doc, event)
	}
	// Send the entire ChangeEvent as JSON
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, "", err
	}
	return payload, "application/json", nil
}

// buildFullDocumentPayload creates a BSON payload from the raw fullDocument.
func (t *Transport) buildFullDocumentPayload(doc changeStreamDoc, event ChangeEvent) ([]byte, string, error) {
	contentType := "application/bson"

	// Send the raw fullDocument BSON - preserves all MongoDB types
	if len(doc.FullDocument) > 0 {
		// FullDocument is already bson.Raw, just return it directly
		return []byte(doc.FullDocument), contentType, nil
	}

	// For delete events, send just the document ID as a string
	if event.OperationType == OperationDelete && event.DocumentKey != "" {
		return []byte(event.DocumentKey), "text/plain", nil
	}

	// Skip events without fullDocument and without documentKey
	t.logger.Debug("skipping event without fullDocument",
		"operation", event.OperationType,
		"document_key", event.DocumentKey)
	return nil, "", nil
}

// buildMetadata creates the metadata map for an event message.
func (t *Transport) buildMetadata(event ChangeEvent, contentType string) map[string]string {
	metadata := map[string]string{
		MetadataContentType: contentType,
		MetadataOperation:   string(event.OperationType),
		MetadataDatabase:    event.Database,
		MetadataCollection:  event.Collection,
		MetadataNamespace:   event.Namespace,
		MetadataDocumentKey: event.DocumentKey,
		MetadataClusterTime: event.Timestamp.Format(time.RFC3339Nano),
	}

	// Include update description fields when enabled
	if t.includeUpdateDescription && event.UpdateDesc != nil {
		if event.UpdateDesc.RemovedFields != nil {
			if data, err := json.Marshal(event.UpdateDesc.RemovedFields); err == nil {
				metadata[MetadataRemovedFields] = string(data)
			}
		}
		if event.UpdateDesc.UpdatedFields != nil {
			if data, err := json.Marshal(event.UpdateDesc.UpdatedFields); err == nil {
				if t.maxUpdatedFieldsSize <= 0 || len(data) <= t.maxUpdatedFieldsSize {
					metadata[MetadataUpdatedFields] = string(data)
				} else {
					t.logger.Debug("updated fields exceeds max size, omitting from metadata",
						"size", len(data),
						"max", t.maxUpdatedFieldsSize,
						"document_key", event.DocumentKey)
				}
			}
		}
	}

	return metadata
}

// buildMessage creates a transport message with an ack callback.
func (t *Transport) buildMessage(event ChangeEvent, payload []byte, metadata map[string]string) transport.Message {
	return transport.NewMessageWithAck(
		event.ID,
		"mongodb://"+event.Database+"/"+event.Collection,
		payload,
		metadata,
		0,
		func(err error) error {
			if err == nil && t.ackStore != nil {
				ackCtx, cancel := context.WithTimeout(context.Background(), detachedTimeout)
				defer cancel()
				return t.ackStore.Ack(ackCtx, event.ID)
			}
			return nil
		},
	)
}

// storeAndPublish records the event in the ack store (if configured) and
// publishes it to all registered event subscribers.
func (t *Transport) storeAndPublish(event ChangeEvent, msg transport.Message) error {
	// Store as pending if ack store configured.
	// If the store fails, skip publishing to maintain at-least-once guarantee.
	if t.ackStore != nil {
		storeCtx, storeCancel := context.WithTimeout(context.Background(), detachedTimeout)
		err := t.ackStore.Store(storeCtx, event.ID)
		storeCancel()
		if err != nil {
			return fmt.Errorf("store pending event %s: %w", event.ID, err)
		}
	}

	// Publish to all registered events via channel transport.
	// Use a detached context so publish succeeds even during shutdown.
	publishCtx, publishCancel := context.WithTimeout(context.Background(), detachedTimeout)
	defer publishCancel()

	var publishErr error
	var publishCount int
	t.registeredEvents.Range(func(key, value any) bool {
		eventName, ok := key.(string)
		if !ok {
			return true
		}
		publishCount++
		if err := t.channelTransport.Publish(publishCtx, eventName, msg); err != nil {
			t.logger.Warn("failed to publish to channel transport", "event", eventName, "error", err)
			publishErr = errors.Join(publishErr, fmt.Errorf("publish to %s: %w", eventName, err))
		} else {
			t.logger.Debug("published to channel transport", "event", eventName, "msg_id", msg.ID())
		}
		return true
	})

	if publishCount == 0 {
		t.logger.Warn("storeAndPublish: no registered events to publish to")
	}

	return publishErr
}

// extractChangeEvent extracts a ChangeEvent from the typed change stream document.
func (t *Transport) extractChangeEvent(doc changeStreamDoc) ChangeEvent {
	event := ChangeEvent{
		ID:            doc.ID.Data,
		OperationType: OperationType(doc.OperationType),
		Database:      doc.NS.DB,
		Collection:    doc.NS.Coll,
		Timestamp:     time.Unix(int64(doc.ClusterTime.T), 0),
	}

	// Generate ID if not present in resume token
	if event.ID == "" {
		event.ID = transport.NewID()
	}

	// Fall back to transport-level defaults for namespace
	if event.Database == "" && t.db != nil {
		event.Database = t.db.Name()
	}
	if event.Collection == "" && t.collectionName != "" {
		event.Collection = t.collectionName
	}
	event.Namespace = event.Database + "." + event.Collection

	// Extract document key (_id field)
	if id := bsonDLookup(doc.DocumentKey, "_id"); id != nil {
		event.DocumentKey = formatDocumentKey(id)
	}

	// Extract full document as JSON
	if len(doc.FullDocument) > 0 {
		var fullDoc bson.D
		if err := bson.Unmarshal(doc.FullDocument, &fullDoc); err == nil {
			if jsonData, err := bsonDToJSON(fullDoc); err == nil {
				event.FullDocument = jsonData
			}
		}
	}

	// Extract update description
	if doc.UpdateDesc != nil {
		event.UpdateDesc = &UpdateDescription{
			UpdatedFields: bsonDToMap(doc.UpdateDesc.UpdatedFields),
			RemovedFields: doc.UpdateDesc.RemovedFields,
		}
		for _, ta := range doc.UpdateDesc.TruncatedArrays {
			event.UpdateDesc.TruncatedArrays = append(event.UpdateDesc.TruncatedArrays, ta.Field)
		}
	}

	return event
}

// bsonDLookup returns the value for a key in a bson.D, or nil if not found.
// Still needed for DocumentKey extraction since _id can be any type.
func bsonDLookup(d bson.D, key string) any {
	for _, elem := range d {
		if elem.Key == key {
			return elem.Value
		}
	}
	return nil
}

// bsonDToMap converts bson.D to a map for easier access.
// Nested bson.D values are also converted recursively.
// BSON types (ObjectID, DateTime, etc.) are converted to JSON-friendly formats.
func bsonDToMap(d bson.D) map[string]any {
	if d == nil {
		return nil
	}
	m := make(map[string]any, len(d))
	for _, elem := range d {
		m[elem.Key] = convertBSONTypes(elem.Value)
	}
	return m
}

// formatDocumentKey converts any MongoDB _id type to a string representation.
func formatDocumentKey(id any) string {
	if id == nil {
		return ""
	}
	switch v := id.(type) {
	case bson.ObjectID:
		return v.Hex()
	case string:
		return v
	case bson.Binary:
		// UUID or other binary types
		return fmt.Sprintf("%x", v.Data)
	case int, int32, int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	default:
		// Fallback: try to convert via JSON
		if data, err := json.Marshal(v); err == nil {
			return string(data)
		}
		return fmt.Sprintf("%v", v)
	}
}

// bsonDToJSON converts a BSON document (bson.D) to JSON, handling MongoDB-specific types.
func bsonDToJSON(doc bson.D) (json.RawMessage, error) {
	// Convert MongoDB types to JSON-friendly types
	converted := convertBSONTypes(doc)
	return json.Marshal(converted)
}

// convertBSONTypes recursively converts BSON-specific types to JSON-friendly types.
// Uses MongoDB Extended JSON format for special types so they can be unmarshaled
// back into their original Go types (e.g., bson.ObjectID).
func convertBSONTypes(v any) any {
	switch val := v.(type) {
	case bson.D:
		result := make(map[string]any, len(val))
		for _, elem := range val {
			result[elem.Key] = convertBSONTypes(elem.Value)
		}
		return result
	case bson.M:
		result := make(map[string]any, len(val))
		for k, v := range val {
			result[k] = convertBSONTypes(v)
		}
		return result
	case bson.A:
		result := make([]any, len(val))
		for i, v := range val {
			result[i] = convertBSONTypes(v)
		}
		return result
	case bson.ObjectID:
		// Use Extended JSON format so bson.ObjectID can unmarshal it
		return map[string]string{"$oid": val.Hex()}
	case bson.DateTime:
		// Use ISO string format - compatible with Go's time.Time JSON unmarshal
		return time.Unix(int64(val)/1000, (int64(val)%1000)*1000000).Format(time.RFC3339Nano)
	case bson.Timestamp:
		// Use ISO string format for timestamps too
		return time.Unix(int64(val.T), 0).Format(time.RFC3339Nano)
	case bson.Binary:
		return map[string]any{"$binary": map[string]any{"base64": val.Data, "subType": fmt.Sprintf("%02x", val.Subtype)}}
	case bson.Decimal128:
		return val.String()
	default:
		return val
	}
}

// isEmptyUpdate returns true if the event is an update operation
// with no updated or removed fields in the UpdateDescription.
// Replace operations are not considered empty updates since they
// replace the entire document and don't produce UpdateDescription.
func isEmptyUpdate(e ChangeEvent) bool {
	if e.OperationType != OperationUpdate {
		return false
	}
	if e.UpdateDesc == nil {
		return true
	}
	return len(e.UpdateDesc.UpdatedFields) == 0 && len(e.UpdateDesc.RemovedFields) == 0
}

// SupportsRedelivery returns false because MongoDB Change Streams are
// broadcast-only with no built-in re-delivery of unacknowledged messages.
func (t *Transport) SupportsRedelivery() bool { return false }

// Compile-time checks
var (
	_ transport.Transport     = (*Transport)(nil)
	_ transport.HealthChecker = (*Transport)(nil)
	_ transport.Redeliverable = (*Transport)(nil)
)
