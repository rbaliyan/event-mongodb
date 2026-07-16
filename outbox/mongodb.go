package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	event "github.com/rbaliyan/event/v3"
	evtoutbox "github.com/rbaliyan/event/v3/outbox"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Compile-time checks that MongoStore implements the backend-neutral outbox
// contract from github.com/rbaliyan/event/v3/outbox, plus event.OutboxStore
// for bus-level integration (Store has the identical signature).
var (
	_ event.OutboxStore        = (*MongoStore)(nil)
	_ evtoutbox.Store          = (*MongoStore)(nil)
	_ evtoutbox.StuckRecoverer = (*MongoStore)(nil)
	_ evtoutbox.Starter        = (*MongoStore)(nil)
	_ evtoutbox.Waker          = (*MongoStore)(nil)
	_ evtoutbox.Batch          = (*mongoBatch)(nil)
)

// isNamespaceNotFoundError checks if the error is a MongoDB namespace not found error.
// This occurs when querying collection stats for a non-existent collection.
func isNamespaceNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "ns not found") ||
		strings.Contains(errStr, "NamespaceNotFound") ||
		strings.Contains(errStr, "Collection") && strings.Contains(errStr, "not found")
}

/*
MongoDB Schema:

Collection: event_outbox

Document structure:
{
    "_id": ObjectId,
    "event_name": string,
    "event_id": string,
    "payload": Binary,
    "metadata": object,
    "created_at": ISODate,
    "published_at": ISODate (optional),
    "status": string,
    "retry_count": int,
    "last_error": string (optional)
}

Indexes:
- { "status": 1, "created_at": 1 } for pending message queries
- { "published_at": 1 } for cleanup operations

Create indexes with:
db.event_outbox.createIndex({ "status": 1, "created_at": 1 })
db.event_outbox.createIndex({ "published_at": 1 }, { sparse: true })
*/

// mongoMessage represents a message document in MongoDB
type mongoMessage struct {
	ID          bson.ObjectID     `bson:"_id,omitempty"`
	EventName   string            `bson:"event_name"`
	EventID     string            `bson:"event_id"`
	Payload     []byte            `bson:"payload"`
	Metadata    map[string]string `bson:"metadata,omitempty"`
	CreatedAt   time.Time         `bson:"created_at"`
	ClaimedAt   *time.Time        `bson:"claimed_at,omitempty"`
	ClaimedBy   string            `bson:"claimed_by,omitempty"`
	PublishedAt *time.Time        `bson:"published_at,omitempty"`
	Status      evtoutbox.Status  `bson:"status"`
	RetryCount  int               `bson:"retry_count"`
	LastError   string            `bson:"last_error,omitempty"`
	Priority    int               `bson:"priority"`
}

// toClaimedMessage converts a mongoMessage to the backend-neutral
// evtoutbox.Message, stashing the ObjectID as the claim token via
// evtoutbox.NewClaimedMessage (Message has no public id field; the token is
// how mongoBatch.Ack/Fail resolve back to this document).
func (m *mongoMessage) toClaimedMessage() evtoutbox.Message {
	msg := evtoutbox.Message{
		EventName:   m.EventName,
		EventID:     m.EventID,
		Payload:     m.Payload,
		Metadata:    m.Metadata,
		CreatedAt:   m.CreatedAt,
		PublishedAt: m.PublishedAt,
		Status:      m.Status,
		RetryCount:  m.RetryCount,
		LastError:   m.LastError,
		Priority:    m.Priority,
	}
	return evtoutbox.NewClaimedMessage(msg, m.ID)
}

// cappedInfo contains information about a capped collection
type cappedInfo struct {
	Capped   bool  // Whether the collection is capped
	Size     int64 // Maximum size in bytes
	MaxDocs  int64 // Maximum number of documents (0 = unlimited)
	StorSize int64 // Current storage size in bytes
	Count    int64 // Current document count
}

// MongoStoreOption configures a MongoStore.
type MongoStoreOption func(*mongoStoreOptions)

type mongoStoreOptions struct {
	collection string
}

// WithCollection sets a custom collection name for the MongoDB outbox store.
func WithCollection(name string) MongoStoreOption {
	return func(o *mongoStoreOptions) {
		if name != "" {
			o.collection = name
		}
	}
}

// MongoStore defines the interface for MongoDB outbox storage
type MongoStore struct {
	collection *mongo.Collection

	cappedMu   sync.Mutex
	cappedInfo *cappedInfo // Cached capped info (nil = not checked yet), guarded by cappedMu
}

// NewMongoStore creates a new MongoDB outbox store.
func NewMongoStore(db *mongo.Database, opts ...MongoStoreOption) (*MongoStore, error) {
	if db == nil {
		return nil, errors.New("mongodb: database is required")
	}

	o := &mongoStoreOptions{
		collection: "event_outbox",
	}
	for _, opt := range opts {
		opt(o)
	}

	// Index creation is not performed here: the constructor does no I/O. The
	// generic evtoutbox.Relay calls EnsureReady (which wraps EnsureIndexes)
	// once at the start of its Start() loop. When using the store without a
	// relay, call EnsureIndexes (or EnsureReady) once during startup.
	return &MongoStore{
		collection: db.Collection(o.collection),
	}, nil
}

// Collection returns the underlying MongoDB collection
func (s *MongoStore) Collection() *mongo.Collection {
	return s.collection
}

// Indexes returns the required indexes for the outbox collection.
// Users can use this to create indexes manually or merge with their own indexes.
//
// Example:
//
//	indexes := store.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoStore) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		// Primary relay query: claim next pending/failed message ordered by priority then age.
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "priority", Value: -1},
				{Key: "created_at", Value: 1},
			},
		},
		// RecoverStuck: find processing messages whose claimed_at has expired.
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "claimed_at", Value: 1},
			},
		},
		// MarkPublishedByEventID / MarkFailedByEventID: update by application event ID.
		{
			Keys: bson.D{{Key: "event_id", Value: 1}},
		},
		// Delete: cleanup published messages older than a cutoff.
		// Sparse so the index only contains documents where published_at is set.
		{
			Keys:    bson.D{{Key: "published_at", Value: 1}},
			Options: options.Index().SetSparse(true),
		},
	}
}

// EnsureIndexes creates the required indexes for the outbox collection
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// EnsureReady implements evtoutbox.Starter: the generic relay calls this once
// at startup, before the first ClaimPending, so the required indexes exist.
// Wraps EnsureIndexes.
func (s *MongoStore) EnsureReady(ctx context.Context) error {
	return s.EnsureIndexes(ctx)
}

// Notifications implements evtoutbox.Waker. It currently always returns nil
// (poll-only): a real-time wakeup backed by a MongoDB change stream was
// deferred because it could not be exercised against a live deployment in
// this change (see the outbox package doc for the migration note). A nil
// channel is safe for the relay engine — the corresponding select case blocks
// forever, leaving the poll ticker as the sole wakeup source, identical to
// the store's prior polling behavior.
func (s *MongoStore) Notifications() <-chan struct{} {
	return nil
}

// IsCapped returns whether the collection is a capped collection.
// The result is cached after the first call.
func (s *MongoStore) IsCapped(ctx context.Context) (bool, error) {
	info, err := s.getCappedInfo(ctx)
	if err != nil {
		return false, err
	}
	return info.Capped, nil
}

// getCappedInfo returns detailed information about the collection's capped status.
// The result is cached after the first call.
func (s *MongoStore) getCappedInfo(ctx context.Context) (*cappedInfo, error) {
	s.cappedMu.Lock()
	defer s.cappedMu.Unlock()

	if s.cappedInfo != nil {
		return s.cappedInfo, nil
	}

	info, err := s.fetchCappedInfo(ctx)
	if err != nil {
		return nil, err
	}

	s.cappedInfo = info
	return info, nil
}

// fetchCappedInfo queries MongoDB for collection stats to determine if capped.
func (s *MongoStore) fetchCappedInfo(ctx context.Context) (*cappedInfo, error) {
	var result bson.M
	err := s.collection.Database().RunCommand(ctx, bson.D{
		{Key: "collStats", Value: s.collection.Name()},
	}).Decode(&result)

	if err != nil {
		// Collection might not exist yet - treat as non-capped
		// MongoDB returns "ns not found" or similar for missing collections
		if isNamespaceNotFoundError(err) {
			return &cappedInfo{Capped: false}, nil
		}
		return nil, fmt.Errorf("collStats: %w", err)
	}

	info := &cappedInfo{}

	if capped, ok := result["capped"].(bool); ok {
		info.Capped = capped
	}
	if size, ok := result["maxSize"].(int64); ok {
		info.Size = size
	} else if size, ok := result["maxSize"].(int32); ok {
		info.Size = int64(size)
	}
	if maxDocs, ok := result["max"].(int64); ok {
		info.MaxDocs = maxDocs
	} else if maxDocs, ok := result["max"].(int32); ok {
		info.MaxDocs = int64(maxDocs)
	}
	if storSize, ok := result["storageSize"].(int64); ok {
		info.StorSize = storSize
	} else if storSize, ok := result["storageSize"].(int32); ok {
		info.StorSize = int64(storSize)
	}
	if count, ok := result["count"].(int64); ok {
		info.Count = count
	} else if count, ok := result["count"].(int32); ok {
		info.Count = int64(count)
	}

	return info, nil
}

// CreateCapped creates the collection as a capped collection.
// This must be called before any documents are inserted.
// Returns an error if the collection already exists.
//
// Parameters:
//   - sizeBytes: Maximum size of the collection in bytes (required, minimum 4096)
//   - maxDocs: Maximum number of documents (0 = no limit, only size matters)
//
// Example:
//
//	// Create 100MB capped collection
//	err := store.CreateCapped(ctx, 100*1024*1024, 0)
//
//	// Create capped collection with max 10000 documents
//	err := store.CreateCapped(ctx, 100*1024*1024, 10000)
func (s *MongoStore) CreateCapped(ctx context.Context, sizeBytes int64, maxDocs int64) error {
	opts := options.CreateCollection().SetCapped(true).SetSizeInBytes(sizeBytes)
	if maxDocs > 0 {
		opts.SetMaxDocuments(maxDocs)
	}

	err := s.collection.Database().CreateCollection(ctx, s.collection.Name(), opts)
	if err != nil {
		return fmt.Errorf("create capped collection: %w", err)
	}

	// Refresh cached info
	s.cappedMu.Lock()
	s.cappedInfo = nil
	s.cappedMu.Unlock()

	return nil
}

// InsertInSession adds a message to the outbox within a MongoDB session/transaction.
// In MongoDB driver v2, pass the session context directly as ctx.
func (s *MongoStore) InsertInSession(ctx context.Context, msg *mongoMessage) error {
	if msg.ID.IsZero() {
		msg.ID = bson.NewObjectID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	msg.Status = evtoutbox.StatusPending

	_, err := s.collection.InsertOne(ctx, msg)
	return err
}

// Insert adds a message to the outbox (without transaction)
func (s *MongoStore) Insert(ctx context.Context, msg *mongoMessage) error {
	if msg.ID.IsZero() {
		msg.ID = bson.NewObjectID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	msg.Status = evtoutbox.StatusPending

	_, err := s.collection.InsertOne(ctx, msg)
	return err
}

// Store implements event.OutboxStore interface.
// It stores a message in the outbox within the active transaction.
// The transaction session is extracted from the context using event.OutboxTx().
//
// If the context contains a mongo.SessionContext (via event.WithOutboxTx),
// the message is stored within that session's transaction.
// Otherwise, it stores without a transaction (for testing or non-transactional use).
//
// Example:
//
//	sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
//	    ctx := event.WithOutboxTx(ctx, ctx)
//	    // ... business logic ...
//	    return nil, orderEvent.Publish(ctx, order)  // Routed to Store()
//	})
func (s *MongoStore) Store(ctx context.Context, eventName string, eventID string, payload []byte, metadata map[string]string) error {
	msg := &mongoMessage{
		ID:        bson.NewObjectID(),
		EventName: eventName,
		EventID:   eventID,
		Payload:   payload,
		Metadata:  metadata,
		CreatedAt: time.Now(),
		Status:    evtoutbox.StatusPending,
	}

	// Check if we're inside a transaction (context contains session in v2)
	if session := event.OutboxTx(ctx); session != nil {
		if txCtx, ok := session.(context.Context); ok {
			_, err := s.collection.InsertOne(txCtx, msg)
			return err
		}
	}

	// Fallback to non-transactional insert
	_, err := s.collection.InsertOne(ctx, msg)
	return err
}

// ClaimPending implements evtoutbox.Store. It atomically claims up to limit
// pending/failed messages and returns them as a mongoBatch, which resolves
// each message's Ack/Fail back to its document via the ObjectID token stashed
// by toClaimedMessage (see evtoutbox.NewClaimedMessage / evtoutbox.Token).
func (s *MongoStore) ClaimPending(ctx context.Context, limit int) (evtoutbox.Batch, error) {
	msgs, err := s.claimBatch(ctx, limit)
	if err != nil {
		return nil, err
	}
	claimed := make([]evtoutbox.Message, len(msgs))
	for i, m := range msgs {
		claimed[i] = m.toClaimedMessage()
	}
	return &mongoBatch{store: s, msgs: claimed}, nil
}

// claimBatch atomically claims up to limit pending/failed messages in 3 round-trips
// instead of limit round-trips, regardless of batch size.
//
// Algorithm:
//  1. Find up to limit candidate IDs (read, uses {status,priority,created_at} index)
//  2. UpdateMany on those IDs with a unique claim token (write, atomic per-doc status check)
//  3. Fetch the messages we won — IDs we lost to a concurrent relay are excluded by token
func (s *MongoStore) claimBatch(ctx context.Context, limit int) ([]*mongoMessage, error) {
	if limit <= 0 {
		return nil, nil
	}

	pendingFilter := bson.M{"status": bson.M{"$in": []evtoutbox.Status{evtoutbox.StatusPending, evtoutbox.StatusFailed}}}

	// Step 1: collect candidate IDs ordered by priority then age.
	candidateCursor, err := s.collection.Find(ctx, pendingFilter,
		options.Find().
			SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "created_at", Value: 1}}).
			SetLimit(int64(limit)).
			SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("find candidates: %w", err)
	}
	defer func() { _ = candidateCursor.Close(ctx) }()

	var ids []bson.ObjectID
	for candidateCursor.Next(ctx) {
		var doc struct {
			ID bson.ObjectID `bson:"_id"`
		}
		if err := candidateCursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode candidate: %w", err)
		}
		ids = append(ids, doc.ID)
	}
	if err := candidateCursor.Err(); err != nil {
		return nil, fmt.Errorf("candidates cursor: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// Step 2: atomically claim. The status check in the filter means any ID
	// already claimed by a concurrent relay instance is silently skipped.
	claimToken := uuid.New().String()
	now := time.Now()
	_, err = s.collection.UpdateMany(ctx,
		bson.M{
			"_id":    bson.M{"$in": ids},
			"status": bson.M{"$in": []evtoutbox.Status{evtoutbox.StatusPending, evtoutbox.StatusFailed}},
		},
		bson.M{"$set": bson.M{
			"status":     evtoutbox.StatusProcessing,
			"claimed_at": now,
			"claimed_by": claimToken,
		}},
	)
	if err != nil {
		return nil, fmt.Errorf("claim batch: %w", err)
	}

	// Step 3: fetch exactly the messages we claimed.
	// Primary key lookup on ids (efficient) + claimed_by filter to drop any
	// that a concurrent relay won the race on between steps 1 and 2. Re-apply
	// the priority/age sort: $in does not preserve the step-1 ordering, and
	// callers rely on highest-priority-then-oldest delivery order.
	fetchCursor, err := s.collection.Find(ctx, bson.M{
		"_id":        bson.M{"$in": ids},
		"claimed_by": claimToken,
	}, options.Find().SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "created_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("fetch claimed: %w", err)
	}
	defer func() { _ = fetchCursor.Close(ctx) }()

	var messages []*mongoMessage
	for fetchCursor.Next(ctx) {
		var msg mongoMessage
		if err := fetchCursor.Decode(&msg); err != nil {
			return nil, fmt.Errorf("decode message: %w", err)
		}
		messages = append(messages, &msg)
	}
	return messages, fetchCursor.Err()
}

// MarkPublishedByEventID marks a message as published using the event ID
func (s *MongoStore) MarkPublishedByEventID(ctx context.Context, eventID string) error {
	now := time.Now()
	filter := bson.M{"event_id": eventID}
	update := bson.M{
		"$set": bson.M{
			"status":       evtoutbox.StatusPublished,
			"published_at": now,
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", eventID)
	}

	return nil
}

// MarkFailedByEventID marks a message as failed using the event ID
func (s *MongoStore) MarkFailedByEventID(ctx context.Context, eventID string, err error) error {
	filter := bson.M{"event_id": eventID}
	update := bson.M{
		"$set": bson.M{
			"status":     evtoutbox.StatusFailed,
			"last_error": err.Error(),
		},
		"$inc": bson.M{
			"retry_count": 1,
		},
	}

	result, updateErr := s.collection.UpdateOne(ctx, filter, update)
	if updateErr != nil {
		return fmt.Errorf("update: %w", updateErr)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", eventID)
	}

	return nil
}

// Cleanup implements evtoutbox.Store. It removes old published messages.
// For capped collections, this is a no-op since MongoDB handles cleanup automatically.
// Returns 0 for capped collections without error.
func (s *MongoStore) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return 0, fmt.Errorf("check capped: %w", err)
	}
	if capped {
		// Capped collections auto-cleanup, deletion not needed
		return 0, nil
	}

	cutoff := time.Now().Add(-olderThan)
	filter := bson.M{
		"status":       evtoutbox.StatusPublished,
		"published_at": bson.M{"$lt": cutoff},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// RetryFailed moves failed messages back to pending status
func (s *MongoStore) RetryFailed(ctx context.Context, maxRetries int) (int64, error) {
	filter := bson.M{
		"status":      evtoutbox.StatusFailed,
		"retry_count": bson.M{"$lt": maxRetries},
	}
	update := bson.M{
		"$set":   bson.M{"status": evtoutbox.StatusPending},
		"$unset": bson.M{"claimed_by": ""},
	}

	result, err := s.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, fmt.Errorf("update: %w", err)
	}

	return result.ModifiedCount, nil
}

// Count returns the count of messages by status
func (s *MongoStore) Count(ctx context.Context, status evtoutbox.Status) (int64, error) {
	filter := bson.M{"status": status}
	return s.collection.CountDocuments(ctx, filter)
}

// RecoverStuck moves messages stuck in "processing" status back to pending.
// This handles relay crashes where a message was claimed but never published.
// Should be called periodically (e.g., every minute) or at relay startup.
//
// Parameters:
//   - ctx: Context for cancellation
//   - stuckDuration: How long a message must be in processing to be considered stuck
//
// Returns the number of recovered messages.
func (s *MongoStore) RecoverStuck(ctx context.Context, stuckDuration time.Duration) (int64, error) {
	cutoff := time.Now().Add(-stuckDuration)
	filter := bson.M{
		"status":     evtoutbox.StatusProcessing,
		"claimed_at": bson.M{"$lt": cutoff},
	}
	update := bson.M{
		"$set":   bson.M{"status": evtoutbox.StatusPending},
		"$unset": bson.M{"claimed_at": "", "claimed_by": ""},
	}

	result, err := s.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, fmt.Errorf("recover stuck: %w", err)
	}

	return result.ModifiedCount, nil
}

// mongoBatch implements evtoutbox.Batch over a set of claimed messages.
// Ack marks a message published; Fail marks it failed and increments its
// retry count. Both resolve the target document via the ObjectID stashed in
// the message's token (evtoutbox.Token) rather than a public Message.ID,
// since evtoutbox.Message carries no exported identifier. Close is a no-op:
// unlike Postgres (which holds a claim tx open until Close), MongoDB's claim
// (claimBatch's UpdateMany) already committed by the time ClaimPending
// returns, so there is no resource to release here.
type mongoBatch struct {
	store *MongoStore
	msgs  []evtoutbox.Message
}

func (b *mongoBatch) Messages() []evtoutbox.Message { return b.msgs }

func (b *mongoBatch) Ack(ctx context.Context, msg evtoutbox.Message) error {
	id, ok := evtoutbox.Token(msg).(bson.ObjectID)
	if !ok {
		return fmt.Errorf("mongodb outbox: expected bson.ObjectID token, got %T", evtoutbox.Token(msg))
	}
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":       evtoutbox.StatusPublished,
			"published_at": now,
		},
	}
	result, err := b.store.collection.UpdateByID(ctx, id, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", id.Hex())
	}
	return nil
}

func (b *mongoBatch) Fail(ctx context.Context, msg evtoutbox.Message, cause error) error {
	id, ok := evtoutbox.Token(msg).(bson.ObjectID)
	if !ok {
		return fmt.Errorf("mongodb outbox: expected bson.ObjectID token, got %T", evtoutbox.Token(msg))
	}
	var lastError string
	if cause != nil {
		lastError = cause.Error()
	}
	update := bson.M{
		"$set": bson.M{
			"status":     evtoutbox.StatusFailed,
			"last_error": lastError,
		},
		"$inc": bson.M{
			"retry_count": 1,
		},
	}
	result, err := b.store.collection.UpdateByID(ctx, id, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", id.Hex())
	}
	return nil
}

func (b *mongoBatch) Close(context.Context) error { return nil }

// Transaction executes the given function within a MongoDB transaction.
// The context passed to fn contains the transaction session via event.WithOutboxTx,
// so any event.Publish() calls within fn will automatically route to the outbox.
//
// If fn returns an error, the transaction is rolled back.
// If fn returns nil, the transaction is committed.
//
// Example:
//
//	store := outbox.NewMongoStore(db)
//	bus, _ := event.NewBus("mybus", event.WithTransport(t), event.WithOutbox(store))
//	orderEvent := event.New[Order]("order.created")
//	event.Register(ctx, bus, orderEvent)
//
//	err := outbox.Transaction(ctx, mongoClient, func(ctx context.Context) error {
//	    // Business logic - uses the transaction context
//	    _, err := ordersCol.InsertOne(ctx, order)
//	    if err != nil {
//	        return err
//	    }
//
//	    // This automatically goes to the outbox (same transaction)
//	    return orderEvent.Publish(ctx, order)
//	})
func Transaction(ctx context.Context, client *mongo.Client, fn func(ctx context.Context) error) error {
	// Piggy-back only if the existing transaction is a Mongo session (context.Context).
	// Other session types (e.g., *sql.Tx) are ignored to prevent
	// cross-store type confusion that could silently break atomicity.
	if session := event.OutboxTx(ctx); session != nil {
		if _, ok := session.(context.Context); ok {
			return fn(ctx)
		}
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		// Wrap the session context with outbox transaction marker
		txCtx := event.WithOutboxTx(ctx, ctx)
		return nil, fn(txCtx)
	})

	return err
}

// TransactionWithOptions executes the given function within a MongoDB transaction
// with custom transaction options.
//
// Example:
//
//	opts := options.Transaction().SetReadConcern(readconcern.Snapshot())
//	err := outbox.TransactionWithOptions(ctx, mongoClient, opts, func(ctx context.Context) error {
//	    // Business logic...
//	    return orderEvent.Publish(ctx, order)
//	})
func TransactionWithOptions(ctx context.Context, client *mongo.Client, opts *options.TransactionOptionsBuilder, fn func(ctx context.Context) error) error {
	// Piggy-back only if the existing transaction is a Mongo session (context.Context).
	// Other session types (e.g., *sql.Tx) are ignored to prevent
	// cross-store type confusion that could silently break atomicity.
	if session := event.OutboxTx(ctx); session != nil {
		if _, ok := session.(context.Context); ok {
			return fn(ctx)
		}
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	callback := func(ctx context.Context) (any, error) {
		// Wrap the session context with outbox transaction marker
		txCtx := event.WithOutboxTx(ctx, ctx)
		return nil, fn(txCtx)
	}

	if opts != nil {
		_, err = sess.WithTransaction(ctx, callback, opts)
	} else {
		_, err = sess.WithTransaction(ctx, callback)
	}

	return err
}
