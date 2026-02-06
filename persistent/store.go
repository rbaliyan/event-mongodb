package persistent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/transport/persistent"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Errors returned by the store.
var (
	// ErrCollectionRequired is returned when a nil collection is passed to NewStore.
	ErrCollectionRequired = errors.New("mongodb collection is required")

	// ErrMessageNotFound is returned when Ack or Nack is called for a non-existent message.
	ErrMessageNotFound = errors.New("message not found")

	// ErrInvalidSequenceID is returned when an invalid sequence ID is provided.
	ErrInvalidSequenceID = errors.New("invalid sequence ID")
)

// Message status constants
const (
	statusPending  = "pending"
	statusInflight = "inflight"
	statusAcked    = "acked"
)

// Default configuration values
const (
	defaultVisibilityTimeout = 5 * time.Minute
)

// storedMessage represents the MongoDB document structure for messages.
type storedMessage struct {
	ID         bson.ObjectID `bson:"_id"`
	EventName  string        `bson:"event_name"`
	Data       []byte        `bson:"data"`
	Status     string        `bson:"status"`
	Timestamp  time.Time     `bson:"timestamp"`
	RetryCount int           `bson:"retry_count"`
	InflightAt time.Time     `bson:"inflight_at,omitempty"`
	AckedAt    time.Time     `bson:"acked_at,omitempty"`
}

// Store implements persistent.Store using MongoDB.
// Messages are stored in a collection and delivered with at-least-once semantics.
//
// Document structure:
//
//	{
//	    "_id": ObjectId("..."),
//	    "event_name": "orders",
//	    "data": Binary(...),
//	    "status": "pending|inflight|acked",
//	    "timestamp": ISODate("..."),
//	    "retry_count": 0,
//	    "inflight_at": ISODate("..."),  // When message was claimed
//	    "acked_at": ISODate("...")       // When message was acknowledged
//	}
type Store struct {
	collection        *mongo.Collection
	ttl               time.Duration
	visibilityTimeout time.Duration
}

// StoreOption configures the MongoDB store.
type StoreOption func(*Store)

// WithTTL sets the TTL for acknowledged messages.
// MongoDB will automatically delete acked messages after this duration.
// Default is 0 (no automatic deletion).
func WithTTL(ttl time.Duration) StoreOption {
	return func(s *Store) {
		if ttl > 0 {
			s.ttl = ttl
		}
	}
}

// WithVisibilityTimeout sets how long a message remains invisible after Fetch.
// If not Acked within this time, another Fetch can claim it.
// Default is 5 minutes.
func WithVisibilityTimeout(d time.Duration) StoreOption {
	return func(s *Store) {
		if d > 0 {
			s.visibilityTimeout = d
		}
	}
}

// NewStore creates a new MongoDB-backed persistent store.
//
// The collection will store event messages with the following indexes
// (call EnsureIndexes to create them):
//   - (event_name, status, timestamp) for efficient Fetch queries
//   - (acked_at) with TTL for automatic cleanup of processed messages
//
// Example:
//
//	store, err := persistent.NewStore(
//	    mongoClient.Database("events").Collection("messages"),
//	    persistent.WithTTL(7*24*time.Hour),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	store.EnsureIndexes(ctx)
func NewStore(collection *mongo.Collection, opts ...StoreOption) (*Store, error) {
	if collection == nil {
		return nil, ErrCollectionRequired
	}

	s := &Store{
		collection:        collection,
		visibilityTimeout: defaultVisibilityTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Append adds a message to the store for the given event.
// Returns a unique sequence ID (MongoDB ObjectID hex string) for the stored message.
func (s *Store) Append(ctx context.Context, eventName string, data []byte) (string, error) {
	id := bson.NewObjectID()
	doc := storedMessage{
		ID:        id,
		EventName: eventName,
		Data:      data,
		Status:    statusPending,
		Timestamp: time.Now(),
	}

	_, err := s.collection.InsertOne(ctx, doc)
	if err != nil {
		return "", fmt.Errorf("append message: %w", err)
	}

	return id.Hex(), nil
}

// Fetch retrieves the next unprocessed message after the given checkpoint.
// Returns nil if no messages are available.
//
// The message is atomically marked as "inflight" to prevent other consumers
// from claiming it. If not Acked within the visibility timeout, the message
// becomes available again.
//
// Checkpoint is the last processed sequence ID (ObjectID hex string).
// Pass empty string to fetch from the beginning.
func (s *Store) Fetch(ctx context.Context, eventName string, checkpoint string) (*persistent.StoredMessage, error) {
	now := time.Now()
	visibilityThreshold := now.Add(-s.visibilityTimeout)

	// Build filter: pending messages OR stale inflight messages
	// After checkpoint (if provided)
	filter := bson.M{
		"event_name": eventName,
		"$or": []bson.M{
			{"status": statusPending},
			{
				"status":      statusInflight,
				"inflight_at": bson.M{"$lt": visibilityThreshold},
			},
		},
	}

	// Apply checkpoint filter if provided
	if checkpoint != "" {
		oid, err := bson.ObjectIDFromHex(checkpoint)
		if err != nil {
			return nil, fmt.Errorf("invalid checkpoint: %w", err)
		}
		filter["_id"] = bson.M{"$gt": oid}
	}

	// Atomically claim the message
	update := bson.M{
		"$set": bson.M{
			"status":      statusInflight,
			"inflight_at": now,
		},
	}

	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "timestamp", Value: 1}, {Key: "_id", Value: 1}}).
		SetReturnDocument(options.After)

	var doc storedMessage
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil // No messages available
		}
		return nil, fmt.Errorf("fetch message: %w", err)
	}

	return &persistent.StoredMessage{
		SequenceID: doc.ID.Hex(),
		Data:       doc.Data,
		Timestamp:  doc.Timestamp,
		RetryCount: doc.RetryCount,
	}, nil
}

// Ack acknowledges a message as successfully processed.
// The message is marked as "acked" and will be automatically deleted
// after the TTL (if configured).
func (s *Store) Ack(ctx context.Context, eventName string, sequenceID string) error {
	oid, err := bson.ObjectIDFromHex(sequenceID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidSequenceID, err)
	}

	result, err := s.collection.UpdateOne(ctx,
		bson.M{"_id": oid, "event_name": eventName},
		bson.M{"$set": bson.M{
			"status":   statusAcked,
			"acked_at": time.Now(),
		}},
	)
	if err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("%w: %s", ErrMessageNotFound, sequenceID)
	}

	return nil
}

// Nack marks a message for redelivery (processing failed).
// The message is returned to "pending" status and its retry count is incremented.
func (s *Store) Nack(ctx context.Context, eventName string, sequenceID string) error {
	oid, err := bson.ObjectIDFromHex(sequenceID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidSequenceID, err)
	}

	result, err := s.collection.UpdateOne(ctx,
		bson.M{"_id": oid, "event_name": eventName},
		bson.M{
			"$set":   bson.M{"status": statusPending},
			"$inc":   bson.M{"retry_count": 1},
			"$unset": bson.M{"inflight_at": ""},
		},
	)
	if err != nil {
		return fmt.Errorf("nack message: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("%w: %s", ErrMessageNotFound, sequenceID)
	}

	return nil
}

// EnsureIndexes creates the required indexes for efficient queries.
// Call this once during application startup.
//
// Creates the following indexes:
//   - (event_name, status, timestamp) - for Fetch queries
//   - (event_name, _id) - for checkpoint-based queries
//   - (acked_at) with TTL - for automatic cleanup (if TTL configured)
func (s *Store) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "event_name", Value: 1},
				{Key: "status", Value: 1},
				{Key: "timestamp", Value: 1},
			},
			Options: options.Index().SetName("event_status_timestamp"),
		},
		{
			Keys: bson.D{
				{Key: "event_name", Value: 1},
				{Key: "_id", Value: 1},
			},
			Options: options.Index().SetName("event_id"),
		},
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "inflight_at", Value: 1},
			},
			Options: options.Index().SetName("inflight_visibility"),
		},
	}

	// Add TTL index for automatic cleanup if configured
	if s.ttl > 0 {
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{{Key: "acked_at", Value: 1}},
			Options: options.Index().
				SetName("acked_ttl").
				SetExpireAfterSeconds(int32(s.ttl.Seconds())).
				SetPartialFilterExpression(bson.M{"status": statusAcked}),
		})
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	return nil
}

// Indexes returns the index models for manual creation.
// Use this if you prefer to manage indexes separately (e.g., via migrations).
func (s *Store) Indexes() []mongo.IndexModel {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "event_name", Value: 1},
				{Key: "status", Value: 1},
				{Key: "timestamp", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "event_name", Value: 1},
				{Key: "_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "inflight_at", Value: 1},
			},
		},
	}

	if s.ttl > 0 {
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{{Key: "acked_at", Value: 1}},
			Options: options.Index().
				SetExpireAfterSeconds(int32(s.ttl.Seconds())).
				SetPartialFilterExpression(bson.M{"status": statusAcked}),
		})
	}

	return indexes
}

// Collection returns the underlying MongoDB collection for custom queries.
// Use this for monitoring, debugging, or custom cleanup operations.
func (s *Store) Collection() *mongo.Collection {
	return s.collection
}

// Stats returns statistics about the store for monitoring.
type Stats struct {
	Pending  int64 `json:"pending"`
	Inflight int64 `json:"inflight"`
	Acked    int64 `json:"acked"`
	Total    int64 `json:"total"`
}

// GetStats returns message counts by status for the given event.
// Pass empty eventName to get stats across all events.
func (s *Store) GetStats(ctx context.Context, eventName string) (*Stats, error) {
	filter := bson.M{}
	if eventName != "" {
		filter["event_name"] = eventName
	}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$group", Value: bson.M{
			"_id":   "$status",
			"count": bson.M{"$sum": 1},
		}}},
	}

	cursor, err := s.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate stats: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	stats := &Stats{}
	for cursor.Next(ctx) {
		var result struct {
			ID    string `bson:"_id"`
			Count int64  `bson:"count"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("decode stats result: %w", err)
		}
		switch result.ID {
		case statusPending:
			stats.Pending = result.Count
		case statusInflight:
			stats.Inflight = result.Count
		case statusAcked:
			stats.Acked = result.Count
		}
		stats.Total += result.Count
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate stats: %w", err)
	}

	return stats, nil
}

// Purge deletes all acked messages older than the given age.
// Use this for manual cleanup if TTL is not configured.
func (s *Store) Purge(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	result, err := s.collection.DeleteMany(ctx, bson.M{
		"status":   statusAcked,
		"acked_at": bson.M{"$lt": cutoff},
	})
	if err != nil {
		return 0, fmt.Errorf("purge messages: %w", err)
	}
	return result.DeletedCount, nil
}

// Compile-time check
var _ persistent.Store = (*Store)(nil)
