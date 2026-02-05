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

// checkpointDoc represents the MongoDB document structure for checkpoints.
type checkpointDoc struct {
	ID         string    `bson:"_id"`        // Composite key: eventName:consumerID
	EventName  string    `bson:"event_name"`
	ConsumerID string    `bson:"consumer_id"`
	Checkpoint string    `bson:"checkpoint"` // Last processed sequence ID
	UpdatedAt  time.Time `bson:"updated_at"`
}

// CheckpointStore implements persistent.CheckpointStore using MongoDB.
// Checkpoints are stored as documents keyed by event name and consumer ID.
//
// Document structure:
//
//	{
//	    "_id": "orders:consumer-1",
//	    "event_name": "orders",
//	    "consumer_id": "consumer-1",
//	    "checkpoint": "507f1f77bcf86cd799439011",
//	    "updated_at": ISODate("...")
//	}
type CheckpointStore struct {
	collection *mongo.Collection
	ttl        time.Duration
}

// CheckpointStoreOption configures the checkpoint store.
type CheckpointStoreOption func(*CheckpointStore)

// WithCheckpointTTL sets the TTL for checkpoint documents.
// Stale checkpoints will be automatically deleted after this duration.
// Default is 0 (no automatic deletion).
func WithCheckpointTTL(ttl time.Duration) CheckpointStoreOption {
	return func(s *CheckpointStore) {
		if ttl > 0 {
			s.ttl = ttl
		}
	}
}

// NewCheckpointStore creates a new MongoDB-backed checkpoint store.
//
// Example:
//
//	cpStore, err := persistent.NewCheckpointStore(
//	    mongoClient.Database("events").Collection("checkpoints"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	cpStore.EnsureIndexes(ctx)
//
//	transport, _ := composite.New(store, signal,
//	    composite.WithCheckpointStore(cpStore),
//	)
func NewCheckpointStore(collection *mongo.Collection, opts ...CheckpointStoreOption) (*CheckpointStore, error) {
	if collection == nil {
		return nil, ErrCollectionRequired
	}

	s := &CheckpointStore{
		collection: collection,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// checkpointKey generates the composite document ID.
func checkpointKey(eventName, consumerID string) string {
	return eventName + ":" + consumerID
}

// Load retrieves the checkpoint for a consumer.
// Returns empty string if no checkpoint exists.
func (s *CheckpointStore) Load(ctx context.Context, eventName, consumerID string) (string, error) {
	key := checkpointKey(eventName, consumerID)

	var doc checkpointDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil // No checkpoint stored yet
		}
		return "", fmt.Errorf("load checkpoint: %w", err)
	}

	return doc.Checkpoint, nil
}

// Save persists the checkpoint for a consumer.
func (s *CheckpointStore) Save(ctx context.Context, eventName, consumerID string, checkpoint string) error {
	key := checkpointKey(eventName, consumerID)

	doc := checkpointDoc{
		ID:         key,
		EventName:  eventName,
		ConsumerID: consumerID,
		Checkpoint: checkpoint,
		UpdatedAt:  time.Now(),
	}

	_, err := s.collection.ReplaceOne(
		ctx,
		bson.M{"_id": key},
		doc,
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

// Delete removes a checkpoint for a consumer.
func (s *CheckpointStore) Delete(ctx context.Context, eventName, consumerID string) error {
	key := checkpointKey(eventName, consumerID)

	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": key})
	if err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}

	return nil
}

// DeleteByEvent removes all checkpoints for an event.
func (s *CheckpointStore) DeleteByEvent(ctx context.Context, eventName string) (int64, error) {
	result, err := s.collection.DeleteMany(ctx, bson.M{"event_name": eventName})
	if err != nil {
		return 0, fmt.Errorf("delete checkpoints by event: %w", err)
	}
	return result.DeletedCount, nil
}

// List returns all checkpoints for an event.
func (s *CheckpointStore) List(ctx context.Context, eventName string) (map[string]string, error) {
	filter := bson.M{}
	if eventName != "" {
		filter["event_name"] = eventName
	}

	cursor, err := s.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer cursor.Close(ctx)

	checkpoints := make(map[string]string)
	for cursor.Next(ctx) {
		var doc checkpointDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode checkpoint: %w", err)
		}
		checkpoints[doc.ConsumerID] = doc.Checkpoint
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}

	return checkpoints, nil
}

// EnsureIndexes creates the required indexes for efficient queries.
// Call this once during application startup.
func (s *CheckpointStore) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "event_name", Value: 1}},
			Options: options.Index().SetName("event_name"),
		},
		{
			Keys:    bson.D{{Key: "consumer_id", Value: 1}},
			Options: options.Index().SetName("consumer_id"),
		},
	}

	// Add TTL index if configured
	if s.ttl > 0 {
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{{Key: "updated_at", Value: 1}},
			Options: options.Index().
				SetName("checkpoint_ttl").
				SetExpireAfterSeconds(int32(s.ttl.Seconds())),
		})
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	return nil
}

// Indexes returns the index models for manual creation.
func (s *CheckpointStore) Indexes() []mongo.IndexModel {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "event_name", Value: 1}}},
		{Keys: bson.D{{Key: "consumer_id", Value: 1}}},
	}

	if s.ttl > 0 {
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{{Key: "updated_at", Value: 1}},
			Options: options.Index().
				SetExpireAfterSeconds(int32(s.ttl.Seconds())),
		})
	}

	return indexes
}

// Collection returns the underlying MongoDB collection for custom queries.
func (s *CheckpointStore) Collection() *mongo.Collection {
	return s.collection
}

// Compile-time check
var _ persistent.CheckpointStore = (*CheckpointStore)(nil)
