package checkpoint

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	evtcheckpoint "github.com/rbaliyan/event/v3/checkpoint"
)

// MongoStore implements CheckpointStore using MongoDB.
// Checkpoints are stored as documents with subscriber ID as the key.
type MongoStore struct {
	collection *mongo.Collection
	ttl        time.Duration
}

// MongoOption configures the MongoDB checkpoint store.
type MongoOption func(*MongoStore)

// WithMongoTTL sets a TTL for checkpoint documents.
func WithMongoTTL(ttl time.Duration) MongoOption {
	return func(s *MongoStore) {
		s.ttl = ttl
	}
}

type checkpointDoc struct {
	ID        string    `bson:"_id"`
	Position  time.Time `bson:"position"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// NewMongoStore creates a new MongoDB-backed checkpoint store.
func NewMongoStore(collection *mongo.Collection, opts ...MongoOption) (*MongoStore, error) {
	if collection == nil {
		return nil, errors.New("checkpoint: collection is required")
	}

	s := &MongoStore{
		collection: collection,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Indexes returns the index models for the checkpoint collection.
func (s *MongoStore) Indexes() []mongo.IndexModel {
	var indexes []mongo.IndexModel

	if s.ttl > 0 {
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{{Key: "updated_at", Value: 1}},
			Options: options.Index().
				SetExpireAfterSeconds(int32(s.ttl.Seconds())).
				SetName("checkpoint_ttl"),
		})
	}

	return indexes
}

// EnsureIndexes creates the required indexes for the checkpoint collection.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	indexes := s.Indexes()
	if len(indexes) == 0 {
		return nil
	}
	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// Save persists the checkpoint position for a subscriber.
func (s *MongoStore) Save(ctx context.Context, subscriberID string, position time.Time) error {
	now := time.Now()
	doc := checkpointDoc{
		ID:        subscriberID,
		Position:  position,
		UpdatedAt: now,
	}

	opts := options.Replace().SetUpsert(true)
	_, err := s.collection.ReplaceOne(
		ctx,
		bson.M{"_id": subscriberID},
		doc,
		opts,
	)
	return err
}

// Load retrieves the last saved checkpoint for a subscriber.
// Returns zero time and nil error if no checkpoint exists.
func (s *MongoStore) Load(ctx context.Context, subscriberID string) (time.Time, error) {
	var doc checkpointDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": subscriberID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return doc.Position, nil
}

// Delete removes a checkpoint for a subscriber.
func (s *MongoStore) Delete(ctx context.Context, subscriberID string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": subscriberID})
	return err
}

// DeleteAll removes all checkpoints.
func (s *MongoStore) DeleteAll(ctx context.Context) error {
	_, err := s.collection.DeleteMany(ctx, bson.M{})
	return err
}

// List returns all subscriber IDs with checkpoints.
func (s *MongoStore) List(ctx context.Context) ([]string, error) {
	cursor, err := s.collection.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	var ids []string
	for cursor.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		ids = append(ids, doc.ID)
	}
	return ids, cursor.Err()
}

// GetAll returns all checkpoints as a map.
func (s *MongoStore) GetAll(ctx context.Context) (map[string]time.Time, error) {
	cursor, err := s.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	checkpoints := make(map[string]time.Time)
	for cursor.Next(ctx) {
		var doc checkpointDoc
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		checkpoints[doc.ID] = doc.Position
	}
	return checkpoints, cursor.Err()
}

// GetCheckpointInfo returns detailed checkpoint information for a subscriber.
func (s *MongoStore) GetCheckpointInfo(ctx context.Context, subscriberID string) (*evtcheckpoint.CheckpointInfo, error) {
	var doc checkpointDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": subscriberID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &evtcheckpoint.CheckpointInfo{
		SubscriberID: doc.ID,
		Position:     doc.Position,
		UpdatedAt:    doc.UpdatedAt,
	}, nil
}

// Compile-time check
var _ evtcheckpoint.CheckpointStore = (*MongoStore)(nil)
