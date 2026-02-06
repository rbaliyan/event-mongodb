package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoResumeTokenStore implements ResumeTokenStore using a MongoDB collection.
// It persists resume tokens for reliable change stream resumption across restarts.
type MongoResumeTokenStore struct {
	collection *mongo.Collection
}

// resumeTokenDoc represents the stored resume token document.
type resumeTokenDoc struct {
	ID        string    `bson:"_id"`        // Collection name
	Token     bson.Raw  `bson:"token"`      // Resume token
	UpdatedAt time.Time `bson:"updated_at"` // Last update time
}

// NewMongoResumeTokenStore creates a resume token store using the given collection.
//
// Example:
//
//	store := mongodb.NewMongoResumeTokenStore(db.Collection("_resume_tokens"))
//	t, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithResumeTokenStore(store),
//	)
func NewMongoResumeTokenStore(collection *mongo.Collection) *MongoResumeTokenStore {
	return &MongoResumeTokenStore{collection: collection}
}

// Load retrieves the resume token for a collection.
func (s *MongoResumeTokenStore) Load(ctx context.Context, collectionName string) (bson.Raw, error) {
	var doc resumeTokenDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": collectionName}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil // No token stored yet
		}
		return nil, err
	}
	return doc.Token, nil
}

// Save persists the resume token for a collection.
// If token is nil, the stored token is deleted (used to clear stale tokens).
func (s *MongoResumeTokenStore) Save(ctx context.Context, collectionName string, token bson.Raw) error {
	// If token is nil, delete the document to clear the resume position
	if token == nil {
		_, err := s.collection.DeleteOne(ctx, bson.M{"_id": collectionName})
		return err
	}

	_, err := s.collection.UpdateOne(
		ctx,
		bson.M{"_id": collectionName},
		bson.M{"$set": bson.M{
			"token":      token,
			"updated_at": time.Now(),
		}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// MongoAckStore implements AckStore using a MongoDB collection.
// It tracks which change events have been acknowledged for at-least-once delivery.
type MongoAckStore struct {
	collection *mongo.Collection
	ttl        time.Duration
}

// ackDoc represents a pending acknowledgment document.
type ackDoc struct {
	ID        string    `bson:"_id"`        // Event ID
	CreatedAt time.Time `bson:"created_at"` // When the event was received
	AckedAt   time.Time `bson:"acked_at"`   // When the event was acknowledged
}

// NewMongoAckStore creates an ack store using the given collection.
// The ttl parameter controls how long acknowledged events are retained.
//
// Example:
//
//	store := mongodb.NewMongoAckStore(db.Collection("_event_acks"), 24*time.Hour)
//	t, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithAckStore(store),
//	)
func NewMongoAckStore(collection *mongo.Collection, ttl time.Duration) *MongoAckStore {
	return &MongoAckStore{
		collection: collection,
		ttl:        ttl,
	}
}

// Store marks an event as pending (not yet acknowledged).
func (s *MongoAckStore) Store(ctx context.Context, eventID string) error {
	_, err := s.collection.InsertOne(ctx, ackDoc{
		ID:        eventID,
		CreatedAt: time.Now(),
	})
	// Ignore duplicate key errors (event already stored)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

// Ack marks an event as acknowledged.
func (s *MongoAckStore) Ack(ctx context.Context, eventID string) error {
	_, err := s.collection.UpdateOne(
		ctx,
		bson.M{"_id": eventID},
		bson.M{"$set": bson.M{"acked_at": time.Now()}},
	)
	return err
}

// IsPending checks if an event is still pending acknowledgment.
func (s *MongoAckStore) IsPending(ctx context.Context, eventID string) (bool, error) {
	var doc ackDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": eventID}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil // Not found = not pending
		}
		return false, err
	}
	// Pending if acked_at is zero
	return doc.AckedAt.IsZero(), nil
}

// CreateIndexes creates the necessary indexes for the ack store.
// Call this once during application startup.
func (s *MongoAckStore) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "created_at", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "acked_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(int32(s.ttl.Seconds())).
				SetPartialFilterExpression(bson.M{"acked_at": bson.M{"$gt": time.Time{}}}),
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// List returns ack entries matching the filter.
func (s *MongoAckStore) List(ctx context.Context, filter AckFilter) ([]AckEntry, error) {
	mongoFilter := buildAckFilter(filter)

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetLimit(int64(limit))
	if filter.Offset > 0 {
		opts.SetSkip(int64(filter.Offset))
	}

	cursor, err := s.collection.Find(ctx, mongoFilter, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	var entries []AckEntry
	for cursor.Next(ctx) {
		var doc ackDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		entries = append(entries, AckEntry{
			EventID:   doc.ID,
			CreatedAt: doc.CreatedAt,
			AckedAt:   doc.AckedAt,
		})
	}

	return entries, cursor.Err()
}

// Count returns the number of entries matching the filter.
func (s *MongoAckStore) Count(ctx context.Context, filter AckFilter) (int64, error) {
	mongoFilter := buildAckFilter(filter)
	return s.collection.CountDocuments(ctx, mongoFilter)
}

// buildAckFilter creates a MongoDB filter from AckFilter.
func buildAckFilter(filter AckFilter) bson.M {
	f := bson.M{}

	switch filter.Status {
	case AckStatusPending:
		f["acked_at"] = bson.M{"$eq": time.Time{}}
	case AckStatusAcked:
		f["acked_at"] = bson.M{"$gt": time.Time{}}
	}

	if !filter.StartTime.IsZero() {
		if f["created_at"] == nil {
			f["created_at"] = bson.M{}
		}
		f["created_at"].(bson.M)["$gte"] = filter.StartTime
	}

	if !filter.EndTime.IsZero() {
		if f["created_at"] == nil {
			f["created_at"] = bson.M{}
		}
		f["created_at"].(bson.M)["$lt"] = filter.EndTime
	}

	return f
}

// Compile-time checks
var (
	_ ResumeTokenStore = (*MongoResumeTokenStore)(nil)
	_ AckStore         = (*MongoAckStore)(nil)
	_ AckQueryStore    = (*MongoAckStore)(nil)
)
