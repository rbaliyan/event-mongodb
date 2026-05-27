package mongodb

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
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
func NewMongoResumeTokenStore(collection *mongo.Collection) (*MongoResumeTokenStore, error) {
	if collection == nil {
		return nil, ErrCollectionNil
	}
	s := &MongoResumeTokenStore{collection: collection}
	go func() { // #nosec G118 — background goroutine intentionally outlives constructor context
		if err := s.EnsureIndexes(context.Background()); err != nil {
			slog.Default().Error("failed to ensure resume token indexes", "error", err)
		}
	}()
	return s, nil
}

// Load retrieves the resume token for a collection.
func (s *MongoResumeTokenStore) Load(ctx context.Context, collectionName string) (bson.Raw, error) {
	var doc resumeTokenDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": collectionName}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
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

// EnsureIndexes creates the necessary indexes for the resume token store.
// Call this once during application startup.
func (s *MongoResumeTokenStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// Indexes returns the index models for manual creation.
// Use this if you prefer to manage indexes separately (e.g., via migrations).
func (s *MongoResumeTokenStore) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "updated_at", Value: 1}},
		},
	}
}

// MongoAckStore implements AckStore using a MongoDB collection.
// It tracks which change events have been acknowledged for at-least-once delivery.
//
// pending is a local atomic counter of events Store()d but not yet Ack()d by
// this process. It enables a cheap PendingCount() that avoids querying MongoDB
// on every metrics scrape. The counter is per-instance: in multi-instance
// deployments each process tracks only the events it stored; summing the gauge
// across instances (e.g. via Prometheus) yields the cluster-wide total.
type MongoAckStore struct {
	collection *mongo.Collection
	ttl        time.Duration
	pending    atomic.Int64
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
func NewMongoAckStore(collection *mongo.Collection, ttl time.Duration) (*MongoAckStore, error) {
	if collection == nil {
		return nil, ErrCollectionNil
	}
	s := &MongoAckStore{
		collection: collection,
		ttl:        ttl,
	}
	go func() { // #nosec G118 — background goroutine intentionally outlives constructor context
		if err := s.EnsureIndexes(context.Background()); err != nil {
			slog.Default().Error("failed to ensure ack store indexes", "error", err)
		}
	}()
	return s, nil
}

// Store marks an event as pending (not yet acknowledged).
func (s *MongoAckStore) Store(ctx context.Context, eventID string) error {
	_, err := s.collection.InsertOne(ctx, ackDoc{
		ID:        eventID,
		CreatedAt: time.Now(),
	})
	// Ignore duplicate key errors (event already stored) without bumping the
	// pending counter — the original Store() already accounted for it.
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	if err == nil {
		s.pending.Add(1)
	}
	return err
}

// Ack marks an event as acknowledged.
//
// The pending counter is decremented only when a previously-pending document is
// transitioned to acked. Re-acking an already-acked event (MatchedCount == 1
// but ModifiedCount == 0) and acking a non-existent event (MatchedCount == 0)
// both leave the counter unchanged so it stays consistent with the collection.
func (s *MongoAckStore) Ack(ctx context.Context, eventID string) error {
	res, err := s.collection.UpdateOne(
		ctx,
		bson.M{"_id": eventID, "acked_at": time.Time{}},
		bson.M{"$set": bson.M{"acked_at": time.Now()}},
	)
	if err != nil {
		return err
	}
	if res != nil && res.ModifiedCount > 0 {
		s.pending.Add(-1)
	}
	return nil
}

// PendingCount returns the number of events stored by this process that have
// not yet been acknowledged.
//
// This is an in-memory counter; it does not query MongoDB and is safe to call
// at high frequency (e.g. from a Prometheus gauge callback). Note that the
// counter is process-local: it reflects only events that this MongoAckStore
// instance Store()d and has not yet Ack()d. For a cluster-wide pending total
// in multi-instance deployments, sum the per-process gauge across instances
// (the typical Prometheus pattern) or use Count(AckFilter{Status:
// AckStatusPending}) — which is now index-backed thanks to the partial index
// on acked_at (see Indexes).
func (s *MongoAckStore) PendingCount() int64 {
	return s.pending.Load()
}

// EnsureIndexes creates the necessary indexes for the ack store.
// Call this once during application startup.
func (s *MongoAckStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// Indexes returns the index models for manual creation.
// Use this if you prefer to manage indexes separately (e.g., via migrations).
//
// The two acked_at indexes are intentionally asymmetric. The TTL index covers
// only acked documents (acked_at > epoch) so MongoDB can age them out after
// ttl. The "pending" index covers only pending documents (acked_at == epoch)
// so the buildAckFilter(AckStatusPending) query — { acked_at: { $eq: epoch } }
// — can be served from an index instead of a full collection scan. Without
// the pending index a count of un-acked events grows linearly with collection
// size and can drive query-targeting alerts.
func (s *MongoAckStore) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "created_at", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "acked_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(int32(s.ttl.Seconds())).
				SetPartialFilterExpression(bson.M{"acked_at": bson.M{"$gt": time.Time{}}}),
		},
		{
			Keys: bson.D{{Key: "acked_at", Value: 1}},
			Options: options.Index().
				SetName("acked_at_pending").
				SetPartialFilterExpression(bson.M{"acked_at": bson.M{"$eq": time.Time{}}}),
		},
	}
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

	if !filter.StartTime.IsZero() || !filter.EndTime.IsZero() {
		createdFilter := bson.M{}
		if !filter.StartTime.IsZero() {
			createdFilter["$gte"] = filter.StartTime
		}
		if !filter.EndTime.IsZero() {
			createdFilter["$lt"] = filter.EndTime
		}
		f["created_at"] = createdFilter
	}

	return f
}

// Compile-time checks
var (
	_ ResumeTokenStore = (*MongoResumeTokenStore)(nil)
	_ AckStore         = (*MongoAckStore)(nil)
	_ AckQueryStore    = (*MongoAckStore)(nil)
)
