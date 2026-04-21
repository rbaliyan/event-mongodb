package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	evtidempotency "github.com/rbaliyan/event/v3/idempotency"
	evtbase "github.com/rbaliyan/event/v3/store/base"
)

// MongoStore implements Store using MongoDB for distributed idempotency.
type MongoStore struct {
	collection      *mongo.Collection
	collectionName  string
	ttl             time.Duration
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
}

// MongoOption configures a MongoStore.
type MongoOption func(*MongoStore)

// WithMongoTTL sets the default TTL for idempotency entries. Default is 24 hours.
func WithMongoTTL(ttl time.Duration) MongoOption {
	return func(s *MongoStore) {
		s.ttl = ttl
	}
}

// WithMongoCollection sets a custom collection name. Default is "event_idempotency".
func WithMongoCollection(name string) MongoOption {
	return func(s *MongoStore) {
		if name != "" {
			s.collectionName = name
		}
	}
}

// WithMongoCleanupInterval sets how often to run manual cleanup. Default is 1 hour.
func WithMongoCleanupInterval(interval time.Duration) MongoOption {
	return func(s *MongoStore) {
		s.cleanupInterval = interval
	}
}

type idempotencyEntry struct {
	ID          string    `bson:"_id"`
	ProcessedAt time.Time `bson:"processed_at"`
	ExpiresAt   time.Time `bson:"expires_at"`
}

// NewMongoStore creates a new MongoDB-based idempotency store.
func NewMongoStore(db *mongo.Database, opts ...MongoOption) (*MongoStore, error) {
	if db == nil {
		return nil, errors.New("idempotency: database is required")
	}

	s := &MongoStore{
		collectionName:  "event_idempotency",
		ttl:             24 * time.Hour,
		cleanupInterval: time.Hour,
		stopCleanup:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.collection = db.Collection(s.collectionName)

	if s.cleanupInterval > 0 {
		go evtbase.SimpleCleanupLoop(s.cleanupInterval, s.stopCleanup, s.cleanup)
	}

	return s, nil
}

// NewMongoStoreWithCollection creates a new MongoDB-based idempotency store with a custom collection name.
func NewMongoStoreWithCollection(db *mongo.Database, collectionName string, opts ...MongoOption) (*MongoStore, error) {
	if db == nil {
		return nil, errors.New("idempotency: database is required")
	}
	if collectionName == "" {
		return nil, errors.New("idempotency: collection name is required")
	}

	s := &MongoStore{
		collectionName:  collectionName,
		ttl:             24 * time.Hour,
		cleanupInterval: time.Hour,
		stopCleanup:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.collection = db.Collection(s.collectionName)

	if s.cleanupInterval > 0 {
		go evtbase.SimpleCleanupLoop(s.cleanupInterval, s.stopCleanup, s.cleanup)
	}

	return s, nil
}

// IsDuplicate checks if a message ID has already been processed.
func (s *MongoStore) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	return s.isDuplicateWithCollection(ctx, s.collection, messageID)
}

// IsDuplicateTx checks for duplicate within a MongoDB session/transaction.
func (s *MongoStore) IsDuplicateTx(ctx context.Context, messageID string) (bool, error) {
	return s.isDuplicateWithCollection(ctx, s.collection, messageID)
}

func (s *MongoStore) isDuplicateWithCollection(ctx context.Context, coll *mongo.Collection, messageID string) (bool, error) {
	now := time.Now()
	expiresAt := now.Add(s.ttl)

	filter := bson.M{
		"_id": messageID,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"processed_at": now,
			"expires_at":   expiresAt,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result idempotencyEntry
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return true, nil
		}
		if mongo.IsDuplicateKeyError(err) {
			return true, nil
		}
		return false, fmt.Errorf("mongo findOneAndUpdate: %w", err)
	}

	return false, nil
}

// MarkProcessed marks a message ID as processed using the default TTL.
func (s *MongoStore) MarkProcessed(ctx context.Context, messageID string) error {
	return s.MarkProcessedWithTTL(ctx, messageID, s.ttl)
}

// MarkProcessedWithTTL marks a message ID as processed with a custom TTL.
func (s *MongoStore) MarkProcessedWithTTL(ctx context.Context, messageID string, ttl time.Duration) error {
	return s.markProcessedWithCollection(ctx, s.collection, messageID, ttl)
}

// MarkProcessedTx marks a message ID as processed within a MongoDB transaction.
func (s *MongoStore) MarkProcessedTx(ctx context.Context, messageID string) error {
	return s.markProcessedWithCollection(ctx, s.collection, messageID, s.ttl)
}

// MarkProcessedWithTTLTx marks a message ID as processed with custom TTL within a transaction.
func (s *MongoStore) MarkProcessedWithTTLTx(ctx context.Context, messageID string, ttl time.Duration) error {
	return s.markProcessedWithCollection(ctx, s.collection, messageID, ttl)
}

func (s *MongoStore) markProcessedWithCollection(ctx context.Context, coll *mongo.Collection, messageID string, ttl time.Duration) error {
	now := time.Now()
	expiresAt := now.Add(ttl)

	entry := idempotencyEntry{
		ID:          messageID,
		ProcessedAt: now,
		ExpiresAt:   expiresAt,
	}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.M{"_id": messageID}, entry, opts)
	if err != nil {
		return fmt.Errorf("mongo replace: %w", err)
	}

	return nil
}

// Remove removes a message ID from the store.
func (s *MongoStore) Remove(ctx context.Context, messageID string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": messageID})
	if err != nil {
		return fmt.Errorf("mongo delete: %w", err)
	}
	return nil
}

// Close stops the background cleanup goroutine.
func (s *MongoStore) Close() error {
	select {
	case <-s.stopCleanup:
	default:
		close(s.stopCleanup)
	}
	return nil
}

func (s *MongoStore) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, _ = s.collection.DeleteMany(ctx, bson.M{
		"expires_at": bson.M{"$lt": time.Now()},
	})
}

// EnsureIndexes creates the TTL index on expires_at.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0),
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	return nil
}

// Collection returns the underlying MongoDB collection.
func (s *MongoStore) Collection() *mongo.Collection {
	return s.collection
}

// Compile-time check
var _ evtidempotency.Store = (*MongoStore)(nil)
