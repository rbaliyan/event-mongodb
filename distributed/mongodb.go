package distributed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	evtdistributed "github.com/rbaliyan/event/v3/distributed"
)

const (
	defaultStateCollection = "_message_state"

	statusProcessing = "processing"
	statusCompleted  = "completed"
	statusReleased   = "released"
)

// stateDocument represents a state record in MongoDB.
type stateDocument struct {
	ID        string            `bson:"_id"`
	Status    string            `bson:"status"`
	WorkerID  string            `bson:"worker_id,omitempty"`
	ExpiresAt time.Time         `bson:"expires_at"`
	CreatedAt time.Time         `bson:"created_at"`
	UpdatedAt time.Time         `bson:"updated_at"`
	Payload   []byte            `bson:"payload,omitempty"`
	Metadata  map[string]string `bson:"metadata,omitempty"`
	EventName string            `bson:"event_name,omitempty"`
}

// MongoStateManager implements Coordinator and PayloadStore using MongoDB for distributed deployments.
type MongoStateManager struct {
	collection    *mongo.Collection
	completionTTL time.Duration
	instanceID    string
	capped        bool
	cappedSize    int64
	cappedMaxDocs int64
}

// NewMongoStateManager creates a new MongoDB-based state manager.
func NewMongoStateManager(db *mongo.Database, opts ...Option) (*MongoStateManager, error) {
	if db == nil {
		return nil, errors.New("distributed: database is required")
	}

	o := defaultStateOptions()
	for _, opt := range opts {
		opt(o)
	}

	collName := defaultStateCollection
	if o.collectionName != "" {
		collName = o.collectionName
	}

	return &MongoStateManager{
		collection:    db.Collection(collName),
		completionTTL: o.completionTTL,
		instanceID:    o.instanceID,
		capped:        o.capped,
		cappedSize:    o.cappedSize,
		cappedMaxDocs: o.cappedMaxDocs,
	}, nil
}

// CreateCollection creates the state collection (capped or regular).
func (s *MongoStateManager) CreateCollection(ctx context.Context) error {
	if !s.capped {
		return nil
	}

	opts := options.CreateCollection().
		SetCapped(true).
		SetSizeInBytes(s.cappedSize)

	if s.cappedMaxDocs > 0 {
		opts.SetMaxDocuments(s.cappedMaxDocs)
	}

	err := s.collection.Database().CreateCollection(ctx, s.collection.Name(), opts)
	if err != nil {
		if !mongo.IsDuplicateKeyError(err) && !isNamespaceExistsError(err) {
			return fmt.Errorf("create capped collection: %w", err)
		}
	}

	return nil
}

func isNamespaceExistsError(err error) bool {
	if err == nil {
		return false
	}
	if cmdErr, ok := err.(mongo.CommandError); ok {
		return cmdErr.Code == 48
	}
	return false
}

func (s *MongoStateManager) generateWorkerID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	nonce := hex.EncodeToString(b)
	if s.instanceID != "" {
		return s.instanceID + ":" + nonce
	}
	return nonce
}

// Acquire atomically transitions a message to "processing" state.
func (s *MongoStateManager) Acquire(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	now := time.Now()
	expiresAt := now.Add(ttl)
	workerID := s.generateWorkerID()

	filter := bson.M{
		"_id": messageID,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}},
			{"status": bson.M{"$exists": false}},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"status":     statusProcessing,
			"worker_id":  workerID,
			"expires_at": expiresAt,
			"updated_at": now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result stateDocument
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		if errors.Is(err, mongo.ErrNoDocuments) {
			var existing stateDocument
			findErr := s.collection.FindOne(ctx, bson.M{"_id": messageID}).Decode(&existing)
			if findErr == nil && existing.ExpiresAt.After(now) {
				return false, nil
			}
			return s.tryInsert(ctx, messageID, ttl, workerID)
		}
		return false, fmt.Errorf("mongodb find and update: %w", err)
	}

	return result.WorkerID == workerID, nil
}

// MarkProcessed transitions a message to "completed" state.
func (s *MongoStateManager) MarkProcessed(ctx context.Context, messageID string) error {
	now := time.Now()

	filter := bson.M{"_id": messageID}
	update := bson.M{
		"$set": bson.M{
			"status":     statusCompleted,
			"expires_at": now.Add(s.completionTTL),
			"updated_at": now,
		},
	}

	_, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("mongodb update: %w", err)
	}

	return nil
}

// Reset removes the message state to allow immediate reacquisition.
func (s *MongoStateManager) Reset(ctx context.Context, messageID string) error {
	if s.capped {
		now := time.Now()
		filter := bson.M{"_id": messageID}
		update := bson.M{
			"$set": bson.M{
				"status":     statusReleased,
				"expires_at": now,
				"updated_at": now,
			},
		}
		_, err := s.collection.UpdateOne(ctx, filter, update)
		if err != nil {
			return fmt.Errorf("mongodb update (reset): %w", err)
		}
		return nil
	}

	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": messageID})
	if err != nil {
		return fmt.Errorf("mongodb delete: %w", err)
	}

	return nil
}

// ListStale returns message IDs of states that have been processing longer than staleTimeout.
func (s *MongoStateManager) ListStale(ctx context.Context, staleTimeout time.Duration, limit int) ([]string, error) {
	cutoff := time.Now().Add(-staleTimeout)

	filter := bson.M{
		"status":     statusProcessing,
		"updated_at": bson.M{"$lt": cutoff},
	}

	opts := options.Find().SetProjection(bson.M{"_id": 1})
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb find stale: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var stale []string
	for cursor.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		stale = append(stale, doc.ID)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongodb cursor: %w", err)
	}

	return stale, nil
}

// ResetStale resets all stale states, allowing them to be reacquired.
func (s *MongoStateManager) ResetStale(ctx context.Context, staleTimeout time.Duration, limit int) (int64, error) {
	stale, err := s.ListStale(ctx, staleTimeout, limit)
	if err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}

	filter := bson.M{"_id": bson.M{"$in": stale}}

	if s.capped {
		now := time.Now()
		update := bson.M{
			"$set": bson.M{
				"status":     statusReleased,
				"expires_at": now,
				"updated_at": now,
			},
		}
		result, err := s.collection.UpdateMany(ctx, filter, update)
		if err != nil {
			return 0, fmt.Errorf("mongodb update stale: %w", err)
		}
		return result.ModifiedCount, nil
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("mongodb delete stale: %w", err)
	}
	return result.DeletedCount, nil
}

func (s *MongoStateManager) tryInsert(ctx context.Context, messageID string, ttl time.Duration, workerID string) (bool, error) {
	now := time.Now()

	doc := stateDocument{
		ID:        messageID,
		Status:    statusProcessing,
		WorkerID:  workerID,
		ExpiresAt: now.Add(ttl),
		CreatedAt: now,
		UpdatedAt: now,
	}

	_, err := s.collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("mongodb insert: %w", err)
	}

	return true, nil
}

// StorePayload persists message payload alongside state for recovery re-publishing.
func (s *MongoStateManager) StorePayload(ctx context.Context, messageID string, data *evtdistributed.MessageData) error {
	if data == nil || len(data.Payload) == 0 {
		return nil
	}

	setFields := bson.M{
		"payload": data.Payload,
	}
	if len(data.Metadata) > 0 {
		setFields["metadata"] = data.Metadata
	}
	if data.EventName != "" {
		setFields["event_name"] = data.EventName
	}

	_, err := s.collection.UpdateOne(ctx, bson.M{"_id": messageID}, bson.M{"$set": setFields})
	if err != nil {
		return fmt.Errorf("mongodb store payload: %w", err)
	}
	return nil
}

// LoadStalePayloads returns stale messages that have stored payload.
func (s *MongoStateManager) LoadStalePayloads(ctx context.Context, staleTimeout time.Duration, limit int) ([]*evtdistributed.StaleMessage, error) {
	cutoff := time.Now().Add(-staleTimeout)

	filter := bson.M{
		"status":     statusProcessing,
		"updated_at": bson.M{"$lt": cutoff},
		"payload":    bson.M{"$exists": true},
	}

	opts := options.Find()
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb find stale payloads: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var results []*evtdistributed.StaleMessage
	for cursor.Next(ctx) {
		var doc stateDocument
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		results = append(results, &evtdistributed.StaleMessage{
			MessageID: doc.ID,
			Data: evtdistributed.MessageData{
				Payload:   doc.Payload,
				Metadata:  doc.Metadata,
				EventName: doc.EventName,
			},
			CreatedAt: doc.CreatedAt,
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongodb cursor: %w", err)
	}

	return results, nil
}

// ClearPayload removes the stored binary payload for a message.
func (s *MongoStateManager) ClearPayload(ctx context.Context, messageID string) error {
	_, err := s.collection.UpdateOne(ctx, bson.M{"_id": messageID}, bson.M{
		"$unset": bson.M{"payload": ""},
	})
	if err != nil {
		return fmt.Errorf("mongodb clear payload: %w", err)
	}
	return nil
}

// EnsureIndexes creates the necessary indexes for the state collection.
func (s *MongoStateManager) EnsureIndexes(ctx context.Context) error {
	indexes := s.Indexes()
	if len(indexes) == 0 {
		return nil
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	return nil
}

// Indexes returns the index models for the state collection.
func (s *MongoStateManager) Indexes() []mongo.IndexModel {
	if s.capped {
		return []mongo.IndexModel{
			{Keys: bson.D{{Key: "expires_at", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "updated_at", Value: 1}, {Key: "_id", Value: 1}}},
			{Keys: bson.D{{Key: "updated_at", Value: 1}, {Key: "_id", Value: 1}}},
			{Keys: bson.D{{Key: "event_name", Value: 1}, {Key: "updated_at", Value: 1}, {Key: "_id", Value: 1}}},
		}
	}

	return []mongo.IndexModel{
		{Keys: bson.D{{Key: "expires_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "updated_at", Value: 1}, {Key: "_id", Value: 1}}},
		{Keys: bson.D{{Key: "updated_at", Value: 1}, {Key: "_id", Value: 1}}},
		{Keys: bson.D{{Key: "event_name", Value: 1}, {Key: "updated_at", Value: 1}, {Key: "_id", Value: 1}}},
	}
}

// Compile-time interface checks
var (
	_ evtdistributed.Coordinator   = (*MongoStateManager)(nil)
	_ evtdistributed.PayloadStore  = (*MongoStateManager)(nil)
	_ evtdistributed.StaleResetter = (*MongoStateManager)(nil)
)
