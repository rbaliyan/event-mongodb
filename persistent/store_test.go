package persistent

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// mustNewStore creates a store for testing, failing the test if creation fails.
func mustNewStore(t testing.TB, mt *mtest.T) *Store {
	t.Helper()
	store, err := NewStore(mt.Coll)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	return store
}

func TestStore_NewStore(t *testing.T) {
	t.Run("nil collection", func(t *testing.T) {
		_, err := NewStore(nil)
		if !errors.Is(err, ErrCollectionRequired) {
			t.Errorf("expected ErrCollectionRequired, got %v", err)
		}
	})

	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("valid collection", func(mt *mtest.T) {
		store, err := NewStore(mt.Coll)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if store == nil {
			t.Fatal("expected store, got nil")
		}
	})
}

func TestStore_Append(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(mtest.CreateSuccessResponse())

		seqID, err := store.Append(context.Background(), "test-event", []byte("test-data"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify sequence ID is a valid ObjectID hex string
		if _, err := primitive.ObjectIDFromHex(seqID); err != nil {
			t.Errorf("invalid sequence ID: %s", seqID)
		}
	})

	mt.Run("insert error", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(mtest.CreateWriteErrorsResponse(mtest.WriteError{
			Index:   0,
			Code:    11000,
			Message: "duplicate key error",
		}))

		_, err := store.Append(context.Background(), "test-event", []byte("test-data"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestStore_Fetch(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		id := primitive.NewObjectID()
		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "value", Value: bson.D{
				{Key: "_id", Value: id},
				{Key: "event_name", Value: "test-event"},
				{Key: "data", Value: []byte("test-data")},
				{Key: "status", Value: statusInflight},
				{Key: "timestamp", Value: time.Now()},
				{Key: "retry_count", Value: 0},
			}},
		})

		msg, err := store.Fetch(context.Background(), "test-event", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if msg.SequenceID != id.Hex() {
			t.Errorf("expected sequence ID %s, got %s", id.Hex(), msg.SequenceID)
		}
		if string(msg.Data) != "test-data" {
			t.Errorf("expected data 'test-data', got '%s'", string(msg.Data))
		}
	})

	mt.Run("no messages", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "value", Value: nil},
		})

		msg, err := store.Fetch(context.Background(), "test-event", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg != nil {
			t.Error("expected nil message")
		}
	})

	mt.Run("with checkpoint", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		id := primitive.NewObjectID()
		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "value", Value: bson.D{
				{Key: "_id", Value: id},
				{Key: "event_name", Value: "test-event"},
				{Key: "data", Value: []byte("test-data")},
				{Key: "status", Value: statusInflight},
				{Key: "timestamp", Value: time.Now()},
				{Key: "retry_count", Value: 0},
			}},
		})

		checkpoint := primitive.NewObjectID().Hex()
		msg, err := store.Fetch(context.Background(), "test-event", checkpoint)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
	})

	mt.Run("invalid checkpoint", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		_, err := store.Fetch(context.Background(), "test-event", "invalid-oid")
		if err == nil {
			t.Fatal("expected error for invalid checkpoint")
		}
	})
}

func TestStore_Ack(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "nModified", Value: 1},
			{Key: "n", Value: 1},
		})

		id := primitive.NewObjectID().Hex()
		err := store.Ack(context.Background(), "test-event", id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	mt.Run("message not found", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "nModified", Value: 0},
			{Key: "n", Value: 0},
		})

		id := primitive.NewObjectID().Hex()
		err := store.Ack(context.Background(), "test-event", id)
		if !errors.Is(err, ErrMessageNotFound) {
			t.Errorf("expected ErrMessageNotFound, got %v", err)
		}
	})

	mt.Run("invalid sequence ID", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		err := store.Ack(context.Background(), "test-event", "invalid-oid")
		if !errors.Is(err, ErrInvalidSequenceID) {
			t.Errorf("expected ErrInvalidSequenceID, got %v", err)
		}
	})
}

func TestStore_Nack(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "nModified", Value: 1},
			{Key: "n", Value: 1},
		})

		id := primitive.NewObjectID().Hex()
		err := store.Nack(context.Background(), "test-event", id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	mt.Run("message not found", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "nModified", Value: 0},
			{Key: "n", Value: 0},
		})

		id := primitive.NewObjectID().Hex()
		err := store.Nack(context.Background(), "test-event", id)
		if !errors.Is(err, ErrMessageNotFound) {
			t.Errorf("expected ErrMessageNotFound, got %v", err)
		}
	})

	mt.Run("invalid sequence ID", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		err := store.Nack(context.Background(), "test-event", "invalid-oid")
		if !errors.Is(err, ErrInvalidSequenceID) {
			t.Errorf("expected ErrInvalidSequenceID, got %v", err)
		}
	})
}

func TestStore_Options(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("with TTL", func(mt *mtest.T) {
		store, err := NewStore(mt.Coll, WithTTL(24*time.Hour))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if store.ttl != 24*time.Hour {
			t.Errorf("expected TTL 24h, got %v", store.ttl)
		}
	})

	mt.Run("with visibility timeout", func(mt *mtest.T) {
		store, err := NewStore(mt.Coll, WithVisibilityTimeout(10*time.Minute))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if store.visibilityTimeout != 10*time.Minute {
			t.Errorf("expected visibility timeout 10m, got %v", store.visibilityTimeout)
		}
	})

	mt.Run("default visibility timeout", func(mt *mtest.T) {
		store, err := NewStore(mt.Coll)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if store.visibilityTimeout != defaultVisibilityTimeout {
			t.Errorf("expected default visibility timeout %v, got %v",
				defaultVisibilityTimeout, store.visibilityTimeout)
		}
	})
}

func TestStore_EnsureIndexes(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success without TTL", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(mtest.CreateSuccessResponse())

		err := store.EnsureIndexes(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	mt.Run("success with TTL", func(mt *mtest.T) {
		store, err := NewStore(mt.Coll, WithTTL(24*time.Hour))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		mt.AddMockResponses(mtest.CreateSuccessResponse())

		err = store.EnsureIndexes(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify TTL index is in the list
		indexes := store.Indexes()
		hasTTL := false
		for _, idx := range indexes {
			if opts := idx.Options; opts != nil {
				if opts.ExpireAfterSeconds != nil {
					hasTTL = true
					break
				}
			}
		}
		if !hasTTL {
			t.Error("expected TTL index when TTL is configured")
		}
	})
}

func TestStore_GetStats(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		// Create a cursor response with aggregation results
		first := mtest.CreateCursorResponse(1, "db.collection", mtest.FirstBatch,
			bson.D{{Key: "_id", Value: statusPending}, {Key: "count", Value: int64(5)}},
			bson.D{{Key: "_id", Value: statusInflight}, {Key: "count", Value: int64(2)}},
			bson.D{{Key: "_id", Value: statusAcked}, {Key: "count", Value: int64(10)}},
		)
		killCursors := mtest.CreateCursorResponse(0, "db.collection", mtest.NextBatch)
		mt.AddMockResponses(first, killCursors)

		stats, err := store.GetStats(context.Background(), "test-event")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if stats.Pending != 5 {
			t.Errorf("expected 5 pending, got %d", stats.Pending)
		}
		if stats.Inflight != 2 {
			t.Errorf("expected 2 inflight, got %d", stats.Inflight)
		}
		if stats.Acked != 10 {
			t.Errorf("expected 10 acked, got %d", stats.Acked)
		}
		if stats.Total != 17 {
			t.Errorf("expected 17 total, got %d", stats.Total)
		}
	})
}

func TestStore_Purge(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "n", Value: int64(5)},
		})

		deleted, err := store.Purge(context.Background(), 24*time.Hour)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if deleted != 5 {
			t.Errorf("expected 5 deleted, got %d", deleted)
		}
	})
}

func TestStore_Collection(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("returns collection", func(mt *mtest.T) {
		store := mustNewStore(t, mt)
		if store.Collection() != mt.Coll {
			t.Error("expected same collection reference")
		}
	})
}

// TestStoreIntegration contains integration tests that require a real MongoDB.
// Skip these in CI unless MONGODB_URI is set.
func TestStoreIntegration(t *testing.T) {
	// Skip if not running integration tests
	t.Skip("integration tests require MONGODB_URI environment variable")
}
