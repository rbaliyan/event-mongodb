package persistent

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// mustNewCheckpointStore creates a checkpoint store for testing.
func mustNewCheckpointStore(t testing.TB, mt *mtest.T, opts ...CheckpointStoreOption) *CheckpointStore {
	t.Helper()
	store, err := NewCheckpointStore(mt.Coll, opts...)
	if err != nil {
		t.Fatalf("NewCheckpointStore failed: %v", err)
	}
	return store
}

func TestCheckpointStore_NewCheckpointStore(t *testing.T) {
	t.Run("nil collection", func(t *testing.T) {
		_, err := NewCheckpointStore(nil)
		if !errors.Is(err, ErrCollectionRequired) {
			t.Errorf("expected ErrCollectionRequired, got %v", err)
		}
	})

	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("valid collection", func(mt *mtest.T) {
		store, err := NewCheckpointStore(mt.Coll)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if store == nil {
			t.Fatal("expected store, got nil")
		}
	})
}

func TestCheckpointStore_Load(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		mt.AddMockResponses(mtest.CreateCursorResponse(1, "db.checkpoints", mtest.FirstBatch,
			bson.D{
				{Key: "_id", Value: "test-event:consumer-1"},
				{Key: "event_name", Value: "test-event"},
				{Key: "consumer_id", Value: "consumer-1"},
				{Key: "checkpoint", Value: "507f1f77bcf86cd799439011"},
				{Key: "updated_at", Value: time.Now()},
			},
		))

		checkpoint, err := store.Load(context.Background(), "test-event", "consumer-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if checkpoint != "507f1f77bcf86cd799439011" {
			t.Errorf("expected checkpoint '507f1f77bcf86cd799439011', got '%s'", checkpoint)
		}
	})

	mt.Run("not found", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		mt.AddMockResponses(mtest.CreateCursorResponse(0, "db.checkpoints", mtest.FirstBatch))

		checkpoint, err := store.Load(context.Background(), "test-event", "consumer-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if checkpoint != "" {
			t.Errorf("expected empty checkpoint, got '%s'", checkpoint)
		}
	})
}

func TestCheckpointStore_Save(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "nModified", Value: 1},
		})

		err := store.Save(context.Background(), "test-event", "consumer-1", "507f1f77bcf86cd799439011")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestCheckpointStore_Delete(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "n", Value: 1},
		})

		err := store.Delete(context.Background(), "test-event", "consumer-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestCheckpointStore_DeleteByEvent(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		mt.AddMockResponses(bson.D{
			{Key: "ok", Value: 1},
			{Key: "n", Value: int64(3)},
		})

		deleted, err := store.DeleteByEvent(context.Background(), "test-event")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if deleted != 3 {
			t.Errorf("expected 3 deleted, got %d", deleted)
		}
	})
}

func TestCheckpointStore_List(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		first := mtest.CreateCursorResponse(1, "db.checkpoints", mtest.FirstBatch,
			bson.D{
				{Key: "_id", Value: "test-event:consumer-1"},
				{Key: "event_name", Value: "test-event"},
				{Key: "consumer_id", Value: "consumer-1"},
				{Key: "checkpoint", Value: "checkpoint-1"},
				{Key: "updated_at", Value: time.Now()},
			},
			bson.D{
				{Key: "_id", Value: "test-event:consumer-2"},
				{Key: "event_name", Value: "test-event"},
				{Key: "consumer_id", Value: "consumer-2"},
				{Key: "checkpoint", Value: "checkpoint-2"},
				{Key: "updated_at", Value: time.Now()},
			},
		)
		killCursors := mtest.CreateCursorResponse(0, "db.checkpoints", mtest.NextBatch)
		mt.AddMockResponses(first, killCursors)

		checkpoints, err := store.List(context.Background(), "test-event")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(checkpoints) != 2 {
			t.Errorf("expected 2 checkpoints, got %d", len(checkpoints))
		}
		if checkpoints["consumer-1"] != "checkpoint-1" {
			t.Errorf("expected checkpoint-1 for consumer-1, got %s", checkpoints["consumer-1"])
		}
		if checkpoints["consumer-2"] != "checkpoint-2" {
			t.Errorf("expected checkpoint-2 for consumer-2, got %s", checkpoints["consumer-2"])
		}
	})

	mt.Run("empty", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		first := mtest.CreateCursorResponse(0, "db.checkpoints", mtest.FirstBatch)
		mt.AddMockResponses(first)

		checkpoints, err := store.List(context.Background(), "test-event")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(checkpoints) != 0 {
			t.Errorf("expected 0 checkpoints, got %d", len(checkpoints))
		}
	})
}

func TestCheckpointStore_Options(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("with TTL", func(mt *mtest.T) {
		store, err := NewCheckpointStore(mt.Coll, WithCheckpointTTL(7*24*time.Hour))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if store.ttl != 7*24*time.Hour {
			t.Errorf("expected TTL 7d, got %v", store.ttl)
		}
	})

	mt.Run("default no TTL", func(mt *mtest.T) {
		store, err := NewCheckpointStore(mt.Coll)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if store.ttl != 0 {
			t.Errorf("expected no TTL, got %v", store.ttl)
		}
	})
}

func TestCheckpointStore_EnsureIndexes(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success without TTL", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)

		mt.AddMockResponses(mtest.CreateSuccessResponse())

		err := store.EnsureIndexes(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	mt.Run("success with TTL", func(mt *mtest.T) {
		store, err := NewCheckpointStore(mt.Coll, WithCheckpointTTL(24*time.Hour))
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

func TestCheckpointStore_Collection(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("returns collection", func(mt *mtest.T) {
		store := mustNewCheckpointStore(t, mt)
		if store.Collection() != mt.Coll {
			t.Error("expected same collection reference")
		}
	})
}

func TestCheckpointKey(t *testing.T) {
	key := checkpointKey("orders", "consumer-1")
	expected := "orders:consumer-1"
	if key != expected {
		t.Errorf("expected key '%s', got '%s'", expected, key)
	}
}
