package persistent

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupPersistentDB connects to MongoDB (skipping cleanly under -short or when
// MongoDB is unavailable) and returns a per-test database that is dropped on
// cleanup. The shared mongotest helper handles connect+ping+disconnect.
func setupPersistentDB(t *testing.T) *mongo.Database {
	t.Helper()

	client := mongotest.Connect(t)
	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_persistent"))
	t.Cleanup(func() { _ = db.Drop(context.Background()) })
	return db
}

// newStoreForTest builds a Store on a fresh collection and ensures its indexes
// are created synchronously before the test proceeds.
func newStoreForTest(t *testing.T, db *mongo.Database, opts ...StoreOption) *Store {
	t.Helper()

	s, err := NewStore(db.Collection("messages"), opts...)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return s
}

// newCheckpointStoreForTest builds a CheckpointStore on a fresh collection and
// ensures its indexes are created synchronously before the test proceeds.
func newCheckpointStoreForTest(t *testing.T, db *mongo.Database, opts ...CheckpointStoreOption) *CheckpointStore {
	t.Helper()

	s, err := NewCheckpointStore(db.Collection("checkpoints"), opts...)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return s
}

// TestIntegration_Store_AppendFetchAck exercises the core lifecycle:
// Append returns a sequence id, Fetch from a checkpoint returns the next
// message in order, and Ack removes it from the visible set.
func TestIntegration_Store_AppendFetchAck(t *testing.T) {
	db := setupPersistentDB(t)
	store := newStoreForTest(t, db)
	ctx := context.Background()

	const event = "orders"

	seq, err := store.Append(ctx, event, []byte("payload-1"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq == "" {
		t.Fatal("Append returned empty sequence id")
	}

	msg, err := store.Fetch(ctx, event, "")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if msg == nil {
		t.Fatal("Fetch returned nil for a pending message")
	}
	if msg.SequenceID != seq {
		t.Errorf("SequenceID = %q, want %q", msg.SequenceID, seq)
	}
	if string(msg.Data) != "payload-1" {
		t.Errorf("Data = %q, want %q", msg.Data, "payload-1")
	}

	if err := store.Ack(ctx, event, seq); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// After ack, fetching from the start (empty checkpoint) yields nothing.
	got, err := store.Fetch(ctx, event, "")
	if err != nil {
		t.Fatalf("Fetch after Ack: %v", err)
	}
	if got != nil {
		t.Errorf("Fetch after Ack returned %+v, want nil", got)
	}

	stats, err := store.GetStats(ctx, event)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Acked != 1 {
		t.Errorf("Acked = %d, want 1", stats.Acked)
	}
	if stats.Pending != 0 || stats.Inflight != 0 {
		t.Errorf("Pending=%d Inflight=%d, want 0/0", stats.Pending, stats.Inflight)
	}
}

// TestIntegration_Store_FetchOrderingAndCheckpoint confirms that messages are
// fetched in append order and that advancing the checkpoint walks the stream.
func TestIntegration_Store_FetchOrderingAndCheckpoint(t *testing.T) {
	db := setupPersistentDB(t)
	// Large visibility timeout so claimed (inflight) messages are not re-fetched
	// before we advance the checkpoint.
	store := newStoreForTest(t, db, WithVisibilityTimeout(time.Hour))
	ctx := context.Background()

	const event = "stream"
	payloads := []string{"m0", "m1", "m2", "m3"}
	seqs := make([]string, len(payloads))
	for i, p := range payloads {
		// A tiny spacing keeps the per-message timestamp strictly increasing,
		// matching the (timestamp, _id) sort the store uses.
		seq, err := store.Append(ctx, event, []byte(p))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		seqs[i] = seq
		time.Sleep(time.Millisecond)
	}

	checkpoint := ""
	for i, want := range payloads {
		msg, err := store.Fetch(ctx, event, checkpoint)
		if err != nil {
			t.Fatalf("Fetch %d: %v", i, err)
		}
		if msg == nil {
			t.Fatalf("Fetch %d returned nil, want %q", i, want)
		}
		if string(msg.Data) != want {
			t.Errorf("Fetch %d Data = %q, want %q", i, msg.Data, want)
		}
		if msg.SequenceID != seqs[i] {
			t.Errorf("Fetch %d SequenceID = %q, want %q", i, msg.SequenceID, seqs[i])
		}
		checkpoint = msg.SequenceID
	}

	// Fetch past the end returns no message.
	msg, err := store.Fetch(ctx, event, checkpoint)
	if err != nil {
		t.Fatalf("Fetch past end: %v", err)
	}
	if msg != nil {
		t.Errorf("Fetch past end returned %+v, want nil", msg)
	}
}

// TestIntegration_Store_Nack returns a fetched message to pending so it can be
// fetched again, and increments its retry count.
func TestIntegration_Store_Nack(t *testing.T) {
	db := setupPersistentDB(t)
	store := newStoreForTest(t, db, WithVisibilityTimeout(time.Hour))
	ctx := context.Background()

	const event = "retryable"
	seq, err := store.Append(ctx, event, []byte("body"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	first, err := store.Fetch(ctx, event, "")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if first == nil {
		t.Fatal("Fetch returned nil")
	}
	if first.RetryCount != 0 {
		t.Errorf("initial RetryCount = %d, want 0", first.RetryCount)
	}

	// Without Nack the message is inflight (long visibility) and not re-fetchable.
	if again, err := store.Fetch(ctx, event, ""); err != nil {
		t.Fatalf("Fetch (inflight): %v", err)
	} else if again != nil {
		t.Fatalf("inflight message was re-fetched: %+v", again)
	}

	if err := store.Nack(ctx, event, seq); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	second, err := store.Fetch(ctx, event, "")
	if err != nil {
		t.Fatalf("Fetch after Nack: %v", err)
	}
	if second == nil {
		t.Fatal("Fetch after Nack returned nil, want the redelivered message")
	}
	if second.SequenceID != seq {
		t.Errorf("redelivered SequenceID = %q, want %q", second.SequenceID, seq)
	}
	if second.RetryCount != 1 {
		t.Errorf("RetryCount after Nack = %d, want 1", second.RetryCount)
	}
}

// TestIntegration_Store_VisibilityTimeout confirms an un-acked, fetched
// (inflight) message becomes re-fetchable once its visibility timeout expires.
func TestIntegration_Store_VisibilityTimeout(t *testing.T) {
	db := setupPersistentDB(t)
	const visibility = 300 * time.Millisecond
	store := newStoreForTest(t, db, WithVisibilityTimeout(visibility))
	ctx := context.Background()

	const event = "expiring"
	seq, err := store.Append(ctx, event, []byte("v"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	first, err := store.Fetch(ctx, event, "")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if first == nil || first.SequenceID != seq {
		t.Fatalf("first Fetch = %+v, want seq %q", first, seq)
	}

	// Immediately re-fetching finds nothing: the message is still inflight.
	if got, err := store.Fetch(ctx, event, ""); err != nil {
		t.Fatalf("Fetch (still inflight): %v", err)
	} else if got != nil {
		t.Fatalf("message re-fetched before timeout: %+v", got)
	}

	// Wait a bounded amount past the visibility timeout, then it reappears.
	time.Sleep(visibility + 250*time.Millisecond)

	reclaimed, err := store.Fetch(ctx, event, "")
	if err != nil {
		t.Fatalf("Fetch after visibility timeout: %v", err)
	}
	if reclaimed == nil {
		t.Fatal("message was not re-fetchable after visibility timeout")
	}
	if reclaimed.SequenceID != seq {
		t.Errorf("reclaimed SequenceID = %q, want %q", reclaimed.SequenceID, seq)
	}
}

// TestIntegration_Store_GetStats checks pending/inflight/acked/total counts.
func TestIntegration_Store_GetStats(t *testing.T) {
	db := setupPersistentDB(t)
	store := newStoreForTest(t, db, WithVisibilityTimeout(time.Hour))
	ctx := context.Background()

	const event = "metrics"
	var seqs []string
	for i := 0; i < 3; i++ {
		seq, err := store.Append(ctx, event, []byte{byte('a' + i)})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		seqs = append(seqs, seq)
	}

	stats, err := store.GetStats(ctx, event)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Pending != 3 || stats.Inflight != 0 || stats.Acked != 0 || stats.Total != 3 {
		t.Fatalf("after appends: %+v, want pending=3 total=3", stats)
	}

	// Claim one (inflight) and ack another.
	if _, err := store.Fetch(ctx, event, ""); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if err := store.Ack(ctx, event, seqs[2]); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	stats, err = store.GetStats(ctx, event)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Inflight != 1 {
		t.Errorf("Inflight = %d, want 1", stats.Inflight)
	}
	if stats.Acked != 1 {
		t.Errorf("Acked = %d, want 1", stats.Acked)
	}
	if stats.Total != 3 {
		t.Errorf("Total = %d, want 3", stats.Total)
	}

	// Stats across all events (empty eventName) sees the same documents here.
	all, err := store.GetStats(ctx, "")
	if err != nil {
		t.Fatalf("GetStats(all): %v", err)
	}
	if all.Total != 3 {
		t.Errorf("GetStats(all).Total = %d, want 3", all.Total)
	}
}

// TestIntegration_Store_Purge removes acked messages older than the given age
// while leaving recent and non-acked messages in place.
func TestIntegration_Store_Purge(t *testing.T) {
	db := setupPersistentDB(t)
	store := newStoreForTest(t, db)
	ctx := context.Background()

	const event = "purgeable"

	// One acked message, then a pending one left untouched.
	ackedSeq, err := store.Append(ctx, event, []byte("old"))
	if err != nil {
		t.Fatalf("Append acked: %v", err)
	}
	if err := store.Ack(ctx, event, ackedSeq); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if _, err := store.Append(ctx, event, []byte("keep")); err != nil {
		t.Fatalf("Append pending: %v", err)
	}

	// Purge with a generous future age (negative cutoff) removes nothing yet,
	// because acked_at is "now" which is not older than now+1h.
	removed, err := store.Purge(ctx, -time.Hour)
	if err != nil {
		t.Fatalf("Purge (future cutoff): %v", err)
	}
	if removed != 0 {
		t.Errorf("Purge with future cutoff removed %d, want 0", removed)
	}

	// Allow a small margin so the acked_at timestamp is comfortably in the past.
	time.Sleep(50 * time.Millisecond)

	// Purge anything acked older than 0 (i.e. before now) -> removes the acked one.
	removed, err = store.Purge(ctx, 0)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if removed != 1 {
		t.Errorf("Purge removed %d, want 1", removed)
	}

	stats, err := store.GetStats(ctx, event)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Acked != 0 {
		t.Errorf("Acked after purge = %d, want 0", stats.Acked)
	}
	if stats.Pending != 1 {
		t.Errorf("Pending after purge = %d, want 1 (pending message preserved)", stats.Pending)
	}
}

// TestIntegration_Store_TTLIndex verifies the TTL index is created when a TTL
// is configured (the store exposes 4 index models, and the collection reports
// a TTL index via expireAfterSeconds).
func TestIntegration_Store_TTLIndex(t *testing.T) {
	db := setupPersistentDB(t)
	store := newStoreForTest(t, db, WithTTL(48*time.Hour))
	ctx := context.Background()

	if got := len(store.Indexes()); got != 4 {
		t.Fatalf("Indexes() with TTL returned %d models, want 4", got)
	}

	cursor, err := store.Collection().Indexes().List(ctx)
	if err != nil {
		t.Fatalf("Indexes().List: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	foundTTL := false
	for cursor.Next(ctx) {
		var idx struct {
			ExpireAfterSeconds *int32 `bson:"expireAfterSeconds"`
		}
		if err := cursor.Decode(&idx); err != nil {
			t.Fatalf("decode index: %v", err)
		}
		if idx.ExpireAfterSeconds != nil {
			foundTTL = true
		}
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("iterate indexes: %v", err)
	}
	if !foundTTL {
		t.Error("expected a TTL index (expireAfterSeconds) on the collection")
	}
}

// TestIntegration_Store_AckErrors covers the sentinel errors for invalid or
// missing sequence ids on Ack and Nack.
func TestIntegration_Store_AckErrors(t *testing.T) {
	db := setupPersistentDB(t)
	store := newStoreForTest(t, db)
	ctx := context.Background()

	const event = "errs"

	if err := store.Ack(ctx, event, "not-a-valid-objectid"); err == nil {
		t.Error("Ack with invalid id: expected error, got nil")
	} else if !errors.Is(err, ErrInvalidSequenceID) {
		t.Errorf("Ack invalid id: error = %v, want ErrInvalidSequenceID", err)
	}

	if err := store.Nack(ctx, event, "still-not-valid"); err == nil {
		t.Error("Nack with invalid id: expected error, got nil")
	} else if !errors.Is(err, ErrInvalidSequenceID) {
		t.Errorf("Nack invalid id: error = %v, want ErrInvalidSequenceID", err)
	}

	// A well-formed but non-existent ObjectID hex yields ErrMessageNotFound.
	const missing = "507f1f77bcf86cd799439011"
	if err := store.Ack(ctx, event, missing); err == nil {
		t.Error("Ack missing id: expected error, got nil")
	} else if !errors.Is(err, ErrMessageNotFound) {
		t.Errorf("Ack missing id: error = %v, want ErrMessageNotFound", err)
	}
}

// TestIntegration_Checkpoint_RoundTrip covers Save/Load/Delete plus the missing
// key behavior.
func TestIntegration_Checkpoint_RoundTrip(t *testing.T) {
	db := setupPersistentDB(t)
	cp := newCheckpointStoreForTest(t, db)
	ctx := context.Background()

	const (
		event    = "orders"
		consumer = "consumer-1"
	)

	// Missing key returns the empty value, not an error.
	got, err := cp.Load(ctx, event, consumer)
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if got != "" {
		t.Errorf("Load missing = %q, want empty", got)
	}

	if err := cp.Save(ctx, event, consumer, "seq-100"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err = cp.Load(ctx, event, consumer)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "seq-100" {
		t.Errorf("Load = %q, want %q", got, "seq-100")
	}

	// Save again overwrites (upsert/replace).
	if err := cp.Save(ctx, event, consumer, "seq-200"); err != nil {
		t.Fatalf("Save overwrite: %v", err)
	}
	got, err = cp.Load(ctx, event, consumer)
	if err != nil {
		t.Fatalf("Load after overwrite: %v", err)
	}
	if got != "seq-200" {
		t.Errorf("Load after overwrite = %q, want %q", got, "seq-200")
	}

	if err := cp.Delete(ctx, event, consumer); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = cp.Load(ctx, event, consumer)
	if err != nil {
		t.Fatalf("Load after Delete: %v", err)
	}
	if got != "" {
		t.Errorf("Load after Delete = %q, want empty", got)
	}

	// Deleting a missing key is a no-op (no error).
	if err := cp.Delete(ctx, event, "ghost"); err != nil {
		t.Errorf("Delete missing key: %v", err)
	}
}

// TestIntegration_Checkpoint_ListAndDeleteByEvent covers List returning the
// consumer->checkpoint map and DeleteByEvent removing all consumers for an event.
func TestIntegration_Checkpoint_ListAndDeleteByEvent(t *testing.T) {
	db := setupPersistentDB(t)
	cp := newCheckpointStoreForTest(t, db)
	ctx := context.Background()

	const eventA = "orders"
	const eventB = "shipments"

	saves := []struct {
		event, consumer, checkpoint string
	}{
		{eventA, "c1", "a1"},
		{eventA, "c2", "a2"},
		{eventB, "c1", "b1"},
	}
	for _, s := range saves {
		if err := cp.Save(ctx, s.event, s.consumer, s.checkpoint); err != nil {
			t.Fatalf("Save %v: %v", s, err)
		}
	}

	listA, err := cp.List(ctx, eventA)
	if err != nil {
		t.Fatalf("List(%s): %v", eventA, err)
	}
	want := map[string]string{"c1": "a1", "c2": "a2"}
	if len(listA) != len(want) {
		t.Fatalf("List(%s) = %v, want %v", eventA, listA, want)
	}
	for k, v := range want {
		if listA[k] != v {
			t.Errorf("List(%s)[%q] = %q, want %q", eventA, k, listA[k], v)
		}
	}

	// List across all events (empty eventName) sees every checkpoint. Both
	// events use consumer "c1"; the map is keyed by consumer only, so it has
	// at most one entry per distinct consumer id.
	listAll, err := cp.List(ctx, "")
	if err != nil {
		t.Fatalf("List(all): %v", err)
	}
	if _, ok := listAll["c2"]; !ok {
		t.Errorf("List(all) missing consumer c2: %v", listAll)
	}

	// DeleteByEvent removes both consumers for eventA and reports the count.
	n, err := cp.DeleteByEvent(ctx, eventA)
	if err != nil {
		t.Fatalf("DeleteByEvent: %v", err)
	}
	if n != 2 {
		t.Errorf("DeleteByEvent count = %d, want 2", n)
	}

	listA, err = cp.List(ctx, eventA)
	if err != nil {
		t.Fatalf("List(%s) after delete: %v", eventA, err)
	}
	if len(listA) != 0 {
		t.Errorf("List(%s) after DeleteByEvent = %v, want empty", eventA, listA)
	}

	// eventB is untouched.
	listB, err := cp.List(ctx, eventB)
	if err != nil {
		t.Fatalf("List(%s): %v", eventB, err)
	}
	if listB["c1"] != "b1" {
		t.Errorf("List(%s)[c1] = %q, want %q", eventB, listB["c1"], "b1")
	}
}
