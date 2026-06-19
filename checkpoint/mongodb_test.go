package checkpoint

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// lazyCollection returns a *mongo.Collection backed by a lazily-connected
// client. No server round-trips occur, so this is safe for pure unit tests.
func lazyCollection(t *testing.T) *mongo.Collection {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Fatalf("lazy connect: %v", err)
	}
	return client.Database("unit_test_db").Collection("checkpoints")
}

func TestNewMongoStore_NilCollection(t *testing.T) {
	s, err := NewMongoStore(nil)
	if err == nil {
		t.Fatal("expected error for nil collection, got nil")
	}
	if s != nil {
		t.Fatalf("expected nil store on error, got %v", s)
	}
}

func TestNewMongoStore_Valid(t *testing.T) {
	coll := lazyCollection(t)
	s, err := NewMongoStore(coll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil store")
	}
	if s.collection != coll {
		t.Fatal("collection not assigned")
	}
	if s.ttl != 0 {
		t.Fatalf("expected default ttl 0, got %v", s.ttl)
	}
}

func TestWithMongoTTL(t *testing.T) {
	coll := lazyCollection(t)
	s, err := NewMongoStore(coll, WithMongoTTL(30*time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.ttl != 30*time.Minute {
		t.Fatalf("expected ttl 30m, got %v", s.ttl)
	}
}

func TestIndexes_NoTTL(t *testing.T) {
	s, err := NewMongoStore(lazyCollection(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := s.Indexes(); len(got) != 0 {
		t.Fatalf("expected no indexes without ttl, got %d", len(got))
	}
}

func TestIndexes_WithTTL(t *testing.T) {
	s, err := NewMongoStore(lazyCollection(t), WithMongoTTL(time.Hour))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	idx := s.Indexes()
	if len(idx) != 1 {
		t.Fatalf("expected 1 index with ttl, got %d", len(idx))
	}

	keys, ok := idx[0].Keys.(bson.D)
	if !ok {
		t.Fatalf("expected bson.D keys, got %T", idx[0].Keys)
	}
	if len(keys) != 1 || keys[0].Key != "updated_at" || keys[0].Value != 1 {
		t.Fatalf("unexpected index keys: %#v", keys)
	}

	if idx[0].Options == nil {
		t.Fatal("expected index options to be set")
	}
	resolved := resolveIndexOptions(t, idx[0].Options)
	if resolved.Name == nil || *resolved.Name != "checkpoint_ttl" {
		t.Fatalf("expected index name checkpoint_ttl, got %v", resolved.Name)
	}
	if resolved.ExpireAfterSeconds == nil || *resolved.ExpireAfterSeconds != 3600 {
		t.Fatalf("expected ExpireAfterSeconds 3600, got %v", resolved.ExpireAfterSeconds)
	}
}

// resolveIndexOptions applies a builder's option funcs to a concrete
// IndexOptions so the configured values can be inspected in unit tests.
func resolveIndexOptions(t *testing.T, b *options.IndexOptionsBuilder) *options.IndexOptions {
	t.Helper()
	io := &options.IndexOptions{}
	for _, fn := range b.List() {
		if err := fn(io); err != nil {
			t.Fatalf("apply index option: %v", err)
		}
	}
	return io
}
