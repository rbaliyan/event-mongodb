package distributed

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// lazyDatabase returns a *mongo.Database backed by a lazily-connected client.
// No server round-trips occur, so this is safe for pure unit tests.
func lazyDatabase(t *testing.T) *mongo.Database {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Fatalf("lazy connect: %v", err)
	}
	return client.Database("unit_test_db")
}

func TestNewMongoStateManager_NilDB(t *testing.T) {
	s, err := NewMongoStateManager(nil)
	if err == nil {
		t.Fatal("expected error for nil db, got nil")
	}
	if s != nil {
		t.Fatalf("expected nil manager on error, got %v", s)
	}
}

func TestNewMongoStateManager_Defaults(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStateManager(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil manager")
	}
	if s.completionTTL != 24*time.Hour {
		t.Fatalf("expected default completionTTL 24h, got %v", s.completionTTL)
	}
	if s.capped {
		t.Fatal("expected capped false by default")
	}
	if s.instanceID != "" {
		t.Fatalf("expected empty instanceID, got %q", s.instanceID)
	}
	if s.collection == nil {
		t.Fatal("expected collection to be set")
	}
	if s.collection.Name() != defaultStateCollection {
		t.Fatalf("expected default collection %q, got %q", defaultStateCollection, s.collection.Name())
	}
}

func TestWithCompletedTTL(t *testing.T) {
	db := lazyDatabase(t)

	s, err := NewMongoStateManager(db, WithCompletedTTL(2*time.Hour))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.completionTTL != 2*time.Hour {
		t.Fatalf("expected completionTTL 2h, got %v", s.completionTTL)
	}

	// Non-positive values are ignored; default retained.
	s2, err := NewMongoStateManager(db, WithCompletedTTL(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s2.completionTTL != 24*time.Hour {
		t.Fatalf("expected default 24h when ttl<=0, got %v", s2.completionTTL)
	}

	s3, err := NewMongoStateManager(db, WithCompletedTTL(-time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s3.completionTTL != 24*time.Hour {
		t.Fatalf("expected default 24h when ttl negative, got %v", s3.completionTTL)
	}
}

func TestWithCollection(t *testing.T) {
	db := lazyDatabase(t)

	s, err := NewMongoStateManager(db, WithCollection("custom_state"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.collection.Name() != "custom_state" {
		t.Fatalf("expected collection custom_state, got %q", s.collection.Name())
	}

	// Empty name is ignored; default retained.
	s2, err := NewMongoStateManager(db, WithCollection(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s2.collection.Name() != defaultStateCollection {
		t.Fatalf("expected default collection when name empty, got %q", s2.collection.Name())
	}
}

func TestWithInstanceID(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStateManager(db, WithInstanceID("node-7"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.instanceID != "node-7" {
		t.Fatalf("expected instanceID node-7, got %q", s.instanceID)
	}
}

func TestWithCapped(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStateManager(db, WithCapped(1024, 100))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !s.capped {
		t.Fatal("expected capped true")
	}
	if s.cappedSize != 1024 {
		t.Fatalf("expected cappedSize 1024, got %d", s.cappedSize)
	}
	if s.cappedMaxDocs != 100 {
		t.Fatalf("expected cappedMaxDocs 100, got %d", s.cappedMaxDocs)
	}
}

// TestWithCapped_NonPositiveSizeIgnored verifies that a non-positive size is
// ignored and capped mode is NOT enabled (a capped collection requires a size),
// matching the validation style of the other options.
func TestWithCapped_NonPositiveSizeIgnored(t *testing.T) {
	db := lazyDatabase(t)

	for _, size := range []int64{0, -1} {
		s, err := NewMongoStateManager(db, WithCapped(size, 0))
		if err != nil {
			t.Fatalf("size %d: unexpected error: %v", size, err)
		}
		if s.capped {
			t.Fatalf("size %d: expected capped to remain false", size)
		}
		if s.cappedSize != 0 {
			t.Fatalf("size %d: expected cappedSize 0, got %d", size, s.cappedSize)
		}
	}
}

func TestGenerateWorkerID(t *testing.T) {
	db := lazyDatabase(t)

	// No instance ID: bare nonce, unique across calls.
	s, err := NewMongoStateManager(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	a := s.generateWorkerID()
	b := s.generateWorkerID()
	if a == "" {
		t.Fatal("expected non-empty worker ID")
	}
	if a == b {
		t.Fatal("expected unique worker IDs across calls")
	}
	if len(a) != 24 { // 12 random bytes hex-encoded
		t.Fatalf("expected 24-char hex nonce, got %d (%q)", len(a), a)
	}

	// With instance ID: prefixed with "id:".
	s2, err := NewMongoStateManager(db, WithInstanceID("node-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id := s2.generateWorkerID()
	const prefix = "node-1:"
	if len(id) <= len(prefix) || id[:len(prefix)] != prefix {
		t.Fatalf("expected worker ID prefixed with %q, got %q", prefix, id)
	}
}

// indexKeySet returns the ordered key names of an index for comparison.
func indexKeySet(t *testing.T, idx mongo.IndexModel) []string {
	t.Helper()
	keys, ok := idx.Keys.(bson.D)
	if !ok {
		t.Fatalf("expected bson.D keys, got %T", idx.Keys)
	}
	out := make([]string, len(keys))
	for i, e := range keys {
		out[i] = e.Key
	}
	return out
}

// resolveExpireAfterSeconds applies the builder's option funcs to a concrete
// IndexOptions and returns the configured ExpireAfterSeconds value (or nil).
func resolveExpireAfterSeconds(t *testing.T, b *options.IndexOptionsBuilder) *int32 {
	t.Helper()
	io := &options.IndexOptions{}
	for _, fn := range b.List() {
		if err := fn(io); err != nil {
			t.Fatalf("apply index option: %v", err)
		}
	}
	return io.ExpireAfterSeconds
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIndexes_Regular(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStateManager(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	idx := s.Indexes()
	if len(idx) != 4 {
		t.Fatalf("expected 4 indexes, got %d", len(idx))
	}

	// First index is a TTL index on expires_at (ExpireAfterSeconds=0).
	if !equalStrings(indexKeySet(t, idx[0]), []string{"expires_at"}) {
		t.Fatalf("expected first index on expires_at, got %v", indexKeySet(t, idx[0]))
	}
	if idx[0].Options == nil {
		t.Fatal("expected regular expires_at index to have options")
	}
	if eas := resolveExpireAfterSeconds(t, idx[0].Options); eas == nil || *eas != 0 {
		t.Fatalf("expected regular expires_at index to be a TTL index (ExpireAfterSeconds=0), got %v", eas)
	}

	want := [][]string{
		{"expires_at"},
		{"status", "updated_at", "_id"},
		{"updated_at", "_id"},
		{"event_name", "updated_at", "_id"},
	}
	for i, w := range want {
		if got := indexKeySet(t, idx[i]); !equalStrings(got, w) {
			t.Fatalf("index %d keys = %v, want %v", i, got, w)
		}
	}
}

func TestIndexes_Capped(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStateManager(db, WithCapped(1024, 0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	idx := s.Indexes()
	if len(idx) != 4 {
		t.Fatalf("expected 4 indexes, got %d", len(idx))
	}

	// Capped collections cannot use TTL indexes: expires_at index must NOT
	// set ExpireAfterSeconds.
	if !equalStrings(indexKeySet(t, idx[0]), []string{"expires_at"}) {
		t.Fatalf("expected first index on expires_at, got %v", indexKeySet(t, idx[0]))
	}
	if idx[0].Options != nil {
		if eas := resolveExpireAfterSeconds(t, idx[0].Options); eas != nil {
			t.Fatal("capped expires_at index must not be a TTL index")
		}
	}

	want := [][]string{
		{"expires_at"},
		{"status", "updated_at", "_id"},
		{"updated_at", "_id"},
		{"event_name", "updated_at", "_id"},
	}
	for i, w := range want {
		if got := indexKeySet(t, idx[i]); !equalStrings(got, w) {
			t.Fatalf("index %d keys = %v, want %v", i, got, w)
		}
	}
}

func TestIsNamespaceExistsError(t *testing.T) {
	if isNamespaceExistsError(nil) {
		t.Fatal("nil error should not be a namespace-exists error")
	}
	if isNamespaceExistsError(mongo.CommandError{Code: 11000}) {
		t.Fatal("code 11000 should not be a namespace-exists error")
	}
	if !isNamespaceExistsError(mongo.CommandError{Code: 48}) {
		t.Fatal("code 48 should be a namespace-exists error")
	}
}
