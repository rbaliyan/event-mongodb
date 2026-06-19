package idempotency

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// lazyDatabase returns a *mongo.Database without contacting any server.
// mongo.Connect is lazy, so this is safe for pure unit tests as long as no
// method requiring a live server is invoked.
func lazyDatabase(t *testing.T) *mongo.Database {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27018/?directConnection=true"))
	if err != nil {
		t.Fatalf("unexpected error connecting (lazy) client: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(t.Context()) })
	return client.Database("idempotency_unit_test")
}

func TestNewMongoStore_NilDatabase(t *testing.T) {
	s, err := NewMongoStore(nil)
	if err == nil {
		t.Fatal("expected error for nil database, got nil")
	}
	if s != nil {
		t.Fatalf("expected nil store on error, got %v", s)
	}
}

func TestNewMongoStoreWithCollection_NilDatabase(t *testing.T) {
	s, err := NewMongoStoreWithCollection(nil, "custom")
	if err == nil {
		t.Fatal("expected error for nil database, got nil")
	}
	if s != nil {
		t.Fatalf("expected nil store on error, got %v", s)
	}
}

func TestNewMongoStoreWithCollection_EmptyName(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStoreWithCollection(db, "")
	if err == nil {
		t.Fatal("expected error for empty collection name, got nil")
	}
	if s != nil {
		t.Fatalf("expected nil store on error, got %v", s)
	}
}

func TestNewMongoStore_Defaults(t *testing.T) {
	db := lazyDatabase(t)
	// cleanupInterval 0 disables the background goroutine.
	s, err := NewMongoStore(db, WithMongoCleanupInterval(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.collectionName != "event_idempotency" {
		t.Errorf("collectionName = %q, want %q", s.collectionName, "event_idempotency")
	}
	if s.ttl != 24*time.Hour {
		t.Errorf("ttl = %v, want %v", s.ttl, 24*time.Hour)
	}
	if s.cleanupInterval != 0 {
		t.Errorf("cleanupInterval = %v, want 0", s.cleanupInterval)
	}
	if s.collection == nil {
		t.Error("collection should be initialized")
	}
	if s.collection.Name() != "event_idempotency" {
		t.Errorf("collection name = %q, want %q", s.collection.Name(), "event_idempotency")
	}
}

func TestNewMongoStore_DefaultsWithoutOptions(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStore(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A cleanup goroutine started with the default 1h interval; stop it.
	t.Cleanup(func() { _ = s.Close() })

	if s.cleanupInterval != time.Hour {
		t.Errorf("default cleanupInterval = %v, want %v", s.cleanupInterval, time.Hour)
	}
	if s.ttl != 24*time.Hour {
		t.Errorf("default ttl = %v, want %v", s.ttl, 24*time.Hour)
	}
}

func TestNewMongoStore_OptionsApply(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStore(db,
		WithMongoTTL(2*time.Hour),
		WithMongoCollection("custom_idem"),
		WithMongoCleanupInterval(0),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.ttl != 2*time.Hour {
		t.Errorf("ttl = %v, want %v", s.ttl, 2*time.Hour)
	}
	if s.collectionName != "custom_idem" {
		t.Errorf("collectionName = %q, want %q", s.collectionName, "custom_idem")
	}
	if s.collection.Name() != "custom_idem" {
		t.Errorf("collection name = %q, want %q", s.collection.Name(), "custom_idem")
	}
	if s.cleanupInterval != 0 {
		t.Errorf("cleanupInterval = %v, want 0", s.cleanupInterval)
	}
}

func TestNewMongoStoreWithCollection_OptionsApply(t *testing.T) {
	db := lazyDatabase(t)
	// The explicit collection name argument takes precedence; verify other
	// options still apply and the loop is disabled via interval 0.
	s, err := NewMongoStoreWithCollection(db, "explicit_coll",
		WithMongoTTL(15*time.Minute),
		WithMongoCleanupInterval(0),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.collectionName != "explicit_coll" {
		t.Errorf("collectionName = %q, want %q", s.collectionName, "explicit_coll")
	}
	if s.ttl != 15*time.Minute {
		t.Errorf("ttl = %v, want %v", s.ttl, 15*time.Minute)
	}
}

func TestMongoStore_CloseIdempotent(t *testing.T) {
	db := lazyDatabase(t)
	s, err := NewMongoStore(db, WithMongoCleanupInterval(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Close must be safe to call repeatedly without panicking on a
	// double-close of the stop channel.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
