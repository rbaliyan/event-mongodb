package monitor

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// materializeIndexOptions runs the v2 driver's IndexOptionsBuilder against a
// fresh IndexOptions value so tests can inspect the resulting fields directly.
func materializeIndexOptions(t *testing.T, b *options.IndexOptionsBuilder) options.IndexOptions {
	t.Helper()
	var out options.IndexOptions
	if b == nil {
		return out
	}
	for _, f := range b.Opts {
		if err := f(&out); err != nil {
			t.Fatalf("apply IndexOptions builder: %v", err)
		}
	}
	return out
}

// firstKey returns the first key name of a compound index, or "" if the keys
// are not a bson.D (defensive — all indexes in this package use bson.D).
func firstKey(t *testing.T, keys any) string {
	t.Helper()
	d, ok := keys.(bson.D)
	if !ok || len(d) == 0 {
		return ""
	}
	return d[0].Key
}

// TestMongoStore_Indexes_WithoutTTL verifies that no TTL index is added when
// WithTTL is not called. The default behavior must remain non-evictive so
// existing callers retain full control over retention via DeleteOlderThan.
func TestMongoStore_Indexes_WithoutTTL(t *testing.T) {
	s := &MongoStore{}
	idx := s.Indexes()

	for _, m := range idx {
		opts := materializeIndexOptions(t, m.Options)
		if opts.ExpireAfterSeconds != nil {
			t.Errorf("found unexpected TTL index on key %q before WithTTL was called", firstKey(t, m.Keys))
		}
	}
}

// TestMongoStore_Indexes_WithTTL verifies that WithTTL adds a TTL index on
// started_at with the configured expireAfterSeconds, and that the new index
// has an explicit name to coexist with the compound {started_at, event_id,
// subscription_id} index.
func TestMongoStore_Indexes_WithTTL(t *testing.T) {
	const ttl = 30 * 24 * time.Hour
	s := (&MongoStore{}).WithTTL(ttl)
	idx := s.Indexes()

	var found bool
	for _, m := range idx {
		opts := materializeIndexOptions(t, m.Options)
		if opts.ExpireAfterSeconds == nil {
			continue
		}
		if firstKey(t, m.Keys) != "started_at" {
			t.Errorf("TTL index should target started_at, got %q", firstKey(t, m.Keys))
		}
		// Index must be single-key to be a valid TTL index.
		if d, ok := m.Keys.(bson.D); ok && len(d) != 1 {
			t.Errorf("TTL index must be single-key, got %d keys", len(d))
		}
		if got, want := int64(*opts.ExpireAfterSeconds), int64(ttl.Seconds()); got != want {
			t.Errorf("ExpireAfterSeconds = %d, want %d", got, want)
		}
		if opts.Name == nil || *opts.Name == "" {
			t.Error("TTL index should have an explicit Name to coexist with the compound started_at index")
		}
		found = true
	}
	if !found {
		t.Error("expected a TTL index on started_at after WithTTL was called")
	}
}

// TestMongoStore_Indexes_WithTTL_ZeroIsNoOp verifies that WithTTL(0) (and
// negative values) clears the TTL configuration and does not add a TTL index.
func TestMongoStore_Indexes_WithTTL_ZeroIsNoOp(t *testing.T) {
	s := (&MongoStore{}).WithTTL(time.Hour).WithTTL(0)
	for _, m := range s.Indexes() {
		opts := materializeIndexOptions(t, m.Options)
		if opts.ExpireAfterSeconds != nil {
			t.Errorf("WithTTL(0) should clear TTL, but found TTL index on key %q", firstKey(t, m.Keys))
		}
	}
}
