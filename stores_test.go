package mongodb

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

func TestAckFilter_Defaults(t *testing.T) {
	filter := AckFilter{}

	if filter.Limit != 0 {
		t.Errorf("expected default Limit 0, got %d", filter.Limit)
	}
	if filter.Offset != 0 {
		t.Errorf("expected default Offset 0, got %d", filter.Offset)
	}
	if filter.Status != "" {
		t.Errorf("expected empty Status, got %q", filter.Status)
	}
}

func TestAckEntry_Fields(t *testing.T) {
	now := time.Now()
	entry := AckEntry{
		EventID:   "evt-123",
		CreatedAt: now,
		AckedAt:   time.Time{},
	}

	if entry.EventID != "evt-123" {
		t.Errorf("expected EventID evt-123, got %s", entry.EventID)
	}
	if entry.AckedAt.IsZero() != true {
		t.Error("expected AckedAt to be zero (pending)")
	}
}

func TestAckStatus_Constants(t *testing.T) {
	if AckStatusPending != "pending" {
		t.Errorf("expected pending, got %s", AckStatusPending)
	}
	if AckStatusAcked != "acked" {
		t.Errorf("expected acked, got %s", AckStatusAcked)
	}
}

func TestBuildAckFilter_Empty(t *testing.T) {
	filter := AckFilter{}
	f := buildAckFilter(filter)

	if len(f) != 0 {
		t.Errorf("expected empty filter, got %v", f)
	}
}

func TestBuildAckFilter_Pending(t *testing.T) {
	filter := AckFilter{Status: AckStatusPending}
	f := buildAckFilter(filter)

	ackedAt, ok := f["acked_at"]
	if !ok {
		t.Fatal("expected acked_at filter")
	}
	m := ackedAt.(bson.M)
	if _, ok := m["$eq"]; !ok {
		t.Error("expected $eq for pending status")
	}
}

func TestBuildAckFilter_Acked(t *testing.T) {
	filter := AckFilter{Status: AckStatusAcked}
	f := buildAckFilter(filter)

	ackedAt, ok := f["acked_at"]
	if !ok {
		t.Fatal("expected acked_at filter")
	}
	m := ackedAt.(bson.M)
	if _, ok := m["$gt"]; !ok {
		t.Error("expected $gt for acked status")
	}
}

func TestBuildAckFilter_TimeRange(t *testing.T) {
	now := time.Now()
	filter := AckFilter{
		StartTime: now.Add(-1 * time.Hour),
		EndTime:   now,
	}
	f := buildAckFilter(filter)

	createdAt, ok := f["created_at"]
	if !ok {
		t.Fatal("expected created_at filter")
	}
	m := createdAt.(bson.M)
	if _, ok := m["$gte"]; !ok {
		t.Error("expected $gte for StartTime")
	}
	if _, ok := m["$lt"]; !ok {
		t.Error("expected $lt for EndTime")
	}
}

func TestBuildAckFilter_Combined(t *testing.T) {
	now := time.Now()
	filter := AckFilter{
		Status:    AckStatusPending,
		StartTime: now.Add(-1 * time.Hour),
	}
	f := buildAckFilter(filter)

	if _, ok := f["acked_at"]; !ok {
		t.Error("expected acked_at filter for status")
	}
	if _, ok := f["created_at"]; !ok {
		t.Error("expected created_at filter for time range")
	}
}

// Compile-time check that MongoAckStore implements AckQueryStore.
var _ AckQueryStore = (*MongoAckStore)(nil)

// TestMongoAckStore_PendingCount_Initial verifies the in-memory pending counter
// starts at zero on a freshly constructed store.
func TestMongoAckStore_PendingCount_Initial(t *testing.T) {
	s := &MongoAckStore{}
	if got := s.PendingCount(); got != 0 {
		t.Errorf("PendingCount() = %d, want 0", got)
	}
}

// TestMongoAckStore_Indexes_HasPendingPartial verifies that Indexes() returns a
// partial index covering only un-acked documents (acked_at == epoch). Without
// it, AckFilter{Status: AckStatusPending} counts/lists fall back to a full
// collection scan.
func TestMongoAckStore_Indexes_HasPendingPartial(t *testing.T) {
	s := &MongoAckStore{ttl: time.Hour}
	idx := s.Indexes()

	var found bool
	for _, m := range idx {
		opts := materializeIndexOptions(t, m.Options)
		// The pending index has no TTL but has a partialFilterExpression
		// equal to {acked_at: {$eq: epoch}}.
		if opts.ExpireAfterSeconds != nil {
			continue
		}
		if opts.PartialFilterExpression == nil {
			continue
		}
		raw, ok := opts.PartialFilterExpression.(bson.M)
		if !ok {
			continue
		}
		ackedAt, ok := raw["acked_at"].(bson.M)
		if !ok {
			continue
		}
		v, ok := ackedAt["$eq"].(time.Time)
		if !ok || !v.IsZero() {
			continue
		}
		found = true
		if opts.Name == nil || *opts.Name == "" {
			t.Error("pending partial index should have an explicit Name to avoid IndexOptionsConflict with the TTL index")
		}
	}
	if !found {
		t.Error("expected a partial index on acked_at with filter {acked_at: {$eq: epoch}} for pending lookups")
	}
}

// TestMongoAckStore_Indexes_PendingAndTTLNamesDiffer verifies the pending and
// TTL indexes share the same key pattern but have distinct names so MongoDB
// will accept both on the same collection.
func TestMongoAckStore_Indexes_PendingAndTTLNamesDiffer(t *testing.T) {
	s := &MongoAckStore{ttl: time.Hour}
	idx := s.Indexes()

	var ackedAtNames []string
	for _, m := range idx {
		// Filter to indexes whose first (and only) key is acked_at.
		keys, ok := m.Keys.(bson.D)
		if !ok || len(keys) != 1 || keys[0].Key != "acked_at" {
			continue
		}
		opts := materializeIndexOptions(t, m.Options)
		name := ""
		if opts.Name != nil {
			name = *opts.Name
		}
		ackedAtNames = append(ackedAtNames, name)
	}

	if len(ackedAtNames) < 2 {
		t.Fatalf("expected >= 2 indexes on acked_at, got %d (%v)", len(ackedAtNames), ackedAtNames)
	}

	seen := make(map[string]bool, len(ackedAtNames))
	for _, n := range ackedAtNames {
		// MongoDB auto-generates a name for empty Name, so empty is a valid
		// distinct name. But two empties would collide, so empties count once.
		if seen[n] {
			t.Errorf("duplicate index name on acked_at: %q", n)
		}
		seen[n] = true
	}
}
