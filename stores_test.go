package mongodb

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

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
