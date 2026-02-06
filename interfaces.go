package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ResumeTokenStore persists resume tokens for reliable change stream resumption.
// Implement this interface to store tokens in MongoDB, Redis, or another backend.
type ResumeTokenStore interface {
	// Load retrieves the last resume token for a collection.
	// Returns nil if no token exists.
	Load(ctx context.Context, collection string) (bson.Raw, error)

	// Save persists a resume token for a collection.
	Save(ctx context.Context, collection string, token bson.Raw) error
}

// AckStatus represents the acknowledgment state.
type AckStatus string

const (
	// AckStatusPending represents events that have not been acknowledged.
	AckStatusPending AckStatus = "pending"
	// AckStatusAcked represents events that have been acknowledged.
	AckStatusAcked AckStatus = "acked"
)

// AckFilter specifies criteria for listing ack entries.
// All fields are optional. Empty filter returns all entries.
type AckFilter struct {
	Status    AckStatus // pending, acked, or empty for all
	StartTime time.Time // Created after this time (inclusive)
	EndTime   time.Time // Created before this time (exclusive)
	Limit     int       // Max results (0 = default 100)
	Offset    int       // Offset for pagination
}

// AckEntry represents an acknowledgment record.
type AckEntry struct {
	EventID   string
	CreatedAt time.Time
	AckedAt   time.Time // Zero value = pending
}

// AckStore tracks event acknowledgments for at-least-once delivery.
// Use this to prevent reprocessing of acknowledged events on restart.
type AckStore interface {
	// Store records that an event was received but not yet processed.
	// Returns nil if already exists (idempotent for retries).
	Store(ctx context.Context, eventID string) error

	// Ack marks an event as successfully processed.
	// Call this after your handler completes successfully.
	Ack(ctx context.Context, eventID string) error
}

// AckQueryStore extends AckStore with list and count capabilities.
// Implementations may optionally satisfy this interface to provide
// monitoring and debugging features.
type AckQueryStore interface {
	AckStore

	// List returns ack entries matching the filter.
	List(ctx context.Context, filter AckFilter) ([]AckEntry, error)

	// Count returns the number of entries matching the filter.
	Count(ctx context.Context, filter AckFilter) (int64, error)
}
