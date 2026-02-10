package mongodb

import (
	event "github.com/rbaliyan/event/v3"
)

// CoalesceByDocumentKey returns a subscribe option that coalesces change
// events by their MongoDB document _id (extracted from message metadata).
//
// When multiple changes arrive for the same document while the handler is
// processing, only the latest change is delivered. Superseded messages are
// auto-acknowledged.
//
// This uses metadata-based coalescing, which is more efficient than
// payload-based coalescing because it avoids decoding messages that will
// be superseded.
//
// Ideal for:
//   - Cache invalidation
//   - Materialized view updates
//   - Any subscriber that only needs the current document state
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler,
//	    mongodb.CoalesceByDocumentKey[Order](),
//	)
//
//	// Combined with best-effort delivery:
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithBestEffort[Order](),
//	    mongodb.CoalesceByDocumentKey[Order](),
//	)
func CoalesceByDocumentKey[T any]() event.SubscribeOption[T] {
	return event.WithCoalesceByMetadata[T](MetadataDocumentKey)
}
