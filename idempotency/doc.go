// Package idempotency provides a MongoDB-backed idempotency store for
// deduplicating event messages in distributed deployments.
//
// MongoStore tracks processed message IDs in MongoDB with configurable TTL,
// allowing multiple service instances to share a single deduplication state.
// Use IsDuplicate to check whether a message has already been processed, and
// MarkProcessed to record completion after successful handling.
//
// For single-instance deployments, prefer the in-memory store provided by
// github.com/rbaliyan/event/v3/idempotency.
package idempotency
