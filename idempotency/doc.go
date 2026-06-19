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
//
// # Constructors
//
//   - NewMongoStore(db, ...) uses the default collection ("event_idempotency").
//   - NewMongoStoreWithCollection(db, name, ...) uses a custom collection.
//
// Both start a background cleanup loop (see WithMongoCleanupInterval) that
// removes expired entries as a fallback for the server-side TTL index; call
// Close to stop it.
//
//	store, err := idempotency.NewMongoStore(db,
//	    idempotency.WithMongoTTL(48*time.Hour),
//	)
//	if err != nil {
//	    return err
//	}
//	defer store.Close()
//	if err := store.EnsureIndexes(ctx); err != nil { // creates the TTL index
//	    return err
//	}
//
// # Operations
//
//   - IsDuplicate / MarkProcessed / MarkProcessedWithTTL for the common path.
//   - IsDuplicateTx / MarkProcessedTx / MarkProcessedWithTTLTx are transaction
//     variants: in the v2 driver the session travels via the context, so these
//     behave identically to their non-Tx counterparts but signal transactional
//     intent at the call site.
//   - Remove deletes a single record; Collection exposes the underlying handle.
//
// # Options
//
//   - WithMongoTTL(d): retention for processed IDs (default 24h).
//   - WithMongoCollection(name): override the collection name.
//   - WithMongoCleanupInterval(d): background cleanup cadence; 0 disables the
//     loop (rely on the server-side TTL index instead).
package idempotency
