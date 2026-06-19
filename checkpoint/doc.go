// Package checkpoint provides a MongoDB implementation of the CheckpointStore interface.
//
// CheckpointStore persists the last-processed position for each subscriber,
// enabling consumers to resume from where they left off after a restart.
// Checkpoints are stored as MongoDB documents keyed by subscriber ID and
// support optional TTL-based expiry.
//
// # Constructor
//
// NewMongoStore wraps a *mongo.Collection. The constructor does no I/O; call
// EnsureIndexes once during startup to create the supporting indexes.
//
//	store, err := checkpoint.NewMongoStore(
//	    db.Collection("checkpoints"),
//	    checkpoint.WithMongoTTL(7*24*time.Hour), // optional server-side expiry
//	)
//	if err != nil {
//	    return err
//	}
//	if err := store.EnsureIndexes(ctx); err != nil {
//	    return err
//	}
//
// # Operations
//
//   - Save(ctx, subscriberID, position) records a subscriber's position.
//   - Load(ctx, subscriberID) returns the stored position (zero time if none).
//   - Delete / DeleteAll remove checkpoints.
//   - List returns the known subscriber IDs; GetAll returns every position.
//   - GetCheckpointInfo returns metadata for a single subscriber.
//
// # Options
//
//   - WithMongoTTL(d) adds a TTL index so MongoDB evicts checkpoints older than
//     d. Omit it (or pass a non-positive value) to retain checkpoints
//     indefinitely.
package checkpoint
