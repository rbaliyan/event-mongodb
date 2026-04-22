// Package checkpoint provides a MongoDB implementation of the CheckpointStore interface.
//
// CheckpointStore persists the last-processed position for each subscriber,
// enabling consumers to resume from where they left off after a restart.
// Checkpoints are stored as MongoDB documents keyed by subscriber ID and
// support optional TTL-based expiry.
package checkpoint
