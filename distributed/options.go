package distributed

import "time"

// Option configures a MongoStateManager.
type Option func(*stateOptions)

// stateOptions holds configuration for MongoStateManager.
type stateOptions struct {
	completionTTL  time.Duration
	instanceID     string
	collectionName string
	capped         bool
	cappedSize     int64
	cappedMaxDocs  int64
}

func defaultStateOptions() *stateOptions {
	return &stateOptions{
		completionTTL: 24 * time.Hour,
	}
}

// WithCompletedTTL sets how long to remember completed messages.
func WithCompletedTTL(ttl time.Duration) Option {
	return func(o *stateOptions) {
		if ttl > 0 {
			o.completionTTL = ttl
		}
	}
}

// WithCollection sets the MongoDB collection name for state storage.
// Default: "_message_state"
func WithCollection(name string) Option {
	return func(o *stateOptions) {
		if name != "" {
			o.collectionName = name
		}
	}
}

// WithInstanceID sets an instance identifier prepended to the worker ID on each Acquire call.
func WithInstanceID(id string) Option {
	return func(o *stateOptions) {
		o.instanceID = id
	}
}

// WithCapped enables MongoDB capped collection mode.
// sizeBytes is the max collection size; maxDocs is max number of docs (0 = unlimited).
func WithCapped(sizeBytes int64, maxDocs int64) Option {
	return func(o *stateOptions) {
		o.capped = true
		o.cappedSize = sizeBytes
		o.cappedMaxDocs = maxDocs
	}
}
