// Package distributed provides a MongoDB-backed state manager for emulating
// WorkerPool semantics over MongoDB change streams.
//
// MongoDB change streams are Broadcast-only: every subscriber receives every
// change. MongoStateManager uses atomic findOneAndUpdate operations to claim
// each message so that only one worker processes it, providing the same
// at-most-once-per-worker guarantee as a true WorkerPool transport.
//
// Use WorkerPoolMiddleware from github.com/rbaliyan/event/v3/distributed to
// wire the state manager into your subscriber:
//
//	claimer, _ := distributed.NewMongoStateManager(db,
//	    distributed.WithCollection("_message_state"),
//	    distributed.WithCompletedTTL(24*time.Hour),
//	)
//	claimer.EnsureIndexes(ctx)
//
//	orderChanges.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        evtdistributed.WorkerPoolMiddleware[mongodb.ChangeEvent](claimer, 5*time.Minute),
//	    ),
//	)
package distributed
