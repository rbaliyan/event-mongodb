// Package persistent provides MongoDB implementations of persistent.Store
// and persistent.CheckpointStore for use with the composite transport.
//
// This enables buffered event delivery combining:
//   - MongoDB for durable message storage (at-least-once delivery)
//   - A real-time transport (Redis, NATS) for low-latency notifications
//
// # Architecture
//
// The composite transport pattern works as follows:
//
//  1. Publish: Messages are written to MongoDB first (durable), then a
//     lightweight signal is sent via the real-time transport.
//
//  2. Subscribe: Consumers wait for signals (fast path) or poll MongoDB
//     (fallback path) to fetch and process messages.
//
//  3. Acknowledgment: Successfully processed messages are marked as "acked"
//     in MongoDB and optionally auto-deleted via TTL.
//
// # Quick Start
//
//	import (
//	    "github.com/rbaliyan/event/v3"
//	    "github.com/rbaliyan/event/v3/transport/composite"
//	    "github.com/rbaliyan/event/v3/transport/redis"
//	    mongopersistent "github.com/rbaliyan/event-mongodb/persistent"
//	)
//
//	func main() {
//	    // MongoDB for durable storage
//	    store, err := mongopersistent.NewStore(
//	        mongoClient.Database("events").Collection("messages"),
//	        mongopersistent.WithTTL(7*24*time.Hour), // Auto-delete acked messages after 7 days
//	    )
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    store.EnsureIndexes(ctx)
//
//	    // Optional: checkpoint store for resume after restart
//	    checkpointStore, err := mongopersistent.NewCheckpointStore(
//	        mongoClient.Database("events").Collection("checkpoints"),
//	    )
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    checkpointStore.EnsureIndexes(ctx)
//
//	    // Redis for real-time signals
//	    signal := redis.New(redisClient)
//
//	    // Composite transport: durability + low latency
//	    transport, _ := composite.New(store, signal,
//	        composite.WithCheckpointStore(checkpointStore),
//	        composite.WithPollInterval(5*time.Second),
//	    )
//
//	    // Create bus with the composite transport
//	    bus, _ := event.NewBus("orders", event.WithTransport(transport))
//	    defer bus.Close(ctx)
//
//	    // Use normally
//	    orderEvent := event.New[Order]("order.created")
//	    event.Register(ctx, bus, orderEvent)
//	    orderEvent.Publish(ctx, order)
//	}
//
// # Message Lifecycle
//
// Messages go through the following states:
//
//		pending -> inflight -> acked
//		    ^          |
//		    +---nack---+
//
//	  - pending: Message is stored and waiting to be processed
//	  - inflight: Message has been fetched and is being processed
//	  - acked: Message was successfully processed
//
// If a message is not acknowledged within the visibility timeout (default 5min),
// it returns to "pending" state and can be fetched again.
//
// # Visibility Timeout
//
// The visibility timeout prevents message loss when consumers crash:
//
//	store := mongopersistent.NewStore(collection,
//	    mongopersistent.WithVisibilityTimeout(10*time.Minute),
//	)
//
// Set this to longer than your maximum expected processing time.
//
// # Monitoring
//
// Use GetStats to monitor queue depth:
//
//	stats, _ := store.GetStats(ctx, "orders")
//	fmt.Printf("Pending: %d, Inflight: %d\n", stats.Pending, stats.Inflight)
//
// # Manual Cleanup
//
// If TTL is not configured, use Purge for manual cleanup:
//
//	deleted, _ := store.Purge(ctx, 30*24*time.Hour) // Delete acked messages older than 30 days
package persistent
