package mongodb

import (
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/bridge"
)

// DedupKeyFromChangeStream returns a [bridge.DedupKeyFn] that derives
// a stable deduplication key from the MongoDB change stream event ID.
//
// The key is the message ID, which is set to the resume token _data field
// (doc._id._data) by the MongoDB transport. This field is the hex-encoded
// oplog position and is deterministic: all bridge replicas watching the same
// change stream will produce the same _data value for the same change event,
// making it the correct deduplication key across replicas.
//
// Using the resume token is more precise than a (cluster_time, namespace,
// document_key) tuple because the MongoDB cluster time has only second-level
// precision in the transport metadata; two updates to the same document within
// the same second would otherwise share an identical tuple and cause one event
// to be silently dropped.
//
// Returns an empty string when the message is nil or has no ID, which causes
// [bridge.Dedup] to bypass dedup for that message (forward unchanged).
//
// Example:
//
//	source, _ := mongodb.NewClusterWatch(mongoClient, mongodb.WithPipeline(p))
//	sink, _   := redis.New(redisClient, redis.WithConsumerGroup("my-svc"))
//
//	t, _ := bridge.New(source, sink,
//	    bridge.WithMiddleware(
//	        bridge.Dedup(coord, mongodb.DedupKeyFromChangeStream(), 24*time.Hour),
//	    ),
//	)
func DedupKeyFromChangeStream() bridge.DedupKeyFn {
	return func(msg transport.Message) string {
		if msg == nil {
			return ""
		}
		// The message ID equals doc._id._data — the MongoDB resume token, which
		// encodes the exact oplog position of this change event. It is unique
		// per event and identical across all replicas watching the same stream.
		return msg.ID()
	}
}
