package mongodb

import (
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/bridge"
)

// DedupKeyFromChangeStream returns a [bridge.DedupKeyFn] that derives
// a stable deduplication key from MongoDB change stream metadata.
//
// The key is the tuple (cluster_time, namespace, document_key) which
// uniquely identifies a single change event across replay: two
// messages with the same cluster time, namespace, and document key
// represent the same logical change regardless of which bridge replica
// received them. This is the correct key for a [bridge.Dedup]
// middleware that deduplicates change-stream messages into a sink
// transport with consumer groups (Redis Streams, Kafka, NATS JetStream).
//
// Returns an empty string when any required metadata field is missing
// — for example when the message did not originate from this
// transport. An empty key causes [bridge.Dedup] to bypass dedup for
// that message, which is the correct fallback (the bridge forwards it
// unchanged rather than silently dropping it).
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
		md := msg.Metadata()
		if md == nil {
			return ""
		}
		ct := md[MetadataClusterTime]
		ns := md[MetadataNamespace]
		dk := md[MetadataDocumentKey]
		if ct == "" || ns == "" || dk == "" {
			return ""
		}
		return ct + ":" + ns + ":" + dk
	}
}
