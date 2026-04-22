// Package outbox provides a MongoDB implementation of the transactional outbox
// pattern for reliable event publishing.
//
// MongoStore durably records outgoing events inside the same MongoDB transaction
// as the business write, ensuring events are never lost even if the publisher
// crashes before they are sent. A relay (MongoRelay for polling, or
// NewChangeStreamRelay for real-time) reads pending entries and publishes them
// to the event transport.
//
// MongoPublisher wraps the store and client to expose a single
// PublishWithTransaction method that performs both the business write and the
// outbox insert atomically:
//
//	publisher, _ := outbox.NewMongoPublisher(client, db)
//	err := publisher.PublishWithTransaction(ctx, "order.created", payload, metadata,
//	    func(txCtx context.Context) error {
//	        _, err := ordersCol.InsertOne(txCtx, order)
//	        return err
//	    },
//	)
package outbox
