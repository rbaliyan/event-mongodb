// Package outbox provides a MongoDB implementation of the transactional outbox
// pattern for reliable event publishing.
//
// MongoStore durably records outgoing events inside the same MongoDB transaction
// as the business write, ensuring events are never lost even if the publisher
// crashes before they are sent. MongoStore implements the backend-neutral
// contract from github.com/rbaliyan/event/v3/outbox (Store, StuckRecoverer,
// Starter, Waker), so it works with that package's generic Publisher and
// Relay rather than MongoDB-specific ones:
//
//	store, _ := outbox.NewMongoStore(db)
//	publisher := evtoutbox.NewPublisher(store)
//	relay := evtoutbox.NewRelay(store, transport,
//	    evtoutbox.WithPollDelay(100 * time.Millisecond),
//	    evtoutbox.WithBatchSize(100),
//	)
//	go relay.Start(ctx)
//
//	err := outbox.Transaction(ctx, mongoClient, func(txCtx context.Context) error {
//	    if _, err := ordersCol.InsertOne(txCtx, order); err != nil {
//	        return err
//	    }
//	    return publisher.Publish(txCtx, "order.created", order, metadata)
//	})
//
// Transaction (and TransactionWithOptions) wrap a MongoDB session and mark it
// on the context via event.WithOutboxTx, so store.Store — and therefore
// publisher.Publish — routes the insert into the same session/transaction as
// the business write.
//
// evtoutbox.Waker is implemented but currently poll-only: MongoStore.Notifications
// always returns nil, so the relay relies solely on its poll ticker rather than
// a change-stream-backed early wakeup. See MongoStore.Notifications for why.
package outbox
