// Package transaction provides a MongoDB implementation of the transaction
// manager interface for coordinating multi-document atomic operations.
//
// MongoManager wraps the MongoDB client to expose Begin, ExecuteWithContext,
// and related methods that integrate with the event library's transaction
// abstractions. MongoDB transactions require a replica set or sharded cluster;
// standalone deployments are not supported.
//
// Use MongoSessionProvider to access the session-embedded context when
// performing MongoDB operations inside a manually managed transaction:
//
//	mgr, _ := transaction.NewMongoManager(client)
//	tx, _ := mgr.Begin(ctx)
//	provider := tx.(transaction.MongoSessionProvider)
//	col.InsertOne(provider.Context(), doc)
//	tx.Commit()
package transaction
