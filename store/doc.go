// Package store provides a generic MongoDB CRUD store for event-related
// documents.
//
// MongoStore[T] implements the CoreStore interface for any document type T
// that satisfies the MongoDocument constraint (GetID and GetCreatedAt). It
// supports paginated listing, cursor-based pagination, count, create, update,
// upsert, and delete operations backed by a MongoDB collection.
//
// Example usage:
//
//	s := store.NewMongoStore[OrderDoc](collection,
//	    store.WithMongoDefaultLimit[OrderDoc](50),
//	)
//	s.EnsureIndexes(ctx)
//	s.Create(ctx, order)
package store
