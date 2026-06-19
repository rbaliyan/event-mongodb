// Package store provides a generic MongoDB CRUD store for event-related
// documents.
//
// MongoStore[T] implements the CoreStore and CleanableStore interfaces for any
// document type T that satisfies the MongoDocument constraint (an
// evtstore.Identifier with a GetCreatedAt method). It supports create, get,
// update, upsert, delete, count, time-filtered listing, and keyset
// (cursor-based) pagination backed by a MongoDB collection.
//
// # Constraint
//
// T must implement MongoDocument:
//
//	type MongoDocument interface {
//	    evtstore.Identifier // GetID() string
//	    GetCreatedAt() time.Time
//	}
//
// The store sorts and paginates on (created_at, _id), so both fields must be
// populated on stored documents.
//
// # Pagination
//
// List returns a Page[T] whose NextCursor is non-empty when more results
// remain. Pass it back via Filter.Cursor to fetch the next page; the cursor
// encodes the last (created_at, _id) pair, giving stable keyset pagination that
// does not skip or repeat rows under concurrent writes. Set Filter.OrderDesc to
// page newest-first.
//
// # Field customization
//
// By default documents are keyed by "_id" and ordered by "created_at". Override
// these with WithMongoIDField and WithMongoCreatedAtField when your schema uses
// different names; WithMongoDefaultLimit sets the page size used when a Filter
// omits Limit (default 100).
//
// # Example
//
//	s := store.NewMongoStore[OrderDoc](collection,
//	    store.WithMongoDefaultLimit[OrderDoc](50),
//	)
//	if err := s.EnsureIndexes(ctx); err != nil { // (created_at) and (created_at,_id)
//	    return err
//	}
//	if err := s.Create(ctx, order); err != nil { // errors.Is(err, evtstore.ErrAlreadyExists) on dup
//	    return err
//	}
//	page, err := s.List(ctx, evtstore.Filter{Limit: 50})
package store
