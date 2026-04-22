// Package schema provides a MongoDB-backed implementation of the event schema
// provider for persisting and retrieving event schema definitions.
//
// MongoProvider stores EventSchema records in a MongoDB collection and
// optionally invokes a publisher callback whenever a schema is created or
// updated. This enables schema changes to propagate to other services via the
// event bus.
//
// Use NewMongoProvider to create a provider and EnsureIndexes to set up the
// required indexes before use:
//
//	provider, _ := schema.NewMongoProvider(db, publisherFn)
//	provider = provider.WithCollection("event_schemas")
//	provider.EnsureIndexes(ctx)
package schema
