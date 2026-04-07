package mongodb

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// CollectionFilterPipeline builds a MongoDB aggregation pipeline that filters
// change stream events to only include the specified database and collections.
// This is useful for cluster-level change streams that need to be scoped to
// specific namespaces.
//
// Returns an empty pipeline if database is empty or collections is empty.
//
// Example:
//
//	pipeline := mongodb.CollectionFilterPipeline("mydb", []string{"orders", "users"})
//	transport, _ := mongodb.NewClusterWatch(client, mongodb.WithPipeline(pipeline))
func CollectionFilterPipeline(database string, collections []string) mongo.Pipeline {
	if database == "" || len(collections) == 0 {
		return mongo.Pipeline{}
	}
	return mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "ns.db", Value: database},
			{Key: "ns.coll", Value: bson.D{
				{Key: "$in", Value: collections},
			}},
		}}},
	}
}
