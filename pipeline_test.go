package mongodb

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionFilterPipeline(t *testing.T) {
	t.Run("empty collections returns empty pipeline", func(t *testing.T) {
		pipeline := CollectionFilterPipeline("mydb", nil)
		if len(pipeline) != 0 {
			t.Errorf("Expected empty pipeline, got %d stages", len(pipeline))
		}

		pipeline = CollectionFilterPipeline("mydb", []string{})
		if len(pipeline) != 0 {
			t.Errorf("Expected empty pipeline, got %d stages", len(pipeline))
		}
	})

	t.Run("empty database returns empty pipeline", func(t *testing.T) {
		pipeline := CollectionFilterPipeline("", []string{"orders"})
		if len(pipeline) != 0 {
			t.Errorf("Expected empty pipeline for empty database, got %d stages", len(pipeline))
		}
	})

	t.Run("single collection returns correct $match", func(t *testing.T) {
		pipeline := CollectionFilterPipeline("mydb", []string{"orders"})
		if len(pipeline) != 1 {
			t.Fatalf("Expected 1 stage, got %d", len(pipeline))
		}
		stage := pipeline[0]
		if stage[0].Key != "$match" {
			t.Fatalf("Expected $match stage, got %s", stage[0].Key)
		}

		matchDoc := stage[0].Value.(bson.D)

		// Verify ns.db
		if matchDoc[0].Key != "ns.db" || matchDoc[0].Value != "mydb" {
			t.Errorf("Expected ns.db=mydb, got %s=%v", matchDoc[0].Key, matchDoc[0].Value)
		}

		// Verify ns.coll.$in
		collFilter := matchDoc[1].Value.(bson.D)
		if collFilter[0].Key != "$in" {
			t.Fatalf("Expected $in operator, got %s", collFilter[0].Key)
		}
		collections := collFilter[0].Value.([]string)
		if len(collections) != 1 || collections[0] != "orders" {
			t.Errorf("Expected [orders], got %v", collections)
		}
	})

	t.Run("multiple collections returns correct $match", func(t *testing.T) {
		pipeline := CollectionFilterPipeline("mydb", []string{"orders", "users"})
		if len(pipeline) != 1 {
			t.Fatalf("Expected 1 stage, got %d", len(pipeline))
		}

		matchDoc := pipeline[0][0].Value.(bson.D)

		// Verify ns.db
		if matchDoc[0].Key != "ns.db" || matchDoc[0].Value != "mydb" {
			t.Errorf("Expected ns.db=mydb, got %s=%v", matchDoc[0].Key, matchDoc[0].Value)
		}

		// Verify ns.coll.$in contains both collections
		collFilter := matchDoc[1].Value.(bson.D)
		collections := collFilter[0].Value.([]string)
		if len(collections) != 2 || collections[0] != "orders" || collections[1] != "users" {
			t.Errorf("Expected [orders, users], got %v", collections)
		}
	})
}
