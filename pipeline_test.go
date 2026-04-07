package mongodb

import "testing"

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

	t.Run("single collection returns one $match stage", func(t *testing.T) {
		pipeline := CollectionFilterPipeline("mydb", []string{"orders"})
		if len(pipeline) != 1 {
			t.Fatalf("Expected 1 stage, got %d", len(pipeline))
		}
		stage := pipeline[0]
		if stage[0].Key != "$match" {
			t.Errorf("Expected $match stage, got %s", stage[0].Key)
		}
	})

	t.Run("multiple collections returns one $match stage", func(t *testing.T) {
		pipeline := CollectionFilterPipeline("mydb", []string{"orders", "users", "products"})
		if len(pipeline) != 1 {
			t.Fatalf("Expected 1 stage, got %d", len(pipeline))
		}
		stage := pipeline[0]
		if stage[0].Key != "$match" {
			t.Errorf("Expected $match stage, got %s", stage[0].Key)
		}
	})
}
