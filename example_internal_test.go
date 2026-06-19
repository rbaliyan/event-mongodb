package mongodb

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ExampleTransport_ResumeTokenKey shows the deterministic key format used to
// store resume tokens. A cluster-level watch with a fixed resume token ID
// always produces the same key.
func ExampleTransport_ResumeTokenKey() {
	tr := &Transport{
		transportOptions: transportOptions{resumeTokenID: "host-1"},
		level:            watchLevelCluster,
	}
	fmt.Println(tr.ResumeTokenKey())
	// Output: *.*:host-1
}

// Example_extractChangeEvent is a golden example showing how a raw MongoDB
// change document is decoded into a ChangeEvent. It uses a string _id so the
// document key is stable across runs.
func Example_extractChangeEvent() {
	tr := &Transport{transportOptions: transportOptions{collectionName: "orders", logger: discardLogger()}}

	raw := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "resume-123"}}},
		{Key: "operationType", Value: "update"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: "order-7"}}},
		{Key: "updateDescription", Value: bson.D{
			{Key: "updatedFields", Value: bson.D{{Key: "status", Value: "shipped"}}},
			{Key: "removedFields", Value: bson.A{"hold"}},
		}},
	}

	data, _ := bson.Marshal(raw)
	var doc changeStreamDoc
	_ = bson.Unmarshal(data, &doc)

	ev := tr.extractChangeEvent(doc)
	fmt.Println("id:", ev.ID)
	fmt.Println("operation:", ev.OperationType)
	fmt.Println("namespace:", ev.Namespace)
	fmt.Println("document_key:", ev.DocumentKey)
	fmt.Println("status:", ev.UpdateDesc.UpdatedFields["status"])
	fmt.Println("removed:", ev.UpdateDesc.RemovedFields)
	// Output:
	// id: resume-123
	// operation: update
	// namespace: shop.orders
	// document_key: order-7
	// status: shipped
	// removed: [hold]
}

// ExampleEventType_String shows the deterministic string mapping of event types.
func ExampleEventType_String() {
	fmt.Println(EventCreated)
	fmt.Println(EventUpdated)
	fmt.Println(EventDeleted)
	// Output:
	// created
	// updated
	// deleted
}

// ExampleEventType_Operations shows the change-stream operations each event
// type maps to.
func ExampleEventType_Operations() {
	fmt.Println(EventUpdated.Operations())
	// Output: [update replace]
}

// ExampleField demonstrates typed extraction with numeric coercion. A value
// stored as a BSON int32 is read back as an int64.
func ExampleField() {
	desc := &UpdateDescription{
		UpdatedFields: map[string]any{
			"status": "shipped",
			"count":  int32(3),
		},
	}

	status, _ := Field[string](desc, "status")
	count, _ := Field[int64](desc, "count")
	fmt.Println(status, count)
	// Output: shipped 3
}
