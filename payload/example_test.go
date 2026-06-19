package payload_test

import (
	"fmt"

	"github.com/rbaliyan/event-mongodb/payload"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ExampleBSON demonstrates a deterministic round-trip of a typed document
// through the BSON payload codec.
func ExampleBSON() {
	c := payload.BSON{}

	type order struct {
		ID     string `bson:"id"`
		Amount int    `bson:"amount"`
	}

	data, err := c.Encode(order{ID: "abc-123", Amount: 4200})
	if err != nil {
		fmt.Println("encode error:", err)
		return
	}

	var out order
	if err := c.Decode(data, &out); err != nil {
		fmt.Println("decode error:", err)
		return
	}

	fmt.Println("id:", out.ID)
	fmt.Println("amount:", out.Amount)

	// Output:
	// id: abc-123
	// amount: 4200
}

// ExampleBSON_map shows decoding into a map and reading a single field, keeping
// the output deterministic.
func ExampleBSON_map() {
	c := payload.BSON{}

	data, _ := c.Encode(bson.M{"name": "alice"})

	var out bson.M
	if err := c.Decode(data, &out); err != nil {
		fmt.Println("decode error:", err)
		return
	}

	fmt.Println("name:", out["name"])
	fmt.Println("content-type:", c.ContentType())

	// Output:
	// name: alice
	// content-type: application/bson
}
