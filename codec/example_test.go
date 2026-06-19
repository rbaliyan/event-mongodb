package codec_test

import (
	"fmt"

	"github.com/rbaliyan/event-mongodb/codec"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ExampleBSON demonstrates a deterministic encode/decode round-trip through the
// BSON codec, recovering the message fields on the other side.
func ExampleBSON() {
	c := codec.BSON{}

	payload, _ := bson.Marshal(bson.M{"order_id": "abc-123"})
	in := message.New(
		"evt-1",
		"shop.orders",
		payload,
		map[string]string{"operation": "insert"},
		message.WithRetryCount(2),
	)

	data, err := c.Encode(in)
	if err != nil {
		fmt.Println("encode error:", err)
		return
	}

	out, err := c.Decode(data)
	if err != nil {
		fmt.Println("decode error:", err)
		return
	}

	fmt.Println("id:", out.ID())
	fmt.Println("source:", out.Source())
	fmt.Println("retries:", out.RetryCount())
	fmt.Println("operation:", out.Metadata()["operation"])

	// Output:
	// id: evt-1
	// source: shop.orders
	// retries: 2
	// operation: insert
}

// ExampleBSON_ContentType shows the MIME type and name advertised by the codec.
func ExampleBSON_ContentType() {
	c := codec.BSON{}
	fmt.Println(c.ContentType())
	fmt.Println(c.Name())

	// Output:
	// application/bson
	// bson
}
