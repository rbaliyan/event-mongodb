package payload

import (
	"fmt"

	evtpayload "github.com/rbaliyan/event/v3/payload"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	// Register BSON codec in the global registry
	evtpayload.Register(BSON{})
}

// BSON codec uses MongoDB's BSON format for serialization.
// This is ideal for MongoDB change stream events where the payload
// is already BSON and contains MongoDB-specific types like ObjectID.
//
// Usage:
//
//	event := New[Model]("orders", WithPayloadCodec(payload.BSON{}))
type BSON struct{}

// Encode serializes the payload to BSON bytes.
func (BSON) Encode(v any) ([]byte, error) {
	data, err := bson.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("bson encode: %w", err)
	}
	return data, nil
}

// Decode deserializes BSON bytes to the target type.
func (BSON) Decode(data []byte, v any) error {
	if err := bson.Unmarshal(data, v); err != nil {
		return fmt.Errorf("bson decode: %w", err)
	}
	return nil
}

// ContentType returns the MIME type for BSON.
func (BSON) ContentType() string {
	return "application/bson"
}

// Compile-time check
var _ evtpayload.Codec = BSON{}
