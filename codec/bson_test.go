package codec

import (
	"bytes"
	"errors"
	"testing"
	"time"

	evtcodec "github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// mustMarshalBSON marshals v into a valid bson document, failing the test on error.
func mustMarshalBSON(t *testing.T, v any) bson.Raw {
	t.Helper()
	data, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return bson.Raw(data)
}

func TestBSON_ContentType(t *testing.T) {
	if got := (BSON{}).ContentType(); got != "application/bson" {
		t.Errorf("ContentType() = %q, want %q", got, "application/bson")
	}
}

func TestBSON_Name(t *testing.T) {
	if got := (BSON{}).Name(); got != "bson" {
		t.Errorf("Name() = %q, want %q", got, "bson")
	}
}

func TestBSON_EncodeDecode_RoundTrip(t *testing.T) {
	// Use UTC and truncate to milliseconds: BSON time precision is milliseconds.
	ts := time.Date(2026, 6, 19, 12, 30, 45, 123000000, time.UTC)

	tests := []struct {
		name       string
		id         string
		source     string
		payload    bson.Raw
		metadata   map[string]string
		timestamp  time.Time
		retryCount int
	}{
		{
			name:       "full message",
			id:         "evt-123",
			source:     "mydb.orders",
			payload:    mustMarshalBSON(t, bson.M{"order_id": "abc", "amount": 4200}),
			metadata:   map[string]string{"operation": "insert", "collection": "orders"},
			timestamp:  ts,
			retryCount: 3,
		},
		{
			name:       "no metadata, no retries",
			id:         "evt-456",
			source:     "mydb.users",
			payload:    mustMarshalBSON(t, bson.M{"name": "alice"}),
			metadata:   nil,
			timestamp:  ts,
			retryCount: 0,
		},
		{
			name:       "empty payload document",
			id:         "evt-789",
			source:     "mydb.empty",
			payload:    mustMarshalBSON(t, bson.M{}),
			metadata:   map[string]string{"k": "v"},
			timestamp:  ts,
			retryCount: 7,
		},
		{
			name:       "objectid payload",
			id:         "evt-oid",
			source:     "mydb.things",
			payload:    mustMarshalBSON(t, bson.M{"_id": bson.NewObjectID()}),
			metadata:   map[string]string{"operation": "update"},
			timestamp:  ts,
			retryCount: 1,
		},
	}

	codec := BSON{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := message.New(
				tt.id,
				tt.source,
				tt.payload,
				tt.metadata,
				message.WithTimestamp(tt.timestamp),
				message.WithRetryCount(tt.retryCount),
			)

			data, err := codec.Encode(in)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(data) == 0 {
				t.Fatal("Encode returned empty data")
			}

			out, err := codec.Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if out.ID() != tt.id {
				t.Errorf("ID() = %q, want %q", out.ID(), tt.id)
			}
			if out.Source() != tt.source {
				t.Errorf("Source() = %q, want %q", out.Source(), tt.source)
			}
			if !out.Timestamp().Equal(tt.timestamp) {
				t.Errorf("Timestamp() = %v, want %v", out.Timestamp(), tt.timestamp)
			}
			if out.RetryCount() != tt.retryCount {
				t.Errorf("RetryCount() = %d, want %d", out.RetryCount(), tt.retryCount)
			}

			// Payload should be byte-equal after round-trip.
			if !bytes.Equal(out.Payload(), tt.payload) {
				t.Errorf("Payload() = %x, want %x", out.Payload(), []byte(tt.payload))
			}

			// Metadata round-trip: nil and empty are both acceptable for an
			// originally-nil/empty map due to the omitempty tag.
			if len(tt.metadata) == 0 {
				if len(out.Metadata()) != 0 {
					t.Errorf("Metadata() = %v, want empty", out.Metadata())
				}
			} else {
				if len(out.Metadata()) != len(tt.metadata) {
					t.Fatalf("Metadata() len = %d, want %d", len(out.Metadata()), len(tt.metadata))
				}
				for k, v := range tt.metadata {
					if out.Metadata()[k] != v {
						t.Errorf("Metadata()[%q] = %q, want %q", k, out.Metadata()[k], v)
					}
				}
			}
		})
	}
}

func TestBSON_Decode_InvalidBytes(t *testing.T) {
	codec := BSON{}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "garbage bytes", data: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{name: "empty bytes", data: []byte{}},
		{name: "truncated bson length prefix", data: []byte{0x05, 0x00, 0x00}},
		{
			name: "length prefix exceeds buffer",
			// Claims a 255-byte document but supplies far fewer bytes.
			data: []byte{0xFF, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := codec.Decode(tt.data)
			if err == nil {
				t.Fatalf("Decode(%v) = nil error, want error", tt.data)
			}
			if msg != nil {
				t.Errorf("Decode returned non-nil message %v on error", msg)
			}
			if !errors.Is(err, evtcodec.ErrDecodeFailure) {
				t.Errorf("error does not wrap ErrDecodeFailure: %v", err)
			}
		})
	}
}

func TestBSON_ImplementsCodecInterface(t *testing.T) {
	var _ evtcodec.Codec = BSON{}
}
