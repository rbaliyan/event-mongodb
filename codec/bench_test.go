package codec

import (
	"fmt"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// benchSize describes a benchmark payload size class.
type benchSize struct {
	name   string
	fields int // number of payload fields
}

var benchSizes = []benchSize{
	{name: "small", fields: 1},
	{name: "medium", fields: 25},
	{name: "large", fields: 250},
}

// benchPayload builds a BSON document with n fields.
func benchPayload(b *testing.B, n int) bson.Raw {
	b.Helper()
	doc := bson.M{}
	for i := 0; i < n; i++ {
		doc[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value-%d", i)
	}
	data, err := bson.Marshal(doc)
	if err != nil {
		b.Fatalf("bson.Marshal: %v", err)
	}
	return bson.Raw(data)
}

// benchMessage builds a representative message for a given payload size.
func benchMessage(b *testing.B, n int) message.Message {
	b.Helper()
	ts := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	return message.New(
		"evt-bench",
		"benchdb.benchcoll",
		benchPayload(b, n),
		map[string]string{"operation": "insert", "collection": "benchcoll"},
		message.WithTimestamp(ts),
		message.WithRetryCount(2),
	)
}

func BenchmarkBSONCodec_Encode(b *testing.B) {
	codec := BSON{}
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			msg := benchMessage(b, sz.fields)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := codec.Encode(msg); err != nil {
					b.Fatalf("Encode: %v", err)
				}
			}
		})
	}
}

func BenchmarkBSONCodec_Decode(b *testing.B) {
	codec := BSON{}
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			data, err := codec.Encode(benchMessage(b, sz.fields))
			if err != nil {
				b.Fatalf("Encode setup: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := codec.Decode(data); err != nil {
					b.Fatalf("Decode: %v", err)
				}
			}
		})
	}
}

func BenchmarkBSONCodec_RoundTrip(b *testing.B) {
	codec := BSON{}
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			msg := benchMessage(b, sz.fields)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := codec.Encode(msg)
				if err != nil {
					b.Fatalf("Encode: %v", err)
				}
				if _, err := codec.Decode(data); err != nil {
					b.Fatalf("Decode: %v", err)
				}
			}
		})
	}
}
