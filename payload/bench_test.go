package payload

import (
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// pbSize describes a benchmark document size class.
type pbSize struct {
	name   string
	fields int
}

var pbSizes = []pbSize{
	{name: "small", fields: 1},
	{name: "medium", fields: 25},
	{name: "large", fields: 250},
}

// pbDoc builds a representative document with n string fields plus a few typed
// fields, exercising heterogeneous BSON encoding.
func pbDoc(n int) bson.M {
	doc := bson.M{
		"_id":     bson.NewObjectID(),
		"created": time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC),
		"active":  true,
		"score":   3.14159,
	}
	for i := 0; i < n; i++ {
		doc[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value-%d", i)
	}
	return doc
}

func BenchmarkPayloadBSON_Encode(b *testing.B) {
	codec := BSON{}
	for _, sz := range pbSizes {
		b.Run(sz.name, func(b *testing.B) {
			doc := pbDoc(sz.fields)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := codec.Encode(doc); err != nil {
					b.Fatalf("Encode: %v", err)
				}
			}
		})
	}
}

func BenchmarkPayloadBSON_Decode(b *testing.B) {
	codec := BSON{}
	for _, sz := range pbSizes {
		b.Run(sz.name, func(b *testing.B) {
			data, err := codec.Encode(pbDoc(sz.fields))
			if err != nil {
				b.Fatalf("Encode setup: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bson.M
				if err := codec.Decode(data, &out); err != nil {
					b.Fatalf("Decode: %v", err)
				}
			}
		})
	}
}

func BenchmarkPayloadBSON_RoundTrip(b *testing.B) {
	codec := BSON{}
	for _, sz := range pbSizes {
		b.Run(sz.name, func(b *testing.B) {
			doc := pbDoc(sz.fields)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := codec.Encode(doc)
				if err != nil {
					b.Fatalf("Encode: %v", err)
				}
				var out bson.M
				if err := codec.Decode(data, &out); err != nil {
					b.Fatalf("Decode: %v", err)
				}
			}
		})
	}
}
