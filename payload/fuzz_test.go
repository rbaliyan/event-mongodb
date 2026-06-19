package payload

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// pfMarshalBSON marshals v to BSON bytes for seeding fuzz corpora.
func pfMarshalBSON(f *testing.F, v any) []byte {
	f.Helper()
	data, err := bson.Marshal(v)
	if err != nil {
		f.Fatalf("bson.Marshal: %v", err)
	}
	return data
}

// FuzzPayloadBSONDecode feeds arbitrary bytes into BSON.Decode targeting a
// map[string]any. The oracle is liveness: Decode must never panic on hostile
// input, regardless of whether it returns an error.
func FuzzPayloadBSONDecode(f *testing.F) {
	// Seed corpus: real encodings plus malformed inputs.
	f.Add(pfMarshalBSON(f, bson.M{"name": "alice", "count": int64(7)}))
	f.Add(pfMarshalBSON(f, bson.M{}))
	f.Add(pfMarshalBSON(f, bson.M{"_id": bson.NewObjectID(), "nested": bson.M{"a": "b"}}))
	f.Add([]byte{})
	f.Add([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	f.Add([]byte{0x05, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xFF, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01})

	codec := BSON{}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Decode into a fresh map; only contract under test is "no panic".
		var out map[string]any
		_ = codec.Decode(data, &out)
	})
}

// pfDoc is a fuzz round-trip target struct with a variety of BSON field types.
type pfDoc struct {
	Name   string  `bson:"name"`
	Count  int64   `bson:"count"`
	Active bool    `bson:"active"`
	Ratio  float64 `bson:"ratio"`
}

// FuzzPayloadBSONRoundTrip marshals a fuzzer-derived struct, decodes it back,
// and asserts every field survives the round-trip exactly.
func FuzzPayloadBSONRoundTrip(f *testing.F) {
	f.Add("alice", int64(7), true, 3.14)
	f.Add("", int64(0), false, 0.0)
	f.Add("\x00\xff unicode ☃", int64(-1), true, -123.456)

	codec := BSON{}

	f.Fuzz(func(t *testing.T, name string, count int64, active bool, ratio float64) {
		in := pfDoc{Name: name, Count: count, Active: active, Ratio: ratio}

		data, err := codec.Encode(in)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}

		var out pfDoc
		if err := codec.Decode(data, &out); err != nil {
			t.Fatalf("Decode: %v", err)
		}

		if out.Name != in.Name {
			t.Errorf("Name = %q, want %q", out.Name, in.Name)
		}
		if out.Count != in.Count {
			t.Errorf("Count = %d, want %d", out.Count, in.Count)
		}
		if out.Active != in.Active {
			t.Errorf("Active = %v, want %v", out.Active, in.Active)
		}
		// NaN is never equal to itself; compare bit patterns indirectly by
		// treating both NaNs as equal.
		if out.Ratio != in.Ratio && !(isNaN(out.Ratio) && isNaN(in.Ratio)) {
			t.Errorf("Ratio = %v, want %v", out.Ratio, in.Ratio)
		}
	})
}

// isNaN reports whether f is an IEEE-754 NaN without importing math.
func isNaN(f float64) bool { return f != f }
