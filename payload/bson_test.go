package payload

import (
	"strings"
	"testing"
	"time"

	evtpayload "github.com/rbaliyan/event/v3/payload"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type sampleDoc struct {
	ID      bson.ObjectID  `bson:"_id"`
	Name    string         `bson:"name"`
	Count   int            `bson:"count"`
	Active  bool           `bson:"active"`
	Score   float64        `bson:"score"`
	Tags    []string       `bson:"tags"`
	Meta    map[string]int `bson:"meta"`
	Created time.Time      `bson:"created"`
}

func TestBSON_ContentType(t *testing.T) {
	if got := (BSON{}).ContentType(); got != "application/bson" {
		t.Errorf("ContentType() = %q, want %q", got, "application/bson")
	}
}

func TestBSON_EncodeDecode_Struct(t *testing.T) {
	codec := BSON{}

	oid := bson.NewObjectID()
	// BSON stores time at millisecond precision; truncate to compare equal.
	created := time.Date(2026, 6, 19, 8, 0, 0, 456000000, time.UTC)

	in := sampleDoc{
		ID:      oid,
		Name:    "widget",
		Count:   42,
		Active:  true,
		Score:   3.14,
		Tags:    []string{"a", "b", "c"},
		Meta:    map[string]int{"x": 1, "y": 2},
		Created: created,
	}

	data, err := codec.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Encode returned empty data")
	}

	var out sampleDoc
	if err := codec.Decode(data, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if out.ID != oid {
		t.Errorf("ID = %v, want %v", out.ID, oid)
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
	if out.Score != in.Score {
		t.Errorf("Score = %v, want %v", out.Score, in.Score)
	}
	if !out.Created.Equal(in.Created) {
		t.Errorf("Created = %v, want %v", out.Created, in.Created)
	}
	if len(out.Tags) != len(in.Tags) {
		t.Fatalf("Tags len = %d, want %d", len(out.Tags), len(in.Tags))
	}
	for i, tag := range in.Tags {
		if out.Tags[i] != tag {
			t.Errorf("Tags[%d] = %q, want %q", i, out.Tags[i], tag)
		}
	}
	if len(out.Meta) != len(in.Meta) {
		t.Fatalf("Meta len = %d, want %d", len(out.Meta), len(in.Meta))
	}
	for k, v := range in.Meta {
		if out.Meta[k] != v {
			t.Errorf("Meta[%q] = %d, want %d", k, out.Meta[k], v)
		}
	}
}

func TestBSON_EncodeDecode_Map(t *testing.T) {
	codec := BSON{}

	in := bson.M{
		"string": "hello",
		"int":    int64(99),
		"nested": bson.M{"a": "b"},
		"list":   bson.A{"one", "two"},
	}

	data, err := codec.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var out bson.M
	if err := codec.Decode(data, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if out["string"] != "hello" {
		t.Errorf("out[string] = %v, want hello", out["string"])
	}
	if out["int"] != int64(99) {
		t.Errorf("out[int] = %v, want 99", out["int"])
	}
	// Nested documents decode into bson.D when the target value is interface{}.
	nested, ok := out["nested"].(bson.D)
	if !ok {
		t.Fatalf("out[nested] type = %T, want bson.D", out["nested"])
	}
	var foundA bool
	for _, e := range nested {
		if e.Key == "a" {
			foundA = true
			if e.Value != "b" {
				t.Errorf("out[nested][a] = %v, want b", e.Value)
			}
		}
	}
	if !foundA {
		t.Errorf("out[nested] missing key %q", "a")
	}
	list, ok := out["list"].(bson.A)
	if !ok {
		t.Fatalf("out[list] type = %T, want bson.A", out["list"])
	}
	if len(list) != 2 || list[0] != "one" || list[1] != "two" {
		t.Errorf("out[list] = %v, want [one two]", list)
	}
}

func TestBSON_EncodeDecode_SliceWrapped(t *testing.T) {
	codec := BSON{}

	// A top-level BSON document cannot be a bare slice, so wrap it.
	type wrapper struct {
		Items []int `bson:"items"`
	}
	in := wrapper{Items: []int{1, 2, 3, 4, 5}}

	data, err := codec.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var out wrapper
	if err := codec.Decode(data, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if len(out.Items) != len(in.Items) {
		t.Fatalf("Items len = %d, want %d", len(out.Items), len(in.Items))
	}
	for i, v := range in.Items {
		if out.Items[i] != v {
			t.Errorf("Items[%d] = %d, want %d", i, out.Items[i], v)
		}
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
		{name: "truncated length prefix", data: []byte{0x05, 0x00, 0x00}},
		{
			name: "length prefix exceeds buffer",
			data: []byte{0xFF, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bson.M
			err := codec.Decode(tt.data, &out)
			if err == nil {
				t.Fatalf("Decode(%v) = nil error, want error", tt.data)
			}
			if !strings.Contains(err.Error(), "bson decode") {
				t.Errorf("error %q does not contain wrapping prefix %q", err.Error(), "bson decode")
			}
		})
	}
}

func TestBSON_Encode_UnsupportedValue(t *testing.T) {
	codec := BSON{}

	// A function cannot be marshaled to BSON; expect a wrapped encode error.
	_, err := codec.Encode(func() {})
	if err == nil {
		t.Fatal("Encode(func) = nil error, want error")
	}
	if !strings.Contains(err.Error(), "bson encode") {
		t.Errorf("error %q does not contain wrapping prefix %q", err.Error(), "bson encode")
	}
}

func TestBSON_ImplementsCodecInterface(t *testing.T) {
	var _ evtpayload.Codec = BSON{}
}

// TestBSON_AutoRegistered verifies the package init() registers the BSON codec
// in the global payload registry under its content type.
func TestBSON_AutoRegistered(t *testing.T) {
	got, ok := evtpayload.Get("application/bson")
	if !ok {
		t.Fatal("application/bson codec not registered in global registry")
	}
	if _, isBSON := got.(BSON); !isBSON {
		t.Errorf("registered codec type = %T, want payload.BSON", got)
	}
}
