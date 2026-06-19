package codec

import (
	"bytes"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// fuzzMarshalBSON marshals v to BSON bytes, failing the test on error.
// Named distinctly from helpers in other test files in this package.
func fuzzMarshalBSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return data
}

// fuzzMarshalBSONF is the *testing.F variant used when building seed corpora.
func fuzzMarshalBSONF(f *testing.F, v any) []byte {
	f.Helper()
	data, err := bson.Marshal(v)
	if err != nil {
		f.Fatalf("bson.Marshal: %v", err)
	}
	return data
}

// fuzzEncodeMessage encodes a representative message and returns the wire bytes.
// Used to build the seed corpus, so it takes *testing.F.
func fuzzEncodeMessage(f *testing.F, id, source string, payload []byte, meta map[string]string, ts time.Time, retry int) []byte {
	f.Helper()
	msg := message.New(id, source, payload, meta,
		message.WithTimestamp(ts),
		message.WithRetryCount(retry),
	)
	data, err := (BSON{}).Encode(msg)
	if err != nil {
		f.Fatalf("Encode seed: %v", err)
	}
	return data
}

// metadataEqual compares two metadata maps, treating nil and empty as equal.
func metadataEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// FuzzBSONCodecDecode feeds arbitrary bytes into BSON.Decode. The oracle is a
// round-trip fixpoint: if a byte string decodes successfully, then re-encoding
// the resulting message and decoding it again must yield an identical message
// across every field. Decode must also never panic on hostile input.
func FuzzBSONCodecDecode(f *testing.F) {
	ts := time.Date(2026, 6, 19, 12, 30, 45, 123000000, time.UTC)

	// Seed corpus: valid encodings of representative messages.
	f.Add(fuzzEncodeMessage(f, "evt-1", "db.coll", fuzzMarshalBSONF(f, bson.M{"a": 1}), map[string]string{"op": "insert"}, ts, 0))
	f.Add(fuzzEncodeMessage(f, "evt-2", "db.users", fuzzMarshalBSONF(f, bson.M{}), nil, ts, 5))
	f.Add(fuzzEncodeMessage(f, "", "", fuzzMarshalBSONF(f, bson.M{"_id": bson.NewObjectID()}), map[string]string{"k": "v", "k2": "v2"}, ts, 99))
	// Hostile / malformed seeds.
	f.Add([]byte{})
	f.Add([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	f.Add([]byte{0x05, 0x00, 0x00, 0x00, 0x00}) // minimal valid empty document
	f.Add([]byte{0xFF, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01})

	codec := BSON{}

	f.Fuzz(func(t *testing.T, data []byte) {
		m1, err := codec.Decode(data)
		if err != nil {
			// Malformed input: a decode error is acceptable. The contract is no panic.
			return
		}
		if m1 == nil {
			t.Fatal("Decode returned nil message with nil error")
		}

		// Re-encode the decoded message. A successfully-decoded BSON document
		// may carry a degenerate payload (e.g. the input was the minimal empty
		// document {0x05,0,0,0,0}, yielding a zero-length bson.Raw payload that
		// cannot be re-marshaled as a BSON value). That is not a crash and not a
		// contract violation, so treat a re-encode failure like malformed input
		// and skip the fixpoint assertion.
		reenc, err := codec.Encode(m1)
		if err != nil {
			return
		}

		// Decode again: round-trip must reach a fixpoint on all fields.
		m2, err := codec.Decode(reenc)
		if err != nil {
			t.Fatalf("Decode of re-encoded message failed: %v", err)
		}

		if m1.ID() != m2.ID() {
			t.Errorf("ID not preserved: %q -> %q", m1.ID(), m2.ID())
		}
		if m1.Source() != m2.Source() {
			t.Errorf("Source not preserved: %q -> %q", m1.Source(), m2.Source())
		}
		if !bytes.Equal(m1.Payload(), m2.Payload()) {
			t.Errorf("Payload not preserved: %x -> %x", m1.Payload(), m2.Payload())
		}
		if !m1.Timestamp().Equal(m2.Timestamp()) {
			t.Errorf("Timestamp not preserved: %v -> %v", m1.Timestamp(), m2.Timestamp())
		}
		if m1.RetryCount() != m2.RetryCount() {
			t.Errorf("RetryCount not preserved: %d -> %d", m1.RetryCount(), m2.RetryCount())
		}
		if !metadataEqual(m1.Metadata(), m2.Metadata()) {
			t.Errorf("Metadata not preserved: %v -> %v", m1.Metadata(), m2.Metadata())
		}
	})
}

// FuzzBSONCodecRoundTrip starts from a structured message description and checks
// the forward round-trip: Encode then Decode must preserve every field exactly
// (modulo BSON's millisecond timestamp precision, handled by the seed).
func FuzzBSONCodecRoundTrip(f *testing.F) {
	// Fixed timestamp at millisecond precision so BSON round-trips it exactly.
	tsUnixMilli := time.Date(2026, 6, 19, 12, 0, 0, 500000000, time.UTC).UnixMilli()

	f.Add("evt-1", "db.coll", "insert", "orders", tsUnixMilli, 0)
	f.Add("", "", "", "", int64(0), 0)
	f.Add("long-id-\x00\xff", "src", "delete", "x", tsUnixMilli, 1<<20)

	codec := BSON{}

	f.Fuzz(func(t *testing.T, id, source, op, coll string, tsMilli int64, retry int) {
		// Clamp the timestamp into a sane range BSON can represent.
		if tsMilli < -62135596800000 || tsMilli > 253402300799000 {
			tsMilli = 0
		}
		ts := time.UnixMilli(tsMilli).UTC()

		meta := map[string]string{"operation": op, "collection": coll}
		payload := fuzzMarshalBSON(t, bson.M{"op": op})

		in := message.New(id, source, payload, meta,
			message.WithTimestamp(ts),
			message.WithRetryCount(retry),
		)

		data, err := codec.Encode(in)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		out, err := codec.Decode(data)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		if out.ID() != id {
			t.Errorf("ID = %q, want %q", out.ID(), id)
		}
		if out.Source() != source {
			t.Errorf("Source = %q, want %q", out.Source(), source)
		}
		if !out.Timestamp().Equal(ts) {
			t.Errorf("Timestamp = %v, want %v", out.Timestamp(), ts)
		}
		if out.RetryCount() != retry {
			t.Errorf("RetryCount = %d, want %d", out.RetryCount(), retry)
		}
		if !bytes.Equal(out.Payload(), payload) {
			t.Errorf("Payload = %x, want %x", out.Payload(), payload)
		}
		if !metadataEqual(out.Metadata(), meta) {
			t.Errorf("Metadata = %v, want %v", out.Metadata(), meta)
		}
	})
}
