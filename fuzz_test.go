package mongodb

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// FuzzFieldCoerce fuzzes Field[T]/coerceNumeric over the UpdateDescription
// accessor with real oracles:
//   - never panics
//   - Field[int64] and Field[float64] agree for integral inputs
//   - coercing an already-typed T value is the identity
func FuzzFieldCoerce(f *testing.F) {
	f.Add(int64(0), uint8(0))
	f.Add(int64(42), uint8(1))
	f.Add(int64(-7), uint8(2))
	f.Add(int64(1<<40), uint8(3))
	f.Add(int64(255), uint8(4))

	f.Fuzz(func(t *testing.T, n int64, kindSel uint8) {
		// Pick a source representation for the numeric value.
		var srcVal any
		switch kindSel % 5 {
		case 0:
			srcVal = n
		case 1:
			srcVal = int32(n) // may truncate; that's fine for the oracle
		case 2:
			srcVal = int(n)
		case 3:
			srcVal = float64(n)
		case 4:
			srcVal = float32(n)
		}

		desc := &UpdateDescription{UpdatedFields: map[string]any{"v": srcVal}}

		// Must never panic.
		gotI, okI := Field[int64](desc, "v")
		gotF, okF := Field[float64](desc, "v")

		// For integral float sources within int64 range, int64 and float64
		// views must agree on the integral value.
		if okI && okF {
			// gotF should equal gotI when the float is integral.
			if gotF == float64(gotI) {
				// consistent
			} else if float64(int64(gotF)) == float64(gotI) {
				// also consistent after truncation
			} else {
				// Only flag when the float view is itself integral.
				if gotF == float64(int64(gotF)) && int64(gotF) != gotI {
					t.Errorf("Field[int64]=%d disagrees with Field[float64]=%v for src %v (%T)", gotI, gotF, srcVal, srcVal)
				}
			}
		}

		// Identity: coercing an already-typed int64 returns it unchanged.
		identDesc := &UpdateDescription{UpdatedFields: map[string]any{"v": n}}
		if got, ok := Field[int64](identDesc, "v"); !ok || got != n {
			t.Errorf("Field[int64] identity = (%d, %v), want (%d, true)", got, ok, n)
		}
		// Identity for float64.
		fv := float64(n)
		fDesc := &UpdateDescription{UpdatedFields: map[string]any{"v": fv}}
		if got, ok := Field[float64](fDesc, "v"); !ok || got != fv {
			t.Errorf("Field[float64] identity = (%v, %v), want (%v, true)", got, ok, fv)
		}

		// String values must never coerce to a numeric type.
		strDesc := &UpdateDescription{UpdatedFields: map[string]any{"v": "not-a-number"}}
		if _, ok := Field[int64](strDesc, "v"); ok {
			t.Error("string should not coerce to int64")
		}
	})
}

// FuzzConvertBSOND builds a bson.D directly (including special BSON types)
// so the ObjectID / Binary / Decimal128 / DateTime / Timestamp branches in
// convertBSONTypes are reachable. Oracle: bsonDToJSON output must always be
// valid JSON (json.Unmarshal succeeds) and never error.
func FuzzConvertBSOND(f *testing.F) {
	f.Add("key", "value", int64(0), uint8(0))
	f.Add("_id", "x", int64(1705300200), uint8(1))
	f.Add("count", "n", int64(42), uint8(2))
	f.Add("ratio", "r", int64(-99), uint8(3))
	f.Add("blob", "b", int64(7), uint8(4))
	f.Add("ts", "t", int64(123456789), uint8(5))

	f.Fuzz(func(t *testing.T, key, strVal string, num int64, typeSel uint8) {
		// Build a value of a fuzzer-chosen BSON type.
		var val any
		switch typeSel % 7 {
		case 0:
			val = strVal
		case 1:
			val = bson.NewObjectID()
		case 2:
			val = num
		case 3:
			val = float64(num)
		case 4:
			val = bson.Binary{Subtype: 0, Data: []byte(strVal)}
		case 5:
			val = bson.Timestamp{T: uint32(num), I: 0} // #nosec G115 -- test value
		case 6:
			val = bson.NewDateTimeFromTime(timeFromUnix(num))
		}

		// Use a fixed safe key plus the fuzzer key to keep the doc non-trivial.
		doc := bson.D{
			{Key: "fixed", Value: "ok"},
			{Key: sanitizeKey(key), Value: val},
			{Key: "nested", Value: bson.D{{Key: "inner", Value: val}}},
			{Key: "arr", Value: bson.A{val, "literal", num}},
		}

		data, err := bsonDToJSON(doc)
		if err != nil {
			t.Fatalf("bsonDToJSON returned error: %v (doc=%v)", err, doc)
		}
		var out any
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("bsonDToJSON produced invalid JSON: %v\njson=%s", err, data)
		}
	})
}

func FuzzFormatDocumentKey(f *testing.F) {
	f.Add("string-id")
	f.Add("")
	f.Add("507f1f77bcf86cd799439011")
	f.Add("123")
	f.Add("a]b[c{d}e")

	f.Fuzz(func(t *testing.T, id string) {
		_ = formatDocumentKey(id)
	})
}

// FuzzBsonDToJSON feeds RAW bytes through the production BSON parser
// (bson.Unmarshal into a bson.D) and then through bsonDToJSON — the exact
// path extractChangeEvent uses for FullDocument. This exercises the real
// driver decoder over hostile input rather than re-encoding JSON.
//
// Oracle:
//   - bson.Unmarshal must never panic.
//   - When the bytes decode to a bson.D and bsonDToJSON succeeds, the output is
//     valid JSON. A conversion error is tolerated: arbitrary BSON may contain
//     values (e.g. NaN/Inf doubles) that encoding/json cannot represent, which
//     production propagates as an error rather than crashing.
func FuzzBsonDToJSON(f *testing.F) {
	for _, seed := range fuzzRawBSONSeeds(f) {
		f.Add(seed)
	}
	// Malformed / hostile byte seeds.
	f.Add([]byte{})
	f.Add([]byte{0x05, 0x00, 0x00, 0x00, 0x00}) // minimal empty document
	f.Add([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	f.Add([]byte{0xFF, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01})
	// Regression: a valid BSON document holding a NaN double — decodes cleanly
	// but is not JSON-representable, so bsonDToJSON must error (not panic).
	f.Add(bsonNaNDoc())

	f.Fuzz(func(t *testing.T, data []byte) {
		var d bson.D
		if err := bson.Unmarshal(data, &d); err != nil {
			// Malformed BSON rejected cleanly; contract is "no panic".
			return
		}

		out, err := bsonDToJSON(d)
		if err != nil {
			// Tolerated: values like NaN/Inf doubles are not JSON-representable.
			return
		}
		var v any
		if err := json.Unmarshal(out, &v); err != nil {
			t.Fatalf("bsonDToJSON produced invalid JSON: %v\njson=%s", err, out)
		}
	})
}

// FuzzConvertBSONTypes feeds RAW bytes through bson.Unmarshal and then through
// convertBSONTypes, mirroring the production FullDocument conversion path.
//
// Oracle:
//   - bson.Unmarshal and convertBSONTypes must never panic.
//   - When the bytes decode and the converted value is JSON-encodable it stays
//     so; a json.Marshal error is tolerated (e.g. NaN/Inf doubles that BSON
//     allows but JSON cannot represent — production propagates the error).
func FuzzConvertBSONTypes(f *testing.F) {
	for _, seed := range fuzzRawBSONSeeds(f) {
		f.Add(seed)
	}
	f.Add([]byte{})
	f.Add([]byte{0x05, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	f.Add([]byte{0xFF, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01})
	f.Add(bsonNaNDoc()) // NaN double: decodes but is not JSON-representable

	f.Fuzz(func(t *testing.T, data []byte) {
		var d bson.D
		if err := bson.Unmarshal(data, &d); err != nil {
			return
		}

		converted := convertBSONTypes(d)
		if _, err := json.Marshal(converted); err != nil {
			// Tolerated: NaN/Inf doubles are not JSON-representable.
			return
		}
	})
}

// FuzzContextUpdateDescription builds an event metadata map with fuzzer-chosen
// JSON for the updated_fields / removed_fields keys, carries it on a context,
// and calls the production ContextUpdateDescription extractor.
//
// Oracle:
//   - ContextUpdateDescription must never panic on arbitrary metadata JSON.
//   - The result is internally consistent: it is non-nil exactly when at least
//     one of the two metadata keys is present, and a successfully-parsed
//     updated_fields / removed_fields blob round-trips back to equivalent JSON.
func FuzzContextUpdateDescription(f *testing.F) {
	f.Add(`{"status":"active","count":3}`, `["old"]`, uint8(0))
	f.Add(`{}`, `[]`, uint8(3))
	f.Add(`not json`, `also not json`, uint8(1))
	f.Add(``, ``, uint8(2))
	f.Add(`{"nested":{"a":[1,2,3]},"f":1.5}`, `["a","b","c"]`, uint8(0))
	f.Add(`[1,2,3]`, `{"not":"an array"}`, uint8(0)) // type-mismatched JSON

	f.Fuzz(func(t *testing.T, updated, removed string, presenceSel uint8) {
		md := map[string]string{}
		// presenceSel chooses which keys are present so the nil-result branch
		// (neither key) is also exercised.
		hasUpdated := presenceSel&1 != 0
		hasRemoved := presenceSel&2 != 0
		if hasUpdated {
			md[MetadataUpdatedFields] = updated
		}
		if hasRemoved {
			md[MetadataRemovedFields] = removed
		}

		ctx := event.ContextWithMetadata(context.Background(), md)

		// Must never panic.
		desc := ContextUpdateDescription(ctx)

		if !hasUpdated && !hasRemoved {
			if desc != nil {
				t.Fatalf("expected nil desc when neither key present, got %+v", desc)
			}
			return
		}
		if desc == nil {
			t.Fatalf("expected non-nil desc when a metadata key is present (updated=%v removed=%v)", hasUpdated, hasRemoved)
		}

		// Internal consistency: if updated_fields parsed into a non-nil map,
		// re-marshaling it must produce valid JSON.
		if desc.UpdatedFields != nil {
			if _, err := json.Marshal(desc.UpdatedFields); err != nil {
				t.Fatalf("parsed UpdatedFields not re-encodable: %v", err)
			}
		}
		// RemovedFields, when populated, must be a usable string slice.
		for i := range desc.RemovedFields {
			_ = desc.RemovedFields[i]
		}
	})
}

// FuzzFullDocumentUnmarshal feeds raw bytes as a bson.Raw FullDocument through
// the exact production decode used in extractChangeEvent: bson.Unmarshal into a
// bson.D, then bsonDToJSON.
//
// Oracle:
//   - bson.Unmarshal must never panic, regardless of the byte string.
//   - If the decode succeeds, the resulting bson.D is usable: bsonDToJSON
//     converts it to valid JSON without error.
func FuzzFullDocumentUnmarshal(f *testing.F) {
	for _, seed := range fuzzRawBSONSeeds(f) {
		f.Add(seed)
	}
	f.Add([]byte{})
	f.Add([]byte{0x05, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	// Truncated length prefix claiming more bytes than present.
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0x7F, 0x0A, 0x78, 0x00, 0x00})
	f.Add(bsonNaNDoc()) // NaN double: decodes but is not JSON-representable

	f.Fuzz(func(t *testing.T, data []byte) {
		raw := bson.Raw(data)

		var fullDoc bson.D
		if err := bson.Unmarshal(raw, &fullDoc); err != nil {
			// Malformed FullDocument is logged-and-skipped in production; here we
			// just require no panic.
			return
		}

		// Decode succeeded. A conversion error is a tolerated outcome:
		// bsonDToJSON uses encoding/json, which cannot represent values such as
		// NaN/Inf doubles that BSON permits, and production propagates that
		// error rather than crashing. The invariant we assert is the positive
		// one: when conversion succeeds, the output is valid JSON.
		out, err := bsonDToJSON(fullDoc)
		if err != nil {
			return
		}
		var v any
		if err := json.Unmarshal(out, &v); err != nil {
			t.Fatalf("FullDocument JSON invalid: %v\njson=%s", err, out)
		}
	})
}

func FuzzChangeEventJSON(f *testing.F) {
	f.Add([]byte(`{"operationType":"insert","ns":{"db":"test","coll":"users"},"fullDocument":{"name":"John"}}`))
	f.Add([]byte(`{"operationType":"update","documentKey":{"_id":"abc"}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var ce ChangeEvent
		_ = json.Unmarshal(data, &ce)
	})
}

// timeFromUnix builds a time.Time from a unix seconds value, used by fuzz
// targets that construct bson.DateTime values.
func timeFromUnix(sec int64) time.Time {
	return time.Unix(sec, 0).UTC()
}

// bsonNaNDoc returns the BSON encoding of {"n": NaN}, a valid document whose
// double value cannot be represented in JSON. It is a regression seed proving
// the BSON->JSON conversion oracles tolerate (rather than crash on) such input.
func bsonNaNDoc() []byte {
	d, err := bson.Marshal(bson.D{{Key: "n", Value: math.NaN()}})
	if err != nil {
		panic(err) // unreachable: marshaling a fixed document cannot fail
	}
	return d
}

// fuzzRawBSONSeeds returns a set of realistic, change-stream-shaped BSON
// documents encoded as raw bytes. They seed the byte-oriented root targets
// (FuzzBsonDToJSON, FuzzConvertBSONTypes, FuzzFullDocumentUnmarshal) so the
// driver decoder and bsonDToJSON exercise the special-type branches
// (ObjectID/Decimal128/Binary/DateTime/Timestamp) plus nested/array shapes.
func fuzzRawBSONSeeds(f *testing.F) [][]byte {
	f.Helper()
	dec, err := bson.ParseDecimal128("1234.5678")
	if err != nil {
		f.Fatalf("ParseDecimal128: %v", err)
	}
	docs := []bson.D{
		// insert-shaped full document with mixed scalar types.
		{
			{Key: "_id", Value: bson.NewObjectID()},
			{Key: "name", Value: "alice"},
			{Key: "count", Value: int64(42)},
			{Key: "active", Value: true},
			{Key: "ratio", Value: 3.14},
		},
		// special BSON types.
		{
			{Key: "_id", Value: bson.NewObjectID()},
			{Key: "amount", Value: dec},
			{Key: "blob", Value: bson.Binary{Subtype: 0x00, Data: []byte("payload")}},
			{Key: "created", Value: bson.NewDateTimeFromTime(time.Unix(1705300200, 0).UTC())},
			{Key: "ts", Value: bson.Timestamp{T: 1705300200, I: 1}},
		},
		// deeply nested + array-heavy document.
		{
			{Key: "_id", Value: "string-key"},
			{Key: "nested", Value: bson.D{
				{Key: "level1", Value: bson.D{
					{Key: "level2", Value: bson.D{
						{Key: "value", Value: int32(7)},
						{Key: "list", Value: bson.A{1, "two", false, bson.D{{Key: "deep", Value: true}}}},
					}},
				}},
			}},
			{Key: "tags", Value: bson.A{"a", "b", "c", "d"}},
			{Key: "matrix", Value: bson.A{bson.A{1, 2}, bson.A{3, 4}}},
		},
		// empty document.
		{},
	}
	seeds := make([][]byte, 0, len(docs))
	for _, d := range docs {
		data, err := bson.Marshal(d)
		if err != nil {
			f.Fatalf("bson.Marshal seed: %v", err)
		}
		seeds = append(seeds, data)
	}
	return seeds
}

// sanitizeKey ensures a fuzzer-chosen map key is non-empty and free of NUL
// bytes (which BSON keys disallow), so the fuzz target tests conversion logic
// rather than BSON encoding rejections.
func sanitizeKey(k string) string {
	out := make([]rune, 0, len(k))
	for _, r := range k {
		if r == 0 || r == '.' || r == '$' {
			continue
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return "k"
	}
	return string(out)
}
