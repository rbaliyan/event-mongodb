package mongodb

import (
	"encoding/json"
	"testing"
	"time"

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

func FuzzBsonDToJSON(f *testing.F) {
	f.Add([]byte(`{"name":"test","value":42}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"nested":{"key":"val"}}`))
	f.Add([]byte(`{"arr":[1,2,3]}`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, data []byte) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return
		}

		doc := mapToBsonD(m)
		_, _ = bsonDToJSON(doc)
	})
}

func FuzzConvertBSONTypes(f *testing.F) {
	f.Add([]byte(`{"key":"value"}`))
	f.Add([]byte(`{"num":123,"bool":true,"null":null}`))
	f.Add([]byte(`{"nested":{"a":1},"arr":[1,"two",false]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, data []byte) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return
		}

		doc := mapToBsonD(m)
		_ = convertBSONTypes(doc)
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

// mapToBsonD converts a map to bson.D for fuzz testing.
func mapToBsonD(m map[string]any) bson.D {
	if m == nil {
		return nil
	}
	doc := make(bson.D, 0, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case map[string]any:
			doc = append(doc, bson.E{Key: k, Value: mapToBsonD(val)})
		case []any:
			arr := make(bson.A, len(val))
			for i, item := range val {
				if nested, ok := item.(map[string]any); ok {
					arr[i] = mapToBsonD(nested)
				} else {
					arr[i] = item
				}
			}
			doc = append(doc, bson.E{Key: k, Value: arr})
		default:
			doc = append(doc, bson.E{Key: k, Value: v})
		}
	}
	return doc
}
