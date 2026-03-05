package mongodb

import (
	"encoding/json"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
