package mongodb

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// --- shared synthetic fixtures ---

// benchFlatDoc returns a small flat document of scalar fields.
func benchFlatDoc() bson.D {
	return bson.D{
		{Key: "name", Value: "alice"},
		{Key: "age", Value: int32(30)},
		{Key: "active", Value: true},
		{Key: "score", Value: 12.5},
	}
}

// benchWideDoc returns a document with many scalar fields.
func benchWideDoc() bson.D {
	d := make(bson.D, 0, 50)
	for i := 0; i < 50; i++ {
		d = append(d, bson.E{Key: "field" + string(rune('a'+i%26)) + string(rune('0'+i/26)), Value: int32(i)})
	}
	return d
}

// benchNestedDoc returns a deeply nested document.
func benchNestedDoc() bson.D {
	return bson.D{
		{Key: "level1", Value: bson.D{
			{Key: "level2", Value: bson.D{
				{Key: "level3", Value: bson.D{
					{Key: "value", Value: "deep"},
					{Key: "id", Value: bson.NewObjectID()},
				}},
			}},
		}},
		{Key: "meta", Value: bson.D{{Key: "created", Value: bson.NewDateTimeFromTime(time.Now())}}},
	}
}

// benchArrayDoc returns a document dominated by arrays.
func benchArrayDoc() bson.D {
	arr := make(bson.A, 0, 100)
	for i := 0; i < 100; i++ {
		arr = append(arr, int32(i))
	}
	return bson.D{
		{Key: "ints", Value: arr},
		{Key: "tags", Value: bson.A{"go", "mongodb", "bson", "json"}},
		{Key: "nested", Value: bson.A{bson.D{{Key: "k", Value: "v"}}, bson.D{{Key: "k2", Value: int64(99)}}}},
	}
}

// benchTypeMixDoc exercises all the special BSON conversion branches.
func benchTypeMixDoc() bson.D {
	dec, _ := bson.ParseDecimal128("123.456")
	return bson.D{
		{Key: "oid", Value: bson.NewObjectID()},
		{Key: "dt", Value: bson.NewDateTimeFromTime(time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC))},
		{Key: "ts", Value: bson.Timestamp{T: 1705300200, I: 1}},
		{Key: "bin", Value: bson.Binary{Subtype: 4, Data: []byte{1, 2, 3, 4}}},
		{Key: "dec", Value: dec},
		{Key: "str", Value: "plain"},
		{Key: "num", Value: int64(42)},
	}
}

// benchChangeDoc returns a synthetic changeStreamDoc for an insert event.
func benchChangeDoc(b *testing.B, full bson.D) changeStreamDoc {
	b.Helper()
	raw := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "resume-token-bench"}}},
		{Key: "operationType", Value: "insert"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "benchdb"}, {Key: "coll", Value: "orders"}}},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: bson.NewObjectID()}}},
		{Key: "fullDocument", Value: full},
		{Key: "clusterTime", Value: bson.Timestamp{T: 1705300200, I: 1}},
	}
	data, err := bson.Marshal(raw)
	if err != nil {
		b.Fatalf("marshal change doc: %v", err)
	}
	var doc changeStreamDoc
	if err := bson.Unmarshal(data, &doc); err != nil {
		b.Fatalf("unmarshal change doc: %v", err)
	}
	return doc
}

func BenchmarkConvertBSONTypes(b *testing.B) {
	cases := map[string]bson.D{
		"flat":     benchFlatDoc(),
		"wide":     benchWideDoc(),
		"nested":   benchNestedDoc(),
		"array":    benchArrayDoc(),
		"type-mix": benchTypeMixDoc(),
	}
	for name, doc := range cases {
		doc := doc
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = convertBSONTypes(doc)
			}
		})
	}
}

func BenchmarkBsonDToJSON(b *testing.B) {
	cases := map[string]bson.D{
		"flat":     benchFlatDoc(),
		"wide":     benchWideDoc(),
		"nested":   benchNestedDoc(),
		"array":    benchArrayDoc(),
		"type-mix": benchTypeMixDoc(),
	}
	for name, doc := range cases {
		doc := doc
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := bsonDToJSON(doc); err != nil {
					b.Fatalf("bsonDToJSON: %v", err)
				}
			}
		})
	}
}

func BenchmarkExtractChangeEvent(b *testing.B) {
	tr := &Transport{transportOptions: transportOptions{collectionName: "orders", logger: discardLogger()}}
	cases := map[string]bson.D{
		"flat":   benchFlatDoc(),
		"nested": benchNestedDoc(),
		"array":  benchArrayDoc(),
	}
	for name, full := range cases {
		doc := benchChangeDoc(b, full)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tr.extractChangeEvent(doc)
			}
		})
	}
}

func BenchmarkBuildPayload(b *testing.B) {
	full := benchTypeMixDoc()
	doc := benchChangeDoc(b, full)

	b.Run("json", func(b *testing.B) {
		tr := &Transport{transportOptions: transportOptions{logger: discardLogger()}}
		ev := tr.extractChangeEvent(doc)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := tr.buildPayload(doc, ev); err != nil {
				b.Fatalf("buildPayload: %v", err)
			}
		}
	})

	b.Run("fullDocOnly", func(b *testing.B) {
		tr := &Transport{transportOptions: transportOptions{fullDocumentOnly: true, logger: discardLogger()}}
		ev := tr.extractChangeEvent(doc)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := tr.buildPayload(doc, ev); err != nil {
				b.Fatalf("buildPayload: %v", err)
			}
		}
	})
}

func BenchmarkBuildMetadata(b *testing.B) {
	smallEvent := ChangeEvent{
		OperationType: OperationUpdate,
		Database:      "db",
		Collection:    "orders",
		Namespace:     "db.orders",
		DocumentKey:   "doc1",
		Timestamp:     time.Now(),
		UpdateDesc: &UpdateDescription{
			UpdatedFields: map[string]any{"status": "shipped", "count": 5},
			RemovedFields: []string{"temp"},
		},
	}

	// Build a larger updated-fields map to test the size-limit branch.
	bigFields := make(map[string]any, 64)
	for i := 0; i < 64; i++ {
		bigFields["field"+string(rune('a'+i%26))+string(rune('0'+i/26))] = i
	}
	bigEvent := smallEvent
	bigEvent.UpdateDesc = &UpdateDescription{UpdatedFields: bigFields, RemovedFields: []string{"temp"}}

	b.Run("updateDescription-off", func(b *testing.B) {
		tr := &Transport{transportOptions: transportOptions{includeUpdateDescription: false, logger: discardLogger()}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = tr.buildMetadata(smallEvent, "application/json")
		}
	})

	b.Run("updateDescription-on", func(b *testing.B) {
		tr := &Transport{transportOptions: transportOptions{includeUpdateDescription: true, logger: discardLogger()}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = tr.buildMetadata(smallEvent, "application/json")
		}
	})

	b.Run("underMaxSize", func(b *testing.B) {
		tr := &Transport{transportOptions: transportOptions{includeUpdateDescription: true, maxUpdatedFieldsSize: 8192, logger: discardLogger()}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = tr.buildMetadata(bigEvent, "application/json")
		}
	})

	b.Run("overMaxSize", func(b *testing.B) {
		tr := &Transport{transportOptions: transportOptions{includeUpdateDescription: true, maxUpdatedFieldsSize: 16, logger: discardLogger()}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = tr.buildMetadata(bigEvent, "application/json")
		}
	})
}

func BenchmarkFormatDocumentKey(b *testing.B) {
	oid := bson.NewObjectID()
	cases := map[string]any{
		"objectID": oid,
		"string":   "some-string-id",
		"int":      42,
		"int64":    int64(9999999),
		"float64":  3.14159,
		"binary":   bson.Binary{Subtype: 4, Data: []byte{0xab, 0xcd, 0xef}},
	}
	for name, id := range cases {
		id := id
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = formatDocumentKey(id)
			}
		})
	}
}

func BenchmarkField(b *testing.B) {
	descBSON := &UpdateDescription{
		UpdatedFields: map[string]any{
			"name":  "alice",
			"count": int32(42),
			"ratio": 3.14,
		},
	}

	b.Run("string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = Field[string](descBSON, "name")
		}
	})

	b.Run("int64-from-int32", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = Field[int64](descBSON, "count")
		}
	})

	b.Run("float64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = Field[float64](descBSON, "ratio")
		}
	})
}
