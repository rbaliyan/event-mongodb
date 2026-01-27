package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// --- validate() tests ---

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*Transport)
		wantErr error
	}{
		{
			name:  "default transport passes",
			setup: func(tr *Transport) {},
		},
		{
			name: "maxUpdatedFieldsSize without fullDocument fails",
			setup: func(tr *Transport) {
				tr.maxUpdatedFieldsSize = 1024
			},
			wantErr: ErrMaxUpdatedFieldsSizeRequiresFull,
		},
		{
			name: "maxUpdatedFieldsSize with fullDocument passes",
			setup: func(tr *Transport) {
				tr.maxUpdatedFieldsSize = 1024
				tr.fullDocument = FullDocumentUpdateLookup
			},
		},
		{
			name: "fullDocumentOnly without fullDocument fails",
			setup: func(tr *Transport) {
				tr.fullDocumentOnly = true
			},
			wantErr: ErrFullDocumentRequired,
		},
		{
			name: "fullDocumentOnly with fullDocument passes",
			setup: func(tr *Transport) {
				tr.fullDocumentOnly = true
				tr.fullDocument = FullDocumentUpdateLookup
			},
		},
		{
			name: "fullDocumentOnly with FullDocumentDefault fails",
			setup: func(tr *Transport) {
				tr.fullDocumentOnly = true
				tr.fullDocument = FullDocumentDefault
			},
			wantErr: ErrFullDocumentRequired,
		},
		{
			name: "includeUpdateDescription alone passes",
			setup: func(tr *Transport) {
				tr.includeUpdateDescription = true
			},
		},
		{
			name: "both maxUpdatedFieldsSize and fullDocumentOnly with fullDocument passes",
			setup: func(tr *Transport) {
				tr.maxUpdatedFieldsSize = 512
				tr.fullDocumentOnly = true
				tr.fullDocument = FullDocumentUpdateLookup
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Transport{}
			tt.setup(tr)
			err := tr.validate()
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("validate() = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("validate() unexpected error: %v", err)
			}
		})
	}
}

// --- isEmptyUpdate() tests ---

func TestIsEmptyUpdate(t *testing.T) {
	tests := []struct {
		name string
		event ChangeEvent
		want  bool
	}{
		{
			name:  "insert is never empty",
			event: ChangeEvent{OperationType: OperationInsert},
			want:  false,
		},
		{
			name:  "delete is never empty",
			event: ChangeEvent{OperationType: OperationDelete},
			want:  false,
		},
		{
			name:  "replace is never empty",
			event: ChangeEvent{OperationType: OperationReplace},
			want:  false,
		},
		{
			name:  "replace with nil UpdateDesc is not empty",
			event: ChangeEvent{OperationType: OperationReplace, UpdateDesc: nil},
			want:  false,
		},
		{
			name:  "update with nil UpdateDesc is empty",
			event: ChangeEvent{OperationType: OperationUpdate, UpdateDesc: nil},
			want:  true,
		},
		{
			name: "update with empty UpdateDesc is empty",
			event: ChangeEvent{
				OperationType: OperationUpdate,
				UpdateDesc:    &UpdateDescription{},
			},
			want: true,
		},
		{
			name: "update with empty maps is empty",
			event: ChangeEvent{
				OperationType: OperationUpdate,
				UpdateDesc: &UpdateDescription{
					UpdatedFields: map[string]any{},
					RemovedFields: []string{},
				},
			},
			want: true,
		},
		{
			name: "update with updated fields is not empty",
			event: ChangeEvent{
				OperationType: OperationUpdate,
				UpdateDesc: &UpdateDescription{
					UpdatedFields: map[string]any{"name": "test"},
				},
			},
			want: false,
		},
		{
			name: "update with removed fields is not empty",
			event: ChangeEvent{
				OperationType: OperationUpdate,
				UpdateDesc: &UpdateDescription{
					RemovedFields: []string{"old_field"},
				},
			},
			want: false,
		},
		{
			name: "update with both fields is not empty",
			event: ChangeEvent{
				OperationType: OperationUpdate,
				UpdateDesc: &UpdateDescription{
					UpdatedFields: map[string]any{"name": "test"},
					RemovedFields: []string{"old_field"},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isEmptyUpdate(tt.event)
			if got != tt.want {
				t.Errorf("isEmptyUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- formatDocumentKey() tests ---

func TestFormatDocumentKey(t *testing.T) {
	oid := primitive.NewObjectID()

	tests := []struct {
		name string
		id   any
		want string
	}{
		{name: "nil", id: nil, want: ""},
		{name: "ObjectID", id: oid, want: oid.Hex()},
		{name: "string", id: "my-id", want: "my-id"},
		{name: "int", id: 42, want: "42"},
		{name: "int32", id: int32(100), want: "100"},
		{name: "int64", id: int64(999), want: "999"},
		{name: "float64", id: 3.14, want: "3.14"},
		{
			name: "Binary",
			id:   primitive.Binary{Subtype: 4, Data: []byte{0xab, 0xcd}},
			want: "abcd",
		},
		{
			name: "unknown type fallback",
			id:   true,
			want: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDocumentKey(tt.id)
			if got != tt.want {
				t.Errorf("formatDocumentKey(%v) = %q, want %q", tt.id, got, tt.want)
			}
		})
	}
}

// --- convertBSONTypes() tests ---

func TestConvertBSONTypes(t *testing.T) {
	oid := primitive.NewObjectID()

	t.Run("ObjectID", func(t *testing.T) {
		result := convertBSONTypes(oid)
		m, ok := result.(map[string]string)
		if !ok {
			t.Fatalf("expected map[string]string, got %T", result)
		}
		if m["$oid"] != oid.Hex() {
			t.Errorf("got %q, want %q", m["$oid"], oid.Hex())
		}
	})

	t.Run("DateTime", func(t *testing.T) {
		dt := primitive.NewDateTimeFromTime(time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC))
		result := convertBSONTypes(dt)
		s, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		parsed, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatalf("failed to parse result: %v", err)
		}
		if parsed.Year() != 2025 || parsed.Month() != 1 || parsed.Day() != 15 {
			t.Errorf("unexpected date: %v", parsed)
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		ts := primitive.Timestamp{T: 1705300200, I: 1}
		result := convertBSONTypes(ts)
		s, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		parsed, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatalf("failed to parse result: %v", err)
		}
		if parsed.Unix() != 1705300200 {
			t.Errorf("got unix %d, want 1705300200", parsed.Unix())
		}
	})

	t.Run("Decimal128", func(t *testing.T) {
		d, _ := primitive.ParseDecimal128("123.456")
		result := convertBSONTypes(d)
		s, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		if s != "123.456" {
			t.Errorf("got %q, want %q", s, "123.456")
		}
	})

	t.Run("Binary", func(t *testing.T) {
		b := primitive.Binary{Subtype: 4, Data: []byte{1, 2, 3}}
		result := convertBSONTypes(b)
		m, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", result)
		}
		binary, ok := m["$binary"].(map[string]any)
		if !ok {
			t.Fatalf("expected $binary map, got %T", m["$binary"])
		}
		if binary["subType"] != "04" {
			t.Errorf("subType = %v, want 04", binary["subType"])
		}
	})

	t.Run("nested bson.M", func(t *testing.T) {
		input := bson.M{
			"name": "test",
			"nested": bson.M{
				"id": oid,
			},
		}
		result := convertBSONTypes(input)
		m, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", result)
		}
		if m["name"] != "test" {
			t.Errorf("name = %v, want test", m["name"])
		}
		nested, ok := m["nested"].(map[string]any)
		if !ok {
			t.Fatalf("expected nested map, got %T", m["nested"])
		}
		idMap, ok := nested["id"].(map[string]string)
		if !ok {
			t.Fatalf("expected id map, got %T", nested["id"])
		}
		if idMap["$oid"] != oid.Hex() {
			t.Errorf("nested id = %v, want %v", idMap["$oid"], oid.Hex())
		}
	})

	t.Run("bson.A", func(t *testing.T) {
		input := bson.A{"hello", int32(42), oid}
		result := convertBSONTypes(input)
		arr, ok := result.([]any)
		if !ok {
			t.Fatalf("expected []any, got %T", result)
		}
		if len(arr) != 3 {
			t.Fatalf("len = %d, want 3", len(arr))
		}
		if arr[0] != "hello" {
			t.Errorf("arr[0] = %v, want hello", arr[0])
		}
	})

	t.Run("passthrough types", func(t *testing.T) {
		if convertBSONTypes("hello") != "hello" {
			t.Error("string passthrough failed")
		}
		if convertBSONTypes(42) != 42 {
			t.Error("int passthrough failed")
		}
		if convertBSONTypes(true) != true {
			t.Error("bool passthrough failed")
		}
		if convertBSONTypes(nil) != nil {
			t.Error("nil passthrough failed")
		}
	})
}

// --- bsonMToMap() tests ---

func TestBsonMToMap(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		if bsonMToMap(nil) != nil {
			t.Error("expected nil for nil input")
		}
	})

	t.Run("empty map", func(t *testing.T) {
		result := bsonMToMap(bson.M{})
		if result == nil || len(result) != 0 {
			t.Errorf("expected empty map, got %v", result)
		}
	})

	t.Run("converts types", func(t *testing.T) {
		oid := primitive.NewObjectID()
		result := bsonMToMap(bson.M{
			"name": "test",
			"id":   oid,
		})
		if result["name"] != "test" {
			t.Errorf("name = %v, want test", result["name"])
		}
		idMap, ok := result["id"].(map[string]string)
		if !ok {
			t.Fatalf("expected map for id, got %T", result["id"])
		}
		if idMap["$oid"] != oid.Hex() {
			t.Errorf("id = %v, want %v", idMap["$oid"], oid.Hex())
		}
	})
}

// --- extractChangeEvent() tests ---

func TestExtractChangeEvent(t *testing.T) {
	tr := &Transport{
		collectionName: "orders",
	}
	// Give tr a db-like fallback by leaving db nil but setting collectionName

	t.Run("insert with full document", func(t *testing.T) {
		oid := primitive.NewObjectID()
		raw := bson.M{
			"_id":           bson.M{"_data": "resume-token-123"},
			"operationType": "insert",
			"ns":            bson.M{"db": "testdb", "coll": "orders"},
			"documentKey":   bson.M{"_id": oid},
			"fullDocument":  bson.M{"_id": oid, "name": "test"},
			"clusterTime":   primitive.Timestamp{T: 1705300200, I: 1},
		}

		event := tr.extractChangeEvent(raw)

		if event.ID != "resume-token-123" {
			t.Errorf("ID = %q, want resume-token-123", event.ID)
		}
		if event.OperationType != OperationInsert {
			t.Errorf("OperationType = %q, want insert", event.OperationType)
		}
		if event.Database != "testdb" {
			t.Errorf("Database = %q, want testdb", event.Database)
		}
		if event.Collection != "orders" {
			t.Errorf("Collection = %q, want orders", event.Collection)
		}
		if event.Namespace != "testdb.orders" {
			t.Errorf("Namespace = %q, want testdb.orders", event.Namespace)
		}
		if event.DocumentKey != oid.Hex() {
			t.Errorf("DocumentKey = %q, want %q", event.DocumentKey, oid.Hex())
		}
		if event.FullDocument == nil {
			t.Error("FullDocument is nil")
		}
		if event.Timestamp.Unix() != 1705300200 {
			t.Errorf("Timestamp = %v, want unix 1705300200", event.Timestamp)
		}
	})

	t.Run("update with updateDescription", func(t *testing.T) {
		raw := bson.M{
			"_id":           bson.M{"_data": "token-456"},
			"operationType": "update",
			"ns":            bson.M{"db": "testdb", "coll": "orders"},
			"documentKey":   bson.M{"_id": "string-id"},
			"updateDescription": bson.M{
				"updatedFields": bson.M{"status": "shipped", "count": int32(5)},
				"removedFields": bson.A{"old_field", "deprecated"},
				"truncatedArrays": bson.A{
					bson.M{"field": "items", "newSize": int32(10)},
					bson.M{"field": "tags", "newSize": int32(3)},
				},
			},
		}

		event := tr.extractChangeEvent(raw)

		if event.OperationType != OperationUpdate {
			t.Errorf("OperationType = %q, want update", event.OperationType)
		}
		if event.DocumentKey != "string-id" {
			t.Errorf("DocumentKey = %q, want string-id", event.DocumentKey)
		}
		if event.UpdateDesc == nil {
			t.Fatal("UpdateDesc is nil")
		}
		if len(event.UpdateDesc.UpdatedFields) != 2 {
			t.Errorf("UpdatedFields len = %d, want 2", len(event.UpdateDesc.UpdatedFields))
		}
		if event.UpdateDesc.UpdatedFields["status"] != "shipped" {
			t.Errorf("UpdatedFields[status] = %v, want shipped", event.UpdateDesc.UpdatedFields["status"])
		}
		if len(event.UpdateDesc.RemovedFields) != 2 {
			t.Errorf("RemovedFields len = %d, want 2", len(event.UpdateDesc.RemovedFields))
		}
		if event.UpdateDesc.RemovedFields[0] != "old_field" {
			t.Errorf("RemovedFields[0] = %q, want old_field", event.UpdateDesc.RemovedFields[0])
		}
		if len(event.UpdateDesc.TruncatedArrays) != 2 {
			t.Errorf("TruncatedArrays len = %d, want 2", len(event.UpdateDesc.TruncatedArrays))
		}
		if event.UpdateDesc.TruncatedArrays[0] != "items" {
			t.Errorf("TruncatedArrays[0] = %q, want items", event.UpdateDesc.TruncatedArrays[0])
		}
	})

	t.Run("delete without full document", func(t *testing.T) {
		oid := primitive.NewObjectID()
		raw := bson.M{
			"_id":           bson.M{"_data": "token-789"},
			"operationType": "delete",
			"ns":            bson.M{"db": "testdb", "coll": "orders"},
			"documentKey":   bson.M{"_id": oid},
		}

		event := tr.extractChangeEvent(raw)

		if event.OperationType != OperationDelete {
			t.Errorf("OperationType = %q, want delete", event.OperationType)
		}
		if event.FullDocument != nil {
			t.Errorf("FullDocument should be nil for delete, got %s", event.FullDocument)
		}
		if event.UpdateDesc != nil {
			t.Error("UpdateDesc should be nil for delete")
		}
	})

	t.Run("missing fields use fallbacks", func(t *testing.T) {
		raw := bson.M{
			"operationType": "insert",
		}

		event := tr.extractChangeEvent(raw)

		// ID should be auto-generated
		if event.ID == "" {
			t.Error("ID should be auto-generated when missing")
		}
		// Collection should fall back to transport config
		if event.Collection != "orders" {
			t.Errorf("Collection = %q, want orders (fallback)", event.Collection)
		}
		// Timestamp should fall back to time.Now()
		if event.Timestamp.IsZero() {
			t.Error("Timestamp should default to now")
		}
	})
}

// --- ContextUpdateDescription() tests ---

func TestContextUpdateDescription(t *testing.T) {
	t.Run("nil context metadata", func(t *testing.T) {
		ctx := context.Background()
		desc := ContextUpdateDescription(ctx)
		if desc != nil {
			t.Errorf("expected nil, got %v", desc)
		}
	})

	t.Run("metadata without update fields", func(t *testing.T) {
		ctx := event.ContextWithMetadata(context.Background(), map[string]string{
			"operation": "insert",
		})
		desc := ContextUpdateDescription(ctx)
		if desc != nil {
			t.Errorf("expected nil, got %v", desc)
		}
	})

	t.Run("metadata with updated_fields only", func(t *testing.T) {
		ctx := event.ContextWithMetadata(context.Background(), map[string]string{
			MetadataUpdatedFields: `{"name":"test","count":5}`,
		})
		desc := ContextUpdateDescription(ctx)
		if desc == nil {
			t.Fatal("expected non-nil")
		}
		if desc.UpdatedFields["name"] != "test" {
			t.Errorf("UpdatedFields[name] = %v, want test", desc.UpdatedFields["name"])
		}
		// JSON numbers decode as float64
		if desc.UpdatedFields["count"] != float64(5) {
			t.Errorf("UpdatedFields[count] = %v, want 5", desc.UpdatedFields["count"])
		}
		if desc.RemovedFields != nil {
			t.Errorf("RemovedFields should be nil, got %v", desc.RemovedFields)
		}
	})

	t.Run("metadata with removed_fields only", func(t *testing.T) {
		ctx := event.ContextWithMetadata(context.Background(), map[string]string{
			MetadataRemovedFields: `["old_field","deprecated"]`,
		})
		desc := ContextUpdateDescription(ctx)
		if desc == nil {
			t.Fatal("expected non-nil")
		}
		if desc.UpdatedFields != nil {
			t.Errorf("UpdatedFields should be nil, got %v", desc.UpdatedFields)
		}
		if len(desc.RemovedFields) != 2 {
			t.Fatalf("RemovedFields len = %d, want 2", len(desc.RemovedFields))
		}
		if desc.RemovedFields[0] != "old_field" {
			t.Errorf("RemovedFields[0] = %q, want old_field", desc.RemovedFields[0])
		}
	})

	t.Run("metadata with both fields", func(t *testing.T) {
		ctx := event.ContextWithMetadata(context.Background(), map[string]string{
			MetadataUpdatedFields: `{"status":"active"}`,
			MetadataRemovedFields: `["legacy"]`,
		})
		desc := ContextUpdateDescription(ctx)
		if desc == nil {
			t.Fatal("expected non-nil")
		}
		if desc.UpdatedFields["status"] != "active" {
			t.Errorf("UpdatedFields[status] = %v, want active", desc.UpdatedFields["status"])
		}
		if len(desc.RemovedFields) != 1 || desc.RemovedFields[0] != "legacy" {
			t.Errorf("RemovedFields = %v, want [legacy]", desc.RemovedFields)
		}
	})

	t.Run("empty string values return empty desc", func(t *testing.T) {
		ctx := event.ContextWithMetadata(context.Background(), map[string]string{
			MetadataUpdatedFields: "",
			MetadataRemovedFields: "",
		})
		desc := ContextUpdateDescription(ctx)
		if desc == nil {
			t.Fatal("expected non-nil (keys present)")
		}
		// Empty strings don't unmarshal, so fields stay nil
		if desc.UpdatedFields != nil {
			t.Errorf("UpdatedFields should be nil for empty string, got %v", desc.UpdatedFields)
		}
	})

	t.Run("malformed JSON returns partial result", func(t *testing.T) {
		ctx := event.ContextWithMetadata(context.Background(), map[string]string{
			MetadataUpdatedFields: `{invalid json`,
			MetadataRemovedFields: `["valid"]`,
		})
		desc := ContextUpdateDescription(ctx)
		if desc == nil {
			t.Fatal("expected non-nil")
		}
		// Invalid JSON should leave field nil
		if desc.UpdatedFields != nil {
			t.Errorf("UpdatedFields should be nil for invalid JSON, got %v", desc.UpdatedFields)
		}
		if len(desc.RemovedFields) != 1 {
			t.Errorf("RemovedFields should still parse, got %v", desc.RemovedFields)
		}
	})
}

// --- Option tests ---

func TestWithCollection(t *testing.T) {
	t.Run("sets collection name", func(t *testing.T) {
		tr := &Transport{}
		WithCollection("orders")(tr)
		if tr.collectionName != "orders" {
			t.Errorf("collectionName = %q, want orders", tr.collectionName)
		}
	})

	t.Run("empty string is ignored", func(t *testing.T) {
		tr := &Transport{collectionName: "existing"}
		WithCollection("")(tr)
		if tr.collectionName != "existing" {
			t.Errorf("collectionName = %q, want existing (unchanged)", tr.collectionName)
		}
	})
}

func TestWithUpdateDescription(t *testing.T) {
	tr := &Transport{}
	WithUpdateDescription()(tr)
	if !tr.includeUpdateDescription {
		t.Error("expected includeUpdateDescription = true")
	}
}

func TestWithEmptyUpdates(t *testing.T) {
	tr := &Transport{}
	WithEmptyUpdates()(tr)
	if !tr.emptyUpdates {
		t.Error("expected emptyUpdates = true")
	}
}

func TestWithMaxUpdatedFieldsSize(t *testing.T) {
	t.Run("positive value sets size and enables update description", func(t *testing.T) {
		tr := &Transport{}
		WithMaxUpdatedFieldsSize(1024)(tr)
		if tr.maxUpdatedFieldsSize != 1024 {
			t.Errorf("maxUpdatedFieldsSize = %d, want 1024", tr.maxUpdatedFieldsSize)
		}
		if !tr.includeUpdateDescription {
			t.Error("expected includeUpdateDescription = true")
		}
	})

	t.Run("zero value enables update description but no size limit", func(t *testing.T) {
		tr := &Transport{}
		WithMaxUpdatedFieldsSize(0)(tr)
		if tr.maxUpdatedFieldsSize != 0 {
			t.Errorf("maxUpdatedFieldsSize = %d, want 0", tr.maxUpdatedFieldsSize)
		}
		if !tr.includeUpdateDescription {
			t.Error("expected includeUpdateDescription = true")
		}
	})

	t.Run("negative value is ignored", func(t *testing.T) {
		tr := &Transport{}
		WithMaxUpdatedFieldsSize(-100)(tr)
		if tr.maxUpdatedFieldsSize != 0 {
			t.Errorf("maxUpdatedFieldsSize = %d, want 0", tr.maxUpdatedFieldsSize)
		}
		if !tr.includeUpdateDescription {
			t.Error("expected includeUpdateDescription = true")
		}
	})
}

func TestWithFullDocumentOnly(t *testing.T) {
	tr := &Transport{}
	WithFullDocumentOnly()(tr)
	if !tr.fullDocumentOnly {
		t.Error("expected fullDocumentOnly = true")
	}
}

func TestWithFullDocument(t *testing.T) {
	tr := &Transport{}
	WithFullDocument(FullDocumentUpdateLookup)(tr)
	if tr.fullDocument != FullDocumentUpdateLookup {
		t.Errorf("fullDocument = %q, want %q", tr.fullDocument, FullDocumentUpdateLookup)
	}
}

// --- Metadata building tests ---

func TestMetadataConstants(t *testing.T) {
	// Ensure all constants have expected values
	checks := map[string]string{
		"MetadataContentType":   MetadataContentType,
		"MetadataOperation":     MetadataOperation,
		"MetadataDatabase":      MetadataDatabase,
		"MetadataCollection":    MetadataCollection,
		"MetadataNamespace":     MetadataNamespace,
		"MetadataDocumentKey":   MetadataDocumentKey,
		"MetadataClusterTime":   MetadataClusterTime,
		"MetadataUpdatedFields": MetadataUpdatedFields,
		"MetadataRemovedFields": MetadataRemovedFields,
	}
	for name, val := range checks {
		if val == "" {
			t.Errorf("%s is empty", name)
		}
	}
}

// --- ChangeEvent JSON marshaling tests ---

func TestChangeEventJSON(t *testing.T) {
	event := ChangeEvent{
		ID:            "test-id",
		OperationType: OperationUpdate,
		Database:      "mydb",
		Collection:    "mycoll",
		DocumentKey:   "doc123",
		Namespace:     "mydb.mycoll",
		Timestamp:     time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		UpdateDesc: &UpdateDescription{
			UpdatedFields:   map[string]any{"name": "updated"},
			RemovedFields:   []string{"old"},
			TruncatedArrays: []string{"items"},
		},
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var decoded ChangeEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if decoded.ID != event.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, event.ID)
	}
	if decoded.OperationType != event.OperationType {
		t.Errorf("OperationType = %q, want %q", decoded.OperationType, event.OperationType)
	}
	if decoded.UpdateDesc == nil {
		t.Fatal("UpdateDesc is nil after roundtrip")
	}
	if len(decoded.UpdateDesc.TruncatedArrays) != 1 {
		t.Errorf("TruncatedArrays len = %d, want 1", len(decoded.UpdateDesc.TruncatedArrays))
	}
}

// --- resumeTokenKey() tests ---

func TestResumeTokenKey(t *testing.T) {
	// Collection/database levels require a real *mongo.Database which cannot
	// be constructed without a live connection, so we test cluster and default.
	t.Run("cluster level", func(t *testing.T) {
		tr := &Transport{
			watchLevel:    WatchLevelCluster,
			resumeTokenID: "host1",
		}
		got := tr.resumeTokenKey()
		want := "*.*:host1"
		if got != want {
			t.Errorf("resumeTokenKey() = %q, want %q", got, want)
		}
	})

	t.Run("unknown watch level uses default namespace", func(t *testing.T) {
		tr := &Transport{
			watchLevel:    WatchLevel(99),
			resumeTokenID: "host1",
		}
		got := tr.resumeTokenKey()
		want := "default:host1"
		if got != want {
			t.Errorf("resumeTokenKey() = %q, want %q", got, want)
		}
	})
}

// --- Status constants tests ---

func TestStatusConstants(t *testing.T) {
	if statusOpen == statusClosed {
		t.Error("statusOpen and statusClosed must be different")
	}
	if statusClosed != 0 {
		t.Errorf("statusClosed = %d, want 0", statusClosed)
	}
	if statusOpen != 1 {
		t.Errorf("statusOpen = %d, want 1", statusOpen)
	}
}

// --- WatchLevel string tests ---

func TestWatchLevelString(t *testing.T) {
	tests := []struct {
		level WatchLevel
		want  string
	}{
		{WatchLevelCollection, "collection"},
		{WatchLevelDatabase, "database"},
		{WatchLevelCluster, "cluster"},
		{WatchLevel(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("level_%d", tt.level), func(t *testing.T) {
			tr := &Transport{watchLevel: tt.level}
			got := tr.watchLevelString()
			if got != tt.want {
				t.Errorf("watchLevelString() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- FullDocumentOption tests ---

func TestFullDocumentOptionToDriver(t *testing.T) {
	tests := []struct {
		option FullDocumentOption
		panics bool
	}{
		{FullDocumentDefault, false},
		{FullDocumentUpdateLookup, false},
		{FullDocumentWhenAvailable, false},
		{FullDocumentRequired, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.option), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil && !tt.panics {
					t.Errorf("toDriverOption panicked: %v", r)
				}
			}()
			_ = tt.option.toDriverOption()
		})
	}
}

// --- New() constructor tests ---

func TestNewRequiresDatabase(t *testing.T) {
	_, err := New(nil)
	if err != ErrDatabaseRequired {
		t.Errorf("New(nil) = %v, want ErrDatabaseRequired", err)
	}
}

func TestNewClusterWatchRequiresClient(t *testing.T) {
	_, err := NewClusterWatch(nil)
	if err != ErrClientRequired {
		t.Errorf("NewClusterWatch(nil) = %v, want ErrClientRequired", err)
	}
}

func TestNewValidationErrors(t *testing.T) {
	// We cannot construct a real *mongo.Database without a live connection,
	// but we can test that validation runs by using options that trigger
	// validation errors. Since New() requires a non-nil db, we test the
	// validation through the validate() method directly and verify New()
	// returns the same errors through option combination tests.

	t.Run("maxUpdatedFieldsSize without fullDocument via options", func(t *testing.T) {
		tr := &Transport{}
		WithMaxUpdatedFieldsSize(1024)(tr)
		err := tr.validate()
		if err != ErrMaxUpdatedFieldsSizeRequiresFull {
			t.Errorf("validate() = %v, want ErrMaxUpdatedFieldsSizeRequiresFull", err)
		}
	})

	t.Run("fullDocumentOnly without fullDocument via options", func(t *testing.T) {
		tr := &Transport{}
		WithFullDocumentOnly()(tr)
		err := tr.validate()
		if err != ErrFullDocumentRequired {
			t.Errorf("validate() = %v, want ErrFullDocumentRequired", err)
		}
	})
}

// --- Publish returns ErrPublishNotSupported ---

func TestPublishNotSupported(t *testing.T) {
	tr := &Transport{}
	err := tr.Publish(context.Background(), "test-event", nil)
	if err != ErrPublishNotSupported {
		t.Errorf("Publish() = %v, want ErrPublishNotSupported", err)
	}
}

// --- isOpen() tests ---

func TestIsOpen(t *testing.T) {
	t.Run("open transport", func(t *testing.T) {
		tr := &Transport{status: statusOpen}
		if !tr.isOpen() {
			t.Error("expected isOpen() = true for statusOpen")
		}
	})

	t.Run("closed transport", func(t *testing.T) {
		tr := &Transport{status: statusClosed}
		if tr.isOpen() {
			t.Error("expected isOpen() = false for statusClosed")
		}
	})
}

// --- isChangeStreamHistoryLost() tests ---

func TestIsChangeStreamHistoryLost(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"unrelated error", errors.New("connection refused"), false},
		{"ChangeStreamHistoryLost in message", errors.New("ChangeStreamHistoryLost: oplog truncated"), true},
		{"resume point message", errors.New("resume point may no longer be in the oplog"), true},
		{"partial match ChangeStreamHistoryLost", errors.New("error code 286 ChangeStreamHistoryLost"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isChangeStreamHistoryLost(tt.err)
			if got != tt.want {
				t.Errorf("isChangeStreamHistoryLost(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// --- Additional Option tests ---

func TestWithLogger(t *testing.T) {
	t.Run("sets logger", func(t *testing.T) {
		tr := &Transport{}
		logger := slog.Default()
		WithLogger(logger)(tr)
		if tr.logger != logger {
			t.Error("expected logger to be set")
		}
	})

	t.Run("nil logger is ignored", func(t *testing.T) {
		existing := slog.Default()
		tr := &Transport{logger: existing}
		WithLogger(nil)(tr)
		if tr.logger != existing {
			t.Error("expected logger to remain unchanged")
		}
	})
}

func TestWithErrorHandler(t *testing.T) {
	t.Run("sets handler", func(t *testing.T) {
		tr := &Transport{}
		called := false
		WithErrorHandler(func(error) { called = true })(tr)
		if tr.onError == nil {
			t.Fatal("expected onError to be set")
		}
		tr.onError(nil)
		if !called {
			t.Error("expected handler to be called")
		}
	})

	t.Run("nil handler is ignored", func(t *testing.T) {
		existing := func(error) {}
		tr := &Transport{onError: existing}
		WithErrorHandler(nil)(tr)
		if tr.onError == nil {
			t.Error("expected onError to remain set")
		}
	})
}

func TestWithBufferSize(t *testing.T) {
	t.Run("positive value", func(t *testing.T) {
		tr := &Transport{}
		WithBufferSize(256)(tr)
		if tr.bufferSize != 256 {
			t.Errorf("bufferSize = %d, want 256", tr.bufferSize)
		}
	})

	t.Run("zero is ignored", func(t *testing.T) {
		tr := &Transport{bufferSize: 100}
		WithBufferSize(0)(tr)
		if tr.bufferSize != 100 {
			t.Errorf("bufferSize = %d, want 100 (unchanged)", tr.bufferSize)
		}
	})

	t.Run("negative is ignored", func(t *testing.T) {
		tr := &Transport{bufferSize: 100}
		WithBufferSize(-5)(tr)
		if tr.bufferSize != 100 {
			t.Errorf("bufferSize = %d, want 100 (unchanged)", tr.bufferSize)
		}
	})
}

func TestWithResumeTokenStore(t *testing.T) {
	tr := &Transport{}
	store := &MongoResumeTokenStore{}
	WithResumeTokenStore(store)(tr)
	if tr.resumeTokenStore != store {
		t.Error("expected resumeTokenStore to be set")
	}
}

func TestWithoutResume(t *testing.T) {
	tr := &Transport{}
	WithoutResume()(tr)
	if !tr.disableResume {
		t.Error("expected disableResume = true")
	}
}

func TestWithResumeTokenID(t *testing.T) {
	tr := &Transport{}
	WithResumeTokenID("instance-42")(tr)
	if tr.resumeTokenID != "instance-42" {
		t.Errorf("resumeTokenID = %q, want instance-42", tr.resumeTokenID)
	}
}

func TestWithAckStore(t *testing.T) {
	tr := &Transport{}
	store := &MongoAckStore{}
	WithAckStore(store)(tr)
	if tr.ackStore != store {
		t.Error("expected ackStore to be set")
	}
}

func TestWithPipeline(t *testing.T) {
	tr := &Transport{}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
	}
	WithPipeline(pipeline)(tr)
	if len(tr.pipeline) != 1 {
		t.Errorf("pipeline len = %d, want 1", len(tr.pipeline))
	}
}

func TestWithBatchSize(t *testing.T) {
	tr := &Transport{}
	WithBatchSize(50)(tr)
	if tr.batchSize == nil || *tr.batchSize != 50 {
		t.Errorf("batchSize = %v, want 50", tr.batchSize)
	}
}

func TestWithMaxAwaitTime(t *testing.T) {
	tr := &Transport{}
	d := 5 * time.Second
	WithMaxAwaitTime(d)(tr)
	if tr.maxAwaitTime == nil || *tr.maxAwaitTime != d {
		t.Errorf("maxAwaitTime = %v, want %v", tr.maxAwaitTime, d)
	}
}

// --- bsonToJSON() tests ---

func TestBsonToJSON(t *testing.T) {
	t.Run("simple document", func(t *testing.T) {
		doc := bson.M{
			"name":  "test",
			"count": int32(42),
		}
		data, err := bsonToJSON(doc)
		if err != nil {
			t.Fatalf("bsonToJSON() error: %v", err)
		}

		var result map[string]any
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("json.Unmarshal failed: %v", err)
		}
		if result["name"] != "test" {
			t.Errorf("name = %v, want test", result["name"])
		}
	})

	t.Run("document with ObjectID", func(t *testing.T) {
		oid := primitive.NewObjectID()
		doc := bson.M{"_id": oid}
		data, err := bsonToJSON(doc)
		if err != nil {
			t.Fatalf("bsonToJSON() error: %v", err)
		}

		var result map[string]any
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("json.Unmarshal failed: %v", err)
		}
		idMap, ok := result["_id"].(map[string]any)
		if !ok {
			t.Fatalf("expected _id to be a map, got %T", result["_id"])
		}
		if idMap["$oid"] != oid.Hex() {
			t.Errorf("$oid = %v, want %v", idMap["$oid"], oid.Hex())
		}
	})

	t.Run("nested arrays", func(t *testing.T) {
		doc := bson.M{
			"tags": bson.A{"go", "mongodb"},
		}
		data, err := bsonToJSON(doc)
		if err != nil {
			t.Fatalf("bsonToJSON() error: %v", err)
		}

		var result map[string]any
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("json.Unmarshal failed: %v", err)
		}
		tags, ok := result["tags"].([]any)
		if !ok {
			t.Fatalf("expected tags to be array, got %T", result["tags"])
		}
		if len(tags) != 2 {
			t.Errorf("tags len = %d, want 2", len(tags))
		}
	})
}

// --- Error sentinel value tests ---

func TestErrorValues(t *testing.T) {
	// Verify error messages are meaningful
	errors := map[string]error{
		"ErrClientRequired":                    ErrClientRequired,
		"ErrDatabaseRequired":                  ErrDatabaseRequired,
		"ErrPublishNotSupported":               ErrPublishNotSupported,
		"ErrMaxUpdatedFieldsSizeRequiresFull":   ErrMaxUpdatedFieldsSizeRequiresFull,
		"ErrFullDocumentRequired":              ErrFullDocumentRequired,
	}
	for name, err := range errors {
		if err == nil {
			t.Errorf("%s is nil", name)
		}
		if err.Error() == "" {
			t.Errorf("%s has empty message", name)
		}
	}
}

// --- OperationType constants tests ---

func TestOperationTypeConstants(t *testing.T) {
	tests := []struct {
		op   OperationType
		want string
	}{
		{OperationInsert, "insert"},
		{OperationUpdate, "update"},
		{OperationReplace, "replace"},
		{OperationDelete, "delete"},
	}

	for _, tt := range tests {
		if string(tt.op) != tt.want {
			t.Errorf("OperationType %v = %q, want %q", tt.op, string(tt.op), tt.want)
		}
	}
}

// --- WatchLevel constants tests ---

func TestWatchLevelConstants(t *testing.T) {
	// Verify values are distinct
	levels := []WatchLevel{WatchLevelCollection, WatchLevelDatabase, WatchLevelCluster}
	seen := make(map[WatchLevel]bool)
	for _, l := range levels {
		if seen[l] {
			t.Errorf("duplicate WatchLevel value: %d", l)
		}
		seen[l] = true
	}
}

// --- FullDocumentOption constants tests ---

func TestFullDocumentOptionConstants(t *testing.T) {
	tests := []struct {
		option FullDocumentOption
		want   string
	}{
		{FullDocumentDefault, "default"},
		{FullDocumentUpdateLookup, "updateLookup"},
		{FullDocumentWhenAvailable, "whenAvailable"},
		{FullDocumentRequired, "required"},
	}

	for _, tt := range tests {
		if string(tt.option) != tt.want {
			t.Errorf("FullDocumentOption %v = %q, want %q", tt.option, string(tt.option), tt.want)
		}
	}
}

// --- extractChangeEvent additional edge cases ---

func TestExtractChangeEvent_EdgeCases(t *testing.T) {
	tr := &Transport{}

	t.Run("empty raw document", func(t *testing.T) {
		raw := bson.M{}
		ev := tr.extractChangeEvent(raw)
		// Should not panic, ID should be auto-generated
		if ev.ID == "" {
			t.Error("expected auto-generated ID")
		}
		if ev.OperationType != "" {
			t.Errorf("OperationType = %q, want empty", ev.OperationType)
		}
	})

	t.Run("ns with only db", func(t *testing.T) {
		raw := bson.M{
			"ns": bson.M{"db": "mydb"},
		}
		ev := tr.extractChangeEvent(raw)
		if ev.Database != "mydb" {
			t.Errorf("Database = %q, want mydb", ev.Database)
		}
		if ev.Collection != "" {
			t.Errorf("Collection = %q, want empty", ev.Collection)
		}
	})

	t.Run("clusterTime sets timestamp", func(t *testing.T) {
		ts := primitive.Timestamp{T: 1700000000, I: 1}
		raw := bson.M{
			"clusterTime": ts,
		}
		ev := tr.extractChangeEvent(raw)
		if ev.Timestamp.Unix() != 1700000000 {
			t.Errorf("Timestamp unix = %d, want 1700000000", ev.Timestamp.Unix())
		}
	})

	t.Run("documentKey with int32 id", func(t *testing.T) {
		raw := bson.M{
			"documentKey": bson.M{"_id": int32(42)},
		}
		ev := tr.extractChangeEvent(raw)
		if ev.DocumentKey != "42" {
			t.Errorf("DocumentKey = %q, want 42", ev.DocumentKey)
		}
	})

	t.Run("documentKey with int64 id", func(t *testing.T) {
		raw := bson.M{
			"documentKey": bson.M{"_id": int64(999)},
		}
		ev := tr.extractChangeEvent(raw)
		if ev.DocumentKey != "999" {
			t.Errorf("DocumentKey = %q, want 999", ev.DocumentKey)
		}
	})

	t.Run("updateDescription with empty fields", func(t *testing.T) {
		raw := bson.M{
			"updateDescription": bson.M{
				"updatedFields": bson.M{},
				"removedFields": bson.A{},
			},
		}
		ev := tr.extractChangeEvent(raw)
		if ev.UpdateDesc == nil {
			t.Fatal("UpdateDesc should not be nil")
		}
		if len(ev.UpdateDesc.UpdatedFields) != 0 {
			t.Errorf("UpdatedFields len = %d, want 0", len(ev.UpdateDesc.UpdatedFields))
		}
		if len(ev.UpdateDesc.RemovedFields) != 0 {
			t.Errorf("RemovedFields len = %d, want 0", len(ev.UpdateDesc.RemovedFields))
		}
	})
}

// --- Store compile-time interface checks ---

func TestStoreInterfaceCompliance(t *testing.T) {
	// These are compile-time checks; if they compile, the test passes.
	var _ ResumeTokenStore = (*MongoResumeTokenStore)(nil)
	var _ AckStore = (*MongoAckStore)(nil)
}

// --- NewMongoResumeTokenStore constructor ---

func TestNewMongoResumeTokenStore(t *testing.T) {
	// Can only verify the constructor does not panic with a nil collection
	// (actual operations would fail, but construction should work)
	store := NewMongoResumeTokenStore(nil)
	if store == nil {
		t.Error("expected non-nil store")
	}
}

// --- NewMongoAckStore constructor ---

func TestNewMongoAckStore(t *testing.T) {
	store := NewMongoAckStore(nil, 24*time.Hour)
	if store == nil {
		t.Error("expected non-nil store")
	}
	if store.ttl != 24*time.Hour {
		t.Errorf("ttl = %v, want 24h", store.ttl)
	}
}

// --- DefaultResumeTokenCollection constant ---

func TestDefaultResumeTokenCollection(t *testing.T) {
	if DefaultResumeTokenCollection == "" {
		t.Error("DefaultResumeTokenCollection should not be empty")
	}
	if DefaultResumeTokenCollection != "_event_resume_tokens" {
		t.Errorf("DefaultResumeTokenCollection = %q, want _event_resume_tokens", DefaultResumeTokenCollection)
	}
}

