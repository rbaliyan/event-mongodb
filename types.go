package mongodb

import (
	"encoding/json"
	"slices"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// OperationType represents the type of change operation
type OperationType string

const (
	// OperationInsert represents a document insert operation.
	OperationInsert OperationType = "insert"
	// OperationUpdate represents a document update operation (partial field changes).
	OperationUpdate OperationType = "update"
	// OperationReplace represents a full document replacement operation.
	OperationReplace OperationType = "replace"
	// OperationDelete represents a document delete operation.
	OperationDelete OperationType = "delete"
)

// FullDocumentOption specifies how to return full documents in change events.
//
// For insert and replace operations, the full document is always included.
// This option controls behavior for update and delete operations.
type FullDocumentOption string

const (
	// FullDocumentDefault returns the full document only for insert and replace.
	// Update events only include the changed fields in UpdateDescription.
	// Delete events don't include the document.
	FullDocumentDefault FullDocumentOption = "default"

	// FullDocumentUpdateLookup performs a lookup to return the current document
	// for update events. Note: the document returned is the current state,
	// which may have been modified by subsequent updates.
	// This is the most commonly used option for update events.
	FullDocumentUpdateLookup FullDocumentOption = "updateLookup"

	// FullDocumentWhenAvailable returns the post-image if available.
	// Requires MongoDB 6.0+ with document pre/post images enabled on the collection.
	// Falls back gracefully if not available.
	FullDocumentWhenAvailable FullDocumentOption = "whenAvailable"

	// FullDocumentRequired returns the post-image or fails if not available.
	// Requires MongoDB 6.0+ with document pre/post images enabled on the collection.
	// Use this when you must have the full document for every event.
	FullDocumentRequired FullDocumentOption = "required"
)

// toDriverOption converts our FullDocumentOption to MongoDB driver's option.
func (f FullDocumentOption) toDriverOption() options.FullDocument {
	switch f {
	case FullDocumentUpdateLookup:
		return options.UpdateLookup
	case FullDocumentWhenAvailable:
		return options.WhenAvailable
	case FullDocumentRequired:
		return options.Required
	default:
		return options.Default
	}
}

// ChangeEvent represents a MongoDB change stream event.
// This struct is JSON-serializable for compatibility with the event system's default codec.
type ChangeEvent struct {
	ID            string             `json:"id"`
	OperationType OperationType      `json:"operation_type"`
	Database      string             `json:"database"`
	Collection    string             `json:"collection"`
	DocumentKey   string             `json:"document_key"`            // String representation of _id (hex for ObjectID, string for others)
	FullDocument  json.RawMessage    `json:"full_document,omitempty"` // Raw JSON of the full document
	UpdateDesc    *UpdateDescription `json:"update_description,omitempty"`
	Timestamp     time.Time          `json:"timestamp"`
	Namespace     string             `json:"namespace"` // "database.collection" format
}

// UpdateDescription contains details about an update operation.
//
// Use [Field] for typed extraction from UpdatedFields, [HasField] for
// presence checks, and [HasFieldRemoved] for deletion checks. These
// accessors handle nil receivers and the JSON/BSON type divergence
// transparently.
type UpdateDescription struct {
	UpdatedFields   map[string]any `json:"updated_fields,omitempty"`
	RemovedFields   []string       `json:"removed_fields,omitempty"`
	TruncatedArrays []string       `json:"truncated_arrays,omitempty"`
}

// HasField reports whether key is present in UpdatedFields.
func (d *UpdateDescription) HasField(key string) bool {
	if d == nil {
		return false
	}
	_, ok := d.UpdatedFields[key]
	return ok
}

// HasFieldRemoved reports whether key was removed (unset) by the update.
func (d *UpdateDescription) HasFieldRemoved(key string) bool {
	if d == nil {
		return false
	}
	return slices.Contains(d.RemovedFields, key)
}

// Field extracts a typed value from UpdatedFields. Returns the zero
// value and false if the key is absent, the receiver is nil, or the
// stored value cannot be converted to T.
//
// Field performs cross-conversion between numeric types (int, int32,
// int64, float32, float64) so the same call works whether
// UpdatedFields was populated via BSON decoding (ChangeEvent.UpdateDesc
// — values are int32, int64, etc.) or JSON decoding
// ([ContextUpdateDescription] — all numbers are float64).
//
//	status, ok := mongodb.Field[string](desc, "status")
//	count, ok := mongodb.Field[int64](desc, "retry_count")
func Field[T any](desc *UpdateDescription, key string) (T, bool) {
	var zero T
	if desc == nil {
		return zero, false
	}
	v, ok := desc.UpdatedFields[key]
	if !ok || v == nil {
		return zero, false
	}
	if typed, ok := v.(T); ok {
		return typed, true
	}
	// Numeric cross-conversion: BSON gives int32/int64, JSON gives float64.
	if out, ok := coerceNumeric[T](v); ok {
		return out, true
	}
	return zero, false
}

// coerceNumeric attempts to convert v to the target numeric type T.
// Returns false if v is not a numeric type or T is not numeric.
func coerceNumeric[T any](v any) (T, bool) {
	var zero T

	// Extract source as float64 (universal intermediate).
	var f float64
	switch src := v.(type) {
	case float64:
		f = src
	case float32:
		f = float64(src)
	case int:
		f = float64(src)
	case int32:
		f = float64(src)
	case int64:
		f = float64(src)
	default:
		return zero, false
	}

	// Convert to target type.
	var result any
	switch any(*new(T)).(type) {
	case float64:
		result = f
	case float32:
		result = float32(f)
	case int:
		result = int(f)
	case int32:
		result = int32(f)
	case int64:
		result = int64(f)
	default:
		return zero, false
	}

	out, ok := result.(T)
	return out, ok
}

// changeStreamDoc represents the MongoDB change stream document structure.
// Using a struct with bson tags provides type safety and cleaner code than
// manual bson.D parsing. See: https://www.mongodb.com/docs/manual/reference/change-events/
type changeStreamDoc struct {
	ID            changeStreamID      `bson:"_id"`
	OperationType string              `bson:"operationType"`
	NS            changeStreamNS      `bson:"ns"`
	DocumentKey   bson.D              `bson:"documentKey"`
	FullDocument  bson.Raw            `bson:"fullDocument,omitempty"`
	UpdateDesc    *changeStreamUpdate `bson:"updateDescription,omitempty"`
	ClusterTime   bson.Timestamp      `bson:"clusterTime"`
}

type changeStreamID struct {
	Data string `bson:"_data"`
}

type changeStreamNS struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

type changeStreamUpdate struct {
	UpdatedFields   bson.D           `bson:"updatedFields"`
	RemovedFields   []string         `bson:"removedFields"`
	TruncatedArrays []truncatedArray `bson:"truncatedArrays,omitempty"`
}

type truncatedArray struct {
	Field   string `bson:"field"`
	NewSize int32  `bson:"newSize"`
}
