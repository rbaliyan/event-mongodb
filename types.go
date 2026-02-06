package mongodb

import (
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// OperationType represents the type of change operation
type OperationType string

const (
	OperationInsert  OperationType = "insert"
	OperationUpdate  OperationType = "update"
	OperationReplace OperationType = "replace"
	OperationDelete  OperationType = "delete"
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

// UpdateDescription contains details about an update operation
type UpdateDescription struct {
	UpdatedFields   map[string]any `json:"updated_fields,omitempty"`
	RemovedFields   []string       `json:"removed_fields,omitempty"`
	TruncatedArrays []string       `json:"truncated_arrays,omitempty"`
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
