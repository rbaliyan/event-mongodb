package mongodb

import (
	"context"
	"encoding/json"
	"log/slog"

	event "github.com/rbaliyan/event/v3"
)

// Metadata keys for event context.
const (
	// MetadataContentType is the MIME type of the message payload (e.g. "application/json").
	MetadataContentType = "Content-Type"
	// MetadataOperation is the MongoDB change stream operation type (insert, update, replace, delete).
	MetadataOperation = "operation"
	// MetadataDatabase is the name of the MongoDB database where the change occurred.
	MetadataDatabase = "database"
	// MetadataCollection is the name of the MongoDB collection where the change occurred.
	MetadataCollection = "collection"
	// MetadataNamespace is the fully qualified namespace in "database.collection" format.
	MetadataNamespace = "namespace"
	// MetadataDocumentKey is the string representation of the changed document's _id field.
	MetadataDocumentKey = "document_key"
	// MetadataClusterTime is the MongoDB cluster time of the change event in RFC3339Nano format.
	MetadataClusterTime = "cluster_time"
	// MetadataUpdatedFields is the JSON-encoded map of fields updated by an update operation.
	// Set only when WithUpdateDescription() is enabled on the transport.
	MetadataUpdatedFields = "updated_fields"
	// MetadataRemovedFields is the JSON-encoded array of field names removed by an update operation.
	// Set only when WithUpdateDescription() is enabled on the transport.
	MetadataRemovedFields = "removed_fields"
)

// ContextUpdateDescription extracts UpdateDescription from event context metadata.
// Returns nil if metadata doesn't contain update description fields.
// Requires WithUpdateDescription() to be set on the transport.
func ContextUpdateDescription(ctx context.Context) *UpdateDescription {
	md := event.ContextMetadata(ctx)
	if md == nil {
		return nil
	}
	updated, hasUpdated := md[MetadataUpdatedFields]
	removed, hasRemoved := md[MetadataRemovedFields]
	if !hasUpdated && !hasRemoved {
		return nil
	}
	desc := &UpdateDescription{}
	if hasUpdated && updated != "" {
		if err := json.Unmarshal([]byte(updated), &desc.UpdatedFields); err != nil {
			slog.DebugContext(ctx, "failed to unmarshal updated_fields metadata", "error", err)
		}
	}
	if hasRemoved && removed != "" {
		if err := json.Unmarshal([]byte(removed), &desc.RemovedFields); err != nil {
			slog.DebugContext(ctx, "failed to unmarshal removed_fields metadata", "error", err)
		}
	}
	return desc
}
