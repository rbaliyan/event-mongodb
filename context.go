package mongodb

import (
	"context"
	"encoding/json"

	event "github.com/rbaliyan/event/v3"
)

// Metadata keys for event context.
const (
	MetadataContentType  = "Content-Type"
	MetadataOperation    = "operation"
	MetadataDatabase     = "database"
	MetadataCollection   = "collection"
	MetadataNamespace    = "namespace"
	MetadataDocumentKey  = "document_key"
	MetadataClusterTime  = "cluster_time"
	MetadataUpdatedFields = "updated_fields"
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
		json.Unmarshal([]byte(updated), &desc.UpdatedFields) //nolint:errcheck
	}
	if hasRemoved && removed != "" {
		json.Unmarshal([]byte(removed), &desc.RemovedFields) //nolint:errcheck
	}
	return desc
}
