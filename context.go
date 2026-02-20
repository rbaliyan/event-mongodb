package mongodb

import (
	"context"
	"encoding/json"
	"log/slog"

	event "github.com/rbaliyan/event/v3"
)

// Metadata keys for event context.
const (
	MetadataContentType   = "Content-Type"
	MetadataOperation     = "operation"
	MetadataDatabase      = "database"
	MetadataCollection    = "collection"
	MetadataNamespace     = "namespace"
	MetadataDocumentKey   = "document_key"
	MetadataClusterTime   = "cluster_time"
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
