package mongodb

// EventType specifies which change stream operation an event listens to.
// This provides a standard CRUD-to-operation mapping for MongoDB change streams,
// allowing callers to register events for specific operation types with automatic
// pre-decode filtering via MessageFilter.
type EventType int

const (
	// EventCreated listens for insert operations.
	EventCreated EventType = iota
	// EventUpdated listens for update and replace operations.
	EventUpdated
	// EventDeleted listens for delete operations.
	EventDeleted
)

// String returns the human-readable name of the event type.
func (t EventType) String() string {
	switch t {
	case EventCreated:
		return "created"
	case EventUpdated:
		return "updated"
	case EventDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// Operations returns the MongoDB change stream operation types for this event type.
func (t EventType) Operations() []OperationType {
	switch t {
	case EventCreated:
		return []OperationType{OperationInsert}
	case EventUpdated:
		return []OperationType{OperationUpdate, OperationReplace}
	case EventDeleted:
		return []OperationType{OperationDelete}
	default:
		return nil
	}
}

// MessageFilter returns a pre-decode filter function for use with event.WithMessageFilter.
// It returns true only for messages whose "operation" metadata matches this event type's
// operations, allowing non-matching messages to be skipped before payload decoding.
//
// For unknown event types (where Operations returns nil), the returned filter rejects
// all messages.
//
// Example:
//
//	evt := event.New[Model]("order.created",
//	    event.WithPayloadCodec(payload.BSON{}),
//	    event.WithMessageFilter(mongodb.EventCreated.MessageFilter()),
//	)
func (t EventType) MessageFilter() func(map[string]string) bool {
	ops := t.Operations()
	allowed := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		allowed[string(op)] = struct{}{}
	}
	return func(meta map[string]string) bool {
		_, ok := allowed[meta[MetadataOperation]]
		return ok
	}
}
