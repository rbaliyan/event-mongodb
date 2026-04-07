package mongodb

import "testing"

func TestEventType_String(t *testing.T) {
	tests := []struct {
		et   EventType
		want string
	}{
		{EventCreated, "created"},
		{EventUpdated, "updated"},
		{EventDeleted, "deleted"},
		{EventType(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.et.String(); got != tt.want {
			t.Errorf("EventType(%d).String() = %q, want %q", tt.et, got, tt.want)
		}
	}
}

func TestEventType_Operations(t *testing.T) {
	tests := []struct {
		et   EventType
		want []OperationType
	}{
		{EventCreated, []OperationType{OperationInsert}},
		{EventUpdated, []OperationType{OperationUpdate, OperationReplace}},
		{EventDeleted, []OperationType{OperationDelete}},
		{EventType(99), nil},
	}
	for _, tt := range tests {
		got := tt.et.Operations()
		if len(got) != len(tt.want) {
			t.Errorf("EventType(%d).Operations() returned %d ops, want %d", tt.et, len(got), len(tt.want))
			continue
		}
		for i, op := range got {
			if op != tt.want[i] {
				t.Errorf("EventType(%d).Operations()[%d] = %q, want %q", tt.et, i, op, tt.want[i])
			}
		}
	}
}

func TestEventType_MessageFilter(t *testing.T) {
	tests := []struct {
		et        EventType
		operation string
		want      bool
	}{
		// Created matches insert only
		{EventCreated, "insert", true},
		{EventCreated, "update", false},
		{EventCreated, "replace", false},
		{EventCreated, "delete", false},
		// Updated matches update and replace
		{EventUpdated, "update", true},
		{EventUpdated, "replace", true},
		{EventUpdated, "insert", false},
		{EventUpdated, "delete", false},
		// Deleted matches delete only
		{EventDeleted, "delete", true},
		{EventDeleted, "insert", false},
		{EventDeleted, "update", false},
		// Missing operation
		{EventCreated, "", false},
	}
	for _, tt := range tests {
		filter := tt.et.MessageFilter()
		meta := map[string]string{"operation": tt.operation}
		if got := filter(meta); got != tt.want {
			t.Errorf("EventType(%d).MessageFilter()(operation=%q) = %v, want %v", tt.et, tt.operation, got, tt.want)
		}
	}
}
