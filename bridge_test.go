package mongodb_test

import (
	"context"
	"testing"
	"time"

	mongodb "github.com/rbaliyan/event-mongodb"
	"github.com/rbaliyan/event/v3/transport/bridge"
)

func TestDedupKeyFromChangeStream(t *testing.T) {
	keyFn := mongodb.DedupKeyFromChangeStream()

	tests := []struct {
		name string
		meta map[string]string
		want string
	}{
		{
			name: "all fields present",
			meta: map[string]string{
				mongodb.MetadataClusterTime: "1234567890",
				mongodb.MetadataNamespace:   "mydb.orders",
				mongodb.MetadataDocumentKey: "abc123",
			},
			want: "1234567890:mydb.orders:abc123",
		},
		{
			name: "missing cluster_time",
			meta: map[string]string{
				mongodb.MetadataNamespace:   "mydb.orders",
				mongodb.MetadataDocumentKey: "abc123",
			},
			want: "",
		},
		{
			name: "missing namespace",
			meta: map[string]string{
				mongodb.MetadataClusterTime: "1234567890",
				mongodb.MetadataDocumentKey: "abc123",
			},
			want: "",
		},
		{
			name: "missing document_key",
			meta: map[string]string{
				mongodb.MetadataClusterTime: "1234567890",
				mongodb.MetadataNamespace:   "mydb.orders",
			},
			want: "",
		},
		{
			name: "nil metadata",
			meta: nil,
			want: "",
		},
		{
			name: "empty metadata",
			meta: map[string]string{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &fakeMsg{id: "test", meta: tt.meta}
			got := keyFn(msg)
			if got != tt.want {
				t.Errorf("DedupKeyFromChangeStream() = %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("nil message", func(t *testing.T) {
		if got := keyFn(nil); got != "" {
			t.Errorf("nil message: got %q, want empty", got)
		}
	})

	// Verify it satisfies the bridge.DedupKeyFn type.
	var _ bridge.DedupKeyFn = keyFn
}

// fakeMsg is a minimal transport.Message for tests.
type fakeMsg struct {
	id   string
	meta map[string]string
}

func (m *fakeMsg) ID() string                  { return m.id }
func (m *fakeMsg) Source() string              { return "test" }
func (m *fakeMsg) Payload() []byte             { return nil }
func (m *fakeMsg) Metadata() map[string]string { return m.meta }
func (m *fakeMsg) Timestamp() time.Time        { return time.Time{} }
func (m *fakeMsg) RetryCount() int             { return 0 }
func (m *fakeMsg) Context() context.Context    { return context.Background() }
func (m *fakeMsg) Ack(error) error             { return nil }
