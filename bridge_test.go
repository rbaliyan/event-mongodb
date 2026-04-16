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

	t.Run("returns message ID", func(t *testing.T) {
		msg := &fakeMsg{id: "8264BEB9F3...resumetoken", meta: map[string]string{
			mongodb.MetadataClusterTime: "2024-04-16T22:10:00Z",
			mongodb.MetadataNamespace:   "mydb.orders",
			mongodb.MetadataDocumentKey: "abc123",
		}}
		got := keyFn(msg)
		if got != "8264BEB9F3...resumetoken" {
			t.Errorf("DedupKeyFromChangeStream() = %q, want resume token ID", got)
		}
	})

	t.Run("two events same second same doc get distinct keys via resume token", func(t *testing.T) {
		// Simulates two updates within the same second — they have different resume
		// tokens (different oplog ordinals) so they must not collide.
		msg1 := &fakeMsg{id: "resumetoken-ordinal-1", meta: map[string]string{
			mongodb.MetadataClusterTime: "2024-04-16T22:10:00Z",
			mongodb.MetadataNamespace:   "mydb.calls",
			mongodb.MetadataDocumentKey: "docid",
		}}
		msg2 := &fakeMsg{id: "resumetoken-ordinal-2", meta: map[string]string{
			mongodb.MetadataClusterTime: "2024-04-16T22:10:00Z", // same second
			mongodb.MetadataNamespace:   "mydb.calls",
			mongodb.MetadataDocumentKey: "docid", // same document
		}}
		key1 := keyFn(msg1)
		key2 := keyFn(msg2)
		if key1 == key2 {
			t.Errorf("two events in same second must have distinct dedup keys, both got %q", key1)
		}
	})

	t.Run("empty ID returns empty key", func(t *testing.T) {
		msg := &fakeMsg{id: "", meta: map[string]string{}}
		if got := keyFn(msg); got != "" {
			t.Errorf("empty ID: got %q, want empty", got)
		}
	})

	t.Run("nil message returns empty key", func(t *testing.T) {
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
