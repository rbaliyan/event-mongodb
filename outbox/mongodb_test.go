package outbox

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	evtoutbox "github.com/rbaliyan/event/v3/outbox"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestWithCollection(t *testing.T) {
	t.Run("custom name applies", func(t *testing.T) {
		o := &mongoStoreOptions{collection: "event_outbox"}
		WithCollection("custom_outbox")(o)
		if o.collection != "custom_outbox" {
			t.Fatalf("expected collection %q, got %q", "custom_outbox", o.collection)
		}
	})

	t.Run("empty name is ignored", func(t *testing.T) {
		o := &mongoStoreOptions{collection: "event_outbox"}
		WithCollection("")(o)
		if o.collection != "event_outbox" {
			t.Fatalf("expected default collection %q to remain, got %q", "event_outbox", o.collection)
		}
	})
}

func TestNewMongoStoreNilDB(t *testing.T) {
	store, err := NewMongoStore(nil)
	if err == nil {
		t.Fatal("expected error for nil database, got nil")
	}
	if store != nil {
		t.Fatalf("expected nil store, got %v", store)
	}
}

func TestNewMongoPublisherNilClient(t *testing.T) {
	pub, err := NewMongoPublisher(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil client, got nil")
	}
	if pub != nil {
		t.Fatalf("expected nil publisher, got %v", pub)
	}
}

func TestIsNamespaceNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"ns not found", errors.New("ns not found"), true},
		{"NamespaceNotFound", errors.New("NamespaceNotFound: db.coll"), true},
		{"Collection X not found", errors.New("Collection foo not found"), true},
		{"unrelated error", errors.New("connection refused"), false},
		{"partial collection only", errors.New("Collection exists already"), false},
		{"partial not found only", errors.New("document not found"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNamespaceNotFoundError(tt.err); got != tt.want {
				t.Errorf("isNamespaceNotFoundError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestMongoMessageToMessage(t *testing.T) {
	objID := bson.NewObjectID()
	created := time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
	published := time.Date(2026, 6, 19, 11, 0, 0, 0, time.UTC)

	mm := &mongoMessage{
		ID:          objID,
		EventName:   "order.created",
		EventID:     "evt-123",
		Payload:     []byte(`{"id":1}`),
		Metadata:    map[string]string{"k": "v"},
		CreatedAt:   created,
		PublishedAt: &published,
		Status:      evtoutbox.StatusPublished,
		RetryCount:  3,
		LastError:   "boom",
		Priority:    7,
	}

	got := mm.toMessage()

	if got.ID != objID.Timestamp().Unix() {
		t.Errorf("ID = %d, want %d", got.ID, objID.Timestamp().Unix())
	}
	if got.EventName != "order.created" {
		t.Errorf("EventName = %q, want %q", got.EventName, "order.created")
	}
	if got.EventID != "evt-123" {
		t.Errorf("EventID = %q, want %q", got.EventID, "evt-123")
	}
	if string(got.Payload) != `{"id":1}` {
		t.Errorf("Payload = %q, want %q", string(got.Payload), `{"id":1}`)
	}
	if !reflect.DeepEqual(got.Metadata, map[string]string{"k": "v"}) {
		t.Errorf("Metadata = %v, want %v", got.Metadata, map[string]string{"k": "v"})
	}
	if !got.CreatedAt.Equal(created) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, created)
	}
	if got.PublishedAt == nil || !got.PublishedAt.Equal(published) {
		t.Errorf("PublishedAt = %v, want %v", got.PublishedAt, published)
	}
	if got.Status != evtoutbox.StatusPublished {
		t.Errorf("Status = %q, want %q", got.Status, evtoutbox.StatusPublished)
	}
	if got.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want %d", got.RetryCount, 3)
	}
	if got.LastError != "boom" {
		t.Errorf("LastError = %q, want %q", got.LastError, "boom")
	}
	if got.Priority != 7 {
		t.Errorf("Priority = %d, want %d", got.Priority, 7)
	}
}

func TestMongoMessageToMessageNilPublishedAt(t *testing.T) {
	mm := &mongoMessage{
		ID:        bson.NewObjectID(),
		EventName: "x",
		Status:    evtoutbox.StatusPending,
	}
	got := mm.toMessage()
	if got.PublishedAt != nil {
		t.Errorf("PublishedAt = %v, want nil", got.PublishedAt)
	}
}

// keyStrings flattens a bson.D index key spec into "field:dir" tokens for comparison.
func keyStrings(d bson.D) []string {
	out := make([]string, 0, len(d))
	for _, e := range d {
		var dir int
		switch v := e.Value.(type) {
		case int:
			dir = v
		case int32:
			dir = int(v)
		case int64:
			dir = int(v)
		}
		out = append(out, e.Key+":"+strconv.Itoa(dir))
	}
	return out
}

func TestIndexes(t *testing.T) {
	store := &MongoStore{}
	indexes := store.Indexes()

	if len(indexes) != 4 {
		t.Fatalf("expected 4 indexes, got %d", len(indexes))
	}

	want := [][]string{
		{"status:1", "priority:-1", "created_at:1"},
		{"status:1", "claimed_at:1"},
		{"event_id:1"},
		{"published_at:1"},
	}

	for i, idx := range indexes {
		keys, ok := idx.Keys.(bson.D)
		if !ok {
			t.Fatalf("index %d Keys is not bson.D: %T", i, idx.Keys)
		}
		got := keyStrings(keys)
		if !reflect.DeepEqual(got, want[i]) {
			t.Errorf("index %d keys = %v, want %v", i, got, want[i])
		}
	}

	// The published_at index must be sparse. In driver v2 the Options field is a
	// builder; materialize it by applying its option funcs to an IndexOptions.
	sparseIdx := indexes[3]
	if sparseIdx.Options == nil {
		t.Fatal("published_at index has nil Options, expected sparse")
	}
	var built options.IndexOptions
	for _, fn := range sparseIdx.Options.List() {
		if err := fn(&built); err != nil {
			t.Fatalf("applying index option: %v", err)
		}
	}
	if built.Sparse == nil || !*built.Sparse {
		t.Errorf("published_at index Sparse = %v, want true", built.Sparse)
	}
}
