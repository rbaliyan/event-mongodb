package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	eventlib "github.com/rbaliyan/event/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// buFakeStream is a changeStream fake driven by a pre-built slice of
// changeStreamDoc values. It exercises the processing pipeline without a live
// MongoDB connection. Named with the "bu" prefix to avoid clashes with helpers
// in other test files.
type buFakeStream struct {
	docs   []bson.D // raw change docs, marshaled lazily on Decode
	idx    int      // index of the doc returned by the most recent Next/TryNext
	tokens []bson.Raw
	err    error
	decErr error // forced Decode error
	t      *testing.T
}

func (s *buFakeStream) Next(_ context.Context) bool {
	if s.idx+1 >= len(s.docs) {
		// Advance past the end so Decode after the final doc is well-defined.
		s.idx = len(s.docs)
		return false
	}
	s.idx++
	return true
}

func (s *buFakeStream) TryNext(ctx context.Context) bool { return s.Next(ctx) }

func (s *buFakeStream) Decode(val any) error {
	if s.decErr != nil {
		return s.decErr
	}
	s.t.Helper()
	if s.idx < 0 || s.idx >= len(s.docs) {
		return errors.New("buFakeStream: Decode called with no current document")
	}
	data, err := bson.Marshal(s.docs[s.idx])
	if err != nil {
		return err
	}
	return bson.Unmarshal(data, val)
}

func (s *buFakeStream) ResumeToken() bson.Raw {
	if s.idx >= 0 && s.idx < len(s.tokens) {
		return s.tokens[s.idx]
	}
	return nil
}

func (s *buFakeStream) Err() error                    { return s.err }
func (s *buFakeStream) Close(_ context.Context) error { return nil }

func newBuFakeStream(t *testing.T, docs ...bson.D) *buFakeStream {
	t.Helper()
	return &buFakeStream{docs: docs, idx: -1, t: t}
}

// buFakeAckStore is an in-memory AckStore stub recording Store/Ack calls.
type buFakeAckStore struct {
	mu       sync.Mutex
	stored   []string
	acked    []string
	storeErr error
	ackErr   error
}

func (s *buFakeAckStore) Store(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.storeErr != nil {
		return s.storeErr
	}
	s.stored = append(s.stored, id)
	return nil
}

func (s *buFakeAckStore) Ack(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ackErr != nil {
		return s.ackErr
	}
	s.acked = append(s.acked, id)
	return nil
}

func (s *buFakeAckStore) storedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.stored...)
}

func (s *buFakeAckStore) ackedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.acked...)
}

var _ AckStore = (*buFakeAckStore)(nil)

// buNewTransport builds a Transport directly from transportOptions for
// unit-testing the build* helpers without a live MongoDB connection.
func buNewTransport(opts transportOptions) *Transport {
	if opts.logger == nil {
		opts.logger = discardLogger()
	}
	return &Transport{transportOptions: opts, status: statusOpen}
}

// --- buildPayload / buildFullDocumentPayload tests ---

func TestBuildPayload(t *testing.T) {
	t.Parallel()

	oid := bson.NewObjectID()
	fullDocRaw := mustMarshalBSON(t, bson.D{{Key: "_id", Value: oid}, {Key: "name", Value: "alice"}})

	tests := []struct {
		name            string
		opts            transportOptions
		doc             changeStreamDoc
		event           ChangeEvent
		wantContentType string
		wantNil         bool
		check           func(t *testing.T, payload []byte)
	}{
		{
			name:            "default ChangeEvent JSON mode",
			opts:            transportOptions{},
			doc:             changeStreamDoc{},
			event:           ChangeEvent{ID: "e1", OperationType: OperationInsert, Database: "db", Collection: "c"},
			wantContentType: "application/json",
			check: func(t *testing.T, payload []byte) {
				var ce ChangeEvent
				if err := json.Unmarshal(payload, &ce); err != nil {
					t.Fatalf("payload is not valid ChangeEvent JSON: %v", err)
				}
				if ce.ID != "e1" || ce.OperationType != OperationInsert {
					t.Errorf("decoded ChangeEvent mismatch: %+v", ce)
				}
			},
		},
		{
			name:            "fullDocumentOnly with full document returns raw BSON",
			opts:            transportOptions{fullDocumentOnly: true},
			doc:             changeStreamDoc{FullDocument: fullDocRaw},
			event:           ChangeEvent{ID: "e2", OperationType: OperationInsert},
			wantContentType: "application/bson",
			check: func(t *testing.T, payload []byte) {
				if string(payload) != string(fullDocRaw) {
					t.Error("expected raw fullDocument bytes returned verbatim")
				}
			},
		},
		{
			name:            "fullDocumentOnly delete with document key returns text",
			opts:            transportOptions{fullDocumentOnly: true},
			doc:             changeStreamDoc{},
			event:           ChangeEvent{ID: "e3", OperationType: OperationDelete, DocumentKey: oid.Hex()},
			wantContentType: "text/plain",
			check: func(t *testing.T, payload []byte) {
				if string(payload) != oid.Hex() {
					t.Errorf("payload = %q, want %q", payload, oid.Hex())
				}
			},
		},
		{
			name:    "fullDocumentOnly without document or key is skipped",
			opts:    transportOptions{fullDocumentOnly: true},
			doc:     changeStreamDoc{},
			event:   ChangeEvent{ID: "e4", OperationType: OperationUpdate},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := buNewTransport(tt.opts)
			payload, ct, err := tr.buildPayload(tt.doc, tt.event)
			if err != nil {
				t.Fatalf("buildPayload error: %v", err)
			}
			if tt.wantNil {
				if payload != nil {
					t.Errorf("expected nil payload, got %q", payload)
				}
				return
			}
			if payload == nil {
				t.Fatal("expected non-nil payload")
			}
			if ct != tt.wantContentType {
				t.Errorf("contentType = %q, want %q", ct, tt.wantContentType)
			}
			if tt.check != nil {
				tt.check(t, payload)
			}
		})
	}
}

// --- buildMetadata tests ---

func TestBuildMetadata(t *testing.T) {
	t.Parallel()

	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	baseEvent := func() ChangeEvent {
		return ChangeEvent{
			OperationType: OperationUpdate,
			Database:      "mydb",
			Collection:    "orders",
			Namespace:     "mydb.orders",
			DocumentKey:   "doc123",
			Timestamp:     ts,
			UpdateDesc: &UpdateDescription{
				UpdatedFields: map[string]any{"status": "shipped"},
				RemovedFields: []string{"temp"},
			},
		}
	}

	t.Run("base fields always present", func(t *testing.T) {
		t.Parallel()
		tr := buNewTransport(transportOptions{})
		md := tr.buildMetadata(baseEvent(), "application/json")
		want := map[string]string{
			MetadataContentType: "application/json",
			MetadataOperation:   "update",
			MetadataDatabase:    "mydb",
			MetadataCollection:  "orders",
			MetadataNamespace:   "mydb.orders",
			MetadataDocumentKey: "doc123",
			MetadataClusterTime: ts.Format(time.RFC3339Nano),
		}
		for k, v := range want {
			if md[k] != v {
				t.Errorf("metadata[%q] = %q, want %q", k, md[k], v)
			}
		}
	})

	t.Run("includeUpdateDescription off omits update fields", func(t *testing.T) {
		t.Parallel()
		tr := buNewTransport(transportOptions{includeUpdateDescription: false})
		md := tr.buildMetadata(baseEvent(), "application/json")
		if _, ok := md[MetadataUpdatedFields]; ok {
			t.Error("updated_fields should be omitted when includeUpdateDescription is off")
		}
		if _, ok := md[MetadataRemovedFields]; ok {
			t.Error("removed_fields should be omitted when includeUpdateDescription is off")
		}
	})

	t.Run("includeUpdateDescription on adds update fields", func(t *testing.T) {
		t.Parallel()
		tr := buNewTransport(transportOptions{includeUpdateDescription: true})
		md := tr.buildMetadata(baseEvent(), "application/json")
		if md[MetadataUpdatedFields] == "" {
			t.Error("expected updated_fields metadata")
		}
		if !strings.Contains(md[MetadataUpdatedFields], "shipped") {
			t.Errorf("updated_fields = %q, want it to contain shipped", md[MetadataUpdatedFields])
		}
		if md[MetadataRemovedFields] == "" {
			t.Error("expected removed_fields metadata")
		}
	})

	t.Run("maxUpdatedFieldsSize exceeded omits updated_fields but keeps removed_fields", func(t *testing.T) {
		t.Parallel()
		// 1 byte limit forces the JSON of updated_fields to exceed the cap.
		tr := buNewTransport(transportOptions{includeUpdateDescription: true, maxUpdatedFieldsSize: 1})
		md := tr.buildMetadata(baseEvent(), "application/json")
		if _, ok := md[MetadataUpdatedFields]; ok {
			t.Error("updated_fields should be omitted when it exceeds maxUpdatedFieldsSize")
		}
		if md[MetadataRemovedFields] == "" {
			t.Error("removed_fields should still be present regardless of size limit")
		}
	})

	t.Run("maxUpdatedFieldsSize generous keeps updated_fields", func(t *testing.T) {
		t.Parallel()
		tr := buNewTransport(transportOptions{includeUpdateDescription: true, maxUpdatedFieldsSize: 4096})
		md := tr.buildMetadata(baseEvent(), "application/json")
		if md[MetadataUpdatedFields] == "" {
			t.Error("updated_fields should be present when under the size limit")
		}
	})

	t.Run("nil UpdateDesc with includeUpdateDescription on", func(t *testing.T) {
		t.Parallel()
		ev := baseEvent()
		ev.UpdateDesc = nil
		tr := buNewTransport(transportOptions{includeUpdateDescription: true})
		md := tr.buildMetadata(ev, "application/json")
		if _, ok := md[MetadataUpdatedFields]; ok {
			t.Error("updated_fields should be omitted when UpdateDesc is nil")
		}
	})
}

// --- buildMessage tests ---

func TestBuildMessage(t *testing.T) {
	t.Parallel()

	t.Run("message carries id source payload metadata", func(t *testing.T) {
		t.Parallel()
		tr := buNewTransport(transportOptions{})
		ev := ChangeEvent{ID: "msg-1", Database: "db", Collection: "coll"}
		payload := []byte(`{"k":"v"}`)
		md := map[string]string{"a": "b"}

		msg := tr.buildMessage(ev, payload, md)
		if msg.ID() != "msg-1" {
			t.Errorf("ID = %q, want msg-1", msg.ID())
		}
		if msg.Source() != "mongodb://db/coll" {
			t.Errorf("Source = %q, want mongodb://db/coll", msg.Source())
		}
		if string(msg.Payload()) != string(payload) {
			t.Errorf("Payload = %q, want %q", msg.Payload(), payload)
		}
		if msg.Metadata()["a"] != "b" {
			t.Errorf("Metadata = %v", msg.Metadata())
		}
	})

	t.Run("ack callback calls ack store on success", func(t *testing.T) {
		t.Parallel()
		ackStore := &buFakeAckStore{}
		tr := buNewTransport(transportOptions{ackStore: ackStore})
		ev := ChangeEvent{ID: "ack-me"}
		msg := tr.buildMessage(ev, []byte("p"), map[string]string{})
		if err := msg.Ack(nil); err != nil {
			t.Fatalf("Ack(nil) error: %v", err)
		}
		if got := ackStore.ackedIDs(); len(got) != 1 || got[0] != "ack-me" {
			t.Errorf("acked = %v, want [ack-me]", got)
		}
	})

	t.Run("ack callback acks on ErrAck", func(t *testing.T) {
		t.Parallel()
		ackStore := &buFakeAckStore{}
		tr := buNewTransport(transportOptions{ackStore: ackStore})
		ev := ChangeEvent{ID: "ack-err"}
		msg := tr.buildMessage(ev, []byte("p"), map[string]string{})
		if err := msg.Ack(eventlib.ErrAck); err != nil {
			t.Fatalf("Ack(ErrAck) error: %v", err)
		}
		if got := ackStore.ackedIDs(); len(got) != 1 {
			t.Errorf("acked = %v, want one entry", got)
		}
	})

	t.Run("ack callback does NOT ack on non-ack error", func(t *testing.T) {
		t.Parallel()
		ackStore := &buFakeAckStore{}
		tr := buNewTransport(transportOptions{ackStore: ackStore})
		ev := ChangeEvent{ID: "nack"}
		msg := tr.buildMessage(ev, []byte("p"), map[string]string{})
		if err := msg.Ack(errors.New("handler failed")); err != nil {
			t.Fatalf("Ack(err) returned: %v", err)
		}
		if got := ackStore.ackedIDs(); len(got) != 0 {
			t.Errorf("acked = %v, want none (non-ack error)", got)
		}
	})

	t.Run("ack callback no-op without ack store", func(t *testing.T) {
		t.Parallel()
		tr := buNewTransport(transportOptions{})
		msg := tr.buildMessage(ChangeEvent{ID: "x"}, []byte("p"), map[string]string{})
		if err := msg.Ack(nil); err != nil {
			t.Errorf("Ack(nil) without store = %v, want nil", err)
		}
	})
}

// --- processChange (via fake stream) tests ---

func TestProcessChange(t *testing.T) {
	t.Parallel()

	insertDoc := func(id string) bson.D {
		return bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: id}}},
			{Key: "operationType", Value: "insert"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "mydb"}, {Key: "coll", Value: "orders"}}},
			{Key: "documentKey", Value: bson.D{{Key: "_id", Value: "doc-" + id}}},
			{Key: "fullDocument", Value: bson.D{{Key: "name", Value: "v"}}},
			{Key: "clusterTime", Value: bson.Timestamp{T: 1705300200, I: 1}},
		}
	}

	t.Run("publishes JSON payload to registered event", func(t *testing.T) {
		t.Parallel()
		tr := buMustNewChannelTransport(t, transportOptions{})
		captured := buRegisterCapture(t, tr, "orders.changes")

		stream := newBuFakeStream(t, insertDoc("tok1"))
		stream.Next(context.Background())
		if err := tr.processChange(context.Background(), stream); err != nil {
			t.Fatalf("processChange error: %v", err)
		}

		msg := captured.wait(t)
		if msg.Metadata()[MetadataOperation] != "insert" {
			t.Errorf("operation metadata = %q, want insert", msg.Metadata()[MetadataOperation])
		}
		if msg.Metadata()[MetadataContentType] != "application/json" {
			t.Errorf("content type = %q, want application/json", msg.Metadata()[MetadataContentType])
		}
		var ce ChangeEvent
		if err := json.Unmarshal(msg.Payload(), &ce); err != nil {
			t.Fatalf("payload not ChangeEvent JSON: %v", err)
		}
		if ce.OperationType != OperationInsert {
			t.Errorf("operation = %q, want insert", ce.OperationType)
		}
	})

	t.Run("stores pending in ack store before publish", func(t *testing.T) {
		t.Parallel()
		ackStore := &buFakeAckStore{}
		tr := buMustNewChannelTransport(t, transportOptions{ackStore: ackStore})
		buRegisterCapture(t, tr, "orders.changes")

		stream := newBuFakeStream(t, insertDoc("tok2"))
		stream.Next(context.Background())
		if err := tr.processChange(context.Background(), stream); err != nil {
			t.Fatalf("processChange error: %v", err)
		}
		if got := ackStore.storedIDs(); len(got) != 1 || got[0] != "tok2" {
			t.Errorf("stored = %v, want [tok2]", got)
		}
	})

	t.Run("ack store failure prevents publish and returns error", func(t *testing.T) {
		t.Parallel()
		ackStore := &buFakeAckStore{storeErr: errors.New("write failed")}
		tr := buMustNewChannelTransport(t, transportOptions{ackStore: ackStore})
		captured := buRegisterCapture(t, tr, "orders.changes")

		stream := newBuFakeStream(t, insertDoc("tok3"))
		stream.Next(context.Background())
		err := tr.processChange(context.Background(), stream)
		if err == nil {
			t.Fatal("expected error when ack store Store fails")
		}
		if captured.count() != 0 {
			t.Error("no message should be published when store fails")
		}
	})

	t.Run("empty update is discarded by default", func(t *testing.T) {
		t.Parallel()
		tr := buMustNewChannelTransport(t, transportOptions{})
		captured := buRegisterCapture(t, tr, "orders.changes")

		emptyUpdate := bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: "tok4"}}},
			{Key: "operationType", Value: "update"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "mydb"}, {Key: "coll", Value: "orders"}}},
			{Key: "updateDescription", Value: bson.D{
				{Key: "updatedFields", Value: bson.D{}},
				{Key: "removedFields", Value: bson.A{}},
			}},
		}
		stream := newBuFakeStream(t, emptyUpdate)
		stream.Next(context.Background())
		if err := tr.processChange(context.Background(), stream); err != nil {
			t.Fatalf("processChange error: %v", err)
		}
		if captured.count() != 0 {
			t.Error("empty update should be discarded (no publish)")
		}
	})

	t.Run("empty update delivered with WithEmptyUpdates", func(t *testing.T) {
		t.Parallel()
		tr := buMustNewChannelTransport(t, transportOptions{emptyUpdates: true})
		captured := buRegisterCapture(t, tr, "orders.changes")

		emptyUpdate := bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: "tok5"}}},
			{Key: "operationType", Value: "update"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "mydb"}, {Key: "coll", Value: "orders"}}},
			{Key: "updateDescription", Value: bson.D{
				{Key: "updatedFields", Value: bson.D{}},
				{Key: "removedFields", Value: bson.A{}},
			}},
		}
		stream := newBuFakeStream(t, emptyUpdate)
		stream.Next(context.Background())
		if err := tr.processChange(context.Background(), stream); err != nil {
			t.Fatalf("processChange error: %v", err)
		}
		_ = captured.wait(t) // should publish
	})

	t.Run("decode error is returned", func(t *testing.T) {
		t.Parallel()
		tr := buMustNewChannelTransport(t, transportOptions{})
		stream := &buFakeStream{decErr: errors.New("decode boom"), idx: -1, t: t}
		if err := tr.processChange(context.Background(), stream); err == nil {
			t.Fatal("expected decode error")
		}
	})

	t.Run("fullDocumentOnly skip (nil payload) does not publish", func(t *testing.T) {
		t.Parallel()
		tr := buMustNewChannelTransport(t, transportOptions{
			fullDocumentOnly: true,
			fullDocument:     FullDocumentUpdateLookup,
		})
		captured := buRegisterCapture(t, tr, "orders.changes")

		// delete with no full document and no document key -> skipped
		del := bson.D{
			{Key: "_id", Value: bson.D{{Key: "_data", Value: "tok6"}}},
			{Key: "operationType", Value: "delete"},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "mydb"}, {Key: "coll", Value: "orders"}}},
		}
		stream := newBuFakeStream(t, del)
		stream.Next(context.Background())
		if err := tr.processChange(context.Background(), stream); err != nil {
			t.Fatalf("processChange error: %v", err)
		}
		if captured.count() != 0 {
			t.Error("fullDocumentOnly skip should not publish")
		}
	})
}

// --- isChangeStreamHistoryLost classifier extra cases ---

func TestIsChangeStreamHistoryLost_TypedAndEOF(t *testing.T) {
	t.Parallel()
	if isChangeStreamHistoryLost(nil) {
		t.Error("nil should be false")
	}
	if got := isChangeStreamHistoryLost(mongo.CommandError{Code: 286, Message: "ChangeStreamHistoryLost"}); !got {
		t.Error("code 286 ServerError should be true")
	}
	if got := isChangeStreamHistoryLost(errors.New("EOF")); got {
		t.Error("io.EOF-like error should be false")
	}
}

func mustMarshalBSON(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	data, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("marshal bson: %v", err)
	}
	return data
}
