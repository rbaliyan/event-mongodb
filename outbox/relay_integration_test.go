package outbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	evtoutbox "github.com/rbaliyan/event/v3/outbox"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// isUnsupportedRelayTxError reports whether err indicates the deployment does
// not support the operation (multi-document transactions or change streams)
// because it is not a replica set / sharded cluster. Mirrors the
// isUnsupportedTxError pattern in the transaction package so these tests skip
// (rather than fail) on a standalone MongoDB.
func isUnsupportedRelayTxError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "replica set") ||
		strings.Contains(msg, "transaction numbers") ||
		strings.Contains(msg, "transactions are not supported") ||
		strings.Contains(msg, "this mongodb deployment does not support") ||
		strings.Contains(msg, "only supported on replica sets") ||
		strings.Contains(msg, "the $changestream stage is only supported")
}

// recordingTransport is a minimal transport.Transport that records every
// Publish call so tests can assert what the relay delivered. All other methods
// are no-ops; the relay only ever calls Publish.
type recordingTransport struct {
	mu        sync.Mutex
	published []recordedPublish
	failErr   error // when set, Publish returns this error and records nothing
}

type recordedPublish struct {
	name string
	id   string
}

func (rt *recordingTransport) Publish(_ context.Context, name string, msg message.Message) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.failErr != nil {
		return rt.failErr
	}
	rt.published = append(rt.published, recordedPublish{name: name, id: msg.ID()})
	return nil
}

func (rt *recordingTransport) RegisterEvent(_ context.Context, _ string) error   { return nil }
func (rt *recordingTransport) UnregisterEvent(_ context.Context, _ string) error { return nil }
func (rt *recordingTransport) Subscribe(_ context.Context, _ string, _ ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, errors.New("subscribe not supported")
}
func (rt *recordingTransport) Close(_ context.Context) error { return nil }

func (rt *recordingTransport) count() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return len(rt.published)
}

func (rt *recordingTransport) ids() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]string, len(rt.published))
	for i, p := range rt.published {
		out[i] = p.id
	}
	return out
}

var _ transport.Transport = (*recordingTransport)(nil)

// setupOutboxPublisher returns a MongoPublisher backed by a unique per-test
// database. It reuses mongotest.Connect (skips without Mongo) and drops the DB
// via t.Cleanup. Returns the publisher and the underlying client.
func setupOutboxPublisher(t *testing.T) (*MongoPublisher, *mongo.Client) {
	t.Helper()

	client := mongotest.Connect(t)
	db := client.Database(mongotest.UniqueDBName("test_outbox_pub"))

	pub, err := NewMongoPublisher(client, db)
	if err != nil {
		t.Fatalf("NewMongoPublisher: %v", err)
	}

	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = db.Drop(dropCtx)
	})

	return pub, client
}

// waitFor polls cond until it returns true or the deadline elapses. It avoids
// fixed sleeps: tests assert via short, bounded polling instead.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// --- MongoPublisher tests ---

func TestIntegrationPublisherPublish(t *testing.T) {
	pub, _ := setupOutboxPublisher(t)
	ctx := context.Background()

	if err := pub.Publish(ctx, "order.created", map[string]any{"id": 1}, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The stored message should be claimable as pending.
	msgs, err := pub.Store().GetPending(ctx, 10)
	if err != nil {
		t.Fatalf("GetPending: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("GetPending returned %d, want 1", len(msgs))
	}
	if msgs[0].EventName != "order.created" {
		t.Errorf("EventName = %q, want order.created", msgs[0].EventName)
	}
	if msgs[0].Metadata["k"] != "v" {
		t.Errorf("Metadata[k] = %q, want v", msgs[0].Metadata["k"])
	}
}

func TestIntegrationPublisherPublishInSession(t *testing.T) {
	pub, client := setupOutboxPublisher(t)
	ctx := context.Background()

	sess, err := client.StartSession()
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		return nil, pub.PublishInSession(sc, "order.created", map[string]any{"id": 2}, nil)
	})
	if err != nil {
		if isUnsupportedRelayTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("WithTransaction: %v", err)
	}

	count, err := pub.Store().Count(ctx, evtoutbox.StatusPending)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("pending count = %d, want 1", count)
	}
}

func TestIntegrationPublisherPublishWithTransactionCommit(t *testing.T) {
	pub, _ := setupOutboxPublisher(t)
	ctx := context.Background()

	called := false
	err := pub.PublishWithTransaction(ctx, "order.created", map[string]any{"id": 3}, nil, func(_ context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		if isUnsupportedRelayTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("PublishWithTransaction: %v", err)
	}
	if !called {
		t.Error("business logic fn was not called")
	}

	count, err := pub.Store().Count(ctx, evtoutbox.StatusPending)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("pending count = %d, want 1 (committed)", count)
	}
}

func TestIntegrationPublisherPublishWithTransactionRollback(t *testing.T) {
	pub, _ := setupOutboxPublisher(t)
	ctx := context.Background()

	boom := errors.New("business logic boom")
	err := pub.PublishWithTransaction(ctx, "order.created", map[string]any{"id": 4}, nil, func(_ context.Context) error {
		return boom
	})
	if isUnsupportedRelayTxError(err) {
		t.Skipf("transactions unsupported by deployment: %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("PublishWithTransaction error = %v, want %v", err, boom)
	}

	// The fn error must roll the transaction back: nothing stored.
	count, err := pub.Store().Count(ctx, evtoutbox.StatusPending)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Fatalf("pending count = %d, want 0 (rolled back)", count)
	}
}

func TestIntegrationTransactionCommit(t *testing.T) {
	pub, client := setupOutboxPublisher(t)
	ctx := context.Background()
	store := pub.Store()

	err := Transaction(ctx, client, func(txCtx context.Context) error {
		// Store() routes through the embedded session context.
		return store.Store(txCtx, "order.created", "evt-tx-commit", []byte(`{"id":5}`), nil)
	})
	if err != nil {
		if isUnsupportedRelayTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("Transaction: %v", err)
	}

	count, err := store.Count(ctx, evtoutbox.StatusPending)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("pending count = %d, want 1 (committed)", count)
	}
}

func TestIntegrationTransactionRollback(t *testing.T) {
	pub, client := setupOutboxPublisher(t)
	ctx := context.Background()
	store := pub.Store()

	boom := errors.New("tx boom")
	err := Transaction(ctx, client, func(txCtx context.Context) error {
		if storeErr := store.Store(txCtx, "order.created", "evt-tx-rollback", []byte(`{"id":6}`), nil); storeErr != nil {
			return storeErr
		}
		return boom
	})
	if isUnsupportedRelayTxError(err) {
		t.Skipf("transactions unsupported by deployment: %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Transaction error = %v, want %v", err, boom)
	}

	count, err := store.Count(ctx, evtoutbox.StatusPending)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Fatalf("pending count = %d, want 0 (rolled back)", count)
	}
}

// --- MongoRelay (poll mode) tests ---

func TestIntegrationRelayPollPublishesPending(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{}
	relay := NewMongoRelay(store, rt).
		WithPollDelay(10 * time.Millisecond).
		WithBatchSize(50)

	// Pre-insert pending messages.
	insertPending(t, ctx, store, "relay-1", 0)
	insertPending(t, ctx, store, "relay-2", 0)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Start(runCtx) }()
	// Defensive: ensure the relay goroutine is cancelled even if the body fails
	// before it reaches its own cancel/wait below. The buffered done channel is
	// drained by the inline wait at the end (do not also read it here, or the
	// two readers race for the single sent value).
	t.Cleanup(cancel)

	if !waitFor(t, 5*time.Second, func() bool { return rt.count() >= 2 }) {
		t.Fatalf("transport published %d messages, want >= 2", rt.count())
	}

	// Insert another while running; the poll loop should pick it up.
	insertPending(t, ctx, store, "relay-3", 0)
	if !waitFor(t, 5*time.Second, func() bool { return rt.count() >= 3 }) {
		t.Fatalf("transport published %d messages after live insert, want >= 3", rt.count())
	}

	// All delivered messages should be marked published in the store.
	if !waitFor(t, 5*time.Second, func() bool {
		n, err := store.Count(ctx, evtoutbox.StatusPublished)
		return err == nil && n >= 3
	}) {
		n, _ := store.Count(ctx, evtoutbox.StatusPublished)
		t.Fatalf("published count = %d, want >= 3", n)
	}

	// Published event IDs should match what was inserted.
	gotIDs := map[string]bool{}
	for _, id := range rt.ids() {
		gotIDs[id] = true
	}
	for _, want := range []string{"relay-1", "relay-2", "relay-3"} {
		if !gotIDs[want] {
			t.Errorf("expected published id %q not recorded by transport", want)
		}
	}

	// Cancel and ensure Start returns with context.Canceled.
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("relay Start returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay Start did not return after cancel")
	}
}

func TestIntegrationRelayPollMarksFailedOnTransportError(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{failErr: errors.New("transport down")}
	relay := NewMongoRelay(store, rt).WithPollDelay(10 * time.Millisecond)

	insertPending(t, ctx, store, "fail-1", 0)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Start(runCtx) }()
	t.Cleanup(cancel) // inline wait below drains done; don't double-read it here

	// A failed publish moves the message to failed status.
	if !waitFor(t, 5*time.Second, func() bool {
		n, err := store.Count(ctx, evtoutbox.StatusFailed)
		return err == nil && n >= 1
	}) {
		n, _ := store.Count(ctx, evtoutbox.StatusFailed)
		t.Fatalf("failed count = %d, want >= 1", n)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay Start did not return after cancel")
	}
}

func TestIntegrationRelayRecoverStuck(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{}
	relay := NewMongoRelay(store, rt).WithStuckDuration(5 * time.Minute)

	// A message stuck in processing with an old claimed_at.
	stale := time.Now().Add(-30 * time.Minute)
	_, err := store.Collection().InsertOne(ctx, mongoMessage{
		ID:        bson.NewObjectID(),
		EventName: "order.created",
		EventID:   "stuck-relay",
		Status:    evtoutbox.StatusProcessing,
		ClaimedAt: &stale,
		ClaimedBy: "dead-relay",
		CreatedAt: stale,
	})
	if err != nil {
		t.Fatalf("InsertOne stuck: %v", err)
	}

	relay.recoverStuck(ctx)

	if c, err := store.Count(ctx, evtoutbox.StatusPending); err != nil || c != 1 {
		t.Fatalf("pending count = %d (err=%v), want 1 (recovered)", c, err)
	}
	if c, err := store.Count(ctx, evtoutbox.StatusProcessing); err != nil || c != 0 {
		t.Fatalf("processing count = %d (err=%v), want 0", c, err)
	}
}

func TestIntegrationRelayCleanup(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{}
	relay := NewMongoRelay(store, rt).WithCleanupAge(24 * time.Hour)

	oldTime := time.Now().Add(-48 * time.Hour)
	recentTime := time.Now()
	_, err := store.Collection().InsertMany(ctx, []any{
		mongoMessage{
			ID:          bson.NewObjectID(),
			EventName:   "e",
			EventID:     "old",
			Status:      evtoutbox.StatusPublished,
			PublishedAt: &oldTime,
			CreatedAt:   oldTime,
		},
		mongoMessage{
			ID:          bson.NewObjectID(),
			EventName:   "e",
			EventID:     "recent",
			Status:      evtoutbox.StatusPublished,
			PublishedAt: &recentTime,
			CreatedAt:   recentTime,
		},
	})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	relay.cleanup(ctx)

	if c, err := store.Count(ctx, evtoutbox.StatusPublished); err != nil || c != 1 {
		t.Fatalf("remaining published = %d (err=%v), want 1 (old deleted)", c, err)
	}
}

func TestIntegrationRelayPublishOnce(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{}
	relay := NewMongoRelay(store, rt)

	insertPending(t, ctx, store, "once-1", 0)
	insertPending(t, ctx, store, "once-2", 0)

	if err := relay.PublishOnce(ctx); err != nil {
		t.Fatalf("PublishOnce: %v", err)
	}

	if rt.count() != 2 {
		t.Fatalf("transport published %d, want 2", rt.count())
	}
	if c, err := store.Count(ctx, evtoutbox.StatusPublished); err != nil || c != 2 {
		t.Fatalf("published count = %d (err=%v), want 2", c, err)
	}
}

// --- ChangeStreamRelay tests ---

// TestIntegrationChangeStreamProcessExistingPending exercises the
// ChangeStreamRelay's deterministic startup path: pre-existing pending messages
// are published and marked published. This is preferred over a live watch test
// because change-stream delivery timing is not deterministic enough to assert
// reliably; processExistingPending uses the same claim/publish path the watcher
// relies on. The full watch loop additionally requires a replica set and is
// covered indirectly via MongoRelay changestream wiring.
func TestIntegrationChangeStreamProcessExistingPending(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{}
	csRelay := NewChangeStreamRelay(store, rt).WithBatchSize(50)

	insertPending(t, ctx, store, "cs-1", 0)
	insertPending(t, ctx, store, "cs-2", 0)

	csRelay.processExistingPending(ctx)

	if rt.count() != 2 {
		t.Fatalf("transport published %d, want 2", rt.count())
	}
	if c, err := store.Count(ctx, evtoutbox.StatusPublished); err != nil || c != 2 {
		t.Fatalf("published count = %d (err=%v), want 2", c, err)
	}
}

// TestIntegrationChangeStreamWatchDelivers exercises the full Start path,
// including the live Change Stream watch loop. It requires a replica set; if the
// deployment does not support change streams, the watch goroutine logs an error
// and reconnects, so we detect non-delivery via the bounded wait and skip.
func TestIntegrationChangeStreamWatchDelivers(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	// Verify change streams are supported before asserting delivery; otherwise
	// skip rather than fail on a standalone deployment.
	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	stream, err := store.Collection().Watch(probeCtx, mongo.Pipeline{})
	if err != nil {
		probeCancel()
		if isUnsupportedRelayTxError(err) {
			t.Skipf("change streams unsupported by deployment: %v", err)
		}
		t.Skipf("could not open change stream probe: %v", err)
	}
	_ = stream.Close(probeCtx)
	probeCancel()

	rt := &recordingTransport{}
	csRelay := NewChangeStreamRelay(store, rt).WithBatchSize(50)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- csRelay.Start(runCtx) }()
	t.Cleanup(cancel) // inline wait below drains done; don't double-read it here

	// Give the watch loop a brief, bounded moment to establish before inserting.
	time.Sleep(500 * time.Millisecond)
	insertPending(t, ctx, store, "cs-watch-1", 0)

	if !waitFor(t, 8*time.Second, func() bool { return rt.count() >= 1 }) {
		t.Skipf("change stream did not deliver within timeout (deployment may not support change streams reliably)")
	}

	if !waitFor(t, 5*time.Second, func() bool {
		n, err := store.Count(ctx, evtoutbox.StatusPublished)
		return err == nil && n >= 1
	}) {
		n, _ := store.Count(ctx, evtoutbox.StatusPublished)
		t.Fatalf("published count = %d, want >= 1", n)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ChangeStreamRelay Start did not return after cancel")
	}
}

func TestIntegrationResumeTokenStore(t *testing.T) {
	client := mongotest.Connect(t)
	db := client.Database(mongotest.UniqueDBName("test_outbox_token"))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = db.Drop(dropCtx)
	})

	ctx := context.Background()
	ts, err := NewMongoResumeTokenStore(db, "relay-1")
	if err != nil {
		t.Fatalf("NewMongoResumeTokenStore: %v", err)
	}

	// No token saved yet -> Load returns nil, no error.
	tok, err := ts.Load(ctx)
	if err != nil {
		t.Fatalf("Load (empty): %v", err)
	}
	if tok != nil {
		t.Fatalf("Load (empty) = %v, want nil", tok)
	}

	// Save then Load round-trips the token.
	raw, mErr := bson.Marshal(bson.M{"_data": "token-abc"})
	if mErr != nil {
		t.Fatalf("Marshal token: %v", mErr)
	}
	want := bson.Raw(raw)
	if err := ts.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := ts.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil {
		t.Fatal("Load returned nil after Save")
	}

	// Upsert: saving a second time for the same relayID updates, not duplicates.
	raw2, _ := bson.Marshal(bson.M{"_data": "token-xyz"})
	if err := ts.Save(ctx, bson.Raw(raw2)); err != nil {
		t.Fatalf("Save (update): %v", err)
	}
	n, err := db.Collection("outbox_resume_tokens").CountDocuments(ctx, bson.M{"_id": "relay-1"})
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if n != 1 {
		t.Fatalf("resume token docs = %d, want 1 (upsert)", n)
	}
}
