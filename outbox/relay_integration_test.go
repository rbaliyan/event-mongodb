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

// setupOutboxPublisher returns a generic evtoutbox.Publisher backed by a
// MongoStore in a unique per-test database. It reuses mongotest.Connect
// (skips without Mongo) and drops the DB via t.Cleanup. Returns the publisher,
// the underlying MongoStore (for assertions), and the client (for
// Transaction/TransactionWithOptions).
func setupOutboxPublisher(t *testing.T) (*evtoutbox.Publisher, *MongoStore, *mongo.Client) {
	t.Helper()

	client := mongotest.Connect(t)
	db := client.Database(mongotest.UniqueDBName("test_outbox_pub"))

	store, err := NewMongoStore(db)
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}

	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = db.Drop(dropCtx)
	})

	return evtoutbox.NewPublisher(store), store, client
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

// --- Generic Publisher tests ---

func TestIntegrationGenericPublisherPublish(t *testing.T) {
	pub, store, _ := setupOutboxPublisher(t)
	ctx := context.Background()

	if err := pub.Publish(ctx, "order.created", map[string]any{"id": 1}, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The stored message should be claimable as pending.
	batch, err := store.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	defer func() { _ = batch.Close(ctx) }()
	msgs := batch.Messages()
	if len(msgs) != 1 {
		t.Fatalf("ClaimPending returned %d, want 1", len(msgs))
	}
	if msgs[0].EventName != "order.created" {
		t.Errorf("EventName = %q, want order.created", msgs[0].EventName)
	}
	if msgs[0].Metadata["k"] != "v" {
		t.Errorf("Metadata[k] = %q, want v", msgs[0].Metadata["k"])
	}
}

func TestIntegrationGenericPublisherInTransactionCommit(t *testing.T) {
	pub, store, client := setupOutboxPublisher(t)
	ctx := context.Background()

	err := Transaction(ctx, client, func(txCtx context.Context) error {
		return pub.Publish(txCtx, "order.created", map[string]any{"id": 2}, nil)
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

func TestIntegrationGenericPublisherInTransactionRollback(t *testing.T) {
	pub, store, client := setupOutboxPublisher(t)
	ctx := context.Background()

	boom := errors.New("business logic boom")
	err := Transaction(ctx, client, func(txCtx context.Context) error {
		if pubErr := pub.Publish(txCtx, "order.created", map[string]any{"id": 3}, nil); pubErr != nil {
			return pubErr
		}
		return boom
	})
	if isUnsupportedRelayTxError(err) {
		t.Skipf("transactions unsupported by deployment: %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Transaction error = %v, want %v", err, boom)
	}

	// The fn error must roll the transaction back: nothing stored.
	count, err := store.Count(ctx, evtoutbox.StatusPending)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Fatalf("pending count = %d, want 0 (rolled back)", count)
	}
}

func TestIntegrationTransactionCommit(t *testing.T) {
	_, store, client := setupOutboxPublisher(t)
	ctx := context.Background()

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
	_, store, client := setupOutboxPublisher(t)
	ctx := context.Background()

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

// --- Generic Relay (evtoutbox.NewRelay) tests ---

func TestIntegrationRelayPollPublishesPending(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{}
	relay := evtoutbox.NewRelay(store, rt,
		evtoutbox.WithPollDelay(10*time.Millisecond),
		evtoutbox.WithBatchSize(50),
	)

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
	relay := evtoutbox.NewRelay(store, rt, evtoutbox.WithPollDelay(10*time.Millisecond))

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

// TestIntegrationRelayRecoverStuckViaStart exercises stuck-message recovery
// through the generic relay's Start loop (rather than calling an unexported
// relay method, which is not reachable from this package now that Relay lives
// in github.com/rbaliyan/event/v3/outbox). A short WithStuckInterval makes the
// sweep run promptly; success is observed end-to-end via the store, since a
// recovered ("pending") message is then claimed and published by the same
// running relay.
func TestIntegrationRelayRecoverStuckViaStart(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

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

	rt := &recordingTransport{}
	relay := evtoutbox.NewRelay(store, rt,
		evtoutbox.WithPollDelay(20*time.Millisecond),
		evtoutbox.WithStuckInterval(20*time.Millisecond, 5*time.Minute),
	)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Start(runCtx) }()
	t.Cleanup(cancel)

	if !waitFor(t, 5*time.Second, func() bool {
		n, err := store.Count(ctx, evtoutbox.StatusPublished)
		return err == nil && n >= 1
	}) {
		t.Fatal("stuck message was not recovered and republished via relay Start")
	}
	if rt.count() < 1 {
		t.Fatalf("transport published %d messages, want >= 1", rt.count())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay Start did not return after cancel")
	}
}

func TestIntegrationRelayPublishOnce(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	rt := &recordingTransport{}
	relay := evtoutbox.NewRelay(store, rt)

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
