package outbox

import (
	"context"
	"testing"

	evtoutbox "github.com/rbaliyan/event/v3/outbox"
)

// TestMongoStoreConformance runs the backend-neutral Store conformance suite
// (claim/ack/fail/close contract) against a live MongoStore. It reuses
// setupOutboxStore (mongodb_integration_test.go), which skips via
// mongotest.Connect when no MongoDB is reachable, so this test is a no-op
// without a live deployment rather than a failure.
func TestMongoStoreConformance(t *testing.T) {
	store, _ := setupOutboxStore(t)
	ctx := context.Background()

	seed := func(ctx context.Context, eventID string) error {
		return store.Store(ctx, "conf.event", eventID, []byte(`{}`), nil)
	}
	evtoutbox.RunStoreConformance(t, ctx, store, seed)
}
