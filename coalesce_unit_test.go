package mongodb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"go.opentelemetry.io/otel/trace"
)

// coDoc is a tiny payload type for coalesce tests.
type coDoc struct {
	Value int `json:"value"`
}

// TestCoalesceByDocumentKey_Applies verifies that the SubscribeOption returned
// by CoalesceByDocumentKey actually coalesces messages sharing the same
// document_key metadata. It uses a channel transport (no MongoDB required) and
// relies on event.ContextCoalescedCount as the oracle: when the handler is busy,
// later messages for the same key supersede earlier ones.
func TestCoalesceByDocumentKey_Applies(t *testing.T) {
	ctx := context.Background()

	ch := channel.New(channel.WithBufferSize(64))
	t.Cleanup(func() { _ = ch.Close(ctx) })

	busName := "coalesce-test-bus"
	bus, err := event.NewBus(busName, event.WithTransport(ch))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close(ctx) })

	ev := event.New[coDoc]("doc.changes")
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var (
		mu             sync.Mutex
		delivered      int
		coalescedTotal int
		releaseFirst   = make(chan struct{})
		firstSeen      atomic.Bool
	)

	err = ev.Subscribe(ctx, func(hctx context.Context, _ event.Event[coDoc], _ coDoc) error {
		if firstSeen.CompareAndSwap(false, true) {
			// Block the first delivery so subsequent same-key messages queue
			// and coalesce behind it.
			<-releaseFirst
		}
		mu.Lock()
		delivered++
		coalescedTotal += event.ContextCoalescedCount(hctx)
		mu.Unlock()
		return nil
	}, CoalesceByDocumentKey[coDoc]())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish several messages with the SAME document_key directly via the
	// channel transport so we control metadata precisely. The transport
	// registers events under the bare event name (the bus strips its prefix).
	const n = 5
	for i := 0; i < n; i++ {
		msg := transport.NewMessage(
			"id-"+string(rune('a'+i)),
			"test",
			[]byte(`{"value":1}`),
			map[string]string{
				MetadataDocumentKey: "same-doc",
				MetadataContentType: "application/json",
			},
			trace.SpanContext{},
		)
		if err := ch.Publish(ctx, "doc.changes", msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Give the first message time to be picked up and block in the handler.
	time.Sleep(200 * time.Millisecond)
	close(releaseFirst)

	// Wait for the queue to drain.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		d, c := delivered, coalescedTotal
		mu.Unlock()
		if d+c >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	d, c := delivered, coalescedTotal
	mu.Unlock()

	if d >= n {
		t.Errorf("delivered = %d with no coalescing; expected fewer than %d deliveries", d, n)
	}
	if c == 0 {
		t.Errorf("coalesced count = 0; expected superseded messages with same document_key")
	}
	if d+c < 1 {
		t.Errorf("delivered+coalesced = %d, want >= 1", d+c)
	}
}

// TestCoalesceByDocumentKey_NonNil is a fast guard that the option constructor
// returns a usable, non-nil SubscribeOption.
func TestCoalesceByDocumentKey_NonNil(t *testing.T) {
	t.Parallel()
	if CoalesceByDocumentKey[coDoc]() == nil {
		t.Fatal("CoalesceByDocumentKey returned nil option")
	}
	if CoalesceByDocumentKey[ChangeEvent]() == nil {
		t.Fatal("CoalesceByDocumentKey[ChangeEvent] returned nil option")
	}
}
