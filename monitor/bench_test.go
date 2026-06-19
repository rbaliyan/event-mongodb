package monitor

import (
	"testing"
	"time"

	evtmonitor "github.com/rbaliyan/event/v3/monitor"
)

// benchSimpleFilter is a minimal filter exercising a single scalar branch.
var benchSimpleFilter = evtmonitor.Filter{EventName: "order.created"}

// benchFullFilter exercises every branch of buildFilter.
var benchFullFilter = evtmonitor.Filter{
	EventID:        "evt-1",
	SubscriptionID: "sub-1",
	SubscriberName: "orders",
	EventName:      "order.created",
	BusID:          "bus-1",
	InstanceID:     "pod-1",
	WorkerGroup:    "wg-1",
	DeliveryMode:   deliveryModePtr(evtmonitor.WorkerPool),
	Status:         []evtmonitor.Status{evtmonitor.StatusPending, evtmonitor.StatusFailed},
	HasError:       boolPtr(true),
	StartTime:      time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC),
	EndTime:        time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	MinDuration:    250 * time.Millisecond,
	MinRetries:     3,
}

func BenchmarkBuildFilter(b *testing.B) {
	s := &MongoStore{}

	b.Run("simple", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = s.buildFilter(benchSimpleFilter)
		}
	})

	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = s.buildFilter(benchFullFilter)
		}
	})
}

var benchCursor = mongoCursor{
	StartedAt: time.Date(2026, 6, 19, 10, 30, 0, 0, time.UTC),
	EventID:   "evt-42",
	SubID:     "sub-7",
}

func BenchmarkEncodeMongoCursor(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = encodeMongoCursor(benchCursor)
	}
}

func BenchmarkDecodeMongoCursor(b *testing.B) {
	encoded := encodeMongoCursor(benchCursor)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := decodeMongoCursor(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

var benchEntry = &evtmonitor.Entry{
	EventID:               "evt-1",
	SubscriptionID:        "sub-1",
	SubscriberName:        "orders",
	SubscriberDescription: "orders worker",
	EventName:             "order.created",
	BusID:                 "bus-1",
	InstanceID:            "pod-1",
	DeliveryMode:          evtmonitor.Broadcast,
	Metadata:              map[string]string{"k": "v"},
	Status:                evtmonitor.StatusCompleted,
	RetryCount:            2,
	StartedAt:             time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC),
	Duration:              1500 * time.Millisecond,
	TraceID:               "trace-1",
	SpanID:                "span-1",
	WorkerGroup:           "wg-1",
}

func BenchmarkToEntry(b *testing.B) {
	me := fromEntry(benchEntry)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = me.toEntry()
	}
}

func BenchmarkFromEntry(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fromEntry(benchEntry)
	}
}
