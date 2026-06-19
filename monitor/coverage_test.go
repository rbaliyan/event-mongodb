package monitor

import (
	"encoding/base64"
	"reflect"
	"testing"
	"time"

	evtmonitor "github.com/rbaliyan/event/v3/monitor"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// boolPtr is a local helper for constructing *bool filter fields.
func boolPtr(b bool) *bool { return &b }

// deliveryModePtr is a local helper for constructing *DeliveryMode filter fields.
func deliveryModePtr(m evtmonitor.DeliveryMode) *evtmonitor.DeliveryMode { return &m }

// TestBuildFilter exercises buildFilter for each Filter field in isolation plus
// a few combinations, asserting the exact bson.M produced. This is pure logic:
// no MongoDB connection is involved.
func TestBuildFilter(t *testing.T) {
	t.Parallel()

	// Fixed timestamps so combination cases are deterministic.
	start := time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		filter evtmonitor.Filter
		want   bson.M
	}{
		{
			name:   "empty filter",
			filter: evtmonitor.Filter{},
			want:   bson.M{},
		},
		{
			name:   "event_id",
			filter: evtmonitor.Filter{EventID: "evt-1"},
			want:   bson.M{"event_id": "evt-1"},
		},
		{
			name:   "subscription_id",
			filter: evtmonitor.Filter{SubscriptionID: "sub-1"},
			want:   bson.M{"subscription_id": "sub-1"},
		},
		{
			name:   "subscriber_name",
			filter: evtmonitor.Filter{SubscriberName: "orders-worker"},
			want:   bson.M{"subscriber_name": "orders-worker"},
		},
		{
			name:   "event_name",
			filter: evtmonitor.Filter{EventName: "order.created"},
			want:   bson.M{"event_name": "order.created"},
		},
		{
			name:   "bus_id",
			filter: evtmonitor.Filter{BusID: "bus-1"},
			want:   bson.M{"bus_id": "bus-1"},
		},
		{
			name:   "instance_id",
			filter: evtmonitor.Filter{InstanceID: "pod-1"},
			want:   bson.M{"instance_id": "pod-1"},
		},
		{
			name:   "worker_group",
			filter: evtmonitor.Filter{WorkerGroup: "wg-1"},
			want:   bson.M{"worker_group": "wg-1"},
		},
		{
			name:   "delivery_mode broadcast",
			filter: evtmonitor.Filter{DeliveryMode: deliveryModePtr(evtmonitor.Broadcast)},
			want:   bson.M{"delivery_mode": "broadcast"},
		},
		{
			name:   "delivery_mode worker_pool",
			filter: evtmonitor.Filter{DeliveryMode: deliveryModePtr(evtmonitor.WorkerPool)},
			want:   bson.M{"delivery_mode": "worker_pool"},
		},
		{
			name:   "status single",
			filter: evtmonitor.Filter{Status: []evtmonitor.Status{evtmonitor.StatusFailed}},
			want:   bson.M{"status": bson.M{"$in": []string{"failed"}}},
		},
		{
			name: "status multiple",
			filter: evtmonitor.Filter{Status: []evtmonitor.Status{
				evtmonitor.StatusPending, evtmonitor.StatusRetrying,
			}},
			want: bson.M{"status": bson.M{"$in": []string{"pending", "retrying"}}},
		},
		{
			name:   "has_error true",
			filter: evtmonitor.Filter{HasError: boolPtr(true)},
			want:   bson.M{"error": bson.M{"$ne": ""}},
		},
		{
			name:   "has_error false",
			filter: evtmonitor.Filter{HasError: boolPtr(false)},
			want: bson.M{"$or": []bson.M{
				{"error": ""},
				{"error": bson.M{"$exists": false}},
			}},
		},
		{
			name:   "start_time only",
			filter: evtmonitor.Filter{StartTime: start},
			want:   bson.M{"started_at": bson.M{"$gte": start}},
		},
		{
			name:   "end_time only",
			filter: evtmonitor.Filter{EndTime: end},
			want:   bson.M{"started_at": bson.M{"$lt": end}},
		},
		{
			name:   "start_time and end_time merge into one started_at map",
			filter: evtmonitor.Filter{StartTime: start, EndTime: end},
			want:   bson.M{"started_at": bson.M{"$gte": start, "$lt": end}},
		},
		{
			name:   "min_duration",
			filter: evtmonitor.Filter{MinDuration: 250 * time.Millisecond},
			want:   bson.M{"duration_ms": bson.M{"$gte": int64(250)}},
		},
		{
			name:   "min_duration sub-millisecond is dropped",
			filter: evtmonitor.Filter{MinDuration: 500 * time.Microsecond},
			want:   bson.M{"duration_ms": bson.M{"$gte": int64(0)}},
		},
		{
			name:   "min_retries",
			filter: evtmonitor.Filter{MinRetries: 3},
			want:   bson.M{"retry_count": bson.M{"$gte": 3}},
		},
		{
			name: "combination of scalar fields",
			filter: evtmonitor.Filter{
				EventID:        "evt-1",
				SubscriptionID: "sub-1",
				EventName:      "order.created",
				BusID:          "bus-1",
			},
			want: bson.M{
				"event_id":        "evt-1",
				"subscription_id": "sub-1",
				"event_name":      "order.created",
				"bus_id":          "bus-1",
			},
		},
		{
			name: "combination with time range, status, and has_error",
			filter: evtmonitor.Filter{
				EventName: "order.created",
				Status:    []evtmonitor.Status{evtmonitor.StatusFailed},
				HasError:  boolPtr(true),
				StartTime: start,
				EndTime:   end,
			},
			want: bson.M{
				"event_name": "order.created",
				"status":     bson.M{"$in": []string{"failed"}},
				"error":      bson.M{"$ne": ""},
				"started_at": bson.M{"$gte": start, "$lt": end},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &MongoStore{}
			got := s.buildFilter(tc.filter)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("buildFilter() mismatch\n got: %#v\nwant: %#v", got, tc.want)
			}
		})
	}
}

// TestBuildFilter_ZeroValuesOmitted asserts that fields left at their zero
// value never appear in the produced filter (e.g. negative MinRetries and
// nil pointers are skipped).
func TestBuildFilter_ZeroValuesOmitted(t *testing.T) {
	t.Parallel()
	s := &MongoStore{}
	got := s.buildFilter(evtmonitor.Filter{
		MinRetries:  0,
		MinDuration: 0,
		Status:      nil,
		HasError:    nil,
		// Cursor/Limit/OrderDesc do not affect buildFilter.
		Cursor:    "ignored",
		Limit:     50,
		OrderDesc: true,
	})
	if len(got) != 0 {
		t.Errorf("expected empty filter for all-zero criteria, got %#v", got)
	}
}

// TestMongoCursor_RoundTrip verifies encode -> decode returns an equal cursor.
func TestMongoCursor_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cur  mongoCursor
	}{
		{
			name: "fully populated",
			cur: mongoCursor{
				StartedAt: time.Date(2026, 6, 19, 10, 30, 0, 0, time.UTC),
				EventID:   "evt-42",
				SubID:     "sub-7",
			},
		},
		{
			name: "zero value",
			cur:  mongoCursor{},
		},
		{
			name: "only timestamp",
			cur:  mongoCursor{StartedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
		{
			name: "ids with special chars",
			cur:  mongoCursor{EventID: "evt/with:weird\"chars", SubID: "sub\nnewline"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded := encodeMongoCursor(tc.cur)
			got, err := decodeMongoCursor(encoded)
			if err != nil {
				t.Fatalf("decodeMongoCursor(%q) error: %v", encoded, err)
			}
			// time.Time round-trips through JSON as RFC3339; compare with Equal.
			if !got.StartedAt.Equal(tc.cur.StartedAt) {
				t.Errorf("StartedAt = %v, want %v", got.StartedAt, tc.cur.StartedAt)
			}
			if got.EventID != tc.cur.EventID {
				t.Errorf("EventID = %q, want %q", got.EventID, tc.cur.EventID)
			}
			if got.SubID != tc.cur.SubID {
				t.Errorf("SubID = %q, want %q", got.SubID, tc.cur.SubID)
			}
		})
	}
}

// TestDecodeMongoCursor_Errors covers the empty-string, non-base64, and
// valid-base64-of-invalid-json paths.
func TestDecodeMongoCursor_Errors(t *testing.T) {
	t.Parallel()

	t.Run("empty string yields zero cursor without error", func(t *testing.T) {
		t.Parallel()
		got, err := decodeMongoCursor("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != (mongoCursor{}) {
			t.Errorf("expected zero cursor, got %#v", got)
		}
	})

	t.Run("non-base64 input returns error", func(t *testing.T) {
		t.Parallel()
		// '!' and '@' are not valid standard base64 characters.
		_, err := decodeMongoCursor("not!base64@@")
		if err == nil {
			t.Fatal("expected an error for non-base64 input, got nil")
		}
	})

	t.Run("valid base64 of invalid json returns error", func(t *testing.T) {
		t.Parallel()
		// "{ this is not json" base64-encoded.
		enc := base64.StdEncoding.EncodeToString([]byte("{ this is not json"))
		_, err := decodeMongoCursor(enc)
		if err == nil {
			t.Fatal("expected a JSON unmarshal error, got nil")
		}
	})
}

// TestEntryRoundTrip verifies that fromEntry(toEntry(e)) preserves fields,
// including zero time and zero duration handling.
func TestEntryRoundTrip(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 6, 19, 11, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		in   *evtmonitor.Entry
	}{
		{
			name: "fully populated broadcast entry",
			in: &evtmonitor.Entry{
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
				Error:                 "",
				RetryCount:            2,
				StartedAt:             time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC),
				CompletedAt:           &completedAt,
				Duration:              1500 * time.Millisecond,
				TraceID:               "trace-1",
				SpanID:                "span-1",
				WorkerGroup:           "wg-1",
			},
		},
		{
			name: "zero time and zero duration",
			in: &evtmonitor.Entry{
				EventID:        "evt-2",
				SubscriptionID: "sub-2",
				EventName:      "order.shipped",
				BusID:          "bus-1",
				DeliveryMode:   evtmonitor.Broadcast,
				Status:         evtmonitor.StatusPending,
				StartedAt:      time.Time{},
				Duration:       0,
			},
		},
		{
			name: "failed entry with error",
			in: &evtmonitor.Entry{
				EventID:        "evt-3",
				SubscriptionID: "sub-3",
				EventName:      "order.failed",
				BusID:          "bus-1",
				DeliveryMode:   evtmonitor.Broadcast,
				Status:         evtmonitor.StatusFailed,
				Error:          "boom",
				StartedAt:      time.Date(2026, 6, 19, 9, 0, 0, 0, time.UTC),
				Duration:       42 * time.Millisecond,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := fromEntry(tc.in).toEntry()

			assertEntryEqual(t, got, tc.in)
		})
	}
}

// TestEntryRoundTrip_WorkerPoolClearsSubscriptionID documents the WorkerPool
// special case: fromEntry blanks SubscriptionID, so the round-trip does not
// preserve it. This is intentional production behavior.
func TestEntryRoundTrip_WorkerPoolClearsSubscriptionID(t *testing.T) {
	t.Parallel()
	in := &evtmonitor.Entry{
		EventID:        "evt-wp",
		SubscriptionID: "should-be-cleared",
		EventName:      "wp.event",
		BusID:          "bus-1",
		DeliveryMode:   evtmonitor.WorkerPool,
		Status:         evtmonitor.StatusPending,
		StartedAt:      time.Date(2026, 6, 19, 8, 0, 0, 0, time.UTC),
	}
	got := fromEntry(in).toEntry()
	if got.SubscriptionID != "" {
		t.Errorf("WorkerPool SubscriptionID = %q, want empty", got.SubscriptionID)
	}
	if got.DeliveryMode != evtmonitor.WorkerPool {
		t.Errorf("DeliveryMode = %v, want WorkerPool", got.DeliveryMode)
	}
}

// assertEntryEqual compares two entries field-by-field, using time.Equal for
// timestamps so monotonic-clock and location differences do not cause spurious
// failures.
func assertEntryEqual(t *testing.T, got, want *evtmonitor.Entry) {
	t.Helper()
	if got.EventID != want.EventID {
		t.Errorf("EventID = %q, want %q", got.EventID, want.EventID)
	}
	if got.SubscriptionID != want.SubscriptionID {
		t.Errorf("SubscriptionID = %q, want %q", got.SubscriptionID, want.SubscriptionID)
	}
	if got.SubscriberName != want.SubscriberName {
		t.Errorf("SubscriberName = %q, want %q", got.SubscriberName, want.SubscriberName)
	}
	if got.SubscriberDescription != want.SubscriberDescription {
		t.Errorf("SubscriberDescription = %q, want %q", got.SubscriberDescription, want.SubscriberDescription)
	}
	if got.EventName != want.EventName {
		t.Errorf("EventName = %q, want %q", got.EventName, want.EventName)
	}
	if got.BusID != want.BusID {
		t.Errorf("BusID = %q, want %q", got.BusID, want.BusID)
	}
	if got.InstanceID != want.InstanceID {
		t.Errorf("InstanceID = %q, want %q", got.InstanceID, want.InstanceID)
	}
	if got.DeliveryMode != want.DeliveryMode {
		t.Errorf("DeliveryMode = %v, want %v", got.DeliveryMode, want.DeliveryMode)
	}
	if !reflect.DeepEqual(got.Metadata, want.Metadata) {
		t.Errorf("Metadata = %#v, want %#v", got.Metadata, want.Metadata)
	}
	if got.Status != want.Status {
		t.Errorf("Status = %q, want %q", got.Status, want.Status)
	}
	if got.Error != want.Error {
		t.Errorf("Error = %q, want %q", got.Error, want.Error)
	}
	if got.RetryCount != want.RetryCount {
		t.Errorf("RetryCount = %d, want %d", got.RetryCount, want.RetryCount)
	}
	if !got.StartedAt.Equal(want.StartedAt) {
		t.Errorf("StartedAt = %v, want %v", got.StartedAt, want.StartedAt)
	}
	switch {
	case got.CompletedAt == nil && want.CompletedAt != nil:
		t.Errorf("CompletedAt = nil, want %v", *want.CompletedAt)
	case got.CompletedAt != nil && want.CompletedAt == nil:
		t.Errorf("CompletedAt = %v, want nil", *got.CompletedAt)
	case got.CompletedAt != nil && want.CompletedAt != nil && !got.CompletedAt.Equal(*want.CompletedAt):
		t.Errorf("CompletedAt = %v, want %v", *got.CompletedAt, *want.CompletedAt)
	}
	if got.Duration != want.Duration {
		t.Errorf("Duration = %v, want %v", got.Duration, want.Duration)
	}
	if got.TraceID != want.TraceID {
		t.Errorf("TraceID = %q, want %q", got.TraceID, want.TraceID)
	}
	if got.SpanID != want.SpanID {
		t.Errorf("SpanID = %q, want %q", got.SpanID, want.SpanID)
	}
	if got.WorkerGroup != want.WorkerGroup {
		t.Errorf("WorkerGroup = %q, want %q", got.WorkerGroup, want.WorkerGroup)
	}
}
