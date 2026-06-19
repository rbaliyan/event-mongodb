package monitor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	evtmonitor "github.com/rbaliyan/event/v3/monitor"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupMonitorIntegration connects to MongoDB and returns a per-test MongoStore
// over a unique database with indexes ensured. Tests are skipped when MongoDB is
// unavailable or when -short is set. The database is dropped via t.Cleanup so it
// runs even if a test panics.
func setupMonitorIntegration(t *testing.T) *MongoStore {
	t.Helper()

	client := mongotest.Connect(t)

	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_monitor"))
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	store, err := NewMongoStore(db)
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	if err := store.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return store
}

// monitorTestTime returns a deterministic, BSON-safe timestamp: UTC and
// truncated to milliseconds, since MongoDB stores time at millisecond
// resolution and drops the monotonic clock / location.
func monitorTestTime() time.Time {
	return time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
}

// newMonitorEntry constructs a realistic broadcast Entry for integration tests.
func newMonitorEntry(eventID, subID string, status evtmonitor.Status, startedAt time.Time) *evtmonitor.Entry {
	return &evtmonitor.Entry{
		EventID:               eventID,
		SubscriptionID:        subID,
		SubscriberName:        "orders-worker",
		SubscriberDescription: "processes order events",
		EventName:             "order.created",
		BusID:                 "bus-1",
		InstanceID:            "pod-1",
		DeliveryMode:          evtmonitor.Broadcast,
		Metadata:              map[string]string{"region": "us-east-1"},
		Status:                status,
		RetryCount:            0,
		StartedAt:             startedAt,
		TraceID:               "trace-" + eventID,
		SpanID:                "span-" + eventID,
	}
}

func TestIntegrationMonitorRecordAndGet(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	start := monitorTestTime()
	entry := newMonitorEntry("evt-1", "sub-1", evtmonitor.StatusPending, start)
	if err := s.Record(ctx, entry); err != nil {
		t.Fatalf("Record: %v", err)
	}

	got, err := s.Get(ctx, "evt-1", "sub-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("Get returned nil for an entry that was recorded")
	}
	assertEntryEqual(t, got, entry)
}

func TestIntegrationMonitorGetMissingReturnsNil(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	got, err := s.Get(ctx, "does-not-exist", "nope")
	if err != nil {
		t.Fatalf("Get of missing entry returned error: %v", err)
	}
	if got != nil {
		t.Errorf("Get of missing entry = %+v, want nil", got)
	}
}

func TestIntegrationMonitorRecordUpsert(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	start := monitorTestTime()
	entry := newMonitorEntry("evt-up", "sub-up", evtmonitor.StatusPending, start)
	if err := s.Record(ctx, entry); err != nil {
		t.Fatalf("Record (insert): %v", err)
	}

	// Re-record the same key with a new status; broadcast mode uses $set, so the
	// existing document is updated in place rather than duplicated.
	entry.Status = evtmonitor.StatusCompleted
	entry.Error = ""
	if err := s.Record(ctx, entry); err != nil {
		t.Fatalf("Record (update): %v", err)
	}

	count, err := s.Count(ctx, evtmonitor.Filter{EventID: "evt-up"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("Count after re-record = %d, want 1 (upsert, not duplicate)", count)
	}

	got, err := s.Get(ctx, "evt-up", "sub-up")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != evtmonitor.StatusCompleted {
		t.Errorf("Status = %q, want %q", got.Status, evtmonitor.StatusCompleted)
	}
}

func TestIntegrationMonitorGetByEventID(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	start := monitorTestTime()
	// One event fanned out to three subscriptions plus an unrelated event.
	subs := []string{"sub-a", "sub-b", "sub-c"}
	for i, sub := range subs {
		e := newMonitorEntry("evt-fan", sub, evtmonitor.StatusCompleted, start.Add(time.Duration(i)*time.Second))
		if err := s.Record(ctx, e); err != nil {
			t.Fatalf("Record %s: %v", sub, err)
		}
	}
	other := newMonitorEntry("evt-other", "sub-x", evtmonitor.StatusCompleted, start)
	if err := s.Record(ctx, other); err != nil {
		t.Fatalf("Record other: %v", err)
	}

	entries, err := s.GetByEventID(ctx, "evt-fan")
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if len(entries) != len(subs) {
		t.Fatalf("GetByEventID returned %d entries, want %d", len(entries), len(subs))
	}
	// GetByEventID sorts by started_at ascending — verify order and identity.
	for i, e := range entries {
		if e.EventID != "evt-fan" {
			t.Errorf("entry[%d].EventID = %q, want evt-fan", i, e.EventID)
		}
		if e.SubscriptionID != subs[i] {
			t.Errorf("entry[%d].SubscriptionID = %q, want %q (sorted by started_at)", i, e.SubscriptionID, subs[i])
		}
	}

	// Missing event ID yields no entries and no error.
	none, err := s.GetByEventID(ctx, "no-such-event")
	if err != nil {
		t.Fatalf("GetByEventID(missing): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("GetByEventID(missing) returned %d entries, want 0", len(none))
	}
}

func TestIntegrationMonitorUpdateStatus(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	start := monitorTestTime()
	entry := newMonitorEntry("evt-us", "sub-us", evtmonitor.StatusPending, start)
	if err := s.Record(ctx, entry); err != nil {
		t.Fatalf("Record: %v", err)
	}

	wantErr := errors.New("handler blew up")
	const dur = 750 * time.Millisecond
	if err := s.UpdateStatus(ctx, "evt-us", "sub-us", evtmonitor.StatusFailed, wantErr, dur); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	got, err := s.Get(ctx, "evt-us", "sub-us")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != evtmonitor.StatusFailed {
		t.Errorf("Status = %q, want %q", got.Status, evtmonitor.StatusFailed)
	}
	if got.Error != wantErr.Error() {
		t.Errorf("Error = %q, want %q", got.Error, wantErr.Error())
	}
	if got.Duration != dur {
		t.Errorf("Duration = %v, want %v", got.Duration, dur)
	}
	if got.CompletedAt == nil {
		t.Error("CompletedAt = nil, want a timestamp after UpdateStatus")
	}
}

func TestIntegrationMonitorListFilters(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	base := monitorTestTime()
	// Seed a deterministic mix of statuses and times.
	seed := []struct {
		eventID string
		subID   string
		status  evtmonitor.Status
		errMsg  string
		offset  time.Duration
	}{
		{"e1", "s1", evtmonitor.StatusCompleted, "", 0},
		{"e2", "s1", evtmonitor.StatusFailed, "boom", 1 * time.Hour},
		{"e3", "s1", evtmonitor.StatusPending, "", 2 * time.Hour},
		{"e4", "s1", evtmonitor.StatusFailed, "boom again", 3 * time.Hour},
		{"e5", "s1", evtmonitor.StatusCompleted, "", 4 * time.Hour},
	}
	for _, sd := range seed {
		e := newMonitorEntry(sd.eventID, sd.subID, sd.status, base.Add(sd.offset))
		e.Error = sd.errMsg
		if err := s.Record(ctx, e); err != nil {
			t.Fatalf("Record %s: %v", sd.eventID, err)
		}
	}

	assertListCount := func(name string, filter evtmonitor.Filter, want int) {
		t.Helper()
		page, err := s.List(ctx, filter)
		if err != nil {
			t.Fatalf("%s: List: %v", name, err)
		}
		if len(page.Entries) != want {
			t.Errorf("%s: List returned %d entries, want %d", name, len(page.Entries), want)
		}
		count, err := s.Count(ctx, filter)
		if err != nil {
			t.Fatalf("%s: Count: %v", name, err)
		}
		if count != int64(want) {
			t.Errorf("%s: Count = %d, want %d", name, count, want)
		}
	}

	assertListCount("by status failed",
		evtmonitor.Filter{Status: []evtmonitor.Status{evtmonitor.StatusFailed}}, 2)
	assertListCount("by status completed+pending",
		evtmonitor.Filter{Status: []evtmonitor.Status{evtmonitor.StatusCompleted, evtmonitor.StatusPending}}, 3)
	assertListCount("has error true",
		evtmonitor.Filter{HasError: boolPtr(true)}, 2)
	assertListCount("has error false",
		evtmonitor.Filter{HasError: boolPtr(false)}, 3)
	// Time range [base+1h, base+3h): e2 and e3 (e4 at +3h is excluded, EndTime is exclusive).
	assertListCount("time range",
		evtmonitor.Filter{StartTime: base.Add(1 * time.Hour), EndTime: base.Add(3 * time.Hour)}, 2)
	assertListCount("no filter returns all",
		evtmonitor.Filter{}, len(seed))
}

func TestIntegrationMonitorListPagination(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	base := monitorTestTime()
	const total = 7
	for i := 0; i < total; i++ {
		e := newMonitorEntry(fmt.Sprintf("evt-%02d", i), "sub-1", evtmonitor.StatusCompleted, base.Add(time.Duration(i)*time.Minute))
		if err := s.Record(ctx, e); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	// Page through 3 at a time using the returned cursor and accumulate IDs.
	var seen []string
	cursor := ""
	pages := 0
	for {
		page, err := s.List(ctx, evtmonitor.Filter{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		pages++
		for _, e := range page.Entries {
			seen = append(seen, e.EventID)
		}
		if !page.HasMore {
			if page.NextCursor != "" {
				t.Errorf("last page has no more results but NextCursor = %q", page.NextCursor)
			}
			break
		}
		if page.NextCursor == "" {
			t.Fatal("HasMore is true but NextCursor is empty")
		}
		cursor = page.NextCursor
		if pages > total+2 {
			t.Fatal("pagination did not terminate")
		}
	}

	if len(seen) != total {
		t.Fatalf("paginated %d entries across %d pages, want %d", len(seen), pages, total)
	}
	// Default ascending order by started_at, and no duplicates across pages.
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("evt-%02d", i)
		if seen[i] != want {
			t.Errorf("seen[%d] = %q, want %q (ascending, no dupes)", i, seen[i], want)
		}
	}
}

func TestIntegrationMonitorDeleteOlderThan(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	now := time.Now().UTC()
	// Two old entries (well beyond the cutoff) and two recent ones.
	old1 := newMonitorEntry("old-1", "s", evtmonitor.StatusCompleted, now.Add(-48*time.Hour))
	old2 := newMonitorEntry("old-2", "s", evtmonitor.StatusCompleted, now.Add(-36*time.Hour))
	new1 := newMonitorEntry("new-1", "s", evtmonitor.StatusCompleted, now.Add(-1*time.Hour))
	new2 := newMonitorEntry("new-2", "s", evtmonitor.StatusCompleted, now.Add(-2*time.Minute))
	for _, e := range []*evtmonitor.Entry{old1, old2, new1, new2} {
		if err := s.Record(ctx, e); err != nil {
			t.Fatalf("Record %s: %v", e.EventID, err)
		}
	}

	deleted, err := s.DeleteOlderThan(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 2 {
		t.Errorf("DeleteOlderThan returned %d, want 2", deleted)
	}

	remaining, err := s.Count(ctx, evtmonitor.Filter{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if remaining != 2 {
		t.Errorf("remaining entries = %d, want 2", remaining)
	}
}

func TestIntegrationMonitorRecordStartComplete(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	// Lifecycle 1: success.
	startOK := event.RecordStartParams{
		EventID:        "evt-ok",
		SubscriptionID: "sub-ok",
		EventName:      "order.created",
		BusID:          "bus-1",
		SubscriberName: "orders",
		Metadata:       map[string]string{"k": "v"},
		TraceID:        "trace-ok",
		SpanID:         "span-ok",
	}
	if err := s.RecordStart(ctx, startOK); err != nil {
		t.Fatalf("RecordStart(ok): %v", err)
	}
	pending, err := s.Get(ctx, "evt-ok", "sub-ok")
	if err != nil {
		t.Fatalf("Get(pending): %v", err)
	}
	if pending == nil || pending.Status != evtmonitor.StatusPending {
		t.Fatalf("after RecordStart, status = %v, want pending", pending)
	}

	if err := s.RecordComplete(ctx, event.RecordCompleteParams{
		EventID:        "evt-ok",
		SubscriptionID: "sub-ok",
		Status:         string(evtmonitor.StatusCompleted),
		Duration:       120 * time.Millisecond,
	}); err != nil {
		t.Fatalf("RecordComplete(ok): %v", err)
	}
	done, err := s.Get(ctx, "evt-ok", "sub-ok")
	if err != nil {
		t.Fatalf("Get(done): %v", err)
	}
	if done.Status != evtmonitor.StatusCompleted {
		t.Errorf("status = %q, want completed", done.Status)
	}
	if done.Duration != 120*time.Millisecond {
		t.Errorf("duration = %v, want 120ms", done.Duration)
	}

	// Lifecycle 2: failure.
	if err := s.RecordStart(ctx, event.RecordStartParams{
		EventID:        "evt-bad",
		SubscriptionID: "sub-bad",
		EventName:      "order.created",
		BusID:          "bus-1",
	}); err != nil {
		t.Fatalf("RecordStart(bad): %v", err)
	}
	if err := s.RecordComplete(ctx, event.RecordCompleteParams{
		EventID:        "evt-bad",
		SubscriptionID: "sub-bad",
		Status:         string(evtmonitor.StatusFailed),
		Error:          errors.New("kaboom"),
		Duration:       5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("RecordComplete(bad): %v", err)
	}
	bad, err := s.Get(ctx, "evt-bad", "sub-bad")
	if err != nil {
		t.Fatalf("Get(bad): %v", err)
	}
	if bad.Status != evtmonitor.StatusFailed {
		t.Errorf("status = %q, want failed", bad.Status)
	}
	if bad.Error != "kaboom" {
		t.Errorf("error = %q, want kaboom", bad.Error)
	}

	// List should reflect both completed and failed lifecycles.
	failedPage, err := s.List(ctx, evtmonitor.Filter{Status: []evtmonitor.Status{evtmonitor.StatusFailed}})
	if err != nil {
		t.Fatalf("List(failed): %v", err)
	}
	if len(failedPage.Entries) != 1 || failedPage.Entries[0].EventID != "evt-bad" {
		t.Errorf("List(failed) = %+v, want exactly evt-bad", failedPage.Entries)
	}
}

func TestIntegrationMonitorSummary(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	// Use recent timestamps so Summary's default 24h window includes them.
	now := time.Now().UTC()
	seed := []struct {
		eventID string
		name    string
		status  evtmonitor.Status
	}{
		{"s1", "order.created", evtmonitor.StatusCompleted},
		{"s2", "order.created", evtmonitor.StatusCompleted},
		{"s3", "order.created", evtmonitor.StatusFailed},
		{"s4", "order.shipped", evtmonitor.StatusPending},
	}
	for i, sd := range seed {
		e := newMonitorEntry(sd.eventID, "sub", sd.status, now.Add(-time.Duration(i)*time.Minute))
		e.EventName = sd.name
		if err := s.Record(ctx, e); err != nil {
			t.Fatalf("Record %s: %v", sd.eventID, err)
		}
	}

	summary, err := s.Summary(ctx, evtmonitor.Filter{})
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	if summary.TotalEntries != int64(len(seed)) {
		t.Errorf("TotalEntries = %d, want %d", summary.TotalEntries, len(seed))
	}
	if got := summary.ByStatus[evtmonitor.StatusCompleted]; got != 2 {
		t.Errorf("ByStatus[completed] = %d, want 2", got)
	}
	if got := summary.ByStatus[evtmonitor.StatusFailed]; got != 1 {
		t.Errorf("ByStatus[failed] = %d, want 1", got)
	}
	if got := summary.ByStatus[evtmonitor.StatusPending]; got != 1 {
		t.Errorf("ByStatus[pending] = %d, want 1", got)
	}
	created, ok := summary.ByEventName["order.created"]
	if !ok {
		t.Fatal("ByEventName missing order.created")
	}
	if created.Total != 3 || created.Completed != 2 || created.Failed != 1 {
		t.Errorf("order.created stats = %+v, want total=3 completed=2 failed=1", created)
	}
	if summary.TimeRange.Oldest == nil || summary.TimeRange.Newest == nil {
		t.Error("Summary.TimeRange should have oldest and newest set")
	}
}

func TestIntegrationMonitorStuckPending(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	now := time.Now().UTC()
	// Two pending entries with an old started_at (stuck), one recent pending
	// (not stuck), and one old completed (wrong status — must be ignored).
	stuck1 := newMonitorEntry("stuck-1", "s", evtmonitor.StatusPending, now.Add(-30*time.Minute))
	stuck2 := newMonitorEntry("stuck-2", "s", evtmonitor.StatusPending, now.Add(-20*time.Minute))
	freshPending := newMonitorEntry("fresh", "s", evtmonitor.StatusPending, now.Add(-1*time.Minute))
	oldDone := newMonitorEntry("done", "s", evtmonitor.StatusCompleted, now.Add(-45*time.Minute))
	for _, e := range []*evtmonitor.Entry{stuck1, stuck2, freshPending, oldDone} {
		if err := s.Record(ctx, e); err != nil {
			t.Fatalf("Record %s: %v", e.EventID, err)
		}
	}

	const threshold = 10 * time.Minute
	count, err := s.StuckPendingCount(ctx, threshold)
	if err != nil {
		t.Fatalf("StuckPendingCount: %v", err)
	}
	if count != 2 {
		t.Errorf("StuckPendingCount = %d, want 2", count)
	}

	entries, err := s.StuckPendingEntries(ctx, threshold, 10)
	if err != nil {
		t.Fatalf("StuckPendingEntries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("StuckPendingEntries returned %d, want 2", len(entries))
	}
	// Sorted oldest-first.
	if entries[0].EventID != "stuck-1" || entries[1].EventID != "stuck-2" {
		t.Errorf("StuckPendingEntries order = [%s, %s], want [stuck-1, stuck-2]",
			entries[0].EventID, entries[1].EventID)
	}
}

func TestIntegrationMonitorRecordPublish(t *testing.T) {
	s := setupMonitorIntegration(t)
	ctx := context.Background()

	if err := s.RecordPublish(ctx, event.RecordPublishParams{
		EventID:   "evt-pub",
		EventName: "order.created",
		BusID:     "bus-1",
		BusName:   "orders-bus",
		Metadata:  map[string]string{"k": "v"},
		TraceID:   "trace-pub",
		SpanID:    "span-pub",
	}); err != nil {
		t.Fatalf("RecordPublish: %v", err)
	}

	// The publish milestone is keyed under (EventID, PublishMarker).
	got, err := s.Get(ctx, "evt-pub", evtmonitor.PublishMarker)
	if err != nil {
		t.Fatalf("Get(publish): %v", err)
	}
	if got == nil {
		t.Fatal("publish entry not found")
	}
	if got.Status != evtmonitor.StatusPublished {
		t.Errorf("status = %q, want %q", got.Status, evtmonitor.StatusPublished)
	}
	if got.EventName != "order.created" {
		t.Errorf("event name = %q, want order.created", got.EventName)
	}

	// GetByEventID returns the publish entry alongside subscriber lineage.
	sub := newMonitorEntry("evt-pub", "sub-1", evtmonitor.StatusCompleted, got.StartedAt)
	if err := s.Record(ctx, sub); err != nil {
		t.Fatalf("Record subscriber: %v", err)
	}
	lineage, err := s.GetByEventID(ctx, "evt-pub")
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if len(lineage) != 2 {
		t.Errorf("lineage has %d entries, want 2 (publish + subscriber)", len(lineage))
	}
}

func TestIntegrationMonitorWithTTLCreatesIndex(t *testing.T) {
	client := mongotest.Connect(t)
	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_monitor_ttl"))
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	store, err := NewMongoStore(db)
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	const ttl = 36 * time.Hour
	store.WithTTL(ttl)
	if err := store.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	ctx := context.Background()
	cursor, err := store.Collection().Indexes().List(ctx)
	if err != nil {
		t.Fatalf("Indexes().List: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var ttlFound bool
	for cursor.Next(ctx) {
		var idx struct {
			Name               string `bson:"name"`
			ExpireAfterSeconds *int64 `bson:"expireAfterSeconds"`
		}
		if err := cursor.Decode(&idx); err != nil {
			t.Fatalf("decode index: %v", err)
		}
		if idx.Name == "started_at_ttl" {
			ttlFound = true
			if idx.ExpireAfterSeconds == nil {
				t.Error("started_at_ttl index has no expireAfterSeconds")
			} else if *idx.ExpireAfterSeconds != int64(ttl.Seconds()) {
				t.Errorf("expireAfterSeconds = %d, want %d", *idx.ExpireAfterSeconds, int64(ttl.Seconds()))
			}
		}
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor error: %v", err)
	}
	if !ttlFound {
		t.Error("TTL index started_at_ttl not created after WithTTL + EnsureIndexes")
	}
}
