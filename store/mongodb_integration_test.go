package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	evtstore "github.com/rbaliyan/event/v3/store"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupStoreIntegrationTest connects to MongoDB and returns a per-test
// MongoStore over a unique database. Tests are skipped when MongoDB is
// unavailable or when -short is set. Teardown is registered via t.Cleanup so it
// runs even if a test panics.
func setupStoreIntegrationTest(t *testing.T) (*MongoStore[testDoc], func()) {
	t.Helper()

	client := mongotest.Connect(t)

	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_store"))
	coll := db.Collection("docs")

	store := NewMongoStore[testDoc](coll)

	cleanup := func() { _ = db.Drop(context.Background()) }
	t.Cleanup(cleanup)

	return store, cleanup
}

func newDoc(id string, createdAt time.Time, name string) testDoc {
	return testDoc{ID: id, CreatedAt: createdAt.UTC().Truncate(time.Millisecond), Name: name}
}

func TestIntegrationCreateAndGet(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	doc := newDoc("id-1", time.Now(), "alice")
	if err := s.Create(ctx, doc); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, "id-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != doc.ID || got.Name != doc.Name {
		t.Errorf("Get returned %+v, want %+v", got, doc)
	}
	if !got.CreatedAt.Equal(doc.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, doc.CreatedAt)
	}
}

func TestIntegrationCreateEmptyID(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	err := s.Create(ctx, newDoc("", time.Now(), "noid"))
	if !errors.Is(err, evtstore.ErrInvalidID) {
		t.Errorf("Create empty ID: got %v, want ErrInvalidID", err)
	}
}

func TestIntegrationCreateDuplicate(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	doc := newDoc("dup", time.Now(), "first")
	if err := s.Create(ctx, doc); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := s.Create(ctx, newDoc("dup", time.Now(), "second"))
	if !errors.Is(err, evtstore.ErrAlreadyExists) {
		t.Errorf("duplicate Create: got %v, want ErrAlreadyExists", err)
	}
}

func TestIntegrationGetMissing(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	_, err := s.Get(ctx, "missing")
	if !errors.Is(err, evtstore.ErrNotFound) {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}

	_, err = s.Get(ctx, "")
	if !errors.Is(err, evtstore.ErrInvalidID) {
		t.Errorf("Get empty ID: got %v, want ErrInvalidID", err)
	}
}

func TestIntegrationUpdate(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	t.Run("missing yields ErrNotFound", func(t *testing.T) {
		err := s.Update(ctx, newDoc("nope", time.Now(), "x"))
		if !errors.Is(err, evtstore.ErrNotFound) {
			t.Errorf("Update missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty ID yields ErrInvalidID", func(t *testing.T) {
		err := s.Update(ctx, newDoc("", time.Now(), "x"))
		if !errors.Is(err, evtstore.ErrInvalidID) {
			t.Errorf("Update empty ID: got %v, want ErrInvalidID", err)
		}
	})

	t.Run("existing updates fields", func(t *testing.T) {
		doc := newDoc("upd", time.Now(), "before")
		if err := s.Create(ctx, doc); err != nil {
			t.Fatalf("Create: %v", err)
		}

		doc.Name = "after"
		if err := s.Update(ctx, doc); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := s.Get(ctx, "upd")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "after" {
			t.Errorf("Name = %q, want %q", got.Name, "after")
		}
	})
}

func TestIntegrationDelete(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	doc := newDoc("del", time.Now(), "x")
	if err := s.Create(ctx, doc); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Delete(ctx, "del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "del"); !errors.Is(err, evtstore.ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}

	// Deleting a non-existent document is not an error.
	if err := s.Delete(ctx, "never-existed"); err != nil {
		t.Errorf("Delete missing: got %v, want nil", err)
	}
}

func TestIntegrationUpsert(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	t.Run("empty ID yields ErrInvalidID", func(t *testing.T) {
		if err := s.Upsert(ctx, newDoc("", time.Now(), "x")); !errors.Is(err, evtstore.ErrInvalidID) {
			t.Errorf("Upsert empty ID: got %v, want ErrInvalidID", err)
		}
	})

	t.Run("insert then update", func(t *testing.T) {
		doc := newDoc("ups", time.Now(), "v1")
		if err := s.Upsert(ctx, doc); err != nil {
			t.Fatalf("Upsert insert: %v", err)
		}
		got, err := s.Get(ctx, "ups")
		if err != nil {
			t.Fatalf("Get after insert: %v", err)
		}
		if got.Name != "v1" {
			t.Errorf("Name = %q, want %q", got.Name, "v1")
		}

		doc.Name = "v2"
		if err := s.Upsert(ctx, doc); err != nil {
			t.Fatalf("Upsert update: %v", err)
		}
		got, err = s.Get(ctx, "ups")
		if err != nil {
			t.Fatalf("Get after update: %v", err)
		}
		if got.Name != "v2" {
			t.Errorf("Name = %q, want %q", got.Name, "v2")
		}
	})
}

func TestIntegrationListPagination(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	base := time.Now().Truncate(time.Second)
	const total = 5
	for i := 0; i < total; i++ {
		doc := newDoc(fmt.Sprintf("p-%d", i), base.Add(time.Duration(i)*time.Second), fmt.Sprintf("n%d", i))
		if err := s.Create(ctx, doc); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// Page through 2 at a time, ascending by created_at.
	var seen []string
	filter := evtstore.Filter{Limit: 2}
	pages := 0
	for {
		page, err := s.List(ctx, filter)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		pages++
		for _, it := range page.Items {
			seen = append(seen, it.ID)
		}
		if !page.HasMore() {
			break
		}
		filter.Cursor = page.NextCursor
		if pages > total+2 {
			t.Fatal("pagination did not terminate")
		}
	}

	if len(seen) != total {
		t.Fatalf("saw %d items across pages, want %d: %v", len(seen), total, seen)
	}
	// Ascending order: p-0, p-1, ... p-4.
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("p-%d", i)
		if seen[i] != want {
			t.Errorf("seen[%d] = %q, want %q (full: %v)", i, seen[i], want, seen)
		}
	}
}

func TestIntegrationListDescendingAndTimeFilter(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	base := time.Now().Truncate(time.Second)
	for i := 0; i < 4; i++ {
		doc := newDoc(fmt.Sprintf("d-%d", i), base.Add(time.Duration(i)*time.Second), "")
		if err := s.Create(ctx, doc); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// Descending: newest first.
	page, err := s.List(ctx, evtstore.Filter{OrderDesc: true, Limit: 10})
	if err != nil {
		t.Fatalf("List desc: %v", err)
	}
	if len(page.Items) != 4 {
		t.Fatalf("got %d items, want 4", len(page.Items))
	}
	if page.Items[0].ID != "d-3" {
		t.Errorf("first desc item = %q, want d-3", page.Items[0].ID)
	}

	// Time filter: [base+1s, base+3s) should include d-1 and d-2.
	page, err = s.List(ctx, evtstore.Filter{
		StartTime: base.Add(1 * time.Second),
		EndTime:   base.Add(3 * time.Second),
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("List time-filtered: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("time-filtered got %d items, want 2: %+v", len(page.Items), page.Items)
	}
}

func TestIntegrationCount(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	base := time.Now().Truncate(time.Second)
	for i := 0; i < 3; i++ {
		if err := s.Create(ctx, newDoc(fmt.Sprintf("c-%d", i), base.Add(time.Duration(i)*time.Second), "")); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	n, err := s.Count(ctx, evtstore.Filter{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("Count = %d, want 3", n)
	}

	n, err = s.Count(ctx, evtstore.Filter{StartTime: base.Add(1 * time.Second)})
	if err != nil {
		t.Fatalf("Count filtered: %v", err)
	}
	if n != 2 {
		t.Errorf("Count filtered = %d, want 2", n)
	}
}

func TestIntegrationDeleteOlderThan(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	now := time.Now()
	// Old document: created 2 hours ago.
	if err := s.Create(ctx, newDoc("old", now.Add(-2*time.Hour), "")); err != nil {
		t.Fatalf("Create old: %v", err)
	}
	// Fresh document: created now.
	if err := s.Create(ctx, newDoc("fresh", now, "")); err != nil {
		t.Fatalf("Create fresh: %v", err)
	}

	deleted, err := s.DeleteOlderThan(ctx, 1*time.Hour)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}

	if _, err := s.Get(ctx, "old"); !errors.Is(err, evtstore.ErrNotFound) {
		t.Errorf("old doc still present: %v", err)
	}
	if _, err := s.Get(ctx, "fresh"); err != nil {
		t.Errorf("fresh doc removed unexpectedly: %v", err)
	}
}

func TestIntegrationEnsureIndexes(t *testing.T) {
	s, _ := setupStoreIntegrationTest(t)
	ctx := context.Background()

	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	cur, err := s.Collection().Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	var idx []map[string]any
	if err := cur.All(ctx, &idx); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	// _id index plus the two created by EnsureIndexes.
	if len(idx) < 3 {
		t.Errorf("got %d indexes, want at least 3: %v", len(idx), idx)
	}

	// Calling again must be idempotent.
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes (second call): %v", err)
	}
}
