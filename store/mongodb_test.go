package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	evtstore "github.com/rbaliyan/event/v3/store"
)

// testDoc is a minimal concrete type implementing MongoDocument for unit tests.
type testDoc struct {
	ID        string    `bson:"_id"`
	CreatedAt time.Time `bson:"created_at"`
	Name      string    `bson:"name"`
}

func (d testDoc) GetID() string           { return d.ID }
func (d testDoc) GetCreatedAt() time.Time { return d.CreatedAt }

// Compile-time check that testDoc satisfies the MongoDocument constraint.
var _ MongoDocument = testDoc{}

func TestNewMongoStoreDefaults(t *testing.T) {
	s := NewMongoStore[testDoc](nil)

	if s.defaultLimit != 100 {
		t.Errorf("defaultLimit = %d, want 100", s.defaultLimit)
	}
	if s.idField != "_id" {
		t.Errorf("idField = %q, want %q", s.idField, "_id")
	}
	if s.createdAtField != "created_at" {
		t.Errorf("createdAtField = %q, want %q", s.createdAtField, "created_at")
	}
}

func TestWithMongoDefaultLimit(t *testing.T) {
	t.Run("positive value applies", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoDefaultLimit[testDoc](50))
		if s.defaultLimit != 50 {
			t.Errorf("defaultLimit = %d, want 50", s.defaultLimit)
		}
	})

	t.Run("zero is ignored", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoDefaultLimit[testDoc](0))
		if s.defaultLimit != 100 {
			t.Errorf("defaultLimit = %d, want 100 (default kept)", s.defaultLimit)
		}
	})

	t.Run("negative is ignored", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoDefaultLimit[testDoc](-5))
		if s.defaultLimit != 100 {
			t.Errorf("defaultLimit = %d, want 100 (default kept)", s.defaultLimit)
		}
	})
}

func TestWithMongoIDField(t *testing.T) {
	t.Run("non-empty applies", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoIDField[testDoc]("uid"))
		if s.idField != "uid" {
			t.Errorf("idField = %q, want %q", s.idField, "uid")
		}
	})

	t.Run("empty is ignored", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoIDField[testDoc](""))
		if s.idField != "_id" {
			t.Errorf("idField = %q, want %q (default kept)", s.idField, "_id")
		}
	})
}

func TestWithMongoCreatedAtField(t *testing.T) {
	t.Run("non-empty applies", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoCreatedAtField[testDoc]("ts"))
		if s.createdAtField != "ts" {
			t.Errorf("createdAtField = %q, want %q", s.createdAtField, "ts")
		}
	})

	t.Run("empty is ignored", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoCreatedAtField[testDoc](""))
		if s.createdAtField != "created_at" {
			t.Errorf("createdAtField = %q, want %q (default kept)", s.createdAtField, "created_at")
		}
	})
}

func TestEmptyIDGuards(t *testing.T) {
	// These guards short-circuit with ErrInvalidID before touching the
	// collection, so a nil collection is safe here.
	ctx := context.Background()
	s := NewMongoStore[testDoc](nil)
	empty := testDoc{} // GetID() == ""

	t.Run("Create rejects empty id", func(t *testing.T) {
		if err := s.Create(ctx, empty); !errors.Is(err, evtstore.ErrInvalidID) {
			t.Errorf("Create err = %v, want ErrInvalidID", err)
		}
	})

	t.Run("Get rejects empty id", func(t *testing.T) {
		_, err := s.Get(ctx, "")
		if !errors.Is(err, evtstore.ErrInvalidID) {
			t.Errorf("Get err = %v, want ErrInvalidID", err)
		}
	})

	t.Run("Update rejects empty id", func(t *testing.T) {
		if err := s.Update(ctx, empty); !errors.Is(err, evtstore.ErrInvalidID) {
			t.Errorf("Update err = %v, want ErrInvalidID", err)
		}
	})

	t.Run("Upsert rejects empty id", func(t *testing.T) {
		if err := s.Upsert(ctx, empty); !errors.Is(err, evtstore.ErrInvalidID) {
			t.Errorf("Upsert err = %v, want ErrInvalidID", err)
		}
	})
}

func TestBuildTimeQuery(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	t.Run("empty filter yields empty query", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil)
		q := s.buildTimeQuery(evtstore.Filter{})
		if len(q) != 0 {
			t.Errorf("query = %v, want empty", q)
		}
	})

	t.Run("start time only", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil)
		q := s.buildTimeQuery(evtstore.Filter{StartTime: start})

		tq := timeSubQuery(t, q, "created_at")
		if got := tq["$gte"]; got != start {
			t.Errorf("$gte = %v, want %v", got, start)
		}
		if _, ok := tq["$lt"]; ok {
			t.Errorf("unexpected $lt present: %v", tq)
		}
	})

	t.Run("end time only", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil)
		q := s.buildTimeQuery(evtstore.Filter{EndTime: end})

		tq := timeSubQuery(t, q, "created_at")
		if got := tq["$lt"]; got != end {
			t.Errorf("$lt = %v, want %v", got, end)
		}
		if _, ok := tq["$gte"]; ok {
			t.Errorf("unexpected $gte present: %v", tq)
		}
	})

	t.Run("both start and end", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil)
		q := s.buildTimeQuery(evtstore.Filter{StartTime: start, EndTime: end})

		tq := timeSubQuery(t, q, "created_at")
		if got := tq["$gte"]; got != start {
			t.Errorf("$gte = %v, want %v", got, start)
		}
		if got := tq["$lt"]; got != end {
			t.Errorf("$lt = %v, want %v", got, end)
		}
	})

	t.Run("respects custom createdAt field", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil, WithMongoCreatedAtField[testDoc]("ts"))
		q := s.buildTimeQuery(evtstore.Filter{StartTime: start})

		if _, ok := q["created_at"]; ok {
			t.Errorf("query keyed on default field, want custom field: %v", q)
		}
		tq := timeSubQuery(t, q, "ts")
		if got := tq["$gte"]; got != start {
			t.Errorf("$gte = %v, want %v", got, start)
		}
	})
}

// timeSubQuery asserts the query has exactly one entry under field that is a
// bson.M, and returns that sub-query.
func timeSubQuery(t *testing.T, q bson.M, field string) bson.M {
	t.Helper()
	if len(q) != 1 {
		t.Fatalf("query has %d keys, want 1: %v", len(q), q)
	}
	raw, ok := q[field]
	if !ok {
		t.Fatalf("query missing field %q: %v", field, q)
	}
	tq, ok := raw.(bson.M)
	if !ok {
		t.Fatalf("query[%q] is %T, want bson.M", field, raw)
	}
	return tq
}

func TestBuildCursorQuery(t *testing.T) {
	ts := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	id := "abc123"
	cursor := evtstore.NewCursor(ts, id)

	t.Run("ascending uses $gt", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil)
		q := s.buildCursorQuery(cursor, false)
		assertCursorQuery(t, q, "$gt", "created_at", "_id", ts, id)
	})

	t.Run("descending uses $lt", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil)
		q := s.buildCursorQuery(cursor, true)
		assertCursorQuery(t, q, "$lt", "created_at", "_id", ts, id)
	})

	t.Run("respects custom field names", func(t *testing.T) {
		s := NewMongoStore[testDoc](nil,
			WithMongoIDField[testDoc]("uid"),
			WithMongoCreatedAtField[testDoc]("ts"),
		)
		q := s.buildCursorQuery(cursor, false)
		assertCursorQuery(t, q, "$gt", "ts", "uid", ts, id)
	})
}

// assertCursorQuery verifies the compound $or cursor query has the expected
// shape: an $or of two clauses, the first comparing the createdAt field with
// op against the timestamp, the second matching the exact timestamp and
// comparing the id field with op against the id.
func assertCursorQuery(t *testing.T, q bson.M, op, tsField, idField string, ts time.Time, id string) {
	t.Helper()

	rawOr, ok := q["$or"]
	if !ok {
		t.Fatalf("query missing $or: %v", q)
	}
	clauses, ok := rawOr.([]bson.M)
	if !ok {
		t.Fatalf("$or is %T, want []bson.M", rawOr)
	}
	if len(clauses) != 2 {
		t.Fatalf("$or has %d clauses, want 2: %v", len(clauses), clauses)
	}

	// First clause: { tsField: { op: ts } }
	first := clauses[0]
	tsCmp, ok := first[tsField].(bson.M)
	if !ok {
		t.Fatalf("first clause[%q] is %T, want bson.M: %v", tsField, first[tsField], first)
	}
	if got := tsCmp[op]; got != ts {
		t.Errorf("first clause %s = %v, want %v", op, got, ts)
	}

	// Second clause: { tsField: ts, idField: { op: id } }
	second := clauses[1]
	if got := second[tsField]; got != ts {
		t.Errorf("second clause %q = %v, want %v", tsField, got, ts)
	}
	idCmp, ok := second[idField].(bson.M)
	if !ok {
		t.Fatalf("second clause[%q] is %T, want bson.M: %v", idField, second[idField], second)
	}
	if got := idCmp[op]; got != id {
		t.Errorf("second clause %s = %v, want %v", op, got, id)
	}
}
