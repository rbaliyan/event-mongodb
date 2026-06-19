package store

import (
	"testing"
	"time"

	evtstore "github.com/rbaliyan/event/v3/store"
)

// BenchmarkBuildTimeQuery measures the pure bson.M construction cost of
// buildTimeQuery across the four filter shapes it branches on: no time bounds,
// start-only, end-only, and both bounds set.
func BenchmarkBuildTimeQuery(b *testing.B) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	s := NewMongoStore[testDoc](nil)

	cases := []struct {
		name   string
		filter evtstore.Filter
	}{
		{"empty", evtstore.Filter{}},
		{"start_only", evtstore.Filter{StartTime: start}},
		{"end_only", evtstore.Filter{EndTime: end}},
		{"both", evtstore.Filter{StartTime: start, EndTime: end}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = s.buildTimeQuery(tc.filter)
			}
		})
	}
}

// BenchmarkBuildCursorQuery measures the pure bson.M construction cost of
// buildCursorQuery for both sort directions, which take separate branches
// ($gt for ascending, $lt for descending).
func BenchmarkBuildCursorQuery(b *testing.B) {
	cursor := evtstore.NewCursor(time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC), "abc123")

	s := NewMongoStore[testDoc](nil)

	b.Run("ascending", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = s.buildCursorQuery(cursor, false)
		}
	})

	b.Run("descending", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = s.buildCursorQuery(cursor, true)
		}
	})
}
