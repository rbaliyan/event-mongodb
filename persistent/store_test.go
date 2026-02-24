package persistent

import (
	"errors"
	"testing"
	"time"

	eventerrors "github.com/rbaliyan/event/v3/errors"
)

func TestNewStore_NilCollection(t *testing.T) {
	_, err := NewStore(nil)
	if err == nil {
		t.Fatal("expected error for nil collection")
	}
	if err != ErrCollectionRequired {
		t.Errorf("expected ErrCollectionRequired, got %v", err)
	}
}

func TestNewStore_ErrCollectionRequired_WrapsInvalidArgument(t *testing.T) {
	if !errors.Is(ErrCollectionRequired, eventerrors.ErrInvalidArgument) {
		t.Error("ErrCollectionRequired should wrap eventerrors.ErrInvalidArgument")
	}
}

func TestStoreOptions_Defaults(t *testing.T) {
	var o storeOptions
	o.visibilityTimeout = defaultVisibilityTimeout

	if o.visibilityTimeout != 5*time.Minute {
		t.Errorf("expected default visibility timeout 5m, got %v", o.visibilityTimeout)
	}
	if o.ttl != 0 {
		t.Errorf("expected default TTL 0, got %v", o.ttl)
	}
}

func TestWithTTL(t *testing.T) {
	tests := []struct {
		name     string
		ttl      time.Duration
		expected time.Duration
	}{
		{"positive TTL", 24 * time.Hour, 24 * time.Hour},
		{"zero TTL ignored", 0, 0},
		{"negative TTL ignored", -1 * time.Hour, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := storeOptions{}
			WithTTL(tc.ttl)(&o)
			if o.ttl != tc.expected {
				t.Errorf("expected TTL %v, got %v", tc.expected, o.ttl)
			}
		})
	}
}

func TestWithVisibilityTimeout(t *testing.T) {
	tests := []struct {
		name     string
		timeout  time.Duration
		expected time.Duration
	}{
		{"positive timeout", 10 * time.Minute, 10 * time.Minute},
		{"zero timeout ignored", 0, 0},
		{"negative timeout ignored", -1 * time.Minute, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := storeOptions{}
			WithVisibilityTimeout(tc.timeout)(&o)
			if o.visibilityTimeout != tc.expected {
				t.Errorf("expected visibility timeout %v, got %v", tc.expected, o.visibilityTimeout)
			}
		})
	}
}

func TestStoreIndexes_WithoutTTL(t *testing.T) {
	s := &Store{
		ttl:               0,
		visibilityTimeout: defaultVisibilityTimeout,
	}

	indexes := s.Indexes()
	// Should have 3 base indexes (no TTL index)
	if len(indexes) != 3 {
		t.Errorf("expected 3 indexes without TTL, got %d", len(indexes))
	}
}

func TestStoreIndexes_WithTTL(t *testing.T) {
	s := &Store{
		ttl:               7 * 24 * time.Hour,
		visibilityTimeout: defaultVisibilityTimeout,
	}

	indexes := s.Indexes()
	// Should have 4 indexes (3 base + 1 TTL)
	if len(indexes) != 4 {
		t.Errorf("expected 4 indexes with TTL, got %d", len(indexes))
	}

	// Last index should have Options (TTL index)
	ttlIdx := indexes[3]
	if ttlIdx.Options == nil {
		t.Fatal("expected TTL index to have Options set")
	}
}

func TestStoreConstants(t *testing.T) {
	if statusPending != "pending" {
		t.Errorf("expected statusPending=pending, got %s", statusPending)
	}
	if statusInflight != "inflight" {
		t.Errorf("expected statusInflight=inflight, got %s", statusInflight)
	}
	if statusAcked != "acked" {
		t.Errorf("expected statusAcked=acked, got %s", statusAcked)
	}
	if defaultVisibilityTimeout != 5*time.Minute {
		t.Errorf("expected defaultVisibilityTimeout=5m, got %v", defaultVisibilityTimeout)
	}
}

func TestStoreSentinelErrors(t *testing.T) {
	if ErrMessageNotFound == nil {
		t.Error("ErrMessageNotFound should not be nil")
	}
	if ErrInvalidSequenceID == nil {
		t.Error("ErrInvalidSequenceID should not be nil")
	}
	if ErrCollectionRequired == nil {
		t.Error("ErrCollectionRequired should not be nil")
	}
}

