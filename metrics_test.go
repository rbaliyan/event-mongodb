package mongodb

import (
	"context"
	"errors"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// testMetrics creates a Metrics instance backed by a ManualReader for deterministic testing.
func testMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m, err := NewMetrics(WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, reader
}

// collectMetrics reads all accumulated metrics from the ManualReader.
func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return rm
}

// findMetric searches for a metric by name across all scope metrics.
func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

// sumCounter returns the total value across all data points for an Int64 Sum metric.
func sumCounter(m *metricdata.Metrics) int64 {
	if m == nil {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

// histogramCount returns the total count across all data points for a Float64 Histogram metric.
func histogramCount(m *metricdata.Metrics) uint64 {
	if m == nil {
		return 0
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		return 0
	}
	var total uint64
	for _, dp := range h.DataPoints {
		total += dp.Count
	}
	return total
}

// hasAttribute checks if a data point's attributes contain a specific key-value pair.
func hasAttribute(attrs attribute.Set, key, value string) bool {
	v, ok := attrs.Value(attribute.Key(key))
	return ok && v.AsString() == value
}

// testContext creates a context with event metadata including cluster_time and operation.
func testContext(clusterTime time.Time, operation, namespace string) context.Context {
	md := map[string]string{
		MetadataClusterTime: clusterTime.Format(time.RFC3339Nano),
		MetadataOperation:   operation,
		MetadataNamespace:   namespace,
	}
	return event.ContextWithMetadata(context.Background(), md)
}

// stubEvent implements event.Event[T] for testing.
type stubEvent[T any] struct{}

func (stubEvent[T]) Name() string                         { return "test-event" }
func (stubEvent[T]) Publish(_ context.Context, _ T) error { return nil }
func (stubEvent[T]) Subscribe(_ context.Context, _ event.Handler[T], _ ...event.SubscribeOption[T]) error {
	return nil
}

func TestMetricsMiddleware_ProcessedCounter(t *testing.T) {
	m, reader := testMetrics(t)

	middleware := MetricsMiddleware[ChangeEvent](m)
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		return nil
	})

	ctx := testContext(time.Now().Add(-100*time.Millisecond), "insert", "db.orders")
	err := handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	rm := collectMetrics(t, reader)

	processed := findMetric(rm, "mongodb_changes_processed_total")
	if got := sumCounter(processed); got != 1 {
		t.Errorf("processed_total = %d, want 1", got)
	}

	failed := findMetric(rm, "mongodb_changes_failed_total")
	if got := sumCounter(failed); got != 0 {
		t.Errorf("failed_total = %d, want 0", got)
	}
}

func TestMetricsMiddleware_FailedCounter(t *testing.T) {
	m, reader := testMetrics(t)

	middleware := MetricsMiddleware[ChangeEvent](m)
	handlerErr := errors.New("processing failed")
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		return handlerErr
	})

	ctx := testContext(time.Now(), "update", "db.orders")
	err := handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})
	if err != handlerErr {
		t.Fatalf("handler error = %v, want %v", err, handlerErr)
	}

	rm := collectMetrics(t, reader)

	failed := findMetric(rm, "mongodb_changes_failed_total")
	if got := sumCounter(failed); got != 1 {
		t.Errorf("failed_total = %d, want 1", got)
	}

	processed := findMetric(rm, "mongodb_changes_processed_total")
	if got := sumCounter(processed); got != 0 {
		t.Errorf("processed_total = %d, want 0", got)
	}
}

func TestMetricsMiddleware_OplogLag(t *testing.T) {
	m, reader := testMetrics(t)

	middleware := MetricsMiddleware[ChangeEvent](m)
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		return nil
	})

	// Set cluster_time to 2 seconds ago
	clusterTime := time.Now().Add(-2 * time.Second)
	ctx := testContext(clusterTime, "insert", "db.orders")
	_ = handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})

	rm := collectMetrics(t, reader)

	lag := findMetric(rm, "mongodb_oplog_lag_seconds")
	if lag == nil {
		t.Fatal("oplog_lag_seconds metric not found")
	}
	if got := histogramCount(lag); got != 1 {
		t.Errorf("oplog_lag count = %d, want 1", got)
	}

	// Verify the recorded lag is at least 2 seconds
	h, ok := lag.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("oplog_lag is not a histogram")
	}
	if len(h.DataPoints) == 0 {
		t.Fatal("no data points in oplog_lag histogram")
	}
	if h.DataPoints[0].Sum < 2.0 {
		t.Errorf("oplog_lag sum = %f, want >= 2.0", h.DataPoints[0].Sum)
	}
}

func TestMetricsMiddleware_HandlerDuration(t *testing.T) {
	m, reader := testMetrics(t)

	middleware := MetricsMiddleware[ChangeEvent](m)
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	ctx := testContext(time.Now(), "insert", "db.orders")
	_ = handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})

	rm := collectMetrics(t, reader)

	duration := findMetric(rm, "mongodb_handler_duration_seconds")
	if duration == nil {
		t.Fatal("handler_duration_seconds metric not found")
	}
	if got := histogramCount(duration); got != 1 {
		t.Errorf("handler_duration count = %d, want 1", got)
	}

	h, ok := duration.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("handler_duration is not a histogram")
	}
	if len(h.DataPoints) == 0 {
		t.Fatal("no data points in handler_duration histogram")
	}
	if h.DataPoints[0].Sum < 0.01 {
		t.Errorf("handler_duration sum = %f, want >= 0.01", h.DataPoints[0].Sum)
	}
}

func TestMetricsMiddleware_ErrorClassification(t *testing.T) {
	m, reader := testMetrics(t)

	tests := []struct {
		name          string
		err           error
		wantProcessed int64
		wantFailed    int64
	}{
		{
			name:          "nil error counts as processed",
			err:           nil,
			wantProcessed: 1,
			wantFailed:    0,
		},
		{
			name:          "ErrAck counts as processed",
			err:           event.ErrAck,
			wantProcessed: 1,
			wantFailed:    0,
		},
		{
			name:          "ErrNack counts as failed",
			err:           event.ErrNack,
			wantProcessed: 0,
			wantFailed:    1,
		},
		{
			name:          "ErrReject counts as failed",
			err:           event.ErrReject,
			wantProcessed: 0,
			wantFailed:    1,
		},
		{
			name:          "ErrDefer counts as failed",
			err:           event.ErrDefer,
			wantProcessed: 0,
			wantFailed:    1,
		},
		{
			name:          "unknown error counts as failed",
			err:           errors.New("unknown"),
			wantProcessed: 0,
			wantFailed:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fresh metrics for each subtest
			subReader := sdkmetric.NewManualReader()
			subProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(subReader))
			t.Cleanup(func() { _ = subProvider.Shutdown(context.Background()) })

			subM, err := NewMetrics(WithMeterProvider(subProvider))
			if err != nil {
				t.Fatalf("NewMetrics: %v", err)
			}
			t.Cleanup(func() { _ = subM.Close() })

			middleware := MetricsMiddleware[ChangeEvent](subM)
			handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
				return tt.err
			})

			ctx := testContext(time.Now(), "insert", "db.orders")
			_ = handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})

			rm := collectMetrics(t, subReader)

			processed := findMetric(rm, "mongodb_changes_processed_total")
			if got := sumCounter(processed); got != tt.wantProcessed {
				t.Errorf("processed_total = %d, want %d", got, tt.wantProcessed)
			}

			failed := findMetric(rm, "mongodb_changes_failed_total")
			if got := sumCounter(failed); got != tt.wantFailed {
				t.Errorf("failed_total = %d, want %d", got, tt.wantFailed)
			}
		})
	}

	// Suppress unused variable warning
	_ = m
	_ = reader
}

func TestMetricsMiddleware_NilMetrics(t *testing.T) {
	middleware := MetricsMiddleware[ChangeEvent](nil)

	called := false
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		called = true
		return nil
	})

	ctx := testContext(time.Now(), "insert", "db.orders")
	err := handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestMetricsMiddleware_NoMetadata(t *testing.T) {
	m, reader := testMetrics(t)

	middleware := MetricsMiddleware[ChangeEvent](m)
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		return nil
	})

	// Use plain context without metadata
	err := handler(context.Background(), stubEvent[ChangeEvent]{}, ChangeEvent{})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	rm := collectMetrics(t, reader)

	// Should still record processed counter (with empty attributes)
	processed := findMetric(rm, "mongodb_changes_processed_total")
	if got := sumCounter(processed); got != 1 {
		t.Errorf("processed_total = %d, want 1", got)
	}

	// Should not record oplog lag (no cluster_time in metadata)
	lag := findMetric(rm, "mongodb_oplog_lag_seconds")
	if lag != nil && histogramCount(lag) != 0 {
		t.Error("oplog_lag should not be recorded without cluster_time metadata")
	}
}

func TestMetrics_SetPendingCallback(t *testing.T) {
	m, reader := testMetrics(t)

	m.SetPendingCallback(func() int64 {
		return 42
	})

	rm := collectMetrics(t, reader)

	pending := findMetric(rm, "mongodb_changes_pending")
	if pending == nil {
		t.Fatal("mongodb_changes_pending metric not found")
	}

	gauge, ok := pending.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatal("pending is not a gauge")
	}
	if len(gauge.DataPoints) == 0 {
		t.Fatal("no data points in pending gauge")
	}
	if gauge.DataPoints[0].Value != 42 {
		t.Errorf("pending value = %d, want 42", gauge.DataPoints[0].Value)
	}
}

func TestMetrics_SetPendingCallback_Nil(t *testing.T) {
	var m *Metrics
	// Should not panic
	m.SetPendingCallback(func() int64 { return 1 })
}

func TestMetrics_Close_Nil(t *testing.T) {
	var m *Metrics
	// Should not panic and return nil
	if err := m.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func TestMetricsMiddleware_Attributes(t *testing.T) {
	m, reader := testMetrics(t)

	middleware := MetricsMiddleware[ChangeEvent](m)
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		return nil
	})

	ctx := testContext(time.Now(), "update", "mydb.users")
	_ = handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})

	rm := collectMetrics(t, reader)

	processed := findMetric(rm, "mongodb_changes_processed_total")
	if processed == nil {
		t.Fatal("processed_total metric not found")
	}

	sum, ok := processed.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatal("processed_total is not a sum")
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points")
	}

	dp := sum.DataPoints[0]
	if !hasAttribute(dp.Attributes, "operation", "update") {
		t.Error("missing or incorrect operation attribute")
	}
	if !hasAttribute(dp.Attributes, "namespace", "mydb.users") {
		t.Error("missing or incorrect namespace attribute")
	}
}

func TestNewMetrics_WithNamespace(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m, err := NewMetrics(WithMeterProvider(provider), WithMetricsNamespace("orders"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	middleware := MetricsMiddleware[ChangeEvent](m)
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		return nil
	})

	ctx := testContext(time.Now(), "insert", "db.orders")
	_ = handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})

	rm := collectMetrics(t, reader)

	// Metrics should be prefixed with "orders_"
	processed := findMetric(rm, "orders_mongodb_changes_processed_total")
	if processed == nil {
		t.Fatal("namespaced processed_total metric not found")
	}
	if got := sumCounter(processed); got != 1 {
		t.Errorf("processed_total = %d, want 1", got)
	}

	// Non-prefixed should not exist
	nonPrefixed := findMetric(rm, "mongodb_changes_processed_total")
	if nonPrefixed != nil {
		t.Error("non-prefixed metric should not exist when namespace is set")
	}
}

func TestMetricsMiddleware_PreservesHandlerError(t *testing.T) {
	m, _ := testMetrics(t)

	expectedErr := errors.New("handler failure")
	middleware := MetricsMiddleware[ChangeEvent](m)
	handler := middleware(func(ctx context.Context, ev event.Event[ChangeEvent], data ChangeEvent) error {
		return expectedErr
	})

	ctx := testContext(time.Now(), "insert", "db.orders")
	err := handler(ctx, stubEvent[ChangeEvent]{}, ChangeEvent{})
	if err != expectedErr {
		t.Errorf("handler error = %v, want %v", err, expectedErr)
	}
}
