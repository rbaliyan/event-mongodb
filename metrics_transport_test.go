package mongodb

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetrics_StreamActive(t *testing.T) {
	m, reader := testMetrics(t)
	ctx := context.Background()

	m.RecordStreamOpened(ctx, "db.orders")
	m.RecordStreamOpened(ctx, "db.orders")
	m.RecordStreamClosed(ctx, "db.orders")

	rm := collectMetrics(t, reader)
	active := findMetric(rm, "mongodb_stream_active")
	if active == nil {
		t.Fatal("mongodb_stream_active not found")
	}
	// Net = +1 (two opened, one closed)
	if got := sumCounter(active); got != 1 {
		t.Errorf("stream_active = %d, want 1", got)
	}
	// Verify namespace attribute is present
	sum, ok := active.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatal("stream_active is not a sum")
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points for stream_active")
	}
	if !hasAttribute(sum.DataPoints[0].Attributes, "namespace", "db.orders") {
		t.Error("missing namespace attribute on stream_active")
	}
}

func TestMetrics_Reconnection(t *testing.T) {
	m, reader := testMetrics(t)
	ctx := context.Background()

	m.RecordReconnection(ctx, "db.orders", "error")
	m.RecordReconnection(ctx, "db.orders", "history_lost")
	m.RecordReconnection(ctx, "db.orders", "error")

	rm := collectMetrics(t, reader)
	reconn := findMetric(rm, "mongodb_stream_reconnections_total")
	if reconn == nil {
		t.Fatal("mongodb_stream_reconnections_total not found")
	}
	if got := sumCounter(reconn); got != 3 {
		t.Errorf("reconnections total = %d, want 3", got)
	}

	sum, ok := reconn.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatal("reconnections is not a sum")
	}

	var errorCount, historyLostCount int64
	for _, dp := range sum.DataPoints {
		switch {
		case hasAttribute(dp.Attributes, "reason", "error"):
			errorCount += dp.Value
		case hasAttribute(dp.Attributes, "reason", "history_lost"):
			historyLostCount += dp.Value
		}
		if !hasAttribute(dp.Attributes, "namespace", "db.orders") {
			t.Error("missing namespace attribute on reconnection datapoint")
		}
	}
	if errorCount != 2 {
		t.Errorf("reason=error count = %d, want 2", errorCount)
	}
	if historyLostCount != 1 {
		t.Errorf("reason=history_lost count = %d, want 1", historyLostCount)
	}
}

func TestMetrics_ReceiveLag(t *testing.T) {
	m, reader := testMetrics(t)
	ctx := context.Background()

	m.RecordReceiveLag(ctx, 1.5, "db.orders")
	m.RecordReceiveLag(ctx, 0.5, "db.orders")
	// Negative is clamped to 0.
	m.RecordReceiveLag(ctx, -10, "db.orders")

	rm := collectMetrics(t, reader)
	lag := findMetric(rm, "mongodb_stream_receive_lag_seconds")
	if lag == nil {
		t.Fatal("mongodb_stream_receive_lag_seconds not found")
	}
	if got := histogramCount(lag); got != 3 {
		t.Errorf("receive_lag count = %d, want 3", got)
	}
	h, ok := lag.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("receive_lag is not a histogram")
	}
	if len(h.DataPoints) == 0 {
		t.Fatal("no data points for receive_lag")
	}
	// Sum should be 1.5 + 0.5 + 0 (clamped) = 2.0
	if h.DataPoints[0].Sum < 1.99 || h.DataPoints[0].Sum > 2.01 {
		t.Errorf("receive_lag sum = %f, want ~2.0", h.DataPoints[0].Sum)
	}
	if !hasAttribute(h.DataPoints[0].Attributes, "namespace", "db.orders") {
		t.Error("missing namespace attribute on receive_lag")
	}
}

func TestMetrics_TransportLevel_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	ctx := context.Background()
	// None of these should panic on a nil receiver.
	m.RecordStreamOpened(ctx, "ns")
	m.RecordStreamClosed(ctx, "ns")
	m.RecordReconnection(ctx, "ns", "error")
	m.RecordReceiveLag(ctx, 1.0, "ns")
}
