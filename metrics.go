package mongodb

import (
	"context"
	"sync"
	"time"

	event "github.com/rbaliyan/event/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	meterName = "github.com/rbaliyan/event-mongodb"
)

// Metrics provides OpenTelemetry metrics for MongoDB change stream processing.
//
// All methods are nil-safe — calling any method on a nil *Metrics is a no-op.
// Use NewMetrics() to create an instance with the global meter provider,
// or pass WithMeterProvider() for a custom provider.
//
// Available metrics:
//   - mongodb_changes_processed_total: Counter of change events processed successfully
//   - mongodb_changes_failed_total: Counter of handler errors
//   - mongodb_oplog_lag_seconds: Histogram of delay from clusterTime to handler execution
//   - mongodb_handler_duration_seconds: Histogram of handler processing time
//   - mongodb_changes_pending: Gauge of pending unacked events (callback-based)
type Metrics struct {
	meter metric.Meter

	// Counters
	processedTotal metric.Int64Counter
	failedTotal    metric.Int64Counter

	// Histograms
	oplogLag        metric.Float64Histogram
	handlerDuration metric.Float64Histogram

	// Observable gauge
	pendingChanges metric.Int64ObservableGauge

	// Callback for observable gauge
	pendingCallback func() int64

	// Registration for cleanup
	registration metric.Registration
	mu           sync.RWMutex
}

// MetricsOption configures the Metrics instance.
type MetricsOption func(*metricsOptions)

type metricsOptions struct {
	meterProvider metric.MeterProvider
	namespace     string
}

// WithMeterProvider sets a custom meter provider for metrics.
// By default, uses the global OpenTelemetry meter provider.
func WithMeterProvider(provider metric.MeterProvider) MetricsOption {
	return func(o *metricsOptions) {
		if provider != nil {
			o.meterProvider = provider
		}
	}
}

// WithMetricsNamespace sets a namespace prefix for all metrics.
// This is useful for distinguishing metrics from different transport instances.
//
// Example:
//
//	metrics, _ := mongodb.NewMetrics(mongodb.WithMetricsNamespace("orders"))
//	// Metrics will be: orders_mongodb_changes_processed_total, etc.
func WithMetricsNamespace(namespace string) MetricsOption {
	return func(o *metricsOptions) {
		if namespace != "" {
			o.namespace = namespace + "_"
		}
	}
}

// NewMetrics creates a new Metrics instance for recording change stream metrics.
//
// By default, uses the global OpenTelemetry meter provider. Use WithMeterProvider
// to specify a custom provider.
//
// Example:
//
//	// Using global provider
//	metrics, err := mongodb.NewMetrics()
//
//	// Using custom provider
//	metrics, err := mongodb.NewMetrics(mongodb.WithMeterProvider(myProvider))
//
//	// With namespace
//	metrics, err := mongodb.NewMetrics(mongodb.WithMetricsNamespace("orders"))
func NewMetrics(opts ...MetricsOption) (*Metrics, error) {
	o := &metricsOptions{
		meterProvider: otel.GetMeterProvider(),
	}
	for _, opt := range opts {
		opt(o)
	}

	meter := o.meterProvider.Meter(meterName)
	prefix := o.namespace

	m := &Metrics{
		meter: meter,
	}

	var err error

	// Create counters
	m.processedTotal, err = meter.Int64Counter(
		prefix+"mongodb_changes_processed_total",
		metric.WithDescription("Total number of change events processed successfully"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	m.failedTotal, err = meter.Int64Counter(
		prefix+"mongodb_changes_failed_total",
		metric.WithDescription("Total number of change events that failed processing"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	// Create histograms
	m.oplogLag, err = meter.Float64Histogram(
		prefix+"mongodb_oplog_lag_seconds",
		metric.WithDescription("Delay between MongoDB clusterTime and handler execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
	)
	if err != nil {
		return nil, err
	}

	m.handlerDuration, err = meter.Float64Histogram(
		prefix+"mongodb_handler_duration_seconds",
		metric.WithDescription("Time spent processing a change event in the handler"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	// Create observable gauge
	m.pendingChanges, err = meter.Int64ObservableGauge(
		prefix+"mongodb_changes_pending",
		metric.WithDescription("Current number of pending unacknowledged change events"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	// Register callback for observable gauge
	m.registration, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			m.mu.RLock()
			defer m.mu.RUnlock()

			if m.pendingCallback != nil {
				o.ObserveInt64(m.pendingChanges, m.pendingCallback())
			}
			return nil
		},
		m.pendingChanges,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetPendingCallback sets the callback function for the pending changes gauge.
// The callback is called on each metrics collection to get the current count.
//
// Example:
//
//	metrics.SetPendingCallback(func() int64 {
//	    count, _ := db.Collection("_event_acks").CountDocuments(ctx,
//	        bson.M{"acked_at": bson.M{"$eq": time.Time{}}})
//	    return count
//	})
func (m *Metrics) SetPendingCallback(fn func() int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingCallback = fn
}

// Close unregisters the metrics callbacks.
// Call this when the transport is stopped to clean up resources.
func (m *Metrics) Close() error {
	if m == nil {
		return nil
	}
	if m.registration != nil {
		return m.registration.Unregister()
	}
	return nil
}

// MetricsMiddleware creates a subscriber middleware that records change stream
// processing metrics using the provided Metrics instance.
//
// The middleware:
//  1. Extracts cluster_time from event context metadata to compute oplog lag
//  2. Wraps the handler call with timing to record processing duration
//  3. Increments processed/failed counters based on handler return value
//  4. Uses event.ClassifyError() for proper error classification
//
// All metric recording is best-effort — failures don't affect the handler.
// If m is nil, the middleware is a no-op passthrough.
//
// Example:
//
//	metrics, _ := mongodb.NewMetrics()
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithMiddleware(mongodb.MetricsMiddleware[mongodb.ChangeEvent](metrics)),
//	)
func MetricsMiddleware[T any](m *Metrics) event.Middleware[T] {
	return func(next event.Handler[T]) event.Handler[T] {
		if m == nil {
			return next
		}
		return func(ctx context.Context, ev event.Event[T], data T) error {
			// Extract metadata for attributes
			md := event.ContextMetadata(ctx)
			eventName := event.ContextName(ctx)

			operation := ""
			namespace := ""
			if md != nil {
				operation = md[MetadataOperation]
				namespace = md[MetadataNamespace]
			}

			attrs := []attribute.KeyValue{
				attribute.String("event", eventName),
				attribute.String("operation", operation),
				attribute.String("namespace", namespace),
			}

			// Record oplog lag from cluster_time metadata
			if md != nil {
				if clusterTimeStr, ok := md[MetadataClusterTime]; ok && clusterTimeStr != "" {
					if clusterTime, err := time.Parse(time.RFC3339Nano, clusterTimeStr); err == nil {
						lag := time.Since(clusterTime).Seconds()
						if lag < 0 {
							lag = 0
						}
						m.oplogLag.Record(ctx, lag, metric.WithAttributes(
							attribute.String("event", eventName),
							attribute.String("namespace", namespace),
						))
					}
				}
			}

			// Execute handler with timing
			start := time.Now()
			handlerErr := next(ctx, ev, data)
			duration := time.Since(start).Seconds()

			// Record handler duration
			m.handlerDuration.Record(ctx, duration, metric.WithAttributes(attrs...))

			// Classify error and increment appropriate counter
			if handlerErr != nil {
				result := event.ClassifyError(handlerErr)
				switch result {
				case event.ResultAck:
					// Error wraps ErrAck — treat as success
					m.processedTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
				default:
					// ResultNack, ResultReject, ResultDefer — all count as failed
					m.failedTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
				}
			} else {
				m.processedTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			}

			return handlerErr
		}
	}
}
