// Package monitor provides a MongoDB-backed implementation of the monitor store
// for per-event lifecycle tracking.
//
// MongoStore records the status of each event delivery attempt (pending,
// completed, failed, retrying) and exposes query methods for operational
// dashboards. It is designed to be used with the monitor middleware from
// github.com/rbaliyan/event/v3/monitor.
//
// MongoStore implements several interfaces consumed by the monitor HTTP
// handler:
//
//   - evtmonitor.Store — core per-event lifecycle storage.
//   - evtmonitor.SummaryProvider — aggregate status counts and per-event
//     statistics via Summary, computed server-side with a $facet pipeline.
//   - evtmonitor.StuckPendingProvider — StuckPendingCount and
//     StuckPendingEntries for detecting events stuck in the pending state.
//   - event.PublishAuditStore — records publish attempts for audit queries.
//
// Use MongoStore together with the monitor HTTP handler to expose a REST API
// for inspecting event lifecycle state in production:
//
//	monitorStore, _ := monitor.NewMongoStore(db)
//	monitorStore.EnsureIndexes(ctx)
//
//	orderChanges.Subscribe(ctx, handler,
//	    event.WithMiddleware(evtmonitor.Middleware[mongodb.ChangeEvent](monitorStore)),
//	)
//
// # Retention
//
// By default entries are retained until deleted explicitly (or via
// DeleteOlderThan). Use WithTTL to add a server-side TTL index so MongoDB
// evicts entries older than the given duration automatically — preferred over
// the application-driven sweep for high-throughput collections:
//
//	monitorStore, _ := monitor.NewMongoStore(db)
//	monitorStore.WithTTL(24 * time.Hour)
//	monitorStore.EnsureIndexes(ctx) // adds the TTL index
package monitor
