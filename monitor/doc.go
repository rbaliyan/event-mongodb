// Package monitor provides a MongoDB-backed implementation of the monitor store
// for per-event lifecycle tracking.
//
// MongoStore records the status of each event delivery attempt (pending,
// completed, failed, retrying) and exposes query methods for operational
// dashboards. It implements the evtmonitor.Store interface and is designed
// to be used with the monitor middleware from
// github.com/rbaliyan/event/v3/monitor.
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
package monitor
