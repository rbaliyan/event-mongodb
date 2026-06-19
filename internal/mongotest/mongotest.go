// Package mongotest provides shared helpers for the MongoDB integration tests
// across the event-mongodb module and its subpackages.
//
// This package is TEST-ONLY: it is imported exclusively from _test.go files. It
// is a normal (non-test) package under internal/ so it can be shared across
// package boundaries, but it has no purpose outside of tests. It depends only on
// the MongoDB driver already required by the module's go.mod; it pulls in no new
// external dependencies.
//
// It centralizes the three pieces of setup that were previously duplicated in
// every *_integration_test.go file:
//
//   - URI():            resolve the MongoDB URI from the environment.
//   - Connect(t):       connect + ping, skipping cleanly when unavailable.
//   - UniqueDBName(p):  a collision-resistant per-test database name.
package mongotest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// defaultURI is used when MONGO_URI is not set. directConnection avoids the
// driver doing replica-set discovery against a host it cannot reach.
const defaultURI = "mongodb://localhost:27018/?directConnection=true"

// connectTimeout bounds the connect+ping probe so an unreachable MongoDB skips
// the test promptly rather than hanging.
const connectTimeout = 10 * time.Second

// URI returns the MongoDB connection URI, honoring the MONGO_URI environment
// variable and falling back to a local direct-connection default.
func URI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return defaultURI
}

// Connect dials MongoDB and verifies connectivity with a Ping. It encapsulates
// the skip policy shared by every integration test:
//
//   - In -short mode the test is skipped before any network I/O.
//   - If MongoDB is unreachable (connect or ping fails) the test is skipped
//     (not failed) so the suite stays green in environments without MongoDB.
//
// The returned client is registered for disconnect via t.Cleanup, so callers do
// not need to disconnect it themselves. Callers remain responsible for dropping
// any databases they create (see UniqueDBName).
func Connect(t *testing.T) *mongo.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(URI()))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		t.Skipf("MongoDB not available: %v", err)
	}

	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

// UniqueDBName builds a collision-resistant database name from the given prefix,
// the process PID, and a nanosecond timestamp. Including both PID and
// nanoseconds avoids collisions between tests that start within the same instant
// and between concurrent test binaries.
//
// The nanosecond count is rendered as a plain integer: MongoDB database names
// may not contain '.', so a fractional-seconds timestamp layout cannot be used.
func UniqueDBName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
}
