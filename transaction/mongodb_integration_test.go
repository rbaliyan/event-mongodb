package transaction

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	evttransaction "github.com/rbaliyan/event/v3/transaction"

	"github.com/rbaliyan/event-mongodb/internal/mongotest"
)

// setupTxIntegrationTest connects to MongoDB and returns a client plus a
// per-test collection over a unique database. Tests are skipped when MongoDB
// is unavailable or when -short is set. Teardown is registered via t.Cleanup so
// it runs even if a test panics.
func setupTxIntegrationTest(t *testing.T) (*mongo.Client, *mongo.Collection, func()) {
	t.Helper()

	client := mongotest.Connect(t)

	db := client.Database(mongotest.UniqueDBName("test_event_mongodb_tx"))
	coll := db.Collection("docs")

	cleanup := func() { _ = db.Drop(context.Background()) }
	t.Cleanup(cleanup)

	return client, coll, cleanup
}

// isUnsupportedTxError reports whether err indicates the deployment does not
// support multi-document transactions (e.g. a standalone, non-replica-set
// MongoDB). Such environments cannot exercise these tests.
func isUnsupportedTxError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "replica set") ||
		strings.Contains(msg, "transaction numbers") ||
		strings.Contains(msg, "transactions are not supported") ||
		strings.Contains(msg, "this mongodb deployment does not support")
}

type txDoc struct {
	ID    string `bson:"_id"`
	Value string `bson:"value"`
}

func countByID(t *testing.T, coll *mongo.Collection, id string) int64 {
	t.Helper()
	n, err := coll.CountDocuments(context.Background(), bson.M{"_id": id})
	if err != nil {
		t.Fatalf("count documents: %v", err)
	}
	return n
}

// TestIntegration_Transaction_RollbackOmitsDocument is the key test requested
// by the design review: a document inserted via tx.Context() must NOT survive a
// Rollback. This proves the session is correctly bound into the context by
// Begin (mongo.NewSessionContext), so the write joins the transaction instead
// of auto-committing.
func TestIntegration_Transaction_RollbackOmitsDocument(t *testing.T) {
	client, coll, _ := setupTxIntegrationTest(t)

	mgr, err := NewMongoManager(client)
	if err != nil {
		t.Fatalf("NewMongoManager: %v", err)
	}

	ctx := context.Background()
	tx, err := mgr.Begin(ctx)
	if err != nil {
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("Begin: %v", err)
	}

	mtx, ok := tx.(MongoSessionProvider)
	if !ok {
		t.Fatal("transaction does not implement MongoSessionProvider")
	}

	const id = "rollback-doc"
	// IMPORTANT: write using the transaction context, not the bare ctx.
	if _, err := coll.InsertOne(mtx.Context(), txDoc{ID: id, Value: "v"}); err != nil {
		_ = tx.Rollback()
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("InsertOne in tx: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("Rollback: %v", err)
	}

	if n := countByID(t, coll, id); n != 0 {
		t.Fatalf("document present after rollback: count=%d, want 0 "+
			"(session-context binding likely broken)", n)
	}
}

// TestIntegration_Transaction_CommitPersistsDocument is the commit counterpart:
// a document inserted via tx.Context() must survive a Commit.
func TestIntegration_Transaction_CommitPersistsDocument(t *testing.T) {
	client, coll, _ := setupTxIntegrationTest(t)

	mgr, err := NewMongoManager(client)
	if err != nil {
		t.Fatalf("NewMongoManager: %v", err)
	}

	ctx := context.Background()
	tx, err := mgr.Begin(ctx)
	if err != nil {
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("Begin: %v", err)
	}

	mtx := tx.(MongoSessionProvider)

	const id = "commit-doc"
	if _, err := coll.InsertOne(mtx.Context(), txDoc{ID: id, Value: "v"}); err != nil {
		_ = tx.Rollback()
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("InsertOne in tx: %v", err)
	}

	if err := tx.Commit(); err != nil {
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("Commit: %v", err)
	}

	if n := countByID(t, coll, id); n != 1 {
		t.Fatalf("document absent after commit: count=%d, want 1", n)
	}
}

// TestIntegration_Manager_Execute_Commit verifies a successful fn commits its writes.
func TestIntegration_Manager_Execute_Commit(t *testing.T) {
	client, coll, _ := setupTxIntegrationTest(t)

	mgr, err := NewMongoManager(client)
	if err != nil {
		t.Fatalf("NewMongoManager: %v", err)
	}

	const id = "execute-commit"
	err = mgr.Execute(context.Background(), func(tx evttransaction.Transaction) error {
		mtx := tx.(MongoSessionProvider)
		_, err := coll.InsertOne(mtx.Context(), txDoc{ID: id, Value: "v"})
		return err
	})
	if err != nil {
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("Execute: %v", err)
	}

	if n := countByID(t, coll, id); n != 1 {
		t.Fatalf("document absent after Execute commit: count=%d, want 1", n)
	}
}

// TestIntegration_Manager_Execute_Rollback verifies an fn returning an error
// rolls back its writes.
func TestIntegration_Manager_Execute_Rollback(t *testing.T) {
	client, coll, _ := setupTxIntegrationTest(t)

	mgr, err := NewMongoManager(client)
	if err != nil {
		t.Fatalf("NewMongoManager: %v", err)
	}

	const id = "execute-rollback"
	sentinel := errors.New("boom")
	err = mgr.Execute(context.Background(), func(tx evttransaction.Transaction) error {
		mtx := tx.(MongoSessionProvider)
		if _, err := coll.InsertOne(mtx.Context(), txDoc{ID: id, Value: "v"}); err != nil {
			return err
		}
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error from Execute, got nil")
	}
	if isUnsupportedTxError(err) {
		t.Skipf("transactions unsupported by deployment: %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("Execute error = %v, want wrapping %v", err, sentinel)
	}

	if n := countByID(t, coll, id); n != 0 {
		t.Fatalf("document present after Execute rollback: count=%d, want 0", n)
	}
}

// TestIntegration_WithTransaction_Commit verifies the WithTransaction helper
// commits on success.
func TestIntegration_WithTransaction_Commit(t *testing.T) {
	client, coll, _ := setupTxIntegrationTest(t)

	const id = "withtx-commit"
	err := WithTransaction(context.Background(), client, func(ctx context.Context) error {
		_, err := coll.InsertOne(ctx, txDoc{ID: id, Value: "v"})
		return err
	})
	if err != nil {
		if isUnsupportedTxError(err) {
			t.Skipf("transactions unsupported by deployment: %v", err)
		}
		t.Fatalf("WithTransaction: %v", err)
	}

	if n := countByID(t, coll, id); n != 1 {
		t.Fatalf("document absent after WithTransaction commit: count=%d, want 1", n)
	}
}

// TestIntegration_WithTransaction_Rollback verifies the WithTransaction helper
// rolls back when fn returns an error.
func TestIntegration_WithTransaction_Rollback(t *testing.T) {
	client, coll, _ := setupTxIntegrationTest(t)

	const id = "withtx-rollback"
	sentinel := errors.New("boom")
	err := WithTransaction(context.Background(), client, func(ctx context.Context) error {
		if _, err := coll.InsertOne(ctx, txDoc{ID: id, Value: "v"}); err != nil {
			return err
		}
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error from WithTransaction, got nil")
	}
	if isUnsupportedTxError(err) {
		t.Skipf("transactions unsupported by deployment: %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTransaction error = %v, want wrapping %v", err, sentinel)
	}

	if n := countByID(t, coll, id); n != 0 {
		t.Fatalf("document present after WithTransaction rollback: count=%d, want 0", n)
	}
}
