package transaction

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	evttransaction "github.com/rbaliyan/event/v3/transaction"
)

func TestNewMongoManager_NilClient(t *testing.T) {
	mgr, err := NewMongoManager(nil)
	if err == nil {
		t.Fatal("expected error for nil client, got nil")
	}
	if mgr != nil {
		t.Fatalf("expected nil manager on error, got %v", mgr)
	}
}

func TestNewMongoManager_ValidClient(t *testing.T) {
	// mongo.Connect is lazy: it does not contact the server, so this is a
	// pure unit test. We never call any method requiring a live server.
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27018/?directConnection=true"))
	if err != nil {
		t.Fatalf("unexpected error connecting (lazy) client: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(t.Context()) })

	mgr, err := NewMongoManager(client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
	if mgr.client != client {
		t.Fatal("manager did not retain the provided client")
	}
}

// TestMongoTransaction_Interfaces asserts the concrete type satisfies the
// generic transaction interface and the MongoDB-specific provider interface.
func TestMongoTransaction_Interfaces(t *testing.T) {
	var _ evttransaction.Transaction = (*MongoTransaction)(nil)
	var _ MongoSessionProvider = (*MongoTransaction)(nil)
	var _ evttransaction.Manager = (*MongoManager)(nil)
}
