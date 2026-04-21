package transaction

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"

	evttransaction "github.com/rbaliyan/event/v3/transaction"
)

// MongoTransaction wraps a MongoDB session to implement Transaction.
//
// This type provides MongoDB transaction support with automatic session
// management. It implements both Transaction and MongoSessionProvider
// interfaces, allowing both generic transaction handling and MongoDB-specific
// operations.
//
// MongoDB transactions require a replica set or sharded cluster.
// Standalone MongoDB deployments do not support transactions.
type MongoTransaction struct {
	session   *mongo.Session
	ctx       context.Context // In v2, session is embedded in context
	closeOnce sync.Once
}

// Commit commits the MongoDB transaction and ends the session.
func (t *MongoTransaction) Commit() error {
	err := t.session.CommitTransaction(t.ctx)
	t.endSession()
	return err
}

// Rollback rolls back the MongoDB transaction and ends the session.
func (t *MongoTransaction) Rollback() error {
	err := t.session.AbortTransaction(t.ctx)
	t.endSession()
	return err
}

func (t *MongoTransaction) endSession() {
	t.closeOnce.Do(func() {
		t.session.EndSession(t.ctx)
	})
}

// Session returns the MongoDB session.
func (t *MongoTransaction) Session() *mongo.Session {
	return t.session
}

// Context returns the context with the session embedded.
// Use this context for all MongoDB operations within the transaction.
func (t *MongoTransaction) Context() context.Context {
	return t.ctx
}

// MongoSessionProvider is implemented by transactions that provide MongoDB session access.
// Use type assertion to access MongoDB-specific functionality from a generic Transaction.
type MongoSessionProvider interface {
	evttransaction.Transaction

	// Session returns the MongoDB session.
	Session() *mongo.Session

	// Context returns the transaction context.
	Context() context.Context
}

// MongoManager implements Manager for MongoDB.
type MongoManager struct {
	client *mongo.Client
}

// NewMongoManager creates a new MongoDB transaction manager.
func NewMongoManager(client *mongo.Client) (*MongoManager, error) {
	if client == nil {
		return nil, errors.New("mongodb: client is required")
	}
	return &MongoManager{client: client}, nil
}

// Begin starts a new MongoDB transaction.
func (m *MongoManager) Begin(ctx context.Context) (evttransaction.Transaction, error) {
	session, err := m.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	return &MongoTransaction{
		session: session,
		ctx:     ctx,
	}, nil
}

// Execute runs a function within a MongoDB transaction.
func (m *MongoManager) Execute(ctx context.Context, fn func(tx evttransaction.Transaction) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		tx := &MongoTransaction{
			session: session,
			ctx:     ctx,
		}
		if err := fn(tx); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// ExecuteWithContext runs a function within a MongoDB transaction with direct context access.
func (m *MongoManager) ExecuteWithContext(ctx context.Context, fn func(ctx context.Context) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		if err := fn(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// MongoTxHandler is a function type that handles MongoDB transaction context.
type MongoTxHandler func(ctx context.Context) error

// WithTransaction executes a function within a MongoDB transaction.
func WithTransaction(ctx context.Context, client *mongo.Client, fn MongoTxHandler) error {
	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		if err := fn(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// Compile-time checks
var _ evttransaction.Manager = (*MongoManager)(nil)
var _ evttransaction.Transaction = (*MongoTransaction)(nil)
var _ MongoSessionProvider = (*MongoTransaction)(nil)
