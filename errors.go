// Package mongodb provides MongoDB Change Stream transport for the event library.
//
// # Error Handling
//
// This package defines sentinel errors for common failure conditions.
// Use errors.Is() to check for specific error types:
//
//	if errors.Is(err, mongodb.ErrPublishNotSupported) {
//	    // Handle publish not supported
//	}
//
//	if errors.Is(err, mongodb.ErrClientRequired) {
//	    // Handle missing client
//	}
package mongodb

import (
	"errors"
	"fmt"

	eventerrors "github.com/rbaliyan/event/v3/errors"
)

// ErrCollectionNil is returned when a nil collection is passed to a store constructor.
var ErrCollectionNil = fmt.Errorf("mongodb: collection must not be nil: %w", eventerrors.ErrInvalidArgument)

// Sentinel errors for the MongoDB transport.
var (
	// ErrClientRequired is returned by NewClusterWatch when client is nil.
	// This wraps ErrInvalidArgument from the shared errors package.
	ErrClientRequired = fmt.Errorf("mongodb client is required: %w", eventerrors.ErrInvalidArgument)

	// ErrDatabaseRequired is returned by New when database is nil.
	// This wraps ErrInvalidArgument from the shared errors package.
	ErrDatabaseRequired = fmt.Errorf("mongodb database is required: %w", eventerrors.ErrInvalidArgument)

	// ErrPublishNotSupported is returned by Publish on every call. The MongoDB
	// change stream transport is subscribe-only: events are produced by writing
	// to the database, not by calling Publish. Code that uses the generic
	// transport.Transport interface should check for this error with
	// errors.Is(err, mongodb.ErrPublishNotSupported) and handle it accordingly.
	ErrPublishNotSupported = errors.New("mongodb transport does not support Publish; changes are triggered by database writes")

	// ErrMaxUpdatedFieldsSizeRequiresFull is returned by New when
	// WithMaxUpdatedFieldsSize is used without WithFullDocument.
	// This wraps ErrInvalidArgument from the shared errors package.
	ErrMaxUpdatedFieldsSizeRequiresFull = fmt.Errorf("WithMaxUpdatedFieldsSize requires WithFullDocument: %w", eventerrors.ErrInvalidArgument)

	// ErrFullDocumentRequired is returned by New when WithFullDocumentOnly
	// is used without WithFullDocument.
	// This wraps ErrInvalidArgument from the shared errors package.
	ErrFullDocumentRequired = fmt.Errorf("WithFullDocumentOnly requires WithFullDocument: %w", eventerrors.ErrInvalidArgument)
)

