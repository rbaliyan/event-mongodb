// Package codec provides a BSON implementation of the transport Codec interface.
//
// The BSON codec serializes and deserializes event messages using MongoDB's
// BSON wire format. It is the preferred codec when messages are stored in or
// originate from MongoDB, as it preserves BSON-native types such as ObjectID
// and Decimal128 without an extra JSON round-trip.
//
// # Type
//
// BSON is a zero-value struct implementing evtcodec.Codec:
//
//   - Encode(msg) marshals a transport Message envelope to BSON bytes.
//   - Decode(data) reconstructs a Message from BSON bytes; decode failures wrap
//     evtcodec.ErrDecodeFailure (check with errors.Is).
//   - ContentType returns "application/bson".
//   - Name returns "bson".
//
// # Usage
//
// Pass a BSON value wherever the transport accepts a codec:
//
//	import mongocodec "github.com/rbaliyan/event-mongodb/codec"
//
//	t, _ := someTransport.New(..., transport.WithCodec(mongocodec.BSON{}))
//
// Unlike the payload package, this package does not self-register in a global
// registry; pass a BSON value explicitly where a codec is required.
package codec
