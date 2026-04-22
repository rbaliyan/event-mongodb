// Package codec provides a BSON implementation of the transport Codec interface.
//
// The BSON codec serializes and deserializes event messages using MongoDB's
// BSON wire format. It is the preferred codec when messages are stored in or
// originate from MongoDB, as it preserves BSON-native types such as ObjectID
// and Decimal128 without an extra JSON round-trip.
package codec
