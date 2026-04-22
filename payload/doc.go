// Package payload provides a BSON implementation of the event payload codec.
//
// The BSON codec encodes and decodes event payloads using MongoDB's BSON format.
// It is ideal for MongoDB change stream events where the payload is already BSON
// and may contain MongoDB-specific types such as ObjectID.
//
// The codec is registered in the global payload registry on package import.
// Wire it to an event type via event.WithPayloadCodec:
//
//	orderEvent := event.New[Order]("order.created", event.WithPayloadCodec(payload.BSON{}))
package payload
