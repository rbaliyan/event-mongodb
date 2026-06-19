package mongodb

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
)

// discardLogger returns a slog.Logger that discards all output, keeping test
// runs quiet while still exercising logging code paths.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// buMustNewChannelTransport builds a Transport with a real internal channel
// transport wired up, but no MongoDB dependency. It is suitable for driving
// processChange/storeAndPublish where fan-out to subscribers must actually work.
func buMustNewChannelTransport(t *testing.T, opts transportOptions) *Transport {
	t.Helper()
	if opts.logger == nil {
		opts.logger = discardLogger()
	}
	if opts.bufferSize <= 0 {
		opts.bufferSize = 100
	}
	if opts.onError == nil {
		opts.onError = func(error) {}
	}
	tr := &Transport{transportOptions: opts, status: statusOpen}
	tr.channelTransport = channel.New(
		channel.WithBufferSize(uint(tr.bufferSize)),
		channel.WithLogger(tr.logger),
		channel.WithFallbackTimeout(5*time.Minute),
	)
	t.Cleanup(func() { _ = tr.channelTransport.Close(context.Background()) })
	return tr
}

// buCapture collects messages published to an event via the channel transport.
type buCapture struct {
	sub transport.Subscription
}

// buRegisterCapture registers an event on the transport and subscribes to it,
// returning a capture handle the test can poll for published messages.
func buRegisterCapture(t *testing.T, tr *Transport, eventName string) *buCapture {
	t.Helper()
	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, eventName)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close(context.Background()) })
	return &buCapture{sub: sub}
}

// wait blocks for the next published message, failing the test on timeout.
func (c *buCapture) wait(t *testing.T) transport.Message {
	t.Helper()
	select {
	case msg := <-c.sub.Messages():
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published message")
		return nil
	}
}

// count returns how many messages are currently buffered without blocking.
func (c *buCapture) count() int {
	n := 0
	for {
		select {
		case <-c.sub.Messages():
			n++
		case <-time.After(100 * time.Millisecond):
			return n
		}
	}
}
