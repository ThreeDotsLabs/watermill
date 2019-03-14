package cqrs

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
)

type publisherStub struct {
	messages map[string]message.Messages

	mu sync.Mutex
}

func newPublisherStub() *publisherStub {
	return &publisherStub{
		messages: make(map[string]message.Messages),
	}
}

func (*publisherStub) Close() error {
	return nil
}

func (p *publisherStub) Publish(topic string, messages ...*message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.messages[topic] = append(p.messages[topic], messages...)

	return nil
}

func TestCommandBus_Send_ContextPropagation(t *testing.T) {
	publisher := newPublisherStub()

	commandBus := NewCommandBus(publisher, "whatever", JSONMarshaler{})

	ctx := context.WithValue(context.Background(), "key", "value")

	err := commandBus.Send(ctx, "message")
	require.NoError(t, err)

	assert.Equal(t, ctx, publisher.messages["whatever"][0].Context())
}
