package bus_test

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventBus(t *testing.T) {
	pub := newPublisherStub()
	generateTopic := func(commandName string) string {
		return ""
	}
	marshaler := cqrs.JSONMarshaler{}

	cb, err := cqrs.NewEventBus(pub, generateTopic, marshaler)
	assert.NotNil(t, cb)
	assert.NoError(t, err)

	cb, err = cqrs.NewEventBus(nil, generateTopic, marshaler)
	assert.Nil(t, cb)
	assert.Error(t, err)

	cb, err = cqrs.NewEventBus(pub, nil, marshaler)
	assert.Nil(t, cb)
	assert.Error(t, err)

	cb, err = cqrs.NewEventBus(pub, generateTopic, nil)
	assert.Nil(t, cb)
	assert.Error(t, err)
}

func TestEventBus_Send_ContextPropagation(t *testing.T) {
	publisher := newPublisherStub()

	eventBus, err := cqrs.NewEventBus(
		publisher,
		func(eventName string) string {
			return "whatever"
		},
		cqrs.JSONMarshaler{},
	)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), "key", "value")

	err = eventBus.Publish(ctx, "message")
	require.NoError(t, err)

	assert.Equal(t, ctx, publisher.messages["whatever"][0].Context())
}

func TestEventBus_Send_topic_name(t *testing.T) {
	cb, err := cqrs.NewEventBus(
		assertPublishTopicPublisher{ExpectedTopic: "cqrs_test.TestEvent", T: t},
		func(commandName string) string {
			return commandName
		},
		cqrs.JSONMarshaler{},
	)
	require.NoError(t, err)

	err = cb.Publish(context.Background(), TestEvent{})
	require.NoError(t, err)
}

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

type assertPublishTopicPublisher struct {
	ExpectedTopic string
	T             *testing.T
}

func (a assertPublishTopicPublisher) Publish(topic string, messages ...*message.Message) error {
	assert.Equal(a.T, a.ExpectedTopic, topic)
	return nil
}

func (assertPublishTopicPublisher) Close() error {
	return nil
}

type TestEvent struct {
	ID   string
	When time.Time
}
