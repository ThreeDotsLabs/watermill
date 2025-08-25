package message

import (
	"context"
)

// Publisher is the emitting part of a Pub/Sub.
type Publisher interface {
	// Publish publishes provided messages to the given topic.
	//
	// Publish can be synchronous or asynchronous - it depends on the implementation.
	//
	// Most publisher implementations don't support atomic publishing of messages.
	// This means that if publishing one of the messages fails, the next messages will not be published.
	//
	// Publish does not work with a single Context.
	// Use the Context() method of each message instead.
	//
	// Publish must be thread safe.
	Publish(topic string, messages ...*Message) error
	// Close should flush unsent messages if publisher is async.
	Close() error
}

// Subscriber is the consuming part of the Pub/Sub.
type Subscriber interface {
	// Subscribe returns an output channel with messages from the provided topic.
	// The channel is closed after Close() is called on the subscriber.
	//
	// To receive the next message, `Ack()` must be called on the received message.
	// If message processing fails and the message should be redelivered `Nack()` should be called instead.
	//
	// When the provided ctx is canceled, the subscriber closes the subscription and the output channel.
	// The provided ctx is passed to all produced messages.
	// When Nack or Ack is called on the message, the context of the message is canceled.
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
	// Close closes all subscriptions with their output channels and flushes offsets etc. when needed.
	Close() error
}

// SubscribeInitializer is used to initialize subscribers.
type SubscribeInitializer interface {
	// SubscribeInitialize can be called to initialize subscribe before consume.
	// When calling Subscribe before Publish, SubscribeInitialize should be not required.
	//
	// Not every Pub/Sub requires this initialization, and it may be optional for performance improvements etc.
	// For detailed SubscribeInitialize functionality, please check Pub/Subs godoc.
	//
	// Implementing SubscribeInitialize is not obligatory.
	SubscribeInitialize(topic string) error
}
