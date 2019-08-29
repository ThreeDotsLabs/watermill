package message

import (
	"context"
)

type Publisher interface {
	// Publish publishes provided messages to given topic.
	//
	// Publish can be synchronous or asynchronous - it depends on the implementation.
	//
	// Most publishers implementations don't support atomic publishing of messages.
	// This means that if publishing one of the messages fails, the next messages will not be published.
	//
	// Publish must be thread safe.
	Publish(topic string, messages ...*Message) error
	// Close should flush unsent messages, if publisher is async.
	Close() error
}

type Subscriber interface {
	// Subscribe returns output channel with messages from provided topic.
	// Channel is closed, when Close() was called on the subscriber.
	//
	// To receive the next message, `Ack()` must be called on the received message.
	// If message processing failed and message should be redelivered `Nack()` should be called.
	//
	// When provided ctx is cancelled, subscriber will close subscribe and close output channel.
	// Provided ctx is set to all produced messages.
	// When Nack or Ack is called on the message, context of the message is canceled.
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
	// Close closes all subscriptions with their output channels and flush offsets etc. when needed.
	Close() error
}

type SubscribeInitializer interface {
	// SubscribeInitialize can be called to initialize subscribe before consume.
	// When calling Subscribe before Publish, SubscribeInitialize should be not required.
	//
	// Not every Pub/Sub requires this initialize and it may be optional for performance improvements etc.
	// For detailed SubscribeInitialize functionality, please check Pub/Subs godoc.
	//
	// Implementing SubscribeInitialize is not obligatory.
	SubscribeInitialize(topic string) error
}
