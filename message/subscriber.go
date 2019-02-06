package message

type subscriber interface {
	// Subscribe returns output channel with messages from provided topic.
	// Channel is closed, when Close() was called to the subscriber.
	//
	// To receive next message, `Ack()` must be called on the received message.
	// If message processing was failed and message should be redelivered `Nack()` should be called.
	Subscribe(topic string) (chan *Message, error)
}

type Subscriber interface {
	subscriber

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
