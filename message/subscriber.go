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
