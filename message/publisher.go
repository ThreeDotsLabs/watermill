package message

type publisher interface {
	// Publish publishes provided messages to given topic.
	//
	// Publish can be synchronous or asynchronous - it depends of implementation.
	//
	// Most publishers implementations doesn't support atomic publishing of messages.
	// That means, that when publishing one of messages failed next messages will be not published.
	//
	// Publish must be thread safe.
	Publish(topic string, messages ...*Message) error
}

type Publisher interface {
	publisher

	// Close should flush unsent messages, if publisher is async.
	Close() error
}
