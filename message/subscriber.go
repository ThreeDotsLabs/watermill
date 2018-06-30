package message

type Subscriber interface {
	Subscribe(topic string) (chan Message, error)
	CloseSubscriber() error
}

type ConsumerGroupSubscriber interface {
	// todo - rename?
	SubscribeConsumerGroup(topic string, consumerGroup string) (chan Message, error)
	CloseSubscriber() error
}
