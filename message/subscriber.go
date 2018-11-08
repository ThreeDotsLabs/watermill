package message

type ConsumerGroup string

type Subscriber interface {
	Subscribe(topic string, consumerGroup ConsumerGroup) (chan *Message, error)
	CloseSubscriber() error
}

type NoConsumerGroupSubscriber interface {
	SubscribeNoGroup(topic string) (chan *Message, error)
	CloseSubscriber() error
}
