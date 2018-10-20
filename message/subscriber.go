package message

type ConsumerGroup string

type Subscriber interface {
	Subscribe(topic string, consumerGroup ConsumerGroup) (chan ConsumedMessage, error)
	CloseSubscriber() error
}

type NoConsumerGroupSubscriber interface {
	SubscribeNoGroup(topic string) (chan ConsumedMessage, error)
	CloseSubscriber() error
}