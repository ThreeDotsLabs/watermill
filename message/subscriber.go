package message

type ConsumerGroup string

type subscriber interface {
	Subscribe(topic string, consumerGroup ConsumerGroup) (chan *Message, error) // todo - remove consumer group from interface

}

type Subscriber interface {
	subscriber
	Close() error
}

type NoConsumerGroupSubscriber interface {
	SubscribeNoGroup(topic string) (chan *Message, error)
	Close() error
}
