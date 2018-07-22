package message

type ConsumerGroup string

type Subscriber interface {
	Subscribe(topic string, consumerGroup ConsumerGroup) (chan ConsumedMessage, error)
	CloseSubscriber() error
}
