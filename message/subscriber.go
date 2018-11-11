package message

type subscriber interface {
	Subscribe(topic string) (chan *Message, error)
}

type Subscriber interface {
	subscriber
	Close() error
}
