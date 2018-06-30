package message

type Subscriber interface {
	Subscribe(topic string) (chan Message, error)
	CloseSubscriber() error
}
