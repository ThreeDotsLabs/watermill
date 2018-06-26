package message

type Publisher interface {
	Publish(topic string, messages []Message) error
	ClosePublisher() error
}

type SubscriberMetadata struct {
	SubscriberName string
	ServerName     string

	Hostname string
}

type Subscriber interface {
	Subscribe(topic string) (chan Message, error)
	CloseSubscriber() error
}

type PubSub interface {
	Publisher
	Subscriber

	Close() error
}
