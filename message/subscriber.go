package message

// todo - make it easier to implement
type Subscriber interface {
	Subscribe(topic string, metadata SubscriberMetadata) (chan *Message, error) // rename to listen? rename to Subscriber??
	Close() error
}

// todo - rename
type SubscriberMetadata struct {
	SubscriberName string // todo - rename (type?)
	ServerName     string

	Hostname string
}
