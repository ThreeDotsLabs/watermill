package message

type Publisher interface {
	Publish(messages []Message) error // todo - remove topic from args?
	Close() error
}
