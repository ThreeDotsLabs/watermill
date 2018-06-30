package message

type Publisher interface {
	Publish(topic string, messages []Message) error
	ClosePublisher() error
}
