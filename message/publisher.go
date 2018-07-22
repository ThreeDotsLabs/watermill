package message

type Publisher interface {
	Publish(topic string, messages []ProducedMessage) error
	ClosePublisher() error
}
