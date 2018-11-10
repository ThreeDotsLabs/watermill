package message

type publisher interface {
	Publish(topic string, messages ...*Message) error
}

type Publisher interface {
	publisher
	Close() error
}
