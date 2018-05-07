package eventlistener

import (
	"github.com/Shopify/sarama"
	"github.com/roblaszczak/gooddd/domain"
	"github.com/roblaszczak/gooddd/msghandler"
	"github.com/pkg/errors"
)

type saramaKafka struct {
	consumer sarama.Consumer
}

func NewSimpleKafka(brokers []string) (msghandler.EventsListener, error) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create consumer")
	}

	return NewSaramaKafka(consumer)
}

func NewSaramaKafka(consumer sarama.Consumer) (msghandler.EventsListener, error) {
	return saramaKafka{consumer}, nil
}

func (s saramaKafka) Subscribe(topic string) (chan domain.Event, error) {
	//s.consumer.ConsumePartition(topic)
	return nil, nil // todo
}

func (s saramaKafka) Close() error {
	return s.consumer.Close()
}
