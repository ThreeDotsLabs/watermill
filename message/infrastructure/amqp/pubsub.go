package amqp

import (
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/streadway/amqp"
)

// todo - consult settings
// todo - add link to rabbit's docs
// todo - add in docs supported features
// todo - add in docs - https://www.rabbitmq.com/production-checklist.html
// todo - production deploy watermill in docs - base on: https://www.rabbitmq.com/production-checklist.html

type PubSub struct {
	config Config
	logger watermill.LoggerAdapter

	connection     *amqp.Connection
	connectionLock sync.Mutex
	connected      chan struct{}

	publishBindingsLock     sync.RWMutex
	publishBindingsPrepared map[string]struct{}

	closing chan struct{}
	closed  bool

	publishingWg  sync.WaitGroup
	subscribingWg sync.WaitGroup
}

func NewPubSub(config Config, logger watermill.LoggerAdapter) (*PubSub, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	pubSub := &PubSub{
		logger:    logger,
		config:    config,
		closing:   make(chan struct{}),
		connected: make(chan struct{}),
	}
	if err := pubSub.connect(); err != nil {
		return nil, err
	}

	go pubSub.handleConnectionClose()

	return pubSub, nil
}

// generateRoutingKey generates routing key based on topic or config.
func (p *PubSub) generateRoutingKey(topic string) string {
	return p.config.Publish.GenerateRoutingKey(topic)
}

// generateExchangeName generates exchange name based on topic.
func (p *PubSub) generateExchangeName(topic string) string {
	return p.config.Exchange.GenerateName(topic)
}

func (p *PubSub) exchangeDeclare(channel *amqp.Channel, exchangeName string) error {
	return channel.ExchangeDeclare(
		exchangeName,
		p.config.Exchange.Type,
		p.config.Exchange.Durable,
		p.config.Exchange.AutoDeleted,
		p.config.Exchange.Internal,
		p.config.Exchange.NoWait,
		p.config.Exchange.Arguments,
	)
}

func (p *PubSub) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.closing)

	p.logger.Info("Closing AMQP Pub/Sub", nil)
	defer p.logger.Info("Closed AMQP Pub/Sub", nil)

	p.publishingWg.Wait()

	if err := p.connection.Close(); err != nil {
		p.logger.Error("Connection close error", err, nil)
	}

	p.subscribingWg.Wait()

	return nil
}
