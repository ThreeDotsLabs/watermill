package amqp

import (
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/streadway/amqp"
)

// todo - compare with https://github.com/micro/go-plugins/blob/master/broker/rabbitmq/channel.go
// todo - compare with https://github.com/RichardKnop/machinery
// todo - compare with https://github.com/assembla/cony
// todo - add link to rabbit's docs
// todo - https://www.rabbitmq.com/confirms.html
// todo - https://www.rabbitmq.com/production-checklist.html
// todo - https://www.rabbitmq.com/monitoring.html
// todo - check bulk processing
// todo - consult settingsd
// todo - add in docs supported features

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
func (p *PubSub) generateRoutingKey(topic string) string {
	// todo - magic, doc it good
	if p.config.Exchange.UseDefaultExchange() && p.config.Publish.RoutingKey == "" {
		return topic
	}

	return p.config.Publish.RoutingKey
}

func (p *PubSub) generateExchangeName(topic string) string {
	// todo - magic, doc it good
	if p.config.Exchange.UseDefaultExchange() {
		return "" // todo -doc
	}

	return topic
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
