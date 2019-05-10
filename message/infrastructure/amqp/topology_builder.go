package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type TopologyBuilder interface {
	BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, config Config, logger watermill.LoggerAdapter) error
	ExchangeDeclare(channel *amqp.Channel, exchangeName string, config Config) error
}

type DefaultTopologyBuilder struct {
}

func (builder DefaultTopologyBuilder) ExchangeDeclare(channel *amqp.Channel, exchangeName string, config Config) error {
	return channel.ExchangeDeclare(
		exchangeName,
		config.Exchange.Type,
		config.Exchange.Durable,
		config.Exchange.AutoDeleted,
		config.Exchange.Internal,
		config.Exchange.NoWait,
		config.Exchange.Arguments,
	)
}

func (builder *DefaultTopologyBuilder) BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, config Config, logger watermill.LoggerAdapter) error  {
	if _, err := channel.QueueDeclare(
		queueName,
		config.Queue.Durable,
		config.Queue.AutoDelete,
		config.Queue.Exclusive,
		config.Queue.NoWait,
		config.Queue.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot declare queue")
	}

	logger.Debug("Queue declared", nil)

	if exchangeName == "" {
		logger.Debug("No exchange to declare", nil)
		return nil
	}
	if err := builder.ExchangeDeclare(channel, exchangeName, config); err != nil {
		return errors.Wrap(err, "cannot declare exchange")
	}

	logger.Debug("Exchange declared", nil)

	if err := channel.QueueBind(
		queueName,
		config.QueueBind.GenerateRoutingKey(queueName),
		exchangeName,
		config.QueueBind.NoWait,
		config.QueueBind.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot bind queue")
	}
	return nil
}
