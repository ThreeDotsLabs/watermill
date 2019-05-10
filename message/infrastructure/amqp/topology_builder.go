package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type TopologyBuilder interface {
	BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, exchangeDeclare ExchangeDeclare, config Config, logger watermill.LoggerAdapter) error
}

type ExchangeDeclare func(channel *amqp.Channel, exchangeName string) error

type DefaultTopologyBuilder struct {
}

func (builder *DefaultTopologyBuilder) BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, exchangeDeclare ExchangeDeclare, config Config, logger watermill.LoggerAdapter) error  {
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
	if err := exchangeDeclare(channel, exchangeName); err != nil {
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
