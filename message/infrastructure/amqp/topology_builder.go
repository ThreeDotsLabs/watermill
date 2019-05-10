package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type TopologyBuilder interface {
	BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, exchangeDeclarer ExchangeDeclarer, config Config, logger watermill.LoggerAdapter, logFields watermill.LogFields) error
}

type ExchangeDeclarer func(channel *amqp.Channel, exchangeName string) error

type DefaultTopologyBuilder struct {
}

func (builder *DefaultTopologyBuilder) BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, exchangeDeclarer ExchangeDeclarer, config Config, logger watermill.LoggerAdapter, logFields watermill.LogFields) error  {
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

	logger.Debug("Queue declared", logFields)

	if exchangeName == "" {
		logger.Debug("No exchange to declare", logFields)
		return nil
	}
	if err := exchangeDeclarer(channel, exchangeName); err != nil {
		return errors.Wrap(err, "cannot declare exchange")
	}

	logger.Debug("Exchange declared", logFields)

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
