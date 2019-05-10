package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type TopologyBuilder interface {
	BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, exchangeDeclarer ExchangeDeclarer) error
}

type ExchangeDeclarer func(channel *amqp.Channel, exchangeName string, config Config, logger watermill.LoggerAdapter) error

type DefaultTopologyBuilder struct {
	config Config
	logger watermill.LoggerAdapter
	logFields watermill.LogFields
}

func (builder *DefaultTopologyBuilder) BuildTopology(channel *amqp.Channel, queueName string, exchangeName string, exchangeDeclarer ExchangeDeclarer) error  {
	if _, err := channel.QueueDeclare(
		queueName,
		builder.config.Queue.Durable,
		builder.config.Queue.AutoDelete,
		builder.config.Queue.Exclusive,
		builder.config.Queue.NoWait,
		builder.config.Queue.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot declare queue")
	}

	builder.logger.Debug("Queue declared", builder.logFields)

	if exchangeName == "" {
		builder.logger.Debug("No exchange to declare", builder.logFields)
		return nil
	}
	if err := exchangeDeclarer(channel, exchangeName); err != nil {
		return errors.Wrap(err, "cannot declare exchange")
	}

	builder.logger.Debug("Exchange declared", builder.logFields)

	if err := channel.QueueBind(
		queueName,
		builder.config.QueueBind.GenerateRoutingKey(queueName),
		exchangeName,
		builder.config.QueueBind.NoWait,
		builder.config.QueueBind.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot bind queue")
	}
	return nil
}
