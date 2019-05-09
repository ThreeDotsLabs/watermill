package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type topologyBuilder interface {
	buildTopology(channel *amqp.Channel, queueName string, logFields watermill.LogFields, exchangeName string, exchangeDeclarer exchangeDeclarer) error
}

type exchangeDeclarer func(channel *amqp.Channel, exchangeName string) error

type defaultTopologyBuilder struct {
	config Config
	logger watermill.LoggerAdapter
}

func (builder *defaultTopologyBuilder) buildTopology(channel *amqp.Channel, queueName string, logFields watermill.LogFields, exchangeName string, exchangeDeclarer exchangeDeclarer) error  {
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

	builder.logger.Debug("Queue declared", logFields)

	if exchangeName == "" {
		builder.logger.Debug("No exchange to declare", logFields)
		return nil
	}
	if err := exchangeDeclarer(channel, exchangeName); err != nil {
		return errors.Wrap(err, "cannot declare exchange")
	}

	builder.logger.Debug("Exchange declared", logFields)

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
