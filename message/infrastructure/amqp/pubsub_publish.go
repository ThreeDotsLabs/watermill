package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Publish publishes messages to AMQP broker.
// Publish is blocking since broker received and saved the message.
// Publish is always thread safe.
//
// Watermill's topic in publish is not mapped to AMQP's topic, but depending on configuration it can be mapped
// to exchange, queue or routing key.
// For detailed description of nomenclature mapping, please check "Nomenclature" paragraph in PubSub GoDoc.
func (p *PubSub) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return errors.New("pub/sub is connection closed")
	}
	p.publishingWg.Add(1)
	defer p.publishingWg.Done()

	if !p.IsConnected() {
		return errors.New("not connected to AMQP")
	}

	channel, err := p.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "cannot open channel")
	}
	defer func() {
		if channelCloseErr := channel.Close(); channelCloseErr != nil {
			err = multierror.Append(err, channelCloseErr)
		}
	}()

	if p.config.Publish.Transactional {
		if err := p.beginTransaction(channel); err != nil {
			return err
		}

		defer func() {
			err = p.commitTransaction(channel, err)
		}()
	}

	if err := p.preparePublishBindings(topic, channel); err != nil {
		return err
	}

	logFields := make(watermill.LogFields, 3)

	exchangeName := p.generateExchangeName(topic)
	logFields["amqp_exchange_name"] = exchangeName

	routingKey := p.generateRoutingKey(topic)
	logFields["amqp_routing_key"] = routingKey

	for _, msg := range messages {
		if err := p.publishMessage(exchangeName, routingKey, msg, channel, logFields); err != nil {
			return err
		}
	}

	p.logger.Trace("All messages published", logFields)

	return nil
}

func (p *PubSub) beginTransaction(channel *amqp.Channel) error {
	if err := channel.Tx(); err != nil {
		return errors.Wrap(err, "cannot start transaction")
	}

	p.logger.Trace("Transaction begun", nil)

	return nil
}

func (p *PubSub) commitTransaction(channel *amqp.Channel, err error) error {
	if err != nil {
		if rollbackErr := channel.TxRollback(); rollbackErr != nil {
			return multierror.Append(err, rollbackErr)
		}
	}

	return channel.TxCommit()
}

func (p *PubSub) publishMessage(
	exchangeName, routingKey string,
	msg *message.Message,
	channel *amqp.Channel,
	logFields watermill.LogFields,
) error {
	logFields = logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})

	p.logger.Trace("Publishing message", logFields)

	amqpMsg, err := p.config.Marshaler.Marshal(msg)
	if err != nil {
		return err
	}

	if err = channel.Publish(
		exchangeName,
		routingKey,
		p.config.Publish.Mandatory,
		p.config.Publish.Immediate,
		amqpMsg,
	); err != nil {
		return errors.Wrap(err, "cannot publish msg")
	}

	p.logger.Trace("Message published", logFields)

	return nil
}

func (p *PubSub) preparePublishBindings(topic string, channel *amqp.Channel) error {
	p.publishBindingsLock.RLock()
	_, prepared := p.publishBindingsPrepared[topic]
	p.publishBindingsLock.RUnlock()

	if prepared {
		return nil
	}

	p.publishBindingsLock.Lock()
	defer p.publishBindingsLock.Unlock()

	if _, err := channel.QueueDeclare(
		p.config.GenerateQueueName(topic),
		p.config.Queue.Durable,
		p.config.Queue.AutoDelete,
		p.config.Queue.Exclusive,
		p.config.Queue.NoWait,
		p.config.Queue.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot declare queue")
	}

	if p.generateExchangeName(topic) != "" {
		if err := p.exchangeDeclare(channel, p.generateExchangeName(topic)); err != nil {
			return err
		}
	}

	if p.publishBindingsPrepared == nil {
		p.publishBindingsPrepared = make(map[string]struct{})
	}
	p.publishBindingsPrepared[topic] = struct{}{}

	return nil
}
