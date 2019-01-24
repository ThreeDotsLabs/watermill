package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func (p *PubSub) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return errors.New("pub/sub is connection closed")
	}
	p.publishingWg.Add(1)
	defer p.publishingWg.Done()

	if !p.isConnected() {
		return errors.New("not connected to AMQP")
	}

	channel, err := p.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "cannot open channel")
	}
	defer func() {
		if err := channel.Close(); err != nil {
			p.logger.Error("cannot close channel", err, nil)
		}
	}()

	if err := p.preparePublishBindings(topic, channel); err != nil {
		return err
	}

	for _, msg := range messages {
		if err := p.publishMessage(topic, msg, channel); err != nil {
			return err
		}
	}

	return nil
}

func (p *PubSub) publishMessage(topic string, msg *message.Message, channel *amqp.Channel) error {
	logFields := watermill.LogFields{"message_uuid": msg.UUID}
	p.logger.Trace("Publishing message", logFields)

	amqpMsg, err := p.config.Marshaler.Marshal(msg)
	if err != nil {
		return err
	}

	if err = channel.Publish(
		p.generateExchangeName(topic),
		p.generateRoutingKey(topic),
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

	if !p.config.Exchange.UseDefaultExchange() {
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
