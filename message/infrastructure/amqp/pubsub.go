package amqp

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/streadway/amqp"
)

// todo - compare with https://github.com/micro/go-plugins/blob/master/broker/rabbitmq/channel.go
// todo - compare with https://github.com/RichardKnop/machinery
// todo - add link to rabbit's docs
// todo - check bulk processing
// todo - consult settings

type PubSub struct {
	config Config
	logger watermill.LoggerAdapter

	connection *amqp.Connection

	closing chan struct{}
	closed  bool

	publishingWg  sync.WaitGroup
	subscribingWg sync.WaitGroup
}

func NewPubSub(config Config, logger watermill.LoggerAdapter) (*PubSub, error) {
	// todo - reconnect support
	// todo - validate config

	pubSub := &PubSub{
		logger:  logger,
		config:  config,
		closing: make(chan struct{}),
	}
	if err := pubSub.connect(); err != nil {
		return nil, err
	}

	return pubSub, nil
}

func (p *PubSub) connect() error {
	connection, err := amqp.Dial(p.config.AmqpURI)
	if err != nil {
		return errors.Wrap(err, "cannot connect to amqp")
	}
	p.connection = connection

	return nil
}

func (p *PubSub) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return errors.New("pub/sub is closed closed")
	}

	p.publishingWg.Add(1)
	defer p.publishingWg.Done()

	// todo - do it once
	channel, err := p.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "cannot open channel")
	}
	defer func() {
		// todo - close on close
		if err := channel.Close(); err != nil {
			p.logger.Error("cannot close channel", err, nil)
		}
	}()

	// todo - do it once
	_, err = channel.QueueDeclare(
		p.config.GenerateQueueName(topic),
		p.config.Queue.Durable,
		p.config.Queue.AutoDelete,
		p.config.Queue.Exclusive,
		p.config.Queue.NoWait,
		p.config.Queue.Arguments,
	)
	if err != nil {
		return errors.Wrap(err, "cannot declare queue")
	}

	// todo - do it once
	if !p.config.Exchange.UseDefaultExchange() {
		// todo - better place
		if err := p.exchangeDeclare(channel, topic); err != nil {
			return nil
		}
	}

	for _, msg := range messages {
		logFields := watermill.LogFields{"message_uuid": msg.UUID}
		p.logger.Trace("Publishing message", logFields)

		amqpMsg, err := p.config.Marshaler.Marshal(msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal msg %s", msg.UUID) // todo - remove uuid from err?
		}

		// todo - amqp.persistent?

		if err = channel.Publish(
			p.generateExchangeName(topic), // exchange // todo - topic? doc?
			p.generateRoutingKey(topic),
			p.config.Publish.Mandatory,
			p.config.Publish.Immediate,
			amqpMsg,
		); err != nil {
			return errors.Wrap(err, "cannot publish msg")
		}

		p.logger.Trace("Message published", logFields)
	}

	return nil
}

func (p *PubSub) generateRoutingKey(topic string) string {
	// todo - magic
	if p.config.Exchange.UseDefaultExchange() && p.config.Publish.RoutingKey == "" {
		return topic
	}

	return p.config.Publish.RoutingKey
}

func (p *PubSub) generateExchangeName(topic string) string {
	// todo - magic
	if p.config.Exchange.UseDefaultExchange() {
		return "" // todo -doc
	}

	return topic
}

func (p *PubSub) Subscribe(topic string) (chan *message.Message, error) {
	if p.closed {
		return nil, errors.New("pub/sub is closed closed")
	}
	p.subscribingWg.Add(1)

	logFields := watermill.LogFields{"topic": topic}

	channel, err := p.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "cannot open channel")
	}
	p.logger.Debug("Channel opened", logFields)

	// todo - benchmark & check
	if p.config.Qos != (QosConfig{}) {
		err = channel.Qos(
			p.config.Qos.PrefetchCount, // prefetch count
			p.config.Qos.PrefetchSize,  // prefetch size
			p.config.Qos.Global,        // global
		)
		p.logger.Debug("Qos set", logFields)
	}

	// todo - validate generateQueueName

	queueName := p.config.GenerateQueueName(topic)
	logFields["amqp_queue_name"] = queueName

	if _, err := channel.QueueDeclare(
		queueName,
		p.config.Queue.Durable,
		p.config.Queue.AutoDelete,
		p.config.Queue.Exclusive,
		p.config.Queue.NoWait,
		p.config.Queue.Arguments,
	); err != nil {
		return nil, errors.Wrap(err, "cannot declare queue")
	}
	p.logger.Debug("Queue declared", logFields)

	if !p.config.Exchange.UseDefaultExchange() {
		exchangeName := p.generateExchangeName(topic)
		logFields["amqp_exchange_name"] = exchangeName

		if err := p.exchangeDeclare(channel, topic); err != nil {
			if err := channel.Close(); err != nil {
				p.logger.Error("cannot close channel", err, nil)
			}
			return nil, errors.Wrap(err, "cannot declare exchange")
		}
		p.logger.Debug("Exchange declared", logFields)

		if err := channel.QueueBind(
			queueName,
			p.config.QueueBind.RoutingKey,
			exchangeName,
			p.config.QueueBind.NoWait,
			p.config.QueueBind.Arguments,
		); err != nil {
			return nil, errors.Wrap(err, "cannot bind queue")
		}
		p.logger.Debug("Queue bound to exchange", logFields)
	}

	amqpMsgs, err := channel.Consume(
		queueName,
		p.config.Consume.Consumer,
		false, // autoAck must be set to true by the design of Watermill
		p.config.Consume.Exclusive,
		p.config.Consume.NoLocal,
		p.config.Consume.NoWait,
		p.config.Consume.Arguments,
	)
	if err != nil {
		return nil, errors.Wrap(err, "cannot consume from channel")
	}

	out := make(chan *message.Message, 0)

	p.logger.Info("Starting consuming from amqp channel", logFields)

	go func(out chan *message.Message) {
		defer func() {
			close(out) // todo - test it
			p.logger.Info("Stopped consuming from amqp channel", logFields)
			p.subscribingWg.Done()
		}()

		for amqpMsg := range amqpMsgs {
			if err := p.processMessage(amqpMsg, out, logFields); err != nil {
				p.logger.Error("processing message failed, sending nack", err, logFields)

				if err := p.nackMsg(amqpMsg); err != nil {
					p.logger.Error("cannot nack message", err, logFields)
					// something went really wrong, closing subscriber
					return
				}
			}
		}
	}(out)

	return out, nil
}

func (p *PubSub) processMessage(amqpMsg amqp.Delivery, out chan *message.Message, logFields watermill.LogFields) error {
	msg, err := p.config.Marshaler.Unmarshal(amqpMsg)
	if err != nil {
		return err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	msg.SetContext(ctx)
	defer cancelCtx()

	msgLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	p.logger.Trace("Unmarshaled message", msgLogFields)

	select {
	case <-p.closing:
		p.logger.Info("Message not consumed, pub/sub is closing", msgLogFields)
		return p.nackMsg(amqpMsg)
	case out <- msg:
		p.logger.Trace("Message sent to consumer", msgLogFields)
	}

	select {
	case <-p.closing:
		p.logger.Trace("Closing pub/sub, message discarded before ack", msgLogFields)
		return p.nackMsg(amqpMsg)
	case <-msg.Acked():
		p.logger.Trace("Message Acked", msgLogFields)
		return amqpMsg.Ack(false)
	case <-msg.Nacked():
		p.logger.Trace("Message Nacked", msgLogFields)
		return p.nackMsg(amqpMsg)
	}
}

func (p *PubSub) nackMsg(amqpMsg amqp.Delivery) error {
	return amqpMsg.Nack(false, p.config.RequeueOnNack)
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

	p.publishingWg.Wait()

	if err := p.connection.Close(); err != nil {
		return err
	}

	p.subscribingWg.Wait()

	return nil
}
