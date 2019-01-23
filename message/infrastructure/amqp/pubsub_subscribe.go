package amqp

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func (p *PubSub) Subscribe(topic string) (chan *message.Message, error) {
	if p.closed {
		return nil, errors.New("pub/sub is closed closed")
	}
	p.subscribingWg.Add(1)

	if !p.isConnected() {
		return nil, errors.New("not connected to AMQP")
	}

	logFields := watermill.LogFields{"topic": topic}

	channel, err := p.openSubscribeChannel(logFields)
	if err != nil {
		return nil, err
	}
	notifyCloseChannel := channel.NotifyClose(make(chan *amqp.Error))

	queueName := p.config.GenerateQueueName(topic)
	logFields["amqp_queue_name"] = queueName

	exchangeName := p.generateExchangeName(topic)
	logFields["amqp_exchange_name"] = exchangeName

	if err := p.prepareConsume(channel, topic, queueName, exchangeName, logFields); err != nil {
		return nil, err
	}

	out := make(chan *message.Message, 0)

	p.logger.Info("Starting consuming from AMQP channel", logFields)

	amqpMsgs, err := p.createMessagesConsumer(queueName, channel)
	if err != nil {
		return nil, err
	}

	sub := subscribtion{}

	go func() {
		sub.ConsumeMessages()
		p.subscribingWg.Done()
	}()

	return out, nil
}

func (p *PubSub) prepareConsume(
	channel *amqp.Channel,
	topic string,
	queueName string,
	exchangeName string,
	logFields watermill.LogFields,
) error {
	if _, err := channel.QueueDeclare(
		queueName,
		p.config.Queue.Durable,
		p.config.Queue.AutoDelete,
		p.config.Queue.Exclusive,
		p.config.Queue.NoWait,
		p.config.Queue.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot declare queue")
	}
	p.logger.Debug("Queue declared", logFields)

	if !p.config.Exchange.UseDefaultExchange() {
		exchangeName := p.generateExchangeName(topic)
		logFields["amqp_exchange_name"] = exchangeName

		if err := p.exchangeDeclare(channel, topic); err != nil {
			if err := channel.Close(); err != nil {
				p.logger.Error("Cannot close channel", err, nil)
			}
			return errors.Wrap(err, "cannot declare exchange")
		}
		p.logger.Debug("Exchange declared", logFields)

		if err := channel.QueueBind(
			queueName,
			p.config.QueueBind.RoutingKey,
			exchangeName,
			p.config.QueueBind.NoWait,
			p.config.QueueBind.Arguments,
		); err != nil {
			return errors.Wrap(err, "cannot bind queue")
		}
		p.logger.Debug("Queue bound to exchange", logFields)
	}

	return nil
}

func (p *PubSub) openSubscribeChannel(logFields watermill.LogFields) (*amqp.Channel, error) {
	if !p.isConnected() {
		return nil, errors.New("not connected to AMQP")
	}
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

	return channel, nil
}

func (p *PubSub) createMessagesConsumer(queueName string, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	amqpMsgs, err := channel.Consume(
		queueName,
		p.config.Consume.Consumer,
		false, // autoAck must be set to true - acks are managed by Watermill
		p.config.Consume.Exclusive,
		p.config.Consume.NoLocal,
		p.config.Consume.NoWait,
		p.config.Consume.Arguments,
	)
	if err != nil {
		return nil, errors.Wrap(err, "cannot consume from channel")
	}

	return amqpMsgs, nil
}

type subscribtion struct {
	out                chan *message.Message
	logFields          watermill.LogFields
	notifyCloseChannel chan *amqp.Error
	channel            *amqp.Channel
	amqpMsgs           <-chan amqp.Delivery
	queueName          string

	logger    watermill.LoggerAdapter
	connected chan struct{}
	closing   chan struct{}
	config    Config
}

func (s *subscribtion) ConsumeMessages() {
	defer func() {
		close(s.out) // todo - test it
		s.logger.Info("Stopped consuming from AMQP channel", s.logFields)
	}()

ReconnectLoop:
	for {
		select {
		case <-s.notifyCloseChannel: // todo - make it more standard way? (auto channel close by connection???)
			if err := s.channel.Close(); err != nil {
				s.logger.Error("Channel close error", err, s.logFields)
			}
			s.channel = nil

			s.logger.Error("Channel closed, waiting for a new connection", nil, s.logFields)

			for {
				<-s.connected
				s.logger.Info("Connected to AMQP, recreating channel", s.logFields)

				var err error
				s.channel, err = s.openSubscribeChannel(s.logFields) // todo !
				if err != nil {
					s.logger.Error("Failed to recreate channel", err, s.logFields)
					continue
				}
				s.notifyCloseChannel = s.channel.NotifyClose(make(chan *amqp.Error))

				s.amqpMsgs, err = s.createMessagesConsumer(queueName, channel) // todo !
				if err != nil {
					s.logger.Error("Failed to start consuming messages again", err, s.logFields)
					continue
				}

				break
			}

		case amqpMsg := <-s.amqpMsgs:
			if err := s.processMessage(amqpMsg, s.out, s.logFields); err != nil {
				s.logger.Error("Processing message failed, sending nack", err, s.logFields)

				if err := s.nackMsg(amqpMsg); err != nil {
					s.logger.Error("Cannot nack message", err, s.logFields)
					continue ReconnectLoop // todo - goood? maybe better to stop?
				}
			}

		case <-s.closing:
			break ReconnectLoop
		}
	}
}

func (p *subscribtion) processMessage(amqpMsg amqp.Delivery, out chan *message.Message, logFields watermill.LogFields) error {
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

func (p *subscribtion) nackMsg(amqpMsg amqp.Delivery) error {
	return amqpMsg.Nack(false, p.config.RequeueOnNack)
}
