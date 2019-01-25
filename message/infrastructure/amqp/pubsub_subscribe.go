package amqp

import (
	"context"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func (p *PubSub) Subscribe(topic string) (chan *message.Message, error) {
	if p.closed {
		return nil, errors.New("pub/sub is closed")
	}

	if !p.IsConnected() {
		return nil, errors.New("not connected to AMQP")
	}

	logFields := watermill.LogFields{"topic": topic}

	out := make(chan *message.Message, 0)

	queueName := p.config.GenerateQueueName(topic)
	logFields["amqp_queue_name"] = queueName

	exchangeName := p.generateExchangeName(topic)
	logFields["amqp_exchange_name"] = exchangeName

	if err := p.prepareConsume(queueName, exchangeName, logFields); err != nil {
		return nil, errors.Wrap(err, "failed to prepare consume")
	}

	p.subscribingWg.Add(1)
	go func() {
		defer func() {
			close(out)
			p.logger.Info("Stopped consuming from AMQP channel", logFields)
			p.subscribingWg.Done()
		}()

	ReconnectLoop:
		for {
			p.logger.Debug("Waiting for p.connected or p.closing in ReconnectLoop", logFields)

			select {
			case <-p.connected:
				p.logger.Debug("Waiting for p.connected or p.closing in ReconnectLoop", logFields)
				p.runSubscriber(out, queueName, exchangeName, logFields)
			case <-p.closing:
				p.logger.Debug("Stopping ReconnectLoop", logFields)
				break ReconnectLoop
			}

			time.Sleep(time.Millisecond * 100)
		}
	}()

	return out, nil
}

func (p *PubSub) prepareConsume(queueName string, exchangeName string, logFields watermill.LogFields) (err error) {
	channel, err := p.openSubscribeChannel(logFields)
	if err != nil {
		return err
	}
	defer func() {
		if channelCloseErr := channel.Close(); channelCloseErr != nil {
			err = multierror.Append(err, channelCloseErr)
		}
	}()

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

	if exchangeName == "" {
		p.logger.Debug("No exchange to declare", logFields)
		return nil
	}

	if err := p.exchangeDeclare(channel, exchangeName); err != nil {
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

	return nil
}

func (p *PubSub) runSubscriber(out chan *message.Message, queueName string, exchangeName string, logFields watermill.LogFields) {
	channel, err := p.openSubscribeChannel(logFields)
	if err != nil {
		p.logger.Error("Failed to open channel", err, logFields)
		return
	}
	defer func() {
		err := channel.Close()
		p.logger.Error("Failed to close channel", err, logFields)
	}()

	notifyCloseChannel := channel.NotifyClose(make(chan *amqp.Error))

	sub := subscription{
		out:                out,
		logFields:          logFields,
		notifyCloseChannel: notifyCloseChannel,
		channel:            channel,
		queueName:          queueName,
		logger:             p.logger,
		closing:            p.closing,
		config:             p.config,
	}

	p.logger.Info("Starting consuming from AMQP channel", logFields)

	sub.ProcessMessages()
}

func (p *PubSub) openSubscribeChannel(logFields watermill.LogFields) (*amqp.Channel, error) {
	if !p.IsConnected() {
		return nil, errors.New("not connected to AMQP")
	}

	channel, err := p.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "cannot open channel")
	}
	p.logger.Debug("Channel opened", logFields)

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

type subscription struct {
	out                chan *message.Message
	logFields          watermill.LogFields
	notifyCloseChannel chan *amqp.Error
	channel            *amqp.Channel
	queueName          string

	logger  watermill.LoggerAdapter
	closing chan struct{}
	config  Config
}

func (s *subscription) ProcessMessages() {
	amqpMsgs, err := s.createConsumer(s.queueName, s.channel)
	if err != nil {
		s.logger.Error("Failed to start consuming messages", err, s.logFields)
		return
	}

ConsumingLoop:
	for {
		select {
		case amqpMsg := <-amqpMsgs:
			if err := s.processMessage(amqpMsg, s.out, s.logFields); err != nil {
				s.logger.Error("Processing message failed, sending nack", err, s.logFields)

				if err := s.nackMsg(amqpMsg); err != nil {
					s.logger.Error("Cannot nack message", err, s.logFields)

					// something went really wrong when we cannot nack, let's reconnect
					break ConsumingLoop
				}
			}
			continue ConsumingLoop

		case <-s.notifyCloseChannel:
			s.logger.Error("Channel closed, stopping ProcessMessages", nil, s.logFields)
			break ConsumingLoop

		case <-s.closing:
			break ConsumingLoop
		}
	}
}

func (s *subscription) createConsumer(queueName string, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	amqpMsgs, err := channel.Consume(
		queueName,
		s.config.Consume.Consumer,
		false, // autoAck must be set to true - acks are managed by Watermill
		s.config.Consume.Exclusive,
		s.config.Consume.NoLocal,
		s.config.Consume.NoWait,
		s.config.Consume.Arguments,
	)
	if err != nil {
		return nil, errors.Wrap(err, "cannot consume from channel")
	}

	return amqpMsgs, nil
}

func (s *subscription) processMessage(amqpMsg amqp.Delivery, out chan *message.Message, logFields watermill.LogFields) error {
	msg, err := s.config.Marshaler.Unmarshal(amqpMsg)
	if err != nil {
		return err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	msg.SetContext(ctx)
	defer cancelCtx()

	msgLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.logger.Trace("Unmarshaled message", msgLogFields)

	select {
	case <-s.closing:
		s.logger.Info("Message not consumed, pub/sub is closing", msgLogFields)
		return s.nackMsg(amqpMsg)
	case out <- msg:
		s.logger.Trace("Message sent to consumer", msgLogFields)
	}

	select {
	case <-s.closing:
		s.logger.Trace("Closing pub/sub, message discarded before ack", msgLogFields)
		return s.nackMsg(amqpMsg)
	case <-msg.Acked():
		s.logger.Trace("Message Acked", msgLogFields)
		return amqpMsg.Ack(false)
	case <-msg.Nacked():
		s.logger.Trace("Message Nacked", msgLogFields)
		return s.nackMsg(amqpMsg)
	}
}

func (s *subscription) nackMsg(amqpMsg amqp.Delivery) error {
	return amqpMsg.Nack(false, !s.config.NoRequeueOnNack)
}
