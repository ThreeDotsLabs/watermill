package gochannel

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type subscriber struct {
	uuid          string
	outputChannel chan *message.Message
}

// GoChannel is the simplest Pub/Sub implementation.
// It is based on Golang's channels which are sent within the process.
//
// GoChannel has no global state,
// that means that you need to use the same instance for Publishing and Subscribing!
type GoChannel struct {
	sendTimeout time.Duration
	buffer      int64

	subscribers     map[string][]*subscriber
	subscribersLock *sync.RWMutex

	logger watermill.LoggerAdapter

	closed  bool
	closing chan struct{}
}

func NewGoChannel(buffer int64, logger watermill.LoggerAdapter, sendTimeout time.Duration) message.PubSub {
	return &GoChannel{
		sendTimeout: sendTimeout,
		buffer:      buffer,

		subscribers:     make(map[string][]*subscriber),
		subscribersLock: &sync.RWMutex{},
		logger:          logger,

		closing: make(chan struct{}),
	}
}

// Publish in GoChannel is blocking until all consumers consume and acknowledge the message.
// Sending message to one subscriber has timeout equal to GoChannel.sendTimeout configured via constructor.
//
// Messages are not persisted. If there are no subscribers and message is produced it will be gone.
func (g *GoChannel) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		if err := g.sendMessage(topic, msg); err != nil {
			return err
		}
	}

	return nil
}

func (g *GoChannel) sendMessage(topic string, message *message.Message) error {
	messageLogFields := watermill.LogFields{
		"message_uuid": message.UUID,
	}

	g.subscribersLock.RLock()
	defer g.subscribersLock.RUnlock()

	subscribers, ok := g.subscribers[topic]
	if !ok {
		return nil
	}

	for _, s := range subscribers {
		if err := g.sendMessageToSubscriber(message, s, messageLogFields); err != nil {
			return err
		}
	}

	return nil
}

func (g *GoChannel) sendMessageToSubscriber(msg *message.Message, s *subscriber, messageLogFields watermill.LogFields) error {
	subscriberLogFields := messageLogFields.Add(watermill.LogFields{
		"subscriber_uuid": s.uuid,
	})

SendToSubscriber:
	for {
		// copy the message to prevent ack/nack propagation to other consumers
		// also allows to make retries on a fresh copy of the original message
		msgToSend := msg.Copy()

		ctx, cancelCtx := context.WithCancel(context.Background())
		msgToSend.SetContext(ctx)
		defer cancelCtx()

		select {
		case s.outputChannel <- msgToSend:
			g.logger.Trace("Sent message to subscriber", subscriberLogFields)
		case <-time.After(g.sendTimeout):
			return errors.Errorf("Sending message %s timeouted after %s", msgToSend.UUID, g.sendTimeout)
		case <-g.closing:
			g.logger.Trace("Closing, message discarded", subscriberLogFields)
			return nil
		}

		select {
		case <-msgToSend.Acked():
			g.logger.Trace("Message acked", subscriberLogFields)
			break SendToSubscriber
		case <-msgToSend.Nacked():
			g.logger.Trace("Nack received, resending message", subscriberLogFields)

			continue SendToSubscriber
		case <-g.closing:
			g.logger.Trace("Closing, message discarded", subscriberLogFields)
			return nil
		}
	}

	return nil
}

// Subscribe returns channel to which all published messages are sent.
// Messages are not persisted. If there are no subscribers and message is produced it will be gone.
//
// There are no consumer groups support etc. Every consumer will receive every produced message.
func (g *GoChannel) Subscribe(topic string) (chan *message.Message, error) {
	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()

	if _, ok := g.subscribers[topic]; !ok {
		g.subscribers[topic] = make([]*subscriber, 0)
	}

	s := &subscriber{
		uuid:          uuid.NewV4().String(),
		outputChannel: make(chan *message.Message, g.buffer),
	}
	g.subscribers[topic] = append(g.subscribers[topic], s)

	return s.outputChannel, nil
}

func (g *GoChannel) Close() error {
	if g.closed {
		return nil
	}
	g.closed = true
	close(g.closing)

	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()

	for _, topicSubscribers := range g.subscribers {
		for _, subscriber := range topicSubscribers {
			close(subscriber.outputChannel)
		}
	}

	return nil
}
