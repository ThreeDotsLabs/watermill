package gochannel

import (
	"sync"
	"time"

	"github.com/satori/go.uuid"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
)

type subscriber struct {
	uuid          string
	outputChannel chan *message.Message
}

type goChannel struct {
	sendTimeout time.Duration
	buffer      int64

	subscribers     map[string][]*subscriber
	subscribersLock *sync.RWMutex

	logger watermill.LoggerAdapter

	closed bool
}

func NewGoChannel(buffer int64, logger watermill.LoggerAdapter, sendTimeout time.Duration) message.PubSub {
	return &goChannel{
		sendTimeout: sendTimeout,
		buffer:      buffer,

		subscribers:     make(map[string][]*subscriber),
		subscribersLock: &sync.RWMutex{},
		logger:          logger,
	}
}

func (g *goChannel) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		if err := g.sendMessage(topic, msg); err != nil {
			return err
		}
	}

	return nil
}

func (g *goChannel) sendMessage(topic string, message *message.Message) error {
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
		subscriberLogFields := messageLogFields.Add(watermill.LogFields{
			"subscriber_uuid": s.uuid,
		})

	SendToSubscriber:
		for {
			select {
			case s.outputChannel <- message:
				select {
				case <-message.Acked():
					g.logger.Trace("message sent", subscriberLogFields)
					break SendToSubscriber
				case <-message.Nacked():
					g.logger.Trace("nack received, resending message", subscriberLogFields)

					// message have nack already sent, we need fresh message
					message = resetMessage(message)

					continue SendToSubscriber
				}
			case <-time.After(g.sendTimeout):
				return errors.Errorf("sending message %s timeouted after %s", message.UUID, g.sendTimeout)
			}
		}
	}

	return nil
}

func (g *goChannel) Subscribe(topic string, consumerGroup message.ConsumerGroup) (chan *message.Message, error) {
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

func (g *goChannel) Close() error {
	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()

	if g.closed {
		return nil
	}
	g.closed = true

	for _, topicSubscribers := range g.subscribers {
		for _, subscriber := range topicSubscribers {
			close(subscriber.outputChannel)
		}
	}

	return nil
}

func resetMessage(oldMsg *message.Message) *message.Message {
	m := message.NewMessage(oldMsg.UUID, oldMsg.Payload)
	m.Metadata = m.Metadata

	return m
}
