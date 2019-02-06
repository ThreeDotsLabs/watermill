package gochannel

import (
	"context"
	"sync"

	"github.com/renstrom/shortuuid"

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
//
// When GoChannel is persistent, messages order is not guaranteed.
type GoChannel struct {
	outputChannelBuffer int64

	subscribers            map[string][]*subscriber
	subscribersLock        sync.RWMutex
	subscribersByTopicLock sync.Map // map of *sync.Mutex

	logger watermill.LoggerAdapter

	closed  bool
	closing chan struct{}

	// If persistent is set to true, when subscriber subscribes to the topic,
	// it will receive all previously produced messages.
	// All messages are persisted to the memory,
	// so be aware that with large amount of messages you can go out of the memory.
	persistent bool

	persistedMessages map[string][]*message.Message
}

func (g *GoChannel) Publisher() message.Publisher {
	return g
}

func (g *GoChannel) Subscriber() message.Subscriber {
	return g
}

// NewGoChannel creates new GoChannel Pub/Sub.
//
// This GoChannel is not persistent.
// That means if you send a message to a topic to which no subscriber is subscribed, that message will be discarded.
func NewGoChannel(outputChannelBuffer int64, logger watermill.LoggerAdapter) message.PubSub {
	return &GoChannel{
		outputChannelBuffer: outputChannelBuffer,

		subscribers:            make(map[string][]*subscriber),
		subscribersByTopicLock: sync.Map{},
		logger: logger.With(watermill.LogFields{
			"pubsub_uuid": shortuuid.New(),
		}),

		closing: make(chan struct{}),
	}
}

// NewPersistentGoChannel creates new persistent GoChannel Pub/Sub.
//
// This GoChannel is persistent.
// That means that when subscriber subscribes to the topic, it will receive all previously produced messages.
// All messages are persisted to the memory, so be aware that with large amount of messages you can go out of the memory.
//
// Messages are persisted per GoChannel, so you must use the same object to consume these persisted messages.
func NewPersistentGoChannel(outputChannelBuffer int64, logger watermill.LoggerAdapter) message.PubSub {
	return &GoChannel{
		outputChannelBuffer: outputChannelBuffer,

		subscribers: make(map[string][]*subscriber),
		logger: logger.With(watermill.LogFields{
			"pubsub_uuid": shortuuid.New(),
		}),

		closing: make(chan struct{}),

		persistent:        true,
		persistedMessages: map[string][]*message.Message{},
	}
}

// Publish in GoChannel is NOT blocking until all consumers consume.
// Messages will be send in background.
//
// Messages may be persisted or not, depending of persistent attribute.
func (g *GoChannel) Publish(topic string, messages ...*message.Message) error {
	if g.closed {
		return errors.New("Pub/Sub closed")
	}

	g.subscribersLock.RLock()
	defer g.subscribersLock.RUnlock()

	subLock, _ := g.subscribersByTopicLock.LoadOrStore(topic, &sync.Mutex{})
	subLock.(*sync.Mutex).Lock()
	defer subLock.(*sync.Mutex).Unlock()

	if g.persistent {
		if _, ok := g.persistedMessages[topic]; !ok {
			g.persistedMessages[topic] = make([]*message.Message, 0)
		}
		g.persistedMessages[topic] = append(g.persistedMessages[topic], messages...)
	}

	for i := range messages {
		if err := g.sendMessage(topic, messages[i]); err != nil {
			return err
		}
	}

	return nil
}

func (g *GoChannel) sendMessage(topic string, message *message.Message) error {
	subscribers := g.topicSubscribers(topic)
	if len(subscribers) == 0 {
		return nil
	}

	for i := range subscribers {
		s := subscribers[i]

		go func(subscriber *subscriber) {
			g.sendMessageToSubscriber(message, subscriber)
		}(s)
	}

	return nil
}

func (g *GoChannel) sendMessageToSubscriber(msg *message.Message, s *subscriber) {
	subscriberLogFields := watermill.LogFields{
		"message_uuid": msg.UUID,
		"pubsub_uuid":  s.uuid,
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

SendToSubscriber:
	for {
		// copy the message to prevent ack/nack propagation to other consumers
		// also allows to make retries on a fresh copy of the original message
		msgToSend := msg.Copy()
		msgToSend.SetContext(ctx)

		g.logger.Trace("Sending msg to subscriber", subscriberLogFields)

		if g.closed {
			g.logger.Info("Pub/Sub closed, discarding msg", subscriberLogFields)
			return
		}

		select {
		case s.outputChannel <- msgToSend:
			g.logger.Trace("Sent message to subscriber", subscriberLogFields)
		case <-g.closing:
			g.logger.Trace("Closing, message discarded", subscriberLogFields)
			return
		}

		select {
		case <-msgToSend.Acked():
			g.logger.Trace("Message acked", subscriberLogFields)
			return
		case <-msgToSend.Nacked():
			g.logger.Trace("Nack received, resending message", subscriberLogFields)
			continue SendToSubscriber
		case <-g.closing:
			g.logger.Trace("Closing, message discarded", subscriberLogFields)
			return
		}
	}
}

// Subscribe returns channel to which all published messages are sent.
// Messages are not persisted. If there are no subscribers and message is produced it will be gone.
//
// There are no consumer groups support etc. Every consumer will receive every produced message.
func (g *GoChannel) Subscribe(topic string) (chan *message.Message, error) {
	if g.closed {
		return nil, errors.New("Pub/Sub closed")
	}

	g.subscribersLock.Lock()

	subLock, _ := g.subscribersByTopicLock.LoadOrStore(topic, &sync.Mutex{})
	subLock.(*sync.Mutex).Lock()

	s := &subscriber{
		uuid:          uuid.NewV4().String(),
		outputChannel: make(chan *message.Message, g.outputChannelBuffer),
	}

	if !g.persistent {
		defer g.subscribersLock.Unlock()
		defer subLock.(*sync.Mutex).Unlock()

		g.addSubscriber(topic, s)

		return s.outputChannel, nil
	}

	go func(s *subscriber) {
		defer g.subscribersLock.Unlock()
		defer subLock.(*sync.Mutex).Unlock()

		if messages, ok := g.persistedMessages[topic]; ok {
			for i := range messages {
				msg := g.persistedMessages[topic][i]

				go func() {
					g.sendMessageToSubscriber(msg, s)
				}()
			}
		}

		g.addSubscriber(topic, s)
	}(s)

	return s.outputChannel, nil
}

func (g *GoChannel) addSubscriber(topic string, s *subscriber) {
	if _, ok := g.subscribers[topic]; !ok {
		g.subscribers[topic] = make([]*subscriber, 0)
	}
	g.subscribers[topic] = append(g.subscribers[topic], s)
}

func (g *GoChannel) topicSubscribers(topic string) []*subscriber {
	subscribers, ok := g.subscribers[topic]
	if !ok {
		return nil
	}

	return subscribers
}

func (g *GoChannel) Close() error {
	if g.closed {
		return nil
	}

	g.closed = true
	close(g.closing)

	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()

	g.logger.Info("Closing Pub/Sub", nil)

	for topic, topicSubscribers := range g.subscribers {
		subLock, _ := g.subscribersByTopicLock.LoadOrStore(topic, &sync.Mutex{})
		subLock.(*sync.Mutex).Lock()

		for _, subscriber := range topicSubscribers {
			g.logger.Debug("Closing subscriber channel", watermill.LogFields{
				"subscriber_uuid": subscriber.uuid,
			})
			close(subscriber.outputChannel)
		}

		subLock.(*sync.Mutex).Unlock()
	}

	g.logger.Info("Pub/Sub closed", nil)
	g.persistedMessages = nil

	return nil
}
