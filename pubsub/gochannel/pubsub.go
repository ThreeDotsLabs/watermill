package gochannel

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Config struct {
	// Output channel buffer size.
	OutputChannelBuffer int64

	// If persistent is set to true, when subscriber subscribes to the topic,
	// it will receive all previously produced messages.
	//
	// All messages are persisted to the memory (simple slice),
	// so be aware that with large amount of messages you can go out of the memory.
	Persistent bool

	// When true, Publish will block until subscriber Ack's the message.
	// If there are no subscribers, Publish will not block (also when Persistent is true).
	BlockPublishUntilSubscriberAck bool
}

// GoChannel is the simplest Pub/Sub implementation.
// It is based on Golang's channels which are sent within the process.
//
// GoChannel has no global state,
// that means that you need to use the same instance for Publishing and Subscribing!
//
// When GoChannel is persistent, messages order is not guaranteed.
type GoChannel struct {
	config Config
	logger watermill.LoggerAdapter

	subscribersWg          sync.WaitGroup
	subscribers            map[string][]*subscriber
	subscribersLock        sync.RWMutex
	subscribersByTopicLock sync.Map // map of *sync.Mutex

	closed     bool
	closedLock sync.Mutex
	closing    chan struct{}

	persistedMessages     map[string][]*message.Message
	persistedMessagesLock sync.RWMutex
}

// NewGoChannel creates new GoChannel Pub/Sub.
//
// This GoChannel is not persistent.
// That means if you send a message to a topic to which no subscriber is subscribed, that message will be discarded.
func NewGoChannel(config Config, logger watermill.LoggerAdapter) *GoChannel {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &GoChannel{
		config: config,

		subscribers:            make(map[string][]*subscriber),
		subscribersByTopicLock: sync.Map{},
		logger: logger.With(watermill.LogFields{
			"pubsub_uuid": shortuuid.New(),
		}),

		closing: make(chan struct{}),

		persistedMessages: map[string][]*message.Message{},
	}
}

// Publish in GoChannel is NOT blocking until all consumers consume.
// Messages will be send in background.
//
// Messages may be persisted or not, depending of persistent attribute.
func (g *GoChannel) Publish(topic string, messages ...*message.Message) error {
	if g.isClosed() {
		return errors.New("Pub/Sub closed")
	}

	for i, msg := range messages {
		messages[i] = msg.Copy()
	}

	g.subscribersLock.RLock()
	defer g.subscribersLock.RUnlock()

	subLock, _ := g.subscribersByTopicLock.LoadOrStore(topic, &sync.Mutex{})
	subLock.(*sync.Mutex).Lock()
	defer subLock.(*sync.Mutex).Unlock()

	if g.config.Persistent {
		g.persistedMessagesLock.Lock()
		if _, ok := g.persistedMessages[topic]; !ok {
			g.persistedMessages[topic] = make([]*message.Message, 0)
		}
		g.persistedMessages[topic] = append(g.persistedMessages[topic], messages...)
		g.persistedMessagesLock.Unlock()
	}

	for i := range messages {
		msg := messages[i]

		ackedBySubscribers, err := g.sendMessage(topic, msg)
		if err != nil {
			return err
		}

		if g.config.BlockPublishUntilSubscriberAck {
			g.waitForAckFromSubscribers(msg, ackedBySubscribers)
		}
	}

	return nil
}

func (g *GoChannel) waitForAckFromSubscribers(msg *message.Message, ackedByConsumer <-chan struct{}) {
	logFields := watermill.LogFields{"message_uuid": msg.UUID}
	g.logger.Debug("Waiting for subscribers ack", logFields)

	select {
	case <-ackedByConsumer:
		g.logger.Trace("Message acked by subscribers", logFields)
	case <-g.closing:
		g.logger.Trace("Closing Pub/Sub before ack from subscribers", logFields)
	}
}

func (g *GoChannel) sendMessage(topic string, message *message.Message) (<-chan struct{}, error) {
	subscribers := g.topicSubscribers(topic)
	ackedBySubscribers := make(chan struct{})

	logFields := watermill.LogFields{"message_uuid": message.UUID, "topic": topic}

	if len(subscribers) == 0 {
		close(ackedBySubscribers)
		g.logger.Info("No subscribers to send message", logFields)
		return ackedBySubscribers, nil
	}

	go func(subscribers []*subscriber) {
		for i := range subscribers {
			subscriber := subscribers[i]
			subscriber.sendMessageToSubscriber(message, logFields)
		}
		close(ackedBySubscribers)
	}(subscribers)

	return ackedBySubscribers, nil
}

// Subscribe returns channel to which all published messages are sent.
// Messages are not persisted. If there are no subscribers and message is produced it will be gone.
//
// There are no consumer groups support etc. Every consumer will receive every produced message.
func (g *GoChannel) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	g.closedLock.Lock()

	if g.closed {
		return nil, errors.New("Pub/Sub closed")
	}

	g.subscribersWg.Add(1)
	g.closedLock.Unlock()

	g.subscribersLock.Lock()

	subLock, _ := g.subscribersByTopicLock.LoadOrStore(topic, &sync.Mutex{})
	subLock.(*sync.Mutex).Lock()

	s := &subscriber{
		ctx:           ctx,
		uuid:          watermill.NewUUID(),
		outputChannel: make(chan *message.Message, g.config.OutputChannelBuffer),
		logger:        g.logger,
		closing:       make(chan struct{}),
	}

	go func(s *subscriber, g *GoChannel) {
		select {
		case <-ctx.Done():
			// unblock
		case <-g.closing:
			// unblock
		}

		s.Close()

		g.subscribersLock.Lock()
		defer g.subscribersLock.Unlock()

		subLock, _ := g.subscribersByTopicLock.Load(topic)
		subLock.(*sync.Mutex).Lock()
		defer subLock.(*sync.Mutex).Unlock()

		g.removeSubscriber(topic, s)
		g.subscribersWg.Done()
	}(s, g)

	if !g.config.Persistent {
		defer g.subscribersLock.Unlock()
		defer subLock.(*sync.Mutex).Unlock()

		g.addSubscriber(topic, s)

		return s.outputChannel, nil
	}

	go func(s *subscriber) {
		defer g.subscribersLock.Unlock()
		defer subLock.(*sync.Mutex).Unlock()

		g.persistedMessagesLock.RLock()
		messages, ok := g.persistedMessages[topic]
		g.persistedMessagesLock.RUnlock()

		if ok {
			for i := range messages {
				msg := g.persistedMessages[topic][i]
				logFields := watermill.LogFields{"message_uuid": msg.UUID, "topic": topic}

				go s.sendMessageToSubscriber(msg, logFields)
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

func (g *GoChannel) removeSubscriber(topic string, toRemove *subscriber) {
	removed := false
	for i, sub := range g.subscribers[topic] {
		if sub == toRemove {
			g.subscribers[topic] = append(g.subscribers[topic][:i], g.subscribers[topic][i+1:]...)
			removed = true
			break
		}
	}
	if !removed {
		panic("cannot remove subscriber, not found " + toRemove.uuid)
	}
}

func (g *GoChannel) topicSubscribers(topic string) []*subscriber {
	subscribers, ok := g.subscribers[topic]
	if !ok {
		return nil
	}

	return subscribers
}

func (g *GoChannel) isClosed() bool {
	g.closedLock.Lock()
	defer g.closedLock.Unlock()

	return g.closed
}

func (g *GoChannel) Close() error {
	g.closedLock.Lock()
	defer g.closedLock.Unlock()

	if g.closed {
		return nil
	}

	g.closed = true
	close(g.closing)

	g.logger.Debug("Closing Pub/Sub, waiting for subscribers", nil)
	g.subscribersWg.Wait()

	g.logger.Info("Pub/Sub closed", nil)
	g.persistedMessages = nil

	return nil
}

type subscriber struct {
	ctx context.Context

	uuid string

	sending       sync.Mutex
	outputChannel chan *message.Message

	logger  watermill.LoggerAdapter
	closed  bool
	closing chan struct{}
}

func (s *subscriber) Close() {
	if s.closed {
		return
	}
	close(s.closing)

	s.logger.Debug("Closing subscriber, waiting for sending lock", nil)

	// ensuring that we are not sending to closed channel
	s.sending.Lock()
	defer s.sending.Unlock()

	s.logger.Debug("GoChannel Pub/Sub Subscriber closed", nil)
	s.closed = true

	close(s.outputChannel)
}

func (s *subscriber) sendMessageToSubscriber(msg *message.Message, logFields watermill.LogFields) {
	s.sending.Lock()
	defer s.sending.Unlock()

	ctx, cancelCtx := context.WithCancel(s.ctx)
	defer cancelCtx()

SendToSubscriber:
	for {
		// copy the message to prevent ack/nack propagation to other consumers
		// also allows to make retries on a fresh copy of the original message
		msgToSend := msg.Copy()
		msgToSend.SetContext(ctx)

		s.logger.Trace("Sending msg to subscriber", logFields)

		if s.closed {
			s.logger.Info("Pub/Sub closed, discarding msg", logFields)
			return
		}

		select {
		case s.outputChannel <- msgToSend:
			s.logger.Trace("Sent message to subscriber", logFields)
		case <-s.closing:
			s.logger.Trace("Closing, message discarded", logFields)
			return
		}

		select {
		case <-msgToSend.Acked():
			s.logger.Trace("Message acked", logFields)
			return
		case <-msgToSend.Nacked():
			s.logger.Trace("Nack received, resending message", logFields)
			continue SendToSubscriber
		case <-s.closing:
			s.logger.Trace("Closing, message discarded", logFields)
			return
		}
	}
}
