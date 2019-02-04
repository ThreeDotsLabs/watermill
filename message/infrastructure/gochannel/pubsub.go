package gochannel

import (
	"context"
	"sync"
	"time"

	"github.com/renstrom/shortuuid"

	"github.com/hashicorp/go-multierror"

	"github.com/pkg/errors"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const noTimeout time.Duration = -1

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
	sendTimeout         time.Duration
	outputChannelBuffer int64

	subscribers            map[string][]*subscriber
	subscribersLock        sync.RWMutex
	subscribersByTopicLock sync.Map // map of *sync.RWMutex, todo - change to mutex?

	logger watermill.LoggerAdapter

	closed  bool
	closing chan struct{}

	persistent bool
	messages   map[string][]*message.Message
}

func (g *GoChannel) Publisher() message.Publisher {
	return g
}

func (g *GoChannel) Subscriber() message.Subscriber {
	return g
}

func NewGoChannel(outputChannelBuffer int64, logger watermill.LoggerAdapter, sendTimeout time.Duration) message.PubSub {
	return &GoChannel{
		sendTimeout:         sendTimeout,
		outputChannelBuffer: outputChannelBuffer,

		subscribers:            make(map[string][]*subscriber),
		subscribersByTopicLock: sync.Map{},
		logger: logger.With(watermill.LogFields{
			"subscriber_uuid": shortuuid.New(),
		}),

		closing: make(chan struct{}),
	}
}

func NewPersistentGoChannel(outputChannelBuffer int64, logger watermill.LoggerAdapter, sendTimeout time.Duration) message.PubSub {
	return &GoChannel{
		sendTimeout:         sendTimeout,
		outputChannelBuffer: outputChannelBuffer,

		subscribers: make(map[string][]*subscriber),
		logger: logger.With(watermill.LogFields{
			"subscriber_uuid": shortuuid.New(),
		}),

		closing: make(chan struct{}),

		persistent: true,
		messages:   map[string][]*message.Message{},
	}
}

// Publish in GoChannel is blocking until all consumers consume and acknowledge the message.
// Sending message to one subscriber has timeout equal to GoChannel.sendTimeout configured via constructor.
//
// Messages are not persisted. If there are no subscribers and message is produced it will be gone.
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
		// todo - to func

		if _, ok := g.messages[topic]; !ok {
			g.messages[topic] = make([]*message.Message, 0)
		}
		g.messages[topic] = append(g.messages[topic], messages...)

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

	subscribersWg := sync.WaitGroup{}
	subscribersWg.Add(len(subscribers))

	sendErrs := make(chan error, len(subscribers))

	for i := range subscribers {
		s := subscribers[i]

		go func(subscriber *subscriber, sendErrs chan<- error) {
			if err := g.sendMessageToSubscriber(message, subscriber, g.sendTimeout); err != nil {
				sendErrs <- err
			}
			subscribersWg.Done()
		}(s, sendErrs)
	}

	subscribersWg.Wait()
	close(sendErrs)

	var err error
	for sendErr := range sendErrs {
		err = multierror.Append(err, sendErr)
	}
	return err
}

func (g *GoChannel) sendMessageToSubscriber(msg *message.Message, s *subscriber, sendTimeout time.Duration) error {
	subscriberLogFields := watermill.LogFields{
		"message_uuid":    msg.UUID,
		"subscriber_uuid": s.uuid,
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

SendToSubscriber:
	for {
		// copy the message to prevent ack/nack propagation to other consumers
		// also allows to make retries on a fresh copy of the original message
		msgToSend := msg.Copy()

		g.logger.Trace("Sending msg to subscriber", subscriberLogFields)

		msgToSend.SetContext(ctx)

		var timeout <-chan time.Time
		if sendTimeout != noTimeout {
			timeout = time.After(sendTimeout)
		} else {
			timeout = make(<-chan time.Time)
		}

		select {
		case s.outputChannel <- msgToSend:
			g.logger.Trace("Sent message to subscriber", subscriberLogFields)
		case <-timeout:
			return errors.Errorf("Sending message %s timeouted after %s", msgToSend.UUID, sendTimeout)
		case <-g.closing:
			g.logger.Trace("Closing, message discarded", subscriberLogFields)
			return nil
		}

		select {
		case <-msgToSend.Acked():
			g.logger.Trace("Message acked", subscriberLogFields)
			return nil
		case <-msgToSend.Nacked():
			g.logger.Trace("Nack received, resending message", subscriberLogFields)
			continue SendToSubscriber
		case <-timeout:
			return errors.Errorf("Sending ACK for msg %s timeouted after %s", msgToSend.UUID, sendTimeout)
		case <-g.closing:
			g.logger.Trace("Closing, message discarded", subscriberLogFields)
			return nil
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

		// todo - move to func
		g.addSubscriber(topic, s)

		return s.outputChannel, nil
	}

	go func(s *subscriber) {
		defer g.subscribersLock.Unlock()
		defer subLock.(*sync.Mutex).Unlock()

		if messages, ok := g.messages[topic]; ok {
			for i := range messages {
				msg := g.messages[topic][i]

				// todo - remove? refactor? move to send msg?-
				g.logger.Trace("Sending msg to new consumer", watermill.LogFields{
					"message_uuid": msg.UUID,
					"subscriber":   s.uuid,
				})

				if err := g.sendMessageToSubscriber(msg, s, noTimeout); err != nil {
					panic(err)
				}
			}
		}

		// todo - remove comment?
		// todo - test out of order when publishing message during resending
		// todo - better docs

		// ensuring that there is no race condition between add to subscribers and read from g.messages
		// for that reason we lock first, then unlock

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

	close(g.closing)

	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()

	g.logger.Info("Closing Pub/Sub", nil)

	g.closed = true

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

	return nil
}
