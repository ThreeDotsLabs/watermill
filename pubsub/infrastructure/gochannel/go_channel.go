package gochannel

import (
	"github.com/satori/go.uuid"
	"context"
	"sync"
	"time"
	"github.com/roblaszczak/gooddd/pubsub"
	"fmt"
)

// todo - graceful shutdown (with context?)
// todo - unittests!!!
type subscriber struct {
	uuid          string
	name          string // todo - reuse?
	outputChannel chan pubsub.Message

	ctx context.Context // todo - reuse?
}

func newSubscriber() *subscriber {
	return &subscriber{uuid: uuid.NewV1().String(), outputChannel: make(chan pubsub.Message)}
}

//
//func (s subscriber) String() string {
//	return fmt.Sprintf("{PubSub subscriber, name: %s uuid: %s}", s.name, s.uuid)
//}
//
//func subscriberLogContext(s subscriber) []zapcore.Field {
//	fields := []zapcore.Field{
//		zap.String("subscriber", s.String()),
//	}
//	fields = append(fields, log.ContextFields(s.ctx)...)
//
//	return fields
//}

// todo - rename to broker?
type PubSub struct {
	subscribers     map[string][]*subscriber
	subscribersLock *sync.RWMutex

	id string // todo - reenable?

	//logger *zap.Logger
}

func NewPubSub() *PubSub {
	return &PubSub{subscribersLock: &sync.RWMutex{}, subscribers: map[string][]*subscriber{}}
}

func (p PubSub) Publish(topic string, msgs ...pubsub.MessagePayload) error {
	// todo - may curse race condition if client should be closed during sending messages, fix (and in other places)
	p.subscribersLock.RLock()
	defer p.subscribersLock.RUnlock()

	//logger := p.logger.With(
	//	zap.Int("subscribers_count", len(p.subscribers)),
	//	zap.String("data", fmt.Sprintf("%s", data)),
	//)
	//
	//logger.Debug("sending message to subscribers")

	// todo - test order cross subscribers
	for _, msgPayload := range msgs {
		msg := pubsub.NewMessage(msgPayload)
		fmt.Println("created", msg)

		for _, s := range p.subscribers[topic] {
			//// todo - support for close (switch with close channel)
			//sendLogger := logger.
			//	With(zap.String("subscriber", s.String())).
			//	With(subscriberLogContext(*s)...)
			//
			//// todo - add subscriber context
			//sendLogger.Debug("sending message to subscriber")

			select {
			case s.outputChannel <- msg:
				//sendLogger.Debug("sent message to subscriber")
				fmt.Println("sending", msg)
			case <-time.After(time.Second): // todo - config it? oor disable?
				// todo - remove or refactor
				//sendLogger.Warn("cannot send message")
			}
		}
	}

	//logger.Info("sent message to subscribers")
	return nil
}

// todo - thread safe
// todo - unsubscribe
// todo - groups support?
func (p *PubSub) Subscribe(topic string) (chan pubsub.Message, error) {
	p.subscribersLock.Lock()
	defer p.subscribersLock.Unlock()

	if _, ok := p.subscribers[topic]; !ok {
		p.subscribers[topic] = []*subscriber{}
	}

	subscriber := newSubscriber()
	p.subscribers[topic] = append(p.subscribers[topic], subscriber)

	//go func() {
	//	<-ctx.Done()
	//	p.unsubscribe(subscriber)
	//}()

	//p.logger.With(subscriberLogContext(*subscriber)...).Info("added subscriber to pubsub")
	fmt.Println("added subscriber")


	return subscriber.outputChannel, nil
}

func (p *PubSub) Close() error {
	// todo
	return nil
}

//func (p *PubSub) unsubscribe(s *subscriber) {
//	p.subscribersLock.Lock()
//	defer p.subscribersLock.Unlock()
//
//	for i, subscriber := range p.subscribers {
//		if subscriber == s {
//			close(s.outputChannel) //  todo - move to subscriber method
//			p.subscribers = append(p.subscribers[:i], p.subscribers[i+1:]...)
//
//			p.logger.With(subscriberLogContext(*subscriber)...).Info("removed subscriber from pubsub")
//			return
//		}
//	}
//
//	// todo - error?
//}
