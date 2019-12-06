package gochannel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// FanOut is a component that receives messages from the subscriber and passes them
// to all publishers. In effect, messages are "multiplied".
//
// A typical use case for using FanOut is having one external subscription and multiple workers
// inside the process.
//
// You need to call AddSubscription method for all topics that you want to listen to.
// This needs to be done *before* the router is started.
//
// FanOut exposes the standard Subscriber interface.
type FanOut struct {
	internalPubSub *GoChannel

	router     *message.Router
	subscriber message.Subscriber

	logger watermill.LoggerAdapter

	subscribedTopics map[string]struct{}
	subscribedLock   sync.Mutex
}

// NewFanOut creates new FanOut.
// The passed router should not be running yet.
func NewFanOut(
	router *message.Router,
	subscriber message.Subscriber,
	logger watermill.LoggerAdapter,
) (*FanOut, error) {
	if router == nil {
		return nil, errors.New("missing router")
	}
	if subscriber == nil {
		return nil, errors.New("missing subscriber")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &FanOut{
		internalPubSub: NewGoChannel(Config{}, logger),

		router:     router,
		subscriber: subscriber,

		logger: logger,

		subscribedTopics: map[string]struct{}{},
	}, nil
}

// AddSubscription add an internal subscription for the given topic.
// You need to call this method with all topics that you want to listen to, before the router is started.
// AddSubscription is idempotent.
func (f *FanOut) AddSubscription(topic string) {
	f.subscribedLock.Lock()
	defer f.subscribedLock.Unlock()

	_, ok := f.subscribedTopics[topic]
	if ok {
		// Subscription already exists
		return
	}

	f.logger.Trace("Adding fan-out subscription for topic", watermill.LogFields{
		"topic": topic,
	})

	f.router.AddHandler(
		fmt.Sprintf("fanout-%s", topic),
		topic,
		f.subscriber,
		topic,
		f.internalPubSub,
		message.PassthroughHandler,
	)

	f.subscribedTopics[topic] = struct{}{}
}

func (f *FanOut) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return f.internalPubSub.Subscribe(ctx, topic)
}

func (f *FanOut) Close() error {
	return f.internalPubSub.Close()
}
