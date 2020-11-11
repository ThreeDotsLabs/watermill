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
// This needs to be done *before* starting the FanOut.
//
// FanOut exposes the standard Subscriber interface.
type FanOut struct {
	internalPubSub *GoChannel
	internalRouter *message.Router

	subscriber message.Subscriber

	logger watermill.LoggerAdapter

	subscribedTopics map[string]struct{}
	subscribedLock   sync.Mutex
}

// NewFanOut creates a new FanOut.
func NewFanOut(
	subscriber message.Subscriber,
	logger watermill.LoggerAdapter,
) (*FanOut, error) {
	if subscriber == nil {
		return nil, errors.New("missing subscriber")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	return &FanOut{
		internalPubSub: NewGoChannel(Config{}, logger),
		internalRouter: router,

		subscriber: subscriber,

		logger: logger,

		subscribedTopics: map[string]struct{}{},
	}, nil
}

// AddSubscription add an internal subscription for the given topic.
// You need to call this method with all topics that you want to listen to, before the FanOut is started.
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

	f.internalRouter.AddHandler(
		fmt.Sprintf("fanout-%s", topic),
		topic,
		f.subscriber,
		topic,
		f.internalPubSub,
		message.PassthroughHandler,
	)

	f.subscribedTopics[topic] = struct{}{}
}

// Run runs the FanOut.
func (f *FanOut) Run(ctx context.Context) error {
	return f.internalRouter.Run(ctx)
}

// Running is closed when FanOut is running.
func (f *FanOut) Running() chan struct{} {
	return f.internalRouter.Running()
}

func (f *FanOut) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return f.internalPubSub.Subscribe(ctx, topic)
}

func (f *FanOut) Close() error {
	return f.internalPubSub.Close()
}
