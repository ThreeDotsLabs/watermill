package gochannel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// FanIn is a component that receives messages from the subscriber and publishes them
// on a specified topic. In effect, messages are "multiplexed".
//
// A typical use case for using FanIn is having events from many subscriptions published on one topic.
//
// You need to call AddSubscription method for all topics that you want to listen to.
// This needs to be done *before* starting the FanIn.
//
// FanIn exposes the standard Subscriber interface, however events are published
// on the topic defined in the constructor. The `topic` value provided when calling Subscribe is ignored.
type FanIn struct {
	internalPubSub *GoChannel
	internalRouter *message.Router

	subscriber message.Subscriber

	logger watermill.LoggerAdapter

	subscribedTopics map[string]struct{}
	subscribedLock   sync.Mutex

	publisherTopic string
}

// NewFanIn creates a new FanIn.
func NewFanIn(
	subscriber message.Subscriber,
	publisherTopic string,
	logger watermill.LoggerAdapter,
) (*FanIn, error) {
	if subscriber == nil {
		return nil, errors.New("missing subscriber")
	}
	if publisherTopic == "" {
		return nil, errors.New("missing publisher topic")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	return &FanIn{
		internalPubSub: NewGoChannel(Config{}, logger),
		internalRouter: router,

		subscriber: subscriber,

		logger: logger,

		subscribedTopics: map[string]struct{}{},
		publisherTopic:   publisherTopic,
	}, nil
}

// AddSubscription add an internal subscription for the given topic.
// You need to call this method with all topics that you want to listen to, before the FanIn is started.
// AddSubscription is idempotent.
func (f *FanIn) AddSubscription(topic string) {
	f.subscribedLock.Lock()
	defer f.subscribedLock.Unlock()

	_, ok := f.subscribedTopics[topic]
	if ok {
		// Subscription already exists
		return
	}

	f.logger.Trace("Adding fan-in subscription for topic", watermill.LogFields{
		"topic": topic,
	})

	f.internalRouter.AddHandler(
		fmt.Sprintf("fanin-%s", topic),
		topic,
		f.subscriber,
		f.publisherTopic,
		f.internalPubSub,
		message.PassthroughHandler,
	)

	f.subscribedTopics[topic] = struct{}{}
}

// Run runs the FanIn.
func (f *FanIn) Run(ctx context.Context) error {
	return f.internalRouter.Run(ctx)
}

// Running is closed when FanIn is running.
func (f *FanIn) Running() chan struct{} {
	return f.internalRouter.Running()
}

// Subscribe implements the standard Subscriber interface.
// The topic argument is ignored
func (f *FanIn) Subscribe(ctx context.Context, _ string) (<-chan *message.Message, error) {
	return f.internalPubSub.Subscribe(ctx, f.publisherTopic)
}

func (f *FanIn) Close() error {
	return f.internalPubSub.Close()
}
