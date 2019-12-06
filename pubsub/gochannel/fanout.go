package gochannel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type FanOut struct {
	pubSub *GoChannel

	internalRouter     *message.Router
	internalSubscriber message.Subscriber

	logger watermill.LoggerAdapter

	subscribedTopics map[string]struct{}
	subscribedLock   sync.Mutex
}

func NewFanOut(
	router *message.Router,
	subscriber message.Subscriber,
	logger watermill.LoggerAdapter,
) (FanOut, error) {
	if router == nil {
		return FanOut{}, errors.New("missing router")
	}
	if subscriber == nil {
		return FanOut{}, errors.New("missing subscriber")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return FanOut{
		pubSub: NewGoChannel(Config{}, logger),

		internalRouter:     router,
		internalSubscriber: subscriber,

		logger: logger,

		subscribedTopics: map[string]struct{}{},
	}, nil
}

func (f FanOut) AddSubscription(topic string) {
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
		f.internalSubscriber,
		topic,
		f.pubSub,
		message.PassthroughHandler,
	)

	f.subscribedTopics[topic] = struct{}{}
}

func (f FanOut) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return f.pubSub.Subscribe(ctx, topic)
}

func (f FanOut) Close() error {
	return f.pubSub.Close()
}
