package gochannel

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// FanIn is a component that receives messages from 1..N topics from a subscriber and publishes them
// on a specified topic. In effect, messages are "multiplexed".
//
// You need to call AddSubscription method with a slice of topics that you want to
// listen to and a topic you want them to be published to.
// This needs to be done *before* starting the FanIn.
//
// FanIn exposes the standard Subscriber interface.
type FanIn struct {
	internalPubSub *GoChannel
	internalRouter *message.Router

	subscriber message.Subscriber

	logger watermill.LoggerAdapter

	subscriptions  map[string]struct{}
	subscribedLock sync.Mutex
}

// NewFanIn creates a new FanIn.
func NewFanIn(
	subscriber message.Subscriber,
	logger watermill.LoggerAdapter,
) (*FanIn, error) {
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

	return &FanIn{
		internalPubSub: NewGoChannel(Config{}, logger),
		internalRouter: router,
		logger:         logger,
		subscriber:     subscriber,
		subscriptions:  map[string]struct{}{},
	}, nil
}

// AddSubscription adds an internal subscriptions.
// You need to call this method with slice of `fromTopics` and `toTopic` before the FanIn is started.
// AddSubscription is idempotent.
func (f *FanIn) AddSubscription(fromTopics []string, toTopic string) {
	f.subscribedLock.Lock()
	defer f.subscribedLock.Unlock()

	if len(fromTopics) == 0 {
		return
	}

	key := createKey(fromTopics, toTopic)
	_, ok := f.subscriptions[key]
	if ok {
		// Subscription already exists
		return
	}

	f.logger.Trace("Adding fan-in subscription for topics", watermill.LogFields{
		"fromTopics": fromTopics,
		"toTopic":    toTopic,
	})

	for _, fromTopic := range fromTopics {
		f.internalRouter.AddHandler(
			fmt.Sprintf("fanin-from-%s-to-%s", fromTopic, toTopic),
			fromTopic,
			f.subscriber,
			toTopic,
			f.internalPubSub,
			message.PassthroughHandler,
		)
	}

	f.subscriptions[key] = struct{}{}
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
func (f *FanIn) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return f.internalPubSub.Subscribe(ctx, topic)
}

func (f *FanIn) Close() error {
	return f.internalPubSub.Close()
}

func createKey(fromTopics []string, toTopic string) string {
	fromTopicsMap := map[string]struct{}{}
	for _, t := range fromTopics {
		fromTopicsMap[t] = struct{}{}
	}

	deduplicatedFromTopics := make([]string, 0, len(fromTopicsMap))
	for fromTopic := range fromTopicsMap {
		deduplicatedFromTopics = append(deduplicatedFromTopics, fromTopic)
	}

	sort.Strings(deduplicatedFromTopics)

	hasher := md5.New()
	for _, topic := range deduplicatedFromTopics {
		hasher.Write([]byte(topic + " "))
	}
	hasher.Write([]byte(">" + toTopic))

	return hex.EncodeToString(hasher.Sum(nil))
}
