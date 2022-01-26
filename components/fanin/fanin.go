package fanin

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Config struct {
	// SourceTopics contains topics on which FanIn subscribes.
	SourceTopics []string

	// TargetTopic determines the topic on which messages from SourceTopics are published.
	TargetTopic string

	// CloseTimeout determines how long router should work for handlers when closing.
	CloseTimeout time.Duration
}

// FanIn is a component that receives messages from 1..N topics from a subscriber and publishes them
// on a specified topic in the publisher. In effect, messages are "multiplexed".
type FanIn struct {
	router    *message.Router
	config    Config
	logger    watermill.LoggerAdapter
}

func (c *Config) setDefaults() {
	if c.CloseTimeout == 0 {
		c.CloseTimeout = time.Second * 30
	}
}

func (c *Config) Validate() error {
	if len(c.SourceTopics) == 0 {
		return errors.New("sourceTopics must not be empty")
	}

	for _, fromTopic := range c.SourceTopics {
		if fromTopic == "" {
			return errors.New("sourceTopics must not be empty")
		}
	}

	if c.TargetTopic == "" {
		return errors.New("targetTopic must not be empty")
	}

	for _, fromTopic := range c.SourceTopics {
		if fromTopic == c.TargetTopic {
			return errors.New("sourceTopics must not contain targetTopic")
		}
	}

	return nil
}

// NewFanIn creates a new FanIn.
func NewFanIn(
	subscriber message.Subscriber,
	publisher message.Publisher,
	config Config,
	logger watermill.LoggerAdapter,
) (*FanIn, error) {
	if subscriber == nil {
		return nil, errors.New("missing subscriber")
	}
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}

	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	routerConfig := message.RouterConfig{CloseTimeout: config.CloseTimeout}
	if err := routerConfig.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid router config")
	}

	router, err := message.NewRouter(routerConfig, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create a router")
	}

	for _, topic := range config.SourceTopics {
		router.AddHandler(
			fmt.Sprintf("fan_in_%s", topic),
			topic,
			subscriber,
			config.TargetTopic,
			publisher,
			func(msg *message.Message) ([]*message.Message, error) {
				return []*message.Message{msg}, nil
			},
		)
	}

	return &FanIn{
		router:    router,
		config:    config,
		logger:    logger,
	}, nil
}

// Run runs the FanIn.
func (f *FanIn) Run(ctx context.Context) error {
	return f.router.Run(ctx)
}

// Running is closed when FanIn is running.
func (f *FanIn) Running() chan struct{} {
	return f.router.Running()
}

// Close gracefully closes the FanIn
func (f *FanIn) Close() error {
	return f.router.Close()
}
