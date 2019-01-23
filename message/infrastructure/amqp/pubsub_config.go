package amqp

import (
	"time"

	"github.com/cenkalti/backoff"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

type QueueNameGenerator func(topic string) string

func GenerateQueueNameTopicName(topic string) string {
	return topic
}

func GenerateQueueNameTopicNameWithSuffix(suffix string) QueueNameGenerator {
	return func(topic string) string {
		return topic + "_" + suffix
	}
}

type Config struct {
	AmqpURI       string
	RequeueOnNack bool

	GenerateQueueName QueueNameGenerator
	Marshaler         Marshaler

	Reconnect ReconnectConfig

	Exchange  ExchangeConfig
	Queue     QueueConfig
	QueueBind QueueBindConfig
	Publish   PublishConfig
	Consume   ConsumeConfig
	Qos       QosConfig
}

func (c Config) Validate() error {
	var err error

	if c.AmqpURI == "" {
		err = multierror.Append(err, errors.New("empty Config.AmqpURI"))
	}
	if c.GenerateQueueName == nil {
		err = multierror.Append(err, errors.New("missing Config.GenerateQueueName"))
	}
	if c.Marshaler == nil {
		err = multierror.Append(err, errors.New("missing Config.Marshaler"))
	}

	return err
}

func NewDurablePubSubConfig(amqpURI string, generateQueueName QueueNameGenerator) Config {
	return Config{
		AmqpURI:       amqpURI,
		RequeueOnNack: true,

		GenerateQueueName: generateQueueName,
		Marshaler:         DefaultMarshaler{},

		Reconnect: DefaultReconnectConfig(),

		Exchange: ExchangeConfig{
			Type:    "fanout",
			Durable: true,
		},
		Queue: QueueConfig{
			Durable: true,
		},
		QueueBind: QueueBindConfig{},
		Publish:   PublishConfig{},
		Consume:   ConsumeConfig{},
		Qos:       QosConfig{},
	}
}

func NewDurableQueueConfig(amqpURI string) Config {
	return Config{
		AmqpURI:       amqpURI,
		RequeueOnNack: true,

		GenerateQueueName: GenerateQueueNameTopicName, // todo -wtf?
		Marshaler:         DefaultMarshaler{},

		Reconnect: DefaultReconnectConfig(),

		Exchange: ExchangeConfig{},
		Queue: QueueConfig{
			Durable: true,
		},
		QueueBind: QueueBindConfig{},
		Publish:   PublishConfig{},
		Consume:   ConsumeConfig{},
	}
}

// todo - copy docs
type ExchangeConfig struct {
	Type        string
	Durable     bool
	AutoDeleted bool
	Internal    bool
	NoWait      bool
	Arguments   map[string]interface{}
}

func (e ExchangeConfig) UseDefaultExchange() bool {
	return e.Type == ""
}

type QueueConfig struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Arguments  map[string]interface{}
}

type QueueBindConfig struct {
	RoutingKey string
	NoWait     bool
	Arguments  map[string]interface{}
}

type PublishConfig struct {
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

type ConsumeConfig struct {
	Consumer  string
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Arguments map[string]interface{}
}

// todo - set defaults?
type QosConfig struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

type ReconnectConfig struct {
	BackoffInitialInterval     time.Duration
	BackoffRandomizationFactor float64
	BackoffMultiplier          float64
	BackoffMaxInterval         time.Duration
}

func (r ReconnectConfig) backoffConfig() *backoff.ExponentialBackOff {
	return &backoff.ExponentialBackOff{
		InitialInterval:     r.BackoffInitialInterval,
		RandomizationFactor: r.BackoffRandomizationFactor,
		Multiplier:          r.BackoffMultiplier,
		MaxInterval:         r.BackoffMaxInterval,
		MaxElapsedTime:      0, // no support for disabling reconnect, only close of Pub/Sub can stop reconnecting
		Clock:               backoff.SystemClock,
	}
}

func DefaultReconnectConfig() ReconnectConfig {
	return ReconnectConfig{
		BackoffInitialInterval:     500 * time.Millisecond,
		BackoffRandomizationFactor: 0.5,
		BackoffMultiplier:          1.5,
		BackoffMaxInterval:         60 * time.Second,
	}
}
