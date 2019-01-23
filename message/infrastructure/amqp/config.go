package amqp

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

	Exchange  ExchangeConfig
	Queue     QueueConfig
	QueueBind QueueBindConfig
	Publish   PublishConfig
	Consume   ConsumeConfig
	Qos       QosConfig
}

func NewDurablePubSubConfig(amqpURI string, generateQueueName QueueNameGenerator) Config {
	return Config{
		// todo - check default values
		AmqpURI:       amqpURI,
		RequeueOnNack: true,

		GenerateQueueName: generateQueueName,
		Marshaler:         DefaultMarshaler{},

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
		// todo - check default values
		AmqpURI:       amqpURI,
		RequeueOnNack: true,

		GenerateQueueName: GenerateQueueNameTopicName, // todo -wtf?
		Marshaler:         DefaultMarshaler{},

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

type QosConfig struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}
