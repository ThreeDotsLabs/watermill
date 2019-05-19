package amqp

import (
	"crypto/tls"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/streadway/amqp"

	"github.com/cenkalti/backoff"

	"github.com/pkg/errors"
)

// NewDurablePubSubConfig creates config for durable PubSub.
// generateQueueName is optional, when passing to the publisher.
// Exchange name is set to the topic name and routing key is empty.
//
// IMPORTANT: Watermill's topic is not mapped directly to the AMQP's topic exchange type.
// It is used to generate exchange name, routing key and queue name, depending on the context.
// To check how topic is mapped, please check Exchange.GenerateName, Queue.GenerateName and Publish.GenerateRoutingKey.
//
// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-three-go.html
// with durable added for exchange, queue and amqp.Persistent DeliveryMode.
// Thanks to this, we don't lose messages on broker restart.
func NewDurablePubSubConfig(amqpURI string, generateQueueName QueueNameGenerator) Config {
	return Config{
		Connection: ConnectionConfig{
			AmqpURI: amqpURI,
		},

		Marshaler: DefaultMarshaler{},

		Exchange: ExchangeConfig{
			GenerateName: func(topic string) string {
				return topic
			},
			Type:    "fanout",
			Durable: true,
		},
		Queue: QueueConfig{
			GenerateName: generateQueueName,
			Durable:      true,
		},
		QueueBind: QueueBindConfig{
			GenerateRoutingKey: func(topic string) string {
				return ""
			},
		},
		Publish: PublishConfig{
			GenerateRoutingKey: func(topic string) string {
				return ""
			},
		},
		Consume: ConsumeConfig{
			Qos: QosConfig{
				PrefetchCount: 1,
			},
		},
		TopologyBuilder: &DefaultTopologyBuilder{},
	}
}

// NewNonDurablePubSubConfig creates config for non durable PubSub.
// generateQueueName is optional, when passing to the publisher.
// Exchange name is set to the topic name and routing key is empty.
//
// IMPORTANT: Watermill's topic is not mapped directly to the AMQP's topic exchange type.
// It is used to generate exchange name, routing key and queue name, depending on the context.
// To check how topic is mapped, please check Exchange.GenerateName, Queue.GenerateName and Publish.GenerateRoutingKey.
//
// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-three-go.html.
// This config is not durable, so on the restart of the broker all messages will be lost.
func NewNonDurablePubSubConfig(amqpURI string, generateQueueName QueueNameGenerator) Config {
	return Config{
		Connection: ConnectionConfig{
			AmqpURI: amqpURI,
		},

		Marshaler: DefaultMarshaler{NotPersistentDeliveryMode: true},

		Exchange: ExchangeConfig{
			GenerateName: func(topic string) string {
				return topic
			},
			Type: "fanout",
		},
		Queue: QueueConfig{
			GenerateName: generateQueueName,
		},
		QueueBind: QueueBindConfig{
			GenerateRoutingKey: func(topic string) string {
				return ""
			},
		},
		Publish: PublishConfig{
			GenerateRoutingKey: func(topic string) string {
				return ""
			},
		},
		Consume: ConsumeConfig{
			Qos: QosConfig{
				PrefetchCount: 1,
			},
		},
		TopologyBuilder: &DefaultTopologyBuilder{},
	}
}

// NewDurableQueueConfig creates config for durable Queue.
// Queue name and routing key is set to the topic name by default. Default ("") exchange is used.
//
// IMPORTANT: Watermill's topic is not mapped directly to the AMQP's topic exchange type.
// It is used to generate exchange name, routing key and queue name, depending on the context.
// To check how topic is mapped, please check Exchange.GenerateName, Queue.GenerateName and Publish.GenerateRoutingKey.
//
// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-two-go.html
// with durable added for exchange, queue and amqp.Persistent DeliveryMode.
// Thanks to this, we don't lose messages on broker restart.
func NewDurableQueueConfig(amqpURI string) Config {
	return Config{
		Connection: ConnectionConfig{
			AmqpURI: amqpURI,
		},

		Marshaler: DefaultMarshaler{},

		Exchange: ExchangeConfig{
			GenerateName: func(topic string) string {
				return ""
			},
		},
		Queue: QueueConfig{
			GenerateName: GenerateQueueNameTopicName,
			Durable:      true,
		},
		QueueBind: QueueBindConfig{
			GenerateRoutingKey: func(topic string) string {
				return ""
			},
		},
		Publish: PublishConfig{
			GenerateRoutingKey: func(topic string) string {
				return topic
			},
		},
		Consume: ConsumeConfig{
			Qos: QosConfig{
				PrefetchCount: 1,
			},
		},
		TopologyBuilder: &DefaultTopologyBuilder{},
	}
}

// NewNonDurableQueueConfig creates config for non durable Queue.
// Queue name and routing key is set to the topic name by default. Default ("") exchange is used.
//
// IMPORTANT: Watermill's topic is not mapped directly to the AMQP's topic exchange type.
// It is used to generate exchange name, routing key and queue name, depending on the context.
// To check how topic is mapped, please check Exchange.GenerateName, Queue.GenerateName and Publish.GenerateRoutingKey.
//
// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-two-go.html.
// This config is not durable, so on the restart of the broker all messages will be lost.
func NewNonDurableQueueConfig(amqpURI string) Config {
	return Config{
		Connection: ConnectionConfig{
			AmqpURI: amqpURI,
		},

		Marshaler: DefaultMarshaler{NotPersistentDeliveryMode: true},

		Exchange: ExchangeConfig{
			GenerateName: func(topic string) string {
				return ""
			},
		},
		Queue: QueueConfig{
			GenerateName: GenerateQueueNameTopicName,
		},
		QueueBind: QueueBindConfig{
			GenerateRoutingKey: func(topic string) string {
				return ""
			},
		},
		Publish: PublishConfig{
			GenerateRoutingKey: func(topic string) string {
				return topic
			},
		},
		Consume: ConsumeConfig{
			Qos: QosConfig{
				PrefetchCount: 1,
			},
		},
		TopologyBuilder: &DefaultTopologyBuilder{},
	}
}

type Config struct {
	Connection ConnectionConfig

	Marshaler Marshaler

	Exchange  ExchangeConfig
	Queue     QueueConfig
	QueueBind QueueBindConfig

	Publish PublishConfig
	Consume ConsumeConfig

	TopologyBuilder TopologyBuilder
}

func (c Config) validate() error {
	var err error

	if c.Connection.AmqpURI == "" {
		err = multierror.Append(err, errors.New("empty Config.AmqpURI"))
	}
	if c.Marshaler == nil {
		err = multierror.Append(err, errors.New("missing Config.Marshaler"))
	}
	if c.Exchange.GenerateName == nil {
		err = multierror.Append(err, errors.New("missing Config.GenerateName"))
	}

	return err
}

func (c Config) ValidatePublisher() error {
	err := c.validate()

	if c.Publish.GenerateRoutingKey == nil {
		err = multierror.Append(err, errors.New("missing Config.GenerateRoutingKey"))
	}

	return err
}

func (c Config) ValidateSubscriber() error {
	err := c.validate()

	if c.Queue.GenerateName == nil {
		err = multierror.Append(err, errors.New("missing Config.Queue.GenerateName"))
	}

	return err
}

type ConnectionConfig struct {
	AmqpURI string

	TLSConfig  *tls.Config
	AmqpConfig *amqp.Config

	Reconnect *ReconnectConfig
}

// Config descriptions are based on descriptions from: https://github.com/streadway/amqp
// Copyright (c) 2012, Sean Treadway, SoundCloud Ltd.
// BSD 2-Clause "Simplified" License

type ExchangeConfig struct {
	// GenerateName is generated based on the topic provided for Publish or Subscribe method.
	//
	// Exchange names starting with "amq." are reserved for pre-declared and
	// standardized exchanges. The client MAY declare an exchange starting with
	// "amq." if the passive option is set, or the exchange already exists.  Names can
	// consist of a non-empty sequence of letters, digits, hyphen, underscore,
	// period, or colon.
	GenerateName func(topic string) string

	// Each exchange belongs to one of a set of exchange kinds/types implemented by
	// the server. The exchange types define the functionality of the exchange - i.e.
	// how messages are routed through it. Once an exchange is declared, its type
	// cannot be changed.  The common types are "direct", "fanout", "topic" and
	// "headers".
	Type string

	// Durable and Non-Auto-Deleted exchanges will survive server restarts and remain
	// declared when there are no remaining bindings.  This is the best lifetime for
	// long-lived exchange configurations like stable routes and default exchanges.
	Durable bool

	// Non-Durable and Auto-Deleted exchanges will be deleted when there are no
	// remaining bindings and not restored on server restart.  This lifetime is
	// useful for temporary topologies that should not pollute the virtual host on
	// failure or after the consumers have completed.
	//
	// Non-Durable and Non-Auto-deleted exchanges will remain as long as the server is
	// running including when there are no remaining bindings.  This is useful for
	// temporary topologies that may have long delays between bindings.
	//
	AutoDeleted bool

	// Exchanges declared as `internal` do not accept accept publishings. Internal
	// exchanges are useful when you wish to implement inter-exchange topologies
	// that should not be exposed to users of the broker.
	Internal bool

	// When noWait is true, declare without waiting for a confirmation from the server.
	// The channel may be closed as a result of an error.  Add a NotifyClose listener-
	// to respond to any exceptions.
	NoWait bool

	// Optional amqp.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters.
	Arguments amqp.Table
}

// QueueNameGenerator generates QueueName based on the topic.
type QueueNameGenerator func(topic string) string

// GenerateQueueNameTopicName generates queueName equal to the topic.
func GenerateQueueNameTopicName(topic string) string {
	return topic
}

// GenerateQueueNameConstant generate queue name equal to queueName.
func GenerateQueueNameConstant(queueName string) QueueNameGenerator {
	return func(topic string) string {
		return queueName
	}
}

// GenerateQueueNameTopicNameWithSuffix generates queue name equal to:
// 	topic + "_" + suffix
func GenerateQueueNameTopicNameWithSuffix(suffix string) QueueNameGenerator {
	return func(topic string) string {
		return topic + "_" + suffix
	}
}

type QueueConfig struct {
	// GenerateRoutingKey is generated based on the topic provided for Subscribe.
	GenerateName QueueNameGenerator

	// Durable and Non-Auto-Deleted queues will survive server restarts and remain
	// when there are no remaining consumers or bindings.  Persistent publishings will
	// be restored in this queue on server restart.  These queues are only able to be
	// bound to durable exchanges.
	Durable bool

	// Non-Durable and Auto-Deleted exchanges will be deleted when there are no
	// remaining bindings and not restored on server restart.  This lifetime is
	// useful for temporary topologies that should not pollute the virtual host on
	// failure or after the consumers have completed.
	//
	// Non-Durable and Non-Auto-deleted exchanges will remain as long as the server is
	// running including when there are no remaining bindings.  This is useful for
	// temporary topologies that may have long delays between bindings.
	AutoDelete bool

	// Exclusive queues are only accessible by the connection that declares them and
	// will be deleted when the connection closes.  Channels on other connections
	// will receive an error when attempting  to declare, bind, consume, purge or
	// delete a queue with the same name.
	Exclusive bool

	// When noWait is true, the queue will assume to be declared on the server.  A
	// channel exception will arrive if the conditions are met for existing queues
	// or attempting to modify an existing queue from a different connection.
	NoWait bool

	// Optional amqpe.Table of arguments that are specific to the server's implementation of
	// the queue can be sent for queue types that require extra parameters.
	Arguments amqp.Table
}

// QueueBind binds an exchange to a queue so that publishings to the exchange will
// be routed to the queue when the publishing routing key matches the binding
// routing key.
type QueueBindConfig struct {
	GenerateRoutingKey func(topic string) string

	// When noWait is false and the queue could not be bound, the channel will be
	// closed with an error.
	NoWait bool

	// Optional amqpe.Table of arguments that are specific to the server's implementation of
	// the queue bind can be sent for queue bind types that require extra parameters.
	Arguments amqp.Table
}

type PublishConfig struct {
	// GenerateRoutingKey is generated based on the topic provided for Publish.
	GenerateRoutingKey func(topic string) string

	// Publishings can be undeliverable when the mandatory flag is true and no queue is
	// bound that matches the routing key, or when the immediate flag is true and no
	// consumer on the matched queue is ready to accept the delivery.
	Mandatory bool

	// Publishings can be undeliverable when the mandatory flag is true and no queue is
	// bound that matches the routing key, or when the immediate flag is true and no
	// consumer on the matched queue is ready to accept the delivery.
	Immediate bool

	// With transactional enabled, all messages wil be added in transaction.
	Transactional bool
}

type ConsumeConfig struct {
	// When true, message will be not requeued when nacked.
	NoRequeueOnNack bool

	// The consumer is identified by a string that is unique and scoped for all
	// consumers on this channel.  If you wish to eventually cancel the consumer, use
	// the same non-empty identifier in Channel.Cancel.  An empty string will cause
	// the library to generate a unique identity.  The consumer identity will be
	// included in every Delivery in the ConsumerTag field
	Consumer string

	// When exclusive is true, the server will ensure that this is the sole consumer
	// from this queue. When exclusive is false, the server will fairly distribute
	// deliveries across multiple consumers.
	Exclusive bool

	// The noLocal flag is not supported by RabbitMQ.
	NoLocal bool

	// When noWait is true, do not wait for the server to confirm the request and
	// immediately begin deliveries.  If it is not possible to consume, a channel
	// exception will be raised and the channel will be closed.
	NoWait bool

	Qos QosConfig

	// Optional arguments can be provided that have specific semantics for the queue
	// or server.
	Arguments amqp.Table
}

// Qos controls how many messages or how many bytes the server will try to keep on
// the network for consumers before receiving delivery acks.  The intent of Qos is
// to make sure the network buffers stay full between the server and client.
type QosConfig struct {
	// With a prefetch count greater than zero, the server will deliver that many
	// messages to consumers before acknowledgments are received.  The server ignores
	// this option when consumers are started with noAck because no acknowledgments
	// are expected or sent.
	//
	// In order to defeat that we can set the prefetch count with the value of 1.
	// This tells RabbitMQ not to give more than one message to a worker at a time.
	// Or, in other words, don't dispatch a new message to a worker until it has
	// processed and acknowledged the previous one.
	// Instead, it will dispatch it to the next worker that is not still busy.
	PrefetchCount int

	// With a prefetch size greater than zero, the server will try to keep at least
	// that many bytes of deliveries flushed to the network before receiving
	// acknowledgments from the consumers.  This option is ignored when consumers are
	// started with noAck.
	PrefetchSize int

	// When global is true, these Qos settings apply to all existing and future
	// consumers on all channels on the same connection.  When false, the Channel.Qos
	// settings will apply to all existing and future consumers on this channel.
	//
	// Please see the RabbitMQ Consumer Prefetch documentation for an explanation of
	// how the global flag is implemented in RabbitMQ, as it differs from the
	// AMQP 0.9.1 specification in that global Qos settings are limited in scope to
	// channels, not connections (https://www.rabbitmq.com/consumer-prefetch.html).
	Global bool
}

type ReconnectConfig struct {
	BackoffInitialInterval     time.Duration
	BackoffRandomizationFactor float64
	BackoffMultiplier          float64
	BackoffMaxInterval         time.Duration
}

func DefaultReconnectConfig() *ReconnectConfig {
	return &ReconnectConfig{
		BackoffInitialInterval:     500 * time.Millisecond,
		BackoffRandomizationFactor: 0.5,
		BackoffMultiplier:          1.5,
		BackoffMaxInterval:         60 * time.Second,
	}
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
