+++
title = "Pub/Sub's implementations"
description = "Golang channel, Kafka, Google Cloud Pub/Sub, RabbitMQ and more!"
date = 2018-12-05T12:47:48+01:00
weight = -800
draft = false
bref = "Golang channel, Kafka, Google Cloud Pub/Sub, RabbitMQ and more!"
toc = false
+++

| Name | Publisher | Subscriber | Status |
|------|-----------|------------|--------|
|  [Golang Channel]({{< ref "#golang-channel" >}}) | x | x | `prod-ready` |
|  [Kafka]({{< ref "#kafka" >}}) | x | x | `prod-ready` |
|  [HTTP]({{< ref "#http" >}})  | x | x | `prod-ready` |
|  [Google Cloud Pub/Sub]({{< ref "#google-cloud-pub-sub" >}})  | x | x | `prod-ready` |
|  [NATS Streaming]({{< ref "#nats-streaming" >}})  | x | x | `prod-ready` |
|  [RabbitMQ (AMQP)]({{< ref "#rabbitmq-amqp" >}})  | x | x | `prod-ready` |
|  MySQL Binlog  |  | x | [`idea`](https://github.com/ThreeDotsLabs/watermill/issues/5) |

All built-in implementations can be found in [message/infrastructure](https://github.com/ThreeDotsLabs/watermill/tree/master/message/infrastructure).

### Golang Channel

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/gochannel/pubsub.go" first_line_contains="// GoChannel" last_line_contains="type GoChannel struct {" %}}
{{% /render-md %}}

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | no | |
| ExactlyOnceDelivery | yes |  |
| GuaranteedOrder | yes |  |
| Persistent | no| |

#### Configuration

You can inject configuration via the constructor.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/gochannel/pubsub.go" first_line_contains="func NewGoChannel" last_line_contains="logger:" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/gochannel/pubsub.go" first_line_contains="// Publish" last_line_contains="func (g *GoChannel) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/gochannel/pubsub.go" first_line_contains="// Subscribe" last_line_contains="func (g *GoChannel) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

No marshaling is needed when sending messages within the process.

### Kafka

Kafka is one of the most popular Pub/Subs. We are providing Pub/Sub implementation based on [Shopify's Sarama](https://github.com/Shopify/sarama).

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes | |
| ExactlyOnceDelivery | no | in theory can be achieved with [Transactions](https://www.confluent.io/blog/transactions-apache-kafka/), currently no support for any Golang client  |
| GuaranteedOrder | yes | require [paritition key usage](#using-partition-key)  |
| Persistent | yes| |

#### Configuration

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="// Subscribe" %}}
{{% /render-md %}}

##### Passing custom `Sarama` config

You can pass [custom config](https://github.com/Shopify/sarama/blob/master/config.go#L20) parameters via `overwriteSaramaConfig *sarama.Config` in `NewSubscriber` and `NewPublisher`.
When `nil` is passed, default config is used (`DefaultSaramaSubscriberConfig`).

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/config.go" first_line_contains="// DefaultSaramaSubscriberConfig" last_line_contains="return config" padding_after="1" %}}
{{% /render-md %}}

#### Connecting

##### Publisher
{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/publisher.go" first_line_contains="// NewPublisher" last_line_contains="(message.Publisher, error)" padding_after="0" %}}

Example:
{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="saramaSubscriberConfig :=" last_line_contains="panic(err)" padding_after="1" %}}

{{% /render-md %}}

##### Subscriber
{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/subscriber.go" first_line_contains="// NewSubscriber" last_line_contains="(message.Subscriber, error)" padding_after="0" %}}

Example:
{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="publisher, err := kafka.NewPublisher" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/publisher.go" first_line_contains="// Publish" last_line_contains="func (p *Publisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/subscriber.go" first_line_contains="// Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

Watermill's messages cannot be directly sent to Kafka - they need to be marshaled. You can implement your marshaler or use default implementation.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/marshaler.go" first_line_contains="// Marshaler" last_line_contains="func (DefaultMarshaler)" padding_after="0" %}}
{{% /render-md %}}

#### Partitioning

Our Publisher has support for the partitioning mechanism.

It can be done with special Marshaler implementation:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/marshaler.go" first_line_contains="type kafkaJsonWithPartitioning" last_line_contains="func (j kafkaJsonWithPartitioning) Marshal" padding_after="0" %}}
{{% /render-md %}}

When using, you need to pass your function to generate partition key.
It's a good idea to pass this partition key with metadata to not unmarshal entire message.

{{< highlight >}}
marshaler := kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
    return msg.Metadata.Get("partition"), nil
})
{{< /highlight >}}

### HTTP

The HTTP subscriber listens to HTTP requests (for example - webhooks) and outputs them as messages.
You can then post them to any Publisher. Here is an example with [sending HTTP messages to Kafka](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/http-to-kafka/main.go).

The HTTP publisher sends HTTP requests as specified in its configuration. Here is an example with [transforming Kafka messages into HTTP webhook requests](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/kafka-to-http).

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | no | |
| ExactlyOnceDelivery | yes |  |
| GuaranteedOrder | yes |  |
| Persistent | no| |

#### Subscriber configuration

Subscriber configuration is done via the config struct passed to the constructor:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/http/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

You can use the `Router` config option to `SubscriberConfig` to pass your own `chi.Router` (see [chi](https://github.com/go-chi/chi)).
This may be helpful if you'd like to add your own HTTP handlers (e.g. a health check endpoint).

#### Publisher configuration

Publisher configuration is done via the config struct passed to the constructor:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/http/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

How the message topic and body translate into the URL, method, headers, and payload of the HTTP request is highly configurable through the use of `MarshalMessageFunc`. 
Use the provided `DefaultMarshalMessageFunc` to send POST requests to a specific url:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/http/publisher.go" first_line_contains="// MarshalMessageFunc" last_line_contains="return req, nil" padding_after="2" %}}
{{% /render-md %}}

You can pass your own `http.Client` to execute the requests or use Golang's default client. 

#### Running

To run HTTP subscriber you need to run `StartHTTPServer()`. It needs to be run after `Subscribe()`.

When using with the router, you should wait for the router to start.

{{< highlight >}}
<-r.Running()
httpSubscriber.StartHTTPServer()
{{< /highlight >}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/http/subscriber.go" first_line_contains="// Subscribe adds" last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

### Google Cloud Pub/Sub

Cloud Pub/Sub brings the flexibility and reliability of enterprise message-oriented middleware to
the cloud.

At the same time, Cloud Pub/Sub is a scalable, durable event ingestion and delivery
system that serves as a foundation for modern stream analytics pipelines.
By providing many-to-many, asynchronous messaging that decouples senders and receivers,
it allows for secure and highly available communication among independently written applications.

Cloud Pub/Sub delivers low-latency, durable messaging that helps developers quickly integrate
systems hosted on the Google Cloud Platform and externally.

Documentation: [https://cloud.google.com/pubsub/docs/](https://cloud.google.com/pubsub/docs/overview)

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes | multiple subscribers within the same Subscription name  |
| ExactlyOnceDelivery | no |  |
| GuaranteedOrder | no | |
| Persistent | yes* | maximum retention time is 7 days |

#### Configuration

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/googlecloud/publisher.go" first_line_contains="type PublisherConfig struct " last_line_contains="func NewPublisher" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/googlecloud/subscriber.go" first_line_contains="type SubscriberConfig struct {" last_line_contains="func NewSubscriber(" %}}
{{% /render-md %}}

##### Subscription name

To receive messages published to a topic, you must create a subscription to that topic.
Only messages published to the topic after the subscription is created are available to subscriber
applications.

The subscription connects the topic to a subscriber application that receives and processes
messages published to the topic.

A topic can have multiple subscriptions, but a given subscription belongs to a single topic.

In Watermill, the subscription is created automatically during calling `Subscribe()`.
Subscription name is generated by function passed to `SubscriberConfig.GenerateSubscriptionName`.
By default, it is just the topic name (`TopicSubscriptionName`).

When you want to consume messages from a topic with multiple subscribers, you should use
`TopicSubscriptionNameWithSuffix` or your custom function to generate the subscription name.

#### Connecting

Watermill will connect to the instance of Google Cloud Pub/Sub indicated by the environment variables. For production setup, set the `GOOGLE_APPLICATION_CREDENTIALS` env, as described in [the official Google Cloud Pub/Sub docs](https://cloud.google.com/pubsub/docs/quickstart-client-libraries#pubsub-client-libraries-go). Note that you won't need to install the Cloud SDK, as Watermill will take care of the administrative tasks (creating topics/subscriptions) with the default settings and proper permissions.

For development, you can use a Docker image with the emulator and the `PUBSUB_EMULATOR_HOST` env ([check out the Getting Started guide]({{< ref "getting-started#subscribing_gcloud" >}})).

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/googlecloud/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/googlecloud/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/googlecloud/publisher.go" first_line_contains="// Publish" last_line_contains="func (p *Publisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/googlecloud/subscriber.go" first_line_contains="// Subscribe " last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

Watermill's messages cannot be directly sent to Kafka - they need to be marshaled. You can implement your marshaler or use default implementation.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/googlecloud/marshaler.go" first_line_contains="// Marshaler" last_line_contains="type DefaultMarshalerUnmarshaler " padding_after="0" %}}
{{% /render-md %}}

### NATS Streaming

NATS Streaming is a data streaming system powered by NATS, and written in the Go programming language. The executable name for the NATS Streaming server is nats-streaming-server. NATS Streaming embeds, extends, and interoperates seamlessly with the core NATS platform.

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes | you need to set `DurableName` and `QueueGroup` name |
| ExactlyOnceDelivery | no |  |
| GuaranteedOrder | no | [with the redelivery feature, order can't be guaranteed](https://github.com/nats-io/nats-streaming-server/issues/187) |
| Persistent | yes| `DurableName` is required |

#### Configuration

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/nats/publisher.go" first_line_contains="type StreamingPublisherConfig struct" last_line_contains="type StreamingPublisher struct {" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/nats/subscriber.go" first_line_contains="type StreamingSubscriberConfig struct" last_line_contains="type StreamingSubscriber struct" %}}
{{% /render-md %}}

#### Connecting

By default NATS client will try to connect to `localhost:4222`. If you are using different hostname or port you should pass custom `stan.Option`: `stan.NatsURL("nats://your-nats-hostname:4222")` to `StreamingSubscriberConfig` and `StreamingPublisherConfig`.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/nats/publisher.go" first_line_contains="// NewStreamingPublisher" last_line_contains="func NewStreamingPublisher" %}}

Example:
{{% load-snippet-partial file="content/docs/getting-started/nats-streaming/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/nats/subscriber.go" first_line_contains="// NewStreamingSubscriber" last_line_contains="func NewStreamingSubscriber" %}}

Example:
{{% load-snippet-partial file="content/docs/getting-started/nats-streaming/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/nats/publisher.go" first_line_contains="// Publish" last_line_contains="func (p StreamingPublisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/nats/subscriber.go" first_line_contains="// Subscribe " last_line_contains="func (s *StreamingSubscriber) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

NATS doesn't implement any mechanism like metadata or headers of the message. For that reason we need to marshal entire message to the `[]byte`.

The default implementation is based on Golang's [`gob`](https://golang.org/pkg/encoding/gob/).

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/nats/marshaler.go" first_line_contains="type Marshaler " last_line_contains="type GobMarshaler struct" padding_after="0" %}}
{{% /render-md %}}

When you have your own format of the messages, you can implement your own Marshaler, which will serialize messages in your format.

When needed, you can bypass both [UUID]({{< ref "message#message" >}}) and [Metadata]({{< ref "message#message" >}}) and send just a `message.Payload`,
but some standard [middlewares]({{< ref "messages-router#middleware" >}}) may be not working.

### RabbitMQ (AMQP)

> RabbitMQ is the most widely deployed open source message broker.

We are providing Pub/Sub implementation based on [github.com/streadway/amqp](https://github.com/streadway/amqp).

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/amqp/doc.go" first_line_contains="// AMQP" last_line_contains="package amqp" padding_after="0" %}}
{{% /render-md %}}

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes* | there are no literal consumer groups in AMQP, but we can achieve similar behaviour with `GenerateQueueNameTopicNameWithSuffix`. For more details please check [AMQP "Consumer Groups" section](#amqp-consumer-groups) |
| ExactlyOnceDelivery | no |  |
| GuaranteedOrder | yes |  yes, please check https://www.rabbitmq.com/semantics.html#ordering |
| Persistent | yes* | when using `NewDurablePubSubConfig` or `NewDurableQueueConfig`  |

#### Configuration

Our AMQP is shipped with some pre-created configurations:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/amqp/config.go" first_line_contains="// NewDurablePubSubConfig" last_line_contains="type Config struct {" %}}
{{% /render-md %}}

For detailed configuration description, please check [message/infrastructure/amqp/pubsub_config.go](https://github.com/ThreeDotsLabs/watermill/tree/master/message/infrastructure/amqp/pubsub_config.go)

##### TLS Config

TLS config can be passed to `Config.TLSConfig`.


##### Connecting

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/amqp/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/amqp/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/amqp/publisher.go" first_line_contains="// Publish" last_line_contains="func (p *Publisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/amqp/subscriber.go" first_line_contains="// Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

Marshaler is responsible for mapping AMQP's messages to Watermill's messages.

Marshaller can be changed via the Configuration.
If you need to customize thing in `amqp.Delivery`, you can do it `PostprocessPublishing` function.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/amqp/marshaler.go" first_line_contains="// Marshaler" last_line_contains="func (DefaultMarshaler)" padding_after="0" %}}
{{% /render-md %}}

#### AMQP "Consumer Groups"

AMQP doesn't provide mechanism like Kafka's "consumer groups". You can still achieve similar behaviour with `GenerateQueueNameTopicNameWithSuffix` and `NewDurablePubSubConfig`.

{{% render-md %}}
{{% load-snippet-partial file="content/docs/snippets/amqp-consumer-groups/main.go" first_line_contains="func createSubscriber(" last_line_contains="go process(\"subscriber_2\", messages2)" %}}
{{% /render-md %}}


In this example both `pubSub1` and `pubSub2` will receive some messages independently.

### Implementing your own Pub/Sub

There aren't your Pub/Sub implementation? Please check [Implementing custom Pub/Sub]({{< ref "pub-sub-implementing" >}}).
