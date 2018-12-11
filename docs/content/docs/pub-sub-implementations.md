+++
title = "Pub/Sub's implementations"
description = "Golang channel, Kafka, HTTP, Google Cloud Pub/Sub and more!"
date = 2018-12-05T12:47:48+01:00
weight = -800
draft = false
bref = "Golang channel, Kafka, HTTP, Google Cloud Pub/Sub and more!"
toc = false
+++

| Name | Publisher | Subscriber | Status |
|------|-----------|------------|--------|
|  [Golang Channel]({{< ref "#golang-channel" >}}) | x | x | `prod-ready` |
|  [Kafka]({{< ref "#kafka" >}}) | x | x | `prod-ready` |
|  [HTTP]({{< ref "#http" >}})  |   | x | `prod-ready` |
|  [Google Cloud Pub/Sub]({{< ref "#google-cloud-pub-sub" >}})  | x | x | [`in-development`](https://github.com/ThreeDotsLabs/watermill/pull/10) |
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

##### Configuration

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

Kafka is one of the most popular Pub/Subs. We are providing Pub/Sub implementation based on [Confluent's bindings to `librdkafka`](https://github.com/confluentinc/confluent-kafka-go).

`librdkafka` is required to run Kafka Pub/Sub. Installation guide can be found in [Getting Started]({{< ref "/docs/getting-started#subscribing_kafka" >}}).

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes | |
| ExactlyOnceDelivery | no | in theory can be achieved with [Transactions](https://www.confluent.io/blog/transactions-apache-kafka/), currently no support for any Golang client  |
| GuaranteedOrder | yes | require [paritition key usage](#using-partition-key)  |
| Persistent | yes| |

#### Configuration

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="func (c SubscriberConfig)" %}}
{{% /render-md %}}

##### Passing custom `librdkafka` config

You can pass custom config parameters (for example SSL Configuration) via `KafkaConfigOverwrite` in `SubscriberConfig` and `kafkaConfigOverwrite` to `NewPublisher`.

You can find a list of available options in [librdkafka documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/publisher.go" first_line_contains="// Publish" last_line_contains="func (p confluentPublisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/kafka/subscriber.go" first_line_contains="// Subscribe" last_line_contains="func (s *confluentSubscriber) Subscribe" %}}
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

For this moment only HTTP subscriber is available. There is an issue for a [HTTP publisher](https://github.com/ThreeDotsLabs/watermill/issues/17).

HTTP subscriber allows us to send messages received by HTTP request (for example - webhooks).
You can then post them to any Publisher. Here is an example with [sending HTTP messages to Kafka](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/http-to-kafka/main.go).

When implemented, HTTP publisher can be used as webhooks sender.

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | no | |
| ExactlyOnceDelivery | no |  |
| GuaranteedOrder | yes |  |
| Persistent | no| |

#### Configuration

The configuration of HTTP subscriber is done via the constructor.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/infrastructure/http/subscriber.go" first_line_contains="// NewSubscriber" last_line_contains="func NewSubscriber(" %}}
{{% /render-md %}}

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

In progress.

### Implementing your own Pub/Sub

There aren't your Pub/Sub implementation? Please check [Implementing custom Pub/Sub]({{< ref "pub-sub-implementing" >}}).
