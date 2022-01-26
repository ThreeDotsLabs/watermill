+++
title = "RabbitMQ (AMQP)"
description = "The most widely deployed open source message broker"
date = 2019-07-06T22:30:00+02:00
bref = "The most widely deployed open source message broker"
weight = -40
type = "docs"
toc = false
+++

### RabbitMQ (AMQP)

> RabbitMQ is the most widely deployed open source message broker.

We are providing Pub/Sub implementation based on [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) official library.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-amqp/pkg/amqp/doc.go" first_line_contains="// AMQP" last_line_contains="package amqp" padding_after="0" %}}
{{% /render-md %}}

### Installation

    go get github.com/ThreeDotsLabs/watermill-amqp/v2

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
{{% load-snippet-partial file="src-link/watermill-amqp/pkg/amqp/config.go" first_line_contains="// NewDurablePubSubConfig" last_line_contains="type Config struct {" %}}
{{% /render-md %}}

For detailed configuration description, please check [watermill-amqp/pkg/amqp/config.go](https://github.com/ThreeDotsLabs/watermill-amqp/tree/master/pkg/amqp/config.go)

##### TLS Config

TLS config can be passed to `Config.TLSConfig`.

##### Connecting

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-amqp/pkg/amqp/publisher.go" first_line_contains="// Publish" last_line_contains="func (p *Publisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-amqp/pkg/amqp/subscriber.go" first_line_contains="// Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

Marshaler is responsible for mapping AMQP's messages to Watermill's messages.

Marshaller can be changed via the Configuration.
If you need to customize thing in `amqp.Delivery`, you can do it `PostprocessPublishing` function.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-amqp/pkg/amqp/marshaler.go" first_line_contains="// Marshaler" last_line_contains="func (d DefaultMarshaler)" padding_after="0" %}}
{{% /render-md %}}

#### AMQP "Consumer Groups"

AMQP doesn't provide mechanism like Kafka's "consumer groups". You can still achieve similar behaviour with `GenerateQueueNameTopicNameWithSuffix` and `NewDurablePubSubConfig`.

{{% render-md %}}
{{% load-snippet-partial file="docs/snippets/amqp-consumer-groups/main.go" first_line_contains="func createSubscriber(" last_line_contains="go process(\"subscriber_2\", messages2)" %}}
{{% /render-md %}}

In this example both `pubSub1` and `pubSub2` will receive some messages independently.

#### AMQP `TopologyBuilder`

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-amqp/pkg/amqp/topology_builder.go" first_line_contains="// TopologyBuilder" last_line_contains="}" padding_after="0" %}}
{{% /render-md %}}

