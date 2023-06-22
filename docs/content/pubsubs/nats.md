+++
title = "NATS Jetstream"
description = "A simple, secure and high performance open source messaging system"
date = 2022-02-03T10:30:00+05:00
bref = "A simple, secure and high performance open source messaging system"
weight = -50
type = "docs"
toc = false
+++

### NATS Jetstream

NATS Jetstream is a data streaming system powered by NATS, and written in the Go programming language.

### Installation

    go get github.com/ThreeDotsLabs/watermill-nats/v2

#### Characteristics

| Feature             | Implements | Note                                                                                                                  |
|---------------------|------------|-----------------------------------------------------------------------------------------------------------------------|
| ConsumerGroups      | yes        | you need to set `QueueGroupPrefix` name or provide an optional calculator                                             |
| ExactlyOnceDelivery | yes        | you need to ensure 'AckAsync' has default false value and set 'TrackMsgId' to true on the Jetstream configuration     |
| GuaranteedOrder     | no         | [with the redelivery feature, order can't be guaranteed](https://github.com/nats-io/nats-streaming-server/issues/187) |
| Persistent          | yes        |                                                                                                                       |

#### Configuration

Configuration is done through PublisherConfig and SubscriberConfig types.  These share a common JetStreamConfig.  To use the experimental nats-core support, set Disabled=true.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/jetstream.go" first_line_contains="// JetStreamConfig contains" last_line_contains="type DurableCalculator =" %}}
{{% /render-md %}}

PublisherConfig:

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="type Publisher struct {" %}}
{{% /render-md %}}

Subscriber Config:

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="type Subscriber struct" %}}
{{% /render-md %}}

#### Connecting

By default NATS client will try to connect to `localhost:4222`. If you are using different hostname or port you should specify using the URL property of `SubscriberConfig` and `PublisherConfig`.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/publisher.go" first_line_contains="// NewPublisher" last_line_contains="func NewPublisher" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-jetstream/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/subscriber.go" first_line_contains="// NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-jetstream/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

You can also use `NewSubscriberWithNATSConn` and `NewPublisherWithNatsConn` to use a custom `stan.Conn` created by `NewStanConnection`.

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/publisher.go" first_line_contains="// Publish publishes" last_line_contains="func (p *Publisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/subscriber.go" first_line_contains="// Subscribe " last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

NATS provides a header passing mechanism that allows conveying the watermill message details as metadata. This is done by default with only the binary payload sent in the message body.  The header `_watermill_message_uuid` is reserved.

Other builtin marshalers are based on Golang's [`gob`](https://golang.org/pkg/encoding/gob/) and [`json`](https://golang.org/packages/encoding/json) packages.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/marshaler.go" first_line_contains="type Marshaler " last_line_contains="func defaultNatsMsg" padding_after="0" %}}
{{% /render-md %}}

When you have your own format of the messages, you can implement your own Marshaler, which will serialize messages in your format.  An example protobuf implementation with tests and benchmarks can be found [here](https://github.com/ThreeDotsLabs/watermill-nats/tree/master/_examples/marshalers/protobuf/)

When needed, you can bypass both [UUID]({{< ref "message#message" >}}) and [Metadata]({{< ref "message#message" >}}) and send just a `message.Payload`,
but some standard [middlewares]({{< ref "messages-router#middleware" >}}) may be not working.

### Core-Nats

This package also includes limited support for connecting to [core-nats](https://docs.nats.io/nats-concepts/core-nats).  While core-nats does not support many of the streaming features needed for a perfect fit with watermill and most acks end up implemented as no-ops, in environments with a mix of jetstream and core-nats messaging in play it can be nice to use watermill consistently on the application side.
