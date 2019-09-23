+++
title = "NATS Streaming"
description = "A simple, secure and high performance open source messaging system"
date = 2019-07-06T22:30:00+02:00
bref = "A simple, secure and high performance open source messaging system"
weight = -50
type = "docs"
toc = false
+++

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
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/publisher.go" first_line_contains="type StreamingPublisherConfig struct" last_line_contains="type StreamingPublisher struct {" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/subscriber.go" first_line_contains="type StreamingSubscriberConfig struct" last_line_contains="type StreamingSubscriber struct" %}}
{{% /render-md %}}

#### Connecting

By default NATS client will try to connect to `localhost:4222`. If you are using different hostname or port you should pass custom `stan.Option`: `stan.NatsURL("nats://your-nats-hostname:4222")` to `StreamingSubscriberConfig` and `StreamingPublisherConfig`.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/publisher.go" first_line_contains="// NewStreamingPublisher" last_line_contains="func NewStreamingPublisher" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/subscriber.go" first_line_contains="// NewStreamingSubscriber" last_line_contains="func NewStreamingSubscriber" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

You can also use `NewStreamingSubscriberWithStanConn` and `NewStreamingPublisherWithStanConn` to use a custom `stan.Conn` created by `NewStanConnection`.

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/publisher.go" first_line_contains="// Publish" last_line_contains="func (p StreamingPublisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/subscriber.go" first_line_contains="// Subscribe " last_line_contains="func (s *StreamingSubscriber) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

NATS doesn't implement any mechanism like metadata or headers of the message. For that reason we need to marshal entire message to the `[]byte`.

The default implementation is based on Golang's [`gob`](https://golang.org/pkg/encoding/gob/).

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-nats/pkg/nats/marshaler.go" first_line_contains="type Marshaler " last_line_contains="type GobMarshaler struct" padding_after="0" %}}
{{% /render-md %}}

When you have your own format of the messages, you can implement your own Marshaler, which will serialize messages in your format.

When needed, you can bypass both [UUID]({{< ref "message#message" >}}) and [Metadata]({{< ref "message#message" >}}) and send just a `message.Payload`,
but some standard [middlewares]({{< ref "messages-router#middleware" >}}) may be not working.

