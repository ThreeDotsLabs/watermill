+++
title = "Getting started"
description = "Watermill up and running"
weight = -9999
draft = false
bref = "Watermill up and running"
+++

## What is Watermill?

Watermill is a Go library for working with message streams.
You can use it to build event-driven systems with popular Pub/Sub implementations like Kafka or RabbitMQ, as well as HTTP or Postgres if that fits your use case.
It comes with a set of Pub/Sub implementations and can be easily extended.

Watermill also ships with standard middlewares like instrumentation, poison queue, throttling, correlation,
and other tools used by every message-driven application.

## Why use Watermill?

When using microservices, synchronous communication is not always the right choice.
Asynchronous methods became a new standard way to communicate.

While there are many tools and libraries for synchronous communication, like HTTP, correctly setting up
a message-oriented project can be challenging. There are many different message queues and streaming systems,
each with different features, client libraries, and APIs.

Watermill aims to be the standard messaging library for Go, hiding all that complexity behind an API that is easy to understand.
It provides all you need to build an application based on events or other asynchronous patterns.

**Watermill is NOT a framework**.
It's a lightweight library that's easy to plug in or remove from your project.

## Install

```bash
go get -u github.com/ThreeDotsLabs/watermill
```

## Learn with quickstart

Docs too boring? Prefer learning by coding?

We have [a free hands-on training]({{< ref "/docs/quickstart/" >}}) where you'll solve exercises to learn how to use Watermill in your projects.

It'll guide you through the basics and a few advanced concepts like message ordering and the Outbox pattern.

## One-Minute Background

The idea behind event-driven applications is always the same: listen to and react to incoming messages.
Watermill supports this behavior for multiple [publishers and subscribers]({{< ref "/pubsubs" >}}).

The core part of Watermill is the [*Message*]({{< ref "/docs/message" >}}).
It is what `http.Request` is for the `net/http` package.
Most Watermill features work with this struct.

Watermill provides a few APIs for working with messages.
They build on top of each other, each step providing a higher-level API:

* At the bottom, the `Publisher` and `Subscriber` interfaces. It's the "raw" way of working with messages. You get full control, but also need to handle everything yourself.
* The `Router` is similar to HTTP routers you probably know. It introduces message handlers.
* The `CQRS` component adds generic handlers without needing to marshal and unmarshal messages yourself.

<div class="text-center">
    <img src="/img/pyramid.png" alt="Watermill components pyramid" style="width:35rem;" />
</div>

## Publisher & Subscriber

Most Pub/Sub libraries come with complex features. For Watermill, it's enough to implement two interfaces to start
working with them: the `Publisher` and `Subscriber`.

```go
type Publisher interface {
	Publish(topic string, messages ...*Message) error
	Close() error
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
	Close() error
}
```

### Subscribing for Messages

`Subscribe` expects a topic name and returns a channel of incoming messages.
What _topic_ exactly means depends on the Pub/Sub implementation.
Usually, it needs to match the topic name used by the publisher.

```go
messages, err := subscriber.Subscribe(ctx, "example.topic")
if err != nil {
	panic(err)
}

for msg := range messages {
	fmt.Printf("received message: %s, payload: %s\n", msg.UUID, string(msg.Payload))
	msg.Ack()
}
```

See detailed examples below for supported PubSubs.

{{< tabs "getting-started" >}}

{{< tab "Go Channel" "go-channel" >}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/go-channel/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/go-channel/main.go" first_line_contains="func process" %}}
{{< /tab >}}

{{< tab "Kafka" "kafka" >}}

<details>
<summary><strong>Running in Docker</strong></summary>

The easiest way to run Watermill locally with Kafka is by using Docker.

{{% load-snippet file="src-link/_examples/pubsubs/kafka/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute the `docker-compose up` command.

A more detailed explanation of how it works (and how to add live code reload) can be found in the [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
</details>

{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="func process" %}}
{{< /tab >}}

{{< tab "NATS Streaming" "nats" >}}

<details>
<summary><strong>Running in Docker</strong></summary>

The easiest way to run Watermill locally with NATS is using Docker.

{{% load-snippet file="src-link/_examples/pubsubs/nats-streaming/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute the `docker-compose up` command.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
</details>

{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="func process" %}}
{{< /tabs >}}


{{< tab "Google Cloud Pub/Sub" "gcp" >}}

<details>
<summary><strong>Running in Docker</strong></summary>

You can run the Google Cloud Pub/Sub emulator locally for development.

{{% load-snippet file="src-link/_examples/pubsubs/googlecloud/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
</details>

{{% load-snippet-partial file="src-link/_examples/pubsubs/googlecloud/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/googlecloud/main.go" first_line_contains="func process" %}}
{{< /tab >}}

{{< tab "RabbitMQ (AMQP)" "amqp" >}}

<details>
<summary><strong>Running in Docker</strong></summary>

{{% load-snippet file="src-link/_examples/pubsubs/amqp/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
</details>

{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="func process" %}}
{{< /tab >}}

{{< tab "SQL" "sql" >}}

<details>
<summary><strong>Running in Docker</strong></summary>

{{% load-snippet file="src-link/_examples/pubsubs/sql/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
</details>

{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="func process" %}}
{{< /tab >}}

{{< tab "AWS SQS" "aws-sqs" >}}

<details>
<summary><strong>Running in Docker</strong></summary>

{{% load-snippet file="src-link/_examples/pubsubs/aws-sqs/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
</details>

{{% load-snippet-partial file="src-link/_examples/pubsubs/aws-sqs/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/aws-sqs/main.go" first_line_contains="func process" %}}
{{< /tab >}}

{{< tab "AWS SNS" "aws-sns" >}}

<details>
<summary><strong>Running in Docker</strong></summary>

{{% load-snippet file="src-link/_examples/pubsubs/aws-sns/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
</details>

{{% load-snippet-partial file="src-link/_examples/pubsubs/aws-sns/main.go" first_line_contains="package main" last_line_contains="go process(" padding_after="1" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/aws-sns/main.go" first_line_contains="func process" %}}
{{< /tab >}}

{{< /tabs >}}

### Creating Messages

Watermill doesn't enforce any message format. `NewMessage` expects a slice of bytes as the payload.
You can use strings, JSON, protobuf, Avro, gob, or anything else that serializes to `[]byte`.

The message UUID is optional but recommended for debugging.

```go
msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))
```

### Publishing Messages

`Publish` expects a topic and one or more `Message`s to be published.

```go
err := publisher.Publish("example.topic", msg)
if err != nil {
    panic(err)
}
```

{{< tabs "publishing" >}}

{{< tab "Go Channel" "go-channel" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/go-channel/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< tab "Kafka" "kafka" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< tab "NATS Streaming" "nats" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< tab "Google Cloud Pub/Sub" "gcp" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/googlecloud/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< tab "RabbitMQ (AMQP)" "amqp" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< tab "SQL" "sql" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< tab "AWS SQS" "aws-sqs" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/aws-sqs/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< tab "AWS SNS" "aws-sns" >}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/aws-sns/main.go" first_line_contains="message.NewMessage" last_line_contains="publisher.Publish" padding_after="2" %}}
{{< /tab >}}

{{< /tabs >}}

## Router

[*Publishers and subscribers*]({{< ref "/docs/pub-sub" >}}) are the low-level parts of Watermill.
For most cases, you want to use a high-level API: [*Router*]({{< ref "/docs/messages-router" >}}) component.

### Router configuration

Start with configuring the router and adding plugins and middlewares.

A middleware is a function executed for each incoming message.
You can use one of the existing ones for things like [correlation, metrics, poison queue, retrying, throttling, etc.]({{< ref "/docs/messages-router#middleware" >}}).
You can also create your own.

{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="message.NewRouter" last_line_contains="middleware.Recoverer," padding_after="1" %}}

### Handlers

Set up handlers that the router uses.
Each handler independently handles incoming messages.

A handler listens to messages from the given subscriber and topic.
Any messages returned from the handler function will be published to the given publisher and topic.

{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="AddHandler returns" last_line_contains=")" padding_after="0" %}}

*Note: the example above uses one `pubSub` argument for both the subscriber and publisher.
It's because we use the `GoChannel` implementation, which is a simple in-memory Pub/Sub.*

Alternatively, if you don't plan to publish messages from within the handler, you can use the simpler `AddConsumerHandler` method.

{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="AddConsumerHandler" last_line_contains=")" padding_after="0" %}}

You can use two types of *handler functions*:

1. a function `func(msg *message.Message) ([]*message.Message, error)`
2. a struct method `func (c structHandler) Handler(msg *message.Message) ([]*message.Message, error)`

Use the first one if your handler is a function without any dependencies.
The second option is useful when your handler requires dependencies such as a database handle or a logger.

{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="func printMessages" last_line_contains="return message.Messages{msg}, nil" padding_after="3" %}}

Finally, run the router.

{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="router.Run" last_line_contains="}" padding_after="0" %}}

The complete example's source can be found at [/_examples/basic/3-router/main.go](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/basic/3-router/main.go).

## Logging

To see Watermill's logs, pass any logger that implements the [LoggerAdapter](https://github.com/ThreeDotsLabs/watermill/blob/master/log.go).
For experimental development, you can use `NewStdLogger`.

Watermill provides ready-to-use `slog` adapter. You can create it with [`watermill.NewSlogLogger`](https://github.com/ThreeDotsLabs/watermill/blob/master/slog.go).
You can also map Watermill's log levels to `slog` levels with [`watermill.NewSlogLoggerWithLevelMapping`](https://github.com/ThreeDotsLabs/watermill/blob/master/slog.go).

## What's next?

For more details, see [documentation topics]({{< ref "/docs" >}}).

See the [CQRS component](/docs/cqrs) for another high-level API.

## Examples

Check out the [examples](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples) that will show you how to start using Watermill.

The recommended entry point is [Your first Watermill application](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/1-your-first-app).
It contains the entire environment in `docker-compose.yml`, including Go and Kafka, which you can run with one command.

After that, you can see the [Realtime feed](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/2-realtime-feed) example.
It uses more middlewares and contains two handlers.

For a different subscriber implementation (**HTTP**), see the [receiving-webhooks](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/receiving-webhooks) example.
It is a straightforward application that saves webhooks to Kafka.

You can find the complete list of examples in the [README](https://github.com/ThreeDotsLabs/watermill#examples).

## Support

If anything is not clear, feel free to use any of our [support channels]({{< ref "/support" >}}); we will be glad to help.
