+++
title = "Getting started"
description = "Watermill up and running"
weight = -9999
draft = false 
toc = true
bref = "Watermill up and running"
type = "docs"
+++

### What is Watermill?

Watermill is a Golang library for working efficiently with message streams. It is intended for building event-driven
applications. It can be used for event sourcing, RPC over messages, sagas, and whatever else comes to your mind.
You can use conventional pub/sub implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog, if that fits your use case.

It comes with a set of Pub/Sub implementations and can be easily extended by your own.

Watermill also ships with standard middlewares like instrumentation, poison queue, throttling, correlation,
and other tools used by every message-driven application.

### Why use Watermill?

With more projects adopting the microservices pattern over recent years, we realized that synchronous communication
is not always the right choice. Asynchronous methods started to grow as a new standard way to communicate.

But while there's a lot of existing tooling for synchronous integration patterns (e.g. HTTP), correctly setting up
a message-oriented project can be a challenge. There's a lot of different message queues and streaming systems,
each with different features and client library API.

Watermill aims to be the standard messaging library for Go, hiding all that complexity behind an API that is easy to
understand. It provides all you might need for building an application based on events or other asynchronous patterns.
After looking at the examples, you should be able to quickly integrate Watermill with your project.

### Install

```bash
go get -u github.com/ThreeDotsLabs/watermill
```

### One Minute Background

The basic idea behind event-driven applications stays always the same: listen for incoming messages and react to them.
Watermill supports this behavior for multiple [publishers and subscribers]({{< ref "/pubsubs" >}}).

The core part of Watermill is the [*Message*]({{< ref "/docs/message" >}}). It is as important as `http.Request`
is for the `http` package. Most Watermill features use this struct in some way.

Even though PubSub libraries come with complex features, for Watermill it's enough to implement two interfaces to start
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

Let's start with subscribing. `Subscribe` expects a topic name and returns a channel of incoming messages.
What _topic_ exactly means depends on the PubSub implementation.

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

{{< tabs id="subscribing" tabs="go-channel,kafka,nats-streaming,gcloud,amqp,sql" labels="Go Channel,Kafka,NATS Streaming,Google Cloud Pub/Sub,RabbitMQ (AMQP),SQL" >}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/go-channel/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/go-channel/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}

{{< collapse id="installing_rdkafka" >}}

{{< collapse-toggle box_id="docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="docker" %}}
The easiest way to run Watermill locally with Kafka is using Docker.

{{% load-snippet file="src-link/_examples/pubsubs/kafka/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up` command.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).

{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="nats-streaming"%}}

{{< collapse id="running_nats" >}}

{{< collapse-toggle box_id="nats-streaming-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="nats-streaming-docker" %}}
The easiest way to run Watermill locally with NATS is using Docker.

{{% load-snippet file="src-link/_examples/pubsubs/nats-streaming/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run execute `docker-compose up` command.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}


{{% tabs-tab id="gcloud"%}}

{{< collapse id="running_gcloud" >}}

{{< collapse-toggle box_id="gcloud-streaming-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="gcloud-streaming-docker" %}}
You can run Google Cloud Pub/Sub emulator locally for development.

{{% load-snippet file="src-link/_examples/pubsubs/googlecloud/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/googlecloud/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/googlecloud/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="amqp"%}}

{{< collapse id="running_amqp" >}}

{{< collapse-toggle box_id="amqp-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="amqp-docker" %}}
{{% load-snippet file="src-link/_examples/pubsubs/amqp/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="sql"%}}

{{< collapse id="running_sql" >}}

{{< collapse-toggle box_id="sql-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="sql-docker" %}}
{{% load-snippet file="src-link/_examples/pubsubs/sql/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, execute `docker-compose up`.

A more detailed explanation of how it is working (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{< /tabs >}}

### Creating Messages

Watermill doesn't enforce any message format. `NewMessage` expects a slice of bytes as the payload. You can use
strings, JSON, protobuf, Avro, gob, or anything else that serializes to `[]byte`.

The message UUID is optional, but recommended, as it helps with debugging.

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

{{< tabs id="publishing" tabs="go-channel,kafka,nats-streaming,gcloud,amqp,sql" labels="Go Channel,Kafka,NATS Streaming,Google Cloud Pub/Sub,RabbitMQ (AMQP),SQL" >}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/go-channel/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="nats-streaming" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/nats-streaming/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="gcloud" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/googlecloud/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="amqp" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/amqp/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="sql" %}}
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{< /tabs >}}

### Using *Message Router*

[*Publishers and subscribers*]({{< ref "/docs/pub-sub" >}}) are rather low-level parts of Watermill.
In most cases, you'd usually want to use a high-level interface and features like [correlation, metrics, poison queue, retrying, throttling, etc.]({{< ref "/docs/messages-router#middleware" >}}).

You might want to send an Ack only if the message was processed successfully. 
In other cases, you'll Ack immediately and then worry about processing.
Sometimes, you want to perform some action based on the incoming message, and publish another message in response.

To handle these requirements, there is a component named [*Router*]({{< ref "/docs/messages-router" >}}).

### Example application of *Message Router*
The flow of the example application looks like this:

1. A message is produced on topic `incoming_messages_topic` every second.
2. `struct_handler` handler listens on `incoming_messages_topic`. When a message is received, the UUID is printed and a new message is produced on `outgoing_messages_topic`.
3. `print_incoming_messages` handler listens on `incoming_messages_topic` and prints the messages' UUID, payload and metadata. 
4. `print_outgoing_messages` handler listens on `outgoing_messages_topic` and prints the messages' UUID, payload and metadata. Correlation ID should be the same as in the message on `incoming_messages_topic`.

#### Router configuration

Start with configuring the router, adding plugins and middlewares.
Then set up handlers that the router will use. Each handler will independently handle messages.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="package" last_line_contains="router.Run(ctx)" padding_after="4" %}}
{{% /render-md %}}

#### Incoming messages

The `struct_handler` consumes messages from `incoming_messages_topic`, so we are simulating incoming traffic by calling `publishMessages()` in the background.
Notice that we've added the `SetCorrelationID` middleware. A Correlation ID will be added to all messages produced by the router (it will be stored in metadata).

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="func publishMessages" last_line_contains="time.Sleep(time.Second)" padding_after="2" %}}
{{% /render-md %}}

#### Handlers

You may have noticed that there are two types of *handler functions*:

1. function `func(msg *message.Message) ([]*message.Message, error)`
2. method `func (c structHandler) Handler(msg *message.Message) ([]*message.Message, error)`

If your handler is a function without any dependencies, it's fine to use the first one.
The second option is useful when your handler requires some dependencies like database handle, a logger, etc.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="func printMessages" last_line_contains="return message.Messages{msg}, nil" padding_after="3" %}}
{{% /render-md %}}

#### Done!

You can run this example by `go run main.go`.

You've just created your first application with Watermill. You can find the full source in [/_examples/basic/3-router/main.go](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/basic/3-router/main.go).

### Logging

To see Watermill's logs, you have to pass any logger that implements the [LoggerAdapter](https://github.com/ThreeDotsLabs/watermill/blob/master/log.go).
For experimental development, you can use `NewStdLogger`.

### Testing

Watermill provides [a set of test scenarios](https://github.com/ThreeDotsLabs/watermill/blob/master/pubsub/tests/test_pubsub.go)
that any Pub/Sub implementation can use. Each test suite needs to declare what features it supports and how to construct a new Pub/Sub.
These scenarios check both basic usage and more uncommon use cases. Stress tests are also included.

### Deployment

Watermill is not a framework. We don't enforce any type of deployment and it's totally up to you.

### What's next?

For more detailed documentation check [documentation topics]({{< ref "/docs" >}}).

#### Examples

Check out the [examples](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples) that will show you how to start using Watermill.

The recommended entry point is [Your first Watermill application](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/1-your-first-app).
It contains the entire environment in `docker-compose.yml`, including Golang and Kafka, which you can run with one command.

After that, you can see the [Realtime feed](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/2-realtime-feed) example.
It uses more middlewares and contains two handlers. There is also a separate application for publishing messages.

For a different subscriber implementation, namely **HTTP**, refer to the [receiving-webhooks](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/receiving-webhooks) example. It is a very simple application that saves webhooks to Kafka.

Full list of examples can be found in the project's [README](https://github.com/ThreeDotsLabs/watermill#examples).

#### Support

If anything is not clear, feel free to use any of our [support channels]({{< ref "/support" >}}), we will be glad to help.
