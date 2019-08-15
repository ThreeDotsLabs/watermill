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
applications. It can be used for event sourcing, RPC over messages, sagas and whatever else comes to your mind.
You can use conventional pub/sub implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog, if that fits your use case.

It comes with a set of Pub/Sub implementations and can be easily extended by your own.

Watermill also ships with standard middlewares like instrumentation, poison queue, throttling, correlation
and other tools used by every message-driven application.

### Why use Watermill?

* Standard messaging library
* Provides all you need for messaging
* Messaging as a standard communication way

### Install

```bash
go get -u github.com/ThreeDotsLabs/watermill/
```

### Subscribing for messages

One of the core parts of Watermill is the [*Message*]({{< ref "/docs/message" >}}). It is as important as `http.Request` is for `http` package.
Almost every part of Watermill uses this struct in some way.

The basic idea behind reactive/event-driven applications stays always the same: listen for incoming messages and react to them.
Watermill is supporting multiple [publishers and subscribers implementations]({{< ref "/pubsubs" >}}) with compatible interfaces
and abstractions, which provide a similar behaviour.

Let's start with subscribing for messages.

{{% tabs id="subscribing" tabs="go-channel,kafka,nats-streaming,gcloud,amqp" labels="Go Channel,Kafka,NATS Streaming,Google Cloud Pub/Sub,RabbitMQ (AMQP)" %}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}

{{< collapse id="installing_rdkafka" >}}

{{< collapse-toggle box_id="docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="docker" %}}
The easiest way to run Watermill locally with Kafka is using Docker.

{{% load-snippet file="content/docs/getting-started/kafka/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, please execute `docker-compose up` command.

A more detailed explanation of how it is running (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).

{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="nats-streaming"%}}

{{< collapse id="running_nats" >}}

{{< collapse-toggle box_id="nats-streaming-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="nats-streaming-docker" %}}
The easiest way to run Watermill locally with NATS is using Docker.

{{% load-snippet file="content/docs/getting-started/nats-streaming/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run please execute `docker-compose up` command.

A more detailed explanation of how it is running (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/nats-streaming/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/nats-streaming/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}


{{% tabs-tab id="gcloud"%}}

{{< collapse id="running_gcloud" >}}

{{< collapse-toggle box_id="gcloud-streaming-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="gcloud-streaming-docker" %}}
You can run Google Cloud Pub/Sub emulator locally for development.

{{% load-snippet file="content/docs/getting-started/googlecloud/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, please execute `docker-compose up`.

A more detailed explanation of how it is running (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/googlecloud/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/googlecloud/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="amqp"%}}

{{< collapse id="running_amqp" >}}

{{< collapse-toggle box_id="amqp-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="amqp-docker" %}}
{{% load-snippet file="content/docs/getting-started/amqp/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run, please execute `docker-compose up`.

A more detailed explanation of how it is running (and how to add live code reload) can be found in [*Go Docker dev environment* article](https://threedots.tech/post/go-docker-dev-environment-with-go-modules-and-live-code-reloading/).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/amqp/main.go" first_line_contains="package main" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/amqp/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% /tabs %}}

### Publishing messages

{{% tabs id="publishing" tabs="go-channel,kafka,nats-streaming,gcloud,amqp" labels="Go Channel,Kafka,NATS Streaming,Google Cloud Pub/Sub,RabbitMQ (AMQP)" %}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="nats-streaming" %}}
{{% load-snippet-partial file="content/docs/getting-started/nats-streaming/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="gcloud" %}}
{{% load-snippet-partial file="content/docs/getting-started/googlecloud/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="amqp" %}}
{{% load-snippet-partial file="content/docs/getting-started/amqp/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% /tabs %}}

##### Message format

Watermill doesn't enforce any message format. You can use strings, JSON, protobuf, Avro, gob or anything else that serializes to `[]byte`.

### Using *Messages Router*

[*Publishers and subscribers*]({{< ref "/docs/pub-sub" >}}) are rather low-level parts of Watermill.
In most cases, you'd usually want to use a high-level interface and features like [correlation, metrics, poison queue, retrying, throttling, etc.]({{< ref "/docs/messages-router#middleware" >}}).

You also might not want to send an Ack when processing was successful. Sometimes, you'd like to send a message after processing of another message finishes.

To handle these requirements, there is a component named [*Router*]({{< ref "/docs/messages-router" >}}).

The flow of our application looks like this:

1. A message is produced on topic `example.topic_1` every second.
2. `struct_handler` handler listens on `example.topic_1`. When a message is received, the UUID is printed and a new message is produced on `example.topic_2`.
3. `print_events_topic_1` handler listens on `example.topic_1` and prints message UUID, payload and metadata. Correlation ID should be the same as in the message on `example.topic_1`.
4. `print_events_topic_2` handler listens on `example.topic_2` and prints message UUID, payload and metadata. Correlation ID should be the same as in the message on `example.topic_2`.

#### Router configuration

Start with configuring the router, adding plugins and middlewares.
Then set up handlers that the router will use. Each handler will independently handle messages.

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="package" last_line_contains="router.Run(ctx)" padding_after="4" %}}
{{% /render-md %}}

#### Producing messages

Producing messages works the same like when using publisher directly. Notice that we've added `SetCorrelationID` middleware.
Correlation ID will be added to all messages produced by the router (it will be stored in metadata).

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="func publishMessages" last_line_contains="time.Sleep(time.Second)" padding_after="2" %}}
{{% /render-md %}}

#### Handlers

You may have noticed that there are two types of *handler functions*:

1. function `func(msg *message.Message) ([]*message.Message, error)`
2. method `func (c structHandler) Handler(msg *message.Message) ([]*message.Message, error)`

If your handler is a function without any dependencies, it's fine to use the first one.
The second option is useful when your handler requires some dependencies like database handle, a logger, etc.

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="func printMessages" last_line_contains="return message.Messages{msg}, nil" padding_after="3" %}}
{{% /render-md %}}

#### Done!

You can run this example by `go run main.go`.

You've just created your first application with Watermill. You can find the full source in [/docs/content/docs/getting-started/router/main.go](https://github.com/ThreeDotsLabs/watermill/blob/master/docs/content/docs/getting-started/router/main.go).

### Deployment

Watermill is not a framework. We don't enforce any type of deployment and it's totally up to you.

### What's next?

For more detailed documentation you should check [documentation topics list]({{< ref "/docs" >}}).

#### Examples

We've also created some examples, that will show you how  to start using Watermill.
The recommended entry point is [Your first Watermill application](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/your-first-app). It contains the entire environment in `docker-compose.yml`, including Golang and Kafka which you can run with one command.

After that, you can check the [Simple app](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/simple-app) example. It uses more middlewares and contains two handlers. There is also a separate application for publishing messages.

The [third example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/http-to-kafka)  showcases the use of a different Subscriber implementation, namely **HTTP**. It is a very simple application, which can save GitLab webhooks to Kafka.

Most recent information can be found in the project's [README](https://github.com/ThreeDotsLabs/watermill#readme).

#### Support

If anything is not clear, feel free to use any of our [support channels]({{< ref "/support" >}}), we will be glad to help.
