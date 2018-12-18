+++
title = "Getting started"
description = "Up and running Watermill"
weight = -9999
draft = false
toc = true
bref = "Up and running Watermill"
type = "docs"
+++

### What is Watermill?

Watermill is a Golang library for working efficiently with message streams. It is intended for building event-driven applications, enabling event sourcing, RPC over messages, sagas and basically whatever else comes to your mind. You can use conventional pub/sub implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

It comes with a set of Pub/Sub,  implementations which can be easily replaced by your own implementation

Watermill is also shipped with the set of standard tools (middlewares) like instrumentation, poison queue, throttling, correlation and other tools used by every message-driven application.

### Install

```bash
go get -u github.com/ThreeDotsLabs/watermill/
```

### Subscribing for messages

One of the most important parts of the Watermill is [*Message*]({{< ref "/docs/message" >}}). It is as important as `http.Request` for `http` package.
Almost every part of Watermill uses this type in some part.

When we are building reactive/event-driven application/[insert your buzzword here] we always want to listen of incoming messages to react for them.
Watermill is supporting multiple [publishers and subscribers implementations]({{< ref "/docs/pub-sub-implementations" >}}), with compatible interface and abstraction which provide similar behaviour.

Let's start with subscribing for messages.

{{% tabs id="subscribing" tabs="go-channel,kafka,nats-streaming,gcloud" labels="Go Channel,Kafka,NATS Streaming,Google Cloud Pub/Sub" %}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}

{{< collapse id="installing_rdkafka" >}}

{{< collapse-toggle box_id="docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="docker" %}}
Easiest way to run Watermill with Kafka locally is using Docker.

{{% load-snippet file="content/docs/getting-started/kafka/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run please execute `docker-compose up` command.

More detailed explanation of how it is running, and how to add live reload you can find in [our [...] article](todo).

{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="nats-streaming"%}}

{{< collapse id="running_nats" >}}

{{< collapse-toggle box_id="nats-streaming-docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="nats-streaming-docker" %}}
Easiest way to run Watermill with NATS locally is using Docker.

{{% load-snippet file="content/docs/getting-started/nats-streaming/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run please execute `docker-compose up` command.

More detailed explanation of how it is running, and how to add live reload you can find in [our [...] article](todo).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/nats-streaming/main.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
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

More detailed explanation of how it is running, and how to add live reload you can find in [our [...] article](todo).
{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/googlecloud/main.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/googlecloud/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% /tabs %}}

### Publishing messages

{{% tabs id="publishing" tabs="go-channel,kafka,nats-streaming,gcloud" labels="Go Channel,Kafka,NATS Streaming,Google Cloud Pub/Sub" %}}

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

{{% /tabs %}}

##### Message format

We don't enforce any message format. You can use strings, JSON, protobuf, Avro, gob or anything else what serializes to `[]byte`.

### Using *Messages Router*

[*Publishers and subscribers*]({{< ref "/docs/pub-sub" >}}) are rather low-level parts of Watermill.
In production use, we want usually use something which is higher level and provides some features like [correlation, metrics, poison queue, retrying, throttling etc]({{< ref "/docs/messages-router#middleware" >}}).

We also don't want to manually send Ack when processing was successful. Sometimes, we also want to send a message after processing another.

To handle these requirements we created component named [*Router*]({{< ref "/docs/messages-router" >}}).

The flow of our application looks like this:

1. We are producing a message to the topic `example.topic_1` every second.
2. `struct_handler` handler is listening to `example.topic_1`. When a message is received, UUID is printed and a new message is produced to `example.topic_2`.
3. `print_events_topic_1` handler is listening to `example.topic_1` and printing message UUID, payload and metadata. Correlation ID should be the same as in message in `example.topic_1`.
4. `print_events_topic_2` handler is listening to `example.topic_2` and printing message UUID, payload and metadata. Correlation ID should be the same as in message in `example.topic_2`.

#### Router configuration

For the beginning, we should start with the configuration of the router. We will configure which plugins and middlewares we want to use.

We also will set up handlers which this router will support. Every handler will independently handle the messages.

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="package" last_line_contains="router.Run()" padding_after="4" %}}
{{% /render-md %}}

#### Producing messages

Producing messages work just like before. We only have added `middleware.SetCorrelationID` to set correlation ID.
Correlation ID will be added to all messages produced by the router (`middleware.CorrelationID`).

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="func publishMessages" last_line_contains="time.Sleep(time.Second)" padding_after="2" %}}
{{% /render-md %}}

#### Handlers

You may notice that we have two types of *handler functions*:

1. function `func(msg *message.Message) ([]*message.Message, error)`
2. method `func (c structHandler) Handler(msg *message.Message) ([]*message.Message, error)`

The second option is useful when our function requires some dependencies like database, logger etc.
When we have just function without dependencies, it's fine to use just a function.

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="func printMessages" last_line_contains="return message.Messages{msg}, nil" padding_after="3" %}}
{{% /render-md %}}

#### Done!

We can just run this example by `go run main.go`.

We just created our first application with Watermill. The full source you can find in [/docs/getting-started/router/main.go](https://github.com/ThreeDotsLabs/watermill/tree/master/content/docs/getting-started/router/main.go).

### Deployment

Watermill is not a framework. We don't enforce any type of deployment and it's totally up to you.

### What's next?

For more detailed documentation you should check [documentation topics list]({{< ref "/docs" >}}).

#### Examples

We also created some examples, which will show you how you can start using Watermill.
The recommended entry point is [Your first Watermill application](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/your-first-app). It contains the entire environment in `docker-compose.yml`, including Golang and Kafka which you can run with one command.

After that, you could check the [Simple app](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/simple-app) example. It uses more middlewares and contains two handlers. There is also a separate application for publishing messages.

The [third example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/http-to-kafka)  showcases the use of a different Subscriber implementation, namely **HTTP**. It is a very simple application, which can save GitLab webhooks to Kafka.

You may also find some useful informations in our [README](https://github.com/ThreeDotsLabs/watermill#readme) .

#### Support

If anything is not clear feel free to use any of our [support channels]({{< ref "/support" >}}), we will we'll be glad to help.
