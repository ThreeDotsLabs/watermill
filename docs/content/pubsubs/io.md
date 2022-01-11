+++
title = "io.Writer/io.Reader"
description = "Pub/Sub implemented as Go stdlib's most loved interfaces"
date = 2019-07-06T22:30:00+02:00
bref = "Pub/Sub implemented as Go stdlib's most loved interfaces"
weight = -30
type = "docs"
toc = false
+++

### io.Writer/io.Reader

This is an experimental Pub/Sub implementation that leverages the [standard library's](https://golang.org/pkg/io/) `io.Writer` and `io.Reader` interfaces as sources of Publisher and Subscriber, respectively.

Note that these aren't full-fledged Pub/Subs like Kafka, RabbitMQ, or the likes, but given the ubiquity of implementations of `Writer` and `Reader` they may come in handy, for uses like:

* Writing messages to file or stdout
* Subscribing for data on a file or stdin and packaging it as messages
* Interfacing with third-party libraries that implement `io.Writer` or `io.Reader`, like [github.com/colinmarc/hdfs](https://github.com/colinmarc/hdfs) or [github.com/mholt/archiver](https://github.com/mholt/archiver).

### Installation

    go get github.com/ThreeDotsLabs/watermill-io

#### Characteristics

This is a very bare-bones implementation for now, so no extra features are supported. However, it is still sufficient for applications like a [CLI producer/consumer](https://github.com/ThreeDotsLabs/watermill/tree/master/tools/mill).

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | no |       |
| ExactlyOnceDelivery | no |  |
| GuaranteedOrder | no |  |
| Persistent | no |   |

#### Configuration

The publisher configuration is relatively simple.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-io/pkg/io/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="// Publisher" %}}
{{% /render-md %}}

The subscriber may work in two modes – either perform buffered reads of constant size from the io.Reader, or split the byte stream into messages using a delimiter byte.

The reading will continue even if the reads come up empty, but they will not be sent out as messages. The time to wait after an empty read is configured through the `PollInterval` parameter. As soon as a non-empty input is read, it will be packaged as a message and sent out.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-io/pkg/io/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="// Subscriber" %}}
{{% /render-md %}}

The continuous reading may be used, for example, to emulate the behaviour of a `tail -f` command, like in this snippet:

{{% render-md %}}
{{% load-snippet-partial file="docs/snippets/tail-log-file/main.go" first_line_contains="// this will" last_line_contains="return false" padding_after="1" %}}
{{% /render-md %}}

#### Marshaling/Unmarshaling

The MarshalFunc is an important part of `io.Publisher`, because it fully controls the format in the underlying `io.Writer` will obtain the messages.

Correspondingly, the UnmarshalFunc regulates how the bytes read by the `io.Reader` will be interpreted as Watermill messages.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-io/pkg/io/marshal.go" first_line_contains="// MarshalMessageFunc" last_line_contains="// PayloadMarshalFunc" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-io/pkg/io/marshal.go" first_line_contains="// UnmarshalMessageFunc" last_line_contains="// PayloadUnmarshalFunc" %}}
{{% /render-md %}}

The package comes with some predefined marshal and unmarshal functions, but you might want to write your own marshaler/unmarshaler to work with the specific implementation of `io.Writer/io.Reader` that you are working with.

#### Topic

For the Publisher/Subscriber implementation itself, the topic has no meaning. It is difficult to interpret the meaning of topic in the general context of `io.Writer` and `io.Reader` interfaces.

However, the topic is passed as a parameter to the marshal/unmarshal functions, so the adaptations to particular `Writer/Reader` implementation may take it into account.
