+++
title = "HTTP"
description = "Call and listen to webhooks asynchronously"
date = 2019-07-06T22:30:00+02:00
bref = "Call and listen to webhooks asynchronously"
weight = -70
type = "docs"
toc = false
+++

### HTTP

The HTTP subscriber listens to HTTP requests (for example - webhooks) and outputs them as messages.
You can then post them to any Publisher. Here is an example with [sending HTTP messages to Kafka](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/real-world-examples/receiving-webhooks/main.go).

The HTTP publisher sends HTTP requests as specified in its configuration. Here is an example with [transforming Kafka messages into HTTP webhook requests](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/sending-webhooks).

### Installation

    go get github.com/ThreeDotsLabs/watermill-http

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
{{% load-snippet-partial file="src-link/watermill-http/pkg/http/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

You can use the `Router` config option to `SubscriberConfig` to pass your own `chi.Router` (see [chi](https://github.com/go-chi/chi)).
This may be helpful if you'd like to add your own HTTP handlers (e.g. a health check endpoint).

#### Publisher configuration

Publisher configuration is done via the config struct passed to the constructor:

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-http/pkg/http/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

How the message topic and body translate into the URL, method, headers, and payload of the HTTP request is highly configurable through the use of `MarshalMessageFunc`. 
Use the provided `DefaultMarshalMessageFunc` to send POST requests to a specific url:

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-http/pkg/http/publisher.go" first_line_contains="// MarshalMessageFunc" last_line_contains="return req, nil" padding_after="2" %}}
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
{{% load-snippet-partial file="src-link/watermill-http/pkg/http/subscriber.go" first_line_contains="// Subscribe adds" last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

