+++
title = "Merging two topics into one"
description = "Merging two topics into one with the FanIn component"
date = 2023-01-21T12:47:30+01:00
weight = -100
draft = false
bref = "Merging two topics into one with the FanIn component"
toc = true
+++

## FanIn component

The FanIn component merges two topics into one.

### Configuring

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/fanin/fanin.go" first_line_contains="type Config struct {" last_line_contains="CloseTimeout time.Duration" padding_after="1" %}}
{{% /render-md %}}

### Running

You need to provide a Publisher and a Subscriber implementation for the FanIn component.

You can find the list of supported Pub/Subs on [Supported Pub/Subs page](/pubsubs/).
The Publisher and subscriber can be implemented by different message brokers (for example, you can merge a Kafka topic with a RabbitMQ topic).

```go

logger := watermill.NewStdLogger(false, false)

// create Publisher and Subscriber
pub, err := // ...
sub, err := // ...

fi, err := fanin.NewFanIn(
    sub,
    pub,
    fanin.Config{
        SourceTopics: upstreamTopics,
        TargetTopic:  downstreamTopic,
    },
    logger,
)
if err != nil {
    panic(err)
}

if err := fi.Run(context.Background()); err != nil {
    panic(err)
}
```

### Controlling FanIn component

The FanIn component can be stopped by cancelling the context passed to the `Run` method or by calling the `Close` method.

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/fanin/fanin.go" first_line_contains="func (f *FanIn) Run" last_line_contains=" Close() error" padding_after="2" %}}
{{% /render-md %}}
