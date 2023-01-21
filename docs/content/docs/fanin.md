+++
title = "Merging two topics into one"
description = "Merging two topics into one with FanIn component"
date = 2023-01-21T12:47:30+01:00
weight = -100
draft = false
bref = "Merging two topics into one with FanIn component"
toc = true
+++

## FanIn component

FanIn component is a component that merges two topics into one.

### Configuring

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/fanin/fanin.go" first_line_contains="type Config struct {" last_line_contains="CloseTimeout time.Duration" padding_after="1" %}}
{{% /render-md %}}

### Running

You need to provide Publisher and Subscriber implementation for FanIn component.

You can find list of supported Pub/Subs on [Supported Pub/Subs page](/pubsubs/).
Publisher and subscriber can be implemented by different message broker (so you can for example merge Kafka topic with RabbitMQ topic).

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

FanIn component can be stopped by cancelling the context passed to `Run` method or by calling `Close` method.

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/fanin/fanin.go" first_line_contains="func (f *FanIn) Run" last_line_contains=" Close() error" padding_after="2" %}}
{{% /render-md %}}
