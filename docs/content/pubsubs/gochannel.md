+++
title = "Golang Channel"
description = "A Pub/Sub implemented on Golang goroutines and channels"
date = 2019-07-06T22:30:00+02:00
bref = "A Pub/Sub implemented on Golang goroutines and channels"
weight = -100
type = "docs"
toc = false
+++

### Golang Channel

{{% render-md %}}
{{% load-snippet-partial file="src-link/pubsub/gochannel/pubsub.go" first_line_contains="// GoChannel" last_line_contains="type GoChannel struct {" %}}
{{% /render-md %}}

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | no | |
| ExactlyOnceDelivery | yes |  |
| GuaranteedOrder | yes |  |
| Persistent | no| |

#### Configuration

You can inject configuration via the constructor.

{{% render-md %}}
{{% load-snippet-partial file="src-link/pubsub/gochannel/pubsub.go" first_line_contains="func NewGoChannel" last_line_contains="logger:" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="src-link/pubsub/gochannel/pubsub.go" first_line_contains="// Publish" last_line_contains="func (g *GoChannel) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="src-link/pubsub/gochannel/pubsub.go" first_line_contains="// Subscribe" last_line_contains="func (g *GoChannel) Subscribe" %}}
{{% /render-md %}}

#### Marshaler

No marshaling is needed when sending messages within the process.

