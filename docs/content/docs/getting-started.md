+++
title = "Getting started"
description = "Up and running in under a minute"
weight = -9999
draft = false
toc = true
bref = "todo"
type = "docs"
+++

### Install

```bash
go get -u github.com/ThreeDotsLabs/watermill/
```

### Subscribing for messages

{{% tabs id="subscribing" tabs="go-channel,kafka" labels="Go Channel,Kafka" %}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% /tabs %}}

### Publishing messages

{{% tabs id="publishing" tabs="go-channel,kafka" labels="Go Channel,Kafka" %}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel.go" first_line_contains="go process(messages)" last_line_contains="func process(messages" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka.go" first_line_contains="go process(messages)" last_line_contains="func process(messages" %}}
{{% /tabs-tab %}}

{{% /tabs %}}


### Using *Messages Router*

### What next?