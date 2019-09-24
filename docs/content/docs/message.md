+++
title = "Message"
description = "Message is one of core parts of Watermill"
date = 2018-12-05T12:42:40+01:00
weight = -1000
draft = false
bref = "Message is one of core parts of Watermill"
toc = true
+++

### Message

Message is one of core parts of Watermill. Messages are emitted by [*Publishers*]({{< ref "/docs/pub-sub#publisher" >}}) and received by [*Subscribers*]({{< ref "/docs/pub-sub#subscriber" >}}).
When a message is processed, you should send an [`Ack()`]({{< ref "#ack" >}}) or a [`Nack()`]({{< ref "#ack" >}}) when the processing failed.

`Acks` and `Nacks` are processed by Subscribers (in default implementations, the subscribers are waiting for an `Ack` or a `Nack`).

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/message.go" first_line_contains="type Message struct {" last_line_contains="ctx context.Context" padding_after="2" %}}
{{% /render-md %}}

### Ack

#### Sending `Ack`

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/message.go" first_line_contains="// Ack" last_line_contains="func (m *Message) Ack() bool {" padding_after="0" %}}
{{% /render-md %}}


### Nack

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/message.go" first_line_contains="// Nack" last_line_contains="func (m *Message) Nack() bool {" padding_after="0" %}}
{{% /render-md %}}

### Receiving `Ack/Nack`

{{% render-md %}}
{{% load-snippet-partial file="docs/message/receiving-ack.go" first_line_contains="select {" last_line_contains="}" padding_after="0" %}}
{{% /render-md %}}


### Context

Message contains the standard library context, just like an HTTP request.

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/message.go" first_line_contains="// Context" last_line_contains="func (m *Message) SetContext" padding_after="2" %}}
{{% /render-md %}}

