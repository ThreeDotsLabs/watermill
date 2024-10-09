+++
title = "FanOut (multiplying messages)"
description = "FanOut is a component that receives messages from the subscriber and passes them to all publishers."
date = 2024-10-09T02:47:30+01:00
weight = -50
draft = false
bref = "FanOut is a component that receives messages from the subscriber and passes them to all publishers."
+++

## FanOut component

FanOut is a component that receives messages from a topic and passes them to all subscribers. In effect, messages are "multiplied".

A typical use case for using FanOut is having one external subscription and multiple workers
inside the process.

### Configuring

{{% load-snippet-partial file="src-link/pubsub/gochannel/fanout.go" first_line_contains="// NewFanOut" last_line_contains=")" padding_after="0" %}}

You need to call AddSubscription method for all topics that you want to listen to.
This needs to be done *before* starting the FanOut.

{{% load-snippet-partial file="src-link/pubsub/gochannel/fanout.go" first_line_contains="// AddSubscription" last_line_contains=")" padding_after="0" %}}

### Running

{{% load-snippet-partial file="src-link/pubsub/gochannel/fanout.go" first_line_contains="// Run" last_line_contains=")" padding_after="0" %}}

Then, use it as any other `message.Subscriber`.
