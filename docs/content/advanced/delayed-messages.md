+++
title = "Delayed Messages"
description = "Receive messages with a delay"
weight = -40
draft = false
bref = "Receive messages with a delay"
+++

Delaying events or commands is a common use case in many applications.
For example, you may want to send the user a reminder after a few days of signing up.
It's not a complex logic to implement, but you can leverage messages to use it out of the box.

## Delay Metadata

Watermill's [`delay`](https://github.com/ThreeDotsLabs/watermill/tree/master/components/delay) package allows you to 
*add delay metadata* to messages.

{{< callout "danger" >}}
**The delay metadata does nothing by itself. You need to use a Pub/Sub implementation that supports it to make it work.**

See below for supported Pub/Subs.
{{< /callout >}}

There are two APIs you can use. If you work with raw messages, use `delay.Message`:

```go
msg := message.NewMessage(watermill.NewUUID(), []byte("hello"))
delay.Message(msg, delay.For(time.Second * 10))
```

If you use the CQRS component, use `delay.WithContext` instead (since you can't access the message directly):

{{% load-snippet-partial file="src-link/_examples/real-world-examples/delayed-messages/main.go" first_line_contains="cmd := SendFeedbackForm" last_line_contains="return err" padding_after="1" %}}

You can also use `delay.Until` instead of `delay.For` to specify `time.Time` instead of `time.Duration`.

## Supported Pub/Subs

* [PostgreSQL](/pubsubs/sql/)
* [MySQL](/pubsubs/sql/)

## Full Example

See the [full example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/delayed-messages) in the Watermill repository.
