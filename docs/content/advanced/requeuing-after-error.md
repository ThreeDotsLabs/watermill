+++
title = "Requeuing After Error"
description = "How to requeue a message after it fails to process"
weight = -20
draft = false
bref = "How to requeue a message after it fails to process"
+++

If a message fails to process (a nack is sent), it usually blocks other messages on the same topic.
If you care about the message order, there's not much you can do about it.
But if you accept that the order might change, you can requeue the failed message back to the tail of the queue.

## Requeuer

The `Requeuer` component is a wrapper on the `Router` that moves messages from one topic to another.

{{% load-snippet-partial file="src-link/components/requeuer/requeuer.go" first_line_contains="type Config" last_line_contains="}" %}}

A trivial usage can look like this. It requeues messages from one topic to the same topic after a delay.

{{< callout "danger" >}}
Using the delay this way is not recommended, as it blocks the entire requeue process for the given time.
{{< /callout >}}

```go
req, err := requeuer.NewRequeuer(requeuer.Config{
    Subscriber:     sub,
    SubscribeTopic: "topic",
    Publisher:      pub,
    GeneratePublishTopic: func(params requeuer.GeneratePublishTopicParams) (string, error) {
        return "topic", nil
    },
    Delay: time.Millisecond * 200,
}, logger)
if err != nil {
	return err
}

err := req.Run(context.Background())
if err != nil {
    return err
}
```

A better way to use the `Requeuer` is to combine it with the `Poison` middleware.
The middleware moves messages to a separate "poison" topic.
Then, the requeuer moves them back to the original topic based on the metadata.

You combine this with a Pub/Sub that supports delayed messages.
See the [full example based on PostgreSQL](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/real-world-examples/delayed-requeue/main.go).

