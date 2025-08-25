+++
title = "Amazon AWS SNS/SQS"
description = "AWS SQS and SNS are fully-managed message queuing and Pub/Sub-like services that make it easy to decouple and scale microservices, distributed systems, and serverless applications."
date = 2024-10-19T15:30:00+02:00
bref = "AWS SQS and SNS are fully-managed message queuing and Pub/Sub-like services that make it easy to decouple and scale microservices, distributed systems, and serverless applications."
weight = 10
+++

AWS SQS and SNS are fully-managed message queuing and Pub/Sub-like services that make it easy to decouple
and scale microservices, distributed systems, and serverless applications.

Watermill provides a simple way to use AWS SQS and SNS with Go.
It handles all the AWS SDK internals and provides a simple API to publish and subscribe messages.

Official Documentation:
- [SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [SNS](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)

You can find a fully functional example with AWS SNS in the Watermill examples:
- [SNS](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/pubsubs/aws-sns)
- [SQS](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/pubsubs/aws-sqs)

## Installation

```bash
go get github.com/ThreeDotsLabs/watermill-aws
```

## SQS vs SNS

While both SQS and SNS are messaging services provided by AWS, they serve different purposes and are best suited for different scenarios in your Watermill applications.

### How SNS is connected with SQS

To use SNS as a Pub/Sub (to have multiple subscribers receiving the same message), you need to create an SNS topic and subscribe to SQS queues.
When a message is published to the SNS topic, it will be delivered to all subscribed SQS queues.
We implemented this logic in the `watermill-aws` package out of the box.

When you subscribe to an SNS topic, Watermill AWS creates an SQS queue and subscribes to it.

[![](https://mermaid.ink/img/pako:eNptkU1uwyAQRq-CWLWScwEW2TjbVnboru4Cw9hB5ccZoFIV5e7FxVRKUjaM5j0-jZgLlV4BZTTAOYGTcNBiRmEHR_JZBEYt9SJcJB0RgfBXTro0Gh1OgI_OijfrzS9a_mP0xchXnyABeXcfj1ZbU3gag0Q9AhaxqN1uv8-U1VGIhRDEDKHgjFahzwIHpyot0Hi_lKqtUueNIZPH-5ie77LSMnKEmNDd5rQFdehlblf2FJ7vwg9gIMLt2zV5w0ew_usPkwm9Jef1Y4qZx6cNtYBWaJW3dFnbA40nsDBQlksl8HOgg7tmT6To-beTlEVM0NC0KBHrRimbhAm5C0pHjy9l7b_bbyj6NJ824_oDU0CtBA?type=png)](https://mermaid.live/edit#pako:eNptkU1uwyAQRq-CWLWScwEW2TjbVnboru4Cw9hB5ccZoFIV5e7FxVRKUjaM5j0-jZgLlV4BZTTAOYGTcNBiRmEHR_JZBEYt9SJcJB0RgfBXTro0Gh1OgI_OijfrzS9a_mP0xchXnyABeXcfj1ZbU3gag0Q9AhaxqN1uv8-U1VGIhRDEDKHgjFahzwIHpyot0Hi_lKqtUueNIZPH-5ie77LSMnKEmNDd5rQFdehlblf2FJ7vwg9gIMLt2zV5w0ew_usPkwm9Jef1Y4qZx6cNtYBWaJW3dFnbA40nsDBQlksl8HOgg7tmT6To-beTlEVM0NC0KBHrRimbhAm5C0pHjy9l7b_bbyj6NJ824_oDU0CtBA)

We can say, that a single SQS queue acts as a consumer group or subscription in other Pub/Sub implementations.

The mechanism is detailed in [AWS documentation](https://docs.aws.amazon.com/sns/latest/dg/subscribe-sqs-queue-to-sns-topic.html).

### How to choose between SQS and SNS

#### SQS (Simple Queue Service)

- Use when you need a simple message queue with a single consumer.
- Great for task queues or background job processing.
- Supports exactly-once processing (with FIFO queues) and guaranteed order (mostly).

Example use case: Processing user uploads in the background.

[![](https://mermaid.ink/img/pako:eNplkT1uwzAMRq8icGoB5wIasjhrASdevdASHQvVj0NJAYogd69c2wmaaCL4HilI3w1U0AQSIl0yeUUHg2dG13lRzoScjDIT-iQagVG0x1Y0ubcmjsTvzoxX65gp07tRb7zNfVRs-nnNojW7_b4QuV0gHMWIZ4oLtiFMS1U_xGCtGAK_mIXtilJLcaKU2W_4OV1Qw0GV9sY-4ufL8gNZSvR_dt684hO5cH1gMXBw4vJ8M3kNFThih0aX773N7Q7SSI46kKXUyN8ddP5ePMwptD9egUycqYI8aUxbFCAHtLF0SZsU-GvJ6y-2Cjjk87ga91_dnpaW?type=png)](https://mermaid.live/edit#pako:eNplkT1uwzAMRq8icGoB5wIasjhrASdevdASHQvVj0NJAYogd69c2wmaaCL4HilI3w1U0AQSIl0yeUUHg2dG13lRzoScjDIT-iQagVG0x1Y0ubcmjsTvzoxX65gp07tRb7zNfVRs-nnNojW7_b4QuV0gHMWIZ4oLtiFMS1U_xGCtGAK_mIXtilJLcaKU2W_4OV1Qw0GV9sY-4ufL8gNZSvR_dt684hO5cH1gMXBw4vJ8M3kNFThih0aX773N7Q7SSI46kKXUyN8ddP5ePMwptD9egUycqYI8aUxbFCAHtLF0SZsU-GvJ6y-2Cjjk87ga91_dnpaW)

#### SNS (Simple Notification Service)

- Use when you need to broadcast messages to multiple subscribers.
- Perfect for implementing pub/sub patterns.
- Useful for event-driven architectures.
- Supports multiple types of subscribers (SQS, Lambda, HTTP/S, email, SMS, etc.).

Example use case: Notifying multiple services about a new user registration.

Our SNS implementation in Watermill automatically creates and manages SQS queues for each subscriber, simplifying the process of using SNS with multiple SQS queues.

Remember, you can use both in the same application where appropriate. For instance, you might use SNS to broadcast events and SQS to process specific tasks triggered by those events.

To learn how SNS and SQS work together, see the [How SNS is connected with SQS](#how-sns-is-connected-with-sqs) section.

## SQS

### Characteristics

| Feature             | Implements | Note                                                                                                                                                                                                                                                                                                                                                                                      |
|---------------------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ConsumerGroups      | no         | it's a queue, for consumer groups-like functionality use [SNS](#sns)                                                                                                                                                                                                                                                                                                                      |
| ExactlyOnceDelivery | no         | [yes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html)                                                                                                                                                                                                                                                                |
| GuaranteedOrder     | yes\*      | from [AWS Docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html): _"(...) due to the highly distributed architecture, more than one copy of a message might be delivered, and messages may occasionally arrive out of order. Despite this, standard queues make a best-effort attempt to maintain the order in which messages are sent."_ |
| Persistent          | yes        |                                                                                                                                                                                                                                                                                                                                                                                           |

### Required permissions

- `"sqs:ReceiveMessage"`
- `"sqs:DeleteMessage"`
- `"sqs:GetQueueUrl"`
- `"sqs:CreateQueue"`
- `"sqs:GetQueueAttributes"`
- `"sqs:SendMessage"`
- `"sqs:ChangeMessageVisibility"`

[todo - verify]

### SQS Configuration

{{% load-snippet-partial file="src-link/watermill-aws/sqs/config.go" first_line_contains="type SubscriberConfig struct " last_line_contains="type GenerateCreateQueueInputFunc" %}}

### Resolving Queue URL

In the Watermill model, we are normalizing the AWS queue url to `topic` used in the `Publish` and `Subscribe` methods.

To give you flexibility of what you want to use as a topic in Watermill, you can customize resolving the queue URL.

{{% load-snippet-partial file="src-link/watermill-aws/sqs/url_resolver.go" first_line_contains="// QueueUrlResolver" last_line_contains="GenerateQueueUrlResolver" %}}

You can implement your own `QueueUrlResolver` or use one of the provided resolvers.

By default, `GetQueueUrlByNameUrlResolver` resolver is used:

{{% load-snippet-partial file="src-link/watermill-aws/sqs/url_resolver.go" first_line_contains="// GetQueueUrlByNameUrlResolver " last_line_contains="NewGetQueueUrlByNameUrlResolver" %}}

There are two more resolvers available:

{{% load-snippet-partial file="src-link/watermill-aws/sqs/url_resolver.go" first_line_contains="// GenerateQueueUrlResolver" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-aws/sqs/url_resolver.go" first_line_contains="// TransparentUrlResolver" last_line_contains="}" %}}

### Using with SQS emulator

You may want to use [`goaws`](https://github.com/Admiral-Piett/goaws) or [`localstack`](https://hub.docker.com/r/localstack/localstack) for local development or testing.

You can override the endpoint using the `OptFns` option in the `SubscriberConfig` or `PublisherConfig`.

```go
package main

import (
    amazonsqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
)

func main() {
	// ...

    sqsOpts := []func(*amazonsqs.Options){
        amazonsqs.WithEndpointResolverV2(sqs.OverrideEndpointResolver{
            Endpoint: transport.Endpoint{
                URI: *lo.Must(url.Parse("http://localstack:4566")),
            },
        }),
    }

	sqsConfig := sqs.SubscriberConfig{
		AWSConfig: cfg,
		OptFns:    sqsOpts,
	}

	sub, err := sqs.NewSubscriber(sqsConfig, logger)
	if err != nil {
		panic(fmt.Errorf("unable to create new subscriber: %w", err))
	}

	// ...
}

```

## SNS

### Characteristics

| Feature             | Implements | Note                                                                                                                                                                                                                                                                                                                                                                                      |
|---------------------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ConsumerGroups      | yes        | yes                                                                                                                                                                                                                                                                                                                                                                                       |
| ExactlyOnceDelivery | no         | [yes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html)                                                                                                                                                                                                                                                                |
| GuaranteedOrder     | yes\*      | from [AWS Docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html): _"(...) due to the highly distributed architecture, more than one copy of a message might be delivered, and messages may occasionally arrive out of order. Despite this, standard queues make a best-effort attempt to maintain the order in which messages are sent."_ |
| Persistent          | yes        |                                                                                                                                                                                                                                                                                                                                                                                           |


### Required permissions

- `sns:Subscribe`
- `sns:ConfirmSubscription`
- `sns:Receive`
- `sns:Unsubscribe`

and all permissions required for SQS:

- `sqs:ReceiveMessage`
- `sqs:DeleteMessage`
- `sqs:GetQueueUrl`
- `sqs:CreateQueue`
- `sqs:GetQueueAttributes`
- `sqs:SendMessage`
- `sqs:ChangeMessageVisibility`
- `sqs:SetQueueAttributes`

Additionally, if `sns.SubscriberConfig.DoNotSetQueueAccessPolicy` is not enabled, you should have the following:

- `sqs:SetQueueAttributes`

### SNS Configuration

{{% load-snippet-partial file="src-link/watermill-aws/sns/config.go" first_line_contains="type SubscriberConfig struct " last_line_contains="type GenerateSqsQueueNameFn" %}}

Additionally, because SNS Subscriber uses SQS queues as "subscriptions", you need to pass [SQS configuration](#sqs-configuration) as well.

### Resolving Queue URL

In the Watermill model, we normalise AWS Topic ARN to the `topic` used in the `Publish` and `Subscribe` methods.

{{% load-snippet-partial file="src-link/watermill-aws/sns/topic.go" first_line_contains="// TopicResolver" last_line_contains="}" %}}

We are providing two out-of-the-box resolvers:

{{% load-snippet-partial file="src-link/watermill-aws/sns/topic.go" first_line_contains="// TransparentTopicResolver" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-aws/sns/topic.go" first_line_contains="// GenerateArnTopicResolver" last_line_contains="}" %}}

### Using with SNS emulator

You may want to use [`goaws`](https://github.com/Admiral-Piett/goaws) or [`localstack`](https://hub.docker.com/r/localstack/localstack) for local development or testing.

You can override the endpoint using the `OptFns` option in the `SubscriberConfig` or `PublisherConfig`.

```go
package main

import (
	amazonsns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/ThreeDotsLabs/watermill-amazonsns/sns"
)

func main() {
	// ...

    snsOpts := []func(*amazonsns.Options){
        amazonsns.WithEndpointResolverV2(sns.OverrideEndpointResolver{
            Endpoint: transport.Endpoint{
                URI: *lo.Must(url.Parse("http://localstack:4566")),
            },
        }),
    }

	snsConfig := sns.SubscriberConfig{
		AWSConfig: cfg,
		OptFns:    snsOpts,
	}

	sub, err := sns.NewSubscriber(snsConfig, sqsConfig, logger)
	if err != nil {
		panic(fmt.Errorf("unable to create new subscriber: %w", err))
	}

	// ...
}
```
