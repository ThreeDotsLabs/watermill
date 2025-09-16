// Sources for https://watermill.io/learn/getting-started/
package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	amazonsns "github.com/aws/aws-sdk-go-v2/service/sns"
	amazonsqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	transport "github.com/aws/smithy-go/endpoints"
	"github.com/samber/lo"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-aws/sns"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	snsOpts := []func(*amazonsns.Options){
		amazonsns.WithEndpointResolverV2(sns.OverrideEndpointResolver{
			Endpoint: transport.Endpoint{
				URI: *lo.Must(url.Parse("http://localstack:4566")),
			},
		}),
	}

	sqsOpts := []func(*amazonsqs.Options){
		amazonsqs.WithEndpointResolverV2(sqs.OverrideEndpointResolver{
			Endpoint: transport.Endpoint{
				URI: *lo.Must(url.Parse("http://localstack:4566")),
			},
		}),
	}

	topicResolver, err := sns.NewGenerateArnTopicResolver("000000000000", "us-east-1")
	if err != nil {
		panic(err)
	}

	newSubscriber := func(name string) (message.Subscriber, error) {
		subscriberConfig := sns.SubscriberConfig{
			AWSConfig: aws.Config{
				Credentials: aws.AnonymousCredentials{},
			},
			OptFns:        snsOpts,
			TopicResolver: topicResolver,
			GenerateSqsQueueName: func(ctx context.Context, snsTopic sns.TopicArn) (string, error) {
				topic, err := sns.ExtractTopicNameFromTopicArn(snsTopic)
				if err != nil {
					return "", err
				}

				return fmt.Sprintf("%v-%v", topic, name), nil
			},
		}

		sqsSubscriberConfig := sqs.SubscriberConfig{
			AWSConfig: aws.Config{
				Credentials: aws.AnonymousCredentials{},
			},
			OptFns: sqsOpts,
		}

		return sns.NewSubscriber(subscriberConfig, sqsSubscriberConfig, logger)
	}

	subA, err := newSubscriber("subA")
	if err != nil {
		panic(err)
	}

	subB, err := newSubscriber("subB")
	if err != nil {
		panic(err)
	}

	messagesA, err := subA.Subscribe(context.Background(), "example-topic")
	if err != nil {
		panic(err)
	}

	messagesB, err := subB.Subscribe(context.Background(), "example-topic")
	if err != nil {
		panic(err)
	}

	go process("A", messagesA)
	go process("B", messagesB)

	publisherConfig := sns.PublisherConfig{
		AWSConfig: aws.Config{
			Credentials: aws.AnonymousCredentials{},
		},
		OptFns:        snsOpts,
		TopicResolver: topicResolver,
	}

	publisher, err := sns.NewPublisher(publisherConfig, logger)
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))

		if err := publisher.Publish("example-topic", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func process(prefix string, messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("%v received message: %s, payload: %s", prefix, msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
