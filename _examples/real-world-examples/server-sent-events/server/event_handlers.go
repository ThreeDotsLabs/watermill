package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/stan.go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

const (
	PostCreatedTopic = "post-created"
	PostUpdatedTopic = "post-updated"
	FeedUpdatedTopic = "feed-updated"
)

func SetupMessageRouter(
	feedsStorage FeedsStorage,
	logger watermill.LoggerAdapter,
) (*message.Router, message.Publisher, message.Subscriber, error) {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, nil, nil, err
	}
	router.AddMiddleware(middleware.Recoverer)

	pub, err := nats.NewStreamingPublisher(nats.StreamingPublisherConfig{
		ClusterID: "test-cluster",
		ClientID:  "publisher",
		StanOptions: []stan.Option{
			stan.NatsURL("nats://nats-streaming:4222"),
		},
		Marshaler: nats.GobMarshaler{},
	}, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	sub, err := nats.NewStreamingSubscriber(nats.StreamingSubscriberConfig{
		ClusterID:  "test-cluster",
		ClientID:   "subscriber",
		QueueGroup: "example",
		StanOptions: []stan.Option{
			stan.NatsURL("nats://nats-streaming:4222"),
		},
		DurableName: "durable",
		Unmarshaler: nats.GobMarshaler{},
	}, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	publishEvents := func(ctx context.Context, tags []string) (messages []*message.Message, err error) {
		defer func() {
			if err == nil {
				logger.Info("Updated posts in feeds", nil)
			} else {
				logger.Error("Error in handler", err, nil)
			}
		}()

		for _, tag := range tags {
			logger.Info("Producing event", watermill.LogFields{"tag": tag})
			event := FeedUpdated{
				Name:       tag,
				OccurredAt: time.Now().UTC(),
			}

			payload, err := json.Marshal(event)
			if err != nil {
				return nil, err
			}

			newMessage := message.NewMessage(watermill.NewUUID(), payload)

			messages = append(messages, newMessage)
		}

		return messages, nil
	}

	router.AddHandler(
		"on-post-created",
		PostCreatedTopic,
		sub,
		FeedUpdatedTopic,
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			event := PostCreated{}
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return nil, err
			}

			for _, tag := range event.Post.Tags {
				logger.Info("Adding tag", watermill.LogFields{"tag": tag})
				err = feedsStorage.Add(msg.Context(), tag)
				if err != nil {
					return nil, err
				}
			}

			err = feedsStorage.AppendPost(msg.Context(), event.Post)
			if err != nil {
				return nil, err
			}

			return publishEvents(msg.Context(), event.Post.Tags)
		},
	)

	router.AddHandler(
		"on-post-updated",
		PostUpdatedTopic,
		sub,
		FeedUpdatedTopic,
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			event := PostUpdated{}
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return nil, err
			}

			for _, tag := range event.NewPost.Tags {
				logger.Info("Adding tag", watermill.LogFields{"tag": tag})
				err = feedsStorage.Add(msg.Context(), tag)
				if err != nil {
					return nil, err
				}
			}

			// TODO handle post removal

			err = feedsStorage.UpdatePost(msg.Context(), event.NewPost)
			if err != nil {
				return nil, err
			}

			return publishEvents(msg.Context(), append(event.NewPost.Tags, event.OriginalPost.Tags...))
		},
	)

	go func() {
		err = router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-router.Running()

	return router, pub, sub, nil
}

type Publisher struct {
	publisher message.Publisher
}

func (p Publisher) Publish(topic string, event interface{}) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)

	return p.publisher.Publish(topic, msg)
}
