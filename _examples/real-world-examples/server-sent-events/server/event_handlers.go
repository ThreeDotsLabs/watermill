package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/nats-io/stan.go"
)

const (
	PostCreatedTopic = "post-created"
	PostUpdatedTopic = "post-updated"
	FeedUpdatedTopic = "feed-updated"
)

func SetupMessageRouter(
	feedsStorage FeedsStorage,
	logger watermill.LoggerAdapter,
) (message.Publisher, message.Subscriber, error) {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, nil, err
	}
	router.AddMiddleware(middleware.Recoverer)

	natsURL := stan.NatsURL("nats://nats-streaming:4222")
	pub, err := nats.NewStreamingPublisher(nats.StreamingPublisherConfig{
		ClusterID:   "test-cluster",
		ClientID:    "publisher",
		StanOptions: []stan.Option{natsURL},
		Marshaler:   nats.GobMarshaler{},
	}, logger)
	if err != nil {
		return nil, nil, err
	}

	sub, err := nats.NewStreamingSubscriber(nats.StreamingSubscriberConfig{
		ClusterID:   "test-cluster",
		ClientID:    "subscriber",
		StanOptions: []stan.Option{natsURL},
		Unmarshaler: nats.GobMarshaler{},
	}, logger)
	if err != nil {
		return nil, nil, err
	}

	router.AddHandler(
		"update-feeds-on-post-created",
		PostCreatedTopic,
		sub,
		FeedUpdatedTopic,
		pub,
		func(msg *message.Message) (messages []*message.Message, err error) {
			defer func() {
				if err == nil {
					logger.Info("Successfully updated feeds on new post created", nil)
				} else {
					logger.Error("Error while updating feeds on new post created", err, nil)
				}
			}()

			event := PostCreated{}
			err = json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return nil, err
			}

			logger.Info("Adding post", watermill.LogFields{"post": event.Post})

			if len(event.Post.Tags) > 0 {
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
			}

			return createFeedUpdatedEvents(event.Post.Tags)
		},
	)

	router.AddHandler(
		"update-feeds-on-post-updated",
		PostUpdatedTopic,
		sub,
		FeedUpdatedTopic,
		pub,
		func(msg *message.Message) (messages []*message.Message, err error) {
			defer func() {
				if err == nil {
					logger.Info("Successfully updated feeds on post updated", nil)
				} else {
					logger.Error("Error while updating feeds on post updated", err, nil)
				}
			}()

			event := PostUpdated{}
			err = json.Unmarshal(msg.Payload, &event)
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

			err = feedsStorage.UpdatePost(msg.Context(), event.NewPost)
			if err != nil {
				return nil, err
			}

			return createFeedUpdatedEvents(append(event.NewPost.Tags, event.OriginalPost.Tags...))
		},
	)

	go func() {
		err = router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-router.Running()

	return pub, sub, nil
}

func createFeedUpdatedEvents(tags []string) ([]*message.Message, error) {
	var messages []*message.Message

	for _, tag := range tags {
		event := FeedUpdated{
			Name:       tag,
			OccurredAt: time.Now().UTC(),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)

		messages = append(messages, msg)
	}

	return messages, nil
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
