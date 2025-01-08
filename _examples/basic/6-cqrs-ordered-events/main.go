package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	logger := watermill.NewSlogLoggerWithLevelMapping(nil, map[slog.Level]slog.Level{
		slog.LevelInfo: slog.LevelDebug,
	})

	// We are decorating ProtobufMarshaler to add extra metadata to the message.
	cqrsMarshaler := CqrsMarshalerDecorator{
		cqrs.ProtobufMarshaler{
			// It will generate topic names based on the event/command type.
			// So for example, for "RoomBooked" name will be "RoomBooked".
			GenerateName: cqrs.StructName,
		},
	}

	watermillLogger := watermill.NewSlogLoggerWithLevelMapping(
		slog.With("watermill", true),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)

	// This marshaler converts Watermill messages to Kafka messages.
	// We are using it to add partition key to the Kafka message.
	kafkaMarshaler := kafka.NewWithPartitioningMarshaler(GenerateKafkaPartitionKey)

	// You can use any Pub/Sub implementation from here: https://watermill.io/pubsubs/
	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{"kafka:9092"},
			Marshaler: kafkaMarshaler,
		},
		watermillLogger,
	)
	if err != nil {
		panic(err)
	}

	// CQRS is built on messages router. Detailed documentation: https://watermill.io/docs/messages-router/
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	// Simple middleware which will recover panics from event or command handlers.
	// More about router middlewares you can find in the documentation:
	// https://watermill.io/docs/messages-router/#middleware
	//
	// List of available middlewares you can find in message/router/middleware.
	router.AddMiddleware(middleware.Recoverer)
	router.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			slog.Debug("Received message", "metadata", msg.Metadata)
			return h(msg)
		}
	})

	commandBus, err := cqrs.NewCommandBusWithConfig(publisher, cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			// We are using one topic for all commands to maintain the order of commands.
			return "commands", nil
		},
		Marshaler: cqrsMarshaler,
		Logger:    logger,
	})
	if err != nil {
		panic(err)
	}

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			// We are using one topic for all events to maintain the order of events.
			return "events", nil
		},
		Marshaler: cqrsMarshaler,
		Logger:    logger,
	})
	if err != nil {
		panic(err)
	}

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "commands", nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return kafka.NewSubscriber(
					kafka.SubscriberConfig{
						Brokers:       []string{"kafka:9092"},
						ConsumerGroup: params.HandlerName,
						Unmarshaler:   kafkaMarshaler,
					},
					watermillLogger,
				)
			},
			Marshaler: cqrsMarshaler,
			Logger:    logger,
		},
	)
	if err != nil {
		panic(err)
	}

	eventProcessor, err := cqrs.NewEventGroupProcessorWithConfig(
		router,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return kafka.NewSubscriber(
					kafka.SubscriberConfig{
						Brokers:       []string{"kafka:9092"},
						ConsumerGroup: params.EventGroupName,
						Unmarshaler:   kafkaMarshaler,
					},
					watermillLogger,
				)
			},
			Marshaler: cqrsMarshaler,
			Logger:    logger,
		},
	)
	if err != nil {
		panic(err)
	}

	err = commandProcessor.AddHandlers(
		cqrs.NewCommandHandler("SubscribeHandler", SubscribeHandler{eventBus}.Handle),
		cqrs.NewCommandHandler("UnsubscribeHandler", UnsubscribeHandler{eventBus}.Handle),
		cqrs.NewCommandHandler("UpdateEmailHandler", UpdateEmailHandler{eventBus}.Handle),
	)
	if err != nil {
		panic(err)
	}

	subscribersReadModel := NewSubscriberReadModel()

	// All messages from this group will have one subscription.
	// When message arrives, Watermill will match it with the correct handler.
	err = eventProcessor.AddHandlersGroup(
		"SubscriberReadModel",
		cqrs.NewGroupEventHandler(subscribersReadModel.OnSubscribed),
		cqrs.NewGroupEventHandler(subscribersReadModel.OnUnsubscribed),
		cqrs.NewGroupEventHandler(subscribersReadModel.OnEmailUpdated),
	)
	if err != nil {
		panic(err)
	}

	activityReadModel := NewActivityTimelineModel()

	// All messages from this group will have one subscription.
	// When message arrives, Watermill will match it with the correct handler.
	err = eventProcessor.AddHandlersGroup(
		"ActivityTimelineReadModel",
		cqrs.NewGroupEventHandler(activityReadModel.OnSubscribed),
		cqrs.NewGroupEventHandler(activityReadModel.OnUnsubscribed),
		cqrs.NewGroupEventHandler(activityReadModel.OnEmailUpdated),
	)
	if err != nil {
		panic(err)
	}

	slog.Info("Starting service")

	go simulateTraffic(commandBus)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

func simulateTraffic(commandBus *cqrs.CommandBus) {
	for i := 0; ; i++ {
		subscriberID := watermill.NewUUID()

		err := commandBus.Send(context.Background(), &Subscribe{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
			Email:        fmt.Sprintf("user%d@example.com", i),
		})
		if err != nil {
			slog.Error("Error sending Subscribe command", "err", err)
		}
		time.Sleep(time.Millisecond * 500)

		err = commandBus.Send(context.Background(), &UpdateEmail{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
			NewEmail:     fmt.Sprintf("updated%d@example.com", i),
		})
		if err != nil {
			slog.Error("Error sending UpdateEmail command", "err", err)
		}
		time.Sleep(time.Millisecond * 500)

		if i%3 == 0 {
			err = commandBus.Send(context.Background(), &Unsubscribe{
				Metadata:     GenerateMessageMetadata(subscriberID),
				SubscriberId: subscriberID,
			})
			if err != nil {
				slog.Error("Error sending Unsubscribe command", "err", err)
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
}

type SubscribeHandler struct {
	eventBus *cqrs.EventBus
}

func (h SubscribeHandler) Handle(ctx context.Context, cmd *Subscribe) error {
	return h.eventBus.Publish(ctx, &SubscriberSubscribed{
		Metadata:     GenerateMessageMetadata(cmd.SubscriberId),
		SubscriberId: cmd.SubscriberId,
		Email:        cmd.Email,
	})
}

type UnsubscribeHandler struct {
	eventBus *cqrs.EventBus
}

func (h UnsubscribeHandler) Handle(ctx context.Context, cmd *Unsubscribe) error {
	return h.eventBus.Publish(ctx, &SubscriberUnsubscribed{
		Metadata:     GenerateMessageMetadata(cmd.SubscriberId),
		SubscriberId: cmd.SubscriberId,
	})
}

type UpdateEmailHandler struct {
	eventBus *cqrs.EventBus
}

func (h UpdateEmailHandler) Handle(ctx context.Context, cmd *UpdateEmail) error {
	return h.eventBus.Publish(ctx, &SubscriberEmailUpdated{
		Metadata:     GenerateMessageMetadata(cmd.SubscriberId),
		SubscriberId: cmd.SubscriberId,
		NewEmail:     cmd.NewEmail,
	})
}
