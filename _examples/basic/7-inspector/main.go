// This example runs five small services and serves the Watermill Inspector for all of them.
//
//	orders:        PlaceOrder command -> OrderPlaced event (through a PostgreSQL outbox and the Forwarder);
//	               marks orders as paid and shipped
//	payments:      OrderPlaced event -> payment (fails sometimes) -> PaymentCharged event, or the poison queue
//	shipping:      PaymentCharged event -> PackOrder command -> OrderShipped event
//	notifications: e-mails about placed and shipped orders
//	webhooks:      webhooks merged by a FanIn, normalized, and delivered through a FanOut
//	               to the ledger and, through Kafka, to the payments service
//
// The services talk to each other through Kafka. Each of them serves its own inspector,
// and they find each other through a Kafka topic, so every inspector shows all running services.
//
// Run everything with `docker compose up`, and open http://localhost:8787.
//
// A service can also be run on the host, with Kafka and PostgreSQL from Docker Compose:
//
//	go run . -service orders
package main

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/fanin"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/components/inspector"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

const (
	outboxTopic = "events_outbox"

	// inspectorToken is shared by all services. Inspectors use it to trust each other.
	inspectorToken = "inspector-example"
)

// The defaults are for running a service on the host. Docker Compose sets the environment variables.
var (
	kafkaBrokers = strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ",")
	postgresDSN  = env("POSTGRES_DSN", "postgres://watermill:password@localhost:15432/watermill?sslmode=disable")
)

var defaultAddrs = map[string]string{
	"orders":        "localhost:8787",
	"payments":      "localhost:8788",
	"webhooks":      "localhost:8789",
	"shipping":      "localhost:8790",
	"notifications": "localhost:8791",
}

type PlaceOrder struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

type OrderPlaced struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

type PaymentCharged struct {
	OrderID string `json:"order_id"`
}

type PackOrder struct {
	OrderID string `json:"order_id"`
}

type OrderShipped struct {
	OrderID string `json:"order_id"`
	Carrier string `json:"carrier"`
}

// component is something that has to run next to the router, like the Forwarder.
type component interface {
	Run(ctx context.Context) error
	Running() chan struct{}
}

func main() {
	service := flag.String("service", "", "service to run: orders, payments, shipping, notifications or webhooks")
	addr := flag.String("addr", "", "address the inspector listens on (each service has its own default)")
	flag.Parse()

	if _, ok := defaultAddrs[*service]; !ok {
		fmt.Println("Usage: go run . -service orders|payments|shipping|notifications|webhooks")
		return
	}
	if *addr == "" {
		*addr = defaultAddrs[*service]
	}
	// The address of the inspector that other services can reach.
	advertiseURL := env("INSPECTOR_ADVERTISE_URL", "http://"+*addr+"/")

	logger := watermill.NewStdLogger(false, false)

	kafkaPublisher, err := kafka.NewPublisher(kafka.PublisherConfig{Brokers: kafkaBrokers}, logger)
	must(err)

	// The inspector has to be created before routers and Pub/Subs.
	// Without Discovery, Token and AdvertiseURL it shows only this service.
	inspectorHandler, err := inspector.NewHandler(inspector.Config{
		ServiceName: *service,
		// Payloads of messages are not shown by default.
		Payloads: true,

		// Services announce their inspectors on a Kafka topic.
		Discovery:    inspector.NewPubSubDiscovery(kafkaPublisher, newDiscoverySubscriber(logger), ""),
		Token:        inspectorToken,
		AdvertiseURL: advertiseURL,
		Logger:       logger,
	})
	must(err)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	must(err)

	var components []component
	var start func(ctx context.Context)

	switch *service {
	case "orders":
		components, start = setUpOrders(router, kafkaPublisher, logger)
	case "payments":
		components, start = setUpPayments(router, kafkaPublisher, logger)
	case "shipping":
		components, start = setUpShipping(router, kafkaPublisher, logger)
	case "notifications":
		components, start = setUpNotifications(router, logger)
	case "webhooks":
		components, start = setUpWebhooks(router, kafkaPublisher, logger)
	}

	ctx := context.Background()
	go func() { must(router.Run(ctx)) }()
	<-router.Running()

	for _, c := range components {
		go func() { must(c.Run(ctx)) }()
		<-c.Running()
	}

	if start != nil {
		go start(ctx)
	}

	fmt.Printf("Inspector of %s: http://%s\n", *service, *addr)
	must(http.ListenAndServe(*addr, inspectorHandler))
}

// setUpOrders adds the handlers of the orders service.
//
// Commands are sent through GoChannel. OrderPlaced is published with the outbox pattern:
// it's stored in a PostgreSQL table first, and the Forwarder moves it to Kafka.
func setUpOrders(router *message.Router, kafkaPublisher message.Publisher, logger watermill.LoggerAdapter) ([]component, func(context.Context)) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	outboxPublisher, outboxForwarder := setUpOutbox(kafkaPublisher, logger)

	commandBus := newCommandBus(pubSub, logger)
	commandProcessor := newCommandProcessor(router, pubSub, logger)

	outboxEventBus := newEventBus(outboxPublisher, logger)

	must(commandProcessor.AddHandlers(
		cqrs.NewCommandHandler("PlaceOrderHandler", func(ctx context.Context, cmd *PlaceOrder) error {
			work()
			return outboxEventBus.Publish(ctx, OrderPlaced{OrderID: cmd.OrderID, Amount: cmd.Amount})
		}),
	))

	must(newEventProcessor(router, logger).AddHandlers(
		cqrs.NewEventHandler("MarkOrderAsPaid", func(ctx context.Context, event *PaymentCharged) error {
			work()
			return nil
		}),
		cqrs.NewEventHandler("MarkOrderAsShipped", func(ctx context.Context, event *OrderShipped) error {
			work()
			return nil
		}),
	))

	placeOrders := func(ctx context.Context) {
		for i := 1; ; i++ {
			must(commandBus.Send(ctx, PlaceOrder{
				OrderID: fmt.Sprintf("order-%d", i),
				Amount:  10 + rand.IntN(490),
			}))
			time.Sleep(time.Duration(80+rand.IntN(140)) * time.Millisecond)
		}
	}

	return []component{outboxForwarder}, placeOrders
}

// setUpOutbox returns a publisher that stores messages in a PostgreSQL table,
// and the Forwarder that moves them to Kafka.
//
// In a real application, the publisher would be created for each database transaction,
// so that the message is stored together with the other changes.
func setUpOutbox(kafkaPublisher message.Publisher, logger watermill.LoggerAdapter) (message.Publisher, *forwarder.Forwarder) {
	db, err := stdSQL.Open("pgx", postgresDSN)
	must(err)
	must(db.Ping())

	sqlPublisher, err := sql.NewPublisher(sql.BeginnerFromStdSQL(db), sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	must(err)

	sqlSubscriber, err := sql.NewSubscriber(sql.BeginnerFromStdSQL(db), sql.SubscriberConfig{
		ConsumerGroup:    "forwarder",
		SchemaAdapter:    sql.DefaultPostgreSQLSchema{},
		OffsetsAdapter:   sql.DefaultPostgreSQLOffsetsAdapter{},
		InitializeSchema: true,
		PollInterval:     100 * time.Millisecond,
	}, logger)
	must(err)

	outboxForwarder, err := forwarder.NewForwarder(sqlSubscriber, kafkaPublisher, logger, forwarder.Config{
		ForwarderTopic: outboxTopic,
	})
	must(err)

	outboxPublisher := forwarder.NewPublisher(sqlPublisher, forwarder.PublisherConfig{
		ForwarderTopic: outboxTopic,
	})

	return outboxPublisher, outboxForwarder
}

// setUpPayments adds the handlers of the payments service.
func setUpPayments(router *message.Router, kafkaPublisher message.Publisher, logger watermill.LoggerAdapter) ([]component, func(context.Context)) {
	eventBus := newEventBus(kafkaPublisher, logger)

	// The payment provider is unreliable. The handler is retried,
	// and if it keeps failing, the message goes to the poison queue.
	chargePayment, err := newEventProcessor(router, logger).AddHandler(
		cqrs.NewEventHandler("ChargePayment", func(ctx context.Context, event *OrderPlaced) error {
			work()
			if rand.IntN(100) < 45 {
				return errors.New("payment provider is not available")
			}
			return eventBus.Publish(ctx, PaymentCharged{OrderID: event.OrderID})
		}),
	)
	must(err)

	poisonQueue, err := middleware.PoisonQueue(kafkaPublisher, "poison.payments")
	must(err)
	chargePayment.AddMiddleware(
		poisonQueue,
		middleware.Retry{MaxRetries: 1, InitialInterval: 100 * time.Millisecond}.Middleware,
	)

	router.AddConsumerHandler(
		"alert-on-failed-payment",
		"poison.payments",
		newKafkaSubscriber("alert-on-failed-payment", logger),
		func(msg *message.Message) error {
			work()
			return nil
		},
	)

	// Updates of payments come from the webhooks service.
	router.AddConsumerHandler(
		"reconcile-payment",
		"payments.updates",
		newKafkaSubscriber("reconcile-payment", logger),
		func(msg *message.Message) error {
			work()
			return nil
		},
	)

	return nil, nil
}

// setUpShipping adds the handlers of the shipping service.
//
// A charged payment schedules packing with a command sent through GoChannel.
// When the order is packed, OrderShipped is published to Kafka.
func setUpShipping(router *message.Router, kafkaPublisher message.Publisher, logger watermill.LoggerAdapter) ([]component, func(context.Context)) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	commandBus := newCommandBus(pubSub, logger)
	eventBus := newEventBus(kafkaPublisher, logger)

	must(newEventProcessor(router, logger).AddHandlers(
		cqrs.NewEventHandler("SchedulePacking", func(ctx context.Context, event *PaymentCharged) error {
			work()
			return commandBus.Send(ctx, PackOrder{OrderID: event.OrderID})
		}),
	))

	carriers := []string{"DHL", "UPS", "InPost"}

	must(newCommandProcessor(router, pubSub, logger).AddHandlers(
		cqrs.NewCommandHandler("PackOrderHandler", func(ctx context.Context, cmd *PackOrder) error {
			work()
			work()
			return eventBus.Publish(ctx, OrderShipped{
				OrderID: cmd.OrderID,
				Carrier: carriers[rand.IntN(len(carriers))],
			})
		}),
	))

	return nil, nil
}

// setUpNotifications adds the handlers of the notifications service. It only reads events from Kafka.
func setUpNotifications(router *message.Router, logger watermill.LoggerAdapter) ([]component, func(context.Context)) {
	must(newEventProcessor(router, logger).AddHandlers(
		cqrs.NewEventHandler("SendConfirmationEmail", func(ctx context.Context, event *OrderPlaced) error {
			work()
			return nil
		}),
		cqrs.NewEventHandler("SendShippingEmail", func(ctx context.Context, event *OrderShipped) error {
			work()
			return nil
		}),
	))

	return nil, nil
}

// setUpWebhooks adds the handlers of the webhooks service. It uses GoChannel inside,
// and publishes the result to Kafka for the payments service:
//
//	webhooks.stripe -+
//	                 +-> (FanIn) webhooks.all -> normalize -> payments.normalized -> (FanOut) -> update-ledger
//	webhooks.paypal -+                                                                        -> publish-payment-update -> Kafka
func setUpWebhooks(router *message.Router, kafkaPublisher message.Publisher, logger watermill.LoggerAdapter) ([]component, func(context.Context)) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	fanIn, err := fanin.NewFanIn(pubSub, pubSub, fanin.Config{
		SourceTopics: []string{"webhooks.stripe", "webhooks.paypal"},
		TargetTopic:  "webhooks.all",
	}, logger)
	must(err)

	router.AddHandler(
		"normalize-webhook",
		"webhooks.all", pubSub,
		"payments.normalized", pubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			work()
			normalized := message.NewMessage(watermill.NewUUID(), msg.Payload)
			normalized.Metadata.Set("normalized_from", msg.UUID)
			return []*message.Message{normalized}, nil
		},
	)

	fanOut, err := gochannel.NewFanOut(pubSub, logger)
	must(err)
	fanOut.AddSubscription("payments.normalized")

	router.AddConsumerHandler("update-ledger", "payments.normalized", fanOut, func(msg *message.Message) error {
		work()
		return nil
	})
	router.AddHandler(
		"publish-payment-update",
		"payments.normalized", fanOut,
		"payments.updates", kafkaPublisher,
		func(msg *message.Message) ([]*message.Message, error) {
			work()
			return []*message.Message{msg}, nil
		},
	)

	// Webhooks are published straight to the Pub/Sub, like an HTTP endpoint receiving them would do.
	sendWebhooks := func(context.Context) {
		providers := []string{"stripe", "paypal"}

		for i := 1; ; i++ {
			provider := providers[rand.IntN(len(providers))]

			payload, err := json.Marshal(map[string]any{
				"provider":   provider,
				"payment_id": fmt.Sprintf("pay-%d", i),
				"status":     "succeeded",
			})
			must(err)

			msg := message.NewMessage(watermill.NewUUID(), payload)
			msg.Metadata.Set("provider", provider)
			must(pubSub.Publish("webhooks."+provider, msg))

			time.Sleep(time.Duration(100+rand.IntN(200)) * time.Millisecond)
		}
	}

	return []component{fanIn, fanOut}, sendWebhooks
}

// newCommandBus returns a command bus that sends commands inside the service, through GoChannel.
func newCommandBus(pubSub *gochannel.GoChannel, logger watermill.LoggerAdapter) *cqrs.CommandBus {
	commandBus, err := cqrs.NewCommandBusWithConfig(pubSub, cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			return "commands." + params.CommandName, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	must(err)

	return commandBus
}

func newCommandProcessor(router *message.Router, pubSub *gochannel.GoChannel, logger watermill.LoggerAdapter) *cqrs.CommandProcessor {
	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(router, cqrs.CommandProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
			return "commands." + params.CommandName, nil
		},
		SubscriberConstructor: func(cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return pubSub, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	must(err)

	return commandProcessor
}

func newEventBus(publisher message.Publisher, logger watermill.LoggerAdapter) *cqrs.EventBus {
	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return "events." + params.EventName, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	must(err)

	return eventBus
}

// newEventProcessor returns an event processor that reads events from Kafka.
// Each event handler has its own consumer group.
func newEventProcessor(router *message.Router, logger watermill.LoggerAdapter) *cqrs.EventProcessor {
	eventProcessor, err := cqrs.NewEventProcessorWithConfig(router, cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return "events." + params.EventName, nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return newKafkaSubscriber(params.HandlerName, logger), nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	must(err)

	return eventProcessor
}

func newKafkaSubscriber(consumerGroup string, logger watermill.LoggerAdapter) *kafka.Subscriber {
	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	// A new consumer group starts from the oldest message, so nothing published before it joined is lost.
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	subscriber, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:               kafkaBrokers,
		ConsumerGroup:         consumerGroup,
		OverwriteSaramaConfig: saramaConfig,
	}, logger)
	must(err)

	return subscriber
}

// newDiscoverySubscriber returns a subscriber for announcements of inspectors.
// It has no consumer group, so every instance receives all announcements, starting from the new ones.
func newDiscoverySubscriber(logger watermill.LoggerAdapter) *kafka.Subscriber {
	subscriber, err := kafka.NewSubscriber(kafka.SubscriberConfig{Brokers: kafkaBrokers}, logger)
	must(err)

	return subscriber
}

// work simulates a handler doing something.
func work() {
	time.Sleep(time.Duration(5+rand.IntN(25)) * time.Millisecond)
}

func env(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
