package main

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/_examples/abandoned-cart"

	"github.com/ThreeDotsLabs/watermill/components/saga"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	brokers = []string{"kafka:9092"}
	topic   = "shop.events"

	logger = watermill.NewStdLogger(
		false, // debug
		false, // trace
	)
	marshaler = kafka.DefaultMarshaler{}
)

// createSubscriber is helper function as previous, but in this case creates Subscriber.
func createSubscriber(consumerGroup string) message.Subscriber {
	kafkaSubscriber, err := kafka.NewConfluentSubscriber(kafka.SubscriberConfig{
		Brokers:         brokers,
		ConsumerGroup:   consumerGroup, // every handler will have separated consumer group
		AutoOffsetReset: "earliest",    // when no offsets (for example: new consumer) we want receive all messages
	}, marshaler, logger)
	if err != nil {
		panic(err)
	}

	return kafkaSubscriber
}

func createPublisher() message.Publisher {
	kafkaPublisher, err := kafka.NewPublisher(
		brokers,
		marshaler,
		nil,
	)
	if err != nil {
		panic(err)
	}

	return kafkaPublisher
}

type cartIDEvent interface {
	EventCartID() string
}

// todo - doc contract, yagni
type abandonedCartEvent struct {
	UserID    string    `json:"user_id"`
	UserEmail string    `json:"user_email"`
	CartID    string    `json:"cart_id"`
	Time      time.Time `json:"-"`
}

func (abandonedCartEvent) EventName() string {
	return "abandoned_cart"
}

func (a abandonedCartEvent) EventOccurredOn() time.Time {
	return a.Time
}

func (a abandonedCartEvent) EventCartID() string {
	return a.CartID
}

type addedToCartEvent struct {
	UserID    string    `json:"user_id"`
	UserEmail string    `json:"user_email"`
	CartID    string    `json:"cart_id"`
	Time      time.Time `json:"-"`
}

func (addedToCartEvent) EventName() string {
	return "added_to_cart"
}

func (a addedToCartEvent) EventOccurredOn() time.Time {
	return a.Time
}

func (a addedToCartEvent) EventCartID() string {
	return a.CartID
}

type orderPlacedEvent struct {
	CartID string    `json:"cart_id"`
	Time   time.Time `json:"-"`
}

func (orderPlacedEvent) EventName() string {
	return "order_placed"
}

func (o orderPlacedEvent) EventOccurredOn() time.Time {
	return o.Time
}

func (a orderPlacedEvent) EventCartID() string {
	return a.CartID
}

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	pubSub := message.NewPubSub(createPublisher(), createSubscriber("handler_1")) // todo -rename

	// todo - partitioning
	// todo - 1 delayed handler - chronologicznie eventy, czekamy do czasu w ktorym event mowi zeby triggerowac
	// todo - 2 order placed handler
	// todo - 3 added to cart handler (add to delayed)
	// todo - use nats

	err = router.AddHandler(
		"handler_1", // todo - rename
		topic,
		topic,
		pubSub,
		saga.Handler{
			Costam: func(i *message.Message, persister saga.Persister) saga.Saga {
				// todo how to choice event?
				abandoned_cart.UnmarshalEvent(i.Payload, []abandoned_cart.Event{
					&abandonedCartEvent{},
					&addedToCartEvent{},
					&orderPlacedEvent{},
				})

				// todo - something
				s, err := persister.Upsert(event.CartID())
				if err != nil {
					return err
				}

				// todo - how to handle?
				s.HandleMessage(event)

				return s
			},
		}.Handler,
	)
	if err != nil {
		panic(err)
	}

	router.Run()
}
