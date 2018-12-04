package main

import (
	"math/rand"
	"time"

	"github.com/ThreeDotsLabs/watermill/_examples/abandoned-cart"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/satori/go.uuid"
)

var (
	brokers = []string{"kafka:9092"}
	topic   = "shop.events"

	marshaler = kafka.DefaultMarshaler{}
)

func main() {
	publisher := createPublisher()

	publishAddToCart(publisher)
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

type addedToCartEvent struct {
	UserID    string    `json:"user_id"`
	UserEmail string    `json:"user_email"`
	CartID    string    `json:"cart_id"`
	ProductID string    `json:"product_id"`
	Time      time.Time `json:"-"`
}

func (addedToCartEvent) EventName() string {
	return "added_to_cart"
}

func (a addedToCartEvent) EventOccurredOn() time.Time {
	return a.Time
}

type orderPlacedEvent struct {
	UserID string    `json:"user_id"`
	CartID string    `json:"cart_id"`
	Time   time.Time `json:"-"`
}

func (orderPlacedEvent) EventName() string {
	return "order_placed"
}

func (o orderPlacedEvent) EventOccurredOn() time.Time {
	return o.Time
}

func publishAddToCart(publisher message.Publisher) {
	i := 0
	for {
		addedToCart := addedToCartEvent{
			UserID:    uuid.NewV4().String(),
			UserEmail: uuid.NewV4().String() + "@gmail.com",
			CartID:    uuid.NewV4().String(),
			ProductID: uuid.NewV4().String(),
			Time:      time.Now(),
		}
		payload, err := abandoned_cart.MarshalEvent(addedToCart)
		if err != nil {
			panic(err)
		}

		err = publisher.Publish(topic, message.NewMessage(
			uuid.NewV4().String(),
			payload,
		))
		if err != nil {
			panic(err)
		}

		if rand.Int31n(3) != 0 {
			go publishOrderPlaced(addedToCart, publisher)
		}

		i++
		time.Sleep(time.Second)
	}
}

func publishOrderPlaced(addedToCart addedToCartEvent, publisher message.Publisher) {
	time.Sleep(time.Second * time.Duration(rand.Int63n(30)))

	payload, err := abandoned_cart.MarshalEvent(orderPlacedEvent{
		UserID: addedToCart.UserID,
		CartID: addedToCart.CartID,
		Time:   time.Now(),
	})
	if err != nil {
		panic(err)
	}

	err = publisher.Publish(topic, message.NewMessage(
		uuid.NewV4().String(),
		payload,
	))
	if err != nil {
		panic(err)
	}
}
