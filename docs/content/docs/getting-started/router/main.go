package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	// just a simplest implementation,
	// probably you want to ship your own implementation of `watermill.LoggerAdapter`
	logger = watermill.NewStdLogger(false, false)
)

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	// this plugin will gracefully shutdown router, when SIGTERM was sent
	// you can also close router by just calling `r.Close()`
	router.AddPlugin(plugin.SignalsHandler)

	router.AddMiddleware(
		// when error occurred, function will be retried,
		// after max retries (or if no Retry middleware is added) Nack is send and message will be resent
		middleware.Retry{
			MaxRetries: 3,
			WaitTime:   time.Millisecond * 100,
			Backoff:    3,
			Logger:     logger,
		}.Middleware,

		// this middleware will handle panics from handlers
		// and pass them as error to retry middleware in this case
		middleware.Recoverer,
	)

	// for simplicity we are using gochannel Pub/Sub here,
	// you can replace it with any Pub/Sub implementation, it will work the same
	pubSub := gochannel.NewGoChannel(0, logger, time.Second)

	// producing some mock events in background
	go produceEvents(pubSub)

	// this handler will send e-mail after placing an order
	// we are using `AddNoPublisherHandler`, because this handler won't emit any eventss
	if err := router.AddNoPublisherHandler(
		"send_order_placed_email", // handler name, must be unique
		"shop.events",             // topic from which we will read events
		pubSub,
		sendOrderPlacedEmail{}.Handler,
	); err != nil {
		panic(err)
	}

	// this handler will send shipping request to delivery company
	// here we are using AddHandler, because this handler will emit events
	if err := router.AddHandler(
		"request_order_shipping",
		"shop.events",
		"shop.events",
		pubSub,
		requestOrderShipping{}.Handler,
	); err != nil {
		panic(err)
	}

	// just for debug, we are printing all events sent to `shop.events`
	if err := router.AddNoPublisherHandler(
		"print_events",
		"shop.events",
		pubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			fmt.Printf(
				"\n> Received message: %s\n> %s\n> metadata: %v\n\n",
				msg.UUID, string(msg.Payload), msg.Metadata,
			)
			return nil, nil
		},
	); err != nil {
		panic(err)
	}

	// when everything is ready, let's run router,
	// this function is blocking since router is running
	if err := router.Run(); err != nil {
		panic(err)
	}
}

type OrderPlaced struct {
	OrderID         string `json:"order_id"`
	ClientEmail     string `json:"client_email"`
	ClientAddress   string `json:"client_address"`
	DeliveryCompany string `json:"delivery_company"`
}

func (OrderPlaced) EventName() string {
	return "order_placed"
}

func produceEvents(publisher message.Publisher) {
	for {
		msg, err := marshalEvent(
			OrderPlaced{
				OrderID:         uuid.NewV4().String(),
				ClientEmail:     fmt.Sprintf("%s@gmail.com", uuid.NewV4().String()),
				ClientAddress:   "308 Negra Arroyo Lane, Albuquerque, New Mexico, 87104",
				DeliveryCompany: "Los Pollos Hermanos",
			},
		)
		if err != nil {
			panic(err)
		}

		// event will be send to every subscribed subscriber
		if err := publisher.Publish("shop.events", msg); err != nil {
			log.Printf("cannot send message: %s", err.Error())
		}

		time.Sleep(time.Second)
	}
}

type sendOrderPlacedEmail struct {
	// here will be some dependencies, like API client or DB connection
}

func (c sendOrderPlacedEmail) Handler(msg *message.Message) ([]*message.Message, error) {
	event := OrderPlaced{}

	if ok, err := unmarshalEvent(msg, &event); err != nil {
		return nil, err
	} else if !ok {
		// this is not OrderPlaced, skipping
		return nil, nil
	}

	log.Printf("sending confirmation e-mail for order to: %s\n", event.ClientEmail)

	return nil, nil
}

type requestOrderShipping struct {
	// here will be some dependencies, like API client or DB connection
}

func (o requestOrderShipping) Handler(msg *message.Message) ([]*message.Message, error) {
	event := OrderPlaced{}

	if ok, err := unmarshalEvent(msg, &event); err != nil {
		// something is wrong with payload,
		// in this router configuration message will be redelivered until unmarshal is fixed,
		//
		// behaviour for these kind of errors is different for many cases, but you should:
		//   - monitor errors like this, and when it occurs fix unmarshaler error
		//   - skip these types of error by using middleware
		//   - add poison queue middleware, which will send errors to separated topic which you should monitor
		return nil, err
	} else if !ok {
		// this is not OrderPlaced, skipping
		return nil, nil
	}

	log.Printf("sending shipping request to delivery company: %s\n", event.DeliveryCompany)

	// here should go call to external API or whatever should be used to requesting shipping
	// ...

	// all went fine, we are emitting `ShippingRequested` event
	shippingRequestedMessage, err := marshalEvent(ShippingRequested{
		OrderID:         event.OrderID,
		DeliveryCompany: event.DeliveryCompany,
	})
	if err != nil {
		return nil, err
	}

	return message.Messages{shippingRequestedMessage}, nil
}

type ShippingRequested struct {
	OrderID         string `json:"order_id"`
	DeliveryCompany string `json:"delivery_company"`
}

func (ShippingRequested) EventName() string {
	return "shipping_requested"
}

// Event is something like transport type, which contains EventName and EventPayload.
type Event struct {
	EventName    string       `json:"event_name"`
	EventPayload EventPayload `json:"event_payload"`
}

type EventPayload interface {
	EventName() string
}

const eventNameMetadataKey = "event_name"

func marshalEvent(payload EventPayload) (*message.Message, error) {
	event := Event{
		EventName:    payload.EventName(),
		EventPayload: payload,
	}

	b, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(uuid.NewV4().String(), b)
	// it's good idea to save event name to metadata,
	// it allows us to check event type without deserialize
	msg.Metadata.Set(eventNameMetadataKey, event.EventName)

	return msg, nil
}

func unmarshalEvent(msg *message.Message, payload EventPayload) (ok bool, err error) {
	if msg.Metadata.Get(eventNameMetadataKey) != payload.EventName() {
		// this is not event type which in we are interested in
		return false, nil
	}

	event := Event{EventPayload: payload}

	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return false, err
	}

	return true, nil
}
