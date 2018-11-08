package main

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/delivery/interfaces"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/app/command"
	cart2 "github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/order"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/product"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/infrastructure/cart"
	"github.com/ThreeDotsLabs/watermill/components/domain"
	"github.com/ThreeDotsLabs/watermill/components/domain/eventstore"
	"github.com/ThreeDotsLabs/watermill/msghandler"
	"github.com/ThreeDotsLabs/watermill/pubsub/infrastructure/gochannel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/satori/go.uuid"
)

// todo - cleanup imports

func main() {
	repo := cart.NewMemoryRepository()
	pubSub := gochannel.NewPubSub()

	db, err := sql.Open("mysql", "root:secret@/shop")
	if err != nil {
		panic(err)
	}
	eventstore := eventstore.NewSQL(db, func(event domain.Event) eventstore.SQLEvent {
		jsonPayload, err := json.Marshal(event.Payload)
		if err != nil {
			// todo - error handling
			panic(err)
		}

		eventID, err := uuid.FromString(event.ID)
		if err != nil {
			panic(err)
		}

		aggregateID, err := uuid.FromString(event.AggregateID)
		if err != nil {
			panic(err)
		}

		return eventstore.SQLEvent{
			ID:          eventID.Bytes(),
			Name:        event.Name,
			JsonPayload: jsonPayload,
			OccurredOn:  event.OccurredOn,

			AggregateVersion: event.AggregateVersion,
			AggregateID:      aggregateID.Bytes(),
			AggregateType:    event.AggregateType,
		}
	})

	eventsFactory := domain.NewEventsFactory(func() string {
		// todo - ordered uuid
		return uuid.NewV4().String()
	})

	handler := msghandler.NewHandler(pubSub)
	interfaces.SetupInterfaces(handler)
	go handler.Run()

	time.Sleep(time.Second * 2)

	cartID := cart2.ID(uuid.NewV4().String())
	addCart(cartID, repo, eventstore, eventsFactory)
	putProductToCart(cartID, "product_1", repo, eventstore, eventsFactory)

	placeOrder(cartID, repo, eventstore, eventsFactory)

	time.Sleep(time.Second * 5)
}

func placeOrder(
	cartID cart2.ID,
	repo *cart.MemoryRepository,
	eventstore domain.Eventstore,
	eventsFactory domain.EventsFactory,
) {
	handler := command.NewPlaceOrderHandler(repo, eventstore, eventsFactory)
	if err := handler.Handle(command.PlaceOrder{order.ID(uuid.NewV4().String()), cartID, "customer_1"}); err != nil {
		panic(err)
	}
}

func putProductToCart(
	cartID cart2.ID,
	productID product.ID,
	repo *cart.MemoryRepository,
	eventstore domain.Eventstore,
	eventsFactory domain.EventsFactory,
) {
	handler := command.NewPutProductToCartHandler(repo, eventstore, eventsFactory)
	cmd := command.PutProductToCart{cartID, productID}

	if err := handler.Handle(cmd); err != nil {
		panic(err)
	}
}

func addCart(
	id cart2.ID, repo *cart.MemoryRepository,
	eventstore domain.Eventstore,
	eventsFactory domain.EventsFactory,
) {
	handler := command.NewAddCartHandler(repo, eventstore, eventsFactory)
	cmd := command.AddCart{id}

	if err := handler.Handle(cmd); err != nil {
		panic(err)
	}
}
