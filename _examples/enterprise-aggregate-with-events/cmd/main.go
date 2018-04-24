package main

import (
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/app/command"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/infrastructure/cart"
	cart2 "github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/product"
	"github.com/roblaszczak/gooddd/domain/eventstore"
	"github.com/roblaszczak/gooddd/domain"
	"github.com/roblaszczak/gooddd/pubsub/infrastructure/gochannel"
	"time"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/delivery/interfaces"
	"github.com/roblaszczak/gooddd/msghandler"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/satori/go.uuid"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/order"
	"encoding/json"
)

// todo - cleanup imports

func main() {
	repo := cart.NewMemoryRepository()
	pubsub := gochannel.NewPubSub()

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

	router := msghandler.NewRouter(pubsub)
	interfaces.SetupInterfaces(router)
	go router.Run()

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
