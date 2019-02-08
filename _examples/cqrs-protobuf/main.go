package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/golang/protobuf/ptypes"
)

// BookRoomHandler is a command processor, which handles BookRoom command and emits RoomBooked.
//
// In CQRS, one command must be handled by only one handler.
// When another handler with this command is added to command processor, error will be retuerned.
type BookRoomHandler struct {
	eventBus *cqrs.EventBus
}

// NewCommand returns type of command which this handle should handle. It must be a pointer.
func (b BookRoomHandler) NewCommand() interface{} {
	return &BookRoom{}
}

func (b BookRoomHandler) Handle(c interface{}) error {
	// c is always the type returned by `NewCommand`, so casting is always safe
	cmd := c.(*BookRoom)

	log.Printf("Booked %s for %s from %s to %s", cmd.RoomId, cmd.GuestName, cmd.StartDate, cmd.EndDate)

	// RoomBooked will be handled by OrderBeerOnRoomBooked event processor,
	// in future RoomBooked may be handled by multiple event processor
	if err := b.eventBus.Publish(&RoomBooked{
		RoomId:    cmd.RoomId,
		GuestName: cmd.GuestName,
		StartDate: cmd.StartDate,
		EndDate:   cmd.EndDate,
	}); err != nil {
		return err
	}

	return nil
}

// OrderBeerOnRoomBooked is a event processor, which handles RoomBooked event and emits OrderBeer command.
type OrderBeerOnRoomBooked struct {
	commandBus *cqrs.CommandBus
}

func (OrderBeerOnRoomBooked) NewEvent() interface{} {
	return &RoomBooked{}
}

func (o OrderBeerOnRoomBooked) Handle(e interface{}) error {
	event := e.(*RoomBooked)

	orderBeerCmd := &OrderBeer{
		RoomId: event.RoomId,
		Count:  rand.Int63n(10) + 1,
	}

	return o.commandBus.Send(orderBeerCmd)
}

// OrderBeerHandler is a command handler, which handles OrderBeer command and emits BeerOrdered.
// BeerOrdered is not handled by any event processor, but we may use persistent Pub/Sub to handle it in the future.
type OrderBeerHandler struct {
	eventBus *cqrs.EventBus
}

func (b OrderBeerHandler) NewCommand() interface{} {
	return &OrderBeer{}
}

func (b OrderBeerHandler) Handle(c interface{}) error {
	cmd := c.(*OrderBeer)

	if rand.Int63n(10) == 0 {
		// sometimes there is no beer left, command will be retried
		return errors.New("no beer left, please try later")
	}

	if err := b.eventBus.Publish(&BeerOrdered{
		RoomId: cmd.RoomId,
		Count:  cmd.Count,
	}); err != nil {
		return err
	}

	log.Printf("%d beers ordered to room %s", cmd.Count, cmd.RoomId)
	return nil
}

func main() {
	logger := watermill.NewStdLogger(true, false)
	marshaler := cqrs.ProtobufMarshaler{}

	// You can use any Pub/Sub implementation from here: https://watermill.io/docs/pub-sub-implementations/
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	// CQRS is built on already existing messages router: https://watermill.io/docs/messages-router/
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

	// cqrs.Facade is facade for Command and Event buses and processors.
	// You can use facade, or create buses and processors manually (you can inspire with cqrs.NewFacade)
	cqrsFacade, err := cqrs.NewFacade(cqrs.FacadeConfig{
		CommandsTopic: "commands",
		EventsTopic:   "events",
		CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
			return []cqrs.CommandHandler{
				BookRoomHandler{eb},
				OrderBeerHandler{eb},
			}
		},
		EventHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
			return []cqrs.EventHandler{
				OrderBeerOnRoomBooked{cb},
			}
		},
		Router:                router,
		CommandsPubSub:        pubSub,
		EventsPubSub:          pubSub,
		Logger:                logger,
		CommandEventMarshaler: marshaler,
	})
	if err != nil {
		panic(err)
	}

	// publishing BookRoom commands every second
	go publishCommands(cqrsFacade.CommandBus())

	// processors are based on router, so they will work when router will start
	if err := router.Run(); err != nil {
		panic(err)
	}
}

func publishCommands(commandBus *cqrs.CommandBus) func() {
	i := 0
	for {
		i++

		startDate, err := ptypes.TimestampProto(time.Now())
		if err != nil {
			panic(err)
		}

		endDate, err := ptypes.TimestampProto(time.Now().Add(time.Hour * 24 * 3))
		if err != nil {
			panic(err)
		}

		bookRoomCmd := &BookRoom{
			RoomId:    fmt.Sprintf("%d", i),
			GuestName: "John",
			StartDate: startDate,
			EndDate:   endDate,
		}
		if err := commandBus.Send(bookRoomCmd); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
