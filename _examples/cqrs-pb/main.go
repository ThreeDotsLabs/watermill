package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/ThreeDotsLabs/watermill/cqrs"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/golang/protobuf/ptypes"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type BookRoomHandler struct {
	eventBus cqrs.EventBus
}

func (b BookRoomHandler) NewCommand() interface{} {
	return &BookRoom{}
}

func (b BookRoomHandler) Handle(c interface{}) error {
	// c is always type returned by `NewCommand`, so cast is always safe
	cmd := c.(*BookRoom) // todo - get rid of interface{} when generics added to Go

	log.Printf("booked %s for %s from %s to %s", cmd.RoomId, cmd.GuestName, cmd.StartDate, cmd.EndDate)

	if err := b.eventBus.Send(&RoomBooked{
		RoomId:    cmd.RoomId,
		GuestName: cmd.GuestName,
		StartDate: cmd.StartDate,
		EndDate:   cmd.EndDate,
	}); err != nil {
		return err
	}

	return nil
}

type OrderBeerOnRoomBooked struct {
	commandBus cqrs.CommandBus
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

type OrderBeerHandler struct {
	eventBus cqrs.EventBus
}

func (b OrderBeerHandler) NewCommand() interface{} {
	return &OrderBeer{}
}

func (b OrderBeerHandler) Handle(c interface{}) error {
	cmd := c.(*OrderBeer)

	if err := b.eventBus.Send(&BeerOrdered{
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
	marshaler := cqrs.ProtoBufMarshaler{}

	// you can use any Pub/Sub implementation from here: https://watermill.io/docs/pub-sub-implementations/
	pubSub := gochannel.NewGoChannel(0, logger, time.Second)

	// cqrs is built on already existing messages router: https://watermill.io/docs/messages-router/
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	commandBus := cqrs.NewCommandBus(pubSub, "commands", marshaler)
	eventBus := cqrs.NewEventBus(pubSub, "events", marshaler)

	commandProcessor := cqrs.NewCommandProcessor(
		[]cqrs.CommandHandler{
			BookRoomHandler{eventBus},
			OrderBeerHandler{eventBus},
		},
		"commands",
		pubSub,
		marshaler,
		logger,
	)
	if err := commandProcessor.AddHandlersToRouter(router); err != nil {
		panic(err)
	}

	eventProcessor := cqrs.NewEventProcessor(
		[]cqrs.EventHandler{
			OrderBeerOnRoomBooked{commandBus},
		},
		"events",
		pubSub,
		marshaler,
		logger,
	)
	if err := eventProcessor.AddHandlersToRouter(router); err != nil {
		panic(err)
	}

	go publishCommands(commandBus)

	if err := router.Run(); err != nil {
		panic(err)
	}
}

func publishCommands(commandBus cqrs.CommandBus) func() {
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
			GuestName: "Andrzej",
			StartDate: startDate,
			EndDate:   endDate,
		}
		if err := commandBus.Send(bookRoomCmd); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
