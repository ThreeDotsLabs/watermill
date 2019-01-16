package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/ThreeDotsLabs/watermill/cqrs/command"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/golang/protobuf/ptypes"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type BookRoomHandler struct{}

func (b BookRoomHandler) NewCommand() interface{} {
	return &BookRoom{}
}

func (b BookRoomHandler) Handle(c interface{}) error {
	// c is always type returned by `NewCommand`, so cast is always safe
	cmd := c.(*BookRoom) // todo - get rid of interface{} when generics added to Go

	log.Printf("booked %s for %s from %s to %s", cmd.RoomId, cmd.GuestName, cmd.StartDate, cmd.EndDate)
	return nil
}

type OrderBeerHandler struct{}

func (b OrderBeerHandler) NewCommand() interface{} {
	return &OrderBeer{}
}

func (b OrderBeerHandler) Handle(c interface{}) error {
	cmd := c.(*OrderBeer)

	log.Printf("%d beers ordered to room %s", cmd.Count, cmd.RoomId)
	return nil
}

func main() {
	logger := watermill.NewStdLogger(true, false)
	cmdMarshaler := command.ProtoBufMarshaler{}

	// you can use any Pub/Sub implementation from here: https://watermill.io/docs/pub-sub-implementations/
	pubSub := gochannel.NewGoChannel(0, logger, time.Second)

	// cqrs is built on already existing messages router: https://watermill.io/docs/messages-router/
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	commandProcessor := command.NewProcessor(
		[]command.Handler{
			BookRoomHandler{},
			OrderBeerHandler{},
		},
		"commands",
		pubSub,
		cmdMarshaler,
		logger,
	)
	if err := commandProcessor.AddHandlersToRouter(router); err != nil {
		panic(err)
	}

	go publishCommands(pubSub, cmdMarshaler)

	if err := router.Run(); err != nil {
		panic(err)
	}
}

func publishCommands(pubSub message.PubSub, cmdMarshaler command.ProtoBufMarshaler) func() {
	commandBus := command.NewBus(
		pubSub,
		"commands",
		cmdMarshaler,
	)

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

		orderBeerCmd := &OrderBeer{
			RoomId: bookRoomCmd.RoomId,
			Count:  rand.Int63n(10) + 1,
		}
		if err := commandBus.Send(orderBeerCmd); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
