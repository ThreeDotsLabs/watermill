package pkg

import (
	"context"
	"sync"

	"github.com/rcrowley/go-metrics"

	"github.com/ThreeDotsLabs/watermill/message"
)

func (ps PubSub) ConsumeMessages(router *message.Router, wg *sync.WaitGroup, meter metrics.Meter) error {
	router.AddNoPublisherHandler(
		"benchmark_read",
		ps.Topic,
		ps.Subscriber,
		func(msg *message.Message) error {
			defer wg.Done()
			defer meter.Mark(1)

			msg.Ack()
			return nil
		},
	)

	if err := router.Run(context.Background()); err != nil {
		return err
	}

	return nil
}
