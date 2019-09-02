package pkg

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func (ps PubSub) ConsumeMessages(wg *sync.WaitGroup, counter *Counter) error {
	router, err := message.NewRouter(message.RouterConfig{}, watermill.NopLogger{})
	if err != nil {
		return err
	}

	router.AddNoPublisherHandler(
		"benchmark_read",
		ps.Topic,
		ps.Subscriber,
		func(msg *message.Message) error {
			defer counter.Add(1)
			defer wg.Done()

			msg.Ack()
			return nil
		},
	)

	return router.Run(context.Background())
}
