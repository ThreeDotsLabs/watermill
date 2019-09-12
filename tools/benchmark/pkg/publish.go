package pkg

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func (ps PubSub) PublishMessages() error {
	messagesLeft := ps.MessagesCount
	workers := 200

	addMsg := make(chan struct{})
	wg := sync.WaitGroup{}

	start := time.Now()

	rand.Seed(time.Now().UnixNano())

	msgPayload, err := ps.payload()
	if err != nil {
		return err
	}

	wg.Add(workers)

	for num := 0; num < workers; num++ {
		go func() {
			defer wg.Done()

			var msg *message.Message

			for range addMsg {
				msg = message.NewMessage(watermill.NewULID(), msgPayload)

				// using function from middleware to set correlation id, useful for debugging
				middleware.SetCorrelationID(watermill.NewShortUUID(), msg)

				if err := ps.Publisher.Publish(ps.Topic, msg); err != nil {
					panic(err)
				}
			}
		}()
	}

	for ; messagesLeft > 0; messagesLeft-- {
		addMsg <- struct{}{}
	}
	close(addMsg)

	wg.Wait()

	elapsed := time.Now().Sub(start)
	fmt.Printf("added %d messages in %s, %f msg/s\n", ps.MessagesCount, elapsed, float64(ps.MessagesCount)/elapsed.Seconds())

	return nil
}

func (ps PubSub) payload() ([]byte, error) {
	msgPayload := make([]byte, ps.MessageSize)
	_, err := rand.Read(msgPayload)
	if err != nil {
		return nil, err
	}

	return msgPayload, nil
}
