package infrastructure

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
)

type NoGroupSubscriberConstructor func(t *testing.T) message.Subscriber

func TestNoGroupSubscriber(
	t *testing.T,
	pubSubConstructor PubSubConstructor,
	noGroupSubscriberConstructor NoGroupSubscriberConstructor,
) {
	t.Run("concurrent_subscribers", func(t *testing.T) {
		t.Parallel()
		testNoGroupSubscriberConcurrentSubscribers(t, pubSubConstructor, noGroupSubscriberConstructor)
	})

	t.Run("joining_subscribers", func(t *testing.T) {
		t.Parallel()
		testNoGroupSubscriberJoiningSubscribers(t, pubSubConstructor, noGroupSubscriberConstructor)
	})

	t.Run("testNoGroupSubscriber_Close", func(t *testing.T) {
		t.Parallel()
		testNoGroupSubscriber_Close(t, pubSubConstructor, noGroupSubscriberConstructor)
	})
}

func testNoGroupSubscriberConcurrentSubscribers(
	t *testing.T,
	pubSubConstructor PubSubConstructor,
	noGroupSubscriberConstructor NoGroupSubscriberConstructor,
) {
	consumersStarted := &sync.WaitGroup{}
	consumersStarted.Add(3)

	topicName := testTopicName()

	receivedMessages := map[int][]*message.Message{}
	receivedMessagesMutex := &sync.Mutex{}

	for i := 0; i < 3; i++ {
		consumerNum := i

		go func() {
			noGroupSub := noGroupSubscriberConstructor(t)
			ch, err := noGroupSub.Subscribe(context.Background(), topicName)
			require.NoError(t, err)

			consumersStarted.Done()

			receivedMessagesMutex.Lock()
			receivedMessages[consumerNum], _ = subscriber.BulkRead(ch, 10, time.Second*10)
			receivedMessagesMutex.Unlock()
		}()
	}

	consumersStarted.Wait()

	var messagesToPublish []*message.Message

	pub, sub := pubSubConstructor(t)
	defer closePubSub(t, pub, sub)

	for i := 0; i < 10; i++ {
		id := watermill.NewUUID()

		msg := message.NewMessage(id, []byte(fmt.Sprintf("%d", i)))
		messagesToPublish = append(messagesToPublish, msg)

		err := pub.Publish(topicName, msg)
		require.NoError(t, err)
	}

	for _, messages := range receivedMessages {
		tests.AssertAllMessagesReceived(t, messagesToPublish, messages)
	}
}

func testNoGroupSubscriberJoiningSubscribers(
	t *testing.T,
	pubSubConstructor PubSubConstructor,
	noGroupSubscriberConstructor NoGroupSubscriberConstructor,
) {
	subscribersCount := 3
	topicName := testTopicName()

	createSubscriber := make(chan struct{})
	subscriberCreated := make(chan struct{})

	consumersMessages := map[int]chan *message.Message{}

	go func() {
		i := 0
		for range createSubscriber {
			subscriberNum := i
			i++

			consumersMessages[subscriberNum] = make(chan *message.Message, 0)
			messagesCh := consumersMessages[subscriberNum]

			go func() {
				subscriber := noGroupSubscriberConstructor(t)
				defer subscriber.Close()

				subscriberCreated <- struct{}{}

				ch, err := subscriber.Subscribe(context.Background(), topicName)
				require.NoError(t, err)

				for msg := range ch {
					messagesCh <- msg
					msg.Ack()
				}
			}()
		}
	}()

	pub, sub := pubSubConstructor(t)
	defer closePubSub(t, pub, sub)

	for i := 0; i < subscribersCount; i++ {
		createSubscriber <- struct{}{}
		<-subscriberCreated

	SendMsgLoop:
		for {
			time.Sleep(time.Millisecond * 500)

			id := watermill.NewUUID()
			err := pub.Publish(topicName, message.NewMessage(id, []byte(fmt.Sprintf("%d", i))))
			require.NoError(t, err)

			for consumerNum, msgCh := range consumersMessages {

			ConsumerLoop:
				for {
					select {
					case msg := <-msgCh:
						if msg.UUID != id {
							log.Printf("expected message: %s, have %s, consumer: %d", id, msg.UUID, consumerNum)
							continue ConsumerLoop
						}

						log.Printf("received expected message: %s, consumer: %d", id, consumerNum)
						break ConsumerLoop
					case <-time.After(time.Second): // todo - make it more robust
						log.Printf("no messages, consumer: %d", consumerNum)
						continue SendMsgLoop
					}
				}
			}

			// messages from all consumers received
			break SendMsgLoop
		}

	}
}

func testNoGroupSubscriber_Close(
	t *testing.T,
	pubSubConstructor PubSubConstructor,
	noGroupSubscriberConstructor NoGroupSubscriberConstructor,
) {

	topicName := testTopicName()

	var messagesToPublish []*message.Message

	pub, sub := pubSubConstructor(t)
	defer closePubSub(t, pub, sub)

	for i := 0; i < 10; i++ {
		id := watermill.NewUUID()

		msg := message.NewMessage(id, []byte(fmt.Sprintf("%d", i)))
		messagesToPublish = append(messagesToPublish, msg)

		err := pub.Publish(topicName, msg)
		require.NoError(t, err)
	}

	noGroupSub := noGroupSubscriberConstructor(t)
	ch, err := noGroupSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	_, all := subscriber.BulkRead(ch, 10, defaultTimeout)
	require.True(t, all)

	require.NoError(t, noGroupSub.Close())
}
