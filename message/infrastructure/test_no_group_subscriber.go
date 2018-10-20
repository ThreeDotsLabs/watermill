package infrastructure

import (
	"github.com/roblaszczak/gooddd/internal/tests"
	"github.com/roblaszczak/gooddd/message"
	subscriber2 "github.com/roblaszczak/gooddd/message/subscriber"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	"sync"
	"testing"
	"time"
)

type NoGroupSubscriberConstructor func(t *testing.T) message.NoConsumerGroupSubscriber

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
}

func testNoGroupSubscriberConcurrentSubscribers(
	t *testing.T,
	pubSubConstructor PubSubConstructor,
	noGroupSubscriberConstructor NoGroupSubscriberConstructor,
) {
	consumersStarted := &sync.WaitGroup{}
	consumersStarted.Add(3)

	topicName := testTopicName()

	receivedMessages := map[int][]message.ConsumedMessage{}

	for i := 0; i < 3; i++ {
		consumerNum := i

		go func() {
			subscriber := noGroupSubscriberConstructor(t)
			ch, err := subscriber.SubscribeNoGroup(topicName)
			require.NoError(t, err)

			consumersStarted.Done()

			receivedMessages[consumerNum], _ = subscriber2.BulkRead(ch, 10, time.Second*10)
		}()
	}

	consumersStarted.Wait()

	var messagesToPublish []message.ProducedMessage

	pubSub := pubSubConstructor(t)
	for i := 0; i < 10; i++ {
		id := uuid.NewV4().String()
		payload := SimpleMessage{i}

		msg := message.NewDefault(id, payload)
		messagesToPublish = append(messagesToPublish, msg)

		err := pubSub.Publish(topicName, []message.ProducedMessage{msg})
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
	consumersCount := 3
	expectedMessagesCountByConsumer := map[int]int{
		0: 3,
		1: 2,
		2: 1,
	}

	consumersStarted := &sync.WaitGroup{}
	consumersStarted.Add(consumersCount)

	consumersReadAll := &sync.WaitGroup{}
	consumersReadAll.Add(consumersCount)

	topicName := testTopicName()

	receivedMessages := map[int][]message.ConsumedMessage{}
	receivedMessagesMutex := &sync.Mutex{}

	createConsumer := make(chan struct{}, 1)
	consumerCreated := make(chan struct{}, 1)

	go func() {
		for i := 0; i < consumersCount; i++ {
			<-createConsumer

			consumerNum := i

			go func() {
				subscriber := noGroupSubscriberConstructor(t)
				ch, err := subscriber.SubscribeNoGroup(topicName)
				require.NoError(t, err)

				consumerCreated <- struct{}{}

				log.Println("consumer created")

				consumersStarted.Done()
				consumersStarted.Wait()

				msgs, all := subscriber2.BulkRead(ch, expectedMessagesCountByConsumer[consumerNum], time.Second*10)
				log.Println(consumerNum, all)

				receivedMessagesMutex.Lock()
				receivedMessages[consumerNum] = msgs
				receivedMessagesMutex.Unlock()

				consumersReadAll.Done()
			}()
		}
	}()

	var messagesToPublish []message.ProducedMessage

	pubSub := pubSubConstructor(t)
	for i := 0; i < consumersCount; i++ {
		createConsumer <- struct{}{}
		<- consumerCreated

		// todo - fix
		// very tricky, but it seems that kafka have some kind of lag
		time.Sleep(time.Second*5)

		id := uuid.NewV4().String()
		payload := SimpleMessage{i}

		msg := message.NewDefault(id, payload)
		messagesToPublish = append(messagesToPublish, msg)

		err := pubSub.Publish(topicName, []message.ProducedMessage{msg})
		require.NoError(t, err)

		log.Println("sending msg")
	}

	consumersReadAll.Wait()

	for consumer, expectedCount := range expectedMessagesCountByConsumer {
		assert.Equal(
			t, expectedCount, len(receivedMessages[consumer]),
			"invalid events count for consumer %d", consumer,
		)
	}
}