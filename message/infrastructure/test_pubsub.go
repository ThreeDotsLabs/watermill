package infrastructure

import (
	"github.com/roblaszczak/gooddd/message"
	"testing"
	"time"
	"github.com/satori/go.uuid"
	"github.com/roblaszczak/gooddd/internal/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/roblaszczak/gooddd/message/subscriber"
	"github.com/pkg/errors"
	"fmt"
)

type Features struct {
	ConsumerGroups      bool
	ExactlyOnceDelivery bool
	GuaranteedOrder     bool
}

type PubSubConstructor func(t *testing.T, consumerGroup string) message.PubSub

type MessageWithType struct {
	Type int `json:"type"`
	Num  int `json:"num"`
}

type SimpleMessage struct {
	Num int `json:"num"`
}

func TestPubSub(t *testing.T, features Features, pubsubConstructor PubSubConstructor) {
	t.Run("publishSubscribe", func(t *testing.T) {
		t.Parallel()
		publishSubscribeTest(t, pubsubConstructor(t, generateConsumerGroup()))
	})

	t.Run("resendOnError", func(t *testing.T) {
		t.Parallel()
		resendOnErrorTest(t, pubsubConstructor(t, generateConsumerGroup()))
	})

	t.Run("noAck", func(t *testing.T) {
		if !features.GuaranteedOrder {
			t.Skip("guaranteed order is required for this test")
		}
		t.Parallel()
		noAckTest(t, pubsubConstructor(t, generateConsumerGroup()))
	})

	t.Run("continueAfterClose", func(t *testing.T) {
		if features.ExactlyOnceDelivery {
			t.Skip("ExactlyOnceDelivery test is not supported yet")
		}

		t.Parallel()
		continueAfterCloseTest(t, pubsubConstructor)
	})

	t.Run("continueAfterErrors", func(t *testing.T) {
		t.Parallel()
		continueAfterErrors(t, pubsubConstructor)
	})

	t.Run("publishSubscribeInOrderTest", func(t *testing.T) {
		if !features.GuaranteedOrder {
			t.Skipf("order is not guaranteed")
		}

		t.Parallel()
		publishSubscribeInOrderTest(t, pubsubConstructor(t, generateConsumerGroup()))
	})

	t.Run("consumerGroupsTest", func(t *testing.T) {
		if !features.ConsumerGroups {
			t.Skip("consumer groups are not supported")
		}

		t.Parallel()
		consumerGroupsTest(t, pubsubConstructor)
	})
}

var stressTestTestsCount = 20

func TestPubSubStressTest(t *testing.T, features Features, pubsubConstructor PubSubConstructor) {
	for i := 0; i < stressTestTestsCount; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			TestPubSub(t, features, pubsubConstructor)
		})
	}
}

func publishSubscribeTest(t *testing.T, pubsub message.PubSub) {
	defer closePubSub(t, pubsub)
	topicName := testTopicName()

	var messagesToPublish []message.Message
	messagesPayloads := map[string]interface{}{}
	messagesTestMetadata := map[string]string{}

	for i := 0; i < 100; i++ {
		id := uuid.NewV4().String()
		testMetadata := uuid.NewV4().String()
		payload := SimpleMessage{i}

		msg := message.NewDefault(id, payload)

		msg.SetMetadata("test", testMetadata)
		messagesTestMetadata[id] = testMetadata

		messagesToPublish = append(messagesToPublish, msg)
		messagesPayloads[id] = payload
	}

	err := pubsub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	messages, err := pubsub.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
	tests.AssertMessagesPayloads(t, messagesPayloads, receivedMessages, func(msg message.Message) interface{} {
		payload := SimpleMessage{}
		msg.UnmarshalPayload(&payload)
		return payload
	})
	tests.AssertMessagesMetadata(t, "test", messagesTestMetadata, receivedMessages)
}

func publishSubscribeInOrderTest(t *testing.T, pubsub message.PubSub) {
	defer closePubSub(t, pubsub)
	topicName := testTopicName()

	var messagesToPublish []message.Message
	expectedMessages := map[int][]string{}

	for i := 0; i < 100; i++ {
		id := uuid.NewV4().String()
		msgType := i % 16

		msg := message.NewDefault(id, MessageWithType{msgType, i})

		messagesToPublish = append(messagesToPublish, msg)

		if _, ok := expectedMessages[msgType]; !ok {
			expectedMessages[msgType] = []string{}
		}
		expectedMessages[msgType] = append(expectedMessages[msgType], msg.UUID())
	}

	err := pubsub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	messages, err := pubsub.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	receivedMessagesByType := map[int][]string{}
	for _, msg := range receivedMessages {
		payload := MessageWithType{}
		err := msg.UnmarshalPayload(&payload)
		require.NoError(t, err)

		if _, ok := receivedMessagesByType[payload.Type]; !ok {
			receivedMessagesByType[payload.Type] = []string{}
		}
		receivedMessagesByType[payload.Type] = append(receivedMessagesByType[payload.Type], msg.UUID())
	}

	require.Equal(t, len(receivedMessagesByType), len(expectedMessages))
	require.Equal(t, len(receivedMessages), len(messagesToPublish))

	for key, ids := range expectedMessages {
		assert.Equal(t, ids, receivedMessagesByType[key])
	}
}

func resendOnErrorTest(t *testing.T, pubsub message.PubSub) {
	defer closePubSub(t, pubsub)
	topicName := testTopicName()

	messagesToPublish := addSimpleMessagesMessages(t, 100, pubsub, topicName)

	messages, err := pubsub.Subscribe(topicName)
	require.NoError(t, err)

	var receivedMessages []message.Message

	i := 0
	errsSent := 0
	for len(receivedMessages) < len(messagesToPublish) {
		select {
		case msg := <-messages:
			if errsSent < 2 {
				fmt.Println("sending err for ", msg.UUID())
				msg.Error(errors.Errorf("error %d", errsSent))
				errsSent++
				continue
			}

			receivedMessages = append(receivedMessages, msg)
			i++

			msg.Acknowledge()
			fmt.Println("acked msg ", msg.UUID())

		case <-time.After(time.Second * 15):
			t.Fatalf(
				"timeouted, received messages %d of %d, missing: %v",
				len(receivedMessages),
				len(messagesToPublish),
				tests.MissingMessages(messagesToPublish, receivedMessages),
			)
		}
	}

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
}

func noAckTest(t *testing.T, pubsub message.PubSub) {
	defer closePubSub(t, pubsub)
	topicName := testTopicName()

	var messagesToPublish []message.Message

	for i := 0; i < 2; i++ {
		id := uuid.NewV4().String()
		// same type, to be sure that messages are sent to same consumer
		payload := MessageWithType{0, i}

		msg := message.NewDefault(id, payload)

		messagesToPublish = append(messagesToPublish, msg)
	}

	err := pubsub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	messages, err := pubsub.Subscribe(topicName)
	require.NoError(t, err)

	wait := make(chan struct{}, 1)
	go func() {
		msg := <-messages
		<-wait
		msg.Acknowledge()
	}()

	select {
	case <-messages:
		t.Fatal("messages channel should be blocked since Acknowledge() was not sent")
	case <-time.After(time.Millisecond * 100):
		// ok
	}

	wait <- struct{}{}

	select {
	case msg := <-messages:
		msg.Acknowledge()
	case <-time.After(time.Second * 5):
		t.Fatal("messages channel should be unblocked after Acknowledge()")
	}

	select {
	case <-messages:
		t.Fatal("msg should be not sent again")
	case <-time.After(time.Millisecond * 50):
		// ok
	}
}

func continueAfterCloseTest(t *testing.T, createPubSub PubSubConstructor) {
	topicName := testTopicName()
	consumerGroup := generateConsumerGroup()

	totalMessagesCount := 500

	pubsub := createPubSub(t, consumerGroup)
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, pubsub, topicName)
	closePubSub(t, pubsub)

	receivedMessagesMap := map[string]message.Message{}
	var receivedMessages []message.Message
	messagesLeft := totalMessagesCount

	// with at-least-once delivery we cannot assume that 5 (5*20msg=100) clients will be enough
	// because messages will be delivered twice
	for i := 0; i < 20; i++ {
		addedByConsumer := 0
		pubsub := createPubSub(t, consumerGroup)

		messages, err := pubsub.Subscribe(topicName)
		require.NoError(t, err)

		receivedMessagesPart, _ := subscriber.BulkRead(messages, 100, time.Second*10)

		for _, msg := range receivedMessagesPart {
			// we assume at at-least-once delivery, so we ignore duplicates
			if _, ok := receivedMessagesMap[msg.UUID()]; ok {
				fmt.Printf("%s is duplicated\n", msg.UUID())
			} else {
				addedByConsumer++
				messagesLeft--
				receivedMessagesMap[msg.UUID()] = msg
				receivedMessages = append(receivedMessages, msg)
			}
		}

		closePubSub(t, pubsub)

		fmt.Println(
			"already received:", len(receivedMessagesMap),
			"total:", len(messagesToPublish),
			"received by this consumer:", addedByConsumer,
			"new in this consumer (unique):", len(receivedMessagesPart),
		)
		if messagesLeft == 0 {
			break
		}
	}

	for _, msgToPublish := range messagesToPublish {
		_, ok := receivedMessagesMap[msgToPublish.UUID()]
		assert.True(t, ok, "missing msg %s", msgToPublish.UUID())
	}

	fmt.Println("received:", len(receivedMessagesMap))
	fmt.Println("missing:", tests.MissingMessages(messagesToPublish, receivedMessages))
	fmt.Println("extra:", tests.MissingMessages(receivedMessages, messagesToPublish))
}

func continueAfterErrors(t *testing.T, createPubSub PubSubConstructor) {
	topicName := testTopicName()
	consumerGroup := generateConsumerGroup()

	totalMessagesCount := 50

	pubsub := createPubSub(t, consumerGroup)
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, pubsub, topicName)
	closePubSub(t, pubsub)

	// sending totalMessagesCount*2 errors from 3 consumers
	for i := 0; i < 3; i++ {
		errorsPubSub := createPubSub(t, consumerGroup)

		messages, err := errorsPubSub.Subscribe(topicName)
		require.NoError(t, err)

		// waiting to initialize
		msg := <-messages
		msg.Error(errors.New("error"))

		for j := 0; j < totalMessagesCount*2; j++ {
			select {
			case msg := <-messages:
				msg.Error(errors.New("error"))
			case <-time.After(time.Second * 5):
				t.Fatal("no messages left, probably seek after error doesn't work")
			}
		}

		closePubSub(t, errorsPubSub)
	}

	pubsub = createPubSub(t, consumerGroup)
	defer closePubSub(t, pubsub)

	messages, err := pubsub.Subscribe(topicName)
	require.NoError(t, err)

	// no message should be consumed
	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
}

func addSimpleMessagesMessages(t *testing.T, messagesCount int, pubSub message.PubSub, topicName string) ([]message.Message) {
	var messagesToPublish []message.Message

	for i := 0; i < messagesCount; i++ {
		id := uuid.NewV4().String()
		payload := SimpleMessage{i}

		msg := message.NewDefault(id, payload)
		messagesToPublish = append(messagesToPublish, msg)
	}

	err := pubSub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	return messagesToPublish
}

func consumerGroupsTest(t *testing.T, pubsubConstructor PubSubConstructor) {
	topicName := testTopicName()
	totalMessagesCount := 50

	publisher := pubsubConstructor(t, generateConsumerGroup())
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, publisher, topicName)
	closePubSub(t, publisher)

	group1 := generateConsumerGroup()
	group2 := generateConsumerGroup()
	assertConsumerGroupReceivedMessages(t, pubsubConstructor, topicName, group1, messagesToPublish)
	assertConsumerGroupReceivedMessages(t, pubsubConstructor, topicName, group2, messagesToPublish)

	subscriberGroup1 := pubsubConstructor(t, group1)
	messages, err := subscriberGroup1.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, _ := subscriber.BulkRead(messages, 1, time.Second*2)
	assert.Equal(t, 0, len(receivedMessages))
}

func assertConsumerGroupReceivedMessages(t *testing.T, pubsubConstructor PubSubConstructor, topicName, consumerGroup string, expectedMessages []message.Message) {
	s := pubsubConstructor(t, consumerGroup)
	defer closePubSub(t, s)

	messages, err := s.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(expectedMessages), time.Second*10)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, expectedMessages, receivedMessages)
}

func testTopicName() string {
	return "_test_" + uuid.NewV4().String()
}

func closePubSub(t *testing.T, pubsub message.PubSub) {
	err := pubsub.Close()
	assert.NoError(t, err)
}

func generateConsumerGroup() string {
	return uuid.NewV4().String()
}
