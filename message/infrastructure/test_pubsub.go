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

type PubSubConstructor func(t *testing.T) message.PubSub

type MessageWithType struct {
	Type int `json:"type"`
	Num  int `json:"num"`
}

type SimpleMessage struct {
	Num int `json:"num"`
}

func TestPubSub(t *testing.T, features Features, pubSubConstructor PubSubConstructor) {
	t.Run("publishSubscribe", func(t *testing.T) {
		t.Parallel()
		publishSubscribeTest(t, pubSubConstructor(t))
	})

	t.Run("resendOnError", func(t *testing.T) {
		t.Parallel()
		resendOnErrorTest(t, pubSubConstructor(t))
	})

	t.Run("noAck", func(t *testing.T) {
		if !features.GuaranteedOrder {
			t.Skip("guaranteed order is required for this test")
		}
		t.Parallel()
		noAckTest(t, pubSubConstructor(t))
	})

	t.Run("continueAfterClose", func(t *testing.T) {
		if features.ExactlyOnceDelivery {
			t.Skip("ExactlyOnceDelivery test is not supported yet")
		}

		t.Parallel()
		continueAfterCloseTest(t, pubSubConstructor)
	})

	t.Run("continueAfterErrors", func(t *testing.T) {
		t.Parallel()
		continueAfterErrors(t, pubSubConstructor)
	})

	t.Run("publishSubscribeInOrderTest", func(t *testing.T) {
		if !features.GuaranteedOrder {
			t.Skipf("order is not guaranteed")
		}

		t.Parallel()
		publishSubscribeInOrderTest(t, pubSubConstructor(t))
	})

	t.Run("consumerGroupsTest", func(t *testing.T) {
		if !features.ConsumerGroups {
			t.Skip("consumer groups are not supported")
		}

		t.Parallel()
		consumerGroupsTest(t, pubSubConstructor)
	})
}

var stressTestTestsCount = 20

func TestPubSubStressTest(t *testing.T, features Features, pubSubConstructor PubSubConstructor) {
	for i := 0; i < stressTestTestsCount; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			TestPubSub(t, features, pubSubConstructor)
		})
	}
}

func publishSubscribeTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	var messagesToPublish []message.ProducedMessage
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

	err := pubSub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	messages, err := pubSub.Subscribe(topicName, generateConsumerGroup())
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
	tests.AssertMessagesPayloads(t, messagesPayloads, receivedMessages, func(msg message.ConsumedMessage) interface{} {
		payload := SimpleMessage{}
		msg.UnmarshalPayload(&payload)
		return payload
	})
	tests.AssertMessagesMetadata(t, "test", messagesTestMetadata, receivedMessages)
}

func publishSubscribeInOrderTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	var messagesToPublish []message.ProducedMessage
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

	err := pubSub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	messages, err := pubSub.Subscribe(topicName, generateConsumerGroup())
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

func resendOnErrorTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	messagesToPublish := addSimpleMessagesMessages(t, 100, pubSub, topicName)

	messages, err := pubSub.Subscribe(topicName, generateConsumerGroup())
	require.NoError(t, err)

	var receivedMessages []message.ConsumedMessage

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

func noAckTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	var messagesToPublish []message.ProducedMessage

	for i := 0; i < 2; i++ {
		id := uuid.NewV4().String()
		// same type, to be sure that messages are sent to same subscriber
		payload := MessageWithType{0, i}

		msg := message.NewDefault(id, payload)

		messagesToPublish = append(messagesToPublish, msg)
	}

	err := pubSub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	messages, err := pubSub.Subscribe(topicName, generateConsumerGroup())
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

	pubSub := createPubSub(t)
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, pubSub, topicName)
	closePubSub(t, pubSub)

	receivedMessagesMap := map[string]message.Message{}
	var receivedMessages []message.ConsumedMessage
	messagesLeft := totalMessagesCount

	// with at-least-once delivery we cannot assume that 5 (5*20msg=100) clients will be enough
	// because messages will be delivered twice
	for i := 0; i < 20; i++ {
		addedBySubscriber := 0
		pubSub := createPubSub(t)

		messages, err := pubSub.Subscribe(topicName, consumerGroup)
		require.NoError(t, err)

		receivedMessagesPart, _ := subscriber.BulkRead(messages, 100, time.Second*10)

		for _, msg := range receivedMessagesPart {
			// we assume at at-least-once delivery, so we ignore duplicates
			if _, ok := receivedMessagesMap[msg.UUID()]; ok {
				fmt.Printf("%s is duplicated\n", msg.UUID())
			} else {
				addedBySubscriber++
				messagesLeft--
				receivedMessagesMap[msg.UUID()] = msg
				receivedMessages = append(receivedMessages, msg)
			}
		}

		closePubSub(t, pubSub)

		fmt.Println(
			"already received:", len(receivedMessagesMap),
			"total:", len(messagesToPublish),
			"received by this subscriber:", addedBySubscriber,
			"new in this subscriber (unique):", len(receivedMessagesPart),
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
	fmt.Println("extra:", tests.MissingMessages(messagesToPublish, receivedMessages))
}

func continueAfterErrors(t *testing.T, createPubSub PubSubConstructor) {
	topicName := testTopicName()
	consumerGroup := generateConsumerGroup()

	totalMessagesCount := 50

	pubSub := createPubSub(t)
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, pubSub, topicName)
	closePubSub(t, pubSub)

	// sending totalMessagesCount*2 errors from 3 subscribers
	for i := 0; i < 3; i++ {
		errorsPubSub := createPubSub(t)

		messages, err := errorsPubSub.Subscribe(topicName, consumerGroup)
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

	pubSub = createPubSub(t)
	defer closePubSub(t, pubSub)

	messages, err := pubSub.Subscribe(topicName, consumerGroup)
	require.NoError(t, err)

	// no message should be consumed
	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
}

func addSimpleMessagesMessages(t *testing.T, messagesCount int, pubSub message.PubSub, topicName string) ([]message.ProducedMessage) {
	var messagesToPublish []message.ProducedMessage

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

func consumerGroupsTest(t *testing.T, pubSubConstructor PubSubConstructor) {
	topicName := testTopicName()
	totalMessagesCount := 50

	publisher := pubSubConstructor(t)
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, publisher, topicName)
	closePubSub(t, publisher)

	group1 := generateConsumerGroup()
	group2 := generateConsumerGroup()
	assertConsumerGroupReceivedMessages(t, pubSubConstructor, topicName, group1, messagesToPublish)
	assertConsumerGroupReceivedMessages(t, pubSubConstructor, topicName, group2, messagesToPublish)

	subscriberGroup1 := pubSubConstructor(t)
	messages, err := subscriberGroup1.Subscribe(topicName, group1)
	require.NoError(t, err)

	receivedMessages, _ := subscriber.BulkRead(messages, 1, time.Second*2)
	assert.Equal(t, 0, len(receivedMessages))
}

func assertConsumerGroupReceivedMessages(t *testing.T, pubSubConstructor PubSubConstructor, topicName string, consumerGroup message.ConsumerGroup, expectedMessages []message.ProducedMessage) {
	s := pubSubConstructor(t)
	defer closePubSub(t, s)

	messages, err := s.Subscribe(topicName, consumerGroup)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(expectedMessages), time.Second*10)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, expectedMessages, receivedMessages)
}

func testTopicName() string {
	return "_test_" + uuid.NewV4().String()
}

func closePubSub(t *testing.T, pubSub message.PubSub) {
	err := pubSub.Close()
	assert.NoError(t, err)
}

func generateConsumerGroup() message.ConsumerGroup {
	return message.ConsumerGroup(uuid.NewV4().String())
}
