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

type MessageWithType struct {
	Type int `json:"type"`
	Num  int `json:"num"`
}

type SimpleMessage struct {
	Num int `json:"num"`
}

func PublishSubscribeTest(t *testing.T, pubsub message.PubSub) {
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

	receivedMessages, err := subscriber.ReadAll(messages, len(messagesToPublish), time.Second*10)
	require.NoError(t, err)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
	tests.AssertMessagesPayloads(t, messagesPayloads, receivedMessages, func(msg message.Message) interface{} {
		payload := SimpleMessage{}
		msg.UnmarshalPayload(&payload)
		return payload
	})
	tests.AssertMessagesMetadata(t, "test", messagesTestMetadata, receivedMessages)
}

func PublishSubscribeInOrderTest(t *testing.T, pubsub message.PubSub) {
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

	receivedMessages, err := subscriber.ReadAll(messages, len(messagesToPublish), time.Second*10)
	require.NoError(t, err)

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

func PublishSubscribeTest_resend_on_error(t *testing.T, pubsub message.PubSub) {
	defer closePubSub(t, pubsub)
	topicName := testTopicName()

	var messagesToPublish []message.Message

	for i := 0; i < 100; i++ {
		id := uuid.NewV4().String()
		payload := SimpleMessage{i}

		msg := message.NewDefault(id, payload)

		messagesToPublish = append(messagesToPublish, msg)
	}

	err := pubsub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

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
				time.Sleep(time.Second)
				continue
			}

			receivedMessages = append(receivedMessages, msg)
			i++

			msg.Acknowledge()
			fmt.Println("acked msg ", msg.UUID())

		case <-time.After(time.Second * 5):
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

func PublishSubscribeTest_no_ack(t *testing.T, pubsub message.PubSub) {
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

func PublishSubscribeTest_continue_after_close(t *testing.T, createPubSub func(t *testing.T) message.PubSub) {
	topicName := testTopicName()

	var messagesToPublish []message.Message

	messagesCount := 50
	for i := 0; i < messagesCount; i++ {
		id := uuid.NewV4().String()
		payload := SimpleMessage{i}

		msg := message.NewDefault(id, payload)
		messagesToPublish = append(messagesToPublish, msg)
	}

	pubsub := createPubSub(t)
	err := pubsub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)
	closePubSub(t, pubsub)

	receivedMessagesMap := map[string]message.Message{}
	var receivedMessages []message.Message

	// with at-least-once delivery we cannot assume that 5 (5*20msg=100) clients will be enough
	// because messages will be delivered twice
	for i := 0; i < 20; i++ {
		pubsub := createPubSub(t)

		messages, err := pubsub.Subscribe(topicName)
		require.NoError(t, err)

		receivedMessagesPart, _ := subscriber.ReadAll(messages, 20, time.Second*5)

		for _, msg := range receivedMessagesPart {
			// we assume at at-least-once delivery, so we ignore duplicates
			if _, ok := receivedMessagesMap[msg.UUID()]; ok {
				fmt.Printf("%s is duplicated", msg.UUID())
			}
			receivedMessagesMap[msg.UUID()] = msg
			receivedMessages = append(receivedMessages, msg)
		}

		closePubSub(t, pubsub)

		if len(receivedMessagesMap) == len(messagesToPublish) {
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

func testTopicName() string {
	return "_test_" + uuid.NewV4().String()
}

func closePubSub(t *testing.T, pubsub message.PubSub) {
	err := pubsub.Close()
	assert.NoError(t, err)
}
