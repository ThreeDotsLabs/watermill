package message_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_Functional(t *testing.T) {
	testID := uuid.NewV4().String()
	topicName := "test_topic_" + testID

	pubSub, err := createPubSub()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, pubSub.Close())
	}()

	messagesCount := 100

	var expectedReceivedMessages message.Messages
	allMessagesSent := make(chan struct{})
	go func() {
		expectedReceivedMessages = publishMessagesForHandler(t, messagesCount, pubSub, topicName)
		allMessagesSent <- struct{}{}
	}()

	receivedMessagesCh1 := make(chan *message.Message, messagesCount)
	receivedMessagesCh2 := make(chan *message.Message, messagesCount)
	sentByHandlerCh := make(chan *message.Message, messagesCount)

	publishedEventsTopic := "published_events_" + testID
	publishedByHandlerCh, err := pubSub.Subscribe(publishedEventsTopic, "test")
	require.NoError(t, err)

	h, err := message.NewRouter(
		message.RouterConfig{
			ServerName:         "test_" + testID,
			PublishEventsTopic: publishedEventsTopic,
		},
		pubSub,
		pubSub,
	)
	require.NoError(t, err)

	err = h.AddHandler(
		"test_subscriber_1",
		topicName,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			receivedMessagesCh1 <- msg

			toPublish := message.NewMessage(uuid.NewV4().String(), nil)
			sentByHandlerCh <- toPublish

			return []*message.Message{toPublish}, nil
		},
	)
	require.NoError(t, err)

	err = h.AddHandler(
		"test_subscriber_2",
		topicName,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			receivedMessagesCh2 <- msg
			return nil, nil
		},
	)
	require.NoError(t, err)

	go h.Run()
	defer func() {
		assert.NoError(t, h.Close())
	}()

	<-allMessagesSent

	expectedSentByHandler, all := readMessages(sentByHandlerCh, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)

	receivedMessages1, all := subscriber.BulkRead(receivedMessagesCh1, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages1)

	receivedMessages2, all := subscriber.BulkRead(receivedMessagesCh2, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages2)

	publishedByHandler, all := subscriber.BulkRead(publishedByHandlerCh, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedSentByHandler, publishedByHandler)
}

func publishMessagesForHandler(t *testing.T, messagesCount int, pubSub message.PubSub, topicName string) []*message.Message {
	var messagesToPublish []*message.Message
	var messagesToPublishMessage []*message.Message
	for i := 0; i < messagesCount; i++ {
		msg := message.NewMessage(uuid.NewV4().String(), []byte(fmt.Sprintf("%d", i)))

		messagesToPublish = append(messagesToPublish, msg)
		messagesToPublishMessage = append(messagesToPublishMessage, msg)
	}

	for _, msg := range messagesToPublish {
		err := pubSub.Publish(topicName, msg)
		require.NoError(t, err)
	}

	return messagesToPublishMessage
}

func createPubSub() (message.PubSub, error) {
	return gochannel.NewGoChannel(0, watermill.NewStdLogger(true, true), time.Second*10), nil
}

func readMessages(messagesCh <-chan *message.Message, limit int, timeout time.Duration) (receivedMessages []*message.Message, all bool) {
	allMessagesReceived := make(chan struct{}, 1)

	go func() {
		for msg := range messagesCh {
			receivedMessages = append(receivedMessages, msg)

			if len(receivedMessages) == limit {
				allMessagesReceived <- struct{}{}
				break
			}
		}
		// messagesCh closed
		allMessagesReceived <- struct{}{}
	}()

	select {
	case <-allMessagesReceived:
	case <-time.After(timeout):
	}

	return receivedMessages, len(receivedMessages) == limit
}
