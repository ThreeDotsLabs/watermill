package message_test

import (
	"testing"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"time"
	"github.com/roblaszczak/gooddd/internal/tests"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/roblaszczak/gooddd"
	"github.com/roblaszczak/gooddd/message/subscriber"
	"github.com/stretchr/testify/assert"
	"github.com/roblaszczak/gooddd/message"
)

type publisherMsg struct {
	Num int `json:"num"`
}

type msgPublishedByHandler struct{}

func TestFunctional(t *testing.T) {
	testID := uuid.NewV4().String()
	topicName := "test_topic_" + testID

	pubSub, err := createPubSub()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, pubSub.Close())
	}()

	messagesCount := 100
	expectedReceivedMessages := publishMessagesForHandler(t, messagesCount, pubSub, topicName)

	receivedMessagesCh1 := make(chan message.ConsumedMessage, messagesCount)
	receivedMessagesCh2 := make(chan message.ConsumedMessage, messagesCount)
	sentByHandlerCh := make(chan message.Message, messagesCount)

	publishedEventsTopic := "published_events_" + testID
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
		func(msg message.ConsumedMessage) (producedMessages []message.ProducedMessage, err error) {
			receivedMessagesCh1 <- msg
			msg.Acknowledge()

			toPublish := message.NewDefault(uuid.NewV4().String(), msgPublishedByHandler{})
			sentByHandlerCh <- toPublish

			return []message.ProducedMessage{toPublish}, nil
		},
	)
	require.NoError(t, err)

	err = h.AddHandler(
		"test_subscriber_2",
		topicName,
		func(msg message.ConsumedMessage) (producedMessages []message.ProducedMessage, err error) {
			receivedMessagesCh2 <- msg
			msg.Acknowledge()
			return nil, nil
		},
	)
	require.NoError(t, err)

	go h.Run()
	defer func() {
		assert.NoError(t, h.Close())
	}()



	expectedSentByHandler, all := readMessages(sentByHandlerCh, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)

	receivedMessages1, all := subscriber.BulkRead(receivedMessagesCh1, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages1)

	receivedMessages2, all := subscriber.BulkRead(receivedMessagesCh2, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages2)

	publishedByHandlerCh, err := pubSub.Subscribe(publishedEventsTopic, "test")
	require.NoError(t, err)
	publishedByHandler, all := subscriber.BulkRead(publishedByHandlerCh, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedSentByHandler, publishedByHandler)
}

func publishMessagesForHandler(t *testing.T, messagesCount int, pubSub message.PubSub, topicName string) ([]message.Message) {
	var messagesToPublish []message.ProducedMessage
	var messagesToPublishMessage []message.Message
	for i := 0; i < messagesCount; i++ {
		msg := message.NewDefault(uuid.NewV4().String(), publisherMsg{i})

		messagesToPublish = append(messagesToPublish, msg)
		messagesToPublishMessage = append(messagesToPublishMessage, msg)
	}

	err := pubSub.Publish(topicName, messagesToPublish)

	require.NoError(t, err)

	return messagesToPublishMessage
}

func createPubSub() (message.PubSub, error) {
	brokers := []string{"localhost:9092"}
	marshaler := marshal.Json{}
	logger := gooddd.NewStdLogger(true, true)

	pub, err := kafka.NewPublisher(brokers, marshaler)
	if err != nil {
		return nil, err
	}

	sub, err := kafka.NewConfluentSubscriber(kafka.SubscriberConfig{
		Brokers:        brokers,
		ConsumersCount: 8,
	}, marshaler, logger)
	if err != nil {
		return nil, err
	}

	return message.NewPubSub(pub, sub), nil
}

func readMessages(messagesCh <-chan message.Message, limit int, timeout time.Duration) (receivedMessages []message.Message, all bool) {
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
