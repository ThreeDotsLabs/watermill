package handler_test

import (
	"testing"
	"github.com/satori/go.uuid"
	"github.com/roblaszczak/gooddd/message"
	"github.com/stretchr/testify/require"
	"github.com/roblaszczak/gooddd/message/handler"
	"time"
	"github.com/roblaszczak/gooddd/internal/tests"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/roblaszczak/gooddd"
	"github.com/roblaszczak/gooddd/message/subscriber"
	"github.com/stretchr/testify/assert"
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
	expectedReceivedMessages := publishMessagesForRouter(t, messagesCount, pubSub, topicName)

	receivedMessagesCh := make(chan message.Message, messagesCount)
	sentByRouterCh := make(chan message.Message, messagesCount)

	publishedEventsTopic := "published_events_" + testID
	router := handler.NewRouter("test_"+testID, publishedEventsTopic, pubSub, pubSub, )
	router.Subscribe(
		"test_consumer",
		topicName,
		func(msg message.Message) (producedMessages []message.Message, err error) {
			receivedMessagesCh <- msg
			msg.Acknowledge()

			toPublish := message.NewDefault(uuid.NewV4().String(), msgPublishedByHandler{})
			sentByRouterCh <- toPublish

			return []message.Message{toPublish}, nil
		},
	)
	go router.Run()
	defer func() {
		assert.NoError(t, router.Close())
	}()

	expectedSentByRouter, all := subscriber.BulkRead(sentByRouterCh, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)

	receivedMessages, all := subscriber.BulkRead(receivedMessagesCh, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages)

	publishedByRouterCh, err := pubSub.Subscribe(publishedEventsTopic)
	require.NoError(t, err)
	publishedByRouter, all := subscriber.BulkRead(publishedByRouterCh, len(expectedReceivedMessages), time.Second*10)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedSentByRouter, publishedByRouter)
}

func publishMessagesForRouter(t *testing.T, messagesCount int, pubSub message.PubSub, topicName string) ([]message.Message) {
	var messagesToPublish []message.Message
	for i := 0; i < messagesCount; i++ {
		messagesToPublish = append(messagesToPublish, message.NewDefault(uuid.NewV4().String(), publisherMsg{i}))
	}

	err := pubSub.Publish(topicName, messagesToPublish)

	require.NoError(t, err)

	return messagesToPublish
}

func createPubSub() (message.PubSub, error) {
	return kafka.NewPubSub(
		[]string{"localhost:9092"},
		marshal.Json{},
		"test",
		gooddd.NewStdLogger(true, true),
	)
}
