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
)

func TestFunctional(t *testing.T) {
	testID := uuid.NewV4().String()
	topicName := "test_topic_" + testID

	// todo - run with other pubsub
	pubsub, err := kafka.NewPubSub(
		[]string{"localhost:9092"},
		marshal.Json{},
		"test",
		gooddd.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	type publisherMsg struct {
		Num int `json:"num"`
	}

	var messagesToPublish []message.Message
	for i := 0; i < 100; i++ {
		messagesToPublish = append(messagesToPublish, message.NewDefault(uuid.NewV4().String(), publisherMsg{i}))
	}

	err = pubsub.Publish(topicName, messagesToPublish)
	require.NoError(t, err)

	receivedMessagesCh := make(chan message.Message)

	router := handler.NewRouter("test_"+testID, "published_events", pubsub, pubsub, )
	router.Subscribe(
		"test_consumer",
		topicName,
		func(msg message.Message) (producedMessages []message.Message, err error) {
			receivedMessagesCh <- msg
			msg.Acknowledge()
			return nil, nil
		},
	)
	go router.Run()

	receivedMessages, err := subscriber.ReadAll(receivedMessagesCh, len(messagesToPublish), time.Second*10)
	require.NoError(t, err)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
}

// todo - test multiple handlers
