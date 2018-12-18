package infrastructure

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultTimeout = time.Second * 10

type Features struct {
	ConsumerGroups      bool
	ExactlyOnceDelivery bool
	GuaranteedOrder     bool
	Persistent          bool
}

type PubSubConstructor func(t *testing.T) message.PubSub
type ConsumerGroupPubSubConstructor func(t *testing.T, consumerGroup string) message.PubSub

type SimpleMessage struct {
	Num int `json:"num"`
}

func TestPubSub(
	t *testing.T,
	features Features,
	pubSubConstructor PubSubConstructor,
	consumerGroupPubSubConstructor ConsumerGroupPubSubConstructor,
) {
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

	t.Run("concurrentClose", func(t *testing.T) {
		if features.ExactlyOnceDelivery {
			t.Skip("ExactlyOnceDelivery test is not supported yet")
		}

		t.Parallel()
		concurrentClose(t, pubSubConstructor)
	})

	t.Run("continueAfterErrors", func(t *testing.T) {
		if !features.Persistent {
			t.Skip("continueAfterErrors test is not supported for non persistent pub/sub")
		}

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
		consumerGroupsTest(t, consumerGroupPubSubConstructor)
	})

	t.Run("publisherCloseTest", func(t *testing.T) {
		t.Parallel()

		pubsub := pubSubConstructor(t)

		publisherCloseTest(t, pubsub, pubsub)
	})

	t.Run("topicTest", func(t *testing.T) {
		t.Parallel()
		topicTest(t, pubSubConstructor(t))
	})

	t.Run("messageCtx", func(t *testing.T) {
		t.Parallel()
		testMessageCtx(t, pubSubConstructor(t))
	})
}

var stressTestTestsCount = 20

func TestPubSubStressTest(
	t *testing.T,
	features Features,
	pubSubConstructor PubSubConstructor,
	consumerGroupPubSubConstructor ConsumerGroupPubSubConstructor,
) {
	for i := 0; i < stressTestTestsCount; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			TestPubSub(t, features, pubSubConstructor, consumerGroupPubSubConstructor)
		})
	}
}

func publishSubscribeTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	var messagesToPublish []*message.Message
	messagesPayloads := map[string]interface{}{}
	messagesTestMetadata := map[string]string{}

	for i := 0; i < 100; i++ {
		id := uuid.NewV4().String()
		testMetadata := uuid.NewV4().String()

		payload := []byte(fmt.Sprintf("%d", i))
		msg := message.NewMessage(id, payload)

		msg.Metadata.Set("test", testMetadata)
		messagesTestMetadata[id] = testMetadata

		messagesToPublish = append(messagesToPublish, msg)
		messagesPayloads[id] = payload
	}

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	go func() {
		err := pubSub.Publish(topicName, messagesToPublish...)
		require.NoError(t, err, "cannot publish message")
	}()

	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), defaultTimeout*3)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
	tests.AssertMessagesPayloads(t, messagesPayloads, receivedMessages)
	tests.AssertMessagesMetadata(t, "test", messagesTestMetadata, receivedMessages)
}

func publishSubscribeInOrderTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	var messagesToPublish []*message.Message
	expectedMessages := map[string][]string{}

	for i := 0; i < 100; i++ {
		id := uuid.NewV4().String()
		msgType := string(i % 16)

		msg := message.NewMessage(id, []byte(msgType))

		messagesToPublish = append(messagesToPublish, msg)

		if _, ok := expectedMessages[msgType]; !ok {
			expectedMessages[msgType] = []string{}
		}
		expectedMessages[msgType] = append(expectedMessages[msgType], msg.UUID)
	}

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	go func() {
		err := pubSub.Publish(topicName, messagesToPublish...)
		require.NoError(t, err)
	}()

	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), defaultTimeout)
	require.True(t, all)

	receivedMessagesByType := map[string][]string{}
	for _, msg := range receivedMessages {

		if _, ok := receivedMessagesByType[string(msg.Payload)]; !ok {
			receivedMessagesByType[string(msg.Payload)] = []string{}
		}
		receivedMessagesByType[string(msg.Payload)] = append(receivedMessagesByType[string(msg.Payload)], msg.UUID)
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

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	//var messagesToPublish message.Messages
	messagesToSend := 100

	var publishedMessages message.Messages
	allMessagesSent := make(chan struct{})

	go func() {
		publishedMessages = addSimpleMessagesMessages(t, messagesToSend, pubSub, topicName)
		allMessagesSent <- struct{}{}
	}()

	var receivedMessages []*message.Message

	i := 0
	errsSent := 0

ReadMessagesLoop:
	for len(receivedMessages) < messagesToSend {
		select {
		case msg := <-messages:
			if errsSent < 2 {
				log.Println("sending err for ", msg.UUID)
				msg.Nack()
				errsSent++
				continue
			}

			receivedMessages = append(receivedMessages, msg)
			i++

			msg.Ack()
			fmt.Println("acked msg ", msg.UUID)

		case <-time.After(defaultTimeout):
			break ReadMessagesLoop
		}
	}

	<-allMessagesSent
	tests.AssertAllMessagesReceived(t, publishedMessages, receivedMessages)
}

func noAckTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	go func() {
		for i := 0; i < 2; i++ {
			id := uuid.NewV4().String()
			log.Printf("sending %s", id)

			msg := message.NewMessage(id, nil)

			err := pubSub.Publish(topicName, msg)
			require.NoError(t, err)
		}
	}()

	receivedMessage := make(chan struct{})
	unlockAck := make(chan struct{}, 1)
	go func() {
		msg := <-messages
		receivedMessage <- struct{}{}
		<-unlockAck
		msg.Ack()
	}()

	<-receivedMessage
	select {
	case msg := <-messages:
		t.Fatalf("messages channel should be blocked since Ack() was not sent, received %s", msg.UUID)
	case <-time.After(time.Millisecond * 100):
		// ok
	}

	unlockAck <- struct{}{}

	select {
	case msg := <-messages:
		msg.Ack()
	case <-time.After(time.Second * 5):
		t.Fatal("messages channel should be unblocked after Ack()")
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
	totalMessagesCount := 500

	pubSub := createPubSub(t)
	defer pubSub.Close()

	// call subscribe once for those pubsubs which require subscribe before publish
	_, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)
	closePubSub(t, pubSub)

	pubSub = createPubSub(t)
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, pubSub, topicName)
	closePubSub(t, pubSub)

	receivedMessagesMap := map[string]*message.Message{}
	var receivedMessages []*message.Message
	messagesLeft := totalMessagesCount

	// with at-least-once delivery we cannot assume that 5 (5*20msg=100) clients will be enough
	// because messages will be delivered twice
	for i := 0; i < 20; i++ {
		addedBySubscriber := 0
		pubSub := createPubSub(t)

		messages, err := pubSub.Subscribe(topicName)
		require.NoError(t, err)

		receivedMessagesPart, _ := subscriber.BulkRead(messages, 100, defaultTimeout)

		for _, msg := range receivedMessagesPart {
			// we assume at at-least-once delivery, so we ignore duplicates
			if _, ok := receivedMessagesMap[msg.UUID]; ok {
				fmt.Printf("%s is duplicated\n", msg.UUID)
			} else {
				addedBySubscriber++
				messagesLeft--
				receivedMessagesMap[msg.UUID] = msg
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
		_, ok := receivedMessagesMap[msgToPublish.UUID]
		assert.True(t, ok, "missing msg %s", msgToPublish.UUID)
	}

	fmt.Println("received:", len(receivedMessagesMap))
	fmt.Println("missing:", tests.MissingMessages(messagesToPublish, receivedMessages))
	fmt.Println("extra:", tests.MissingMessages(messagesToPublish, receivedMessages))
}

func concurrentClose(t *testing.T, createPubSub PubSubConstructor) {
	topicName := testTopicName()
	totalMessagesCount := 50

	closeWg := sync.WaitGroup{}
	closeWg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			defer closeWg.Done()

			pubSub := createPubSub(t)
			_, err := pubSub.Subscribe(topicName)
			require.NoError(t, err)
			closePubSub(t, pubSub)
		}()
	}

	closeWg.Wait()

	pubSub := createPubSub(t)
	expectedMessages := addSimpleMessagesMessages(t, totalMessagesCount, pubSub, topicName)
	closePubSub(t, pubSub)

	pubSub = createPubSub(t)
	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(expectedMessages), defaultTimeout*3)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, expectedMessages, receivedMessages)
}

func continueAfterErrors(t *testing.T, createPubSub PubSubConstructor) {
	topicName := testTopicName()

	totalMessagesCount := 50

	pubSub := createPubSub(t)

	// call subscribe once for those pubsubs which require subscribe before publish
	_, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)
	closePubSub(t, pubSub)

	pubSub = createPubSub(t)
	defer closePubSub(t, pubSub)

	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, pubSub, topicName)

	// sending totalMessagesCount*2 errors from 3 subscribers
	for i := 0; i < 3; i++ {
		errorsPubSub := createPubSub(t)

		messages, err := errorsPubSub.Subscribe(topicName)
		require.NoError(t, err)

		// waiting to initialize
		msg := <-messages
		msg.Nack()

		for j := 0; j < totalMessagesCount*2; j++ {
			select {
			case msg := <-messages:
				msg.Nack()
			case <-time.After(time.Second * 5):
				t.Fatal("no messages left, probably seek after error doesn't work")
			}
		}

		closePubSub(t, errorsPubSub)
	}

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	// only nacks was sent, so all messages should be consumed
	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), defaultTimeout)
	require.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
}

func consumerGroupsTest(t *testing.T, pubSubConstructor ConsumerGroupPubSubConstructor) {
	topicName := testTopicName()
	totalMessagesCount := 50

	group1 := generateConsumerGroup(t, pubSubConstructor, topicName)
	group2 := generateConsumerGroup(t, pubSubConstructor, topicName)

	publisher := pubSubConstructor(t, "test_"+uuid.NewV4().String())
	messagesToPublish := addSimpleMessagesMessages(t, totalMessagesCount, publisher, topicName)
	closePubSub(t, publisher)

	assertConsumerGroupReceivedMessages(t, pubSubConstructor, group1, topicName, messagesToPublish)
	assertConsumerGroupReceivedMessages(t, pubSubConstructor, group2, topicName, messagesToPublish)

	subscriberGroup1 := pubSubConstructor(t, group1)
	defer closePubSub(t, subscriberGroup1)

	messages, err := subscriberGroup1.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, _ := subscriber.BulkRead(messages, 1, time.Second*2)
	assert.Equal(t, 0, len(receivedMessages))
}

func publisherCloseTest(t *testing.T, pub message.Publisher, sub message.Subscriber) {
	topicName := testTopicName()

	messagesCount := 10000

	messages, err := sub.Subscribe(topicName)
	require.NoError(t, err)

	var producedMessages message.Messages
	allMessagesProduced := make(chan struct{})

	go func() {
		producedMessages = addSimpleMessagesMessages(t, messagesCount, pub, topicName)
		close(allMessagesProduced)
	}()

	receivedMessages, _ := subscriber.BulkRead(messages, messagesCount, defaultTimeout*3)

	select {
	case <-allMessagesProduced:
		// ok
	case <-time.After(time.Second * 30):
		t.Fatal("messages send timeouted")
	}

	tests.AssertAllMessagesReceived(t, producedMessages, receivedMessages)

	require.NoError(t, pub.Close())
	require.NoError(t, sub.Close())
}

func topicTest(t *testing.T, pubSub message.PubSub) {
	defer closePubSub(t, pubSub)

	topic1 := testTopicName()
	topic2 := testTopicName()

	messagesTopic1, err := pubSub.Subscribe(topic1)
	require.NoError(t, err)

	messagesTopic2, err := pubSub.Subscribe(topic2)
	require.NoError(t, err)

	topic1Msg := message.NewMessage(uuid.NewV4().String(), nil)
	topic2Msg := message.NewMessage(uuid.NewV4().String(), nil)

	messagesSent := make(chan struct{})
	go func() {
		require.NoError(t, pubSub.Publish(topic1, topic1Msg))
		require.NoError(t, pubSub.Publish(topic2, topic2Msg))
		close(messagesSent)
	}()

	messagesConsumedTopic1, received := subscriber.BulkRead(messagesTopic1, 1, defaultTimeout)
	require.True(t, received, "no messages received in topic %s", topic1)

	messagesConsumedTopic2, received := subscriber.BulkRead(messagesTopic2, 1, defaultTimeout)
	require.True(t, received, "no messages received in topic %s", topic2)

	<-messagesSent

	assert.Equal(t, messagesConsumedTopic1.IDs()[0], topic1Msg.UUID)
	assert.Equal(t, messagesConsumedTopic2.IDs()[0], topic2Msg.UUID)
}

func testMessageCtx(t *testing.T, pubSub message.PubSub) {
	defer pubSub.Close()

	topic := testTopicName()

	messages, err := pubSub.Subscribe(topic)
	require.NoError(t, err)

	go func() {
		msg := message.NewMessage(uuid.NewV4().String(), nil)

		// ensuring that context is not propagated via pub/sub
		ctx, ctxCancel := context.WithCancel(context.Background())
		ctxCancel()
		msg.SetContext(ctx)

		require.NoError(t, pubSub.Publish(topic, msg))
		require.NoError(t, pubSub.Publish(topic, msg))
	}()

	select {
	case msg := <-messages:
		ctx := msg.Context()

		select {
		case <-ctx.Done():
			t.Fatal("context should not be canceled")
		default:
			// ok
		}

		require.NoError(t, msg.Ack())

		select {
		case <-ctx.Done():
			// ok
		case <-time.After(defaultTimeout):
			t.Fatal("context should be canceled after Ack")
		}
	case <-time.After(defaultTimeout):
		t.Fatal("no message received")
	}

	select {
	case msg := <-messages:
		ctx := msg.Context()

		select {
		case <-ctx.Done():
			t.Fatal("context should not be canceled")
		default:
			// ok
		}

		go require.NoError(t, pubSub.Close())

		select {
		case <-ctx.Done():
			// ok
		case <-time.After(defaultTimeout):
			t.Fatal("context should be canceled after pubSub.Close()")
		}
	case <-time.After(defaultTimeout):
		t.Fatal("no message received")
	}
}

func assertConsumerGroupReceivedMessages(
	t *testing.T,
	pubSubConstructor ConsumerGroupPubSubConstructor,
	consumerGroup string,
	topicName string,
	expectedMessages []*message.Message,
) {
	s := pubSubConstructor(t, consumerGroup)
	defer closePubSub(t, s)

	messages, err := s.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(expectedMessages), defaultTimeout)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, expectedMessages, receivedMessages)
}

func testTopicName() string {
	return "topic_" + uuid.NewV4().String()
}

func closePubSub(t *testing.T, pubSub message.PubSub) {
	err := pubSub.Close()
	assert.NoError(t, err)
}

func generateConsumerGroup(t *testing.T, pubSubConstructor ConsumerGroupPubSubConstructor, topicName string) string {
	groupName := "cg_" + uuid.NewV4().String()

	// create a pubsub to ensure that the consumer group exists
	// for those providers that require subscription before publishing messages (e.g. Google Cloud PubSub)
	pubSub := pubSubConstructor(t, groupName)
	_, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)
	closePubSub(t, pubSub)

	return groupName
}

func addSimpleMessagesMessages(t *testing.T, messagesCount int, publisher message.Publisher, topicName string) message.Messages {
	var messagesToPublish []*message.Message

	for i := 0; i < messagesCount; i++ {
		id := uuid.NewV4().String()

		msg := message.NewMessage(id, nil)
		messagesToPublish = append(messagesToPublish, msg)

		err := publisher.Publish(topicName, msg)
		require.NoError(t, err, "cannot publish messages")
	}

	return messagesToPublish
}
