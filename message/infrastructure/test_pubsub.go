package infrastructure

import (
	"context"
	"fmt"
	"go/build"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultTimeout = time.Second * 15

func init() {
	rand.Seed(3)

	for _, tag := range build.Default.BuildTags {
		if tag == "stress" {
			// stress tests may work a bit slower
			defaultTimeout *= 6
			break
		}
	}
}

type Features struct {
	ConsumerGroups      bool
	ExactlyOnceDelivery bool
	GuaranteedOrder     bool
	Persistent          bool

	RestartServiceCommand []string
}

type PubSubConstructor func(t *testing.T) PubSub
type ConsumerGroupPubSubConstructor func(t *testing.T, consumerGroup string) PubSub

type SimpleMessage struct {
	Num int `json:"num"`
}

type PubSub interface {
	message.PubSub

	// Subscriber is needed for unwrapped message.PubSub's subscriber, containing SubscribeInitializer.
	Subscriber() message.Subscriber
}

func TestPubSub(
	t *testing.T,
	features Features,
	pubSubConstructor PubSubConstructor,
	consumerGroupPubSubConstructor ConsumerGroupPubSubConstructor,
) {
	t.Run("TestPublishSubscribe", func(t *testing.T) {
		t.Parallel()
		TestPublishSubscribe(t, pubSubConstructor(t), features)
	})

	t.Run("TestResendOnError", func(t *testing.T) {
		t.Parallel()
		TestResendOnError(t, pubSubConstructor(t), features)
	})

	t.Run("TestNoAck", func(t *testing.T) {
		t.Parallel()
		TestNoAck(t, pubSubConstructor(t), features)
	})

	t.Run("TestContinueAfterSubscribeClose", func(t *testing.T) {
		t.Parallel()
		TestContinueAfterSubscribeClose(t, pubSubConstructor, features)
	})

	t.Run("TestConcurrentClose", func(t *testing.T) {
		t.Parallel()
		TestConcurrentClose(t, pubSubConstructor, features)
	})

	t.Run("TestContinueAfterErrors", func(t *testing.T) {
		t.Parallel()
		TestContinueAfterErrors(t, pubSubConstructor, features)
	})

	t.Run("TestPublishSubscribeInOrder", func(t *testing.T) {
		t.Parallel()
		TestPublishSubscribeInOrder(t, pubSubConstructor(t), features)
	})

	t.Run("TestConsumerGroups", func(t *testing.T) {
		t.Parallel()
		TestConsumerGroups(t, consumerGroupPubSubConstructor, features)
	})

	t.Run("TestPublisherClose", func(t *testing.T) {
		t.Parallel()
		TestPublisherClose(t, pubSubConstructor(t), features)
	})

	t.Run("TestTopic", func(t *testing.T) {
		t.Parallel()
		TestTopic(t, pubSubConstructor(t), features)
	})

	t.Run("TestMessageCtx", func(t *testing.T) {
		t.Parallel()
		TestMessageCtx(t, pubSubConstructor(t), features)
	})

	t.Run("TestSubscribeCtx", func(t *testing.T) {
		t.Parallel()
		TestSubscribeCtx(t, pubSubConstructor(t), features)
	})
}

var stressTestTestsCount = 5 // todo - change to 20 after splitting Pub/Subs to repos

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

func TestPublishSubscribe(t *testing.T, pubSub PubSub, features Features) {
	topicName := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	var messagesToPublish []*message.Message
	messagesPayloads := map[string]interface{}{}
	messagesTestMetadata := map[string]string{}

	for i := 0; i < 100; i++ {
		id := watermill.NewUUID()
		testMetadata := watermill.NewUUID()

		payload := []byte(fmt.Sprintf("%d", i))
		msg := message.NewMessage(id, payload)

		msg.Metadata.Set("test", testMetadata)
		messagesTestMetadata[id] = testMetadata

		messagesToPublish = append(messagesToPublish, msg)
		messagesPayloads[id] = payload
	}
	err := publishWithRetry(pubSub, topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := bulkRead(messages, len(messagesToPublish), defaultTimeout*3, features)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
	tests.AssertMessagesPayloads(t, messagesPayloads, receivedMessages)
	tests.AssertMessagesMetadata(t, "test", messagesTestMetadata, receivedMessages)

	closePubSub(t, pubSub)
	assertMessagesChannelClosed(t, messages)
}

func TestPublishSubscribeInOrder(t *testing.T, pubSub PubSub, features Features) {
	if !features.GuaranteedOrder {
		t.Skipf("order is not guaranteed")
	}

	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	var messagesToPublish []*message.Message
	expectedMessages := map[string][]string{}

	for i := 0; i < 100; i++ {
		id := watermill.NewUUID()
		msgType := string(i % 16)

		msg := message.NewMessage(id, []byte(msgType))

		messagesToPublish = append(messagesToPublish, msg)

		if _, ok := expectedMessages[msgType]; !ok {
			expectedMessages[msgType] = []string{}
		}
		expectedMessages[msgType] = append(expectedMessages[msgType], msg.UUID)
	}

	err := publishWithRetry(pubSub, topicName, messagesToPublish...)
	require.NoError(t, err)

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := bulkRead(messages, len(messagesToPublish), defaultTimeout, features)
	require.True(t, all, "not all messages received (%d of %d)", len(receivedMessages), len(messagesToPublish))

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

func TestResendOnError(t *testing.T, pubSub PubSub, features Features) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	messagesToSend := 100
	nacksCount := 2

	var publishedMessages message.Messages
	allMessagesSent := make(chan struct{})

	publishedMessages = AddSimpleMessages(t, messagesToSend, pubSub, topicName)
	close(allMessagesSent)

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

NackLoop:
	for i := 0; i < nacksCount; i++ {
		select {
		case msg, closed := <-messages:
			if !closed {
				t.Fatal("messages channel closed before all received")
			}

			log.Println("sending err for ", msg.UUID)
			msg.Nack()
		case <-time.After(defaultTimeout):
			break NackLoop
		}
	}

	receivedMessages, _ := bulkRead(messages, messagesToSend, defaultTimeout, features)

	<-allMessagesSent
	tests.AssertAllMessagesReceived(t, publishedMessages, receivedMessages)
}

func TestNoAck(t *testing.T, pubSub PubSub, features Features) {
	if !features.GuaranteedOrder {
		t.Skip("guaranteed order is required for this test")
	}

	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	for i := 0; i < 2; i++ {
		id := watermill.NewUUID()
		log.Printf("sending %s", id)

		msg := message.NewMessage(id, nil)

		err := publishWithRetry(pubSub, topicName, msg)
		require.NoError(t, err)
	}

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessage := make(chan struct{})
	unlockAck := make(chan struct{}, 1)
	go func() {
		msg := <-messages
		receivedMessage <- struct{}{}
		<-unlockAck
		msg.Ack()
	}()

	select {
	case <-receivedMessage:
	// ok
	case <-time.After(defaultTimeout):
		t.Fatal("timeouted")
	}

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

// TestContinueAfterSubscribeClose checks, that we don't lose messages after closing subscriber.
func TestContinueAfterSubscribeClose(t *testing.T, createPubSub PubSubConstructor, features Features) {
	if !features.Persistent {
		t.Skip("ExactlyOnceDelivery test is not supported yet")
	}

	totalMessagesCount := 5000
	batches := 5
	if testing.Short() {
		totalMessagesCount = 50
		batches = 2
	}
	batchSize := int(totalMessagesCount / batches)
	readAttempts := batches * 4

	pubSub := createPubSub(t)
	defer closePubSub(t, pubSub)

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	messagesToPublish := AddSimpleMessages(t, totalMessagesCount, pubSub, topicName)

	receivedMessages := map[string]*message.Message{}
	for i := 0; i < readAttempts; i++ {
		pubSub := createPubSub(t)

		messages, err := pubSub.Subscribe(context.Background(), topicName)
		require.NoError(t, err)

		receivedMessagesBatch, _ := bulkRead(messages, batchSize, defaultTimeout, features)
		for _, msg := range receivedMessagesBatch {
			receivedMessages[msg.UUID] = msg
		}

		closePubSub(t, pubSub)

		if len(receivedMessages) >= totalMessagesCount {
			break
		}
	}

	// we need to deduplicate messages, because bulkRead will deduplicate only per one batch
	uniqueReceivedMessages := message.Messages{}
	for _, msg := range receivedMessages {
		uniqueReceivedMessages = append(uniqueReceivedMessages, msg)
	}

	tests.AssertAllMessagesReceived(t, messagesToPublish, uniqueReceivedMessages)
}

func TestConcurrentClose(t *testing.T, createPubSub PubSubConstructor, features Features) {
	if features.ExactlyOnceDelivery {
		t.Skip("ExactlyOnceDelivery test is not supported yet")
	}

	topicName := testTopicName()
	createTopicPubSub := createPubSub(t)
	if subscribeInitializer, ok := createTopicPubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}
	require.NoError(t, createTopicPubSub.Close())

	totalMessagesCount := 50

	closeWg := sync.WaitGroup{}
	closeWg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			defer closeWg.Done()

			pubSub := createPubSub(t)
			_, err := pubSub.Subscribe(context.Background(), topicName)
			require.NoError(t, err)
			closePubSub(t, pubSub)
		}()
	}

	closeWg.Wait()

	pubSub := createPubSub(t)
	expectedMessages := AddSimpleMessages(t, totalMessagesCount, pubSub, topicName)
	closePubSub(t, pubSub)

	pubSub = createPubSub(t)
	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := bulkRead(messages, len(expectedMessages), defaultTimeout*3, features)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, expectedMessages, receivedMessages)
}

func TestContinueAfterErrors(t *testing.T, createPubSub PubSubConstructor, features Features) {
	pubSub := createPubSub(t)
	defer closePubSub(t, pubSub)

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	totalMessagesCount := 50
	subscribersToNack := 3
	nacksPerSubscriber := 100

	if testing.Short() {
		subscribersToNack = 1
		nacksPerSubscriber = 5
	}

	messagesToPublish := AddSimpleMessages(t, totalMessagesCount, pubSub, topicName)

	for i := 0; i < subscribersToNack; i++ {
		var errorsPubSub PubSub
		if !features.Persistent {
			errorsPubSub = pubSub
		} else {
			errorsPubSub = createPubSub(t)
		}

		messages, err := errorsPubSub.Subscribe(context.Background(), topicName)
		require.NoError(t, err)

		for j := 0; j < nacksPerSubscriber; j++ {
			select {
			case msg := <-messages:
				msg.Nack()
			case <-time.After(defaultTimeout):
				t.Fatal("no messages left, probably seek after error doesn't work")
			}
		}

		if features.Persistent {
			closePubSub(t, errorsPubSub)
		}
	}

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	// only nacks was sent, so all messages should be consumed
	receivedMessages, all := bulkRead(messages, totalMessagesCount, defaultTimeout, features)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
}

func TestConsumerGroups(t *testing.T, pubSubConstructor ConsumerGroupPubSubConstructor, features Features) {
	if !features.ConsumerGroups {
		t.Skip("consumer groups are not supported")
	}

	publisherPubSub := pubSubConstructor(t, "test_"+watermill.NewUUID())

	topicName := testTopicName()
	if subscribeInitializer, ok := publisherPubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}
	totalMessagesCount := 50

	group1 := generateConsumerGroup(t, pubSubConstructor, topicName)
	group2 := generateConsumerGroup(t, pubSubConstructor, topicName)

	messagesToPublish := AddSimpleMessages(t, totalMessagesCount, publisherPubSub, topicName)
	closePubSub(t, publisherPubSub)

	assertConsumerGroupReceivedMessages(t, pubSubConstructor, group1, topicName, messagesToPublish)
	assertConsumerGroupReceivedMessages(t, pubSubConstructor, group2, topicName, messagesToPublish)

	defer closePubSub(t, publisherPubSub)
}

// TestPublisherClose sends big amount of messages and them run close to ensure that messages are not lost during adding.
func TestPublisherClose(t *testing.T, pubSub PubSub, features Features) {
	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	messagesCount := 10000
	if testing.Short() {
		messagesCount = 1000
	}

	producedMessages := AddSimpleMessagesParallel(t, messagesCount, pubSub, topicName, 20)

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)
	receivedMessages, _ := bulkRead(messages, messagesCount, defaultTimeout*3, features)

	tests.AssertAllMessagesReceived(t, producedMessages, receivedMessages)
	require.NoError(t, pubSub.Close())
}

func TestTopic(t *testing.T, pubSub PubSub, features Features) {
	defer closePubSub(t, pubSub)

	topic1 := testTopicName()
	topic2 := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topic1))
	}
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topic2))
	}

	topic1Msg := message.NewMessage(watermill.NewUUID(), nil)
	topic2Msg := message.NewMessage(watermill.NewUUID(), nil)

	require.NoError(t, publishWithRetry(pubSub, topic1, topic1Msg))
	require.NoError(t, publishWithRetry(pubSub, topic2, topic2Msg))

	messagesTopic1, err := pubSub.Subscribe(context.Background(), topic1)
	require.NoError(t, err)

	messagesTopic2, err := pubSub.Subscribe(context.Background(), topic2)
	require.NoError(t, err)

	messagesConsumedTopic1, received := bulkRead(messagesTopic1, 1, defaultTimeout, features)
	require.True(t, received, "no messages received in topic %s", topic1)

	messagesConsumedTopic2, received := bulkRead(messagesTopic2, 1, defaultTimeout, features)
	require.True(t, received, "no messages received in topic %s", topic2)

	assert.Equal(t, messagesConsumedTopic1.IDs()[0], topic1Msg.UUID)
	assert.Equal(t, messagesConsumedTopic2.IDs()[0], topic2Msg.UUID)
}

func TestMessageCtx(t *testing.T, pubSub PubSub, features Features) {
	defer closePubSub(t, pubSub)

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	msg := message.NewMessage(watermill.NewUUID(), nil)

	// ensuring that context is not propagated via pub/sub
	ctx, ctxCancel := context.WithCancel(context.Background())
	ctxCancel()
	msg.SetContext(ctx)

	require.NoError(t, publishWithRetry(pubSub, topicName, msg))
	// this might actually be an error in some pubsubs (http), because we close the subscriber without ACK.
	_ = pubSub.Publish(topicName, msg)

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	select {
	case msg := <-messages:
		ctx := msg.Context()

		select {
		case <-ctx.Done():
			t.Fatal("context should not be canceled")
		default:
			// ok
		}

		require.True(t, msg.Ack())

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

func TestSubscribeCtx(t *testing.T, pubSub PubSub, features Features) {
	defer closePubSub(t, pubSub)

	const messagesCount = 20

	ctxWithCancel, cancel := context.WithCancel(context.Background())
	ctxWithCancel = context.WithValue(ctxWithCancel, "foo", "bar")

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}
	publishedMessages := AddSimpleMessages(t, messagesCount, pubSub, topicName)

	msgsToCancel, err := pubSub.Subscribe(ctxWithCancel, topicName)
	require.NoError(t, err)
	cancel()

	timeout := time.After(defaultTimeout)

ClosedLoop:
	for {
		select {
		case msg, open := <-msgsToCancel:
			if !open {
				break ClosedLoop
			}
			msg.Nack()
		case <-timeout:
			t.Fatal("messages channel is not closed after ", defaultTimeout)
			t.FailNow()
		}
		time.Sleep(time.Millisecond * 100)
	}

	ctx := context.WithValue(context.Background(), "foo", "bar")
	msgs, err := pubSub.Subscribe(ctx, topicName)
	require.NoError(t, err)

	receivedMessages, _ := bulkRead(msgs, messagesCount, defaultTimeout, features)
	tests.AssertAllMessagesReceived(t, publishedMessages, receivedMessages)

	for _, msg := range receivedMessages {
		assert.EqualValues(t, "bar", msg.Context().Value("foo"))
	}
}

func TestReconnect(t *testing.T, pubSub PubSub, features Features) {
	if len(features.RestartServiceCommand) == 0 {
		t.Skip("no RestartServiceCommand provided, cannot test reconnect")
	}

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	const messagesCount = 10000
	const publishersCount = 100

	restartAfterMessages := map[int]struct{}{
		messagesCount / 3: {}, // restart at 1/3 of messages
		messagesCount / 2: {}, // restart at 1/2 of messages
	}

	messages, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	var publishedMessages message.Messages
	messagePublished := make(chan *message.Message, messagesCount)
	publishMessage := make(chan struct{})

	go func() {
		for i := 0; i < messagesCount; i++ {
			publishMessage <- struct{}{}

			if _, shouldRestart := restartAfterMessages[i]; shouldRestart {
				go restartServer(t, features)
			}
		}
		close(publishMessage)
	}()

	go func() {
		for msg := range messagePublished {
			publishedMessages = append(publishedMessages, msg)
		}
	}()

	for i := 0; i < publishersCount; i++ {
		go func() {
			for range publishMessage {
				id := watermill.NewUUID()
				msg := message.NewMessage(id, nil)

				for {
					fmt.Println("publishing message")

					// some randomization in sending
					if rand.Int31n(10) == 0 {
						time.Sleep(time.Millisecond * 500)
					}

					if err := publishWithRetry(pubSub, topicName, msg); err == nil {
						break
					}

					fmt.Printf("cannot publish message %s, trying again, err: %s\n", msg.UUID, err)
					time.Sleep(time.Millisecond * 500)
				}

				messagePublished <- msg
			}
		}()
	}

	receivedMessages, allMessages := bulkRead(messages, messagesCount, time.Second*60, features)
	assert.True(t, allMessages, "not all messages received (has %d of %d)", len(receivedMessages), messagesCount)

	tests.AssertAllMessagesReceived(t, publishedMessages, receivedMessages)

	require.NoError(t, pubSub.Close())
}

func restartServer(t *testing.T, features Features) {
	fmt.Println("restarting server with:", features.RestartServiceCommand)
	cmd := exec.Command(features.RestartServiceCommand[0], features.RestartServiceCommand[1:]...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}

	fmt.Println("server restarted")
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

	messages, err := s.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(expectedMessages), defaultTimeout)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, expectedMessages, receivedMessages)
}

func testTopicName() string {
	return "topic_" + watermill.NewUUID()
}

func closePubSub(t *testing.T, pubSub PubSub) {
	err := pubSub.Close()
	require.NoError(t, err)
}

func generateConsumerGroup(t *testing.T, pubSubConstructor ConsumerGroupPubSubConstructor, topicName string) string {
	groupName := "cg_" + watermill.NewUUID()

	// create a pubsub to ensure that the consumer group exists
	// for those providers that require subscription before publishing messages (e.g. Google Cloud PubSub)
	pubSub := pubSubConstructor(t, groupName)
	_, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)
	closePubSub(t, pubSub)

	return groupName
}

func AddSimpleMessages(t *testing.T, messagesCount int, publisher message.Publisher, topicName string) message.Messages {
	var messagesToPublish []*message.Message

	for i := 0; i < messagesCount; i++ {
		id := watermill.NewUUID()

		msg := message.NewMessage(id, nil)
		messagesToPublish = append(messagesToPublish, msg)

		err := publishWithRetry(publisher, topicName, msg)
		require.NoError(t, err, "cannot publish messages")
	}

	return messagesToPublish
}

func AddSimpleMessagesParallel(t *testing.T, messagesCount int, publisher message.Publisher, topicName string, publishers int) message.Messages {
	var messagesToPublish []*message.Message
	publishMsg := make(chan *message.Message)

	wg := sync.WaitGroup{}
	wg.Add(messagesCount)

	for i := 0; i < publishers; i++ {
		go func() {
			for msg := range publishMsg {
				err := publishWithRetry(publisher, topicName, msg)
				require.NoError(t, err, "cannot publish messages")
				wg.Done()
			}
		}()
	}

	for i := 0; i < messagesCount; i++ {
		id := watermill.NewUUID()

		msg := message.NewMessage(id, nil)
		messagesToPublish = append(messagesToPublish, msg)

		publishMsg <- msg
	}
	close(publishMsg)

	wg.Wait()

	return messagesToPublish
}

func assertMessagesChannelClosed(t *testing.T, messages <-chan *message.Message) bool {
	select {
	case _, open := <-messages:
		return assert.False(t, open)
	default:
		t.Error("messages channel is not closed (blocked)")
		return false
	}
}

func publishWithRetry(publisher message.Publisher, topic string, messages ...*message.Message) error {
	retries := 5

	for {
		err := publisher.Publish(topic, messages...)
		if err == nil {
			return nil
		}
		retries--

		fmt.Printf("error on publish: %s, %d retries left\n", err, retries)

		if retries == 0 {
			return err
		}
	}
}

func bulkRead(messagesCh <-chan *message.Message, limit int, timeout time.Duration, features Features) (receivedMessages message.Messages, all bool) {
	if !features.ExactlyOnceDelivery {
		return subscriber.BulkReadWithDeduplication(messagesCh, limit, timeout)
	}

	return subscriber.BulkRead(messagesCh, limit, timeout)
}
