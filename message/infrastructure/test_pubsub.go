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

	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultTimeout = time.Second * 10

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
		TestPublishSubscribe(t, pubSubConstructor(t), features)
	})

	t.Run("resendOnError", func(t *testing.T) {
		t.Parallel()
		TestResendOnError(t, pubSubConstructor(t), features)
	})

	t.Run("noAck", func(t *testing.T) {
		t.Parallel()
		TestNoAck(t, pubSubConstructor(t), features)
	})

	t.Run("continueAfterClose", func(t *testing.T) {
		t.Parallel()
		TestContinueAfterClose(t, pubSubConstructor, features)
	})

	t.Run("concurrentClose", func(t *testing.T) {
		t.Parallel()
		TestConcurrentClose(t, pubSubConstructor, features)
	})

	t.Run("continueAfterErrors", func(t *testing.T) {
		t.Parallel()
		TestContinueAfterErrors(t, pubSubConstructor, features)
	})

	t.Run("publishSubscribeInOrder", func(t *testing.T) {
		t.Parallel()
		TestPublishSubscribeInOrder(t, pubSubConstructor(t), features)
	})

	t.Run("consumerGroups", func(t *testing.T) {
		t.Parallel()
		TestConsumerGroups(t, consumerGroupPubSubConstructor, features)
	})

	t.Run("publisherClose", func(t *testing.T) {
		t.Parallel()

		// todo - cleanup?
		pubsub := pubSubConstructor(t)
		TestPublisherClose(t, pubsub, features)
	})

	t.Run("topic", func(t *testing.T) {
		t.Parallel()
		TestTopic(t, pubSubConstructor(t), features)
	})

	t.Run("messageCtx", func(t *testing.T) {
		t.Parallel()
		TestMessageCtx(t, pubSubConstructor(t), features)
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

func TestPublishSubscribe(t *testing.T, pubSub message.PubSub, features Features) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

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
	err := publishWithRetry(pubSub, topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, all := bulkRead(messages, len(messagesToPublish), defaultTimeout*3, features)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
	tests.AssertMessagesPayloads(t, messagesPayloads, receivedMessages)
	tests.AssertMessagesMetadata(t, "test", messagesTestMetadata, receivedMessages)

	closePubSub(t, pubSub)
	assertMessagesChannelClosed(t, messages)
}

func TestPublishSubscribeInOrder(t *testing.T, pubSub message.PubSub, features Features) {
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
		id := uuid.NewV4().String()
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

	messages, err := pubSub.Subscribe(topicName)
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

func TestResendOnError(t *testing.T, pubSub message.PubSub, features Features) {
	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	//var messagesToPublish message.Messages
	messagesToSend := 100

	var publishedMessages message.Messages
	allMessagesSent := make(chan struct{})

	publishedMessages = addSimpleMessages(t, messagesToSend, pubSub, topicName)
	close(allMessagesSent)

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	var receivedMessages []*message.Message

	i := 0
	errsSent := 0

ReadMessagesLoop:
	for len(receivedMessages) < messagesToSend {
		select {
		case msg := <-messages:
			if msg == nil {
				break
			}

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

func TestNoAck(t *testing.T, pubSub message.PubSub, features Features) {
	if !features.GuaranteedOrder {
		t.Skip("guaranteed order is required for this test")
	}

	defer closePubSub(t, pubSub)
	topicName := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	for i := 0; i < 2; i++ {
		id := uuid.NewV4().String()
		log.Printf("sending %s", id)

		msg := message.NewMessage(id, nil)

		err := publishWithRetry(pubSub, topicName, msg)
		require.NoError(t, err)
	}

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

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

func TestContinueAfterClose(t *testing.T, createPubSub PubSubConstructor, features Features) {
	if features.ExactlyOnceDelivery {
		t.Skip("ExactlyOnceDelivery test is not supported yet")
	}

	totalMessagesCount := 500

	pubSub := createPubSub(t)
	defer closePubSub(t, pubSub)

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	messagesToPublish := addSimpleMessages(t, totalMessagesCount, pubSub, topicName)

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

		receivedMessagesPart, _ := bulkRead(messages, 100, defaultTimeout, features)

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
			_, err := pubSub.Subscribe(topicName)
			require.NoError(t, err)
			closePubSub(t, pubSub)
		}()
	}

	closeWg.Wait()

	pubSub := createPubSub(t)
	expectedMessages := addSimpleMessages(t, totalMessagesCount, pubSub, topicName)
	closePubSub(t, pubSub)

	pubSub = createPubSub(t)
	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, all := bulkRead(messages, len(expectedMessages), defaultTimeout*3, features)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, expectedMessages, receivedMessages)
}

func TestContinueAfterErrors(t *testing.T, createPubSub PubSubConstructor, features Features) {
	if !features.Persistent {
		t.Skip("continueAfterErrors test is not supported for non persistent pub/sub")
	}

	pubSub := createPubSub(t)
	defer closePubSub(t, pubSub)

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	totalMessagesCount := 50

	messagesToPublish := addSimpleMessages(t, totalMessagesCount, pubSub, topicName)

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
	receivedMessages, all := bulkRead(messages, len(messagesToPublish), defaultTimeout, features)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedMessages)
}

func TestConsumerGroups(t *testing.T, pubSubConstructor ConsumerGroupPubSubConstructor, features Features) {
	if !features.ConsumerGroups {
		t.Skip("consumer groups are not supported")
	}

	publisherPubSub := pubSubConstructor(t, "test_"+uuid.NewV4().String())

	topicName := testTopicName()
	if subscribeInitializer, ok := publisherPubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}
	totalMessagesCount := 50

	group1 := generateConsumerGroup(t, pubSubConstructor, topicName)
	group2 := generateConsumerGroup(t, pubSubConstructor, topicName)

	messagesToPublish := addSimpleMessages(t, totalMessagesCount, publisherPubSub, topicName)
	closePubSub(t, publisherPubSub)

	assertConsumerGroupReceivedMessages(t, pubSubConstructor, group1, topicName, messagesToPublish)
	assertConsumerGroupReceivedMessages(t, pubSubConstructor, group2, topicName, messagesToPublish)

	subscriberGroup1 := pubSubConstructor(t, group1)
	defer closePubSub(t, subscriberGroup1)
}

func TestPublisherClose(t *testing.T, pubSub message.PubSub, features Features) {
	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	messagesCount := 10000

	producedMessages := addSimpleMessages(t, messagesCount, pubSub, topicName)

	messages, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	receivedMessages, _ := bulkRead(messages, messagesCount, defaultTimeout*3, features)

	tests.AssertAllMessagesReceived(t, producedMessages, receivedMessages)

	require.NoError(t, pubSub.Close())
}

func TestTopic(t *testing.T, pubSub message.PubSub, features Features) {
	defer closePubSub(t, pubSub)

	topic1 := testTopicName()
	topic2 := testTopicName()

	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topic1))
	}
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topic2))
	}

	topic1Msg := message.NewMessage(uuid.NewV4().String(), nil)
	topic2Msg := message.NewMessage(uuid.NewV4().String(), nil)

	require.NoError(t, publishWithRetry(pubSub, topic1, topic1Msg))
	require.NoError(t, publishWithRetry(pubSub, topic2, topic2Msg))

	messagesTopic1, err := pubSub.Subscribe(topic1)
	require.NoError(t, err)

	messagesTopic2, err := pubSub.Subscribe(topic2)
	require.NoError(t, err)

	messagesConsumedTopic1, received := bulkRead(messagesTopic1, 1, defaultTimeout, features)
	require.True(t, received, "no messages received in topic %s", topic1)

	messagesConsumedTopic2, received := bulkRead(messagesTopic2, 1, defaultTimeout, features)
	require.True(t, received, "no messages received in topic %s", topic2)

	assert.Equal(t, messagesConsumedTopic1.IDs()[0], topic1Msg.UUID)
	assert.Equal(t, messagesConsumedTopic2.IDs()[0], topic2Msg.UUID)
}

func TestMessageCtx(t *testing.T, pubSub message.PubSub, features Features) {
	defer pubSub.Close()

	topicName := testTopicName()
	if subscribeInitializer, ok := pubSub.Subscriber().(message.SubscribeInitializer); ok {
		require.NoError(t, subscribeInitializer.SubscribeInitialize(topicName))
	}

	msg := message.NewMessage(uuid.NewV4().String(), nil)

	// ensuring that context is not propagated via pub/sub
	ctx, ctxCancel := context.WithCancel(context.Background())
	ctxCancel()
	msg.SetContext(ctx)

	require.NoError(t, publishWithRetry(pubSub, topicName, msg))
	require.NoError(t, publishWithRetry(pubSub, topicName, msg))

	messages, err := pubSub.Subscribe(topicName)
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

func TestReconnect(t *testing.T, pubSub message.PubSub, features Features) {
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

	messages, err := pubSub.Subscribe(topicName)
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
				id := uuid.NewV4().String()
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

func addSimpleMessages(t *testing.T, messagesCount int, publisher message.Publisher, topicName string) message.Messages {
	var messagesToPublish []*message.Message

	for i := 0; i < messagesCount; i++ {
		id := uuid.NewV4().String()

		msg := message.NewMessage(id, nil)
		messagesToPublish = append(messagesToPublish, msg)

		err := publishWithRetry(publisher, topicName, msg)
		require.NoError(t, err, "cannot publish messages")
	}

	return messagesToPublish
}

func assertMessagesChannelClosed(t *testing.T, messages chan *message.Message) bool {
	select {
	case msg := <-messages:
		if msg == nil {
			return true
		}

		t.Error("messages channel is not closed (received message)")
		return false
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
