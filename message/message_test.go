package message_test

import (
	"testing"
	"github.com/roblaszczak/gooddd/message"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessage_Acknowledged_after_ack(t *testing.T) {
	msg, err := message.DefaultFactoryFunc(nil)
	require.NoError(t, err)
	msg.Acknowledge()

	select {
	case <-msg.Acknowledged():
		// ok
	case <-time.After(time.Millisecond * 50):
		t.Fatal("no ack received, timeouted")
	}
}

func TestMessage_Acknowledged_before_ack(t *testing.T) {
	msg, err := message.DefaultFactoryFunc(nil)
	require.NoError(t, err)

	acked := false
	go func() {
		select {
		case <-msg.Acknowledged():
			acked = true
		case <-time.After(time.Millisecond * 50):
			t.Fatal("no ack received, timeouted")
		}
	}()

	msg.Acknowledge()

	time.Sleep(time.Millisecond * 50)

	assert.True(t, acked)
}

func BenchmarkMessage_Acknowledge_after_ack(b *testing.B) {
	messages := createBenchmarkMessages(b)

	for n := 0; n < b.N; n++ {
		msg := messages[n]

		msg.Acknowledge()
		<-msg.Acknowledged()
	}
}

func BenchmarkMessage_Acknowledge_before_ack(b *testing.B) {
	messages := createBenchmarkMessages(b)

	for n := 0; n < b.N; n++ {
		msg := messages[n]

		ackSent := make(chan struct{})
		go func() {
			<-msg.Acknowledged()
			ackSent <- struct{}{}
		}()

		msg.Acknowledge()
		<-ackSent
	}
}

func createBenchmarkMessages(b *testing.B) []*message.Message {
	messages := make([]*message.Message, b.N)
	for n := 0; n < b.N; n++ {
		messages[n], _ = message.DefaultFactoryFunc(nil)
	}
	b.ResetTimer()

	return messages
}
