package message_test

import (
	"testing"
	"github.com/roblaszczak/gooddd/message"
	"time"
	"github.com/stretchr/testify/assert"
)

func TestMessage_Acknowledged_after_ack(t *testing.T) {
	ack := message.NewAck()
	ack.Acknowledge()

	select {
	case <-ack.Acknowledged():
		// ok
	case <-time.After(time.Millisecond * 50):
		t.Fatal("no ack received, timeouted")
	}
}

func TestMessage_Acknowledged_before_ack(t *testing.T) {
	ack := message.NewAck()

	acked := false
	go func() {
		select {
		case <-ack.Acknowledged():
			acked = true
		case <-time.After(time.Millisecond * 50):
			t.Fatal("no ack received, timeouted")
		}
	}()

	ack.Acknowledge()

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

func createBenchmarkMessages(b *testing.B) []*message.Ack {
	acks := make([]*message.Ack, b.N)
	for n := 0; n < b.N; n++ {
		acks[n] = message.NewAck()
	}
	b.ResetTimer()

	return acks
}
