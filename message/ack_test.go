package message_test

import (
	"testing"
	"github.com/roblaszczak/gooddd/message"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/pkg/errors"
)

func TestAck_Acknowledged_after_ack(t *testing.T) {
	testCases := []struct {
		Name           string
		SendAck        func(ack *message.Ack)
		ExpectedAckErr error
	}{
		{
			Name: "ack",
			SendAck: func(ack *message.Ack) {
				err := ack.Acknowledge()
				assert.NoError(t, err)
			},
		},
		{
			Name: "err",
			SendAck: func(ack *message.Ack) {
				err := ack.Error(errors.New("error"))
				assert.NoError(t, err)
			},
			ExpectedAckErr: errors.New("error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			ack := message.NewAck()
			tc.SendAck(ack)

			select {
			case ackErr := <-ack.Acknowledged():
				if tc.ExpectedAckErr == nil {
					assert.Nil(t, ackErr)
				} else {
					assert.Equal(t, tc.ExpectedAckErr.Error(), ackErr.Error())
				}
			case <-time.After(time.Millisecond * 50):
				t.Fatal("no ack received, timeouted")
			}
		})
	}
}

func TestAck_Acknowledged_before_ack(t *testing.T) {
	testCases := []struct {
		Name           string
		SendAck        func(ack *message.Ack)
		ExpectedAckErr error
	}{
		{
			Name: "ack",
			SendAck: func(ack *message.Ack) {
				err := ack.Acknowledge()
				assert.NoError(t, err)
			},
		},
		{
			Name: "err",
			SendAck: func(ack *message.Ack) {
				err := ack.Error(errors.New("error"))
				assert.NoError(t, err)
			},
			ExpectedAckErr: errors.New("error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
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

			tc.SendAck(ack)

			time.Sleep(time.Millisecond * 50)

			assert.True(t, acked)
		})
	}
}

func TestAck_Acknowledge_two_times(t *testing.T) {
	ack := message.NewAck()
	err := ack.Acknowledge()
	assert.NoError(t, err)

	err = ack.Error(errors.New("error"))
	assert.Error(t, err)

	err = ack.Acknowledge()
	assert.Error(t, err)
}

func TestAck_Error_two_times(t *testing.T) {
	ack := message.NewAck()
	err := ack.Error(errors.New("error"))
	assert.NoError(t, err)

	err = ack.Error(errors.New("error 2"))
	assert.Error(t, err)

	err = ack.Acknowledge()
	assert.Error(t, err)
}

func BenchmarkAck_Acknowledge_after_ack(b *testing.B) {
	messages := createBenchmarkMessages(b)

	for n := 0; n < b.N; n++ {
		msg := messages[n]

		msg.Acknowledge()
		<-msg.Acknowledged()
	}
}

func BenchmarkAck_Acknowledge_before_ack(b *testing.B) {
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
