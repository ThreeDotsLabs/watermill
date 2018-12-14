package nats_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestGobMarshaler(t *testing.T) {
	msg := message.NewMessage("1", []byte("zag"))
	msg.Metadata.Set("foo", "bar")

	marshaler := nats.GobMarshaler{}

	b, err := marshaler.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := marshaler.Unmarshal(&stan.Msg{MsgProto: pb.MsgProto{Data: b}})
	require.NoError(t, err)

	assert.Equal(t, msg.UUID, unmarshaledMsg.UUID)
	assert.Equal(t, msg.Metadata, unmarshaledMsg.Metadata)
	assert.Equal(t, msg.Payload, unmarshaledMsg.Payload)

	unmarshaledMsg.Ack()

	select {
	case <-unmarshaledMsg.Acked():
		// ok
	default:
		t.Fatal("ack is not working")
	}
}

func TestGobMarshaler_multiple_messages_async(t *testing.T) {
	marshaler := nats.GobMarshaler{}

	messagesCount := 1000
	wg := sync.WaitGroup{}
	wg.Add(messagesCount)

	for i := 0; i < messagesCount; i++ {
		go func(msgNum int) {
			defer wg.Done()

			msg := message.NewMessage(fmt.Sprintf("%d", msgNum), nil)

			b, err := marshaler.Marshal("topic", msg)
			require.NoError(t, err)

			unmarshaledMsg, err := marshaler.Unmarshal(&stan.Msg{MsgProto: pb.MsgProto{Data: b}})
			require.NoError(t, err)

			assert.Equal(t, msg.UUID, unmarshaledMsg.UUID)
		}(i)
	}

	wg.Wait()
}
