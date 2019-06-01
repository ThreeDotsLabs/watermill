package nats_test

import (
	"fmt"
	"sync"
	"testing"

	stan "github.com/nats-io/stan.go"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"
	"github.com/nats-io/stan.go/pb"
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

	assert.True(t, msg.Equals(unmarshaledMsg))

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

			assert.True(t, msg.Equals(unmarshaledMsg))
		}(i)
	}

	wg.Wait()
}
