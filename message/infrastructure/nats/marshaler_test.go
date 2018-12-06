package nats_test

import (
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
