package marshal_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka/marshal"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJson(t *testing.T) {
	m := marshal.KafkaJson{}

	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(marshaled)
	require.NoError(t, err)

	assert.EqualValues(t, msg.UUID, unmarshaledMsg.UUID)
	assert.EqualValues(t, msg.Metadata, unmarshaledMsg.Metadata)
	assert.EqualValues(t, msg.Payload, unmarshaledMsg.Payload)
}

func TestJsonWithPartitioning(t *testing.T) {
	m := marshal.NewKafkaJsonWithPartitioning(func(topic string, msg *message.Message) (string, error) {
		return msg.Metadata.Get("partition"), nil
	})

	partitionKey := "1"
	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("partition", partitionKey)

	producerMsg, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(producerMsg)
	require.NoError(t, err)

	assert.EqualValues(t, msg.UUID, unmarshaledMsg.UUID)
	assert.EqualValues(t, msg.Metadata, unmarshaledMsg.Metadata)

	assert.Equal(t, msg.Payload, unmarshaledMsg.Payload)

	assert.NoError(t, err)
	assert.Equal(t, string(producerMsg.Key), partitionKey)
}
