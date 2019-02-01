package amqp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/amqp"
	"github.com/satori/go.uuid"
	stdAmqp "github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
)

func TestDefaultMarshaler(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{}

	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	unmarshaledMsg, err := marshaler.Unmarshal(publishingToDelivery(marshaled))
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
	assert.Equal(t, marshaled.DeliveryMode, stdAmqp.Persistent)
}

func TestDefaultMarshaler_not_persistent(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{NotPersistentDeliveryMode: true}

	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	assert.EqualValues(t, marshaled.DeliveryMode, 0)
}

func TestDefaultMarshaler_postprocess_publishing(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{
		PostprocessPublishing: func(publishing stdAmqp.Publishing) stdAmqp.Publishing {
			publishing.CorrelationId = "correlation"
			publishing.ContentType = "application/json"

			return publishing
		},
	}

	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	assert.Equal(t, marshaled.CorrelationId, "correlation")
	assert.Equal(t, marshaled.ContentType, "application/json")
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := amqp.DefaultMarshaler{}

	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	for i := 0; i < b.N; i++ {
		m.Marshal(msg)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := amqp.DefaultMarshaler{}

	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal(msg)
	if err != nil {
		b.Fatal(err)
	}

	consumedMsg := publishingToDelivery(marshaled)

	for i := 0; i < b.N; i++ {
		m.Unmarshal(consumedMsg)
	}
}

func publishingToDelivery(marshaled stdAmqp.Publishing) stdAmqp.Delivery {
	return stdAmqp.Delivery{
		Body:    marshaled.Body,
		Headers: marshaled.Headers,
	}
}
