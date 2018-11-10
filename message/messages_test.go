package message_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
)

func TestMessages_IDs(t *testing.T) {
	msgs := message.Messages{
		message.NewMessage("1", nil),
		message.NewMessage("2", nil),
		message.NewMessage("3", nil),
	}

	assert.Equal(t, []string{"1", "2", "3"}, msgs.IDs())
}
