package sql_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeTopicName(t *testing.T) {
	pubsub := newPubSub(t, newMySQL(t), "").(message.PubSub)
	cleverlyNamedTopic := "some_topic; DROP DATABASE `watermill`"

	err := pubsub.Publish(cleverlyNamedTopic, message.NewMessage("uuid", nil))
	require.Error(t, err)
	assert.Equal(t, sql.ErrInvalidTopicName, errors.Cause(err))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = pubsub.Subscribe(ctx, cleverlyNamedTopic)
	require.Error(t, err)
	assert.Equal(t, sql.ErrInvalidTopicName, errors.Cause(err))
}
