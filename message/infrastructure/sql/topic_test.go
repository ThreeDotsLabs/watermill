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

func TestValidateTopicName(t *testing.T) {
	publisher, subscriber := newPubSub(t, newMySQL(t), "")
	cleverlyNamedTopic := "some_topic; DROP DATABASE `watermill`"

	err := publisher.Publish(cleverlyNamedTopic, message.NewMessage("uuid", nil))
	require.Error(t, err)
	assert.Equal(t, sql.ErrInvalidTopicName, errors.Cause(err))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = subscriber.Subscribe(ctx, cleverlyNamedTopic)
	require.Error(t, err)
	assert.Equal(t, sql.ErrInvalidTopicName, errors.Cause(err))
}
