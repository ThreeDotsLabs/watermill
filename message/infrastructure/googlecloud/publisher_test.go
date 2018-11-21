package googlecloud_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

func TestNewPublisher(t *testing.T) {
	pub, err := googlecloud.NewPublisher(
		context.Background(),
		googlecloud.PublisherConfig{
			CreateMissingTopic: true,
		},
	)
	require.NoError(t, err)

	topicName := fmt.Sprintf("test-topic-%d", rand.Int())

	err = pub.Publish(topicName, testMessage(), testMessage(), testMessage())
	require.NoError(t, err)
}

func TestPublisher_Close(t *testing.T) {
	pub, err := googlecloud.NewPublisher(
		context.Background(),
		googlecloud.PublisherConfig{},
	)
	require.NoError(t, err)

	err = pub.Close()
	require.NoError(t, err)

	err = pub.Publish("some-topic", testMessage())
	require.Error(t, err)
	assert.Equal(t, googlecloud.ErrPublisherClosed, errors.Cause(err))
}
