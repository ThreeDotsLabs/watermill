package middleware_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/stretchr/testify/assert"
)

const (
	perSecond   = 10
	testTimeout = time.Second
)

func TestThrottle_Middleware(t *testing.T) {
	throttle := middleware.Throttle{perSecond}

	ctx, _ := context.WithTimeout(context.Background(), testTimeout)

	producedMessagesCounter := 0
	productionDone := false
	for {
		producedMessages := []*message.Message{message.NewMessage("produced", nil)}
		producedErr := errors.New("produced err")

		produced, err := throttle.Middleware(func(msg *message.Message) ([]*message.Message, error) {
			return producedMessages, producedErr
		})(
			message.NewMessage("uuid", nil),
		)

		assert.Equal(t, producedMessages, produced)
		assert.Equal(t, producedErr, err)

		producedMessagesCounter++

		select {
		case <-ctx.Done():
			productionDone = true
		default:
		}

		if productionDone {
			break
		}
	}

	t.Logf("produced %d messages in %d seconds, at rate of total %d messages per second",
		producedMessagesCounter,
		int(testTimeout.Seconds()),
		perSecond,
	)

	assert.True(t, producedMessagesCounter <= int(perSecond*testTimeout.Seconds()))
}
