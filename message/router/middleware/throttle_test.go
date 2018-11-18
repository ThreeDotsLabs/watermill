package middleware_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	perSecond   = 10
	testTimeout = time.Second
)

func TestThrottle_Middleware(t *testing.T) {
	throttle, err := middleware.NewThrottlePerSecond(perSecond, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)

	producedMessagesCounter := 0
	productionDone := false
	for {
		produced, err := throttle.Middleware(handlerFuncAlwaysOK)(
			message.NewMessage("uuid", nil),
		)
		if err != nil {
			cancel()
			t.Fail()
			return
		}

		assert.NoError(t, err)
		assert.Equal(t, handlerFuncAlwaysOKMessages, produced)
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
