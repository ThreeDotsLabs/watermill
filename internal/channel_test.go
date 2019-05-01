package internal_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/internal"
	"github.com/stretchr/testify/assert"
)

func TestIsChannelClosed(t *testing.T) {
	closed := make(chan struct{})
	close(closed)

	withSentValue := make(chan struct{}, 1)
	withSentValue <- struct{}{}

	testCases := []struct {
		Name           string
		Channel        chan struct{}
		ExpectedPanic  bool
		ExpectedClosed bool
	}{
		{
			Name:           "not_closed",
			Channel:        make(chan struct{}),
			ExpectedPanic:  false,
			ExpectedClosed: false,
		},
		{
			Name:           "closed",
			Channel:        closed,
			ExpectedPanic:  false,
			ExpectedClosed: true,
		},
		{
			Name:           "with_sent_value",
			Channel:        withSentValue,
			ExpectedPanic:  true,
			ExpectedClosed: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			testFunc := func() {
				closed := internal.IsChannelClosed(c.Channel)
				assert.EqualValues(t, c.ExpectedClosed, closed)
			}

			if c.ExpectedPanic {
				assert.Panics(t, testFunc)
			} else {
				assert.NotPanics(t, testFunc)
			}
		})
	}
}
