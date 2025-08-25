package sync

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroupTimeout_no_timeout(t *testing.T) {
	wg := &sync.WaitGroup{}

	timedout := WaitGroupTimeout(wg, time.Millisecond*100)
	assert.False(t, timedout)
}

func TestWaitGroupTimeout_timeout(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	timedout := WaitGroupTimeout(wg, time.Millisecond*100)
	assert.True(t, timedout)
}
