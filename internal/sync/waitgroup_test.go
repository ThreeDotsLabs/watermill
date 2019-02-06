package sync

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroupTimeout_no_timeout(t *testing.T) {
	wg := &sync.WaitGroup{}

	timeouted := WaitGroupTimeout(wg, time.Millisecond*100)
	assert.False(t, timeouted)
}

func TestWaitGroupTimeout_timeout(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	timeouted := WaitGroupTimeout(wg, time.Millisecond*100)
	assert.True(t, timeouted)
}
