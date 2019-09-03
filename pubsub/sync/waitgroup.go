package sync

import (
	"sync"
	"time"
)

// WaitGroupTimeout adds timeout feature for sync.WaitGroup.Wait().
// It returns true, when timeouted.
func WaitGroupTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	wgClosed := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		wgClosed <- struct{}{}
	}()

	select {
	case <-wgClosed:
		return false
	case <-time.After(timeout):
		return true
	}
}
