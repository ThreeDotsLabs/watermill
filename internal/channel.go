package internal

// IsChannelClosed returns true if provided `chan struct{}` is closed.
// IsChannelClosed panics if message is sent to this channel.
func IsChannelClosed(channel chan struct{}) bool {
	select {
	case _, ok := <-channel:
		if ok {
			panic("received unexpected message")
		}
		return true
	default:
		return false
	}
}
