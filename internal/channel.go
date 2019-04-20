package internal

func IsChannelClosed(channel chan struct{}) bool {
	select {
	case _, ok := <-channel:
		if ok {
			panic("received unexpected message to the channel")
		}
		return true
	default:
		return false
	}
}
