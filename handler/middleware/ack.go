package middleware

// todo - test
//func AckMiddleware(h MessageHandler) MessageHandler {
//	return func(msg pubsub.Message) error {
//		err := h(msg)
//		if err == nil {
//			if ackErr := msg.Acknowledge(); ackErr != nil {
//				// todo - handle
//				panic(err)
//			}
//		}
//
//		return err
//	}
//}
