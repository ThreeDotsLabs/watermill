package metrics

import "context"

type contextValue int

const (
	publishObserved contextValue = iota
	subscribeObserved
)

// setPublishObservedToCtx is used to achieve metrics idempotency in case of double applied middleware
func setPublishObservedToCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, publishObserved, true)
}

func publishAlreadyObserved(ctx context.Context) bool {
	return ctx.Value(publishObserved) != nil
}

// setSubscribeObservedToCtx is used to achieve metrics idempotency in case of double applied middleware
func setSubscribeObservedToCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, subscribeObserved, true)
}

func subscribeAlreadyObserved(ctx context.Context) bool {
	return ctx.Value(subscribeObserved) != nil
}
