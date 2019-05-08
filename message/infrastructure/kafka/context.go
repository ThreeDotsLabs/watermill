package kafka

import (
	"context"
)

type contextKey int

const (
	partitionContextKey contextKey = iota
	partitionOffsetContextKey
)

func setPartitionToCtx(ctx context.Context, partition int32) context.Context {
	return context.WithValue(ctx, partitionContextKey, partition)
}

func MessagePartitionFromCtx(ctx context.Context) int32 {
	partition := ctx.Value(partitionContextKey)

	if intPartition, ok := partition.(int32); ok {
		return intPartition
	} else {
		return 0
	}
}

func setPartitionOffsetToCtx(ctx context.Context, offset int64) context.Context {
	return context.WithValue(ctx, partitionOffsetContextKey, offset)
}

func MessagePartitionOffsetFromCtx(ctx context.Context) int64 {
	partitionOffset := ctx.Value(partitionOffsetContextKey)

	if intPartitionOffset, ok := partitionOffset.(int64); ok {
		return intPartitionOffset
	} else {
		return 0
	}
}
