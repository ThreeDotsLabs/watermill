package tracing

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opencensus.io/trace"
)

func DecoratePublisher() message.PublisherDecorator {
	return message.MessageTransformPublisherDecorator(func(msg *message.Message) {
		// todo?
		//span := trace.FromContext(msg.Context())
		//
		//if span != nil {
		//	fmt.Println("found!!!")
		//	SpanContextToMessage(span.SpanContext(), msg)
		//}
	})
}

func DecorateSubscriber() message.SubscriberDecorator {
	return message.MessageTransformSubscriberDecorator(func(msg *message.Message) {
		//var span *trace.Span
		//ctx := msg.Context()
		//sc, ok := SpanContextFromMessage(msg)
		//
		//// todo?
		////if ok {
		////	ctx, span = trace.StartSpanWithRemoteParent(
		////		ctx,
		////		"todo", // todo
		////		sc,
		////		//trace.WithSampler(startOpts.Sampler), // todo
		////		trace.WithSpanKind(trace.SpanKindServer))
		////} else {
		//ctx, span = trace.StartSpan(ctx, "todo", // todo
		//	//trace.WithSampler(startOpts.Sampler), // todo
		//	trace.WithSpanKind(trace.SpanKindServer),
		//)
		//if ok {
		//	span.AddLink(trace.Link{
		//		TraceID:    sc.TraceID,
		//		SpanID:     sc.SpanID,
		//		Type:       trace.LinkTypeParent,
		//		Attributes: nil,
		//	})
		//}
		//
		//// span.AddAttributes(requestAttrs(r)...) todo
		////}
		//
		//// span.End - todo?
		//
		//msg.SetContext(ctx)
	})
}

func Middleware() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (messages []*message.Message, e error) {
			var span *trace.Span
			ctx := msg.Context()

			fmt.Printf("%#v\n", msg.Metadata)

			parentContext, ok := SpanContextFromMessage(msg)
			if ok {
				ctx, span = trace.StartSpanWithRemoteParent(
					ctx,
					message.HandlerNameFromCtx(msg.Context()), // todo
					parentContext,
					trace.WithSampler(trace.AlwaysSample()), // todo
					trace.WithSpanKind(trace.SpanKindServer),
				)

				span.AddLink(trace.Link{
					TraceID:    parentContext.TraceID,
					SpanID:     parentContext.SpanID,
					Type:       trace.LinkTypeParent,
					Attributes: nil,
				})
			} else {
				ctx, span = trace.StartSpan(
					ctx,
					message.HandlerNameFromCtx(msg.Context()), // todo
					trace.WithSampler(trace.AlwaysSample()),   // todo
					trace.WithSpanKind(trace.SpanKindServer),
				)
			}

			defer span.End()

			defer func() {
				for _, producedMsg := range messages {
					// todo - add support for decorator
					SpanContextToMessage(span.SpanContext(), producedMsg)
				}
			}()

			msg.SetContext(ctx)

			return h(msg)
		}
	}
}
