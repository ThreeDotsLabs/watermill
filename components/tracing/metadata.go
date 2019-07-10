package tracing

import (
	"encoding/hex"

	"github.com/ThreeDotsLabs/watermill/message"

	"go.opencensus.io/trace"
)

// B3 headers that OpenCensus understands.
const (
	TraceIDHeader = "X-B3-TraceId"
	SpanIDHeader  = "X-B3-SpanId"
	SampledHeader = "X-B3-Sampled"
)

func SpanContextFromMessage(message *message.Message) (sc trace.SpanContext, ok bool) {
	tid, ok := ParseTraceID(message.Metadata.Get(TraceIDHeader))
	if !ok {
		return trace.SpanContext{}, false
	}
	sid, ok := ParseSpanID(message.Metadata.Get(SpanIDHeader))
	if !ok {
		return trace.SpanContext{}, false
	}
	sampled, _ := ParseSampled(message.Metadata.Get(SampledHeader))
	return trace.SpanContext{
		TraceID:      tid,
		SpanID:       sid,
		TraceOptions: sampled,
	}, true
}

// SpanContextToRequest modifies the given request to include B3 headers.
func SpanContextToMessage(sc trace.SpanContext, msg *message.Message) {
	msg.Metadata.Set(TraceIDHeader, hex.EncodeToString(sc.TraceID[:]))
	msg.Metadata.Set(SpanIDHeader, hex.EncodeToString(sc.SpanID[:]))

	var sampled string
	if sc.IsSampled() {
		sampled = "1"
	} else {
		sampled = "0"
	}
	msg.Metadata.Set(SampledHeader, sampled)
}

// ParseTraceID parses the value of the X-B3-TraceId header.
func ParseTraceID(tid string) (trace.TraceID, bool) {
	if tid == "" {
		return trace.TraceID{}, false
	}
	b, err := hex.DecodeString(tid)
	if err != nil {
		return trace.TraceID{}, false
	}
	var traceID trace.TraceID
	if len(b) <= 8 {
		// The lower 64-bits.
		start := 8 + (8 - len(b))
		copy(traceID[start:], b)
	} else {
		start := 16 - len(b)
		copy(traceID[start:], b)
	}

	return traceID, true
}

// ParseSpanID parses the value of the X-B3-SpanId or X-B3-ParentSpanId headers.
func ParseSpanID(sid string) (spanID trace.SpanID, ok bool) {
	if sid == "" {
		return trace.SpanID{}, false
	}
	b, err := hex.DecodeString(sid)
	if err != nil {
		return trace.SpanID{}, false
	}
	start := 8 - len(b)
	copy(spanID[start:], b)
	return spanID, true
}

// ParseSampled parses the value of the X-B3-Sampled header.
func ParseSampled(sampled string) (trace.TraceOptions, bool) {
	switch sampled {
	case "true", "1":
		return trace.TraceOptions(1), true
	default:
		return trace.TraceOptions(0), false
	}
}
