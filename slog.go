package watermill

import (
	"context"
	"log/slog"
)

// LevelTrace must be added, because [slog] package does not have one by default. Generate it by subtracting 4 levels from [slog.Debug] following the example of [slog.LevelWarn] and [slog.LevelError] which are set to 4 and 8.
const LevelTrace = slog.LevelDebug - 4

func slogAttrsFromFields(fields LogFields) []any {
	result := make([]any, 0, len(fields)*2)

	for key, value := range fields {
		result = append(result, key, value)
	}

	return result
}

// SlogLoggerAdapter wraps [slog.Logger].
type SlogLoggerAdapter struct {
	slog *slog.Logger
}

// Error logs a message to [slog.LevelError].
func (s *SlogLoggerAdapter) Error(msg string, err error, fields LogFields) {
	s.slog.Error(msg, append(slogAttrsFromFields(fields), "error", err)...)
}

// Info logs a message to [slog.LevelInfo].
func (s *SlogLoggerAdapter) Info(msg string, fields LogFields) {
	s.slog.Info(msg, slogAttrsFromFields(fields)...)
}

// Debug logs a message to [slog.LevelDebug].
func (s *SlogLoggerAdapter) Debug(msg string, fields LogFields) {
	s.slog.Debug(msg, slogAttrsFromFields(fields)...)
}

// Trace logs a message to [LevelTrace].
func (s *SlogLoggerAdapter) Trace(msg string, fields LogFields) {
	s.slog.Log(
		// Void context, following the slog example
		// as it treats context slighly differently from
		// normal usage, minding contextual
		// values, but ignoring contextual deadline.
		// See the [slog] package documentation
		// for more details.
		context.Background(),
		LevelTrace,
		msg,
		slogAttrsFromFields(fields)...,
	)
}

// With return a [SlogLoggerAdapter] with a set of fields injected into all consequent logging messages.
func (s *SlogLoggerAdapter) With(fields LogFields) LoggerAdapter {
	return &SlogLoggerAdapter{slog: s.slog.With(slogAttrsFromFields(fields)...)}
}

// NewSlogLogger creates an adapter to the standard library's structured logging package. A `nil` logger is substituted for the result of [slog.Default].
func NewSlogLogger(logger *slog.Logger) LoggerAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogLoggerAdapter{
		slog: logger,
	}
}
