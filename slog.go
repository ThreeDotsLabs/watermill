package watermill

import (
	"log/slog"
)

// LevelTrace must be added, because [slog] package does not have one by default. Generate it by subtracting 4 levels from [slog.Debug] following the example of [slog.LevelWarn] and [slog.LevelError] which are set to 4 and 8.
const LevelTrace slog.Level = slog.LevelDebug - 4

func slogAttrsFromFields(fields LogFields) (result []any) {
	for key, value := range fields {
		// result = append(result, slog.Any(key, value))
		result = append(result, key, value)
	}
	return
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
		nil, // void context, following slog example
		LevelTrace,
		msg,
		slogAttrsFromFields(fields)...,
	)
}

// With return a [SlogLoggerAdapter] with a set of fields injected into all consequent logging messages.
func (s *SlogLoggerAdapter) With(fields LogFields) LoggerAdapter {
	return &SlogLoggerAdapter{slog: s.slog.With(slogAttrsFromFields(fields)...)}
}

// NewSlogLogger creates an adapter to the standard library's experimental structured logging package.
func NewSlogLogger(logger *slog.Logger) LoggerAdapter {
	if logger == nil {
		panic("cannot use a <nil> logger")
	}
	return &SlogLoggerAdapter{
		slog: logger,
	}
}
