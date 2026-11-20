package watermill

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"log/slog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlogLoggerAdapter(t *testing.T) {
	b := &bytes.Buffer{}

	logger := NewSlogLogger(slog.New(slog.NewTextHandler(
		b, // output
		&slog.HandlerOptions{
			Level: LevelTrace,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == "time" && len(groups) == 0 {
					// omit time stamp to make the test idempotent
					a.Value = slog.StringValue("[omit]")
				}
				return a
			},
		},
	)))

	logger = logger.With(LogFields{
		"common1": "commonvalue",
	})
	logger.Trace("test trace", LogFields{
		"field1": "value1",
	})
	logger.Error("test error", errors.New("error message"), LogFields{
		"field2": "value2",
	})
	logger.Info("test info", LogFields{
		"field3": "value3",
	})

	assert.Equal(t,
		strings.TrimSpace(`
time=[omit] level=DEBUG-4 msg="test trace" common1=commonvalue field1=value1
time=[omit] level=ERROR msg="test error" common1=commonvalue field2=value2 error="error message"
time=[omit] level=INFO msg="test info" common1=commonvalue field3=value3
      `),
		strings.TrimSpace(b.String()),
		"Logging output does not match saved template.",
	)
}

func TestSlogLoggerAdapter_source(t *testing.T) {
	b := &bytes.Buffer{}

	logger := NewSlogLogger(slog.New(slog.NewJSONHandler(
		b,
		&slog.HandlerOptions{
			AddSource: true,
			Level:     LevelTrace,
		},
	)))

	logger.Trace("test trace", nil)
	logger.Debug("test debug", nil)
	logger.Info("test info", nil)
	logger.With(LogFields{"common1": "commonvalue"}).Error("test error", errors.New("error message"), nil)

	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	require.Len(t, lines, 4)

	for _, line := range lines {
		var record struct {
			Source slog.Source `json:"source"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &record))

		assert.Equal(t, "github.com/ThreeDotsLabs/watermill.TestSlogLoggerAdapter_source", record.Source.Function, line)
		assert.Equal(t, "slog_test.go", filepath.Base(record.Source.File), line)
	}
}

func TestSlogLoggerAdapter_level_mapping(t *testing.T) {
	b := &bytes.Buffer{}

	logger := NewSlogLoggerWithLevelMapping(
		slog.New(slog.NewTextHandler(
			b, // output
			&slog.HandlerOptions{
				Level: LevelTrace,
				ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
					if a.Key == "time" && len(groups) == 0 {
						// omit time stamp to make the test idempotent
						a.Value = slog.StringValue("[omit]")
					}
					return a
				},
			},
		)),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)

	logger = logger.With(LogFields{
		"common1": "commonvalue",
	})
	logger.Trace("test trace", LogFields{
		"field1": "value1",
	})
	logger.Error("test error", errors.New("error message"), LogFields{
		"field2": "value2",
	})
	logger.Info("test info mapped to debug", LogFields{
		"field3": "value3",
	})

	assert.Equal(t,
		strings.TrimSpace(`
time=[omit] level=DEBUG-4 msg="test trace" common1=commonvalue field1=value1
time=[omit] level=ERROR msg="test error" common1=commonvalue field2=value2 error="error message"
time=[omit] level=DEBUG msg="test info mapped to debug" common1=commonvalue field3=value3
      `),
		strings.TrimSpace(b.String()),
		"Logging output does not match saved template.",
	)
}
