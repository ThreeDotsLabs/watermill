package watermill

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slog"
)

func TestSlogLoggerAdapter(t *testing.T) {
	b := &bytes.Buffer{}

	logger := NewSlogLogger(slog.New(slog.HandlerOptions{
		Level: LevelTrace,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == "time" && len(groups) == 0 {
				// omit time stamp to make the test idempotent
				a.Value = slog.StringValue("[omit]")
			}
			return a
		},
	}.NewTextHandler(b)))

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
		strings.TrimSpace(b.String()),
		strings.TrimSpace(`
time=[omit] level=DEBUG-4 msg="test trace" common1=commonvalue field1=value1
time=[omit] level=ERROR msg="test error" common1=commonvalue field2=value2 error="error message"
time=[omit] level=INFO msg="test info" common1=commonvalue field3=value3
      `),
		"Logging output does not match saved template.",
	)
}
