package watermill_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill"
)

func TestStdLogger_with(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})

	cleanLogger := watermill.NewStdLoggerWithOut(buf, true, true)

	withLogFieldsLogger := cleanLogger.With(watermill.LogFields{"foo": "1"})

	for name, logger := range map[string]watermill.LoggerAdapter{"clean": cleanLogger, "with": withLogFieldsLogger} {
		logger.Error(name, nil, watermill.LogFields{"bar": "2"})
		logger.Info(name, watermill.LogFields{"bar": "2"})
		logger.Debug(name, watermill.LogFields{"bar": "2"})
		logger.Trace(name, watermill.LogFields{"bar": "2"})
	}

	cleanLoggerOut := buf.String()
	assert.Contains(t, cleanLoggerOut, `level=ERROR msg="clean" bar=2 err=<nil>`)
	assert.Contains(t, cleanLoggerOut, `level=INFO  msg="clean" bar=2`)
	assert.Contains(t, cleanLoggerOut, `level=TRACE msg="clean" bar=2`)

	assert.Contains(t, cleanLoggerOut, `level=ERROR msg="with" bar=2 err=<nil> foo=1`)
	assert.Contains(t, cleanLoggerOut, `level=INFO  msg="with" bar=2 foo=1`)
	assert.Contains(t, cleanLoggerOut, `level=TRACE msg="with" bar=2 foo=1`)
}
