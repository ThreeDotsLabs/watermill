package watermill_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill"
)

func TestLogFields_Copy(t *testing.T) {
	fields1 := watermill.LogFields{"foo": "bar"}

	fields2 := fields1.Copy()
	fields2["foo"] = "baz"

	assert.Equal(t, fields1["foo"], "bar")
	assert.Equal(t, fields2["foo"], "baz")
}

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

type stringer struct{}

func (s stringer) String() string {
	return "stringer"
}

func TestStdLoggerAdapter_stringer_field(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	logger := watermill.NewStdLoggerWithOut(buf, true, true)

	logger.Info("foo", watermill.LogFields{"foo": stringer{}})

	out := buf.String()
	assert.Contains(t, out, `foo=stringer`)
}

func TestStdLoggerAdapter_field_with_space(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	logger := watermill.NewStdLoggerWithOut(buf, true, true)

	logger.Info("foo", watermill.LogFields{"foo": `bar baz`})

	out := buf.String()
	assert.Contains(t, out, `foo="bar baz"`)
}

func TestCaptureLoggerAdapter(t *testing.T) {
	var logger watermill.LoggerAdapter = watermill.NewCaptureLogger()

	err := errors.New("error")

	logger = logger.With(watermill.LogFields{"default": "field"})
	logger.Error("error", err, watermill.LogFields{"bar": "2"})
	logger.Info("info", watermill.LogFields{"bar": "2"})
	logger.Debug("debug", watermill.LogFields{"bar": "2"})
	logger.Trace("trace", watermill.LogFields{"bar": "2"})

	expectedLogs := map[watermill.LogLevel][]watermill.CapturedMessage{
		watermill.TraceLogLevel: {
			watermill.CapturedMessage{
				Level:  watermill.TraceLogLevel,
				Fields: watermill.LogFields{"bar": "2", "default": "field"},
				Msg:    "trace",
				Err:    error(nil),
			},
		},
		watermill.DebugLogLevel: {
			watermill.CapturedMessage{
				Level:  watermill.DebugLogLevel,
				Fields: watermill.LogFields{"default": "field", "bar": "2"},
				Msg:    "debug",
				Err:    error(nil),
			},
		},
		watermill.InfoLogLevel: {
			watermill.CapturedMessage{
				Level:  watermill.InfoLogLevel,
				Fields: watermill.LogFields{"default": "field", "bar": "2"},
				Msg:    "info",
				Err:    error(nil),
			},
		},
		watermill.ErrorLogLevel: {
			watermill.CapturedMessage{
				Level:  watermill.ErrorLogLevel,
				Fields: watermill.LogFields{"default": "field", "bar": "2"},
				Msg:    "error",
				Err:    err,
			},
		},
	}

	capturedLogger := logger.(*watermill.CaptureLoggerAdapter)
	assert.EqualValues(t, expectedLogs, capturedLogger.Captured())

	for _, logs := range expectedLogs {
		for _, log := range logs {
			assert.True(t, capturedLogger.Has(log))
		}
	}

	assert.False(t, capturedLogger.Has(watermill.CapturedMessage{
		Level:  0,
		Fields: nil,
		Msg:    "",
		Err:    nil,
	}))

	assert.True(t, capturedLogger.HasError(err))
	assert.False(t, capturedLogger.HasError(errors.New("foo")))
}
