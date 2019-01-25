package watermill

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
)

type LogFields map[string]interface{}

func (l LogFields) Add(newFields LogFields) LogFields {
	resultFields := make(LogFields, len(l)+len(newFields))

	for field, value := range l {
		resultFields[field] = value
	}
	for field, value := range newFields {
		resultFields[field] = value
	}

	return resultFields
}

type LoggerAdapter interface {
	Error(msg string, err error, fields LogFields)
	Info(msg string, fields LogFields)
	Debug(msg string, fields LogFields)
	Trace(msg string, fields LogFields)
}

type NopLogger struct{}

func (NopLogger) Error(msg string, err error, fields LogFields) {}
func (NopLogger) Info(msg string, fields LogFields)             {}
func (NopLogger) Debug(msg string, fields LogFields)            {}
func (NopLogger) Trace(msg string, fields LogFields)            {}

type StdLoggerAdapter struct {
	ErrorLogger *log.Logger
	InfoLogger  *log.Logger
	DebugLogger *log.Logger
	TraceLogger *log.Logger
}

func NewStdLogger(debug, trace bool) LoggerAdapter {
	l := log.New(os.Stderr, "[watermill] ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	a := &StdLoggerAdapter{InfoLogger: l, ErrorLogger: l}

	if debug {
		a.DebugLogger = l
	}
	if trace {
		a.TraceLogger = l
	}

	return a
}

func (l *StdLoggerAdapter) Error(msg string, err error, fields LogFields) {
	l.log(l.ErrorLogger, "ERROR", msg, fields.Add(LogFields{"err": err}))
}

func (l *StdLoggerAdapter) Info(msg string, fields LogFields) {
	l.log(l.InfoLogger, "INFO ", msg, fields)
}

func (l *StdLoggerAdapter) Debug(msg string, fields LogFields) {
	l.log(l.DebugLogger, "DEBUG", msg, fields)
}

func (l *StdLoggerAdapter) Trace(msg string, fields LogFields) {
	l.log(l.TraceLogger, "TRACE", msg, fields)
}

func (l *StdLoggerAdapter) log(logger *log.Logger, level string, msg string, fields LogFields) {
	if logger == nil {
		return
	}

	fieldsStr := ""

	keys := make([]string, len(fields))
	i := 0
	for field := range fields {
		keys[i] = field
		i++
	}

	sort.Strings(keys)

	for _, key := range keys {
		var valueStr string
		value := fields[key]

		if stringer, ok := value.(fmt.Stringer); ok {
			valueStr = stringer.String()
		} else {
			valueStr = fmt.Sprintf("%v", value)
		}

		if strings.Contains(valueStr, " ") {
			valueStr = `"` + valueStr + `"`
		}

		fieldsStr += key + "=" + valueStr + " "
	}

	logger.Output(3, fmt.Sprintf("\t"+`level=%s msg="%s" %s`, level, msg, fieldsStr))
}

type LogLevel uint

const (
	Trace LogLevel = iota + 1
	Debug
	Info
	Error
)

type CapturedMessage struct {
	Level  LogLevel
	Fields LogFields
	Msg    string
	Err    error
}

type CaptureLoggerAdapter struct {
	captured map[LogLevel][]CapturedMessage
}

func NewCaptureLogger() CaptureLoggerAdapter {
	return CaptureLoggerAdapter{
		captured: map[LogLevel][]CapturedMessage{},
	}
}

func (c *CaptureLoggerAdapter) capture(msg CapturedMessage) {
	c.captured[msg.Level] = append(c.captured[msg.Level], msg)
}

func (c CaptureLoggerAdapter) Has(msg CapturedMessage) bool {
	for _, capturedMsg := range c.captured[msg.Level] {
		if reflect.DeepEqual(msg, capturedMsg) {
			return true
		}
	}
	return false
}

func (c CaptureLoggerAdapter) HasError(err error) bool {
	for _, capturedMsg := range c.captured[Error] {
		if capturedMsg.Err == err {
			return true
		}
	}
	return false
}

func (c *CaptureLoggerAdapter) Error(msg string, err error, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  Error,
		Fields: fields,
		Msg:    msg,
		Err:    err,
	})
}

func (c *CaptureLoggerAdapter) Info(msg string, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  Info,
		Fields: fields,
		Msg:    msg,
	})
}

func (c *CaptureLoggerAdapter) Debug(msg string, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  Debug,
		Fields: fields,
		Msg:    msg,
	})
}

func (c *CaptureLoggerAdapter) Trace(msg string, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  Trace,
		Fields: fields,
		Msg:    msg,
	})
}
