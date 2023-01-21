package watermill

import (
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// LogFields is the logger's key-value list of fields.
type LogFields map[string]interface{}

// Add adds new fields to the list of LogFields.
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

// Copy copies the LogFields.
func (l LogFields) Copy() LogFields {
	cpy := make(LogFields, len(l))
	for k, v := range l {
		cpy[k] = v
	}

	return cpy
}

// LoggerAdapter is an interface, that you need to implement to support Watermill logging.
// You can use StdLoggerAdapter as a reference implementation.
type LoggerAdapter interface {
	Error(msg string, err error, fields LogFields)
	Info(msg string, fields LogFields)
	Debug(msg string, fields LogFields)
	Trace(msg string, fields LogFields)
	With(fields LogFields) LoggerAdapter
}

// NopLogger is a logger which discards all logs.
type NopLogger struct{}

func (NopLogger) Error(msg string, err error, fields LogFields) {}
func (NopLogger) Info(msg string, fields LogFields)             {}
func (NopLogger) Debug(msg string, fields LogFields)            {}
func (NopLogger) Trace(msg string, fields LogFields)            {}
func (l NopLogger) With(fields LogFields) LoggerAdapter         { return l }

// StdLoggerAdapter is a logger implementation, which sends all logs to provided standard output.
type StdLoggerAdapter struct {
	ErrorLogger *log.Logger
	InfoLogger  *log.Logger
	DebugLogger *log.Logger
	TraceLogger *log.Logger

	fields LogFields
}

// NewStdLogger creates StdLoggerAdapter which sends all logs to stderr.
func NewStdLogger(debug, trace bool) LoggerAdapter {
	return NewStdLoggerWithOut(os.Stderr, debug, trace)
}

// NewStdLoggerWithOut creates StdLoggerAdapter which sends all logs to provided io.Writer.
func NewStdLoggerWithOut(out io.Writer, debug bool, trace bool) LoggerAdapter {
	l := log.New(out, "[watermill] ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
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

func (l *StdLoggerAdapter) With(fields LogFields) LoggerAdapter {
	return &StdLoggerAdapter{
		ErrorLogger: l.ErrorLogger,
		InfoLogger:  l.InfoLogger,
		DebugLogger: l.DebugLogger,
		TraceLogger: l.TraceLogger,
		fields:      l.fields.Add(fields),
	}
}

func (l *StdLoggerAdapter) log(logger *log.Logger, level string, msg string, fields LogFields) {
	if logger == nil {
		return
	}

	fieldsStr := ""

	allFields := l.fields.Add(fields)

	keys := make([]string, len(allFields))
	i := 0
	for field := range allFields {
		keys[i] = field
		i++
	}

	sort.Strings(keys)

	for _, key := range keys {
		var valueStr string
		value := allFields[key]

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

	_ = logger.Output(3, fmt.Sprintf("\t"+`level=%s msg="%s" %s`, level, msg, fieldsStr))
}

type LogLevel uint

const (
	TraceLogLevel LogLevel = iota + 1
	DebugLogLevel
	InfoLogLevel
	ErrorLogLevel
)

type CapturedMessage struct {
	Level  LogLevel
	Fields LogFields
	Msg    string
	Err    error
}

// CaptureLoggerAdapter is a logger which captures all logs.
// This logger is mostly useful for testing logging.
type CaptureLoggerAdapter struct {
	captured map[LogLevel][]CapturedMessage
	fields   LogFields
	lock     sync.Mutex
}

func NewCaptureLogger() *CaptureLoggerAdapter {
	return &CaptureLoggerAdapter{
		captured: map[LogLevel][]CapturedMessage{},
	}
}

func (c *CaptureLoggerAdapter) With(fields LogFields) LoggerAdapter {
	return &CaptureLoggerAdapter{captured: c.captured, fields: c.fields.Add(fields)}
}

func (c *CaptureLoggerAdapter) capture(msg CapturedMessage) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.captured[msg.Level] = append(c.captured[msg.Level], msg)
}

func (c *CaptureLoggerAdapter) Captured() map[LogLevel][]CapturedMessage {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.captured
}

func (c *CaptureLoggerAdapter) Has(msg CapturedMessage) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, capturedMsg := range c.captured[msg.Level] {
		if reflect.DeepEqual(msg, capturedMsg) {
			return true
		}
	}
	return false
}

func (c *CaptureLoggerAdapter) HasError(err error) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, capturedMsg := range c.captured[ErrorLogLevel] {
		if capturedMsg.Err == err {
			return true
		}
	}
	return false
}

func (c *CaptureLoggerAdapter) Error(msg string, err error, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  ErrorLogLevel,
		Fields: c.fields.Add(fields),
		Msg:    msg,
		Err:    err,
	})
}

func (c *CaptureLoggerAdapter) Info(msg string, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  InfoLogLevel,
		Fields: c.fields.Add(fields),
		Msg:    msg,
	})
}

func (c *CaptureLoggerAdapter) Debug(msg string, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  DebugLogLevel,
		Fields: c.fields.Add(fields),
		Msg:    msg,
	})
}

func (c *CaptureLoggerAdapter) Trace(msg string, fields LogFields) {
	c.capture(CapturedMessage{
		Level:  TraceLogLevel,
		Fields: c.fields.Add(fields),
		Msg:    msg,
	})
}
