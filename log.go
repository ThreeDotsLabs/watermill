package watermill

import (
	"fmt"
	"log"
	"os"
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
	a := &StdLoggerAdapter{InfoLogger: l}

	if debug {
		a.DebugLogger = l
	}
	if trace {
		a.TraceLogger = l
	}

	return a
}

func (l *StdLoggerAdapter) Error(msg string, err error, fields LogFields) {
	l.log(l.TraceLogger, "ERROR", msg, fields.Add(LogFields{"err": err}))
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
	for field, value := range fields {
		var valueStr string
		if stringer, ok := value.(fmt.Stringer); ok {
			valueStr = stringer.String()
		} else {
			valueStr = fmt.Sprintf("%v", value)
		}

		if strings.Contains(valueStr, " ") {
			valueStr = `"` + valueStr + `"`
		}

		fieldsStr += field + "=" + valueStr + " "
	}

	logger.Output(3, fmt.Sprintf("\t"+`level=%s msg="%s" %s`, level, msg, fieldsStr))
}
