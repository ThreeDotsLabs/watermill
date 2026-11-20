package middleware

import (
	"reflect"
	"time"

	"github.com/ThreeDotsLabs/watermill"
)

// From v1.5.0, the Retry middleware used github.com/cenkalti/backoff/v5, and backoff.Retry gave
// special meaning to two of its error types: a handler could return backoff.Permanent(err) to stop
// the retries, and a *backoff.RetryAfterError to set the next delay.
//
// The middleware no longer imports backoff, but still handles both, recognized by their package
// and type name, so that such handlers keep working. This is deprecated in favour of Permanent and
// RetryAfter, and logged when it happens.

// logDeprecatedBackoffError logs that a handler returned a deprecated backoff error.
// It logs at the Error level, as watermill.LoggerAdapter has no warning level.
func logDeprecatedBackoffError(logger watermill.LoggerAdapter, typeName string, replacement string, err error) {
	if logger == nil {
		return
	}
	logger.Error(
		"Deprecation warning: handler returned backoff."+typeName+", which is deprecated in the Retry middleware, use middleware."+replacement+" instead",
		err,
		nil,
	)
}

// deprecatedBackoffPkgPath is the package of the deprecated error types. It is a variable for tests.
var deprecatedBackoffPkgPath = "github.com/cenkalti/backoff/v5"

// deprecatedBackoffPermanentError returns the error wrapped by a *backoff.PermanentError in err's chain.
func deprecatedBackoffPermanentError(err error) (wrapped error, ok bool) {
	found := findDeprecatedBackoffError(err, "PermanentError")
	if found == nil {
		return nil, false
	}

	unwrapper, ok := found.(interface{ Unwrap() error })
	if !ok {
		return nil, false
	}
	return unwrapper.Unwrap(), true
}

// deprecatedBackoffRetryAfterDelay returns the Duration of a *backoff.RetryAfterError in err's chain.
func deprecatedBackoffRetryAfterDelay(err error) (delay time.Duration, ok bool) {
	found := findDeprecatedBackoffError(err, "RetryAfterError")
	if found == nil {
		return 0, false
	}

	field := reflect.ValueOf(found).Elem().FieldByName("Duration")
	if !field.IsValid() || field.Type() != reflect.TypeFor[time.Duration]() {
		return 0, false
	}
	return time.Duration(field.Int()), true
}

// findDeprecatedBackoffError returns the first error in err's chain, in the order errors.As uses,
// that is a pointer to typeName from the backoff package.
func findDeprecatedBackoffError(err error, typeName string) error {
	for err != nil {
		if isDeprecatedBackoffError(err, typeName) {
			return err
		}

		switch e := err.(type) {
		case interface{ Unwrap() error }:
			err = e.Unwrap()
		case interface{ Unwrap() []error }:
			for _, joined := range e.Unwrap() {
				if found := findDeprecatedBackoffError(joined, typeName); found != nil {
					return found
				}
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}

// isDeprecatedBackoffError reports whether err is a pointer to typeName from the deprecated backoff package.
func isDeprecatedBackoffError(err error, typeName string) bool {
	t := reflect.TypeOf(err)
	return t.Kind() == reflect.Pointer &&
		t.Elem().PkgPath() == deprecatedBackoffPkgPath &&
		t.Elem().Name() == typeName
}
