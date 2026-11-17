package middleware

// SetDeprecatedBackoffPkgPath makes the Retry middleware treat the types of the package at path
// as the types of github.com/cenkalti/backoff/v5. It returns a function that restores the path.
func SetDeprecatedBackoffPkgPath(path string) (restore func()) {
	previous := deprecatedBackoffPkgPath
	deprecatedBackoffPkgPath = path
	return func() { deprecatedBackoffPkgPath = previous }
}
