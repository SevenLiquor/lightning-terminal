package firewall

// Config holds all config options for the firewall.
//
//nolint:lll
type Config struct {
	RequestLogger *RequestLoggerConfig `group:"request-logger" namespace:"request-logger" description:"request logger settings"`
}

// RequestLoggerConfig holds all the config options for the request logger.
//
//nolint:lll
type RequestLoggerConfig struct {
	RequestLoggerLevel RequestLoggerLevel `long:"level" description:"Set the request logger level. Options include 'all', 'full' and 'interceptor''"`
}

// DefaultConfig constructs the default firewall Config struct.
func DefaultConfig() *Config {
	return &Config{
		RequestLogger: &RequestLoggerConfig{
			RequestLoggerLevel: RequestLoggerLevelInterceptor,
		},
	}
}
