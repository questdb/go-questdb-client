package questdb

import "fmt"

type ConfigStrParseError struct {
	msg string
}

func (e *ConfigStrParseError) Error() string {
	return fmt.Sprintf("Error parsing config string: %q", e.msg)
}

func NewConfigStrParseError(msg string, args ...interface{}) *ConfigStrParseError {
	return &ConfigStrParseError{
		msg: fmt.Sprintf(msg, args),
	}
}
