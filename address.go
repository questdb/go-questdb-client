package questdb

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type address string

func (a address) checkUrl() error {
	u, err := url.Parse(a)
	if err != nil {
		return err
	}

	if u.Host == "" {
		return errors.New("empty host")
	}

	return nil
}

type schemaType string

const (
	schemaHttp  schemaType = "http"
	schemaHttps schemaType = "https"
	schemaTcp   schemaType = "tcp"
	schemaTcps  schemaType = "tcps"
)

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

func (a address) checkConfigStr() error {
	splitStr := strings.SplitAfterN(string(a), "::", 1)
	if len(splitStr) < 2 {
		return NewConfigStrParseError("no schema separator found '::'")
	}

	schema := schemaType(splitStr[0])
	switch schema {
	case schemaHttp, schemaHttps, schemaTcp, schemaTcps:
		break
	default:
		return NewConfigStrParseError("invalid schema %q", schema)
	}

}
