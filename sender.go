package questdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
)

func NewLineSender(ctx context.Context) (*LineSender, error) {
	return NewLineSenderWithConfig(ctx, newDefaultConfig())
}

func NewLineSenderWithConfig(ctx context.Context, config Config) (*LineSender, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", config.Address)
	if err != nil {
		return nil, err
	}
	s := &LineSender{
		conn:          conn,
		initialBufCap: config.BufferCap,
	}
	return s, nil
}

type LineSender struct {
	conn          net.Conn
	buf           bytes.Buffer
	initialBufCap int
	lastErr       error
	hasTable      bool
	hasFields     bool
}

func (s *LineSender) Close() error {
	return s.conn.Close()
}

func (s *LineSender) Table(name string) *LineSender {
	if s.lastErr != nil {
		return s
	}
	// TODO validate table name
	if s.hasTable {
		s.lastErr = errors.New("table name already provided")
		return s
	}
	s.buf.WriteString(name)
	s.hasTable = true
	return s
}

func (s *LineSender) Symbol(name, val string) *LineSender {
	if s.lastErr != nil {
		return s
	}
	// TODO validate name and value
	if !s.hasTable {
		s.lastErr = errors.New("table name was not provided")
		return s
	}
	if s.hasFields {
		s.lastErr = errors.New("symbol has to be written before any field")
		return s
	}
	s.buf.WriteByte(',')
	s.buf.WriteString(name)
	s.buf.WriteByte('=')
	s.buf.WriteString(val)
	return s
}

func (s *LineSender) FloatField(name string, val float64) *LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = errors.New("table name was not provided")
		return s
	}
	if !s.hasFields {
		s.buf.WriteByte(' ')
	} else {
		s.buf.WriteByte(',')
	}
	// TODO validate NaN and infinity values
	s.buf.WriteString(name)
	s.buf.WriteString("=")
	// TODO implement proper serialization for numbers
	s.buf.WriteString(fmt.Sprintf("%f", val))
	s.hasFields = true
	return s
}

func (s *LineSender) AtNow(ctx context.Context) error {
	// TODO use ctx
	err := s.lastErr
	s.lastErr = nil
	if err != nil {
		return err
	}

	s.buf.WriteByte('\n')

	s.hasTable = false
	s.hasFields = false

	if s.buf.Len() > s.initialBufCap {
		return s.Flush(ctx)
	}
	return nil
}

func (s *LineSender) Flush(ctx context.Context) error {
	// TODO use ctx
	err := s.lastErr
	s.lastErr = nil
	if err != nil {
		return err
	}

	_, err = s.buf.WriteTo(s.conn)
	if err != nil {
		return err
	}

	if s.buf.Cap() > s.initialBufCap {
		// TODO double check whether it's the right way to shrink buffer
		s.buf.Grow(s.initialBufCap)
	}

	return nil
}

func newDefaultConfig() Config {
	return Config{
		Address:   "127.0.0.1:9009",
		BufferCap: 32 * 1024,
	}
}

// TODO introduce config builder
type Config struct {
	Address   string
	BufferCap int
}
