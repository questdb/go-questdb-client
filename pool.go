package questdb

import (
	"container/list"
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"
)

type ConnectionPool struct {
	address         string
	minIdleConns    int
	maxIdleConns    int
	maxConns        int
	idleTimeout     time.Duration
	connHealthCheck time.Duration
	mu              sync.Mutex
	conns           *list.List
	idleConns       int
	dialer          dialer
	closeCh         chan struct{}
}

type dialer func(ctx context.Context, address string) (net.Conn, error)

func NewConnectionPool(address string, minIdleConns, maxIdleConns, maxConns int, idleTimeout, connHealthCheck time.Duration, dialer dialer) *ConnectionPool {
	pool := &ConnectionPool{
		address:         address,
		minIdleConns:    minIdleConns,
		maxIdleConns:    maxIdleConns,
		maxConns:        maxConns,
		idleTimeout:     idleTimeout,
		connHealthCheck: connHealthCheck,
		conns:           list.New(),
		dialer:          dialer,
		closeCh:         make(chan struct{}),
	}

	go pool.Start()

	return pool
}

func (p *ConnectionPool) Get(ctx context.Context) (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.idleConns > 0 {
		c := p.conns.Back()
		if c == nil {
			return nil, errors.New("connection pool is empty but p.idleConns > 0, p.idleConns=" + strconv.Itoa(p.idleConns))
		}
		conn := p.conns.Remove(c).(net.Conn)
		p.idleConns--
		return conn, nil
	}

	if p.maxConns > 0 && p.idleConns >= p.maxConns {
		return nil, errors.New("maximum number of connections reached")
	}

	conn, err := p.dialer(ctx, p.address)
	if err != nil {
		return nil, err
	}

	p.maxConns++
	return conn, nil
}

func (p *ConnectionPool) Release(conn net.Conn, rwErr error) error {
	if conn == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if rwErr != nil {
		p.maxConns--
		return conn.Close()
	}

	if p.idleConns < p.maxIdleConns {
		p.conns.PushBack(conn)
		p.idleConns++
		return nil
	}

	p.maxConns--
	return conn.Close()
}

func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var retErr error
	for conn := p.conns.Front(); conn != nil; conn = conn.Next() {
		if err := conn.Value.(net.Conn).Close(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}

	p.conns.Init()
	p.idleConns = 0
	return retErr
}

func (p *ConnectionPool) Start() {
	go p.idleConnReaper()
	go p.connHealthChecker()
}

func (p *ConnectionPool) idleConnReaper() {
	ticker := time.NewTicker(p.idleTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			p.mu.Lock()
			for p.idleConns > p.minIdleConns {
				conn := p.conns.Remove(p.conns.Back()).(net.Conn)
				p.idleConns--
				conn.Close()
			}
			p.mu.Unlock()
		}
	}
}

func (p *ConnectionPool) connHealthChecker() {
	ticker := time.NewTicker(p.connHealthCheck)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			// Perform health check logic here
			// For example, send a ping message and check for a response
			// If the connection is unhealthy, close it and remove it from the pool
		}
	}
}

func sendPing(conn net.Conn) error {
	// questdb ilp tcp does not support ping
	return nil

	// Send a ping or heartbeat message to check the connection health
	_, err := conn.Write([]byte("PING\n"))
	if err != nil {
		return err
	}

	// Read a response to ensure the connection is still valid
	response := make([]byte, 4)
	_, err = conn.Read(response)
	if err != nil {
		return err
	}

	// Validate the response if needed

	return nil
}
