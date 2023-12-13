package client

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/philpearl/plenc"
)

type Client struct {
	p    *plenc.Plenc
	pool pool
}

func New(addr string) *Client {
	return &Client{}
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() {}

func (c *Client) Publish(v *any) error {
	// grab a connection from the pool
	conn, err := c.pool.get()
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}

	// Send the data. The connection returns itself to the pool
	return conn.publish(v)
}

type pool struct {
	p *plenc.Plenc
}

func (p *pool) get() (*connection, error) {
	// Only one goroutine can get a particular connection at a time

	c, err := p.newConnection(context.Background(), "localhost:1234")
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}

	return c, nil
}

func (p *pool) release(c *connection) {
	// This goroutine has exclusive write-side access to the connection at this
	// point.
	if c.count > 100_000 {
		// TODO: drop this connection! Don't then return it to the pool
	}
}

type connection struct {
	pool *pool

	sendBuf []byte
	conn    *net.TCPConn

	cond       sync.Cond
	m          sync.Mutex
	wg         sync.WaitGroup
	err        error
	terminated bool

	count uint32
	acked uint32
}

func (p *pool) newConnection(ctx context.Context, addr string) (*connection, error) {
	// Looks like a dialer is the modern way to do this
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dialing: %w", err)
	}

	c := &connection{
		pool: p,
		conn: conn.(*net.TCPConn),
	}
	c.cond.L = &c.m

	// TODO: need to send the descriptor at the start of each connection

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := c.readLoop()
		c.terminate(err)
	}()
	return c, nil
}

func (c *connection) close() error {
	c.terminate(nil)
	c.wg.Wait()
	return c.err
}

func (c *connection) publish(v any) error {
	// We have exclusive write-side access to this connection.
	// We send a 4 byte length followed by the data.
	c.sendBuf = append(c.sendBuf[:0], 0, 0, 0, 0)
	var err error
	c.sendBuf, err = c.pool.p.Marshal(c.sendBuf, v)
	if err != nil {
		return fmt.Errorf("marshalling: %w", err)
	}

	binary.BigEndian.PutUint32(c.sendBuf, uint32(len(c.sendBuf)-4))
	if _, err := c.conn.Write(c.sendBuf); err != nil {
		err = fmt.Errorf("writing: %w", err)
		c.terminate(err)
		return err
	}

	c.count++
	seqNo := c.count
	// TODO: close the connection if the count gets too big

	// At this point we can release the connection back to the pool. From this
	// point forward we don't have exclusive write-side access to the
	// connection.
	c.pool.release(c)

	return c.waitFor(seqNo)
}

func (c *connection) waitFor(seqNo uint32) error {
	c.m.Lock()
	defer c.m.Unlock()
	// TODO: also check for errors / closure

	for c.acked < seqNo && c.err == nil {
		c.cond.Wait()
	}

	return c.err
}

func (c *connection) ack(seqNo uint32) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.acked < seqNo {
		c.acked = seqNo
		c.cond.Broadcast()
	}
}

func (c *connection) terminate(err error) {
	c.m.Lock()
	defer c.m.Unlock()

	// We close the write-side of the connection to signal to the server that it
	// should close the read-side. We don't close the read-side here as we want
	// to read any data that the server sends back to us.
	if cerr := c.conn.CloseWrite(); cerr != nil && err == nil {
		err = fmt.Errorf("closing: %w", cerr)
	}
	if c.err == nil {
		c.err = err
	}
	c.terminated = true
	c.cond.Broadcast()
}

func (c *connection) readLoop() error {
	// All we ever receive back from the server is a sequence of 4 byte numbers.
	// And they increase by 1 each time! Could perhaps optimise this!
	var buf [4]byte
	r := bufio.NewReader(c.conn)
	for {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("reading length: %w", err)
		}

		// We don't need to actually process every number received, so if there's data buffered we can skip ahead
		for i := 0; r.Buffered() > 4 && i < 100; i++ {
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return fmt.Errorf("reading length: %w", err)
			}
		}
		ack := binary.BigEndian.Uint32(buf[:])
		c.ack(ack)
	}
}
