package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
)

type Client struct {
	pool            pool
	host            string
	port            string
	dnsCacheTimeout time.Duration
	desc            []byte
}

func New(addr string, pl *plenc.Plenc, desc *protocol.ConnectionDescriptor) (*Client, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("splitting host and port: %w", err)
	}

	data, err := pl.Marshal(nil, desc)
	if err != nil {
		return nil, fmt.Errorf("marshalling descriptor: %w", err)
	}

	c := &Client{
		host:            host,
		port:            port,
		dnsCacheTimeout: time.Second * 10,
		pool: pool{
			p: pl,
		},
		desc: data,
	}
	c.pool.c = c

	return c, nil
}

func (c *Client) Stop() {}

func (c *Client) Publish(ctx context.Context, v any) error {
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	// grab a connection from the pool
	conn, err := c.pool.get(ctx)
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}

	// Send the data. The connection returns itself to the pool
	return conn.publish(ctx, v)
}

// pool manages a pool of connections.
//
// We potentially want to have multiple
// connections to the same server, but we certainly want a set of connections to
// different servers
type pool struct {
	mu       sync.Mutex
	p        *plenc.Plenc
	resolver net.Resolver
	c        *Client

	// The hosts we're sending to. next is the index of the next host to use.
	// This is a slice of pointers so we can remove hosts from the list while
	// another thread is reading/writing the host pool entry.
	hosts []*hostPool
	next  int

	// lastLookup is the time we last looked up the host list
	lastLookup time.Time
}

type hostPool struct {
	pool        *pool
	name        string
	addr        string
	connections []*connection
}

func (hp *hostPool) get(ctx context.Context) (*connection, error) {
	// We expect the pool lock to be held when this function is called.

	// Do we have an existing entry we can use?
	for _, c := range hp.connections {
		if c.inUse {
			continue
		}
		c.inUse = true
		return c, nil
	}

	// TODO: if the connection list is already big enough, we should wait for
	// one to become free. How!?

	c, err := hp.newConnection(ctx, hp.addr)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}

	c.inUse = true
	hp.connections = append(hp.connections, c)

	return c, nil
}

func (hp *hostPool) release(c *connection) {
	// This goroutine has exclusive write-side access to the connection at this
	// point. We don't expect the pool lock or the connection lock to be held.
	if c.count > 100_000 {
		// drop this connection! Don't then return it to the pool
		c.terminate(nil)
	}

	hp.pool.mu.Lock()
	defer hp.pool.mu.Unlock()
	if c.terminated || c.err != nil {
		// Remove this connection from the list of ones to consider.
		keep := hp.connections[:0]
		for _, conn := range hp.connections {
			if c != conn {
				keep = append(keep, conn)
			}
		}
		hp.connections = keep
	}
	c.inUse = false
}

func (p *pool) get(ctx context.Context) (*connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Only one goroutine can own a particular connection for writing at a time.
	// So when this function returns the calling goroutine has exclusive access to the
	// write-side parts of the connection

	if err := p.ensureHostList(ctx); err != nil {
		return nil, fmt.Errorf("ensuring host list: %w", err)
	}

	if len(p.hosts) == 0 {
		return nil, fmt.Errorf("no hosts")
	}

	if p.next >= len(p.hosts) {
		p.next = 0
	}

	c, err := p.hosts[p.next].get(ctx)
	p.next++
	if err != nil {
		return nil, fmt.Errorf("getting connection: %w", err)
	}

	return c, nil
}

func (p *pool) ensureHostList(ctx context.Context) error {
	if time.Since(p.lastLookup) < p.c.dnsCacheTimeout {
		return nil
	}

	// TODO: should do this in the background once we have an initial host list

	addrs, err := p.resolver.LookupHost(ctx, p.c.host)
	if err != nil {
		return fmt.Errorf("looking up host: %w", err)
	}

	// Remove any hosts that are no longer in the list.
	keep := p.hosts[:0]
	for _, h := range p.hosts {

		var found bool
		for _, addr := range addrs {
			if h.name == addr {
				found = true
				break
			}
		}
		if found {
			// keep this host
			keep = append(keep, h)
		}
	}
	p.hosts = keep

	for _, addr := range addrs {
		var found bool
		for _, h := range p.hosts {
			if h.name == addr {
				found = true
				break
			}
		}
		if !found {
			// add this host
			p.hosts = append(p.hosts, &hostPool{
				name: addr,
				addr: net.JoinHostPort(addr, p.c.port),
				pool: p,
			})
		}
	}

	return nil
}
