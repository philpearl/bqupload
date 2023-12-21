package bigquery

import (
	"context"
	"sync"
	"time"
)

type retryQueue struct {
	// tw is an unbuffered channel to send buffers to the table writer. We try
	// harder to send to this destination, but never block unless the current
	// buffer is full
	tw chan<- uploadBuffer
	// dw is a buffered channel to send buffers to the disk writer. We try to
	// only send to this if sending to the table writer would block.
	dw chan<- uploadBuffer

	mu     sync.Mutex
	wg     sync.WaitGroup
	cancel context.CancelFunc
	// We only have this to check whether it's done
	ctx         context.Context
	retryBuffer uploadBuffer
	wait        time.Duration
}

func newRetryQueue(tw, dw chan<- uploadBuffer) *retryQueue {
	return &retryQueue{
		tw:   tw,
		dw:   dw,
		wait: 30 * time.Second,
	}
}

func (p *retryQueue) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.ctx = ctx
	p.cancel = cancel
	p.wg.Add(1)
	go p.retryLoop(ctx)
}

func (p *retryQueue) stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func (p *retryQueue) completionFunc(u uploadBuffer, err error) {
	if err == nil {
		return
	}

	p.mu.Lock()

	// If we're not shutting down then we can try to combine this buffer with
	// what we're already retrying.
	if p.ctx.Err() == nil && p.retryBuffer.combine(u) {
		p.mu.Unlock()
		return
	}
	// Can't combine the buffers. We don't want too much data in memory, so we'll
	// spill the larger of the two buffers to disk, and retry it from disk.
	if len(p.retryBuffer.Data) < len(u.Data) {
		u, p.retryBuffer = p.retryBuffer, u
	}
	p.mu.Unlock()

	// This can block so we don't want to hold the lock at this point
	u.f = p.completionFunc
	p.dw <- u
}

func (p *retryQueue) retryLoop(ctx context.Context) {
	defer p.wg.Done()
	t := time.NewTicker(p.wait)
	for {
		select {
		case <-ctx.Done():
			// Shutting down. Send the retry buffer if there's anything in it
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.retryBuffer.len > 0 {
				p.retryBuffer.f = p.completionFunc
				p.dw <- p.retryBuffer
			}
			return
		case <-t.C:
		}
		// Attempt to send the retry buffer
		p.mu.Lock()
		if p.retryBuffer.len == 0 {
			p.mu.Unlock()
			continue
		}
		b := p.retryBuffer
		p.retryBuffer.zero()
		p.mu.Unlock()

		b.f = p.completionFunc
		select {
		case p.tw <- b:
		default:
			// This will block until the buffer is sent. If we're shutting down the
			// table writer should stop reading and we'll write to disk.
			select {
			case p.tw <- b:
			case p.dw <- b:
			}
		}
	}
}
