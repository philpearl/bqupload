package bigquery

import (
	"context"
	"sync"
	"time"

	"github.com/philpearl/bqupload/protocol"
)

/*
TODO:
- [ ] flush the current buffer to disk on stop (I think this should work now?)
- [x] delay between retries
- [ ] does connection closure cancel the pump context?
*/

// pump is essentially a buffer manager. It manages a buffer of data to be
// uploaded to BigQuery. It has two destinations: the table writer and the disk
// writer. It tries to send to the table writer first, but if that would block
// it sends to the disk writer. It also tries to send to the table writer if
// the current buffer is getting full.
//
// The pump is expected to be used by a single goroutine.
type pump struct {
	// tw is an unbuffered channel to send buffers to the table writer. We try
	// harder to send to this destination, but never block unless the current
	// buffer is full
	tw chan<- uploadBuffer
	// dw is a buffered channel to send buffers to the disk writer. We try to
	// only send to this if sending to the table writer would block.
	dw chan<- uploadBuffer

	// currentBuffer is the buffer we're currently writing to. It is only
	// accessed by a single goroutine, so we do not guard it with locks
	currentBuffer uploadBuffer

	mu     sync.Mutex
	wg     sync.WaitGroup
	cancel context.CancelFunc
	// We only have this to check whether it's done
	ctx         context.Context
	retryBuffer uploadBuffer
}

func newPump(tw, dw chan<- uploadBuffer, desc *protocol.ConnectionDescriptor) *pump {
	p := &pump{
		tw: tw,
		dw: dw,
	}

	p.currentBuffer.f = p.completionFunc
	return p
}

func (p *pump) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.wg.Add(1)
	go p.retryLoop(ctx)
}

func (p *pump) stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func (p *pump) completionFunc(u uploadBuffer, err error) {
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
	p.dw <- u
}

func (p *pump) retryLoop(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			// Shutting down. Send the retry buffer if there's anything in it
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.retryBuffer.len > 0 {
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

		p.send(&b)
	}
}

// BufferFor returns a buffer of exactly size bytes. If the current uploadBuffer
// is not large enough, a new one is created and the old one is sent.
func (p *pump) BufferFor(size int) []byte {
	if !p.currentBuffer.spaceFor(size) {
		// send the current buffer and make a new one. Prefer to send upstream.
		p.Flush()
	}

	// To keep data timely, if we can send to the table writer, do so. Otherwise
	// we'll keep queuing.
	if len(p.currentBuffer.buf) > minUploadCount {
		select {
		case p.tw <- p.currentBuffer:
			p.currentBuffer.regenerate()
		default:
		}
	}

	return p.currentBuffer.next(size)
}

// Add is called to note that the buffer returned by bufferFor was used. It is
// expected that buffer is full of data of the requested size.
func (p *pump) Add(buf []byte) {
	p.currentBuffer.add(buf)
}

func (p *pump) Flush() {
	p.send(&p.currentBuffer)
}

func (p *pump) send(u *uploadBuffer) {
	if len(u.Data) == 0 {
		return
	}
	u.f = p.completionFunc
	select {
	case p.tw <- *u:
	default:
		// This will block until the buffer is sent. If we're shutting down the
		// table writer should stop reading and we'll write to disk.
		select {
		case p.tw <- *u:
		case p.dw <- *u:
		}
	}
	// We've sent the buffer - make a new one
	u.regenerate()
}

type uploadCompletionFunc func(u uploadBuffer, err error)

// Fields are public to make testing easier
type uploadBuffer struct {
	len  int
	buf  []byte
	Data [][]byte
	f    uploadCompletionFunc
}

func (u *uploadBuffer) regenerate() {
	u.len = 0
	u.buf = make([]byte, maxUploadBytes)
	u.Data = make([][]byte, 0, maxUploadCount)
}

func (u *uploadBuffer) zero() {
	u.len = 0
	u.buf = nil
	u.Data = nil
}

func (u *uploadBuffer) spaceFor(size int) bool {
	return u.len+size <= cap(u.buf) && len(u.Data) < maxUploadCount
}

func (u *uploadBuffer) next(size int) []byte {
	return u.buf[u.len : size+u.len]
}

func (u *uploadBuffer) add(buf []byte) {
	u.len += len(buf)
	u.Data = append(u.Data, buf)
}

func (u *uploadBuffer) combine(v uploadBuffer) (ok bool) {
	if len(u.buf) == 0 {
		*u = v
		return true
	}

	if u.len+v.len > cap(u.buf) {
		return false
	}
	if len(u.Data)+len(v.Data) > cap(u.Data) {
		return false
	}
	copy(u.buf[u.len:], v.buf[:v.len])
	for _, d := range v.Data {
		l := len(d)
		u.Data = append(u.Data, u.buf[u.len:u.len+l])
		u.len += l
	}
	return true
}
