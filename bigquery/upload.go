package bigquery

import (
	"context"
	"sync"

	"github.com/philpearl/bqupload/protocol"
)

const (
	maxUploadBytes = 10 * 1024 * 1024
	maxUploadCount = 10_000
)

type Uploader interface {
	// bufferFor returns a buffer of exactly size bytes.
	BufferFor(size int) []byte

	// add is called to note that the buffer returned by bufferFor was used and
	// is full of useful data,
	Add(buf []byte)
}

type UploaderFactory func(ctx context.Context, desc *protocol.ConnectionDescriptor) (Uploader, error)

// Fields are public to make testing easier
type uploadBuffer struct {
	buf  []byte
	Data [][]byte
}

// TODO: the structure of this isn't quite right. We should store the descriptor
// alongside the upload buffers so we can upload after spilling to disk. We also
// need a structure that works nicely with spilling to disk and reloading.

type upload struct {
	currentBuffer uploadBuffer
	ch            chan<- uploadBuffer
	wg            sync.WaitGroup
}

// BufferFor returns a buffer of exactly size bytes. If the current uploadBuffer
// is not large enough, a new one is created and the old one is sent.
func (u *upload) BufferFor(size int) []byte {
	if size > cap(u.currentBuffer.buf) || len(u.currentBuffer.Data) >= maxUploadCount {
		// send the current buffer and make a new one
		u.send()
		u.currentBuffer.buf = make([]byte, maxUploadBytes)
		u.currentBuffer.Data = make([][]byte, 0, maxUploadCount)
	}
	return u.currentBuffer.buf[:size]
}

// Add is called to note that the buffer returned by bufferFor was used. It is
// expected that buffer is full of data of the requested size.
func (u *upload) Add(buf []byte) {
	u.currentBuffer.buf = u.currentBuffer.buf[len(buf):]
	u.currentBuffer.Data = append(u.currentBuffer.Data, buf)
}

// send sends the current uploadBuffer to BigQuery. This is an async operation.
func (u *upload) send() {
	if len(u.currentBuffer.Data) == 0 {
		return
	}
	u.ch <- u.currentBuffer
}

func (u *upload) Wait() {
	u.send()
	close(u.ch)
	u.wg.Wait()
}
