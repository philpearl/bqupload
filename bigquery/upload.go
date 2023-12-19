package bigquery

import (
	"context"

	"github.com/philpearl/bqupload/protocol"
)

const (
	maxUploadBytes = 10 * 1024 * 1024
	maxUploadCount = 10_000

	minUploadCount = 5
)

type Uploader interface {
	// bufferFor returns a buffer of exactly size bytes.
	BufferFor(size int) []byte

	// add is called to note that the buffer returned by bufferFor was used and
	// is full of useful data,
	Add(buf []byte)

	Flush()
}

type UploaderFactory func(ctx context.Context, desc *protocol.ConnectionDescriptor) (Uploader, error)

// Fields are public to make testing easier
type uploadBuffer struct {
	buf  []byte
	Data [][]byte
}
