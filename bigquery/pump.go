package bigquery

import "github.com/philpearl/bqupload/protocol"

/*
TODO:
- [ ] flush the current buffer to disk on stop
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

	currentBuffer uploadBuffer
}

func newPump(tw, dw chan<- uploadBuffer, desc *protocol.ConnectionDescriptor) *pump {
	return &pump{
		tw: tw,
		dw: dw,
	}
}

// BufferFor returns a buffer of exactly size bytes. If the current uploadBuffer
// is not large enough, a new one is created and the old one is sent.
func (u *pump) BufferFor(size int) []byte {
	if !u.currentBuffer.spaceFor(size) {
		// send the current buffer and make a new one. Prefer to send upstream.
		select {
		case u.tw <- u.currentBuffer:
		default:
			select {
			case u.tw <- u.currentBuffer:
			case u.dw <- u.currentBuffer:
			}
		}
		u.currentBuffer.reset()
	}

	if len(u.currentBuffer.buf) > minUploadCount {
		select {
		case u.tw <- u.currentBuffer:
			u.currentBuffer.reset()
		default:
		}
	}

	return u.currentBuffer.next(size)
}

// Add is called to note that the buffer returned by bufferFor was used. It is
// expected that buffer is full of data of the requested size.
func (u *pump) Add(buf []byte) {
	u.currentBuffer.add(buf)
}

func (u *pump) Flush() {
	if len(u.currentBuffer.Data) == 0 {
		return
	}
	select {
	case u.tw <- u.currentBuffer:
	default:
		select {
		case u.tw <- u.currentBuffer:
		case u.dw <- u.currentBuffer:
		}
	}
	u.currentBuffer.reset()
}

// Fields are public to make testing easier
type uploadBuffer struct {
	len  int
	buf  []byte
	Data [][]byte
}

func (u *uploadBuffer) reset() {
	u.len = 0
	u.buf = make([]byte, maxUploadBytes)
	u.Data = make([][]byte, 0, maxUploadCount)
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
