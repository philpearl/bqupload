package bigquery

import "github.com/philpearl/bqupload/protocol"

/*
TODO:
- [ ] flush the current buffer to disk on stop
*/

// Pump is essentially a buffer manager. It manages a buffer of data to be
// uploaded to BigQuery. It has two destinations: the table writer and the disk
// writer. It tries to send to the table writer first, but if that would block
// it sends to the disk writer. It also tries to send to the table writer if
// the current buffer is getting full.
//
// The Pump is expected to be used by a single goroutine.
type Pump struct {
	// tw is an unbuffered channel to send buffers to the table writer. We try
	// harder to send to this destination, but never block unless the current
	// buffer is full
	tw chan<- uploadBuffer
	// dw is a buffered channel to send buffers to the disk writer. We try to
	// only send to this if sending to the table writer would block.
	dw chan<- uploadBuffer

	currentBuffer uploadBuffer
}

func NewPump(tw, dw chan<- uploadBuffer, desc *protocol.ConnectionDescriptor) *Pump {
	return &Pump{
		tw: tw,
		dw: dw,
	}
}

// BufferFor returns a buffer of exactly size bytes. If the current uploadBuffer
// is not large enough, a new one is created and the old one is sent.
func (u *Pump) BufferFor(size int) []byte {
	if size > cap(u.currentBuffer.buf) || len(u.currentBuffer.Data) >= maxUploadCount {
		// send the current buffer and make a new one. Prefer to send upstream.
		select {
		case u.tw <- u.currentBuffer:
		default:
			select {
			case u.tw <- u.currentBuffer:
			case u.dw <- u.currentBuffer:
			}
		}
		u.resetBuffer()
	}

	if len(u.currentBuffer.buf) > minUploadCount {
		select {
		case u.tw <- u.currentBuffer:
			u.resetBuffer()
		default:
		}
	}

	return u.currentBuffer.buf[:size]
}

// Add is called to note that the buffer returned by bufferFor was used. It is
// expected that buffer is full of data of the requested size.
func (u *Pump) Add(buf []byte) {
	u.currentBuffer.buf = u.currentBuffer.buf[len(buf):]
	u.currentBuffer.Data = append(u.currentBuffer.Data, buf)
}

func (u *Pump) Flush() {
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
	u.resetBuffer()
}

func (u *Pump) resetBuffer() {
	u.currentBuffer.buf = make([]byte, maxUploadBytes)
	u.currentBuffer.Data = make([][]byte, 0, maxUploadCount)
}
