package bigquery

/*
TODO:
- [ ] flush the current buffer to disk on stop (I think this should work now?)
- [x] delay between retries
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

	completionFunc uploadCompletionFunc
}

func newPump(tw, dw chan<- uploadBuffer, completionFunc uploadCompletionFunc) *pump {
	p := &pump{
		tw:             tw,
		dw:             dw,
		completionFunc: completionFunc,
	}
	p.currentBuffer.regenerate()
	p.currentBuffer.f = p.completionFunc
	return p
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
	if len(p.currentBuffer.Data) > minUploadCount {
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
	if len(p.currentBuffer.Data) == 0 {
		return
	}
	p.currentBuffer.f = p.completionFunc
	select {
	case p.tw <- p.currentBuffer:
	default:
		// This will block until the buffer is sent. If we're shutting down the
		// table writer should stop reading and we'll write to disk.
		select {
		case p.tw <- p.currentBuffer:
		case p.dw <- p.currentBuffer:
		}
	}

	// We've sent the buffer - make a new one
	p.currentBuffer.regenerate()
}
