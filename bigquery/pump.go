package bigquery

type Pump struct {
	tw chan<- uploadBuffer
	dw chan<- uploadBuffer

	currentBuffer uploadBuffer
}

func NewPump(tw, dw chan<- uploadBuffer) *Pump {
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

func (u *Pump) resetBuffer() {
	u.currentBuffer.buf = make([]byte, maxUploadBytes)
	u.currentBuffer.Data = make([][]byte, 0, maxUploadCount)
}
