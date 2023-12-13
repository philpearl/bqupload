package server

const (
	maxUploadBytes = 10 * 1024 * 1024
	maxUploadCount = 10000
)

type uploadBuffer struct {
	buf  []byte
	data [][]byte
}

type upload struct {
	currentBuffer uploadBuffer
}

// bufferFor returns a buffer of exactly size bytes. If the current uploadBuffer
// is not large enough, a new one is created and the old one is sent.
func (u *upload) bufferFor(size int) []byte {
	if size > cap(u.currentBuffer.buf) || len(u.currentBuffer.data) >= maxUploadCount {
		// send the current buffer and make a new one
		// TODO: send the current buffer!
		u.currentBuffer.buf = make([]byte, maxUploadBytes)
		u.currentBuffer.data = make([][]byte, 0, maxUploadCount)
	}
	return u.currentBuffer.buf[:size]
}

// add is called to note that the buffer returned by bufferFor was used. It is
// expected that buffer is full of data of the requested size.
func (u *upload) add(buf []byte) {
	u.currentBuffer.buf = u.currentBuffer.buf[len(buf):]
	u.currentBuffer.data = append(u.currentBuffer.data, buf)
}
