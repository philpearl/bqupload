package protocol

import (
	"encoding/binary"
	"io"
)

func AppendMessage(buf []byte, toSend []byte) []byte {
	// Write the length then the message.
	l := len(toSend)
	buf = binary.AppendUvarint(buf, uint64(l))
	buf = append(buf, toSend...)
	return buf
}

type reader interface {
	io.ByteReader
	io.Reader
}

func ReadMessage(from reader, dest []byte) ([]byte, error) {
	l, err := binary.ReadUvarint(from)
	if err != nil {
		return nil, err
	}
	if l == 0 {
		return nil, io.EOF
	}
	if l > uint64(cap(dest)) {
		return nil, io.ErrShortBuffer
	}
	dest = dest[:l]

	if _, err := io.ReadFull(from, dest); err != nil {
		return nil, err
	}
	return dest, nil
}
