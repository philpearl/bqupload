package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

type conn struct {
	net.Conn
}

func newConn(c net.Conn) conn {
	return conn{c}
}

func (c *conn) do() error {
	// First we read the stream descriptor. This contains the target table and the
	// protobuf schema descriptor.

	// Then we create a new stream to upload the data to BigQuery.

	var u upload

	// Then we read the data from the stream. We acknowledge each message as we
	// receive it.
	var lengthBuf [4]byte
	for i := uint32(0); ; i++ {
		// Read the message length. Then we read the message directly into the
		// upload buffer. We use fixed size 4 byte lengths for ease of use.
		if _, err := io.ReadFull(c, lengthBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("reading message length: %w", err)
		}
		length := binary.BigEndian.Uint32(lengthBuf[:])
		data := u.bufferFor(int(length))

		if _, err := io.ReadFull(c, data); err != nil {
			return fmt.Errorf("reading message: %w", err)
		}
		u.add(data)

		// Acknowledge the message. We'll just send the message number since we
		// always process them in order. The sender can just wait until the
		// returned number exceeds their sending number.
		binary.BigEndian.PutUint32(lengthBuf[:], i)
		if _, err := c.Write(lengthBuf[:]); err != nil {
			return fmt.Errorf("writing ack: %w", err)
		}
	}
}
