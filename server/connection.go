package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/philpearl/bqupload/bigquery"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
)

type conn struct {
	net.Conn
	getUploader bigquery.UploaderFactory
}

func newConn(c net.Conn, makeUploader bigquery.UploaderFactory) conn {
	return conn{Conn: c, getUploader: makeUploader}
}

func (c *conn) do(ctx context.Context) error {
	// First we read the stream descriptor. This contains the target table and the
	// protobuf schema descriptor.
	var lengthBuf [4]byte
	if _, err := io.ReadFull(c, lengthBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("reading message length: %w", err)
	}
	length := binary.BigEndian.Uint32(lengthBuf[:])

	data := make([]byte, length)
	if _, err := io.ReadFull(c, data); err != nil {
		return fmt.Errorf("reading descriptor: %w", err)
	}
	var desc protocol.ConnectionDescriptor
	if err := plenc.Unmarshal(data, &desc); err != nil {
		return fmt.Errorf("unmarshalling descriptor: %w", err)
	}

	// Here's where we create the stream we're using for uploading
	u, err := c.getUploader(ctx, &desc)
	if err != nil {
		return fmt.Errorf("creating uploader: %w", err)
	}

	defer u.Flush()

	// TODO: if there's no traffic for a while we want to be able to push what
	// we have in hand. Perhaps some deadlines on the reads?

	// Then we read the data from the stream. We acknowledge each message as we
	// receive it.
	for i := uint32(1); ; i++ {
		// Read the message length. Then we read the message directly into the
		// upload buffer. We use fixed size 4 byte lengths for ease of use.
		if _, err := io.ReadFull(c, lengthBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("reading message length: %w", err)
		}
		length := binary.BigEndian.Uint32(lengthBuf[:])
		data := u.BufferFor(int(length))

		if _, err := io.ReadFull(c, data); err != nil {
			return fmt.Errorf("reading message: %w", err)
		}
		u.Add(data)

		// Acknowledge the message. We'll just send the message number since we
		// always process them in order. The sender can just wait until the
		// returned number exceeds their sending number.
		binary.BigEndian.PutUint32(lengthBuf[:], i)
		if _, err := c.Write(lengthBuf[:]); err != nil {
			return fmt.Errorf("writing ack: %w", err)
		}
	}
}
