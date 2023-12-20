package bigquery

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// diskWriter handles writing buffers to disk. One diskWriter is created per
// table & descriptor.
type diskWriter struct {
	// Where this disk writer is writing to
	baseDir string
	// Channel on which buffers are sent to the disk writer
	in  chan uploadBuffer
	wg  sync.WaitGroup
	log *slog.Logger
}

func newDiskWriter(baseDir string, log *slog.Logger, desc []byte) (*diskWriter, error) {
	dw := &diskWriter{
		baseDir: baseDir,
		in:      make(chan uploadBuffer, 100),
		log:     log,
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("making base directory: %w", err)
	}

	// Write the descriptor to disk if not already present
	descName := filepath.Join(baseDir, "descriptor")
	if _, err := os.Stat(descName); os.IsNotExist(err) {
		tempName := descName + ".new"
		if err := os.WriteFile(tempName, desc, 0o644); err != nil {
			return nil, fmt.Errorf("writing descriptor: %w", err)
		}
		if err := os.Rename(tempName, descName); err != nil {
			return nil, fmt.Errorf("renaming descriptor: %w", err)
		}
	}

	return dw, nil
}

func (dw *diskWriter) start() {
	dw.wg.Add(1)
	go dw.loop()
}

func (dw *diskWriter) stop() {
	close(dw.in)
	dw.wg.Wait()
}

func (dw *diskWriter) loop() {
	defer dw.wg.Done()
	for buf := range dw.in {
		// Note we don't call the completion function on the buffer here. If we
		// can't write to disk we'll lose data. Them's the breaks.
		if err := dw.writeBufferToDisk(buf); err != nil {
			dw.log.LogAttrs(context.Background(), slog.LevelError, "writing buffer to disk", slog.Any("error", err))
		}
	}
}

func (dw *diskWriter) writeBufferToDisk(buf uploadBuffer) error {
	uploadID := uuid.NewString()
	bufDir := filepath.Join(dw.baseDir, uploadID)
	if err := os.MkdirAll(bufDir, 0o755); err != nil {
		return fmt.Errorf("making buffer directory: %w", err)
	}

	// Two files:
	// 1. The binary data
	// 2. Lengths of each message in order.
	if err := os.WriteFile(filepath.Join(bufDir, "data"), buf.buf, 0o644); err != nil {
		return fmt.Errorf("writing data: %w", err)
	}
	lbuf := make([]byte, 4*len(buf.Data))
	for i, l := range buf.Data {
		binary.BigEndian.PutUint32(lbuf[i*4:], uint32(len(l)))
	}
	lengthsName := filepath.Join(bufDir, "lengths")
	lengthsNameNew := lengthsName + ".new"
	if err := os.WriteFile(lengthsNameNew, lbuf, 0o644); err != nil {
		return fmt.Errorf("writing lengths: %w", err)
	}
	if err := os.Rename(lengthsNameNew, lengthsName); err != nil {
		return fmt.Errorf("renaming lengths: %w", err)
	}
	return nil
}
