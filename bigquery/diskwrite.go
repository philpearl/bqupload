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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// diskWriter handles writing buffers to disk. One diskWriter is created per
// table & descriptor.
type diskWriter struct {
	// Where this disk writer is writing to
	baseDir string
	// Channel on which buffers are sent to the disk writer
	in      chan uploadBuffer
	wg      sync.WaitGroup
	log     *slog.Logger
	metrics diskWriteMetrics
	attr    attribute.Set
}

func newDiskWriter(baseDir string, log *slog.Logger, metrics diskWriteMetrics, attr attribute.Set, desc []byte) (*diskWriter, error) {
	dw := &diskWriter{
		baseDir: baseDir,
		in:      make(chan uploadBuffer, 100),
		log:     log,
		metrics: metrics,
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

func (dw *diskWriter) start(ctx context.Context) {
	dw.wg.Add(1)
	go dw.loop(ctx)
}

func (dw *diskWriter) stop() {
	close(dw.in)
	dw.wg.Wait()
}

func (dw *diskWriter) loop(ctx context.Context) {
	defer dw.wg.Done()
	for buf := range dw.in {
		dw.metrics.writes.Add(ctx, 1, metric.WithAttributeSet(dw.attr))
		// Note we don't call the completion function on the buffer here. If we
		// can't write to disk we'll lose data. Them's the breaks.
		if err := dw.writeBufferToDisk(ctx, buf); err != nil {
			dw.log.LogAttrs(ctx, slog.LevelError, "writing buffer to disk", slog.Any("error", err))
		}
	}
}

func (dw *diskWriter) writeBufferToDisk(ctx context.Context, buf uploadBuffer) error {
	dw.log.LogAttrs(ctx, slog.LevelInfo, "writing buffer to disk", slog.Int("buffer", buf.len))

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

	dw.metrics.bytesWritten.Add(ctx, int64(buf.len), metric.WithAttributeSet(dw.attr))
	dw.metrics.msgsWritten.Add(ctx, int64(len(buf.Data)), metric.WithAttributeSet(dw.attr))

	return nil
}
