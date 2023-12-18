package bigquery

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
)

/*
TODO:
- [ ] make creating the data on disk atomic
- [ ] create the disk readers for each table.
- [ ] create the disk reader manager
- [X] handle sleeping for the disk reader
- [ ] plumb everything together

*/

type DiskWriter struct {
	baseDir string
	in      chan uploadBuffer
	wg      sync.WaitGroup
	log     *slog.Logger
}

func NewDiskWriter(baseDir string, log *slog.Logger) *DiskWriter {
	return &DiskWriter{
		baseDir: baseDir,
		in:      make(chan uploadBuffer, 100),
		log:     log,
	}
}

func (dw *DiskWriter) Start() {
	dw.wg.Add(1)
	go dw.loop()
}

func (dw *DiskWriter) Stop() {
	close(dw.in)
	dw.wg.Wait()
}

func (dw *DiskWriter) loop() {
	defer dw.wg.Done()
	for buf := range dw.in {
		if err := dw.writeBufferToDisk(buf); err != nil {
			dw.log.LogAttrs(context.Background(), slog.LevelError, "writing buffer to disk", slog.Any("error", err))
		}
	}
}

func (dw *DiskWriter) writeBufferToDisk(buf uploadBuffer) error {
	tableName := buf.desc.ProjectID + "." + buf.desc.DataSetID + "." + buf.desc.TableName
	uploadID := uuid.NewString()

	bufDir := filepath.Join(dw.baseDir, tableName, uploadID)
	if err := os.MkdirAll(bufDir, 0o755); err != nil {
		return fmt.Errorf("making buffer directory: %w", err)
	}

	// Three files:
	// 1. plenc-encoded descriptor
	// 2. The binary data
	// 3. Lengths of each message in order.

	data, err := plenc.Marshal(nil, buf.desc)
	if err != nil {
		return fmt.Errorf("marshalling descriptor: %w", err)
	}
	if err := os.WriteFile(filepath.Join(bufDir, "descriptor"), data, 0o644); err != nil {
		return fmt.Errorf("writing descriptor: %w", err)
	}
	if err := os.WriteFile(filepath.Join(bufDir, "data"), buf.buf, 0o644); err != nil {
		return fmt.Errorf("writing data: %w", err)
	}
	lbuf := make([]byte, 4*len(buf.Data))
	for i, l := range buf.Data {
		binary.BigEndian.PutUint32(lbuf[i*4:], uint32(len(l)))
	}
	if err := os.WriteFile(filepath.Join(bufDir, "lengths"), lbuf, 0o644); err != nil {
		return fmt.Errorf("writing lengths: %w", err)
	}
	return nil
}

// TableDiskReader reads buffers from disk and sends them to the uploader. One
// TableDiskReader is created per table.
type TableDiskReader struct {
	tableDir string
	log      *slog.Logger
	// This is expected to be an unbuffered channel and we'll only be able to
	// send on it if there's no in-memory traffic.
	out    chan<- uploadBuffer
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewTableDiskReader(tableDir string, log *slog.Logger, out chan<- uploadBuffer) *TableDiskReader {
	return &TableDiskReader{
		tableDir: tableDir,
		log:      log,
		out:      out,
	}
}

func (tdr *TableDiskReader) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	tdr.cancel = cancel
	tdr.wg.Add(1)
	go tdr.loop(ctx)
}

func (tdr *TableDiskReader) Stop() {
	if tdr.cancel != nil {
		tdr.cancel()
	}
	tdr.wg.Wait()
}

func (tdr *TableDiskReader) loop(ctx context.Context) {
	defer tdr.wg.Done()

	for {
		de, err := os.ReadDir(tdr.tableDir)
		if err != nil {
			tdr.log.LogAttrs(ctx, slog.LevelError, "reading table directory", slog.Any("error", err), slog.String("table", tdr.tableDir))
			// don't exit. Just wait a bit and try again.
			de = nil
		}

		for _, d := range de {
			if !d.IsDir() {
				continue
			}
			if err := tdr.processFile(ctx, filepath.Join(tdr.tableDir, d.Name())); err != nil {
				tdr.log.LogAttrs(ctx, slog.LevelError, "processing file", slog.Any("error", err), slog.String("table", tdr.tableDir), slog.String("file", d.Name()))
			}
			if err := ctx.Err(); err != nil {
				tdr.log.LogAttrs(ctx, slog.LevelInfo, "context cancelled", slog.Any("error", err), slog.String("table", tdr.tableDir))
				return
			}
		}

		// Now sleep for a bit before we try again.
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (tdr *TableDiskReader) processFile(ctx context.Context, bufDir string) error {
	var desc protocol.ConnectionDescriptor
	data, err := os.ReadFile(filepath.Join(bufDir, "descriptor"))
	if err != nil {
		return fmt.Errorf("reading descriptor: %w", err)
	}
	if err := plenc.Unmarshal(data, &desc); err != nil {
		return fmt.Errorf("unmarshalling descriptor: %w", err)
	}
	dbuf, err := os.ReadFile(filepath.Join(bufDir, "data"))
	if err != nil {
		return fmt.Errorf("reading data: %w", err)
	}
	lbuf, err := os.ReadFile(filepath.Join(bufDir, "lengths"))
	if err != nil {
		return fmt.Errorf("reading lengths: %w", err)
	}
	l := len(lbuf) / 4

	u := uploadBuffer{
		desc: &desc,
		buf:  dbuf,
		Data: make([][]byte, l),
	}

	for i := range u.Data {
		l := binary.BigEndian.Uint32(lbuf[i*4:])
		u.Data[i] = dbuf[:l]
		dbuf = dbuf[l:]
	}

	select {
	case tdr.out <- u:
	case <-ctx.Done():
		// system is shutting down - don't wait to send this file. We'll keep it
		// for when we restart.
		return nil
	}

	// Data is sent. Remove the directory
	if err := os.RemoveAll(bufDir); err != nil {
		return fmt.Errorf("removing buffer directory: %w", err)
	}

	return nil
}
