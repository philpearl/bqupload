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
- [x] create the disk readers for each table.
- [x] create the disk reader manager
- [X] handle sleeping for the disk reader
- [ ] plumb everything together
- [x] directory should be a hash of the descriptor
- [ ] clean up empty directories

*/

// DiskWriter handles writing buffers to disk. One DiskWriter is created per
// table & descriptor.
type DiskWriter struct {
	// Where this disk writer is writing to
	baseDir string
	// Channel on which buffers are sent to the disk writer
	in  chan uploadBuffer
	wg  sync.WaitGroup
	log *slog.Logger
}

func NewDiskWriter(baseDir string, log *slog.Logger, desc []byte) (*DiskWriter, error) {
	dw := &DiskWriter{
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
		if err := os.WriteFile(descName, desc, 0o644); err != nil {
			return nil, fmt.Errorf("writing descriptor: %w", err)
		}
	}

	return dw, nil
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
	if err := os.WriteFile(filepath.Join(bufDir, "lengths"), lbuf, 0o644); err != nil {
		return fmt.Errorf("writing lengths: %w", err)
	}
	return nil
}

// TableDiskReader reads buffers from disk and sends them to the uploader. One
// TableDiskReader is created per table & descriptor
type TableDiskReader struct {
	tableDir string
	log      *slog.Logger
	// This is expected to be an unbuffered channel and we'll only be able to
	// send on it if there's no in-memory traffic.
	out    chan<- uploadBuffer
	wg     sync.WaitGroup
	cancel context.CancelFunc

	desc *protocol.ConnectionDescriptor
}

func NewTableDiskReader(tableDir string, log *slog.Logger) (*TableDiskReader, error) {
	var desc protocol.ConnectionDescriptor
	data, err := os.ReadFile(filepath.Join(tableDir, "descriptor"))
	if err != nil {
		return nil, fmt.Errorf("reading descriptor: %w", err)
	}
	if err := plenc.Unmarshal(data, &desc); err != nil {
		return nil, fmt.Errorf("unmarshalling descriptor: %w", err)
	}

	return &TableDiskReader{
		tableDir: tableDir,
		log:      log,
		desc:     &desc,
	}, nil
}

func (tdr *TableDiskReader) Start(ctx context.Context, out chan<- uploadBuffer) {
	ctx, cancel := context.WithCancel(ctx)
	tdr.cancel = cancel
	tdr.out = out
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

type DiskReadManager struct {
	baseDir string
	log     *slog.Logger

	bq     *bq
	cancel context.CancelFunc
	wg     sync.WaitGroup

	diskReaders map[writerKey]*TableDiskReader
}

func NewDiskReadManager(baseDir string, log *slog.Logger, bq *bq) *DiskReadManager {
	return &DiskReadManager{
		baseDir:     baseDir,
		log:         log,
		bq:          bq,
		diskReaders: make(map[writerKey]*TableDiskReader),
	}
}

func (drm *DiskReadManager) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	drm.cancel = cancel
	go drm.loop(ctx)
}

func (drm *DiskReadManager) Stop() {
	if drm.cancel != nil {
		drm.cancel()
	}
	drm.wg.Wait()

	for _, tdr := range drm.diskReaders {
		tdr.Stop()
	}
}

func (drm *DiskReadManager) loop(ctx context.Context) {
	timer := time.NewTimer(5 * time.Second)
	for {
		drm.scan(ctx)

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(5 * time.Second)

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (drm *DiskReadManager) scan(ctx context.Context) {
	dir, err := os.ReadDir(drm.baseDir)
	if err != nil {
		drm.log.LogAttrs(ctx, slog.LevelError, "scanning", slog.Any("error", err))
		return
	}
	for _, e := range dir {
		if !e.IsDir() {
			continue
		}
		projectID := e.Name()
		drm.scanProject(ctx, projectID)
	}
}

func (drm *DiskReadManager) scanProject(ctx context.Context, projectID string) {
	dir, err := os.ReadDir(filepath.Join(drm.baseDir, projectID))
	if err != nil {
		drm.log.LogAttrs(ctx, slog.LevelError, "scanning project", slog.Any("error", err), slog.String("project", projectID))
		return
	}
	for _, e := range dir {
		if !e.IsDir() {
			continue
		}
		dataSetID := e.Name()
		drm.scanDataSet(ctx, projectID, dataSetID)
	}
}

func (drm *DiskReadManager) scanDataSet(ctx context.Context, projectID, dataSetID string) {
	dir, err := os.ReadDir(filepath.Join(drm.baseDir, projectID, dataSetID))
	if err != nil {
		drm.log.LogAttrs(ctx, slog.LevelError, "scanning dataset", slog.Any("error", err), slog.String("project", projectID), slog.String("dataset", dataSetID))
	}
	for _, e := range dir {
		if !e.IsDir() {
			continue
		}
		tableName := e.Name()
		drm.scanTable(ctx, projectID, dataSetID, tableName)
	}
}

func (drm *DiskReadManager) scanTable(ctx context.Context, projectID, dataSetID, tableName string) {
	tableDir := filepath.Join(drm.baseDir, projectID, dataSetID, tableName)
	dir, err := os.ReadDir(tableDir)
	if err != nil {
		drm.log.LogAttrs(ctx, slog.LevelError, "scanning table", slog.Any("error", err), slog.String("project", projectID), slog.String("dataset", dataSetID), slog.String("table", tableName))
		return
	}
	for _, e := range dir {
		if !e.IsDir() {
			continue
		}
		hash := e.Name()
		key := writerKey{ProjectID: projectID, DataSetID: dataSetID, TableName: tableName, Hash: hash}
		if _, ok := drm.diskReaders[key]; !ok {

			tdr, err := NewTableDiskReader(filepath.Join(tableDir, hash), drm.log)
			if err != nil {
				drm.log.LogAttrs(ctx, slog.LevelError, "creating table disk reader", slog.Any("error", err), slog.String("project", projectID), slog.String("dataset", dataSetID))
				continue
			}

			ug, err := drm.bq.getUploadGroup(ctx, tdr.desc)
			if err != nil {
				drm.log.LogAttrs(ctx, slog.LevelError, "getting upload group", slog.Any("error", err), slog.String("project", projectID), slog.String("dataset", dataSetID))
				continue
			}
			tdr.Start(ctx, ug.tw.inMemory)

			drm.diskReaders[key] = tdr
		}
	}
}
