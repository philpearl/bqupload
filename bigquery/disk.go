package bigquery

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
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
- [x] make creating the data on disk atomic
- [x] create the disk readers for each table.
- [x] create the disk reader manager
- [X] handle sleeping for the disk reader
- [x] plumb everything together
- [x] directory should be a hash of the descriptor
- [ ] clean up empty directories

*/

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

// tableDiskReader reads buffers from disk and sends them to the uploader. One
// tableDiskReader is created per table & descriptor
type tableDiskReader struct {
	tableDir string
	log      *slog.Logger
	// This is expected to be an unbuffered channel and we'll only be able to
	// send on it if there's no in-memory traffic.
	out    chan<- uploadBuffer
	wg     sync.WaitGroup
	cancel context.CancelFunc

	desc *protocol.ConnectionDescriptor
}

func newTableDiskReader(tableDir string, log *slog.Logger) (*tableDiskReader, error) {
	var desc protocol.ConnectionDescriptor
	data, err := os.ReadFile(filepath.Join(tableDir, "descriptor"))
	if err != nil {
		return nil, fmt.Errorf("reading descriptor: %w", err)
	}
	if err := plenc.Unmarshal(data, &desc); err != nil {
		return nil, fmt.Errorf("unmarshalling descriptor: %w", err)
	}

	return &tableDiskReader{
		tableDir: tableDir,
		log:      log,
		desc:     &desc,
	}, nil
}

func (tdr *tableDiskReader) Start(ctx context.Context, out chan<- uploadBuffer) {
	ctx, cancel := context.WithCancel(ctx)
	tdr.cancel = cancel
	tdr.out = out
	tdr.wg.Add(1)
	go tdr.loop(ctx)
}

func (tdr *tableDiskReader) Stop() {
	if tdr.cancel != nil {
		tdr.cancel()
	}
	tdr.wg.Wait()
}

func (tdr *tableDiskReader) loop(ctx context.Context) {
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

func (tdr *tableDiskReader) processFile(ctx context.Context, bufDir string) error {
	// Read lengths first because it is written last (and written atomically).
	// If it isn't present this file isn't ready to be read yet.
	lbuf, err := os.ReadFile(filepath.Join(bufDir, "lengths"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Not ready to be read yet
			return nil
		}
		return fmt.Errorf("reading lengths: %w", err)
	}
	l := len(lbuf) / 4
	dbuf, err := os.ReadFile(filepath.Join(bufDir, "data"))
	if err != nil {
		return fmt.Errorf("reading data: %w", err)
	}

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

type chanForDescriptor func(context.Context, *protocol.ConnectionDescriptor) (chan<- uploadBuffer, error)

type DiskReadManager struct {
	baseDir string
	log     *slog.Logger

	chanForDescriptor chanForDescriptor
	cancel            context.CancelFunc
	wg                sync.WaitGroup

	diskReaders map[writerKey]*tableDiskReader
}

func NewDiskReadManager(baseDir string, log *slog.Logger, chanForDescriptor chanForDescriptor) *DiskReadManager {
	return &DiskReadManager{
		baseDir:           baseDir,
		log:               log,
		chanForDescriptor: chanForDescriptor,
		diskReaders:       make(map[writerKey]*tableDiskReader),
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

			tdr, err := newTableDiskReader(filepath.Join(tableDir, hash), drm.log)
			if err != nil {
				if !errors.Is(err, fs.ErrNotExist) {
					drm.log.LogAttrs(ctx, slog.LevelError, "creating table disk reader", slog.Any("error", err), slog.String("project", projectID), slog.String("dataset", dataSetID))
				}
				continue
			}

			ch, err := drm.chanForDescriptor(ctx, tdr.desc)
			if err != nil {
				drm.log.LogAttrs(ctx, slog.LevelError, "getting write channel", slog.Any("error", err), slog.String("project", projectID), slog.String("dataset", dataSetID))
				continue
			}
			tdr.Start(ctx, ch)

			drm.diskReaders[key] = tdr
		}
	}
}
