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
	"strings"
	"sync"
	"time"

	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
)

/*
TODO:
- [ ] clean up empty directories
- [ ] clean up partial writes on start-up
*/

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
	tick := time.NewTicker(5 * time.Second)
	for {
		drm.scan(ctx)

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
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

	// inflight tracks which buffers are currently being uploaded. We don't want
	// to upload the same buffer again until we've got a result. And neither do
	// we want to delete the files before they're done.
	mu       sync.Mutex
	inflight map[string]struct{}
}

func newTableDiskReader(tableDir string, log *slog.Logger) (*tableDiskReader, error) {
	data, err := os.ReadFile(filepath.Join(tableDir, "descriptor"))
	if err != nil {
		return nil, fmt.Errorf("reading descriptor: %w", err)
	}
	var desc protocol.ConnectionDescriptor
	if err := plenc.Unmarshal(data, &desc.Descriptor); err != nil {
		return nil, fmt.Errorf("unmarshalling descriptor: %w", err)
	}

	parts := strings.Split(tableDir, string(os.PathSeparator))
	desc.TableName = parts[len(parts)-2]
	desc.DataSetID = parts[len(parts)-3]
	desc.ProjectID = parts[len(parts)-4]

	return &tableDiskReader{
		tableDir: tableDir,
		log:      log,
		desc:     &desc,
		inflight: make(map[string]struct{}),
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
			id := d.Name()
			if !d.IsDir() || tdr.isInFlight(id) {
				continue
			}

			if err := tdr.processFile(ctx, id); err != nil {
				tdr.log.LogAttrs(ctx, slog.LevelError, "processing file", slog.Any("error", err), slog.String("table", tdr.tableDir), slog.String("file", id))
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

func (tdr *tableDiskReader) isInFlight(id string) bool {
	tdr.mu.Lock()
	defer tdr.mu.Unlock()
	_, ok := tdr.inflight[id]
	return ok
}

func (tdr *tableDiskReader) processFile(ctx context.Context, id string) error {
	bufDir := filepath.Join(tdr.tableDir, id)
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
		f:    func(u uploadBuffer, err error) { tdr.uploadComplete(u, id, err) },
	}

	for i := range u.Data {
		l := binary.BigEndian.Uint32(lbuf[i*4:])
		u.Data[i] = dbuf[:l]
		dbuf = dbuf[l:]
	}

	tdr.mu.Lock()
	tdr.inflight[id] = struct{}{}
	tdr.mu.Unlock()

	select {
	case tdr.out <- u:
	case <-ctx.Done():
		// system is shutting down - don't wait to send this file. We'll keep it
		// for when we restart.
		return nil
	}

	return nil
}

func (tdr *tableDiskReader) uploadComplete(buf uploadBuffer, id string, err error) {
	tdr.mu.Lock()
	defer tdr.mu.Unlock()
	delete(tdr.inflight, id)
	if err == nil {
		bufDir := filepath.Join(tdr.tableDir, id)
		if err := os.RemoveAll(bufDir); err != nil {
			tdr.log.LogAttrs(context.Background(), slog.LevelError, "removing buffer directory", slog.Any("error", err), slog.String("table", tdr.tableDir), slog.String("buffer", id))
		}
	}
}
