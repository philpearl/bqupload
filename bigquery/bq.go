package bigquery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
)

type writerKey struct {
	ProjectID string
	DataSetID string
	TableName string
	Hash      string
}

type uploadGroup struct {
	tw *TableWriter
	dw *DiskWriter
}

type bq struct {
	client  *managedwriter.Client
	log     *slog.Logger
	baseDir string

	mu        sync.Mutex
	uploaders map[writerKey]uploadGroup
}

func New(client *managedwriter.Client, log *slog.Logger, baseDir string) *bq {
	return &bq{
		client:    client,
		log:       log,
		baseDir:   baseDir,
		uploaders: make(map[writerKey]uploadGroup),
	}
}

func (bq *bq) Wait() {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	for _, u := range bq.uploaders {
		u.tw.Wait()
	}
}

func (bq *bq) Stop() error {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	// This is just waiting for the uploaders to exit
	for _, u := range bq.uploaders {
		u.tw.Stop()
	}

	// It's only safe to call this after all the connections have shut down so
	// there are no senders left.
	for _, u := range bq.uploaders {
		u.dw.Stop()
	}

	return nil
}

func (bq *bq) GetUploader(ctx context.Context, desc *protocol.ConnectionDescriptor) (Uploader, error) {
	ug, err := bq.getUploadGroup(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("getting upload group: %w", err)
	}

	// Pumps are per-connection
	pump := NewPump(ug.tw.inMemory, ug.dw.in, desc)
	return pump, nil
}

func (bq *bq) getUploadGroup(ctx context.Context, desc *protocol.ConnectionDescriptor) (uploadGroup, error) {
	data, err := plenc.Marshal(nil, &desc.Descriptor)
	if err != nil {
		return uploadGroup{}, fmt.Errorf("marshalling descriptor: %w", err)
	}

	// We want separate streams to BigQuery if we have different schemas. So we
	// include a hash of the plenc descriptor in the key. Note this won't help
	// if the schemas are not compatible with each other.
	hash := sha256.Sum256(data)
	key := writerKey{ProjectID: desc.ProjectID, DataSetID: desc.DataSetID, TableName: desc.TableName, Hash: hex.EncodeToString(hash[:])}

	bq.mu.Lock()
	defer bq.mu.Unlock()

	if u, ok := bq.uploaders[key]; ok {
		return u, nil
	}

	// We don't have an existing writer, so we create a new set here.
	tw, err := NewTableWriter(ctx, bq.client, desc, bq.log)
	if err != nil {
		return uploadGroup{}, fmt.Errorf("creating table writer: %w", err)
	}

	dir := filepath.Join(bq.baseDir, key.ProjectID, key.DataSetID, key.TableName, key.Hash)
	dw, err := NewDiskWriter(dir, bq.log, data)
	if err != nil {
		return uploadGroup{}, fmt.Errorf("creating disk writer: %w", err)
	}
	tw.Start(ctx)
	dw.Start()
	u := uploadGroup{tw: tw, dw: dw}
	bq.uploaders[key] = u
	return u, nil
}
