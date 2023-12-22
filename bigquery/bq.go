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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type writerKey struct {
	ProjectID string
	DataSetID string
	TableName string
	Hash      string
}

type uploadGroup struct {
	tw *tableWriter
	dw *diskWriter
	rq *retryQueue
}

func (u *uploadGroup) start(ctx context.Context) {
	u.tw.start(ctx)
	u.dw.start(ctx)
	u.rq.start(ctx)
}

func (u *uploadGroup) stop() {
	// Stop the retry queues first so they don't try to send anything to the
	// table writers. The retry queues will still exist, but will only send to
	// the disk writers.
	u.rq.stop()
	u.tw.stop()
	u.dw.stop()
}

type bq struct {
	client  *managedwriter.Client
	log     *slog.Logger
	baseDir string

	mu        sync.Mutex
	uploaders map[writerKey]uploadGroup

	dwm diskWriteMetrics
	twm tableWriteMetrics
}

func New(client *managedwriter.Client, log *slog.Logger, meter metric.Meter, baseDir string) (*bq, error) {
	bq := &bq{
		client:    client,
		log:       log,
		baseDir:   baseDir,
		uploaders: make(map[writerKey]uploadGroup),
	}

	if err := bq.dwm.init(meter); err != nil {
		return nil, fmt.Errorf("initialising disk write metrics: %w", err)
	}

	if err := bq.twm.init(meter); err != nil {
		return nil, fmt.Errorf("initialising table write metrics: %w", err)
	}

	return bq, nil
}

type diskWriteMetrics struct {
	bytesWritten metric.Int64Counter
	msgsWritten  metric.Int64Counter
	writes       metric.Int64Counter
}

func (dwm *diskWriteMetrics) init(meter metric.Meter) error {
	var err error
	dwm.bytesWritten, err = meter.Int64Counter("bqupload.disk_write.bytes",
		metric.WithDescription("number of bytes written to disk"),
		metric.WithUnit("{byte}"))
	if err != nil {
		return fmt.Errorf("creating bytes_written counter: %w", err)
	}
	dwm.msgsWritten, err = meter.Int64Counter("bqupload.disk_write.msgs",
		metric.WithDescription("number of messages written to disk"),
		metric.WithUnit("{msg}"))
	if err != nil {
		return fmt.Errorf("creating msgs_written counter: %w", err)
	}
	dwm.writes, err = meter.Int64Counter("bqupload.disk_write.writes",
		metric.WithDescription("number of writes to disk"),
		metric.WithUnit("{count}"))
	if err != nil {
		return fmt.Errorf("creating writes counter: %w", err)
	}
	return nil
}

type tableWriteMetrics struct {
	appends      metric.Int64Counter
	rowsAppended metric.Int64Counter
	errors       metric.Int64Counter
}

func (twm *tableWriteMetrics) init(meter metric.Meter) error {
	var err error
	twm.appends, err = meter.Int64Counter("bqupload.table_write.appends",
		metric.WithDescription("number of appends to table"),
		metric.WithUnit("{count}"))
	if err != nil {
		return fmt.Errorf("creating appends counter: %w", err)
	}
	twm.rowsAppended, err = meter.Int64Counter("bqupload.table_write.rows_appended",
		metric.WithDescription("number of rows appended to table"),
		metric.WithUnit("{row}"))
	if err != nil {
		return fmt.Errorf("creating rows_appended counter: %w", err)
	}
	twm.errors, err = meter.Int64Counter("bqupload.table_write.errors",
		metric.WithDescription("number of errors from table"),
		metric.WithUnit("{count}"))
	if err != nil {
		return fmt.Errorf("creating errors counter: %w", err)
	}
	return nil
}

func (bq *bq) Wait() {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	for _, u := range bq.uploaders {
		u.tw.wait()
	}
}

func (bq *bq) Stop() {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	for _, u := range bq.uploaders {
		u.stop()
	}
}

func (bq *bq) GetUploader(ctx context.Context, desc *protocol.ConnectionDescriptor) (Uploader, error) {
	ug, err := bq.getUploadGroup(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("getting upload group: %w", err)
	}

	// Pumps are per-connection
	pump := newPump(ug.tw.inMemory, ug.dw.in, ug.rq.completionFunc)
	return pump, nil
}

func (bq *bq) GetChanForDescriptor(ctx context.Context, desc *protocol.ConnectionDescriptor) (chan<- uploadBuffer, error) {
	ug, err := bq.getUploadGroup(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("getting upload group: %w", err)
	}
	return ug.tw.inMemory, nil
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

	attr := attribute.NewSet(attribute.String("project", key.ProjectID), attribute.String("dataset", key.DataSetID), attribute.String("table", key.TableName), attribute.String("hash", key.Hash))

	bq.mu.Lock()
	defer bq.mu.Unlock()

	if u, ok := bq.uploaders[key]; ok {
		return u, nil
	}

	// We don't have an existing writer, so we create a new set here.
	tw, err := newTableWriter(ctx, bq.client, desc, bq.log, bq.twm, attr)
	if err != nil {
		return uploadGroup{}, fmt.Errorf("creating table writer: %w", err)
	}

	dir := filepath.Join(bq.baseDir, key.ProjectID, key.DataSetID, key.TableName, key.Hash)
	dw, err := newDiskWriter(dir, bq.log, bq.dwm, attr, data)
	if err != nil {
		return uploadGroup{}, fmt.Errorf("creating disk writer: %w", err)
	}

	rg := newRetryQueue(tw.inMemory, dw.in)
	u := uploadGroup{tw: tw, dw: dw, rq: rg}

	u.start(ctx)

	bq.uploaders[key] = u
	return u, nil
}
