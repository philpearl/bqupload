package bigquery

import (
	"context"
	"log/slog"
	"sync"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/googleapis/gax-go/v2/apierror"
)

// TableWriter is responsible for sending data to a single table. It pulls data
// from two channels: one for in-memory buffers and one for disk buffers. It
// sends data to BigQuery in batches.
type TableWriter struct {
	ms  *managedwriter.ManagedStream
	log *slog.Logger
	wg  sync.WaitGroup

	inMemory chan uploadBuffer
	disk     chan uploadBuffer
}

func NewTableWriter(ms *managedwriter.ManagedStream, log *slog.Logger) *TableWriter {
	return &TableWriter{
		ms:       ms,
		log:      log,
		inMemory: make(chan uploadBuffer),
		disk:     make(chan uploadBuffer),
	}
}

func (tw *TableWriter) Start(ctx context.Context) {
	tw.wg.Add(1)
	go tw.loop(ctx)
}

// Callers to Stop should ensure that they have cancelled the context passed to
// start.
func (tw *TableWriter) Stop() {
	// TODO: not great because it doesn't drain the channels?
	close(tw.inMemory)
	close(tw.disk)
	tw.wg.Wait()
}

func (tw *TableWriter) loop(ctx context.Context) {
	defer tw.wg.Done()
	for {
		// wait for a buffer to arrive. We prefer to read from in-memory if
		// there is one.
		var buf uploadBuffer
		select {
		case buf = <-tw.inMemory:
		case <-ctx.Done():
			return
		default:
			select {
			case buf = <-tw.inMemory:
			case buf = <-tw.disk:
			case <-ctx.Done():
				return
			}
		}

		tw.send(ctx, buf)
	}
}

// send sends the buffer to BigQuery.
func (tw *TableWriter) send(ctx context.Context, b uploadBuffer) {
	// TODO: we're just logging errors for now.
	// Longer term we need to decide what to do. I think we either want to
	//
	// - retry the full append.
	// - strip out the bad rows and retry the append.
	// - drop the full append.
	//
	// In future we may also want an option to divert the append or the bad rows
	// to a dead-letter store
	res, err := tw.ms.AppendRows(ctx, b.Data)
	if err != nil {
		// We really want to know whether this is a fatal error or not. If not
		// fatal we can retry. We can just keep blocking until it works? TODO:
		// what?
		tw.log.LogAttrs(ctx, slog.LevelError, "append rows", slog.Any("error", err))
		return
	}

	// TODO: we can read these full responses asynchronously to improve
	// throughput
	full, err := res.FullResponse(ctx)
	if err != nil {
		// We really want to know whether this is a fatal error or not. If not
		// fatal we can retry. We can just keep blocking until it works? TODO:
		// what?

		if apiErr, ok := apierror.FromError(err); ok {
			// We now have an instance of APIError, which directly exposes more specific
			// details about multiple failure conditions include transport-level errors.
			var storageErr storagepb.StorageError
			if e := apiErr.Details().ExtractProtoMessage(&storageErr); e == nil &&
				storageErr.GetCode() == storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS {
				// TODO: this is a common case. The structure has an additional field that's not yet in the schema. Need to handle this cleanly.
				tw.log.LogAttrs(ctx, slog.LevelError, "BQ table schema is missing fields", slog.Any("error", err))
			}
		}
		tw.log.LogAttrs(ctx, slog.LevelError, "append rows full response", slog.Any("error", err))
	}

	for _, re := range full.GetRowErrors() {
		tw.log.LogAttrs(ctx, slog.LevelError, "row error", slog.Int64("index", re.Index), slog.String("code", re.Code.String()), slog.String("message", re.Message))
	}
}
