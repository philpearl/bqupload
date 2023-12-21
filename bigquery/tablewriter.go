package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/philpearl/bqupload/protocol"
)

// tableWriter is responsible for sending data to a single table. It pulls data
// from two channels: one for in-memory buffers and one for disk buffers. It
// sends data to BigQuery in batches.
type tableWriter struct {
	ms     *managedwriter.ManagedStream
	log    *slog.Logger
	wg     sync.WaitGroup
	cancel context.CancelFunc

	inMemory chan uploadBuffer
	disk     chan uploadBuffer
	results  chan partialResult
}

type partialResult struct {
	b   uploadBuffer
	res *managedwriter.AppendResult
}

func newTableWriter(ctx context.Context, client *managedwriter.Client, desc *protocol.ConnectionDescriptor, log *slog.Logger) (*tableWriter, error) {
	// convert the descriptor to a protobuf descriptor
	dp, err := PlencDescriptorToProtobuf(&desc.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("converting descriptor: %w", err)
	}

	table := managedwriter.TableParentFromParts(desc.ProjectID, desc.DataSetID, desc.TableName)
	ms, err := client.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(table),
		managedwriter.WithSchemaDescriptor(dp),
		managedwriter.EnableWriteRetries(true),
		managedwriter.WithDataOrigin("bqupload"),
		managedwriter.WithType(managedwriter.DefaultStream),
	)
	if err != nil {
		return nil, fmt.Errorf("creating managed stream: %w", err)
	}

	return &tableWriter{
		ms:       ms,
		log:      log,
		inMemory: make(chan uploadBuffer),
		disk:     make(chan uploadBuffer),
		results:  make(chan partialResult, 10),
	}, nil
}

func (tw *tableWriter) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	tw.cancel = cancel
	tw.wg.Add(2)
	go tw.resultLoop(ctx)
	go tw.loop(ctx)
}

// Callers to stop should ensure that they have cancelled the context passed to
// start.
func (tw *tableWriter) stop() {
	if tw.cancel != nil {
		tw.cancel()
	}
	// TODO: not great because it doesn't drain the channels? Does that matter since they're unbuffered?
	tw.wg.Wait()
}

// Note this isn't safe to call unless the writers to these channels have
// stopped.
func (tw *tableWriter) wait() {
	close(tw.inMemory)
	close(tw.disk)
	tw.wg.Wait()
}

func (tw *tableWriter) loop(ctx context.Context) {
	defer tw.wg.Done()
	defer close(tw.results)

	inMemoryOpen, diskOpen := true, true
	for inMemoryOpen || diskOpen {
		// wait for a buffer to arrive. We prefer to read from in-memory if
		// there is one.
		var buf uploadBuffer
		var ok bool
		select {
		case buf, ok = <-tw.inMemory:
			if !ok {
				inMemoryOpen = false
				tw.inMemory = nil
			}
		case <-ctx.Done():
			return
		default:
			select {
			case buf, ok = <-tw.inMemory:
				if !ok {
					inMemoryOpen = false
					tw.inMemory = nil
				}
			case buf, ok = <-tw.disk:
				if !ok {
					diskOpen = false
				}
			case <-ctx.Done():
				return
			}
		}

		tw.send(ctx, buf)
	}
}

// send sends the buffer to BigQuery.
func (tw *tableWriter) send(ctx context.Context, b uploadBuffer) {
	if len(b.Data) == 0 {
		// nothing to do
		b.f(b, nil)
		return
	}
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
		b.f(b, err)
		return
	}

	// we can read these full responses asynchronously to improve
	// throughput
	tw.results <- partialResult{b: b, res: res}
}

func (tw *tableWriter) resultLoop(ctx context.Context) {
	defer tw.wg.Done()
	for pr := range tw.results {
		full, err := pr.res.FullResponse(ctx)
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

		rowErrors := full.GetRowErrors()
		for _, re := range rowErrors {
			tw.log.LogAttrs(ctx, slog.LevelError, "row error",
				slog.Int64("index", re.Index),
				slog.String("code", re.Code.String()),
				slog.String("message", re.Message))
		}

		if err == nil && len(rowErrors) > 0 {
			err = fmt.Errorf("row errors")
		}

		pr.b.f(pr.b, err)
	}
}
