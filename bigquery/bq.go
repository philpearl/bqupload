package bigquery

import (
	"context"
	"fmt"
	"log/slog"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/philpearl/bqupload/protocol"
	"google.golang.org/protobuf/types/descriptorpb"
)

type bq struct {
	client *managedwriter.Client
	log    *slog.Logger
}

func New(client *managedwriter.Client, log *slog.Logger) *bq {
	return &bq{
		client: client,
		log:    log,
	}
}

func (bq *bq) MakeUploader(ctx context.Context, desc *protocol.ConnectionDescriptor) (Uploader, error) {
	// convert the descriptor to a protobuf descriptor
	var dp *descriptorpb.DescriptorProto

	table := managedwriter.TableParentFromParts(desc.ProjectID, desc.DataSetID, desc.TableName)
	ms, err := bq.client.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(table),
		managedwriter.WithSchemaDescriptor(dp),
		managedwriter.EnableWriteRetries(true),
		managedwriter.WithDataOrigin("bqupload"),
		managedwriter.WithType(managedwriter.DefaultStream),
	)
	if err != nil {
		return nil, fmt.Errorf("creating managed stream: %w", err)
	}

	ch := make(chan (uploadBuffer), 10)

	bu := newBqUpload(ms, ch, bq.log)
	go func() {
		bu.run(ctx)
	}()

	return &upload{ch: ch}, nil
}

type bqUload struct {
	ch  <-chan uploadBuffer
	ms  *managedwriter.ManagedStream
	log *slog.Logger
}

func newBqUpload(ms *managedwriter.ManagedStream, ch <-chan uploadBuffer, log *slog.Logger) *bqUload {
	return &bqUload{
		ms:  ms,
		ch:  ch,
		log: log,
	}
}

func (bu *bqUload) run(ctx context.Context) {
	for b := range bu.ch {
		bu.send(ctx, b)
	}
}

func (bu *bqUload) send(ctx context.Context, b uploadBuffer) {
	// TODO: we're just logging errors for now.
	// Longer term we need to decide what to do. I think we either want to
	//
	// - retry the full append.
	// - strip out the bad rows and retry the append.
	// - drop the full append.
	//
	// In future we may also want an option to divert the append or the bad rows
	// to a dead-letter store
	res, err := bu.ms.AppendRows(ctx, b.Data)
	if err != nil {
		// We really want to know whether this is a fatal error or not. If not
		// fatal we can retry. We can just keep blocking until it works? TODO:
		// what?
		bu.log.LogAttrs(ctx, slog.LevelError, "append rows", slog.Any("error", err))
		return
	}

	// TODO: we can read these full responses asynchronously to improve
	// throughput
	full, err := res.FullResponse(ctx)
	if err != nil {
		// We really want to know whether this is a fatal error or not. If not
		// fatal we can retry. We can just keep blocking until it works? TODO:
		// what?
		bu.log.LogAttrs(ctx, slog.LevelError, "append rows full response", slog.Any("error", err))
		return
	}

	for _, re := range full.GetRowErrors() {
		bu.log.LogAttrs(ctx, slog.LevelError, "row error", slog.Int64("index", re.Index), slog.String("code", re.Code.String()), slog.String("message", re.Message))
	}
}
