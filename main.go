package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/philpearl/bqupload/bigquery"
	"github.com/philpearl/bqupload/server"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	ctx := context.Background()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cli, err := managedwriter.NewClient(ctx, "")
	if err != nil {
		return fmt.Errorf("creating bigquery client: %w", err)
	}

	bq := bigquery.New(cli, log)

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	s := server.New(addr, log, bq.MakeUploader)
	if err := s.Start(ctx); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	// TODO: add a signal handler so we can know to spill to disk if we've been
	// asked to terminate

	// block forever while the server runs.
	select {}
}
