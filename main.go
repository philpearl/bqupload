package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/philpearl/bqupload/bigquery"
	"github.com/philpearl/bqupload/server"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

type opts struct {
	baseDir string
	addr    string
}

func (o *opts) registerFlags() {
	flag.StringVar(&o.baseDir, "base-dir", "", "base directory for disk storage")
	flag.StringVar(&o.addr, "addr", ":8123", "address to listen on")
}

func (o *opts) validate() error {
	if o.baseDir == "" {
		return fmt.Errorf("base-dir is required")
	}
	return nil
}

func run() error {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	var o opts
	o.registerFlags()
	flag.Parse()
	if err := o.validate(); err != nil {
		return fmt.Errorf("validating options: %w", err)
	}

	cli, err := managedwriter.NewClient(ctx, "")
	if err != nil {
		return fmt.Errorf("creating bigquery client: %w", err)
	}

	bq := bigquery.New(cli, log, o.baseDir)
	drm := bigquery.NewDiskReadManager(o.baseDir, log, bq.GetChanForDescriptor)
	drm.Start(ctx)

	s := server.New(o.addr, log, bq.GetUploader)
	if err := s.Start(ctx); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	// block forever while the server runs.
	<-ctx.Done()
	log.Info("shutting down")

	s.Stop()
	drm.Stop()
	bq.Stop()

	log.Info("shut down")
	return nil
}
