package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/philpearl/bqupload/bigquery"
	"github.com/philpearl/bqupload/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
)

const meterName = "github.com/phipearl/bqupload"

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

type opts struct {
	baseDir  string
	addr     string
	profiler bool
}

func (o *opts) registerFlags() {
	flag.StringVar(&o.baseDir, "base-dir", "", "base directory for disk storage")
	flag.StringVar(&o.addr, "addr", ":8123", "address to listen on")
	flag.BoolVar(&o.profiler, "profiler", false, "enable profiler")
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

	if o.profiler {
		go func() {
			// We just need a http server serving the default mux to get a pprof server
			if err := http.ListenAndServe("localhost:6060", nil); err != nil {
				log.LogAttrs(ctx, slog.LevelError, "profiler server exited", slog.Any("error", err))
			}
		}()
	}

	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("creating prometheus exporter: %w", err)
	}
	// TODO: use metric.WithResource to associate some attributes with all metrics.
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	meter := provider.Meter(meterName)

	// Start the prometheus HTTP server and pass the exporter Collector to it
	go serveMetrics(log, meter, exporter)

	cli, err := managedwriter.NewClient(ctx, "")
	if err != nil {
		return fmt.Errorf("creating bigquery client: %w", err)
	}

	bq, err := bigquery.New(cli, log, meter, o.baseDir)
	if err != nil {
		return fmt.Errorf("creating bigquery: %w", err)
	}
	drm, err := bigquery.NewDiskReadManager(o.baseDir, log, meter, bq.GetChanForDescriptor)
	if err != nil {
		return fmt.Errorf("creating disk read manager: %w", err)
	}
	drm.Start(ctx)

	s, err := server.New(o.addr, log, meter, bq.GetUploader)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}
	if err := s.Start(ctx); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	// block while the server runs.
	<-ctx.Done()
	log.Info("shutting down")

	s.Stop()
	drm.Stop()
	bq.Stop()

	log.Info("shut down")
	return nil
}

func serveMetrics(log *slog.Logger, meter api.Meter, exporter *prometheus.Exporter) {
	log.Info("serving metrics at localhost:2223/metrics")
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2223", nil)
	if err != nil {
		fmt.Printf("error serving http: %v", err)
		return
	}
}
