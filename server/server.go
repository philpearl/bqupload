package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/philpearl/bqupload/bigquery"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	addr         string
	log          *slog.Logger
	listener     net.Listener
	eg           errgroup.Group
	makeUploader bigquery.UploaderFactory

	connectionsStarted metric.Int64Counter
	connectionsRunning metric.Int64UpDownCounter

	connMetrics connMetrics

	cancel context.CancelFunc
}

func New(addr string, log *slog.Logger, meter metric.Meter, makeUploader bigquery.UploaderFactory) (*Server, error) {
	connectionsRunning, err := meter.Int64UpDownCounter("bqupload.server.connections_running",
		metric.WithDescription("number of connections running"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return nil, fmt.Errorf("creating connections_running counter: %w", err)
	}
	connectionsStarted, err := meter.Int64Counter("bqupload.server.connections_started",
		metric.WithDescription("number of connections started"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return nil, fmt.Errorf("creating connections_started counter: %w", err)
	}

	s := &Server{
		addr:               addr,
		log:                log,
		makeUploader:       makeUploader,
		connectionsStarted: connectionsStarted,
		connectionsRunning: connectionsRunning,
	}

	if err := s.connMetrics.init(meter); err != nil {
		return nil, fmt.Errorf("initialising connection metrics: %w", err)
	}

	return s, nil
}

func (s *Server) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.addr, err)
	}
	s.listener = ln

	s.eg.Go(func() error {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
					return nil
				}
				return fmt.Errorf("accepting connection: %w", err)
			}
			s.log.LogAttrs(context.Background(), slog.LevelInfo, "new connection", slog.Any("remote", conn.RemoteAddr()))
			go s.onNewConnection(ctx, conn.(*net.TCPConn))
		}
	})
	return nil
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	if err := s.listener.Close(); err != nil {
		return fmt.Errorf("closing listener: %w", err)
	}
	return s.eg.Wait()
}

func (s *Server) onNewConnection(ctx context.Context, conn *net.TCPConn) {
	c := s.newConn(conn, s.makeUploader)
	s.connectionsStarted.Add(ctx, 1)
	s.connectionsRunning.Add(ctx, 1)
	defer s.connectionsRunning.Add(ctx, -1)
	if err := c.do(ctx); err != nil {
		s.log.LogAttrs(ctx, slog.LevelError, "connection error", slog.Any("error", err))
	}
}
