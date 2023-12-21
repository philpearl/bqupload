package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/philpearl/bqupload/bigquery"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	addr         string
	log          *slog.Logger
	listener     net.Listener
	eg           errgroup.Group
	makeUploader bigquery.UploaderFactory

	cancel context.CancelFunc
}

func New(addr string, log *slog.Logger, makeUploader bigquery.UploaderFactory) *Server {
	return &Server{addr: addr, log: log, makeUploader: makeUploader}
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
	defer conn.Close()
	c := newConn(conn, s.makeUploader)
	if err := c.do(ctx); err != nil {
		s.log.LogAttrs(ctx, slog.LevelError, "connection error", slog.Any("error", err))
	}
}
