package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
)

type Server struct {
	addr string
	log  *slog.Logger
}

func New(addr string, log *slog.Logger) *Server {
	return &Server{addr: addr, log: log}
}

func (s *Server) Start(ctx context.Context) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.addr, err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("accepting connection: %w", err)
		}
		go s.onNewConnection(conn)
	}
}

func (s *Server) Stop() {
}

func (s *Server) onNewConnection(conn net.Conn) {
	defer conn.Close()
	c := newConn(conn)
	if err := c.do(); err != nil {
		s.log.LogAttrs(context.Background(), slog.LevelError, "connection error", slog.Any("error", err))
	}
}
