// Package health serves liveness and readiness probes on a dedicated port.
package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server is an HTTP server for /healthz and /readyz.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// New listens on ":port" and starts serving. ready is evaluated per request;
// a nil function is treated as not ready.
func New(port int, ready func() bool, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("health: listen on port %d: %w", port, err)
	}

	s := &Server{
		log: log,
		srv: &http.Server{
			Handler:           handler(ready),
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("health server stopped", "error", err)
		}
	}()
	return s, nil
}

func handler(ready func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if ready == nil || !ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready\n"))
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}

// Close shuts the server down, forcing it closed if the graceful stop times out.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		_ = s.srv.Close()
		return err
	}
	return nil
}
