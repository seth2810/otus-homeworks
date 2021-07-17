package internalhttp

import (
	"context"
	"errors"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/grpc/pb"
	"google.golang.org/grpc"
)

type Server struct {
	httpAddress string
	grpcAddress string
	logger      Logger
	server      *http.Server
}

type Logger interface {
	Info(msg string)
	Error(msg string)
}

func NewServer(httpAddress, grpcAddress string, logger Logger) *Server {
	return &Server{httpAddress, grpcAddress, logger, nil}
}

func (s *Server) Start(ctx context.Context) error {
	conn, err := grpc.DialContext(ctx, s.grpcAddress, grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		return err
	}

	mux := runtime.NewServeMux()

	if err = pb.RegisterCalendarServiceHandler(ctx, mux, conn); err != nil {
		return err
	}

	s.server = &http.Server{
		Addr:    s.httpAddress,
		Handler: loggingMiddleware(mux, s.logger),
	}

	if err := s.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
