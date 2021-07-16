package calendar

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	internalgrpc "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/grpc"
	internalhttp "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/http"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage"
)

type App struct {
	logger  Logger
	storage Storage
}

type Logger interface {
	Info(msg string)
	Error(msg string)
}

type Storage interface {
	CreateEvent(ctx context.Context, event storage.Event) error
	UpdateEvent(ctx context.Context, id string, event storage.Event) error
	DeleteEvent(ctx context.Context, id string) error
	ListDayEvents(ctx context.Context, date time.Time) ([]storage.Event, error)
	ListWeekEvents(ctx context.Context, date time.Time) ([]storage.Event, error)
	ListMonthEvents(ctx context.Context, date time.Time) ([]storage.Event, error)
}

func New(logger Logger, storage Storage) *App {
	return &App{
		logger:  logger,
		storage: storage,
	}
}

func (a *App) Serve(ctx context.Context, cfg ServerConf) error {
	grpcAddress := net.JoinHostPort(cfg.GRPC.Host, cfg.GRPC.Port)
	httpAddress := net.JoinHostPort(cfg.HTTP.Host, cfg.HTTP.Port)

	grpcServer := internalgrpc.NewServer(grpcAddress, a.logger, a)
	httpServer := internalhttp.NewServer(httpAddress, grpcAddress, a.logger)

	errCh := make(chan error, 2)

	go func() {
		a.logger.Info("grpc is running...")

		if err := grpcServer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("failed to run grpc server: %w", err)
		}
	}()

	go func() {
		a.logger.Info("http is running...")

		if err := httpServer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("failed to run http server: %w", err)
		}
	}()

	go func() {
		<-ctx.Done()

		errCh <- ctx.Err()
	}()

	err := <-errCh

	if errors.Is(err, context.DeadlineExceeded) {
		return stop(a.logger, grpcServer, httpServer)
	}

	return err
}

func (a *App) CreateEvent(ctx context.Context, id, title string) error {
	ownerID := uuid.New()

	return a.storage.CreateEvent(ctx, storage.Event{ID: id, Title: title, OwnerID: ownerID.String()})
}

func (a *App) UpdateEvent(ctx context.Context, id string, event storage.Event) error {
	return a.storage.UpdateEvent(ctx, id, event)
}

func (a *App) DeleteEvent(ctx context.Context, id string) error {
	return a.storage.DeleteEvent(ctx, id)
}

func (a *App) ListDayEvents(ctx context.Context, date time.Time) ([]storage.Event, error) {
	return a.storage.ListDayEvents(ctx, date)
}

func (a *App) ListWeekEvents(ctx context.Context, date time.Time) ([]storage.Event, error) {
	return a.storage.ListWeekEvents(ctx, date)
}

func (a *App) ListMonthEvents(ctx context.Context, date time.Time) ([]storage.Event, error) {
	return a.storage.ListMonthEvents(ctx, date)
}

func stop(log Logger, grpc *internalgrpc.Server, http *internalhttp.Server) error {
	log.Info("calendar is stopping...")

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*3)

	defer cancelFn()

	log.Info("http is stopping...")

	if err := http.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop http server: %w", err)
	}

	log.Info("grpc is stopping...")

	if err := grpc.Stop(); err != nil {
		return fmt.Errorf("failed to stop grpc server: %w", err)
	}

	return nil
}
