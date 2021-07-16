package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/rmq"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage"
	sqlstorage "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage/sql"
)

type App struct {
	logger  Logger
	storage *sqlstorage.Storage
}

type Logger interface {
	Info(msg string)
	Error(msg string)
}

func New(logger Logger, storage *sqlstorage.Storage) *App {
	return &App{
		logger:  logger,
		storage: storage,
	}
}

func (a *App) Serve(ctx context.Context, cfg *Config) error {
	a.logger.Info("sender is running...")

	conn, err := rmq.Dial("amqp", cfg.RMQ)
	if err != nil {
		return fmt.Errorf("failed to create AMQP connection: %w", err)
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open AMQP channel: %w", err)
	}

	defer ch.Close()

	deliveryCh, err := rmq.Consume(ctx, ch, "notifications")
	if err != nil {
		return fmt.Errorf("failed to consume AMQP queue: %w", err)
	}

	var notification *storage.EventNotification

	for m := range deliveryCh {
		notification = &storage.EventNotification{}

		if err := json.Unmarshal(m.Body, notification); err != nil {
			return fmt.Errorf("failed to unmarshal notification: %w", err)
		}
	}

	ticker := time.NewTicker(time.Second)

	defer ticker.Stop()

	defer a.logger.Info("sender is stopping...")

	for t := range ticker.C {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue
		}

		events, err := a.storage.ListDayEvents(ctx, t)
		if err != nil {
			return fmt.Errorf("failed to list events: %w", err)
		}

		for _, e := range events {
			if !t.After(e.StartsAt.Add(-e.NotifyBefore)) {
				continue
			}

			notification := storage.EventNotification{
				ID:       e.ID,
				Title:    e.Title,
				StartsAt: e.StartsAt,
				UserID:   e.OwnerID,
			}

			body, err := json.Marshal(notification)
			if err != nil {
				return fmt.Errorf("failed to marshal notification: %w", err)
			}

			if err := rmq.Publish(ch, "notifications", body); err != nil {
				return fmt.Errorf("failed to publish message: %w", err)
			}
		}
	}

	return nil
}
