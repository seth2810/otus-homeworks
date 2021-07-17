package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/config"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/migrations"
)

func init() {
	goose.AddNamedMigration("00001_create_events_table.go", migrations.Up0001, migrations.Down0001)
}

type Storage struct {
	db *sqlx.DB
}

func Init(ctx context.Context, cfg config.DatabaseConfig) (*Storage, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DB,
	)

	db, err := goose.OpenDBWithDriver("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB: %w", err)
	}

	if err := goose.Up(db, "migrations"); err != nil {
		return nil, fmt.Errorf("failed to migrate: %w", err)
	}

	storage := New(db)

	if err := storage.Connect(ctx); err != nil {
		return nil, err
	}

	return storage, nil
}

func New(conn *sql.DB) *Storage {
	return &Storage{sqlx.NewDb(conn, "postgres")}
}

func (s *Storage) Connect(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Storage) Close(ctx context.Context) error {
	return s.db.Close()
}

func (s *Storage) CreateEvent(ctx context.Context, event storage.Event) error {
	_, err := s.db.NamedExecContext(ctx, `
		insert into events (
			id, title, starts_at, duration, description, owner_id, notify_before
		) values (
			:id, :title, :starts_at, :duration, :description, :owner_id, :notify_before
		)
	`, &event)

	return err
}

func (s *Storage) UpdateEvent(ctx context.Context, id string, event storage.Event) error {
	_, err := s.db.ExecContext(ctx, `
		update events
		set title=?, starts_at=?, duration=?, description=?, owner_id=?, notify_before=?
		where id=?
	`, event.Title, event.StartsAt, event.Duration, event.Description, event.OwnerID, event.NotifyBefore, id)

	return err
}

func (s *Storage) DeleteEvent(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "delete from events where id=?", id)

	return err
}

func (s *Storage) ListDayEvents(ctx context.Context, date time.Time) ([]storage.Event, error) {
	from := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

	return s.listEventsBetween(ctx, from, from.AddDate(0, 0, 1))
}

func (s *Storage) ListWeekEvents(ctx context.Context, date time.Time) ([]storage.Event, error) {
	offset := (int(time.Monday) - int(date.Weekday()) - 7) % 7
	weekStart := date.AddDate(0, 0, offset)
	from := time.Date(weekStart.Year(), weekStart.Month(), weekStart.Day(), 0, 0, 0, 0, date.Location())

	return s.listEventsBetween(ctx, from, from.AddDate(0, 0, 7))
}

func (s *Storage) ListMonthEvents(ctx context.Context, date time.Time) ([]storage.Event, error) {
	from := time.Date(date.Year(), date.Month(), 0, 0, 0, 0, 0, date.Location())

	return s.listEventsBetween(ctx, from, from.AddDate(0, 1, 0))
}

func (s *Storage) listEventsBetween(ctx context.Context, from, to time.Time) ([]storage.Event, error) {
	events := []storage.Event{}

	if err := s.db.SelectContext(ctx, &events, "select * from events where starts_at between ? and ?", from.String(), to.String()); err != nil {
		return nil, err
	}

	return events, nil
}
