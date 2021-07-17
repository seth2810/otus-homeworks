package storage

import "time"

type Event struct {
	ID           string        `db:"id"`
	Title        string        `db:"title"`
	StartsAt     time.Time     `db:"starts_at"`
	Duration     time.Duration `db:"duration"`
	Description  string        `db:"description"`
	OwnerID      string        `db:"owner_id"`
	NotifyBefore time.Duration `db:"notify_before"`
}

type EventNotification struct {
	ID       string    `json:"id"`
	Title    string    `json:"title"`
	StartsAt time.Time `json:"starts_at"`
	UserID   string    `json:"user_id"`
}
