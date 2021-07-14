package migrations

import (
	"database/sql"
)

func Up0001(tx *sql.Tx) error {
	query := `
		CREATE TABLE events (
			id varchar(36) PRIMARY KEY,
			title varchar(255) NOT NULL,
			starts_at timestamp NOT NULL DEFAULT NOW(),
			duration varchar(32) NOT NULL,
			description text,
			owner_id varchar(36) NOT NULL,
			notify_before varchar(32)
		);
	`

	if _, err := tx.Exec(query); err != nil {
		return err
	}

	return nil
}

func Down0001(tx *sql.Tx) error {
	if _, err := tx.Exec("DROP TABLE events;"); err != nil {
		return err
	}

	return nil
}
