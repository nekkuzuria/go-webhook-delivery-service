package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"go-webhook-delivery-service/models"
)

type DB struct {
	conn *sql.DB
}

func New(dbPath string) (*DB, error) {
	conn, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	conn.SetMaxOpenConns(1)

	d := &DB{conn: conn}
	if err := d.migrate(); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return d, nil
}

func (d *DB) Close() error {
	return d.conn.Close()
}

func (d *DB) migrate() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			payload TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS subscriptions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			callback_url TEXT NOT NULL UNIQUE,
			active INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS delivery_attempts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id INTEGER NOT NULL REFERENCES events(id),
			subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
			status TEXT NOT NULL DEFAULT 'pending',
			retry_count INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			next_retry_at DATETIME,
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_da_event ON delivery_attempts(event_id)`,
		`CREATE INDEX IF NOT EXISTS idx_da_status ON delivery_attempts(status)`,
		`CREATE INDEX IF NOT EXISTS idx_da_next_retry ON delivery_attempts(next_retry_at)`,
	}
	for _, q := range queries {
		if _, err := d.conn.Exec(q); err != nil {
			return fmt.Errorf("exec %q: %w", q, err)
		}
	}
	return nil
}

func (d *DB) CreateEvent(payload string) (int64, error) {
	now := time.Now().UTC()
	res, err := d.conn.Exec(
		`INSERT INTO events (payload, created_at, updated_at) VALUES (?, ?, ?)`,
		payload, now, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *DB) GetEvent(id int64) (*models.Event, error) {
	e := &models.Event{}
	err := d.conn.QueryRow(
		`SELECT id, payload, created_at, updated_at FROM events WHERE id = ?`, id,
	).Scan(&e.ID, &e.Payload, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (d *DB) GetEventWithAttempts(id int64) (*models.EventWithAttempts, error) {
	event, err := d.GetEvent(id)
	if err != nil {
		return nil, err
	}

	rows, err := d.conn.Query(
		`SELECT id, event_id, subscription_id, status, retry_count, last_error, next_retry_at, created_at, updated_at
		 FROM delivery_attempts WHERE event_id = ? ORDER BY id`, id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []models.DeliveryAttempt
	for rows.Next() {
		var a models.DeliveryAttempt
		if err := rows.Scan(&a.ID, &a.EventID, &a.SubscriptionID, &a.Status, &a.RetryCount, &a.LastError, &a.NextRetryAt, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}

	return &models.EventWithAttempts{
		Event:    *event,
		Attempts: attempts,
	}, nil
}

func (d *DB) CreateDeliveryAttempt(eventID, subscriptionID int64) (int64, error) {
	now := time.Now().UTC()
	res, err := d.conn.Exec(
		`INSERT INTO delivery_attempts (event_id, subscription_id, status, retry_count, created_at, updated_at) VALUES (?, ?, ?, 0, ?, ?)`,
		eventID, subscriptionID, models.StatusPending, now, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *DB) GetDeliveryAttempt(eventID, subscriptionID int64) (*models.DeliveryAttempt, error) {
	a := &models.DeliveryAttempt{}
	err := d.conn.QueryRow(
		`SELECT id, event_id, subscription_id, status, retry_count, last_error, next_retry_at, created_at, updated_at
		 FROM delivery_attempts WHERE event_id = ? AND subscription_id = ?`, eventID, subscriptionID,
	).Scan(&a.ID, &a.EventID, &a.SubscriptionID, &a.Status, &a.RetryCount, &a.LastError, &a.NextRetryAt, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (d *DB) UpdateDeliveryAttemptStatus(id int64, status models.EventStatus, lastError string, retryCount int, nextRetryAt *time.Time) error {
	_, err := d.conn.Exec(
		`UPDATE delivery_attempts SET status = ?, last_error = ?, retry_count = ?, next_retry_at = ?, updated_at = ? WHERE id = ?`,
		status, lastError, retryCount, nextRetryAt, time.Now().UTC(), id,
	)
	return err
}

func (d *DB) GetActiveSubscriptions() ([]models.Subscription, error) {
	rows, err := d.conn.Query(`SELECT id, name, callback_url, active FROM subscriptions WHERE active = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []models.Subscription
	for rows.Next() {
		var s models.Subscription
		if err := rows.Scan(&s.ID, &s.Name, &s.CallbackURL, &s.Active); err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}
	return subs, nil
}

func (d *DB) CreateSubscription(name, callbackURL string) (int64, error) {
	res, err := d.conn.Exec(
		`INSERT INTO subscriptions (name, callback_url) VALUES (?, ?)`,
		name, callbackURL,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *DB) GetSubscriptions() ([]models.Subscription, error) {
	rows, err := d.conn.Query(`SELECT id, name, callback_url, active FROM subscriptions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []models.Subscription
	for rows.Next() {
		var s models.Subscription
		if err := rows.Scan(&s.ID, &s.Name, &s.CallbackURL, &s.Active); err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}
	return subs, nil
}

func (d *DB) GetPendingDeliveryAttempts() ([]models.DeliveryAttempt, error) {
	rows, err := d.conn.Query(
		`SELECT id, event_id, subscription_id, status, retry_count, last_error, next_retry_at, created_at, updated_at
		 FROM delivery_attempts WHERE status = 'pending'`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []models.DeliveryAttempt
	for rows.Next() {
		var a models.DeliveryAttempt
		if err := rows.Scan(&a.ID, &a.EventID, &a.SubscriptionID, &a.Status, &a.RetryCount, &a.LastError, &a.NextRetryAt, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}
	return attempts, nil
}

func (d *DB) GetStalePendingAttempts(threshold time.Time) ([]models.DeliveryAttempt, error) {
	rows, err := d.conn.Query(
		`SELECT id, event_id, subscription_id, status, retry_count, last_error, next_retry_at, created_at, updated_at
		 FROM delivery_attempts WHERE status = 'pending' AND updated_at < ?`, threshold,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []models.DeliveryAttempt
	for rows.Next() {
		var a models.DeliveryAttempt
		if err := rows.Scan(&a.ID, &a.EventID, &a.SubscriptionID, &a.Status, &a.RetryCount, &a.LastError, &a.NextRetryAt, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}
	return attempts, nil
}
