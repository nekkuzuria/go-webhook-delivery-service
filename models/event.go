package models

import "time"

type EventStatus string

const (
	StatusPending   EventStatus = "pending"
	StatusDelivered EventStatus = "delivered"
	StatusFailed    EventStatus = "failed"
)

type Event struct {
	ID        int64     `json:"id"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type EventRequest struct {
	Payload string `json:"payload"`
}

type EventWithAttempts struct {
	Event
	Attempts []DeliveryAttempt `json:"attempts"`
}

type DeliveryAttempt struct {
	ID             int64       `json:"id"`
	EventID        int64       `json:"event_id"`
	SubscriptionID int64       `json:"subscription_id"`
	Status         EventStatus `json:"status"`
	RetryCount     int         `json:"retry_count"`
	LastError      string      `json:"last_error,omitempty"`
	NextRetryAt    *time.Time  `json:"next_retry_at,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

type Subscription struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	CallbackURL string `json:"callback_url"`
	Active      bool   `json:"active"`
}

type SubscriptionRequest struct {
	Name        string `json:"name"`
	CallbackURL string `json:"callback_url"`
}
