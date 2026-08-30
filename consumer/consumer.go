package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"go-webhook-delivery-service/db"
	"go-webhook-delivery-service/models"
	"go-webhook-delivery-service/rabbit"
)

const defaultMaxRetries = 10

type Consumer struct {
	db         *db.DB
	mq         *rabbit.RabbitMQ
	client     *http.Client
	maxRetries int
}

func New(database *db.DB, mq *rabbit.RabbitMQ) *Consumer {
	return &Consumer{
		db:         database,
		mq:         mq,
		client:     &http.Client{Timeout: 30 * time.Second},
		maxRetries: maxRetriesFromEnv(),
	}
}

func (c *Consumer) Start() error {
	msgs, err := c.mq.Consume()
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	log.Println("[consumer] started, waiting for messages...")

	for msg := range msgs {
		if err := c.handleMessage(msg.Body); err != nil {
			log.Printf("[consumer] processing error: %v", err)
			c.mq.Nack(msg.DeliveryTag, false, false)
			continue
		}
		if err := c.mq.Ack(msg.DeliveryTag, false); err != nil {
			log.Printf("[consumer] ack error: %v", err)
		}
	}

	return nil
}

func (c *Consumer) handleMessage(body []byte) error {
	var msg struct {
		EventID        int64 `json:"event_id"`
		SubscriptionID int64 `json:"subscription_id"`
	}
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("invalid message body: %w", err)
	}

	if msg.SubscriptionID > 0 {
		return c.handleRetry(msg.EventID, msg.SubscriptionID)
	}
	return c.handleFanout(msg.EventID)
}

func (c *Consumer) handleFanout(eventID int64) error {
	event, err := c.db.GetEvent(eventID)
	if err != nil {
		return fmt.Errorf("event %d not found: %w", eventID, err)
	}

	subs, err := c.db.GetActiveSubscriptions()
	if err != nil {
		return fmt.Errorf("get subscriptions: %w", err)
	}

	if len(subs) == 0 {
		log.Printf("[consumer] event %d: no active subscriptions", eventID)
		return nil
	}

	for _, sub := range subs {
		existing, _ := c.db.GetDeliveryAttempt(eventID, sub.ID)
		if existing != nil {
			continue
		}

		attemptID, err := c.db.CreateDeliveryAttempt(eventID, sub.ID)
		if err != nil {
			log.Printf("[consumer] create attempt event=%d sub=%d error: %v", eventID, sub.ID, err)
			continue
		}

		if err := c.deliverToSub(event, &sub); err != nil {
			log.Printf("[consumer] delivery failed event=%d sub=%d: %v", eventID, sub.ID, err)
			c.enqueueRetry(attemptID, eventID, sub.ID, 0, err)
			continue
		}

		c.db.UpdateDeliveryAttemptStatus(attemptID, models.StatusDelivered, "", 0, nil)
		log.Printf("[consumer] delivered event=%d sub=%d", eventID, sub.ID)
	}

	return nil
}

func (c *Consumer) handleRetry(eventID, subscriptionID int64) error {
	attempt, err := c.db.GetDeliveryAttempt(eventID, subscriptionID)
	if err != nil {
		return fmt.Errorf("attempt not found event=%d sub=%d: %w", eventID, subscriptionID, err)
	}

	if attempt.Status == models.StatusDelivered {
		return nil
	}

	if attempt.RetryCount >= c.maxRetries {
		c.db.UpdateDeliveryAttemptStatus(attempt.ID, models.StatusFailed, "max retries exceeded", attempt.RetryCount, nil)
		log.Printf("[consumer] event=%d sub=%d max retries exceeded", eventID, subscriptionID)
		return nil
	}

	event, err := c.db.GetEvent(eventID)
	if err != nil {
		return fmt.Errorf("event %d not found: %w", eventID, err)
	}

	subs, err := c.db.GetActiveSubscriptions()
	if err != nil {
		return fmt.Errorf("get subscriptions: %w", err)
	}

	var target *models.Subscription
	for i := range subs {
		if subs[i].ID == subscriptionID {
			target = &subs[i]
			break
		}
	}
	if target == nil {
		c.db.UpdateDeliveryAttemptStatus(attempt.ID, models.StatusFailed, "subscription not found or inactive", attempt.RetryCount, nil)
		return nil
	}

	if err := c.deliverToSub(event, target); err != nil {
		c.enqueueRetry(attempt.ID, eventID, subscriptionID, attempt.RetryCount, err)
		return fmt.Errorf("delivery failed event=%d sub=%d: %w", eventID, subscriptionID, err)
	}

	c.db.UpdateDeliveryAttemptStatus(attempt.ID, models.StatusDelivered, "", attempt.RetryCount, nil)
	log.Printf("[consumer] delivered event=%d sub=%d (retry %d)", eventID, subscriptionID, attempt.RetryCount)
	return nil
}

func (c *Consumer) deliverToSub(event *models.Event, sub *models.Subscription) error {
	req, err := http.NewRequest("POST", sub.CallbackURL, bytes.NewBufferString(event.Payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-ID", fmt.Sprintf("%d", event.ID))
	req.Header.Set("X-Webhook-Timestamp", event.CreatedAt.Format(time.RFC3339))
	req.Header.Set("X-Webhook-Subscription", fmt.Sprintf("%d", sub.ID))

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("returned status %d", resp.StatusCode)
}

func (c *Consumer) enqueueRetry(attemptID, eventID, subscriptionID int64, currentRetryCount int, deliveryErr error) {
	newRetryCount := currentRetryCount + 1
	nextRetry := time.Now().UTC().Add(backoff(newRetryCount))

	c.db.UpdateDeliveryAttemptStatus(attemptID, models.StatusPending, deliveryErr.Error(), newRetryCount, &nextRetry)

	retryBody, _ := json.Marshal(map[string]int64{
		"event_id":        eventID,
		"subscription_id": subscriptionID,
	})
	if err := c.mq.PublishToRetryQueue(retryBody, newRetryCount); err != nil {
		log.Printf("[consumer] publish to retry queue error: %v", err)
	}
}

func backoff(retryCount int) time.Duration {
	duration := 30 * time.Second
	for i := 1; i < retryCount; i++ {
		duration *= 2
		if duration > time.Hour {
			duration = time.Hour
			break
		}
	}
	return duration
}

func maxRetriesFromEnv() int {
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultMaxRetries
}
