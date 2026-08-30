package consumer

import (
	"encoding/json"
	"log"
	"time"

	"go-webhook-delivery-service/db"
	"go-webhook-delivery-service/rabbit"
)

type RecoveryWorker struct {
	db           *db.DB
	mq           *rabbit.RabbitMQ
	staleTimeout time.Duration
}

func NewRecoveryWorker(database *db.DB, mq *rabbit.RabbitMQ) *RecoveryWorker {
	return &RecoveryWorker{
		db:           database,
		mq:           mq,
		staleTimeout: 1 * time.Hour,
	}
}

func (rw *RecoveryWorker) RecoverPending() {
	attempts, err := rw.db.GetPendingDeliveryAttempts()
	if err != nil {
		log.Printf("[recovery] fetch pending attempts error: %v", err)
		return
	}

	if len(attempts) == 0 {
		return
	}

	log.Printf("[recovery] found %d pending delivery attempts to requeue", len(attempts))

	for _, a := range attempts {
		body, _ := json.Marshal(map[string]int64{
			"event_id":        a.EventID,
			"subscription_id": a.SubscriptionID,
		})

		retryCount := a.RetryCount
		if retryCount == 0 {
			retryCount = 1
		}

		if err := rw.mq.PublishToRetryQueue(body, retryCount); err != nil {
			log.Printf("[recovery] republish event=%d sub=%d error: %v", a.EventID, a.SubscriptionID, err)
			continue
		}

		log.Printf("[recovery] requeued event=%d sub=%d (attempt %d)", a.EventID, a.SubscriptionID, a.RetryCount)
	}
}

func (rw *RecoveryWorker) StartCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[recovery] stale cleanup started, checking every %s", interval)

	for range ticker.C {
		rw.cleanupStale()
	}
}

func (rw *RecoveryWorker) cleanupStale() {
	threshold := time.Now().UTC().Add(-rw.staleTimeout)
	attempts, err := rw.db.GetStalePendingAttempts(threshold)
	if err != nil {
		log.Printf("[recovery] fetch stale attempts error: %v", err)
		return
	}

	for _, a := range attempts {
		rw.db.UpdateDeliveryAttemptStatus(a.ID, "failed", "delivery timeout", a.RetryCount, nil)
		log.Printf("[recovery] marked stale attempt event=%d sub=%d as failed", a.EventID, a.SubscriptionID)
	}
}
