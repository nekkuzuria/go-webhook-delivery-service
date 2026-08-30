package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type eventRequest struct {
	Payload string `json:"payload"`
}

type eventPayload struct {
	ID        int64  `json:"id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

func main() {
	targetURL := envOr("TARGET_URL", "http://localhost:8080/events")
	eventsPerSec := 1000
	if v := os.Getenv("EVENTS_PER_SEC"); v != "" {
		fmt.Sscanf(v, "%d", &eventsPerSec)
	}

	log.Printf("[producer] target=%s rate=%d/s", targetURL, eventsPerSec)

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 100},
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	var (
		totalSent   int64
		totalFailed int64
		second      int64
	)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			select {
			case <-stop:
				return
			default:
			}

			atomic.AddInt64(&second, 1)
			start := time.Now()

			var wg sync.WaitGroup
			sem := make(chan struct{}, 100)

			for i := 0; i < eventsPerSec; i++ {
				wg.Add(1)
				sem <- struct{}{}

				go func(seq int) {
					defer wg.Done()
					defer func() { <-sem }()

					id := atomic.AddInt64(&totalSent, 1)
					body, _ := json.Marshal(eventRequest{
						Payload: mustJSON(eventPayload{
							ID:        id,
							Message:   fmt.Sprintf("event #%d", id),
							Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
						}),
					})

					req, err := http.NewRequest("POST", targetURL, bytes.NewReader(body))
					if err != nil {
						atomic.AddInt64(&totalFailed, 1)
						return
					}
					req.Header.Set("Content-Type", "application/json")

					resp, err := client.Do(req)
					if err != nil {
						atomic.AddInt64(&totalFailed, 1)
						return
					}
					resp.Body.Close()

					if resp.StatusCode >= 400 {
						atomic.AddInt64(&totalFailed, 1)
					}
				}(i)
			}

			wg.Wait()
			elapsed := time.Since(start)
			sent := atomic.LoadInt64(&totalSent)
			failed := atomic.LoadInt64(&totalFailed)

			fmt.Printf("\r[producer] #%d | sent=%d failed=%d elapsed=%s   ", second, sent, failed, elapsed.Truncate(time.Millisecond))
		}
	}()

	<-stop
	fmt.Println()
	log.Printf("[producer] stopped. total sent=%d failed=%d", atomic.LoadInt64(&totalSent), atomic.LoadInt64(&totalFailed))
}

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
