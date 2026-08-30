package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-webhook-delivery-service/consumer"
	"go-webhook-delivery-service/db"
	"go-webhook-delivery-service/handler"
	"go-webhook-delivery-service/rabbit"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	rabbitURL := envOr("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	dbPath := envOr("DB_PATH", "./webhooks.db")
	listenAddr := envOr("LISTEN_ADDR", ":8080")

	database, err := db.New(dbPath)
	if err != nil {
		log.Fatalf("db init: %v", err)
	}
	defer database.Close()

	subURL := envOr("SUBSCRIPTION_URL", "http://localhost:9090/webhook")
	if _, err := database.CreateSubscription("default", subURL); err != nil {
		log.Printf("[main] default subscription already exists or error: %v", err)
	} else {
		log.Printf("[main] default subscription created: %s", subURL)
	}

	mq, err := rabbit.New(rabbitURL)
	if err != nil {
		log.Fatalf("rabbitmq init: %v", err)
	}
	defer mq.Close()

	rw := consumer.NewRecoveryWorker(database, mq)
	rw.RecoverPending()
	go rw.StartCleanup(60 * time.Second)

	mux := http.NewServeMux()
	h := handler.New(database, mq)
	h.RegisterRoutes(mux)

	c := consumer.New(database, mq)
	go func() {
		log.Println("[main] starting consumer...")
		if err := c.Start(); err != nil {
			log.Printf("[main] consumer stopped: %v", err)
		}
	}()

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("[main] listening on %s", listenAddr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-stop
	log.Println("[main] shutting down...")
	srv.Close()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
