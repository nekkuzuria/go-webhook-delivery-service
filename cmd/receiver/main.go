package main

import (
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	addr := ":9090"
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	}

	http.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		log.Printf("[receiver] %s | %s | %s", r.Method, r.URL.Path, string(body))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	http.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	log.Printf("[receiver] listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
