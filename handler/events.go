package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"go-webhook-delivery-service/db"
	"go-webhook-delivery-service/models"
	"go-webhook-delivery-service/rabbit"
)

type Handler struct {
	db *db.DB
	mq *rabbit.RabbitMQ
}

func New(database *db.DB, mq *rabbit.RabbitMQ) *Handler {
	return &Handler{db: database, mq: mq}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /events", h.CreateEvent)
	mux.HandleFunc("GET /events/{id}", h.GetEvent)
	mux.HandleFunc("POST /subscriptions", h.CreateSubscription)
	mux.HandleFunc("GET /subscriptions", h.ListSubscriptions)
}

func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	var req models.EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	if req.Payload == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payload required"})
		return
	}

	eventID, err := h.db.CreateEvent(req.Payload)
	if err != nil {
		log.Printf("[handler] create event db error: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "db error"})
		return
	}

	eventJSON, _ := json.Marshal(map[string]int64{"event_id": eventID})
	if err := h.mq.Publish(eventJSON); err != nil {
		log.Printf("[handler] publish event_id=%d error: %v", eventID, err)
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"id":     eventID,
			"status": "queued_deferred",
		})
		return
	}

	log.Printf("[handler] event created id=%d", eventID)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":     eventID,
		"status": "queued",
	})
}

func (h *Handler) GetEvent(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}

	event, err := h.db.GetEventWithAttempts(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}

	writeJSON(w, http.StatusOK, event)
}

func (h *Handler) CreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req models.SubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	if req.Name == "" || req.CallbackURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name and callback_url required"})
		return
	}

	id, err := h.db.CreateSubscription(req.Name, req.CallbackURL)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "callback_url already registered"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "db error"})
		return
	}

	log.Printf("[handler] subscription created id=%d name=%s url=%s", id, req.Name, req.CallbackURL)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":           id,
		"name":         req.Name,
		"callback_url": req.CallbackURL,
	})
}

func (h *Handler) ListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := h.db.GetSubscriptions()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "db error"})
		return
	}
	if subs == nil {
		subs = []models.Subscription{}
	}
	writeJSON(w, http.StatusOK, subs)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
