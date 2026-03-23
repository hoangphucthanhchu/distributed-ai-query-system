package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"mime"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// JobPayload is the Kafka message body for a single AI job.
type JobPayload struct {
	ID        string    `json:"id"`
	Query     string    `json:"query"`
	CreatedAt time.Time `json:"created_at"`
}

func registerHandlers(mux *http.ServeMux, writer *kafka.Writer, pool *pgxpool.Pool) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "missing job id", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var queryText, aiResult string
		var createdAt time.Time
		err := pool.QueryRow(ctx, `
			SELECT query_text, ai_result, created_at
			FROM job_results WHERE job_id = $1
		`, id).Scan(&queryText, &aiResult, &createdAt)
		if errors.Is(err, pgx.ErrNoRows) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"job_id": id,
				"status": "pending",
			})
			return
		}
		if err != nil {
			log.Printf("job lookup %s: %v", id, err)
			http.Error(w, "lookup failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":     id,
			"status":     "completed",
			"query":      queryText,
			"result":     aiResult,
			"created_at": createdAt.UTC().Format(time.RFC3339Nano),
		})
	})
	mux.HandleFunc("POST /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if ct != "application/json" {
			http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		var body struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.Query == "" {
			http.Error(w, "query is required", http.StatusBadRequest)
			return
		}

		id := uuid.NewString()
		p := JobPayload{
			ID:        id,
			Query:     body.Query,
			CreatedAt: time.Now().UTC(),
		}
		raw, err := json.Marshal(p)
		if err != nil {
			http.Error(w, "encode failed", http.StatusInternalServerError)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		err = writeKafkaJSON(ctx, writer, id, raw, []kafka.Header{
			{Key: "x-retry-count", Value: []byte("0")},
		})
		if err != nil {
			log.Printf("kafka publish: %v", err)
			http.Error(w, "failed to enqueue job", http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id": id,
			"status": "queued",
		})
	})
}
