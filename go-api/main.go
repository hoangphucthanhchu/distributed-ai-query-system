package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	addr := getenv("HTTP_ADDR", ":8080")
	brokers := getenv("KAFKA_BROKERS", "localhost:9094")
	topic := getenv("KAFKA_TOPIC_JOBS", "ai.jobs")
	dbURL := getenv("DATABASE_URL", "postgres://app:app@localhost:5432/app?sslmode=disable")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	writer := newKafkaWriter(brokers, topic)
	defer func() { _ = writer.Close() }()

	mux := http.NewServeMux()
	registerHandlers(mux, writer, pool)

	log.Printf("api listening on %s (kafka brokers=%s topic=%s)", addr, brokers, topic)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
