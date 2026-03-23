package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9094")
	topicJobs := getenv("KAFKA_TOPIC_JOBS", "ai.jobs")
	topicRetry := getenv("KAFKA_TOPIC_RETRY", "ai.jobs.retry")
	topicDLQ := getenv("KAFKA_TOPIC_DLQ", "ai.jobs.dlq")
	group := getenv("KAFKA_CONSUMER_GROUP", "ai-workers")
	maxRetries := getenvInt("MAX_RETRIES", 5)
	backoffMS := getenvInt("RETRY_BACKOFF_MS", 2000)
	aiURL := getenv("AI_SERVICE_URL", "http://localhost:8000")
	dbURL := getenv("DATABASE_URL", "postgres://app:app@localhost:5432/app?sslmode=disable")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	wRetry := newKafkaWriter(brokers, topicRetry)
	wDLQ := newKafkaWriter(brokers, topicDLQ)
	defer func() { _ = wRetry.Close() }()
	defer func() { _ = wDLQ.Close() }()

	brokerList := splitBrokers(brokers)
	r := newJobReader(brokerList, group, topicJobs, topicRetry)
	defer func() { _ = r.Close() }()

	log.Printf("consumer started group=%s topics=[%s,%s] max_retries=%d", group, topicJobs, topicRetry, maxRetries)
	runWorkerLoop(ctx, r, wRetry, wDLQ, aiURL, pool, maxRetries, backoffMS)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
