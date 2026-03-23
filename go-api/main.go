package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	addr := getenv("HTTP_ADDR", ":8080")
	brokers := getenv("KAFKA_BROKERS", "localhost:9094")
	topic := getenv("KAFKA_TOPIC_JOBS", "ai.jobs")

	writer := newKafkaWriter(brokers, topic)
	defer func() { _ = writer.Close() }()

	mux := http.NewServeMux()
	registerHandlers(mux, writer)

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
