package main

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func newKafkaWriter(brokersCSV, topic string) *kafka.Writer {
	brokers := strings.Split(brokersCSV, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		WriteTimeout: 10 * time.Second,
	}
}

func writeKafkaJSON(ctx context.Context, w *kafka.Writer, key string, value []byte, headers []kafka.Header) error {
	return w.WriteMessages(ctx, kafka.Message{
		Key:     []byte(key),
		Value:   value,
		Time:    time.Now().UTC(),
		Headers: headers,
	})
}
