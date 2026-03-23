package main

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const headerRetryCount = "x-retry-count"

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

func newJobReader(brokerList []string, groupID string, topicJobs, topicRetry string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokerList,
		GroupID: groupID,
		GroupTopics: []string{
			topicJobs,
			topicRetry,
		},
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  2 * time.Second,
	})
}

func splitBrokers(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func headerInt(headers []kafka.Header, key string, def int) int {
	for _, h := range headers {
		if h.Key == key {
			n, err := strconv.Atoi(string(h.Value))
			if err != nil {
				return def
			}
			return n
		}
	}
	return def
}

func republishRetry(ctx context.Context, w *kafka.Writer, jobID string, value []byte, retryCount int) error {
	return writeKafkaJSON(ctx, w, jobID, value, []kafka.Header{
		{Key: headerRetryCount, Value: []byte(strconv.Itoa(retryCount))},
	})
}

func publishDLQ(ctx context.Context, w *kafka.Writer, m kafka.Message, lastRetry int, reason string, detail []byte) error {
	h := append([]kafka.Header{}, m.Headers...)
	h = append(h,
		kafka.Header{Key: "x-dlq-reason", Value: []byte(reason)},
		kafka.Header{Key: "x-dlq-last-retry", Value: []byte(strconv.Itoa(lastRetry))},
		kafka.Header{Key: "x-dlq-at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)
	if len(detail) > 0 && len(detail) < 4096 {
		h = append(h, kafka.Header{Key: "x-dlq-detail", Value: detail})
	}
	key := string(m.Key)
	if key == "" {
		key = "unknown"
	}
	return writeKafkaJSON(ctx, w, key, m.Value, h)
}
