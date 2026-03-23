package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// JobPayload matches the JSON produced by go-api.
type JobPayload struct {
	ID        string    `json:"id"`
	Query     string    `json:"query"`
	CreatedAt time.Time `json:"created_at"`
}

func processOnce(ctx context.Context, hc *http.Client, aiBase string, p *JobPayload) (reason string, err error) {
	body, err := json.Marshal(map[string]string{
		"job_id": p.ID,
		"query":  p.Query,
	})
	if err != nil {
		return "marshal_request", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(aiBase, "/")+"/v1/process", bytes.NewReader(body))
	if err != nil {
		return "build_request", err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := hc.Do(req)
	if err != nil {
		return "ai_http", err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "ai_status", fmt.Errorf("ai status %d: %s", res.StatusCode, string(b))
	}

	var out struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return "ai_decode", err
	}

	return "", nil
}

func runWorkerLoop(
	ctx context.Context,
	r *kafka.Reader,
	wRetry, wDLQ *kafka.Writer,
	aiURL string,
	maxRetries, backoffMS int,
) {
	hc := &http.Client{Timeout: 60 * time.Second}
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("fetch: %v", err)
		}

		retryCount := headerInt(m.Headers, headerRetryCount, 0)
		var payload JobPayload
		if err := json.Unmarshal(m.Value, &payload); err != nil {
			log.Printf("skip bad json partition=%d offset=%d: %v", m.Partition, m.Offset, err)
			_ = publishDLQ(ctx, wDLQ, m, retryCount, "invalid_json", []byte(err.Error()))
			if err := r.CommitMessages(ctx, m); err != nil {
				log.Printf("commit: %v", err)
			}
			continue
		}

		if retryCount > 0 && backoffMS > 0 {
			d := time.Duration(backoffMS*retryCount) * time.Millisecond
			if d > 30*time.Second {
				d = 30 * time.Second
			}
			time.Sleep(d)
		}

		reason, perr := processOnce(ctx, hc, aiURL, &payload)
		if perr == nil {
			if err := r.CommitMessages(ctx, m); err != nil {
				log.Printf("commit: %v", err)
			}
			continue
		}

		log.Printf("job %s failed (retry=%d): %v — %s", payload.ID, retryCount, perr, reason)
		next := retryCount + 1
		if next > maxRetries {
			_ = publishDLQ(ctx, wDLQ, m, retryCount, reason, []byte(perr.Error()))
		} else {
			if err := republishRetry(ctx, wRetry, payload.ID, m.Value, next); err != nil {
				log.Printf("retry publish job=%s: %v", payload.ID, err)
				_ = publishDLQ(ctx, wDLQ, m, retryCount, "retry_publish_failed", []byte(err.Error()))
			}
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("commit: %v", err)
		}
	}
}
