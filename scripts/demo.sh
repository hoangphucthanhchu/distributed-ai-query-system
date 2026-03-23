#!/usr/bin/env bash
set -euo pipefail

# Demo nhanh: POST job → chờ GET /v1/jobs/{id} tới khi completed (hoặc hết thời gian).
# Yêu cầu: stack đang chạy (docker compose up), curl, python3.

API_URL="${API_URL:-http://localhost:8080}"
QUERY="${QUERY:-xin chào demo}"
TIMEOUT_SEC="${TIMEOUT_SEC:-60}"
SLEEP_SEC="${SLEEP_SEC:-0.3}"

echo "→ POST ${API_URL}/v1/jobs"
echo "  query: ${QUERY}"

payload="$(QUERY="$QUERY" python3 -c 'import json, os; print(json.dumps({"query": os.environ["QUERY"]}))')"
resp="$(curl -sS -X POST "${API_URL}/v1/jobs" \
	-H 'Content-Type: application/json' \
	-d "$payload")"

job_id="$(echo "$resp" | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])")"
echo "   job_id=${job_id}"

start="$(date +%s)"
while true; do
	now="$(date +%s)"
	if (( now - start > TIMEOUT_SEC )); then
		echo "✗ Hết thời gian (${TIMEOUT_SEC}s), status vẫn chưa completed." >&2
		echo "  Thử: curl -sS ${API_URL}/v1/jobs/${job_id}" >&2
		exit 1
	fi

	body="$(curl -sS "${API_URL}/v1/jobs/${job_id}")"
	status="$(echo "$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")"
	if [[ "$status" == "completed" ]]; then
		echo "✓ Hoàn thành:"
		echo "$body" | python3 -m json.tool
		exit 0
	fi
	sleep "$SLEEP_SEC"
done
