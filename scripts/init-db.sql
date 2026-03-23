CREATE TABLE IF NOT EXISTS job_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id TEXT NOT NULL UNIQUE,
    query_text TEXT NOT NULL,
    ai_result TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_results_created_at ON job_results (created_at DESC);
