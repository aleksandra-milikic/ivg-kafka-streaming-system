CREATE TABLE IF NOT EXISTS ivg_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB
);

CREATE TABLE IF NOT EXISTS dlq_events (
    id SERIAL PRIMARY KEY,
    reason TEXT,
    raw_event JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);