CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE,
    event_type VARCHAR(100),
    user_id VARCHAR(100),
    timestamp TIMESTAMP,
    payload JSONB
);