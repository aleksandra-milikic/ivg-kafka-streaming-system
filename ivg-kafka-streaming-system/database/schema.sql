CREATE TABLE ivg_events (
    id SERIAL PRIMARY KEY,
    event_id TEXT,
    event_type TEXT,
    user_id TEXT,
    artwork_id TEXT,
    timestamp TIMESTAMP
);