CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_customer_time
  ON events(customer_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_events_payload_gin
  ON events USING GIN(payload);

-- Inspect whether PostgreSQL chooses the composite B-tree index.
EXPLAIN ANALYZE
SELECT id, event_time, event_type
FROM events
WHERE customer_id = 42
ORDER BY event_time DESC
LIMIT 50;

-- JSONB containment query; GIN can accelerate this access pattern.
EXPLAIN ANALYZE
SELECT id
FROM events
WHERE payload @> '{"risk":"high"}'::jsonb;
