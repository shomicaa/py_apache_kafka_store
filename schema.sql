- ---------------------------------------------------------------------------
-- TABLE: inventory_events
-- Written by: reporting_consumer.py
-- Purpose: Full append-only log of every inventory outcome event consumed
--          from tee.inventory. Stores the complete raw event as JSONB.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_events (
    id           BIGSERIAL   PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    order_id     TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    product_name TEXT        NOT NULL,
    quantity     INTEGER     NOT NULL,
    reason       TEXT,
    raw_event    JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inventory_events_order_id   ON inventory_events (order_id);
CREATE INDEX IF NOT EXISTS idx_inventory_events_created_at ON inventory_events (created_at);


-- ---------------------------------------------------------------------------
-- TABLE: orders
-- Written by: consumer.py
-- Purpose: Persists every INVENTORY_RESERVED / INVENTORY_REJECTED event
--          received from the tee.inventory topic.
-- Idempotency: event_id is the PRIMARY KEY — duplicate messages are ignored
--              via INSERT ... ON CONFLICT DO NOTHING.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    event_id   TEXT        PRIMARY KEY,
    event_type TEXT        NOT NULL,
    order_id   TEXT        NOT NULL,
    ts         TIMESTAMPTZ NOT NULL,
    payload    JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_order_id   ON orders (order_id);
CREATE INDEX IF NOT EXISTS idx_orders_event_type ON orders (event_type);
CREATE INDEX IF NOT EXISTS idx_orders_ts         ON orders (ts);


-- ---------------------------------------------------------------------------
-- TABLE: order_status
-- Written by: consumer.py
-- Purpose: Maintains the latest status for each order (one row per order).
--          Updated via upsert whenever a new inventory outcome arrives.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_status (
    order_id     TEXT        PRIMARY KEY,
    status       TEXT        NOT NULL,
    product_name TEXT        NOT NULL,
    quantity     INTEGER     NOT NULL,
    reason       TEXT,
    updated_at   TIMESTAMPTZ NOT NULL
);


-- ---------------------------------------------------------------------------
-- TABLE: audit_log
-- Written by: audit_consumer.py
-- Purpose: Append-only audit trail of every message seen on tee.orders and
--          tee.inventory. Includes unparseable messages stored as raw text.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     TEXT,
    event_type   TEXT,
    order_id     TEXT,
    ts           TIMESTAMPTZ,
    payload      JSONB,
    source_topic TEXT        NOT NULL,
    logged_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_order_id     ON audit_log (order_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_event_type   ON audit_log (event_type);
CREATE INDEX IF NOT EXISTS idx_audit_log_source_topic ON audit_log (source_topic);


-- ---------------------------------------------------------------------------
-- TABLE: dlq_events
-- Written by: audit_dlq.py
-- Purpose: Persists all messages that ended up in the tee.dlq Dead Letter
--          Queue, along with the reason for rejection and the original raw
--          value that triggered the failure.
-- Idempotency: event_id is the PRIMARY KEY — same DLQ event won't be stored
--              twice via INSERT ... ON CONFLICT DO NOTHING.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dlq_events (
    event_id     TEXT        PRIMARY KEY,
    order_id     TEXT,
    ts           TIMESTAMPTZ NOT NULL,
    reason       TEXT,
    source_topic TEXT,
    raw_value    TEXT,
    payload      JSONB
);

CREATE INDEX IF NOT EXISTS idx_dlq_events_order_id ON dlq_events (order_id);
CREATE INDEX IF NOT EXISTS idx_dlq_events_reason   ON dlq_events (reason);
