CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS anomalies (
    id          BIGSERIAL,
    symbol      TEXT        NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    mean        DOUBLE PRECISION NOT NULL,
    stddev      DOUBLE PRECISION NOT NULL,
    zscore      DOUBLE PRECISION NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('anomalies', 'detected_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_anomalies_symbol ON anomalies (symbol, detected_at DESC);
