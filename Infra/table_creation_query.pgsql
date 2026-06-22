-- 1. Create a standard relational table
CREATE TABLE IF NOT EXISTS vm_cpu_aggregates (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    vm_id        VARCHAR(255) NOT NULL,
    avg_cpu      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);

-- 2. Transform it into a TimescaleDB Hypertable chunked by the window_start column
SELECT create_hypertable('vm_cpu_aggregates', 'window_start', if_not_exists => TRUE);