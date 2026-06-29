-- =====================================================
-- Telemetry Streaming Pipeline — TimescaleDB Schema
-- =====================================================

-- -----------------------------------------------------
-- 1. MOVING AVERAGE OUTPUT
-- Sliding window aggregation per VM (CPU only)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS vm_cpu_aggregates (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    vm_id        VARCHAR(255) NOT NULL,
    avg_cpu      DOUBLE PRECISION NOT NULL,
    max_cpu      DOUBLE PRECISION,
    min_cpu      DOUBLE PRECISION,
    record_count BIGINT NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);

SELECT create_hypertable(
    'vm_cpu_aggregates',
    'window_start',
    if_not_exists => TRUE
);

-- -----------------------------------------------------
-- 2. TIME-BASED JOIN OUTPUT
-- Cross-VM correlation within event-time windows
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS vm_cpu_correlations (
    window_start TIMESTAMP NOT NULL,
    vm_id_a      VARCHAR(255) NOT NULL,
    vm_id_b      VARCHAR(255) NOT NULL,
    cpu_a        DOUBLE PRECISION,
    cpu_b        DOUBLE PRECISION,
    PRIMARY KEY (window_start, vm_id_a, vm_id_b)
);

SELECT create_hypertable(
    'vm_cpu_correlations',
    'window_start',
    if_not_exists => TRUE
);

-- -----------------------------------------------------
-- 3. SAMPLE-AND-HOLD OUTPUT
-- Last-known CPU value per tumbling window
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS vm_metrics_held (
    window_start     TIMESTAMP NOT NULL,
    window_end       TIMESTAMP NOT NULL,
    vm_id            VARCHAR(255) NOT NULL,
    cpu_held         DOUBLE PRECISION NOT NULL,
    last_event_time  TIMESTAMP NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);

SELECT create_hypertable(
    'vm_metrics_held',
    'window_start',
    if_not_exists => TRUE
);

-- -----------------------------------------------------
-- 4. RAW TELEMETRY BACKUP TABLE (optional batch landing)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS vm_raw_telemetry (
    event_time TIMESTAMP NOT NULL,
    vm_id      VARCHAR(255) NOT NULL,
    min_cpu    DOUBLE PRECISION,
    max_cpu    DOUBLE PRECISION,
    avg_cpu    DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
    'vm_raw_telemetry',
    'event_time',
    if_not_exists => TRUE
);
