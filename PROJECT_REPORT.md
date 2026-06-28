# Streaming Analytics on Cloud Telemetry Data — Complete Project Report

**Course:** Big Data Streaming Analytics  
**Dataset:** Azure 2019 Public Dataset V2  
**Environment:** Windows 11 Local Sandbox  
**Branch:** `development`  
**Date:** June 2025

---

## 1. Executive Summary

This project implements a **real-time streaming analytics pipeline** that replays historical VM telemetry from the Azure 2019 Public Dataset V2 as a live data stream. The pipeline ingests multi-VM CPU metrics (and synthetically generated memory metrics) from compressed CSV files, pushes them through a Kafka-compatible broker (Redpanda), processes them with **Apache Spark Structured Streaming** using three distinct streaming operators, and sinks the aggregated results into **TimescaleDB** for live visualization via **Grafana**.

The entire stack runs locally on Windows using Docker Desktop, requiring zero cloud infrastructure spend.

---

## 2. Requirements vs. Implementation

### 2.1 Original Requirements

| Requirement | Description |
|-------------|-------------|
| **Framework** | Apache Spark Streaming |
| **Operator 1** | Moving Average — sliding window over CPU and memory |
| **Operator 2** | Time-Based Join — event time window join between multiple VMs |
| **Operator 3** | Sample-and-Hold — forward fill for missing/irregular data |
| **Pipeline** | Ingest → simulate event-time stream → windowed ops & joins → output |
| **Dataset** | Azure VM Dataset (AzurePublicDatasetV2) |

### 2.2 Implementation Mapping

| Requirement | Status | Implementation Details |
|-------------|--------|------------------------|
| **Apache Spark Streaming** | ✅ Complete | PySpark 3.5.x Structured Streaming with 3 parallel `writeStream` queries |
| **Moving Average** | ✅ Complete | `window(event_time, 60s, 20s)` sliding window per VM; computes `avg`, `max`, `min`, `count` for CPU and memory |
| **Time-Based Join** | ✅ Complete | Stream-stream self-join on `vm_id_a != vm_id_b` within ±2 minute event-time window; outputs correlated VM pairs |
| **Sample-and-Hold** | ✅ Complete | `last(value, ignoreNulls=True)` over 30s tumbling windows; producer simulates 15% missing memory to demonstrate forward-fill |
| **Event-Time Stream** | ✅ Complete | Producer assigns real Unix timestamps; processor uses `withWatermark` for 15-minute late-data tolerance |
| **Dataset** | ✅ Complete | Reads `vm_cpu_readings-file-*.csv.gz` from Azure V2; synthetic memory generated from CPU |
| **Grafana Output** | ✅ Complete | Auto-provisioned datasource + 9-panel dashboard with live refresh every 5 seconds |

---

## 3. Architecture Overview

### 3.1 High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA LAYER                                   │
│  Azure V2 CPU Readings (.csv.gz)                                     │
│  Columns: timestamp | vm_id | min_cpu | max_cpu | avg_cpu           │
│  ~227,000 VMs per 5-min tick | 10M rows per file                   │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼ producer.py
┌─────────────────────────────────────────────────────────────────────┐
│                       INGESTION LAYER                                │
│  • Reads multiple CSV files                                          │
│  • Samples 50 VMs per tick (from ~227k available)                   │
│  • Generates synthetic memory correlated to CPU                     │
│  • Drops 15% of memory values (missing-data simulation)             │
│  • Sends JSON events to Kafka topic `telemetry-stream`              │
│  • Replay rate: 2 ticks/sec (compresses trace into real time)     │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼ Redpanda (Kafka API on :19092)
┌─────────────────────────────────────────────────────────────────────┐
│                      MESSAGE BROKER                                  │
│  • Redpanda v23.2.1 — ultra-lightweight Kafka-compatible broker   │
│  • Topic: `telemetry-stream`                                        │
│  • External listener: `localhost:19092`                               │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼ processor.py (3 parallel queries)
┌─────────────────────────────────────────────────────────────────────┐
│                      PROCESSING LAYER                                │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Query 1 — MOVING AVERAGE                                     │    │
│  │  window(60s, 20s) + groupBy(vm_id)                         │    │
│  │  → avg_cpu, avg_memory, max_cpu, min_cpu, record_count      │    │
│  │  → Sink: vm_cpu_aggregates                                 │    │
│  └─────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Query 2 — TIME-BASED JOIN                                  │    │
│  │  leftStream.join(rightStream, ±2 min event window)          │    │
│  │  condition: vm_id_a != vm_id_b                               │    │
│  │  → cpu_a, cpu_b, memory_a, memory_b                        │    │
│  │  → Sink: vm_cpu_correlations                               │    │
│  └─────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Query 3 — SAMPLE-AND-HOLD                                  │    │
│  │  window(30s, 30s) + last(value, ignoreNulls=True)           │    │
│  │  → cpu_held, memory_held, last_event_time                  │    │
│  │  → Sink: vm_metrics_held                                    │    │
│  └─────────────────────────────────────────────────────────────┘    │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼ JDBC (postgresql:42.7.2)
┌─────────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                                 │
│  TimescaleDB (PostgreSQL 15 + TimescaleDB extension)               │
│  • 4 Hypertables: vm_cpu_aggregates, vm_cpu_correlations,          │
│    vm_metrics_held, vm_raw_telemetry                               │
│  • Chunked by time column for automatic partitioning               │
│  • Connection: localhost:5432 | postgres / password                │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼ PostgreSQL datasource
┌─────────────────────────────────────────────────────────────────────┐
│                     VISUALIZATION LAYER                              │
│  Grafana (auto-provisioned)                                         │
│  • URL: http://localhost:3000                                        │
│  • Login: admin / admin                                              │
│  • Dashboard: "Cloud Telemetry Streaming Analytics"                  │
│  • 9 panels: 3 operator rows × 2 metrics + 3 health panels          │
│  • Auto-refresh: 5 seconds                                           │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Interaction Diagram

```
                    ┌─────────────┐
                    │  CSV Files  │
                    │  (.csv.gz)  │
                    └──────┬──────┘
                           │ gzip decompression
                           │ row sampling
                           │ memory synthesis
                           ▼
                    ┌─────────────┐
                    │  producer   │
                    │   (Python)  │
                    └──────┬──────┘
                           │ Kafka Producer API
                           │ JSON + gzip compression
                           ▼
              ┌────────────────────────────┐
              │      Redpanda Broker       │
              │      :19092 (external)     │
              │      :9092  (internal)    │
              └────────────────────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  Query 1 │    │  Query 2 │    │  Query 3 │
   │ Moving   │    │  Time    │    │ Sample   │
   │ Average  │    │  Join    │    │ & Hold   │
   └────┬─────┘    └────┬─────┘    └────┬─────┘
        │               │               │
        │ foreachBatch  │ foreachBatch  │ foreachBatch
        ▼               ▼               ▼
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │ vm_cpu_  │    │ vm_cpu_  │    │ vm_metrics│
   │aggregates│    │correlations│   │  _held   │
   └────┬─────┘    └────┬─────┘    └────┬─────┘
        │               │               │
        └───────────────┼───────────────┘
                        │
                        ▼
               ┌────────────────┐
               │   Grafana      │
               │  Dashboards    │
               │  (auto-provision)│
               └────────────────┘
```

---

## 4. Data Layer — Azure 2019 Public Dataset V2

### 4.1 Dataset Origin

The Azure Public Dataset V2 contains VM utilization traces from Microsoft Azure datacenters. It is publicly available at:

https://github.com/Azure/AzurePublicDataset/blob/master/AzurePublicDatasetV2.md

### 4.2 Local File Structure

```
data/azure-dataset/cpu/
├── vm_cpu_readings-file-1-of-195.csv.gz   (10,000,000 rows)
├── vm_cpu_readings-file-2-of-195.csv.gz
├── ...
└── vm_cpu_readings-file-195-of-195.csv.gz
```

Total: ~1.95 billion rows across 195 files.

### 4.3 Row Format

| Column | Type | Example |
|--------|------|---------|
| `timestamp` | int (seconds) | `0` |
| `vm_id` | string (hash) | `yNf/R3X8fyXkOJm3ihXQc...` |
| `min_cpu` | float | `19.8984` |
| `max_cpu` | float | `24.9963` |
| `avg_cpu` | float | `22.6306` |

### 4.4 Temporal Structure

- Each **tick** (unique timestamp) contains ~227,316 VM records
- Ticks are spaced 300 seconds (5 minutes) apart
- The first file contains ~44 ticks = ~220 minutes of trace time

### 4.5 Memory Data Gap

The Azure V2 CPU dataset **does not include memory readings**. For this pipeline, we synthesize memory using a physiologically realistic model:

```python
memory = 20.0 + (avg_cpu × 0.5) + uniform_noise(-3, +3)
```

This is justified because in real cloud VMs, CPU and memory utilization are positively correlated — busier VMs typically allocate more memory for their workloads.

---

## 5. Ingestion Layer — `producer.py`

### 5.1 Purpose

Reads historical CSV data and simulates a **live, real-time event stream** by:
1. Extracting a subset of VMs per tick (configurable sampling)
2. Converting trace-relative timestamps to real Unix timestamps
3. Generating synthetic memory metrics
4. Simulating sensor failures (15% missing memory)
5. Publishing JSON events to Kafka

### 5.2 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Subsample VMs** | 227k VMs/tick is too many for a student demo; 50 VMs/tick is lightweight |
| **Compress time** | Each trace tick maps to 1 real second, making windows visible in minutes |
| **Loop forever** | When the file ends, restart from the beginning for continuous streaming |
| **Missing data** | 15% random nulls in `memory` field to demonstrate Sample-and-Hold |
| **Gzip compression** | Kafka producer uses gzip to reduce network I/O |

### 5.3 Code Structure

```python
# Configurable parameters
TICKS_PER_BATCH = 1000      # Stop after N ticks (or end of files)
VMS_PER_TICK = 50           # Sample size per tick
TICKS_PER_SECOND = 2        # Real-time replay speed
MISSING_DATA_RATE = 0.15    # 15% missing memory

# Functions
get_data_files()            # Discover .csv.gz files
read_trace_batches(files)   # Generator: yield (tick, [rows])
generate_memory(cpu_avg)    # Synthetic memory formula
produce_tick(...)           # Send to Kafka with JSON payload
main()                      # Loop forever
```

### 5.4 Event Payload Schema

```json
{
  "event_time": 1751102345,
  "vm_id": "abc123...",
  "min_cpu": 10.5123,
  "max_cpu": 25.6789,
  "avg_cpu": 18.3456,
  "memory": 32.4567   // or null (15% chance)
}
```

---

## 6. Processing Layer — `processor.py`

### 6.1 Spark Session Configuration

```python
SparkSession.builder \
    .appName("AzureTelemetryStreamingPipeline") \
    .master("local[*]") \                    # Use all CPU cores
    .config("spark.driver.host", "127.0.0.1") \   # Force IPv4
    .config("spark.jars.packages",
            "spark-sql-kafka-0-10_2.13:3.5.x,"
            "postgresql:42.7.2") \              # Kafka + JDBC
    .config("spark.sql.shuffle.partitions", "15") \  # Reduce overhead
    .getOrCreate()
```

### 6.2 Kafka Source Helper

Each query gets its **own independent Kafka consumer** to avoid the known Spark limitation where a single `readStream` cannot be safely shared across multiple `writeStream` queries.

```python
def create_kafka_source_stream(alias_name):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:19092") \
        .option("subscribe", "telemetry-stream") \
        .option("startingOffsets", "latest") \   # Start from newest
        .load() \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(..., telemetry_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(from_unixtime(col("event_time")))) \
        .withWatermark("event_time", "15 minutes") \   # Late data tolerance
        .alias(alias_name)
```

### 6.3 Operator 1: Moving Average (Sliding Window)

**Spark API:**
```python
window(col("event_time"), "60 seconds", "20 seconds")
```

**What it does:**
- Creates **overlapping 60-second windows** that slide forward every 20 seconds
- Groups by `(window, vm_id)`
- Computes aggregate statistics per VM per window

**Aggregations:**
| Metric | Spark Function | Description |
|--------|---------------|-------------|
| `avg_cpu` | `avg("avg_cpu")` | Mean CPU utilization |
| `avg_memory` | `avg("memory")` | Mean memory utilization |
| `max_cpu` | `max("max_cpu")` | Peak CPU in the window |
| `min_cpu` | `min("min_cpu")` | Lowest CPU in the window |
| `record_count` | `count("*")` | Number of raw events aggregated |

**Why sliding windows?**  
Unlike tumbling windows (which are disjoint), sliding windows overlap. This smooths out short-term spikes and reveals gradual trends. A 60s window with 20s slide means each event contributes to **3 windows** on average.

**Output:** `vm_cpu_aggregates` hypertable

---

### 6.4 Operator 2: Time-Based Join (Cross-VM)

**Spark API:**
```python
left_stream.join(
    right_stream,
    expr("""
        left.vm_id != right.vm_id AND
        left.event_time >= right.event_time - INTERVAL 2 MINUTE AND
        left.event_time <= right.event_time + INTERVAL 2 MINUTE
    """),
    how="inner"
)
```

**What it does:**
- Creates **two independent Kafka source streams** (`left` and `right`)
- Joins them on the condition that:
  1. The VM IDs are different (self-join excluding same VM)
  2. The event timestamps are within ±2 minutes of each other
- This is a **stream-stream join** — both sides are unbounded streaming DataFrames

**Why two separate streams?**  
Spark's stream-stream join requires distinct execution plans for each side. Using the same `readStream` instance causes a NullPointerException in the metrics engine.

**Output columns:** `window_start`, `vm_id_a`, `vm_id_b`, `cpu_a`, `cpu_b`, `memory_a`, `memory_b`

**Output:** `vm_cpu_correlations` hypertable

**Use case:** Detect co-located VMs experiencing correlated load spikes (e.g., a noisy neighbor or a cluster-wide batch job).

---

### 6.5 Operator 3: Sample-and-Hold (Forward Fill)

**Spark API:**
```python
window(col("event_time"), "30 seconds", "30 seconds")
agg(
    last("avg_cpu", ignoreNulls=True).alias("cpu_held"),
    last("memory", ignoreNulls=True).alias("memory_held"),
    max("event_time").alias("last_event_time")
)
```

**What it does:**
- Uses **tumbling 30-second windows** (disjoint, non-overlapping)
- Within each window per VM, takes the `last()` non-null value
- If the producer dropped a memory reading (15% chance), the previous non-null value is carried forward
- The result is a **stepped time-series** with no gaps

**Why `last(..., ignoreNulls=True)`?**  
This is the canonical Spark way to implement forward-fill. It scans the window in order and returns the most recent non-null value. When combined with the producer's missing-data simulation, it demonstrates a realistic industrial pattern: IoT sensors that occasionally drop packets, and a downstream system that must hold the last known good value.

**Output:** `vm_metrics_held` hypertable

---

## 7. Storage Layer — TimescaleDB Schema

### 7.1 Why TimescaleDB?

TimescaleDB is a PostgreSQL extension that turns regular tables into **hypertables** — time-series-optimized tables with automatic partitioning (chunking) by time. This provides:

- **Automatic time-based partitioning** (no manual sharding)
- **Efficient time-range queries** (Grafana uses `time_bucket`)
- **Continuous aggregation** support (if needed in future)
- **Compression** of older chunks
- **Full SQL compatibility** (Grafana's PostgreSQL datasource works out of the box)

### 7.2 Table Schema

#### `vm_cpu_aggregates` (Moving Average Output)

```sql
CREATE TABLE vm_cpu_aggregates (
    window_start  TIMESTAMP NOT NULL,
    window_end    TIMESTAMP NOT NULL,
    vm_id         VARCHAR(255) NOT NULL,
    avg_cpu       DOUBLE PRECISION NOT NULL,
    avg_memory    DOUBLE PRECISION NOT NULL,
    max_cpu       DOUBLE PRECISION,
    min_cpu       DOUBLE PRECISION,
    record_count  BIGINT NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);
SELECT create_hypertable('vm_cpu_aggregates', 'window_start');
```

#### `vm_cpu_correlations` (Time-Based Join Output)

```sql
CREATE TABLE vm_cpu_correlations (
    window_start  TIMESTAMP NOT NULL,
    vm_id_a       VARCHAR(255) NOT NULL,
    vm_id_b       VARCHAR(255) NOT NULL,
    cpu_a         DOUBLE PRECISION,
    cpu_b         DOUBLE PRECISION,
    memory_a      DOUBLE PRECISION,
    memory_b      DOUBLE PRECISION,
    PRIMARY KEY (window_start, vm_id_a, vm_id_b)
);
SELECT create_hypertable('vm_cpu_correlations', 'window_start');
```

#### `vm_metrics_held` (Sample-and-Hold Output)

```sql
CREATE TABLE vm_metrics_held (
    window_start     TIMESTAMP NOT NULL,
    window_end       TIMESTAMP NOT NULL,
    vm_id            VARCHAR(255) NOT NULL,
    cpu_held         DOUBLE PRECISION NOT NULL,
    memory_held      DOUBLE PRECISION NOT NULL,
    last_event_time  TIMESTAMP NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);
SELECT create_hypertable('vm_metrics_held', 'window_start');
```

### 7.3 Auto-Initialization

The schema is mounted into the TimescaleDB container via Docker's initdb mechanism:

```yaml
volumes:
  - ./table_creation_query.pgsql:/docker-entrypoint-initdb.d/01-init.sql:ro
```

This script runs **automatically on first container creation** — no manual SQL execution needed.

---

## 8. Visualization Layer — Grafana

### 8.1 Auto-Provisioning

Grafana is configured via **provisioning files** (no manual UI setup):

- **Datasource:** `Infra/grafana/provisioning/datasources/postgres.yml` configures the PostgreSQL/TimescaleDB connection
- **Dashboard provider:** `Infra/grafana/provisioning/dashboards/dashboard.yml` points to the dashboard JSON
- **Dashboard:** `Infra/grafana/dashboards/telemetry-dashboard.json` contains the complete panel layout

### 8.2 Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│  Operator 1 — Moving Average (Sliding Window)                         │
├────────────────────────────┬────────────────────────────────────────┤
│  CPU Moving Average by VM  │  Memory Moving Average by VM           │
│  [Line chart, multi-series]│  [Line chart, multi-series]            │
└────────────────────────────┴────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│  Operator 2 — Time-Based Join (Cross-VM Event Window)               │
├────────────────────────────┬────────────────────────────────────────┤
│  Cross-VM CPU Correlation  │  Cross-VM Memory Correlation          │
│  [Line chart, paired VMs]  │  [Line chart, paired VMs]              │
└────────────────────────────┴────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│  Operator 3 — Sample-and-Hold (Forward Fill)                        │
├────────────────────────────┬────────────────────────────────────────┤
│  CPU Sample-and-Hold       │  Memory Sample-and-Hold              │
│  [Step plot, interpolated] │  [Step plot, interpolated]             │
└────────────────────────────┴────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│  Pipeline Health — Aggregated Statistics                            │
├────────────────┬──────────────────────────┬─────────────────────────┤
│  Records/Window│  Top 10 VMs by Avg CPU   │  Latest VM Summary     │
│  [Bar chart]   │  [Bar gauge]             │  [Table]               │
└────────────────┴──────────────────────────┴─────────────────────────┘
```

### 8.3 Key Grafana Features

- **Time range:** `Last 5 minutes` (adjustable)
- **Auto-refresh:** `5 seconds`
- **Time bucketing:** `time_bucket('10 seconds', ...)` — groups data into 10-second buckets for smooth rendering
- **Multi-series:** Each VM is a separate colored line (legend table on the right)
- **Step plot:** Sample-and-Hold panels use `lineInterpolation: "stepAfter"` to visualize the held-value behavior

---

## 9. Infrastructure Layer

### 9.1 Docker Compose Services

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v23.2.1
    ports: ["19092:19092"]
    # No ZooKeeper needed — Redpanda is self-healing

  timescaledb:
    image: timescale/timescaledb:latest-pg15
    ports: ["5432:5432"]
    volumes:
      - timescale_data:/var/lib/postgresql/data
      - ./table_creation_query.pgsql:/docker-entrypoint-initdb.d/01-init.sql:ro

  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    depends_on:
      timescaledb:
        condition: service_healthy
```

### 9.2 Networking

A dedicated Docker bridge network (`telemetry-net`) is used so containers can communicate by service name:
- Grafana connects to `timescaledb:5432`
- Spark processor connects to `localhost:19092` (external port mapped to Redpanda)
- Spark processor connects to `localhost:5432` (external port mapped to TimescaleDB)

### 9.3 Health Checks

Both Redpanda and TimescaleDB have Docker health checks:
- Redpanda: `rpk cluster info`
- TimescaleDB: `pg_isready -U postgres -d telemetry_db`

Grafana uses `depends_on` with `condition: service_healthy` to ensure the database is ready before starting.

---

## 10. Code Walkthrough by File

### 10.1 `producer.py` (172 lines)

| Section | Lines | Purpose |
|---------|-------|---------|
| Configuration | 10–22 | Kafka brokers, topic, sampling params, missing data rate |
| Kafka Producer | 24–33 | `KafkaProducer` with gzip compression and batching |
| `get_data_files()` | 36–42 | Discover `.csv.gz` files via `glob` |
| `generate_memory()` | 45–53 | Synthetic memory from CPU + noise |
| `read_trace_batches()` | 56–107 | Generator that groups CSV rows by trace timestamp and subsamples VMs |
| `produce_tick()` | 110–135 | Convert trace time → real time, optionally null-out memory, send to Kafka |
| `main()` | 137–163 | Event loop: read batches, produce, sleep, restart |

### 10.2 `processor.py` (227 lines)

| Section | Lines | Purpose |
|---------|-------|---------|
| Spark Init | 13–37 | Session builder with IPv4 overrides, Kafka + JDBC packages |
| Log Mute | 39–48 | Suppress Windows-specific HDFS/checksum warnings |
| Schema | 53–60 | `StructType` matching JSON payload from producer |
| `create_kafka_source_stream()` | 63–80 | Reusable helper for independent Kafka consumers |
| `write_to_postgres()` | 92–112 | ForeachBatch sink with JDBC append mode |
| Query 1 (Moving Avg) | 115–150 | Sliding window aggregation → `vm_cpu_aggregates` |
| Query 2 (Time Join) | 153–184 | Stream-stream self-join → `vm_cpu_correlations` |
| Query 3 (Sample-Hold) | 187–217 | Tumbling window + last() → `vm_metrics_held` |
| Keep Alive | 220–227 | `awaitAnyTermination()` blocks until any query fails |

### 10.3 `Infra/docker-compose.yml` (39 lines)

| Service | Key Config |
|---------|-----------|
| Redpanda | `smp 1`, `overprovisioned`, external port 19092 |
| TimescaleDB | `latest-pg15`, initdb mount, health check |
| Grafana | Admin password `admin`, auto-provisioning mounts, depends_on DB |

### 10.4 `Infra/table_creation_query.pgsql` (68 lines)

Creates 4 hypertables with appropriate primary keys and `create_hypertable()` calls.

### 10.5 `run_pipeline.ps1` (156 lines)

PowerShell orchestration script that:
1. Checks Java, Python, Docker
2. Creates venv and installs dependencies
3. Starts `docker compose`
4. Waits for DB health (up to 60 seconds)
5. Launches processor and producer as background jobs
6. Opens Grafana in browser
7. Monitors job health

---

## 11. How to Run

### Prerequisites
1. Java 17 installed with `JAVA_HOME` set
2. Python 3.11+ installed
3. Docker Desktop running
4. `HADOOP_HOME=C:\hadoop` with `winutils.exe` in `C:\hadoop\bin\`

### Quick Start (One Command)

```powershell
cd Streaming-Analytics
.\run_pipeline.ps1
```

### Manual Start (Two Terminals)

**Terminal 1:**
```powershell
cd Infra
docker compose up -d
cd ..
.\venv\Scripts\activate
python processor.py
```

**Terminal 2:**
```powershell
.\venv\Scripts\activate
python producer.py
```

**Browser:**
```
http://localhost:3000/d/telemetry-streaming-01
```

---

## 12. Troubleshooting Guide

| Symptom | Cause | Fix |
|---------|-------|-----|
| `JAVA_GATEWAY_EXITED` | Wrong Java version or missing `JAVA_HOME` | Install Java 17, set `JAVA_HOME` |
| `HADOOP_HOME` error | Missing `winutils.exe` | Download to `C:\hadoop\bin\winutils.exe` |
| `ClassNotFoundException` for Kafka | Wrong Scala version in jar spec | Change `_2.13` to `_2.12` in processor.py |
| No dashboard data | Producer not running | Start `python producer.py` in a second terminal |
| DB connection refused | TimescaleDB not ready | Wait 15s after `docker compose up` |
| Checkpoint conflict | Processor code changed | Delete `C:\hadoop\checkpoints\telemetry_pipeline` |
| Grafana login fails | First-time setup | Default is `admin / admin` |

---

## 13. Evaluation Against Project Description

### 13.1 Requirement Checklist

| # | Requirement | Status | Evidence |
|---|-------------|--------|----------|
| 1 | **Apache Spark Streaming** | ✅ | `processor.py` uses `SparkSession` + `readStream` + `writeStream` with 3 parallel queries |
| 2 | **Moving Average** | ✅ | `window(60s, 20s)` with `avg("avg_cpu")`, `avg("memory")` in Query 1 |
| 3 | **Time-Based Join** | ✅ | `leftStream.join(rightStream, ±2 min event_time)` in Query 2 |
| 4 | **Sample-and-Hold** | ✅ | `last(value, ignoreNulls=True)` over 30s tumbling windows in Query 3 |
| 5 | **Event-time simulation** | ✅ | Producer assigns real timestamps; processor uses `withWatermark` |
| 6 | **Windowed operations** | ✅ | Sliding windows (Query 1), event-time join windows (Query 2), tumbling windows (Query 3) |
| 7 | **Aggregated results** | ✅ | All three queries write aggregated DataFrames to TimescaleDB |
| 8 | **Azure V2 Dataset** | ✅ | Reads `vm_cpu_readings-file-*.csv.gz` from local `data/` directory |
| 9 | **Grafana visualization** | ✅ | Auto-provisioned 9-panel dashboard with live refresh at `localhost:3000` |

### 13.2 Gaps Acknowledged

| Gap | Explanation | Mitigation |
|-----|-------------|------------|
| **No native memory dataset** | Azure V2 CPU files do not include memory | Synthetic memory generated with realistic CPU correlation |
| **Subset of VMs** | Full dataset has 227k VMs per tick | Producer samples 50 VMs/tick for demo feasibility |
| **No automated retention** | Original README mentioned 15-min retention | TimescaleDB hypertables support retention policies but this is not configured to keep data visible in Grafana |
| **Local-only** | No cloud deployment | Fully documented Windows local setup; portable to Linux by changing paths |

---

## 14. Conclusion

This project delivers a **complete, working streaming analytics pipeline** that satisfies every requirement in the original description:

1. **Apache Spark Structured Streaming** processes real-time data from a Kafka-compatible broker.
2. **Three streaming operators** (Moving Average, Time-Based Join, Sample-and-Hold) are implemented as separate, parallel Spark queries with independent checkpoint locations.
3. **Event-time semantics** are correctly handled via watermarks and timestamp-based windows.
4. **TimescaleDB** stores the aggregated outputs in time-optimized hypertables.
5. **Grafana** auto-provisions with a datasource and a 9-panel dashboard that visualizes all three operators plus pipeline health metrics.

The entire system is **zero-cloud**, runs on **Windows**, and can be started with a **single PowerShell command** (`./run_pipeline.ps1`).

---

*Report generated for the Big Data Streaming Analytics course — Development Branch.*
