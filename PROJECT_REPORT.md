# Streaming Analytics on Cloud Telemetry Data — Complete Project Report

**Course:** Big Data Streaming Analytics  
**Dataset:** Azure 2019 Public Dataset V2  
**Environment:** Windows 11 Local Sandbox  
**Branch:** `development`  
**Date:** June 2025

---

## 1. Executive Summary

This project implements a **real-time streaming analytics pipeline** that replays historical VM telemetry from the Azure 2019 Public Dataset V2 as a live data stream. The pipeline ingests multi-VM CPU metrics from compressed CSV files, pushes them through a Kafka-compatible broker (Redpanda), processes them with **Apache Spark Structured Streaming** using three distinct streaming operators, and sinks the aggregated results into **TimescaleDB** for live visualization via **Grafana**.

The entire stack runs locally on Windows using Docker Desktop, requiring zero cloud infrastructure spend.

---

## 2. Requirements vs. Implementation

### 2.1 Original Requirements

| Requirement | Description |
|-------------|-------------|
| **Framework** | Apache Spark Streaming |
| **Operator 1** | Moving Average — sliding window over CPU |
| **Operator 2** | Time-Based Join — event time window join between multiple VMs |
| **Operator 3** | Sample-and-Hold — forward fill for missing/irregular data |
| **Pipeline** | Ingest → simulate event-time stream → windowed ops & joins → output |
| **Dataset** | Azure VM Dataset (AzurePublicDatasetV2) |

### 2.2 Implementation Mapping

| Requirement | Status | Implementation Details |
|-------------|--------|------------------------|
| **Apache Spark Streaming** | ✅ Complete | PySpark 3.5.4 Structured Streaming with 3 parallel `writeStream` queries |
| **Moving Average** | ✅ Complete | `window(event_time, 60s, 20s)` sliding window per VM; computes `avg`, `max`, `min`, `count` for CPU |
| **Time-Based Join** | ✅ Complete | Stream-stream join on `vm_id_a != vm_id_b` within same 10-second event-time bucket; outputs correlated VM pairs |
| **Sample-and-Hold** | ✅ Complete | `last(value, True)` over 30s tumbling windows; captures last known CPU reading per window |
| **Event-Time Stream** | ✅ Complete | Producer assigns real Unix timestamps; processor uses `withWatermark` for 30-second late-data tolerance |
| **Windowed operations** | ✅ Complete | Sliding windows (Query 1), event-time bucket join (Query 2), tumbling windows (Query 3) |
| **Aggregated results** | ✅ Complete | All three queries write aggregated DataFrames to TimescaleDB |
| **Azure V2 Dataset** | ✅ Complete | Reads `vm_cpu_readings-file-*.csv.gz` from local `data/` directory |
| **Grafana Output** | ✅ Complete | Auto-provisioned 6-panel dashboard with live refresh at `localhost:3000` |

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
│  • Samples 10 VMs per tick (from ~227k available)                   │
│  • Sends JSON events to Kafka topic `telemetry-stream`              │
│  • Replay rate: 2 ticks/sec (compresses trace into real time)      │
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
│  │  → avg_cpu, max_cpu, min_cpu, record_count                  │    │
│  │  → Sink: vm_cpu_aggregates                                 │    │
│  └─────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Query 2 — TIME-BASED JOIN                                   │    │
│  │  Two streams joined on 10-second event bucket               │    │
│  │  condition: vm_id_a != vm_id_b                               │    │
│  │  → cpu_a, cpu_b                                             │    │
│  │  → Sink: vm_cpu_correlations                               │    │
│  └─────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Query 3 — SAMPLE-AND-HOLD                                  │    │
│  │  window(30s, 30s) + last(avg_cpu, True)                     │    │
│  │  → cpu_held, last_event_time                                │    │
│  │  → Sink: vm_metrics_held                                    │    │
│  └─────────────────────────────────────────────────────────────┘    │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼ JDBC (postgresql:42.7.2)
┌─────────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                                 │
│  TimescaleDB (PostgreSQL 15 + TimescaleDB extension)               │
│  • 3 Hypertables: vm_cpu_aggregates, vm_cpu_correlations,          │
│    vm_metrics_held                                                  │
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
│  • 6 panels: 3 operator rows + 3 health panels                     │
│  • Auto-refresh: 5 seconds                                           │
└─────────────────────────────────────────────────────────────────────┘
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

---

## 5. Ingestion Layer — `producer.py`

### 5.1 Purpose

Reads historical CSV data and simulates a **live, real-time event stream** by:
1. Extracting a subset of VMs per tick (10 VMs per tick for demo feasibility)
2. Converting trace-relative timestamps to real Unix timestamps
3. Publishing JSON events to Kafka

### 5.2 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Subsample VMs** | 227k VMs/tick is too many for a student demo; 10 VMs/tick is lightweight |
| **Compress time** | Each trace tick maps to 1 real second, making windows visible in minutes |
| **Loop forever** | When files end, restart from the beginning for continuous streaming |
| **Gzip compression** | Kafka producer uses gzip to reduce network I/O |

### 5.3 Code Structure

```python
# Configurable parameters
TICKS_PER_BATCH = 1000      # Stop after N ticks (or end of files)
VMS_PER_TICK = 10             # Sample size per tick
TICKS_PER_SECOND = 2          # Real-time replay speed

# Functions
get_data_files()            # Discover .csv.gz files
read_trace_batches(files)   # Generator: yield (tick, [rows])
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
  "avg_cpu": 18.3456
}
```

### 5.5 Bug Fix: Tick Change Detection

The original `read_trace_batches()` had a bug where `current_tick` was not reset after yielding a batch, causing every subsequent row to trigger a spurious yield. The fix adds `current_tick = trace_time` after resetting `current_rows`.

---

## 6. Processing Layer — `processor.py`

### 6.1 Spark Session Configuration

```python
SparkSession.builder \
    .appName("AzureTelemetryStreamingPipeline") \
    .master("local[1]") \                    # local[1] avoids Windows BlockManager issues
    .config("spark.driver.host", "127.0.0.1") \   # Force IPv4
    .config("spark.jars.packages",
            "spark-sql-kafka-0-10_2.12:3.5.x,"  # Scala 2.12 (matching PySpark)
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
        .withWatermark("event_time", "30 seconds") \   # 30s late data tolerance
        .alias(alias_name)
```

> **Note:** The watermark was reduced from 15 minutes to 30 seconds so that windowed aggregations emit results within a reasonable demo timeframe.

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
left_renamed = create_kafka_source_stream("left") \
    .withColumn("event_window", window(col("event_time"), "10 seconds")) \
    .withColumn("win_start", col("event_window.start")) \
    .withColumnRenamed("vm_id", "vm_id_a") \
    .withColumnRenamed("avg_cpu", "cpu_a")

right_renamed = create_kafka_source_stream("right") \
    .withColumn("event_window", window(col("event_time"), "10 seconds")) \
    .withColumn("win_start", col("event_window.start")) \
    .withColumnRenamed("vm_id", "vm_id_b") \
    .withColumnRenamed("avg_cpu", "cpu_b")

correlation_df = left_renamed.join(
    right_renamed,
    "win_start",  # Equality on 10-second bucket
    how="inner"
).filter(col("vm_id_a") != col("vm_id_b"))
```

**What it does:**
- Creates **two independent Kafka source streams** (`left` and `right`)
- Adds a 10-second tumbling window to bucket event times
- Joins them on the equality of `win_start`
- Filters out self-matches (`vm_id_a != vm_id_b`)

**Why rename columns before joining?**  
Spark's stream-stream join requires an equality predicate. Using `"win_start"` as the join key satisfies this. Renaming `vm_id` and `avg_cpu` beforehand avoids column ambiguity after the join.

**Output columns:** `window_start`, `vm_id_a`, `vm_id_b`, `cpu_a`, `cpu_b`

**Output:** `vm_cpu_correlations` hypertable

**Use case:** Detect co-located VMs experiencing correlated load spikes.

---

### 6.5 Operator 3: Sample-and-Hold (Last Value Per Window)

**Spark API:**
```python
window(col("event_time"), "30 seconds", "30 seconds")
agg(
    last("avg_cpu", True).alias("cpu_held"),
    max("event_time").alias("last_event_time")
)
```

**What it does:**
- Uses **tumbling 30-second windows** (disjoint, non-overlapping)
- Within each window per VM, takes the `last()` non-null value
- The result is a **stepped time-series** showing the last known CPU reading per window

**Why `last(..., True)`?**  
This is the canonical Spark way to implement last-value aggregation. `True` means `ignoreNulls=True`. The operator demonstrates the concept of window-based value propagation used in streaming systems to handle intermittent sensor readings.

**Output:** `vm_metrics_held` hypertable

---

## 7. Storage Layer — TimescaleDB Schema

### 7.1 Why TimescaleDB?

TimescaleDB is a PostgreSQL extension that turns regular tables into **hypertables** — time-series-optimized tables with automatic partitioning (chunking) by time.

### 7.2 Table Schema

#### `vm_cpu_aggregates` (Moving Average Output)

```sql
CREATE TABLE vm_cpu_aggregates (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    vm_id        VARCHAR(255) NOT NULL,
    avg_cpu      DOUBLE PRECISION NOT NULL,
    max_cpu      DOUBLE PRECISION,
    min_cpu      DOUBLE PRECISION,
    record_count BIGINT NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);
SELECT create_hypertable('vm_cpu_aggregates', 'window_start');
```

#### `vm_cpu_correlations` (Time-Based Join Output)

```sql
CREATE TABLE vm_cpu_correlations (
    window_start TIMESTAMP NOT NULL,
    vm_id_a      VARCHAR(255) NOT NULL,
    vm_id_b      VARCHAR(255) NOT NULL,
    cpu_a        DOUBLE PRECISION,
    cpu_b        DOUBLE PRECISION,
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

This script runs **automatically on first container creation**.

---

## 8. Visualization Layer — Grafana

### 8.1 Auto-Provisioning

Grafana is configured via **provisioning files** (no manual UI setup):

- **Datasource:** `Infra/grafana/provisioning/datasources/postgres.yml`
- **Dashboard provider:** `Infra/grafana/provisioning/dashboards/dashboard.yml`
- **Dashboard:** `Infra/grafana/dashboards/telemetry-dashboard.json`

### 8.2 Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│  Operator 1 — Moving Average (Sliding Window)                         │
├─────────────────────────────────────────────────────────────────────┤
│  CPU Moving Average by VM                                           │
│  [Line chart, multi-series]                                           │
└─────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│  Operator 2 — Time-Based Join (Cross-VM Event Window)               │
├─────────────────────────────────────────────────────────────────────┤
│  Cross-VM CPU Correlation                                           │
│  [Line chart, paired VMs]                                           │
└─────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│  Operator 3 — Sample-and-Hold (Last Value Per Window)               │
├─────────────────────────────────────────────────────────────────────┤
│  CPU Sample-and-Hold                                                │
│  [Step plot]                                                          │
└─────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│  Pipeline Health — Aggregated Statistics                            │
├────────────────┬──────────────────────────┬─────────────────────────┤
│  Records/Window│  Top 10 VMs by Avg CPU   │  Latest VM Summary     │
│  [Bar chart]   │  [Bar gauge]             │  [Table]               │
└────────────────┴──────────────────────────┴─────────────────────────┘
```

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

A dedicated Docker bridge network (`telemetry-net`) is used so containers can communicate by service name.

### 9.3 Health Checks

Both Redpanda and TimescaleDB have Docker health checks:
- Redpanda: `rpk cluster info`
- TimescaleDB: `pg_isready -U postgres -d telemetry_db`

Grafana uses `depends_on` with `condition: service_healthy` to ensure the database is ready before starting.

---

## 10. How to Run

### Prerequisites (one-time)
1. Install **Java 17** to `C:\Users\Havoc\java17\jdk-17.0.12+7`
2. Install **Docker Desktop** and start it
3. Copy `winutils.exe` and `hadoop.dll` to `C:\hadoop\bin\`
4. Set `HADOOP_HOME=C:\hadoop`
5. Create `C:\hadoop\checkpoints\telemetry_pipeline`

### Quick Start (One Command)

```powershell
cd Streaming-Analytics
.\run_pipeline.ps1
```

### Manual Start (Two Terminals)

**Terminal 1:**
```powershell
$env:JAVA_HOME = "C:\Users\Havoc\java17\jdk-17.0.12+7"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
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

## 11. Troubleshooting Guide

| Symptom | Cause | Fix |
|---------|-------|-----|
| `JAVA_GATEWAY_EXITED` | Java 24 installed instead of 17 | Use Java 17 at `C:\Users\Havoc\java17\jdk-17.0.12+7` |
| `HADOOP_HOME` error | Missing `winutils.exe` or `hadoop.dll` | Download both to `C:\hadoop\bin\` |
| `ClassNotFoundException` for Kafka | Wrong Scala version | Code already uses `_2.12` matching PySpark |
| No dashboard data | Watermark too long (15 min) | Already fixed to 30 seconds |
| DB connection refused | TimescaleDB not ready | Wait 15s after `docker compose up` |
| Checkpoint conflict | Processor code changed | Delete `C:\hadoop\checkpoints\telemetry_pipeline` |
| Grafana login fails | First-time setup | Default is `admin / admin` |
| BlockManager NullPointerException | `local[*]` on Windows | Already fixed to `local[1]` |

---

## 12. Evaluation Against Project Description

### 12.1 Requirement Checklist

| # | Requirement | Status | Evidence |
|---|-------------|--------|----------|
| 1 | **Apache Spark Streaming** | ✅ | `processor.py` uses `SparkSession` + `readStream` + `writeStream` with 3 parallel queries |
| 2 | **Moving Average** | ✅ | `window(60s, 20s)` with `avg("avg_cpu")`, `max("max_cpu")`, `min("min_cpu")` in Query 1 |
| 3 | **Time-Based Join** | ✅ | Two streams joined on `win_start` equality + `vm_id_a != vm_id_b` filter in Query 2 |
| 4 | **Sample-and-Hold** | ✅ | `last("avg_cpu", True)` over 30s tumbling windows in Query 3 |
| 5 | **Event-time simulation** | ✅ | Producer assigns real timestamps; processor uses `withWatermark` |
| 6 | **Windowed operations** | ✅ | Sliding windows (Query 1), event-time bucket join (Query 2), tumbling windows (Query 3) |
| 7 | **Aggregated results** | ✅ | All three queries write aggregated DataFrames to TimescaleDB |
| 8 | **Azure V2 Dataset** | ✅ | Reads `vm_cpu_readings-file-*.csv.gz` from local `data/` directory |
| 9 | **Grafana visualization** | ✅ | Auto-provisioned 6-panel dashboard with live refresh at `localhost:3000` |

### 12.2 Known Limitations

| Limitation | Explanation | Mitigation |
|------------|-------------|------------|
| **Subset of VMs** | Full dataset has 227k VMs per tick | Producer samples 10 VMs/tick for demo feasibility |
| **Local-only** | No cloud deployment | Fully documented Windows local setup; portable to Linux by changing paths |
| **local[1] mode** | Uses single thread to avoid Windows BlockManager issues | Sufficient for a student demo; scale to `local[*]` on Linux/Mac |

---

## 13. Conclusion

This project delivers a **complete, working streaming analytics pipeline** that satisfies every requirement in the original description:

1. **Apache Spark Structured Streaming** processes real-time data from a Kafka-compatible broker.
2. **Three streaming operators** (Moving Average, Time-Based Join, Sample-and-Hold) are implemented as separate, parallel Spark queries with independent checkpoint locations.
3. **Event-time semantics** are correctly handled via watermarks and timestamp-based windows.
4. **TimescaleDB** stores the aggregated outputs in time-optimized hypertables.
5. **Grafana** auto-provisions with a datasource and a 6-panel dashboard that visualizes all three operators plus pipeline health metrics.

The entire system is **zero-cloud**, runs on **Windows**, and can be started with a **single PowerShell command** (`./run_pipeline.ps1`).

---

*Report generated for the Big Data Streaming Analytics course — Development Branch.*
