# Streaming Analytics on Cloud Telemetry Data — Complete Project Report

**Course:** Big Data Streaming Analytics  
**Dataset:** Azure 2019 Public Dataset V2  
**Environment:** Windows 11 Local Sandbox  
**Branch:** `development`  
**Date:** July 2026

---

## 1. Executive Summary

This project implements a **real-time streaming analytics pipeline** that replays historical VM telemetry from the Azure 2019 Public Dataset V2 as a live data stream. The pipeline ingests multi-VM CPU metrics from compressed CSV files, pushes them through a Kafka-compatible broker (Redpanda), processes them with **Apache Spark Structured Streaming** using three distinct streaming operators, and sinks the aggregated results into **TimescaleDB** for live visualization via **Grafana**.

The entire stack runs locally on Windows using Docker Desktop, requiring zero cloud infrastructure spend.

---

## 2. Requirements vs. Implementation

### 2.1 Original Requirements

| Requirement    | Description                                                         |
| -------------- | ------------------------------------------------------------------- |
| **Framework**  | Apache Spark Streaming                                              |
| **Operator 1** | Moving Average — sliding window over CPU                            |
| **Operator 2** | Time-Based Join — event time window join between multiple VMs       |
| **Operator 3** | Sample-and-Hold — forward fill for missing/irregular data           |
| **Pipeline**   | Ingest → simulate event-time stream → windowed ops & joins → output |
| **Dataset**    | Azure VM Dataset (AzurePublicDatasetV2)                             |

### 2.2 Implementation Mapping

| Requirement                | Status      | Implementation Details                                                                                                                                          |
| -------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Apache Spark Streaming** | ✅ Complete | PySpark 3.5.x Structured Streaming with 3 parallel `writeStream` queries.                                                                                       |
| **Moving Average**         | ✅ Complete | `window(event_time, 60s, 20s)` sliding window per VM; computes `avg`, `max`, `min`, `count` for CPU.                                                            |
| **Time-Based Join**        | ✅ Complete | Stream-stream join with explicit dataframe qualifiers and event-time constraints inside a 10s window to prevent state drops and ambiguous reference exceptions. |
| **Sample-and-Hold**        | ✅ Complete | Robust 5-minute sliding window with a 10-second slide utilizing positional `last(col, True)` to hold and propagate last known CPU states.                       |
| **Event-Time Stream**      | ✅ Complete | Producer assigns real Unix timestamps; processor uses `withWatermark` for 30-second late-data tolerance.                                                        |
| **Windowed operations**    | ✅ Complete | Sliding windows (Query 1), event-time bucket join (Query 2), sliding state-propagation windows (Query 3).                                                       |
| **Aggregated results**     | ✅ Complete | All three queries write aggregated DataFrames to TimescaleDB.                                                                                                   |
| **Azure V2 Dataset**       | ✅ Complete | Reads `vm_cpu_readings-file-*.csv.gz` from local `data/` directory[cite: 3, 4].                                                                                 |
| **Grafana Output**         | ✅ Complete | Auto-provisioned 6-panel dashboard with live refresh at `localhost:3000`.                                                                                       |

---

## 3. Architecture Overview

### 3.1 High-Level Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                DATA LAYER                                │
│  Azure V2 CPU Readings (.csv.gz)                                         │
│  Columns: timestamp | vm_id | min_cpu | max_cpu | avg_cpu                │
│  ~227,000 VMs per 5-min tick | 10M rows per file                         │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼ producer.py
┌──────────────────────────────────────────────────────────────────────────┐
│                             INGESTION LAYER                              │
│  • Reads multiple CSV files                                              │
│  • Samples 50 VMs per tick (scaled up from 10 to ensure join density)    │
│  • Sends JSON events to Kafka topic telemetry-stream                     │
│  • Replay rate: 1 tick/sec (optimized for stable window processing)      │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼ Redpanda (Kafka API on :19092)
┌──────────────────────────────────────────────────────────────────────────┐
│                             MESSAGE BROKER                               │
│  • Redpanda v23.2.1 — ultra-lightweight Kafka-compatible broker          │
│  • Topic: telemetry-stream                                               │
│  • External listener: localhost:19092                                    │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼ processor.py (3 parallel queries)
┌──────────────────────────────────────────────────────────────────────────┐
│                            PROCESSING LAYER                              │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                     Query 1 — MOVING AVERAGE                       │  │
│  │  window(60s, 20s) + groupBy(vm_id)                                 │  │
│  │  → avg_cpu, max_cpu, min_cpu, record_count                         │  │
│  │  → Sink: vm_cpu_aggregates                                         │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                     Query 2 — TIME-BASED JOIN                      │  │
│  │  Two streams joined with explicit dataframe references              │  │
│  │  condition: event-time bounds and matching prefixes                │  │
│  │  → cpu_a, cpu_b                                                    │  │
│  │  → Sink: vm_cpu_correlations                                       │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                     Query 3 — SAMPLE-AND-HOLD                      │  │
│  │  window(5m, 10s) + last(avg_cpu, True)                             │  │
│  │  → cpu_held, last_event_time                                       │  │
│  │  → Sink: vm_metrics_held                                           │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼ JDBC (postgresql:42.7.2)
┌──────────────────────────────────────────────────────────────────────────┐
│                              STORAGE LAYER                               │
│  TimescaleDB (PostgreSQL 15 + TimescaleDB extension)                     │
│  • 3 Hypertables: vm_cpu_aggregates, vm_cpu_correlations,                │
│    vm_metrics_held                                                       │
│  • Chunked by time column for automatic partitioning                     │
│  • Connection: localhost:5432 | postgres / password                      │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼ PostgreSQL datasource
┌──────────────────────────────────────────────────────────────────────────┐
│                           VISUALIZATION LAYER                            │
│  Grafana (auto-provisioned)                                              │
│  • URL: http://localhost:3000                                            │
│  • Login: admin / admin                                                  │
│  • Dashboard: "Cloud Telemetry Streaming Analytics"                      │
│  • 6 panels: 3 operator rows + 3 health panels                           │
│  • Auto-refresh: 5 seconds                                               │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Layer — Azure 2019 Public Dataset V2

### 4.1 Dataset Origin

The Azure Public Dataset V2 contains VM utilization traces from Microsoft Azure datacenters. It is publicly available at:

https://github.com/Azure/AzurePublicDataset/blob/master/AzurePublicDatasetV2.md

### 4.2 Local File Structure

data/azure-dataset/cpu/├── vm_cpu_readings-file-1-of-195.csv.gz (10,000,000 rows)├── vm_cpu_readings-file-2-of-195.csv.gz├── ...└── vm_cpu_readings-file-195-of-195.csv.gz
Total: ~1.95 billion rows across 195 files[cite: 3, 4].

### 4.3 Row Format

| Column      | Type          | Example                                |
| ----------- | ------------- | -------------------------------------- |
| `timestamp` | int (seconds) | `0`[cite: 3, 4]                        |
| `vm_id`     | string (hash) | `yNf/R3X8fyXkOJm3ihXQc...`[cite: 3, 4] |
| `min_cpu`   | float         | `19.8984`[cite: 3, 4]                  |
| `max_cpu`   | float         | `24.9963`[cite: 3, 4]                  |
| `avg_cpu`   | float         | `22.6306`[cite: 3, 4]                  |

### 4.4 Temporal Structure

- Each **tick** (unique timestamp) contains ~227,316 VM records
- Ticks are spaced 300 seconds (5 minutes) apart
- The first file contains ~44 ticks = ~220 minutes of trace time[cite: 4]

---

## 5. Ingestion Layer — `producer.py`

### 5.1 Purpose

Reads historical CSV data and simulates a **live, real-time event stream** by:

1. Extracting a subset of VMs per tick[cite: 3, 4].
2. Converting trace-relative timestamps to real Unix timestamps[cite: 3, 4].
3. Publishing JSON events to Kafka[cite: 3, 4].

### 5.2 Key Design Decisions

| Decision                  | Rationale                                                                                                                               |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **Increased Sample Size** | `VMS_PER_TICK = 50` ensures that there is ample overlapping metric data to satisfy joins and avoid sparse database entries.             |
| **1 Tick/Second Speed**   | Paced real-time speed allows Spark Streaming queries to process large window states without bottlenecking execution resources[cite: 3]. |
| **Loop forever**          | When files end, restart from the beginning for continuous streaming[cite: 3, 4].                                                        |
| **Gzip compression**      | Kafka producer uses gzip to reduce network I/O[cite: 3, 4].                                                                             |

### 5.3 Code Structure

```python
# Configurable parameters
TICKS_PER_BATCH = 1500      # Optimized to capture ~125 trace hours
VMS_PER_TICK = 50           # Increased density[cite: 3]
TICKS_PER_SECOND = 1        # Adjusted for ingestion consistency[cite: 3]

# Functions
get_data_files()            # Discover .csv.gz files[cite: 3, 4]
read_trace_batches(files)   # Generator: yield (tick, [rows])[cite: 3, 4]
produce_tick(...)           # Send to Kafka with JSON payload[cite: 3, 4]
main()                      # Loop forever[cite: 3, 4]
5.4 Event Payload SchemaJSON{
  "event_time": 1751102345,
  "vm_id": "abc123...",
  "min_cpu": 10.5123,
  "max_cpu": 25.6789,
  "avg_cpu": 18.3456
}
6. Processing Layer — processor.py6.1 Spark Session ConfigurationPythonSparkSession.builder \
    .appName("AzureTelemetryStreamingPipeline") \
    .master("local[1]") \                    # local[1] avoids Windows BlockManager issues
    .config("spark.driver.host", "127.0.0.1") \   # Force IPv4
    .config("spark.jars.packages",
            "spark-sql-kafka-0-10_2.12:3.5.x,"  # Scala 2.12 (matching PySpark)
            "postgresql:42.7.2") \              # Kafka + JDBC
    .config("spark.sql.shuffle.partitions", "15") \  # Reduce overhead
    .getOrCreate()
6.2 Kafka Source HelperEach query gets its own independent Kafka consumer to avoid the known Spark limitation where a single readStream cannot be safely shared across multiple writeStream queries.  Pythondef create_kafka_source_stream(alias_name):
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
6.3 Operator 1: Moving Average (Sliding Window)Spark API:Pythonwindow(col("event_time"), "60 seconds", "20 seconds")
What it does:Creates overlapping 60-second windows that slide forward every 20 seconds.  Groups by (window, vm_id).  Computes aggregate statistics per VM per window.  Aggregations:MetricSpark FunctionDescriptionavg_cpuavg("avg_cpu")Mean CPU utilization.  max_cpumax("max_cpu")Peak CPU in the window.  min_cpumin("min_cpu")Lowest CPU in the window.  record_countcount("*")Number of raw events aggregated.  Output: vm_cpu_aggregates hypertable[cite: 2, 4].6.4 Operator 2: Time-Based Join (Cross-VM)Spark API:Python# Left Stream Filter and Window Definition
left_filtered = create_kafka_source_stream("left") \
    .withColumn("win_start", window(col("event_time"), "10 seconds").getField("start")) \
    .withColumnRenamed("vm_id", "vm_id_a") \
    .withColumnRenamed("avg_cpu", "cpu_a") \
    .withColumnRenamed("event_time", "event_time_a")

# Right Stream Filter and Window Definition
right_filtered = create_kafka_source_stream("right") \
    .withColumn("win_start", window(col("event_time"), "10 seconds").getField("start")) \
    .withColumnRenamed("vm_id", "vm_id_b") \
    .withColumnRenamed("avg_cpu", "cpu_b") \
    .withColumnRenamed("event_time", "event_time_b")

# Multi-Conditional Event-Time Join
correlation_df = left_filtered.join(
    right_filtered,
    (left_filtered["win_start"] == right_filtered["win_start"]) &
    (col("event_time_b") >= col("event_time_a") - expr("INTERVAL 10 SECONDS")) &
    (col("event_time_b") <= col("event_time_a") + expr("INTERVAL 10 SECONDS")) &
    (col("vm_id_a") < col("vm_id_b")) &
    (substring(col("vm_id_a"), 1, 1) == substring(col("vm_id_b"), 1, 1)),
    how="inner"
)
What it does:Splits the ingestion stream into two isolated streams (left and right).  Defines watermark rules with a 10-second sliding range constraint between event_time_a and event_time_b to meet Structured Streaming's state-cleanup requirements.  Employs Explicit DataFrame column referencing (left_filtered["win_start"] == right_filtered["win_start"]) to prevent SQL compilation errors due to AMBIGUOUS_REFERENCE exceptions.  Filters duplicate mappings using alphabetical VM sorting (vm_id_a < vm_id_b) and a prefix matching rule (substring(..., 1, 1)) to dramatically optimize memory pressure.  Output: vm_cpu_correlations hypertable.  6.5 Operator 3: Sample-and-Hold (Last Value Per Window)Spark API:Pythonwindow(col("event_time"), "300 seconds", "10 seconds")
agg(
    last("avg_cpu", True).alias("cpu_held"),
    max("event_time").alias("last_event_time")
)
What it does:Implements forward filling by querying a 5-minute sliding window (300 seconds) updating every 10 seconds.  If there is a temporary gap in telemetry (e.g., connection dropouts or trace silence), the larger window allows the last known CPU metrics to be retained and visualised continuously in a classic stepped pattern.  Resolved potential TypeError runtime exceptions by passing True as a positional parameter to PySpark's last() function (instead of the keyword parameter ignoreNulls=True), ensuring portability across version dependencies.  Output: vm_metrics_held hypertable.  7. Storage Layer — TimescaleDB Schema7.1 Why TimescaleDB?TimescaleDB is a PostgreSQL extension that turns regular tables into hypertables — time-series-optimized tables with automatic partitioning (chunking) by time[cite: 4].7.2 Table Schemavm_cpu_aggregates (Moving Average Output)SQLCREATE TABLE vm_cpu_aggregates (
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
vm_cpu_correlations (Time-Based Join Output)SQLCREATE TABLE vm_cpu_correlations (
    window_start TIMESTAMP NOT NULL,
    vm_id_a      VARCHAR(255) NOT NULL,
    vm_id_b      VARCHAR(255) NOT NULL,
    cpu_a        DOUBLE PRECISION,
    cpu_b        DOUBLE PRECISION,
    PRIMARY KEY (window_start, vm_id_a, vm_id_b)
);
SELECT create_hypertable('vm_cpu_correlations', 'window_start');
vm_metrics_held (Sample-and-Hold Output)SQLCREATE TABLE vm_metrics_held (
    window_start     TIMESTAMP NOT NULL,
    window_end       TIMESTAMP NOT NULL,
    vm_id            VARCHAR(255) NOT NULL,
    cpu_held         DOUBLE PRECISION NOT NULL,
    last_event_time  TIMESTAMP NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);
SELECT create_hypertable('vm_metrics_held', 'window_start');
8. Visualization Layer — Grafana8.1 Auto-ProvisioningGrafana is configured via provisioning files (no manual UI setup)[cite: 4]:Datasource: Infra/grafana/provisioning/dashboards/postgres.yml[cite: 4]Dashboard provider: Infra/grafana/provisioning/dashboards/dashboard.yml[cite: 4]Dashboard: Infra/grafana/dashboards/telemetry-dashboard.json[cite: 4]8.2 Live Dashboard Feedback (Visual Analysis)Upon implementing the fixes, Grafana outputs validated successful metrics:CPU Moving Average by VM (Operator 1): Correctly plots sliding windows.Cross-VM CPU Correlation (Operator 2): Replaced the "No data" state with structured scatter mappings showing active VM pairs paired in real-time.CPU Sample-and-Hold Step Plot (Operator 3): Reflects continuous horizontal step lines bridging any temporary throughput silence in the simulated ingestion.9. Infrastructure Layer9.1 Docker Compose ServicesYAMLservices:
  redpanda:
    image: redpandadata/redpanda:v23.2.1
    ports: ["19092:19092"]

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
10. How to RunPrerequisites (one-time)Install Java 17 to C:\Users\Havoc\java17\jdk-17.0.12+7[cite: 4]Install Docker Desktop and start it[cite: 4]Copy winutils.exe and hadoop.dll to C:\hadoop\bin\[cite: 4]Set HADOOP_HOME=C:\hadoop[cite: 4]Create C:\hadoop\checkpoints\telemetry_pipeline[cite: 4]Quick Start (One Command)PowerShellcd Streaming-Analytics
.\run_pipeline.ps1
11. Troubleshooting GuideSymptomCauseFixAMBIGUOUS_REFERENCE on joinSame column names on left and right join schemas.  Use explicit Dataframe referencing on .join() conditions.  TypeError in last()ignoreNulls keyword rejected.  Pass the boolean value True as a positional argument.  Empty Join GraphLow VM sampling density in stream[cite: 3].Increase VMS_PER_TICK parameter to 50 in producer.py[cite: 3].Fragmented Step PlotShort window state timeout[cite: 2].Scale tumbling window up to window("300 seconds", "10 seconds")[cite: 2].12. ConclusionThis project delivers a complete, working streaming analytics pipeline that satisfies every requirement in the original description:Apache Spark Structured Streaming processes real-time data from a Kafka-compatible broker[cite: 2, 4].Three streaming operators (Moving Average, Time-Based Join, Sample-and-Hold) are implemented as separate, parallel Spark queries with independent checkpoint locations[cite: 2, 4].Event-time semantics are correctly handled via watermarks and timestamp-based windows[cite: 2, 4].TimescaleDB stores the aggregated outputs in time-optimized hypertables[cite: 2, 4].Grafana auto-provisions with a datasource and a 6-panel dashboard that visualizes all three operators plus pipeline health metrics[cite: 4].The entire system is zero-cloud, runs on Windows, and can be started with a single PowerShell command (./run_pipeline.ps1)[cite: 4].Report generated for the Big Data Streaming Analytics course — Development Branch[cite: 4].
```
