# Real-Time Cloud Telemetry Streaming Analytics Pipeline

An end-to-end, production-grade streaming analytics pipeline built as a local sandbox on Windows. This project replays historical multi-core VM CPU telemetry from the **Azure 2019 Public Dataset V2** through a distributed message broker, aggregates metrics dynamically using rolling time windows in Apache PySpark, and stores the analytical outputs in a self-cleaning TimescaleDB instance for live visualization via Grafana.

---

## 🏗️ Architecture Flow

```
Azure Telemetry Dataset (.csv.gz)
        │
        ▼ (producer.py — reads CSV, sends to Kafka)
Redpanda Distributed Broker (Kafka API)
        │
        ▼ (processor.py — Spark Structured Streaming with 3 operators)
Apache PySpark 3.5.x Engine
        │
        ├──► Moving Average ──────► vm_cpu_aggregates
        ├──► Time-Based Join ─────► vm_cpu_correlations
        ├──► Sample-and-Hold ─────► vm_metrics_held
        │
        ▼
TimescaleDB Hypertables ◄──► Live Grafana Dashboard
```

---

## 🛠️ System Requirements & Version Matrix

| Component        | Technology           | Version / Variant                     |
| ---------------- | -------------------- | ------------------------------------- |
| OS               | Windows 10 / 11      | 64-bit                                |
| Container Engine | Docker Desktop       | Latest Stable                         |
| Language Runtime | Python               | 3.11 or 3.12                          |
| Java Runtime     | Eclipse Temurin JDK  | 17 (Required for Spark compatibility) |
| Message Broker   | Redpanda (Kafka API) | Containerized v23.2.1 or later          |
| Time-Series DB   | TimescaleDB          | latest-pg15                           |
| Stream Engine    | Apache PySpark       | 3.5.x                                 |
| Windows Helper   | Hadoop Winutils      | 3.3.0                                 |

---

## 🚀 Step-by-Step Setup Guide

### 1. Install Java 17 (OpenJDK)

**Download and extract** Eclipse Temurin JDK 17 to:

```
C:\Users\Havoc\java17\jdk-17.0.12+7
```

If you have a different Java 17 installation, update the path in `run_pipeline.ps1` accordingly.

Verify:
```powershell
C:\Users\Havoc\java17\jdk-17.0.12+7\bin\java -version
```

> **Note:** Spark 3.5.x does **not** support Java 24. You must use Java 17 or Java 21.

---

### 2. Configure Hadoop Environment Patch (winutils)

Spark on Windows requires a native Hadoop helper binary.

1. Create directory:

```
C:\hadoop\bin
```

2. Download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.0 from:
   https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.0/bin

3. Place both files here:

```
C:\hadoop\bin\winutils.exe
C:\hadoop\bin\hadoop.dll
```

4. Set environment variable:

```
HADOOP_HOME = C:\hadoop
```

5. Add to PATH:

```
%HADOOP_HOME%\bin
```

6. Create a Spark checkpoint directory:

```powershell
mkdir C:\hadoop\checkpoints\telemetry_pipeline
```

---

### 3. Spin Up Infrastructure Stack

The Docker Compose file is located in `Infra/docker-compose.yml`.

```powershell
cd Infra
docker compose up -d
```

This starts three services:

- **Redpanda** on `localhost:19092` (Kafka-compatible broker)
- **TimescaleDB** on `localhost:5432`
- **Grafana** on `localhost:3000` (auto-provisioned with datasource + dashboard)

---

### 4. Database Hypertable Initialization

The TimescaleDB schema is **auto-provisioned** via the Docker initdb mount (`Infra/table_creation_query.pgsql`). No manual step is required on first start.

If you need to re-create the tables manually, connect with:

```powershell
docker exec -it timescaledb psql -U postgres -d telemetry_db
```

Password: `password`

---

### 5. Python Environment Setup

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

---

## 🏃‍♂️ Running the Pipeline

### Option A: One-Click PowerShell Script

```powershell
.\run_pipeline.ps1
```

This script will:
1. Verify Java 17, Python, and Docker
2. Start the Docker stack (if not already running)
3. Launch the Spark processor in a background job
4. Launch the Kafka producer in a background job
5. Open Grafana in your browser

---

### Option B: Manual Steps (Two Terminals)

#### Terminal 1 — Start the Spark Processor

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

First run may take 1–2 minutes to download Kafka + JDBC dependencies from Maven.

#### Terminal 2 — Start the Producer

```powershell
.\venv\Scripts\activate
python producer.py
```

---

## 📊 Grafana Dashboard

Open:

```
http://localhost:3000/d/telemetry-streaming-01
```

Default login:

- User: `admin`
- Password: `admin`

The dashboard is **auto-provisioned** and includes:

| Panel | Description | Operator |
|-------|-------------|----------|
| **CPU Moving Average by VM** | Sliding-window avg of CPU per VM | Moving Average |
| **Cross-VM CPU Correlation** | Paired VM CPU metrics from time-based join | Time-Based Join |
| **CPU Sample-and-Hold** | Step-plot of last-known CPU values per window | Sample-and-Hold |
| **Records per Window** | Bar chart of throughput | Health |
| **Top 10 VMs by Avg CPU** | Bar gauge of hottest VMs | Health |
| **Latest VM Summary** | Table of latest aggregated stats | Health |

---

## 🔬 Streaming Operators Explained

### Operator 1: Moving Average (Sliding Window)

```
Window: 60 seconds
Slide:  20 seconds
```

Spark Structured Streaming groups events by `(window, vm_id)` and computes:
- `avg(avg_cpu)` — mean CPU utilization
- `max(max_cpu)` — peak CPU in window
- `min(min_cpu)` — minimum CPU in window
- `count(*)` — number of raw events

This creates overlapping windows (sliding) that smooth short-term spikes and reveal trends.

**Output table:** `vm_cpu_aggregates`

---

### Operator 2: Time-Based Join (Cross-VM Event Window)

```
Join condition: vm_id_a != vm_id_b
                AND event_time falls within the same 10-second bucket
```

Two independent Kafka source streams are joined on a common 10-second time bucket. This detects VMs that are experiencing similar CPU pressure at the same time, which can indicate:
- Co-located noisy neighbors
- Cluster-wide workload spikes
- Scheduled batch jobs running across multiple VMs

**Output table:** `vm_cpu_correlations`

---

### Operator 3: Sample-and-Hold (Last Value Per Window)

```
Window: 30 seconds (tumbling)
Aggregation: last(avg_cpu, ignoreNulls=True)
```

Captures the last known CPU reading within each tumbling window. This demonstrates the canonical "last-value propagation" pattern used in streaming systems to handle irregular or sparse reporting cadences.

**Output table:** `vm_metrics_held`

---

## 🧪 Dataset Notes

The **Azure 2019 Public Dataset V2** ships CPU readings as `.csv.gz` files. Each file contains:

| Column | Description |
|--------|-------------|
| `timestamp` | Relative trace time (seconds from start) |
| `vm_id` | Unique VM identifier (hashed) |
| `min_cpu` | Minimum CPU % in the 5-minute interval |
| `max_cpu` | Maximum CPU % in the 5-minute interval |
| `avg_cpu` | Average CPU % in the 5-minute interval |

---

## 🛑 Troubleshooting

### JAVA_GATEWAY_EXITED

- Ensure Java 17 is active (NOT Java 24)
- Check `C:\Users\Havoc\java17\jdk-17.0.12+7\bin\java -version`
- Update `JAVA_HOME` in `run_pipeline.ps1` if your path differs

---

### HADOOP_HOME issue

- Must be: `C:\hadoop`
- Both `winutils.exe` and `hadoop.dll` must be in `C:\hadoop\bin\`
- Restart terminal after changes

---

### Kafka / Scala error

- Ensure the Kafka package uses `_2.12` (matching PySpark's Scala version)
- The processor already uses `spark-sql-kafka-0-10_2.12:3.5.4`

---

### Missing dashboard data

1. Ensure the producer is running and sending events
2. Check the processor logs for micro-batch commits
3. Verify database connectivity: `docker exec -it timescaledb psql -U postgres -d telemetry_db -c "SELECT count(*) FROM vm_cpu_aggregates;"`

---

### Spark checkpoint conflicts

If you change the processor code and restart, you may get a checkpoint conflict. Clear the checkpoint directory:

```powershell
Remove-Item -Recurse -Force C:\hadoop\checkpoints\telemetry_pipeline
```

---

## 📌 Tech Stack

- Python 3.11 / 3.12
- Apache Spark Structured Streaming 3.5.x
- Redpanda (Kafka-compatible broker)
- Docker Desktop
- Grafana (auto-provisioned)
- TimescaleDB (PostgreSQL extension)
- Azure 2019 Public Dataset V2
- Hadoop Winutils + hadoop.dll (Windows support)
- Java 17 (Temurin)

---

## 📂 Project Structure

```
Streaming-Analytics/
├── producer.py                          # Kafka producer (reads CSV, replays as live stream)
├── processor.py                         # Spark Structured Streaming (3 operators)
├── requirements.txt                     # Python dependencies
├── run_pipeline.ps1                     # One-click orchestration script
├── Infra/
│   ├── docker-compose.yml               # Docker stack definition
│   ├── table_creation_query.pgsql       # TimescaleDB schema
│   ├── init_schema.sql                  # Standalone schema init script
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/postgres.yml # Auto-configured PostgreSQL datasource
│       │   └── dashboards/dashboard.yml # Dashboard provider config
│       └── dashboards/
│           └── telemetry-dashboard.json # Pre-built Grafana dashboard
├── data/
│   └── azure-dataset/
│       └── cpu/                         # Azure V2 CPU readings (.csv.gz)
├── README.md                            # This file
└── PROJECT_REPORT.md                    # Comprehensive project report
```

---

*Built for the Big Data Streaming Analytics course. All components run locally on Windows with zero cloud dependencies.*
