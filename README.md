# Real-Time Cloud Telemetry Streaming Analytics Pipeline

An end-to-end, production-grade streaming analytics pipeline built as a local sandbox on Windows. This project replays historical multi-core VM CPU telemetry from the **Azure 2019 Public Dataset V2** through a distributed message broker, aggregates metrics dynamically using rolling time windows in Apache PySpark, and stores the analytical outputs in a self-cleaning TimescaleDB instance for live visualization via Grafana.

---

## 🏗️ Architecture Flow

```
Azure Telemetry Dataset (.csv.gz)
        │
        ▼ (producer.py — reads CSV, adds synthetic memory, sends to Kafka)
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
| Message Broker   | Redpanda (Kafka API) | Containerized v23.2.1 or later        |
| Time-Series DB   | TimescaleDB          | latest-pg15                           |
| Stream Engine    | Apache PySpark       | 3.5.x                                 |
| Windows Helper   | Hadoop Winutils      | 3.3.0                                 |

---

## 🚀 Step-by-Step Setup Guide

### 1. Install Java 17 (OpenJDK)

1. Download Windows x64 MSI from Microsoft Open JDK 17:
   https://learn.microsoft.com/en-us/java/openjdk/download
2. Install it.
3. **Important:** Enable **"Set JAVA_HOME environment variable"**

Verify:
```powershell
java -version
```

---

### 2. Configure Hadoop Environment Patch (winutils)

Spark on Windows requires a native Hadoop helper binary.

1. Create directory:

```
C:\hadoop\bin
```

2. Download `winutils.exe` for Hadoop 3.3.0:
   https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.0/bin/winutils.exe

3. Place it here:

```
C:\hadoop\bin\winutils.exe
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
1. Verify Java, Python, and Docker
2. Start the Docker stack (if not already running)
3. Launch the Spark processor in a background job
4. Launch the Kafka producer in a background job
5. Open Grafana in your browser

---

### Option B: Manual Steps

#### Phase 1: Start Docker Infrastructure

```powershell
cd Infra
docker compose up -d
```

Wait ~15 seconds for all services to be healthy.

#### Phase 2: Start the Spark Processor

```powershell
.\venv\Scripts\activate
python processor.py
```

First run may take 1–2 minutes to download Kafka + JDBC dependencies from Maven.

#### Phase 3: Start the Producer (in a new terminal)

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
| **Memory Moving Average by VM** | Sliding-window avg of memory per VM | Moving Average |
| **Cross-VM CPU Correlation** | Paired VM CPU metrics from time-based join | Time-Based Join |
| **Cross-VM Memory Correlation** | Paired VM memory metrics from time-based join | Time-Based Join |
| **CPU Sample-and-Hold** | Step-plot of forward-filled CPU values | Sample-and-Hold |
| **Memory Sample-and-Hold** | Step-plot of forward-filled memory values | Sample-and-Hold |
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
- `avg(memory)` — mean memory utilization
- `max(max_cpu)` — peak CPU in window
- `min(min_cpu)` — minimum CPU in window
- `count(*)` — number of raw events

This creates overlapping windows (sliding) that smooth short-term spikes and reveal trends.

**Output table:** `vm_cpu_aggregates`

---

### Operator 2: Time-Based Join (Cross-VM Event Window)

```
Join condition: vm_id_a != vm_id_b
                AND event_time within ±2 minutes
```

Two independent Kafka source streams are joined on event time. This detects VMs that are experiencing similar CPU/memory pressure at the same time, which can indicate:
- Co-located noisy neighbors
- Cluster-wide workload spikes
- Scheduled batch jobs running across multiple VMs

**Output table:** `vm_cpu_correlations`

---

### Operator 3: Sample-and-Hold (Forward Fill)

```
Window: 30 seconds (tumbling)
Aggregation: last(avg_cpu, ignoreNulls=True)
             last(memory, ignoreNulls=True)
```

The producer intentionally drops ~15% of memory readings to simulate sensor failure or irregular reporting. The Sample-and-Hold operator:
1. Groups events into 30-second tumbling windows per VM
2. Uses `last(..., ignoreNulls=True)` to carry forward the last known non-null value
3. Produces a stepped time-series that never has gaps

This is essential for downstream analytics that cannot tolerate missing data.

**Output table:** `vm_metrics_held`

---

## 🧪 Dataset Notes

The **Azure 2019 Public Dataset V2** ships CPU readings as `.csv.gz` files. Each file contains:

| Column | Description |
|--------|-------------|
| timestamp | Relative trace time (seconds from start) |
| vm_id | Unique VM identifier (hashed) |
| min_cpu | Minimum CPU % in the 5-minute interval |
| max_cpu | Maximum CPU % in the 5-minute interval |
| avg_cpu | Average CPU % in the 5-minute interval |

**Memory readings** are not included in the public CPU dataset. The producer generates **synthetic memory** correlated to CPU load:

```python
memory = 20.0 + (cpu_avg * 0.5) + noise(-3, +3)
```

This is a realistic model because higher CPU utilization typically correlates with higher memory pressure in cloud VMs.

---

## 🛑 Troubleshooting

### JAVA_GATEWAY_EXITED

- Ensure Java 17 is active
- Check `java -version`
- Fix `JAVA_HOME`

---

### HADOOP_HOME issue

- Must be: `C:\hadoop`
- Restart terminal after changes
- Ensure `C:\hadoop\bin\winutils.exe` exists

---

### Kafka / Scala error

- Ensure the Kafka package version matches your PySpark version
- The processor uses `spark-sql-kafka-0-10_2.13` — if this fails, try `_2.12`

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
- Hadoop Winutils (Windows support)
- Java 17 (Temurin)

---

## 📂 Project Structure

```
Streaming-Analytics/
├── producer.py                          # Kafka producer (reads CSV, simulates memory)
├── processor.py                         # Spark Structured Streaming (3 operators)
├── requirements.txt                     # Python dependencies
├── run_pipeline.ps1                     # One-click orchestration script
├── Infra/
│   ├── docker-compose.yml               # Docker stack definition
│   ├── table_creation_query.pgsql       # TimescaleDB schema
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/postgres.yml # Auto-configured PostgreSQL datasource
│       │   └── dashboards/dashboard.yml # Dashboard provider config
│       └── dashboards/
│           └── telemetry-dashboard.json # Pre-built Grafana dashboard
├── data/
│   └── azure-dataset/
│       └── cpu/                         # Azure V2 CPU readings (.csv.gz)
└── README.md                            # This file
```

---

*Built for the Big Data Streaming Analytics course. All components run locally on Windows with zero cloud dependencies.*
