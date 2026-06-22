# Real-Time Cloud Telemetry Streaming Analytics Pipeline

An end-to-end, production-grade streaming analytics pipeline built entirely as a local sandbox on Windows. This project replays historical multi-core VM CPU telemetry from the **Azure 2019 Public Dataset V2** through a distributed message broker, aggregates metrics dynamically using rolling time windows in Apache PySpark, and stores the analytical outputs in a self-cleaning TimescaleDB instance for live visualization via Grafana.

---

## 🏗️ Architecture Flow

```
Azure Telemetry Dataset (.csv.gz)
        │
        ▼ (producer.py via continuous gzip chunking)
Redpanda Distributed Broker (Kafka API)
        │
        ▼ (processor.py via Structured Streaming)
Apache PySpark 4.1.2 Engine (Scala 2.13)
        │
        ├──► TimescaleDB Hypertable ◄──► Live Grafana Dashboard
        │
        └──► Automated 15-Min SQL Retention Purge Loop
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
| Stream Engine    | Apache PySpark       | 4.1.2 (Scala 2.13)                    |
| Windows Helper   | Hadoop Winutils      | 3.3.0                                 |

---

## 🚀 Step-by-Step Setup Guide

### 1. Install Java 17 (OpenJDK)

1. Download Windows x64 MSI from Microsoft Open JDK 17:
   https://learn.microsoft.com/en-us/java/openjdk/download
2. Install it.
3. **Important:** Enable **"Set JAVA_HOME environment variable"**

---

### 2. Configure Hadoop Environment Patch (winutils)

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

---

### 3. Spin Up Infrastructure Stack

Create `docker-compose.yml`:

```yaml
version: "3.8"

services:
  redpanda:
    image: redpandadata/redpanda:v23.2.1
    container_name: redpanda
    command:
      - redpanda start
      - --smp 1
      - --reserve-memory 0M
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    ports:
      - "19092:19092"

  timescaledb:
    image: timescale/timescaledb:latest-pg15
    container_name: timescaledb
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
      POSTGRES_DB: telemetry_db
    ports:
      - "5432:5432"
    volumes:
      - timescale_data:/var/lib/postgresql/data
    restart: always

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    restart: always

volumes:
  timescale_data:
```

Run:

```bash
docker-compose up -d
```

---

## 4. Database Hypertable Initialization

Connect to PostgreSQL (localhost:5432):

- User: `postgres`
- Password: `password`
- DB: `telemetry_db`

Run:

```sql
CREATE TABLE IF NOT EXISTS vm_cpu_aggregates (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    vm_id        VARCHAR(255) NOT NULL,
    avg_cpu      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, vm_id)
);

SELECT create_hypertable(
    'vm_cpu_aggregates',
    'window_start',
    if_not_exists => TRUE
);
```

---

## 5. Python Environment Setup

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install kafka-python-ng pyspark==4.1.2
```

---

## 🏃‍♂️ Executing the Pipeline

### Phase A: Start Stream Processor

```bash
python processor.py
```

First run may take 1–2 minutes to download Kafka + JDBC dependencies.

---

### Phase B: Start Producer

```bash
python producer.py
```

---

## 📊 Grafana Setup

Open:

```
http://localhost:3000
```

Default login:

- admin / admin

### Add PostgreSQL Data Source

- Host: `timescaledb:5432`
- DB: `telemetry_db`
- User: `postgres`
- Password: `password`
- SSL: disable

Enable TimescaleDB toggle.

---

### Dashboard Query

```sql
SELECT
  time_bucket('10 seconds', window_start) AS time,
  vm_id AS metric,
  avg(avg_cpu) AS value
FROM vm_cpu_aggregates
WHERE window_start >= $__timeFrom()
  AND window_start <= $__timeTo()
GROUP BY time, vm_id
ORDER BY time;
```

Set:

- Last 5 minutes
- Refresh: 5 seconds

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

---

### Kafka / Scala error

- Ensure Scala `_2.13` compatibility
- Avoid `_2.12` artifacts

---

### Missing dashboard data

- Ensure `.mode("append")` in Spark streaming sink

---

## 📌 Tech Stack

- Python 3.11 / 3.12
- Apache Spark Structured Streaming 4.1.2
- Redpanda (Kafka-compatible broker)
- Docker Desktop
- Grafana
- Azure 2019 Public Dataset V2
- Hadoop Winutils (Windows support)
- Java 17 (Temurin)
