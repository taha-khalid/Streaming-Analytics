# Streaming Analytics on Cloud Telemetry Data (Local Sandbox)

This project implements a real-time streaming analytics pipeline on a local Windows machine using the Azure 2019 Public Dataset V2. It replays historical VM CPU utilization metrics as a real-time data stream, ingests them via a lightweight message broker, and processes them dynamically using rolling time windows.

---

## 🛠️ System Requirements & Version Matrix

To run this pipeline locally without overloading your laptop, ensure your environment matches these specifications:

| Component        | Technology           | Version / Variant                     |
| ---------------- | -------------------- | ------------------------------------- |
| OS               | Windows 10 / 11      | 64-bit                                |
| Container Engine | Docker Desktop       | Latest Stable                         |
| Language Runtime | Python               | 3.11 or 3.12                          |
| Java Runtime     | Eclipse Temurin JDK  | 17 (Required for Spark compatibility) |
| Message Broker   | Redpanda (Kafka API) | Containerized (v23.2.1 or later)      |
| Stream Engine    | Apache PySpark       | 4.1.2 (Scala 2.13)                    |
| Windows Helper   | Hadoop Winutils      | 3.3.0                                 |

---

## 🚀 Step-by-Step Setup Guide

### 1. Install Java 17 (OpenJDK)

1. Download the Windows x64 MSI installer from Adoptium Temurin JDK 17.
2. Run the installer.
3. **Important:** Enable the option **"Set JAVA_HOME environment variable"** during installation.

---

### 2. Configure Hadoop Environment Patch (winutils)

Windows requires a specific binary helper to mimic Linux file permissions for Spark checkpoints.

#### Create Hadoop Directory

```text
C:\hadoop\bin
```

#### Install winutils.exe

1. Download `winutils.exe` for Hadoop 3.3.0 from a trusted repository (e.g. `cdarlint/winutils` GitHub).
2. Save the file at:

```text
C:\hadoop\bin\winutils.exe
```

#### Configure Environment Variables

Create a new **System Variable**:

```text
Variable Name: HADOOP_HOME
Variable Value: C:\hadoop
```

Add the following entry to your system **Path** variable:

```text
%HADOOP_HOME%\bin
```

---

### 3. Spin Up Ingestion Infrastructure

Make sure Docker Desktop is running, then open a terminal in your project directory and execute:

```bash
docker-compose up -d
```

This starts:

- **Redpanda** (port `19092`)
- **Grafana** (port `3000`)

in detached background mode.

---

### 4. Initialize Python Virtual Environment

Open a fresh Windows PowerShell or Command Prompt window (to register the new environment variables) and run:

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\activate

# Install dependencies
pip install kafka-python-ng pyspark==4.1.2
```

---

## 🏃‍♂️ Running the Pipeline

You will need **two separate terminals** with the virtual environment activated:

```powershell
.\venv\Scripts\activate
```

### Phase A: Start the Stream Processing Engine

In **Terminal 1**, start the PySpark consumer:

```bash
python processor.py
```

> **Note:** On its first execution, Spark will take 1–2 minutes to automatically download the required `spark-sql-kafka-0-10_2.13:4.1.2` package from Maven Central.

---

### Phase B: Launch the Telemetry Stream Replayer

Once the Spark engine indicates it is running and listening, switch to **Terminal 2** and start streaming telemetry data:

```bash
python producer.py
```

---

## 🗂️ Data Pipeline Flow

```text
[Azure .csv.gz File]
       │
       ▼ (producer.py via gzip streaming)
[Redpanda Topic: 'telemetry-stream']
       │
       ▼ (processor.py via PySpark Structured Streaming)
[Console Output: 30-Second Rolling Windows / Avg CPU]
```

---

## 🛑 Troubleshooting Quick-Fixes

### JAVA_GATEWAY_EXITED Error

Your system is likely using an unsupported Java version (such as Java 21 or Java 24).

Verify your active version:

```bash
java -version
```

Ensure `JAVA_HOME` points to Java 17.

---

### HADOOP_HOME Unset Error

Your system cannot find `winutils.exe`.

Verify that the file exists at:

```text
C:\hadoop\bin\winutils.exe
```

Also ensure you restarted your terminal after updating environment variables.

---

### NoSuchMethodError (Scala/Kafka)

Ensure your Spark Session is configured with the **Scala 2.13** Kafka artifact.

PySpark 4.x deprecates older Scala 2.12 Kafka packages.

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
