import os
import time
import sys
import pyspark
from pyspark.sql import SparkSession

sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql.functions import (
    col, from_json, expr, from_unixtime, to_timestamp, window,
    avg, max, min, count, last, coalesce, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# =============================================================================
# 1. SPARK ENGINE INITIALIZATION
# =============================================================================
pyspark_version = pyspark.__version__
print(f"📦 Initializing PySpark Engine v{pyspark_version}...")

os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

# Fix Windows Python detection for PySpark subprocesses
venv_python = os.path.join(os.path.dirname(sys.executable), "python.exe")
os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

CHECKPOINT_DIR = "C:/hadoop/checkpoints/telemetry_pipeline"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("AzureTelemetryStreamingPipeline") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.port", "4040") \
    .config("spark.blockManager.port", "4041") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.locality.wait", "0s") \
    .config("spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version},"
            f"org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.shuffle.partitions", "15") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Mute noisy internal loggers
logger_state = spark._jvm.org.apache.log4j.LogManager.getLogger(
    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
logger_state.setLevel(spark._jvm.org.apache.log4j.LogManager.getLogger("root").getLevel())

logger_checksum = spark._jvm.org.apache.log4j.LogManager.getLogger(
    "org.apache.spark.sql.execution.streaming.ChecksumCheckpointFileManager")
logger_checksum.setLevel(spark._jvm.org.apache.log4j.LogManager.getLogger("root").getLevel())

# =============================================================================
# 2. SCHEMA & KAFKA SOURCE HELPER
# =============================================================================
telemetry_schema = StructType([
    StructField("event_time", LongType(), True),
    StructField("vm_id", StringType(), True),
    StructField("min_cpu", DoubleType(), True),
    StructField("max_cpu", DoubleType(), True),
    StructField("avg_cpu", DoubleType(), True)
])


def create_kafka_source_stream(alias_name):
    """
    Creates an independent Kafka execution pipeline.
    Separate instances are required to avoid stream-stream self-join NPE bugs.
    """
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:19092") \
        .option("subscribe", "telemetry-stream") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), telemetry_schema).alias("data")) \
        .select("data.*") \
        .filter(col("vm_id").isNotNull()) \
        .withColumn("event_time", to_timestamp(from_unixtime(col("event_time")))) \
        .withWatermark("event_time", "30 seconds") \
        .alias(alias_name)


# =============================================================================
# 3. DATABASE WRITE HELPER
# =============================================================================
DB_URL = "jdbc:postgresql://localhost:5432/telemetry_db"
DB_USER = "postgres"
DB_PASSWORD = "password"
DB_DRIVER = "org.postgresql.Driver"


def write_to_postgres(df, batch_id, table_name):
    """ForeachBatch sink: write a micro-batch DataFrame to PostgreSQL/TimescaleDB."""
    if df.isEmpty():
        return

    print(f"\n🚀 Micro-Batch {batch_id} | Writing to table: {table_name}")
    df.show(truncate=False, n=5)

    try:
        df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", DB_DRIVER) \
            .mode("append") \
            .save()
        print(f"✅ Batch {batch_id} committed to {table_name}")
    except Exception as e:
        print(f"❌ DB write failed on batch {batch_id} -> {table_name}: {e}")


# =============================================================================
# 4. OPERATOR 1 — MOVING AVERAGE (Sliding Window Aggregation)
# =============================================================================
# Aggregates CPU metrics over a sliding window per VM.
# Window: 60 seconds, Slide: 20 seconds
# =============================================================================
raw_stream_1 = create_kafka_source_stream("raw_1")

moving_avg = raw_stream_1 \
    .groupBy(
        window(col("event_time"), "60 seconds", "20 seconds"),
        col("vm_id")
    ) \
    .agg(
        avg("avg_cpu").alias("avg_cpu"),
        max("max_cpu").alias("max_cpu"),
        min("min_cpu").alias("min_cpu"),
        count("*").alias("record_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("vm_id"),
        col("avg_cpu"),
        col("max_cpu"),
        col("min_cpu"),
        col("record_count")
    )

query_moving_avg = moving_avg.writeStream \
    .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "vm_cpu_aggregates")) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/moving_avg") \
    .start()

print("⚡ Query 1 started: Moving Average (sliding window 60s/20s) -> vm_cpu_aggregates")

# =============================================================================
# 5. OPERATOR 2 — TIME-BASED JOIN (Cross-VM Event Window)
# =============================================================================
# Joins events from different VMs that fall within the same 10-second event-time
# bucket. This detects correlated behaviour across VMs.
# Spark stream-stream joins require an equality predicate — we use a common
# 10-second time bucket as the equality key.
# =============================================================================
left_renamed = create_kafka_source_stream("left") \
    .withColumn("event_window", window(col("event_time"), "10 seconds", "10 seconds")) \
    .withColumn("win_start", col("event_window.start")) \
    .withColumnRenamed("vm_id", "vm_id_a") \
    .withColumnRenamed("avg_cpu", "cpu_a")

right_renamed = create_kafka_source_stream("right") \
    .withColumn("event_window", window(col("event_time"), "10 seconds", "10 seconds")) \
    .withColumn("win_start", col("event_window.start")) \
    .withColumnRenamed("vm_id", "vm_id_b") \
    .withColumnRenamed("avg_cpu", "cpu_b")

correlation_df = left_renamed.join(
    right_renamed,
    "win_start",
    how="inner"
).filter(col("vm_id_a") != col("vm_id_b")) \
    .select(
        col("win_start").alias("window_start"),
        col("vm_id_a"),
        col("vm_id_b"),
        coalesce(col("cpu_a"), lit(0.0)).alias("cpu_a"),
        coalesce(col("cpu_b"), lit(0.0)).alias("cpu_b")
    )

query_correlation = correlation_df.writeStream \
    .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "vm_cpu_correlations")) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/correlations") \
    .start()

print("⚡ Query 2 started: Time-Based Join (10-second event bucket) -> vm_cpu_correlations")

# =============================================================================
# 6. OPERATOR 3 — SAMPLE-AND-HOLD (Last Value Per Window)
# =============================================================================
# Captures the last known CPU reading within each tumbling window.
# This demonstrates the canonical "last-value propagation" pattern used in
# streaming systems to handle irregular or sparse reporting cadences.
# =============================================================================
raw_stream_3 = create_kafka_source_stream("raw_3")

sample_hold = raw_stream_3 \
    .groupBy(
        window(col("event_time"), "30 seconds", "30 seconds"),
        col("vm_id")
    ) \
    .agg(
        last("avg_cpu", True).alias("cpu_held"),
        max("event_time").alias("last_event_time")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("vm_id"),
        coalesce(col("cpu_held"), lit(0.0)).alias("cpu_held"),
        col("last_event_time")
    )

query_sample_hold = sample_hold.writeStream \
    .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "vm_metrics_held")) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/sample_hold") \
    .start()

print("⚡ Query 3 started: Sample-and-Hold (30s tumbling, last-value) -> vm_metrics_held")

# =============================================================================
# 7. KEEP ALIVE
# =============================================================================
print("\n🟢 All 3 streaming queries are active. Awaiting data from Kafka...")
print("   Press Ctrl+C to stop.\n")

# Use awaitAnyTermination so the app stays alive until any query fails
spark.streams.awaitAnyTermination()
