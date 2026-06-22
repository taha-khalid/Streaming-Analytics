import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

pyspark_version = pyspark.__version__
print(f"📦 Detected PySpark Version: {pyspark_version}")

# 1. Initialize Spark with BOTH the Kafka and PostgreSQL JDBC driver packages
spark = SparkSession.builder \
    .appName("AzureTelemetryProcessor") \
    .config("spark.jars.packages", 
            f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark_version},"
            f"org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema definitions
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("vm_id", StringType(), True),
    StructField("cpu", DoubleType(), True),
    StructField("mem", DoubleType(), True)
])

# Read from Redpanda
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "telemetry-stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payloads
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingest_time", current_timestamp())

# Compute 30-second rolling aggregates updated every 10 seconds
aggregated_df = parsed_stream \
    .groupBy(
        window(col("ingest_time"), "30 seconds", "10 seconds"),
        col("vm_id")
    ) \
    .agg(avg("cpu").alias("avg_cpu")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("vm_id"),
        col("avg_cpu")
    )

# 2. Write Stream directly to TimescaleDB via JDBC
# Note: Since it's an aggregation stream, we must use "complete" output mode
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/telemetry_db") \
        .option("dbtable", "vm_cpu_aggregates") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save() \
    ).start()

print("🔥 PySpark Engine is running and piping aggregates directly to TimescaleDB...")
query.awaitTermination()