import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import datetime

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
def write_and_purge_batch(df, epoch_id):
    db_url = "jdbc:postgresql://localhost:5432/telemetry_db"
    
    # 1. Append the new data to TimescaleDB
    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "vm_cpu_aggregates") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    # 2. Purge data older than 15 minutes to keep local storage clean
    try:
        cutoff_time = (datetime.datetime.now() - datetime.timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(db_url, "postgres", "password")
        stmt = conn.createStatement()
        
        delete_sql = f"DELETE FROM vm_cpu_aggregates WHERE window_start < '{cutoff_time}';"
        stmt.executeUpdate(delete_sql)
        
        stmt.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Maintenance warning: Failed to purge old data: {e}")

# 3. Direct your stream to the new write-and-purge routine
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_and_purge_batch) \
    .start()

print("🔥 PySpark Engine is running with an automatic 15-minute rolling data retention window...")
query.awaitTermination()