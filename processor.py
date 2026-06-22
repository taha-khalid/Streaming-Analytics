import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Detect your exact installed PySpark version dynamically
pyspark_version = pyspark.__version__
print(f"📦 Detected PySpark Version: {pyspark_version}")

# 1. Switched to _2.13 to perfectly match PySpark 4's Scala runtime
spark = SparkSession.builder \
    .appName("AzureTelemetryProcessor") \
    .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark_version}") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Hide messy log spam, only show warnings/errors
spark.sparkContext.setLogLevel("WARN")

# 2. Define the schema matching our incoming Kafka JSON data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("vm_id", StringType(), True),
    StructField("cpu", DoubleType(), True),
    StructField("mem", DoubleType(), True)
])

# 3. Read the live stream from local Redpanda/Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "telemetry-stream") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON and append a processing timestamp
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingest_time", current_timestamp())  # Generates real-world time for windows

# 5. Aggregate: Calculate average CPU in a rolling 30-second window, updating every 10 seconds
aggregated_df = parsed_stream \
    .groupBy(
        window(col("ingest_time"), "30 seconds", "10 seconds"),
        col("vm_id")
    ) \
    .agg(avg("cpu").alias("avg_cpu")) \
    .orderBy(col("window.start").desc())

# 6. Push the stream processing results straight to your console screen
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("🔥 PySpark Engine is running and listening to Redpanda...")
query.awaitTermination()