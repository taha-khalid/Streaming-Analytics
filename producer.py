import csv
import json
import time
import gzip
from kafka import KafkaProducer

DATA_FOLDER = './data/azure-dataset/cpu'
# Connect to our local Redpanda broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'telemetry-stream'

print("🚀 Starting local cloud telemetry stream...")

with gzip.open(f'{DATA_FOLDER}/vm_cpu_readings-file-1-of-195.csv.gz', mode='rt', encoding='utf-8') as file:
    for line in file:
        # Strip newline characters and split by comma
        row = line.strip().split(',')
        
        # Skip empty lines or rows that don't have enough data columns
        if not row or len(row) < 5:
            continue
            
        try:
            # Map explicitly by index positions from our clean split list
            payload = {
                "timestamp": row[0],
                "vm_id": row[1],
                "cpu": float(row[4]),  # avgcpu
                "mem": 0.0             # placeholder
            }
            
            # Send data to the stream
            producer.send(TOPIC_NAME, value=payload)
            print(f"Sent to stream: {payload}")
            
            # Wait 1 second before sending the next metric to simulate real-time
            time.sleep(1)
            
        except ValueError as e:
            # Safely skip lines that might have corrupted strings instead of numbers
            print(f"⚠️ Skipping malformed row: {row} - Error: {e}")
            continue

producer.flush()
print("🏁 Finished streaming sample data.")