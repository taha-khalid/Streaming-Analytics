import gzip
import json
import time
import random
import os
import glob
import sys
from kafka import KafkaProducer

sys.stdout.reconfigure(encoding='utf-8')

# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BROKERS = ['localhost:19092']
TOPIC = 'telemetry-stream'
DATA_DIR = './data/azure-dataset/cpu'

# Streaming simulation parameters
TICKS_PER_BATCH = 1000       # How many 5-min trace ticks to read (~83 hours trace)
VMS_PER_TICK = 3              # Sample N VMs per tick (dataset has ~227k VMs/tick)
TICKS_PER_SECOND = 1           # Real-time replay speed (2 ticks/sec = 500s to consume 1000 ticks)

# =============================================================================
# KAFKA PRODUCER
# =============================================================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='gzip',
    batch_size=16384,
    linger_ms=5
)


def get_data_files():
    """Discover local CPU dataset files."""
    pattern = os.path.join(DATA_DIR, 'vm_cpu_readings-file-*.csv.gz')
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")
    return files


def read_trace_batches(files):
    """
    Generator that yields (trace_time, [rows]) batches from the CSV.
    Each batch contains all VMs that reported at that trace timestamp.
    We subsample VMs to keep the demo lightweight.
    """
    current_tick = None
    current_rows = []
    tick_count = 0
    rows_in_tick = 0

    for filepath in files:
        print(f"📖 Reading {filepath} ...")
        with gzip.open(filepath, 'rt') as f:
            header = f.readline()  # skip header if present

            for line in f:
                parts = line.strip().split(',')
                if len(parts) < 5:
                    continue

                trace_time = int(parts[0])
                vm_id = parts[1]
                min_cpu = float(parts[2])
                max_cpu = float(parts[3])
                avg_cpu = float(parts[4])

                # New tick detected — yield previous batch
                if current_tick is not None and trace_time != current_tick:
                    sampled = random.sample(current_rows, min(VMS_PER_TICK, len(current_rows)))
                    yield current_tick, sampled
                    tick_count += 1
                    current_rows = []
                    rows_in_tick = 0
                    current_tick = trace_time  # <-- BUG FIX: reset to new tick

                    if tick_count >= TICKS_PER_BATCH:
                        return

                if current_tick is None:
                    current_tick = trace_time

                current_rows.append({
                    'vm_id': vm_id,
                    'min_cpu': min_cpu,
                    'max_cpu': max_cpu,
                    'avg_cpu': avg_cpu
                })
                rows_in_tick += 1

    # Yield final batch
    if current_rows:
        sampled = random.sample(current_rows, min(VMS_PER_TICK, len(current_rows)))
        yield current_tick, sampled


def produce_tick(trace_tick, rows, base_time, tick_idx):
    """
    Send one trace tick worth of VM events to Kafka.
    Trace timestamps are compressed so each tick maps to 1 real second,
    making the sliding windows visible in Grafana within minutes.
    """
    event_time = base_time + tick_idx

    for row in rows:
        payload = {
            'event_time': event_time,
            'vm_id': row['vm_id'],
            'min_cpu': round(row['min_cpu'], 4),
            'max_cpu': round(row['max_cpu'], 4),
            'avg_cpu': round(row['avg_cpu'], 4)
        }
        producer.send(TOPIC, value=payload)

    producer.flush()
    print(f"⏱️  Tick {tick_idx} | Trace time {trace_tick}s | {len(rows)} VMs | Event time: {event_time}")


def main():
    files = get_data_files()
    print(f"🚀 Producer started — {len(files)} file(s) | {TICKS_PER_BATCH} ticks | {VMS_PER_TICK} VMs/tick")

    base_time = int(time.time())
    loop_count = 0

    while True:
        loop_count += 1
        print(f"\n🔄 Streaming loop #{loop_count}")
        tick_idx = 0

        for trace_tick, rows in read_trace_batches(files):
            produce_tick(trace_tick, rows, base_time, tick_idx)
            tick_idx += 1

            # Real-time pacing: sleep to match TICKS_PER_SECOND
            time.sleep(1.0 / TICKS_PER_SECOND)

        print(f"✅ Loop {loop_count} complete ({tick_idx} ticks). Restarting...")
        base_time = int(time.time())


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")
    finally:
        producer.close()
