import json
import time
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from datetime import datetime

print("=" * 55)
print("  Real Apache Flink Streaming Job")
print("  Event-at-a-time Processing")
print("  Duration: 180 seconds")
print("=" * 55)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

kafka_props = {
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'flink-benchmark-180s',
    'auto.offset.reset': 'latest'
}

kafka_consumer = FlinkKafkaConsumer(
    topics='retail-events',
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props
)

DURATION_SECONDS = 180
STATE_FILE = "/tmp/flink_state.txt"

with open(STATE_FILE, "w") as f:
    f.write(f"{time.time()},0,0.0,0.0,0")

print("Connecting to Kafka...")

def process_event(event_str):
    try:
        with open(STATE_FILE, "r") as f:
            parts = f.read().strip().split(",")
            start_time = float(parts[0])
            count = int(parts[1])
            total_revenue = float(parts[2])
            total_latency = float(parts[3])
            done = int(parts[4])

        if done == 1:
            return "done"

        elapsed = time.time() - start_time

        if elapsed >= DURATION_SECONDS:
            throughput = count / elapsed if elapsed > 0 else 0
            avg_latency = total_latency / count if count > 0 else 0

            results = {
                "engine": "Apache Flink",
                "total_events": count,
                "total_time_seconds": elapsed,
                "throughput_events_per_sec": throughput,
                "avg_latency_ms": avg_latency,
                "total_revenue": total_revenue
            }

            with open("/tmp/flink_benchmark_results.json", "w") as f:
                json.dump(results, f, indent=4)

            with open(STATE_FILE, "w") as f:
                f.write(f"{start_time},{count},{total_revenue},{total_latency},1")

            print()
            print("=" * 55)
            print("  FLINK BENCHMARK RESULTS (180 seconds)")
            print("=" * 55)
            print(f"Total Events  : {count:,}")
            print(f"Total Time    : {elapsed:.2f} seconds")
            print(f"Throughput    : {throughput:.0f} events/sec")
            print(f"Avg Latency   : {avg_latency:.2f} ms")
            print(f"Total Revenue : £{total_revenue:,.2f}")
            print("=" * 55)
            os._exit(0)

        event = json.loads(event_str)
        revenue = float(event.get("price", 0)) * int(event.get("quantity", 0))
        sent_time = datetime.fromisoformat(event["timestamp"])
        latency = abs((datetime.now() - sent_time).total_seconds() * 1000)

        count += 1
        total_revenue += revenue
        total_latency += latency

        with open(STATE_FILE, "w") as f:
            f.write(f"{start_time},{count},{total_revenue},{total_latency},0")

        if count % 10000 == 0:
            throughput = count / elapsed if elapsed > 0 else 0
            print(f"Events: {count:>8,} | Throughput: {throughput:>8.0f}/sec | Elapsed: {elapsed:.1f}s")

    except Exception as e:
        pass

    return "processed"

stream = env.add_source(kafka_consumer)
stream.map(process_event, output_type=Types.STRING())
print("Starting Flink job... (180 seconds)")
env.execute("FlinkRetailBenchmark")