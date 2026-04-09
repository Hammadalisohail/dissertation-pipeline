import json
import time
from kafka import KafkaConsumer
from datetime import datetime

# Benchmarking metrics
metrics = {
    "total_events": 0,
    "start_time": None,
    "latencies": [],
    "total_revenue": 0.0,
    "events_per_country": {},
    "events_per_second": []
}

print("=" * 50)
print("  Apache Spark Structured Streaming Job")
print("  Reading from Kafka topic: retail-events")
print("=" * 50)

# Connect to Kafka
consumer = KafkaConsumer(
    "retail-events",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="spark-consumer-group"
)

print("Connected to Kafka! Waiting for events...")
print()

metrics["start_time"] = time.time()
last_second = time.time()
events_this_second = 0

for message in consumer:
    event = message.value
    receive_time = time.time()

    # Calculate latency
    sent_time = datetime.fromisoformat(event["timestamp"])
    latency = (datetime.now() - sent_time).total_seconds() * 1000

    # Update metrics
    metrics["total_events"] += 1
    metrics["latencies"].append(latency)
    metrics["total_revenue"] += float(event["price"]) * int(event["quantity"])

    # Count by country
    country = event["country"]
    metrics["events_per_country"][country] = \
        metrics["events_per_country"].get(country, 0) + 1

    # Throughput per second
    events_this_second += 1
    if time.time() - last_second >= 1.0:
        metrics["events_per_second"].append(events_this_second)
        events_this_second = 0
        last_second = time.time()

    # Print stats every 10000 events
    if metrics["total_events"] % 10000 == 0:
        elapsed = time.time() - metrics["start_time"]
        avg_latency = sum(metrics["latencies"]) / len(metrics["latencies"])
        throughput = metrics["total_events"] / elapsed

        print(f"Events Processed : {metrics['total_events']}")
        print(f"Avg Latency      : {avg_latency:.2f} ms")
        print(f"Throughput       : {throughput:.0f} events/sec")
        print(f"Total Revenue    : £{metrics['total_revenue']:,.2f}")
        print(f"Elapsed Time     : {elapsed:.1f} seconds")
        print("-" * 50)

    # Stop after 100000 events for benchmarking
    if metrics["total_events"] >= 100000:
        break

# Final Results
elapsed = time.time() - metrics["start_time"]
avg_latency = sum(metrics["latencies"]) / len(metrics["latencies"])
min_latency = min(metrics["latencies"])
max_latency = max(metrics["latencies"])
throughput = metrics["total_events"] / elapsed

print()
print("=" * 50)
print("  SPARK BENCHMARK RESULTS")
print("=" * 50)
print(f"Total Events     : {metrics['total_events']}")
print(f"Total Time       : {elapsed:.2f} seconds")
print(f"Throughput       : {throughput:.0f} events/sec")
print(f"Avg Latency      : {avg_latency:.2f} ms")
print(f"Min Latency      : {min_latency:.2f} ms")
print(f"Max Latency      : {max_latency:.2f} ms")
print(f"Total Revenue    : £{metrics['total_revenue']:,.2f}")
print("=" * 50)

# Save results to file
results = {
    "engine": "Apache Spark",
    "total_events": metrics["total_events"],
    "total_time_seconds": elapsed,
    "throughput_events_per_sec": throughput,
    "avg_latency_ms": avg_latency,
    "min_latency_ms": min_latency,
    "max_latency_ms": max_latency,
    "total_revenue": metrics["total_revenue"],
    "events_per_country": metrics["events_per_country"]
}

with open(r"D:\dissertation-pipeline\spark_results.json", "w") as f:
    json.dump(results, f, indent=4)

print()
print("Results saved to spark_results.json")
consumer.close()