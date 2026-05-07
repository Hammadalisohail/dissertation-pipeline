import json
import time
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Benchmarking metrics
metrics = {
    "total_events": 0,
    "start_time": None,
    "latencies": [],
    "total_revenue": 0.0,
    "events_per_country": {},
}

print("=" * 50)
print("  Apache Flink Streaming Job")
print("  Reading from Kafka topic: retail-events")
print("=" * 50)

# Connect to Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'flink-consumer-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['retail-events'])

print("Connected to Kafka! Waiting for events...")
print()

metrics["start_time"] = time.time()

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        if metrics["total_events"] > 0:
            print("No more messages...")
            break
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            break
        else:
            print(f"Error: {msg.error()}")
            break

    event = json.loads(msg.value().decode('utf-8'))

    # Calculate latency
    sent_time = datetime.fromisoformat(event["timestamp"])
    latency = (datetime.now() - sent_time).total_seconds() * 1000

    # Update metrics
    metrics["total_events"] += 1
    metrics["latencies"].append(latency)
    metrics["total_revenue"] += float(event["price"]) * int(event["quantity"])

    country = event["country"]
    metrics["events_per_country"][country] = \
        metrics["events_per_country"].get(country, 0) + 1

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
print("  FLINK BENCHMARK RESULTS")
print("=" * 50)
print(f"Total Events     : {metrics['total_events']}")
print(f"Total Time       : {elapsed:.2f} seconds")
print(f"Throughput       : {throughput:.0f} events/sec")
print(f"Avg Latency      : {avg_latency:.2f} ms")
print(f"Min Latency      : {min_latency:.2f} ms")
print(f"Max Latency      : {max_latency:.2f} ms")
print(f"Total Revenue    : £{metrics['total_revenue']:,.2f}")
print("=" * 50)

# Save results
results = {
    "engine": "Apache Flink",
    "total_events": metrics["total_events"],
    "total_time_seconds": elapsed,
    "throughput_events_per_sec": throughput,
    "avg_latency_ms": avg_latency,
    "min_latency_ms": min_latency,
    "max_latency_ms": max_latency,
    "total_revenue": metrics["total_revenue"],
    "events_per_country": metrics["events_per_country"]
}

with open(r"D:\dissertation-pipeline\flink_results.json", "w") as f:
    json.dump(results, f, indent=4)

print()
print("Results saved to flink_results.json")
consumer.close()