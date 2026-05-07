import json

# Load both results
with open(r"D:\dissertation-pipeline\spark_results.json") as f:
    spark = json.load(f)

with open(r"D:\dissertation-pipeline\flink_results.json") as f:
    flink = json.load(f)

print("=" * 60)
print("   SPARK vs FLINK - BENCHMARK COMPARISON RESULTS")
print("   London Metropolitan University")
print("   MSc Data Analytics Dissertation")
print("=" * 60)

print(f"\n{'Metric':<30} {'Spark':>12} {'Flink':>12} {'Winner':>10}")
print("-" * 60)

# Total Events
print(f"{'Total Events':<30} {spark['total_events']:>12,} {flink['total_events']:>12,} {'✅ Same':>10}")

# Total Time
spark_winner = "🏆 Flink" if flink['total_time_seconds'] < spark['total_time_seconds'] else "🏆 Spark"
print(f"{'Total Time (seconds)':<30} {spark['total_time_seconds']:>12.2f} {flink['total_time_seconds']:>12.2f} {spark_winner:>10}")

# Throughput
t_winner = "🏆 Flink" if flink['throughput_events_per_sec'] > spark['throughput_events_per_sec'] else "🏆 Spark"
print(f"{'Throughput (events/sec)':<30} {spark['throughput_events_per_sec']:>12.0f} {flink['throughput_events_per_sec']:>12.0f} {t_winner:>10}")

# Avg Latency
l_winner = "🏆 Spark" if spark['avg_latency_ms'] < flink['avg_latency_ms'] else "🏆 Flink"
print(f"{'Avg Latency (ms)':<30} {spark['avg_latency_ms']:>12.2f} {flink['avg_latency_ms']:>12.2f} {l_winner:>10}")

# Min Latency
ml_winner = "🏆 Spark" if spark['min_latency_ms'] < flink['min_latency_ms'] else "🏆 Flink"
print(f"{'Min Latency (ms)':<30} {spark['min_latency_ms']:>12.2f} {flink['min_latency_ms']:>12.2f} {ml_winner:>10}")

# Max Latency
xl_winner = "🏆 Spark" if spark['max_latency_ms'] < flink['max_latency_ms'] else "🏆 Flink"
print(f"{'Max Latency (ms)':<30} {spark['max_latency_ms']:>12.2f} {flink['max_latency_ms']:>12.2f} {xl_winner:>10}")

# Total Revenue
print(f"{'Total Revenue (£)':<30} {spark['total_revenue']:>12,.2f} {flink['total_revenue']:>12,.2f} {'✅ Same':>10}")

print("-" * 60)

# Summary
print("\n📊 SUMMARY:")
spark_wins = 0
flink_wins = 0

if spark['total_time_seconds'] < flink['total_time_seconds']:
    spark_wins += 1
else:
    flink_wins += 1

if spark['throughput_events_per_sec'] > flink['throughput_events_per_sec']:
    spark_wins += 1
else:
    flink_wins += 1

if spark['avg_latency_ms'] < flink['avg_latency_ms']:
    spark_wins += 1
else:
    flink_wins += 1

if spark['min_latency_ms'] < flink['min_latency_ms']:
    spark_wins += 1
else:
    flink_wins += 1

if spark['max_latency_ms'] < flink['max_latency_ms']:
    spark_wins += 1
else:
    flink_wins += 1

print(f"   Spark wins  : {spark_wins} categories")
print(f"   Flink wins  : {flink_wins} categories")
print()

if flink_wins > spark_wins:
    print("🏆 OVERALL WINNER: Apache Flink")
    print("   Flink shows better overall performance for")
    print("   high volume e-commerce streaming workloads")
else:
    print("🏆 OVERALL WINNER: Apache Spark")
    print("   Spark shows better overall performance for")
    print("   real-time e-commerce streaming workloads")

print()
print("📝 NOTE: Results collected on local Windows machine.")
print("   Final benchmarks will be collected on clean Linux server")
print("   for accurate dissertation results.")
print("=" * 60)

# Save comparison to file
comparison = {
    "spark": spark,
    "flink": flink,
    "summary": {
        "spark_wins": spark_wins,
        "flink_wins": flink_wins,
        "overall_winner": "Flink" if flink_wins > spark_wins else "Spark"
    }
}

with open(r"D:\dissertation-pipeline\comparison_results.json", "w") as f:
    json.dump(comparison, f, indent=4)

print("\nComparison saved to comparison_results.json")