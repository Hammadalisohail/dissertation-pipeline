import json
import csv

# Load both results
with open(r"D:\dissertation-pipeline\spark_results.json") as f:
    spark = json.load(f)

with open(r"D:\dissertation-pipeline\flink_results.json") as f:
    flink = json.load(f)

# 1. Main comparison CSV
with open(r"D:\dissertation-pipeline\powerbi_comparison.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Metric", "Spark", "Flink"])
    writer.writerow(["Total Events", spark["total_events"], flink["total_events"]])
    writer.writerow(["Total Time (seconds)", spark["total_time_seconds"], flink["total_time_seconds"]])
    writer.writerow(["Throughput (events/sec)", spark["throughput_events_per_sec"], flink["throughput_events_per_sec"]])
    writer.writerow(["Avg Latency (ms)", spark["avg_latency_ms"], flink["avg_latency_ms"]])
    writer.writerow(["Min Latency (ms)", spark["min_latency_ms"], flink["min_latency_ms"]])
    writer.writerow(["Max Latency (ms)", spark["max_latency_ms"], flink["max_latency_ms"]])
    writer.writerow(["Total Revenue (£)", spark["total_revenue"], flink["total_revenue"]])

print("powerbi_comparison.csv created!")

# 2. Country breakdown CSV
countries = set(list(spark["events_per_country"].keys()) + 
                list(flink["events_per_country"].keys()))

with open(r"D:\dissertation-pipeline\powerbi_countries.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Country", "Spark_Events", "Flink_Events"])
    for country in sorted(countries):
        spark_count = spark["events_per_country"].get(country, 0)
        flink_count = flink["events_per_country"].get(country, 0)
        writer.writerow([country, spark_count, flink_count])

print("powerbi_countries.csv created!")

# 3. Performance summary CSV
with open(r"D:\dissertation-pipeline\powerbi_performance.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Engine", "Throughput", "Avg_Latency", "Total_Time", "Total_Revenue"])
    writer.writerow(["Apache Spark", 
                     spark["throughput_events_per_sec"],
                     spark["avg_latency_ms"],
                     spark["total_time_seconds"],
                     spark["total_revenue"]])
    writer.writerow(["Apache Flink",
                     flink["throughput_events_per_sec"],
                     flink["avg_latency_ms"],
                     flink["total_time_seconds"],
                     flink["total_revenue"]])

print("powerbi_performance.csv created!")
print()
print("All CSV files ready for Power BI!")
print("Files location: D:\\dissertation-pipeline\\")