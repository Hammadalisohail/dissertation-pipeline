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
    writer.writerow(["Total Events", spark.get("total_events", 0), flink.get("total_events", 0)])
    writer.writerow(["Total Time (seconds)", spark.get("total_time_seconds", 0), flink.get("total_time_seconds", 0)])
    writer.writerow(["Throughput (events/sec)", spark.get("throughput_events_per_sec", 0), flink.get("throughput_events_per_sec", 0)])
    writer.writerow(["Avg Latency (ms)", spark.get("avg_latency_ms", "N/A"), flink.get("avg_latency_ms", "N/A")])
    writer.writerow(["Total Revenue", spark.get("total_revenue", 0), flink.get("total_revenue", 0)])
print("powerbi_comparison.csv created!")

# 2. Country breakdown CSV
spark_countries = spark.get("events_per_country", {})
flink_countries = flink.get("events_per_country", {})
countries = set(list(spark_countries.keys()) + list(flink_countries.keys()))

with open(r"D:\dissertation-pipeline\powerbi_countries.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Country", "Spark_Events", "Flink_Events"])
    for country in sorted(countries):
        writer.writerow([country, spark_countries.get(country, 0), flink_countries.get(country, 0)])
print("powerbi_countries.csv created!")

# 3. Performance summary CSV
with open(r"D:\dissertation-pipeline\powerbi_performance.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Engine", "Throughput", "Avg_Latency", "Total_Time", "Total_Revenue"])
    writer.writerow(["Apache Spark",
                     spark.get("throughput_events_per_sec", 0),
                     spark.get("avg_latency_ms", 0),
                     spark.get("total_time_seconds", 0),
                     spark.get("total_revenue", 0)])
    writer.writerow(["Apache Flink",
                     flink.get("throughput_events_per_sec", 0),
                     flink.get("avg_latency_ms", 0),
                     flink.get("total_time_seconds", 0),
                     flink.get("total_revenue", 0)])
print("powerbi_performance.csv created!")

# 4. Scalability CSV
with open(r"D:\dissertation-pipeline\powerbi_scalability.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Input_Rate", "Spark_Throughput", "Flink_Throughput"])
    writer.writerow([100, 29, 52])
    writer.writerow([1000, 207, 495])
    writer.writerow([10000, 811, 1836])
print("powerbi_scalability.csv created!")

print()
print("All CSV files ready for Power BI!")
print("Files location: D:\\dissertation-pipeline\\")
