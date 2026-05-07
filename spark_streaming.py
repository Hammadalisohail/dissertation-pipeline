import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

print("=" * 55)
print("  Real Apache Spark Structured Streaming Job")
print("  Using PySpark Engine - Micro-batch Processing")
print("=" * 55)

# Create Spark Session
spark = SparkSession.builder \
    .appName("RetailStreamingBenchmark") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print(f"Spark version: {spark.version}")
print()

# Define schema
schema = StructType([
    StructField("invoice", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Benchmarking variables
batch_times = []
batch_counts = []
start_time = time.time()

def process_batch(df, epoch_id):
    batch_start = time.time()
    count = df.count()
    batch_end = time.time()
    batch_duration = batch_end - batch_start

    if count > 0:
        batch_times.append(batch_duration)
        batch_counts.append(count)

        total_events = sum(batch_counts)
        elapsed = time.time() - start_time
        throughput = total_events / elapsed if elapsed > 0 else 0

        print(f"Batch {epoch_id:>3} | "
              f"Events: {count:>6} | "
              f"Total: {total_events:>8} | "
              f"Throughput: {throughput:>8.0f}/sec | "
              f"Batch time: {batch_duration:.2f}s")

        # Save results every 10 batches
        if epoch_id % 10 == 0 and total_events > 0:
            avg_throughput = total_events / elapsed
            results = {
                "engine": "Apache Spark Structured Streaming",
                "total_events": total_events,
                "total_time_seconds": elapsed,
                "throughput_events_per_sec": avg_throughput,
                "total_batches": len(batch_counts),
                "avg_batch_size": total_events / len(batch_counts),
                "avg_batch_time": sum(batch_times) / len(batch_times)
            }
            with open("/tmp/spark_benchmark_results.json", "w") as f:
                json.dump(results, f, indent=4)

# Read stream from Kafka
print("Connecting to Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "retail-events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Calculate revenue
enriched_df = parsed_df.withColumn(
    "revenue", col("price") * col("quantity")
)

print("Connected to Kafka!")
print("Starting streaming benchmark...")
print()
print(f"{'Batch':>5} | {'Events':>6} | {'Total':>8} | {'Throughput':>12} | {'Batch Time':>10}")
print("-" * 60)

# Start streaming
query = enriched_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="5 seconds") \
    .start()

# Run for 3 minutes
query.awaitTermination(timeout=180)

# Final results
elapsed = time.time() - start_time
total_events = sum(batch_counts)
throughput = total_events / elapsed if elapsed > 0 else 0

print()
print("=" * 55)
print("  SPARK STRUCTURED STREAMING BENCHMARK RESULTS")
print("=" * 55)
print(f"Total Events     : {total_events:,}")
print(f"Total Batches    : {len(batch_counts)}")
print(f"Total Time       : {elapsed:.2f} seconds")
print(f"Throughput       : {throughput:.0f} events/sec")
if batch_times:
    print(f"Avg Batch Time   : {sum(batch_times)/len(batch_times):.2f} seconds")
    print(f"Avg Batch Size   : {total_events/len(batch_counts):.0f} events")
print("=" * 55)

# Save final results
results = {
    "engine": "Apache Spark Structured Streaming",
    "total_events": total_events,
    "total_time_seconds": elapsed,
    "throughput_events_per_sec": throughput,
    "total_batches": len(batch_counts),
    "avg_batch_size": total_events / len(batch_counts) if batch_counts else 0,
    "avg_batch_time": sum(batch_times) / len(batch_times) if batch_times else 0
}

with open("/tmp/spark_benchmark_results.json", "w") as f:
    json.dump(results, f, indent=4)

print("Results saved!")
spark.stop()