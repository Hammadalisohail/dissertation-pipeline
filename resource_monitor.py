import psutil
import time
import csv
import json
import threading
from datetime import datetime

# Storage for metrics
metrics = {
    "spark": [],
    "flink": []
}

def monitor_resources(engine_name, duration=60, interval=1):
    """Monitor CPU and RAM usage for a given duration"""
    print(f"Starting resource monitoring for {engine_name}...")
    print(f"Duration: {duration} seconds, Interval: {interval} second")
    print()

    readings = []
    start_time = time.time()

    while time.time() - start_time < duration:
        cpu_percent = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        ram_used_gb = ram.used / (1024 ** 3)
        ram_percent = ram.percent

        reading = {
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": round(time.time() - start_time, 1),
            "cpu_percent": cpu_percent,
            "ram_used_gb": round(ram_used_gb, 2),
            "ram_percent": ram_percent
        }

        readings.append(reading)

        print(f"[{reading['elapsed_seconds']:>5.1f}s] "
              f"CPU: {cpu_percent:>5.1f}% | "
              f"RAM: {ram_used_gb:.2f} GB ({ram_percent:.1f}%)")

    return readings

def save_results(engine_name, readings):
    """Save monitoring results to CSV and JSON"""

    # Calculate averages
    avg_cpu = sum(r["cpu_percent"] for r in readings) / len(readings)
    max_cpu = max(r["cpu_percent"] for r in readings)
    avg_ram = sum(r["ram_used_gb"] for r in readings) / len(readings)
    max_ram = max(r["ram_used_gb"] for r in readings)
    avg_ram_pct = sum(r["ram_percent"] for r in readings) / len(readings)

    # Save detailed CSV
    csv_file = f"D:\\dissertation-pipeline\\{engine_name.lower()}_resources.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=readings[0].keys())
        writer.writeheader()
        writer.writerows(readings)

    # Save summary JSON
    summary = {
        "engine": engine_name,
        "avg_cpu_percent": round(avg_cpu, 2),
        "max_cpu_percent": round(max_cpu, 2),
        "avg_ram_gb": round(avg_ram, 2),
        "max_ram_gb": round(max_ram, 2),
        "avg_ram_percent": round(avg_ram_pct, 2),
        "total_readings": len(readings)
    }

    json_file = f"D:\\dissertation-pipeline\\{engine_name.lower()}_resource_summary.json"
    with open(json_file, "w") as f:
        json.dump(summary, f, indent=4)

    print()
    print("=" * 50)
    print(f"  {engine_name} RESOURCE SUMMARY")
    print("=" * 50)
    print(f"Avg CPU Usage  : {avg_cpu:.2f}%")
    print(f"Max CPU Usage  : {max_cpu:.2f}%")
    print(f"Avg RAM Usage  : {avg_ram:.2f} GB")
    print(f"Max RAM Usage  : {max_ram:.2f} GB")
    print(f"Avg RAM %      : {avg_ram_pct:.2f}%")
    print("=" * 50)
    print(f"Saved to: {csv_file}")
    print(f"Saved to: {json_file}")

    return summary

if __name__ == "__main__":
    import sys

    engine = sys.argv[1] if len(sys.argv) > 1 else "Spark"
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60

    print("=" * 50)
    print(f"  Resource Monitor — {engine}")
    print(f"  Monitoring for {duration} seconds")
    print("=" * 50)
    print()
    print("START your streaming job NOW in another tab!")
    print("Monitoring begins in 5 seconds...")
    time.sleep(5)

    readings = monitor_resources(engine, duration=duration)
    save_results(engine, readings)