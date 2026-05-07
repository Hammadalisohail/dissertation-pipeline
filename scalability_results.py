import csv
import json

# Scalability test results
scalability_data = [
    {"target_rate": 100, "actual_rate": 88, "time_taken": 114.15, "total_events": 10000},
    {"target_rate": 500, "actual_rate": 351, "time_taken": 28.46, "total_events": 10000},
    {"target_rate": 1000, "actual_rate": 539, "time_taken": 18.56, "total_events": 10000},
    {"target_rate": 5000, "actual_rate": 720, "time_taken": 13.89, "total_events": 10000},
    {"target_rate": 10000, "actual_rate": 741, "time_taken": 13.49, "total_events": 10000},
]

# Save to CSV for Power BI
with open(r"D:\dissertation-pipeline\powerbi_scalability.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Target_Rate", "Actual_Rate", "Time_Taken", "Total_Events", "Efficiency_%"])
    for row in scalability_data:
        efficiency = (row["actual_rate"] / row["target_rate"]) * 100
        writer.writerow([
            row["target_rate"],
            row["actual_rate"],
            row["time_taken"],
            row["total_events"],
            round(efficiency, 1)
        ])

print("powerbi_scalability.csv created!")

# Save to JSON
with open(r"D:\dissertation-pipeline\scalability_results.json", "w") as f:
    json.dump(scalability_data, f, indent=4)

print("scalability_results.json created!")

# Print summary table
print()
print("=" * 65)
print(f"{'Target Rate':>12} {'Actual Rate':>12} {'Time (sec)':>12} {'Efficiency':>12}")
print("-" * 65)
for row in scalability_data:
    efficiency = (row["actual_rate"] / row["target_rate"]) * 100
    print(f"{row['target_rate']:>12} {row['actual_rate']:>12} {row['time_taken']:>12.2f} {efficiency:>11.1f}%")
print("=" * 65)
print()
print("All files saved and ready for Power BI!")