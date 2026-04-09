import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
)

# Load the dataset
print("Loading dataset...")
df = pd.read_csv(r"D:\dissertation-pipeline\data\online_retail.csv")
df = df.dropna()  # Remove empty rows
print(f"Loaded {len(df)} records")
print("Starting to stream events to Kafka...")

# Stream each row to Kafka
count = 0
for _, row in df.iterrows():
    event = {
        "invoice": str(row["Invoice"]),
        "stock_code": str(row["StockCode"]),
        "description": str(row["Description"]),
        "quantity": int(row["Quantity"]),
        "invoice_date": str(row["InvoiceDate"]),
        "price": float(row["Price"]),
        "customer_id": str(row["Customer ID"]),
        "country": str(row["Country"]),
        "timestamp": datetime.now().isoformat()
    }

    producer.send("retail-events", value=event)
    count += 1

    # Print progress every 1000 records
    if count % 1000 == 0:
        print(f"Sent {count} events to Kafka...")

    # Small delay to simulate real-time streaming
    time.sleep(0.001)

print(f"Finished! Total events sent: {count}")
producer.flush()
producer.close()