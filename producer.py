import pandas as pd
import json
import time
from confluent_kafka import Producer
from datetime import datetime

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Load the dataset
print("Loading dataset...")
df = pd.read_csv(r"D:\dissertation-pipeline\data\online_retail.csv")
df = df.dropna()
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

    producer.produce(
        "retail-events",
        value=json.dumps(event, default=str).encode("utf-8")
    )
    producer.poll(0)
    count += 1

    if count % 1000 == 0:
        print(f"Sent {count} events to Kafka...")

    time.sleep(0.001)

producer.flush()
print(f"Finished! Total events sent: {count}")