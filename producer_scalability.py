import json
import time
import sys
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import random

# Get rate from command line argument
RATE = int(sys.argv[1]) if len(sys.argv) > 1 else 500
DURATION = 60  # Run for 60 seconds

print(f"Starting producer at {RATE} events/sec for {DURATION} seconds...")

# Load dataset
df = pd.read_csv('Data/online_retail.csv')
df = df.dropna()
records = df.to_dict('records')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1)
)
start_time = time.time()
count = 0
interval = 1.0 / RATE

print(f"Sending events at rate: {RATE}/sec")

while time.time() - start_time < DURATION:
    event_start = time.time()
    
    record = random.choice(records)
    
    rand = random.random()
    if rand < 0.70:
        event_type = "page_view"
    elif rand < 0.90:
        event_type = "add_to_cart"
    else:
        event_type = "purchase"
    
    event = {
        "invoice": str(record.get("Invoice", "")),
        "stock_code": str(record.get("StockCode", "")),
        "description": str(record.get("Description", "")),
        "quantity": int(record.get("Quantity", 1)),
        "price": float(record.get("Price", 0.0)),
        "customer_id": str(record.get("Customer ID", "")),
        "country": str(record.get("Country", "")),
        "event_type": event_type,
        "timestamp": datetime.now().isoformat()
    }
    
    producer.send('retail-events', value=event)
    count += 1
    
    if count % 1000 == 0:
        elapsed = time.time() - start_time
        actual_rate = count / elapsed
        print(f"Sent {count:,} events | Actual rate: {actual_rate:.0f}/sec | Elapsed: {elapsed:.1f}s")
    
    # Rate limiting
    elapsed = time.time() - event_start
    sleep_time = interval - elapsed
    if sleep_time > 0:
        time.sleep(sleep_time)

producer.flush()
elapsed = time.time() - start_time
print(f"\nDone! Sent {count:,} events in {elapsed:.2f} seconds")
print(f"Actual rate: {count/elapsed:.0f} events/sec")
