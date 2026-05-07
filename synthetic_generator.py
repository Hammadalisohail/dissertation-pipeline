import json
import time
import random
import uuid
from confluent_kafka import Producer
from datetime import datetime

# Product catalogue
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones", "price": 79.99, "category": "Electronics"},
    {"id": "P002", "name": "Running Shoes", "price": 59.99, "category": "Sports"},
    {"id": "P003", "name": "Coffee Maker", "price": 49.99, "category": "Kitchen"},
    {"id": "P004", "name": "Yoga Mat", "price": 29.99, "category": "Sports"},
    {"id": "P005", "name": "Laptop Stand", "price": 39.99, "category": "Electronics"},
    {"id": "P006", "name": "Water Bottle", "price": 19.99, "category": "Sports"},
    {"id": "P007", "name": "Desk Lamp", "price": 34.99, "category": "Home"},
    {"id": "P008", "name": "Backpack", "price": 49.99, "category": "Fashion"},
    {"id": "P009", "name": "Phone Case", "price": 14.99, "category": "Electronics"},
    {"id": "P010", "name": "Notebook Set", "price": 12.99, "category": "Stationery"},
]

COUNTRIES = [
    "United Kingdom", "Germany", "France", "Netherlands",
    "Spain", "Italy", "Belgium", "Sweden", "Australia", "USA"
]

EVENT_TYPES = (
    ["page_view"] * 70 +
    ["add_to_cart"] * 20 +
    ["purchase"] * 10
)

def generate_event():
    product = random.choice(PRODUCTS)
    event_type = random.choice(EVENT_TYPES)
    quantity = random.randint(1, 5) if event_type == "purchase" else 1

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": quantity,
        "revenue": product["price"] * quantity if event_type == "purchase" else 0,
        "country": random.choice(COUNTRIES),
        "session_id": str(uuid.uuid4())[:8]
    }

def run_generator(events_per_second=100, total_events=10000):
    print("=" * 55)
    print("  Synthetic E-Commerce Event Generator")
    print(f"  Rate: {events_per_second} events/second")
    print(f"  Total: {total_events} events")
    print("  Distribution: 70% views, 20% cart, 10% purchase")
    print("=" * 55)

    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)

    delay = 1.0 / events_per_second
    count = 0
    start_time = time.time()
    page_views = 0
    add_to_carts = 0
    purchases = 0

    print(f"\nStarting at {events_per_second} events/second...")

    while count < total_events:
        event = generate_event()
        producer.produce(
            "retail-events",
            value=json.dumps(event).encode("utf-8")
        )
        producer.poll(0)

        if event["event_type"] == "page_view":
            page_views += 1
        elif event["event_type"] == "add_to_cart":
            add_to_carts += 1
        elif event["event_type"] == "purchase":
            purchases += 1

        count += 1
        time.sleep(delay)

        if count % 1000 == 0:
            elapsed = time.time() - start_time
            actual_rate = count / elapsed
            print(f"Sent: {count:,} events | "
                  f"Rate: {actual_rate:.0f}/sec | "
                  f"Views: {page_views} | "
                  f"Cart: {add_to_carts} | "
                  f"Purchases: {purchases}")

    producer.flush()
    elapsed = time.time() - start_time
    actual_rate = count / elapsed

    print()
    print("=" * 55)
    print("  GENERATOR COMPLETE")
    print("=" * 55)
    print(f"Total Events    : {count:,}")
    print(f"Time Taken      : {elapsed:.2f} seconds")
    print(f"Actual Rate     : {actual_rate:.0f} events/second")
    print(f"Page Views      : {page_views} ({page_views/count*100:.1f}%)")
    print(f"Add to Cart     : {add_to_carts} ({add_to_carts/count*100:.1f}%)")
    print(f"Purchases       : {purchases} ({purchases/count*100:.1f}%)")
    print("=" * 55)

if __name__ == "__main__":
    import sys
    rate = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    total = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    run_generator(events_per_second=rate, total_events=total)