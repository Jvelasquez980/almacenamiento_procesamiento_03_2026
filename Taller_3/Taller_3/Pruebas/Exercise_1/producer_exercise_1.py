# producer.py
import boto3
import json
import random
import time
from datetime import datetime, timezone
from faker import Faker

fake = Faker("es_CO")

STREAM_NAME = "orders-stream"
REGION      = "us-east-1"

kinesis = boto3.client("kinesis", region_name=REGION)

PRODUCTS = [
    ("Laptop Pro", "Electronica", 1450.00),
    ("Monitor 4K", "Electronica", 520.00),
    ("Silla Ergonomica", "Muebles", 380.00),
    ("Escritorio Pie", "Muebles", 620.00),
    ("Camiseta Algodon", "Ropa", 35.00),
    ("Jean Clasico", "Ropa", 85.00),
    ("Python Avanzado", "Libros", 55.00),
    ("Arroz Premium 5kg", "Alimentos", 18.00),
    ("Auriculares BT", "Electronica", 120.00),
    ("Aceite de Oliva", "Alimentos", 22.00),
]

CITIES = ["Bogota", "Medellin", "Cali", "Barranquilla", "Bucaramanga",
          "Pereira", "Manizales", "Cartagena"]


def make_order(order_num: int) -> dict:
    product, category, price = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    customer_id = f"C{random.randint(1, 50):03d}"
    return {
        "order_id":    f"ORD-{order_num:05d}",
        "customer_id": customer_id,
        "product":     product,
        "category":    category,
        "quantity":    qty,
        "unit_price":  price,
        "total":       round(price * qty, 2),
        "city":        random.choice(CITIES),
        "event_ts":    datetime.now(timezone.utc).isoformat(),
    }


def produce(n_records: int = 500, delay_ms: int = 50):
    print(f"Sending {n_records} records to '{STREAM_NAME}'...")
    success, failed = 0, 0

    for i in range(1, n_records + 1):
        record = make_order(i)
        try:
            resp = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(record),
                PartitionKey=record["category"],  # same category → same shard
            )
            shard = resp["ShardId"]
            seq   = resp["SequenceNumber"]
            success += 1
            if i % 50 == 0:
                print(f"  [{i:>4}] shard={shard}  seq={seq[-8:]}")
        except Exception as e:
            print(f"  ERROR on record {i}: {e}")
            failed += 1

        time.sleep(delay_ms / 1000)

    print(f"\nDone. Success={success}  Failed={failed}")


if __name__ == "__main__":
    produce(n_records=300, delay_ms=50)