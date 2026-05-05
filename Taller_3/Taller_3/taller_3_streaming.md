# Workshop 3 — Real-Time Data Pipelines with Amazon Kinesis
### Data Engineering · Distributed Technologies & Reliable Messaging

---

## Quick Reference

| Item | Detail |
|---|---|
| Estimated duration | 2.5 – 3 hours |
| AWS region | `us-east-1` (required for AWS Academy) |
| IAM role to use | `LabRole` (do **not** create new roles) |
| Primary service | Amazon Kinesis Data Streams |
| Secondary services | AWS Lambda, Amazon S3, Amazon SQS |
| Analytics | Databricks Community Edition |
| Language | Python 3.10+ · boto3 · PySpark |

---

## 1. Workshop Overview

### What You Will Build

An end-to-end streaming pipeline that ingests simulated e-commerce order events,
processes them in real time, stores them durably in S3, and queries the results
in Databricks — all using managed AWS services.

```
[Python Producer]
      │
      ▼
[Kinesis Data Stream]  ←─── 2 shards, partition key = customer_id
      │                 │
      ▼                 ▼
[Lambda Consumer]   [Python Consumer]        ← two independent consumer groups
      │                 │
      ▼                 ▼
  [S3 Bucket]       [Terminal output]
  raw/orders/
      │
      ▼
[Databricks Notebook]
  Bronze → Silver → Gold queries
```

### Learning Objectives

By the end of this workshop you will be able to:

1. **Create and configure** a Kinesis Data Stream with multiple shards and produce records
   using a partition key strategy that distributes load evenly.
2. **Implement two consumer patterns** — a Lambda-based push consumer and a
   pull-based Python consumer using shard iterators — and explain the trade-offs.
3. **Distinguish at-least-once from exactly-once delivery semantics** by observing
   duplicate records in the S3 sink and applying a deduplication strategy.
4. **Connect a Databricks notebook to S3** and run analytical queries on streaming
   data landed as JSON files.
5. **Explain why Kinesis shards map conceptually to Kafka partitions** and describe
   how consumer groups, offsets, and sequence numbers serve the same purpose.

### Concepts Reinforced

| Concept from class | Where it appears in the lab |
|---|---|
| Partitioning & ordering guarantees | Shard assignment via `PartitionKey`; records within a shard are strictly ordered |
| Consumer groups | Lambda trigger vs. Python consumer — both read the same stream independently |
| At-least-once delivery | Lambda retries on failure; Python consumer replays from `TRIM_HORIZON` |
| Offset management (sequence numbers) | `ShardIterator` types: `TRIM_HORIZON`, `LATEST`, `AT_SEQUENCE_NUMBER` |
| Replication & durability | Kinesis default 3-AZ replication, 24h retention |
| MOM pub/sub pattern | Bonus SQS exercise: Lambda fan-out to SQS queue |
| Backpressure & throughput limits | 1 MB/s write and 2 MB/s read per shard |

---

## 2. Architecture — Component Map

### Data Model

Every event produced is a JSON-encoded order record:

```json
{
  "order_id":    "ORD-00042",
  "customer_id": "C019",
  "product":     "Laptop Pro",
  "category":    "Electronica",
  "quantity":    2,
  "unit_price":  1450.00,
  "total":       2900.00,
  "city":        "Bogota",
  "event_ts":    "2024-03-15T14:23:11Z"
}
```

### Component → Service Mapping

| Component | AWS / Tool | Key configuration |
|---|---|---|
| **Stream** | Kinesis Data Stream `orders-stream` | 2 shards · 24h retention |
| **Producer** | Python script (local or Cloud9) | `boto3.client('kinesis')` · partition key = `customer_id` |
| **Push consumer** | AWS Lambda `orders-processor` | Kinesis trigger · batch size 10 · bisect-on-error enabled |
| **Pull consumer** | Python script | `GetShardIterator` → `GetRecords` loop |
| **Storage** | S3 bucket `de-workshop3-<yourname>` | Prefix `raw/orders/year=/month=/day=/` |
| **Analytics** | Databricks Community Edition | Read S3 JSON → Delta Lake pipeline |
| **MOM exercise** | Amazon SQS `orders-dlq` | Lambda publishes failed records; demonstrates queue pattern |

### Why 2 Shards?

Each shard supports **1 MB/s ingress and 2 MB/s egress**. With two shards:
- Shard 0 will receive records for customers whose MD5(`customer_id`) maps to the lower
  half of the hash space.
- Shard 1 receives the upper half.
- All records for the **same customer_id are guaranteed to land on the same shard**,
  preserving per-customer ordering — the same guarantee Kafka gives per partition key.

---

## 3. Step-by-Step Lab Instructions

> **Before you start:** Open your AWS Academy Learner Lab, click **Start Lab**, wait
> for the indicator to turn green, then click **AWS** to open the console.
> Copy your session credentials from **AWS Details → Show** — you will need them
> if you run Python outside the console (e.g., a local terminal or Cloud9).

---

### Part 1 — Create the Stream and Produce Events (45 min)

**Goal:** Provision a Kinesis stream with 2 shards and send at least 500 order events
from a Python producer, validating that records appear in both shards.

#### 1.1 Create the Kinesis Stream

In the AWS Console → **Kinesis** → **Data Streams** → **Create data stream**.

| Field | Value |
|---|---|
| Stream name | `orders-stream` |
| Capacity mode | **Provisioned** |
| Number of shards | `2` |

Click **Create data stream**. Wait for status to become **Active** (~30 seconds).

> **AWS Academy pitfall:** If you see `AccessDeniedException` when creating the stream,
> make sure you are in `us-east-1`. AWS Academy Learner Labs restrict most actions
> to a single region.

#### 1.2 Configure boto3 Credentials

Open **AWS Details → Show** in the Learner Lab panel. You will see three values:
`aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`.

Create the file `~/.aws/credentials` (or set environment variables):

```bash
export AWS_ACCESS_KEY_ID="ASIA..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
export AWS_DEFAULT_REGION="us-east-1"
```

> **Important:** These credentials expire when the lab session ends (~4 hours).
> If you get `ExpiredTokenException`, restart the lab session and re-export the variables.

#### 1.3 Python Producer

Install the dependency if needed:

```bash
pip install boto3 faker
```

Save as `producer.py` and run it:

```python
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
                PartitionKey=record["customer_id"],  # same customer → same shard
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
    produce(n_records=500, delay_ms=50)
```

#### Expected Output

```
Sending 500 records to 'orders-stream'...
  [ 50] shard=shardId-000000000000  seq=...4a8f2c
  [100] shard=shardId-000000000001  seq=...7b3e91
  [150] shard=shardId-000000000000  seq=...2d1c44
  ...
Done. Success=500  Failed=0
```

#### Validation

In the AWS Console → **Kinesis** → `orders-stream` → **Monitoring** tab.
You should see non-zero values in **Put records (success)** and **Incoming records**.

> **Common error:** `ProvisionedThroughputExceededException` — reduce `delay_ms` to 100
> or add exponential backoff. With 2 shards you have 2 MB/s ingress, which is more
> than enough for this producer, but the error can appear if records burst onto one shard.

---

### Part 2 — Lambda Consumer → S3 Sink (50 min)

**Goal:** Deploy a Lambda function triggered by Kinesis that decodes each record
and writes it as a JSON file to S3, creating a durable raw data lake layer.

#### 2.1 Create the S3 Bucket

Console → **S3** → **Create bucket**.

| Field | Value |
|---|---|
| Bucket name | `de-workshop3-<your-lastname>` (must be globally unique) |
| Region | `us-east-1` |
| Block all public access | ✅ enabled |

Inside the bucket, create the following prefix (folder):
`raw/orders/`

#### 2.2 Create the Lambda Function

Console → **Lambda** → **Create function** → **Author from scratch**.

| Field | Value |
|---|---|
| Function name | `orders-processor` |
| Runtime | Python 3.12 |
| Execution role | **Use an existing role** → `LabRole` |

Replace the default code with:

```python
# Lambda function: orders-processor
import base64
import boto3
import json
import os
from datetime import datetime, timezone

s3     = boto3.client("s3")
BUCKET = os.environ["S3_BUCKET"]   # set as environment variable


def lambda_handler(event, context):
    records_written = 0
    errors          = 0

    for rec in event["Records"]:
        try:
            # Kinesis delivers base64-encoded data
            payload = json.loads(base64.b64decode(rec["kinesis"]["data"]))

            # Build an S3 key partitioned by date for efficient querying
            now    = datetime.now(timezone.utc)
            s3_key = (
                f"raw/orders/"
                f"year={now.year}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}/"
                f"{payload['order_id']}.json"
            )

            s3.put_object(
                Bucket=BUCKET,
                Key=s3_key,
                Body=json.dumps(payload),
                ContentType="application/json",
            )
            records_written += 1

        except Exception as e:
            print(f"ERROR processing record: {e}")
            errors += 1
            # Re-raising would cause Lambda to retry the entire batch.
            # We catch here to process the rest of the batch (at-least-once).

    print(f"Batch complete: written={records_written}  errors={errors}")
    return {"written": records_written, "errors": errors}
```

**Add the environment variable:**
Lambda → `orders-processor` → **Configuration** → **Environment variables** → **Edit**:

| Key | Value |
|---|---|
| `S3_BUCKET` | `de-workshop3-<your-lastname>` |

#### 2.3 Add the Kinesis Trigger

Lambda → `orders-processor` → **+ Add trigger** → **Kinesis**.

| Field | Value |
|---|---|
| Kinesis stream | `orders-stream` |
| Batch size | `10` |
| Starting position | `TRIM_HORIZON` |
| Bisect batch on function error | ✅ enabled |

Click **Add**.

> **Why `TRIM_HORIZON`?**
> It tells Lambda to start reading from the oldest available record in the stream,
> not just records arriving after the trigger is created. This ensures you consume
> all 500 records you produced earlier.

> **Why bisect on error?**
> If a batch of 10 records fails, Lambda splits it into two batches of 5 and retries.
> This isolates poison-pill records without dropping the entire batch — a direct
> implementation of the at-least-once guarantee discussed in class.

#### 2.4 Validation

Run `producer.py` again to produce 100 more records (change `n_records=100`).
Wait 30–60 seconds, then check S3:

Console → **S3** → your bucket → **raw/orders/** → navigate into the date prefix.
You should see individual `.json` files, one per order.

Check Lambda logs:
Console → **CloudWatch** → **Log groups** → `/aws/lambda/orders-processor`
Look for lines like `Batch complete: written=10  errors=0`.

> **Common error:** `AccessDeniedException` when Lambda writes to S3.
> Fix: The `LabRole` already has S3 permissions, but verify the bucket name in
> the environment variable is spelled exactly as you created it.

> **Common error:** Lambda trigger shows status **Disabled**.
> Fix: The stream may not have been active when the trigger was created.
> Delete and re-add the trigger.

---

### Part 3 — Pull Consumer & Delivery Semantics (35 min)

**Goal:** Build a pull-based consumer using the Kinesis SDK directly, observe
at-least-once delivery by replaying the stream from the beginning, and
understand the trade-offs between `TRIM_HORIZON` and `LATEST`.

#### 3.1 Pull Consumer Script

```python
# consumer_pull.py
import boto3
import base64
import json
import time
from collections import defaultdict

STREAM_NAME = "orders-stream"
REGION      = "us-east-1"

kinesis = boto3.client("kinesis", region_name=REGION)


def get_shard_ids(stream_name: str) -> list[str]:
    resp = kinesis.describe_stream_summary(StreamName=stream_name)
    shards = kinesis.list_shards(StreamName=stream_name)
    return [s["ShardId"] for s in shards["Shards"]]


def consume_shard(shard_id: str, iterator_type: str = "TRIM_HORIZON") -> list[dict]:
    """Read all available records from one shard."""
    resp = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=iterator_type,
    )
    iterator  = resp["ShardIterator"]
    all_records = []
    empty_polls = 0

    while iterator and empty_polls < 3:
        resp    = kinesis.get_records(ShardIterator=iterator, Limit=100)
        records = resp["Records"]
        iterator = resp.get("NextShardIterator")

        if records:
            empty_polls = 0
            for rec in records:
                payload = json.loads(rec["Data"])
                payload["_seq"] = rec["SequenceNumber"]
                payload["_shard"] = shard_id
                all_records.append(payload)
        else:
            empty_polls += 1
            time.sleep(0.5)   # avoid hot-looping on empty shard

    return all_records


def consume_stream(iterator_type: str = "TRIM_HORIZON"):
    stream_name = STREAM_NAME
    shard_ids   = get_shard_ids(stream_name)
    print(f"Stream '{stream_name}' has {len(shard_ids)} shard(s): {shard_ids}")

    totals = defaultdict(int)
    for shard_id in shard_ids:
        records = consume_shard(shard_id, iterator_type)
        for r in records:
            totals[r["category"]] += r["total"]
        print(f"  {shard_id}: {len(records)} records consumed")

    print("\n--- Revenue by category ---")
    for cat, rev in sorted(totals.items(), key=lambda x: -x[1]):
        print(f"  {cat:<20} ${rev:>10,.2f}")
    print(f"\nTotal records: {sum(len(consume_shard(s)) for s in shard_ids)}")


if __name__ == "__main__":
    # TRIM_HORIZON = replay from the oldest record → demonstrates at-least-once
    # Change to "LATEST" to only see new records
    consume_stream(iterator_type="TRIM_HORIZON")
```

Run it:
```bash
python consumer_pull.py
```

#### Expected Output

```
Stream 'orders-stream' has 2 shard(s): ['shardId-000000000000', 'shardId-000000000001']
  shardId-000000000000: 312 records consumed
  shardId-000000000001: 288 records consumed

--- Revenue by category ---
  Electronica          $  84,230.00
  Muebles              $  51,450.00
  Ropa                 $  12,890.00
  Alimentos            $   8,220.00
  Libros               $   5,940.00

Total records: 600
```

#### 3.2 Observing At-Least-Once Delivery

Run `consumer_pull.py` a **second time without changing anything**.
The output will show the **same records again** — you have now consumed each record
at least twice. This is the definition of **at-least-once delivery**:

> The system guarantees every record is delivered, but makes no promise about
> delivering it exactly once. The consumer is responsible for deduplication.

Notice that the Lambda consumer also re-processed all 600 records when you created
the trigger with `TRIM_HORIZON`. Check CloudWatch — you should see two sets of
`Batch complete` log entries.

#### 3.3 Discussion Questions (write your answers in the notebook)

1. How would you modify `consumer_pull.py` to implement **exactly-once** semantics?
   *(Hint: think about storing the last processed `SequenceNumber` somewhere durable.)*
     **Answer:**
   We could save the the data in a DynamoDB save the SequenceNumber or the lastProcesed in a variable and make the consumer_pull saving the way to doit but changin where the data is collected

2. Why does Kinesis not expose a `commit()` method the way Kafka consumer groups do?
   What is the equivalent mechanism in Kinesis?
   
   **Answer:**
   Kafka exposes `commit()` because it has centralized **Consumer Groups** — the broker tracks offsets for all consumers in a group. When you call `commit()`, the offset is stored on the broker.
   
   Kinesis does **not** have Consumer Groups at the stream level. Each consumer is independent and manages its own iterator position. There is no central broker to store checkpoints. Instead, if you need exactly-once semantics in Kinesis, **you must store the `SequenceNumber` yourself** in an external store (e.g., DynamoDB, Redis, PostgreSQL).
   
   The equivalent of `commit()` in Kinesis is:
   ```python
   # After successfully processing a record:
   ddb.put_item(Item={
       "consumer_id": "my-consumer",
       "shard_id": shard_id,
       "last_sequence": rec["SequenceNumber"]
   })
   # On restart, query this table and use AT_SEQUENCE_NUMBER iterator
   ```
   
   **Key difference:** Kafka offsets are broker-managed (centralized); Kinesis offsets are consumer-managed (decentralized).

3. If you run two instances of `consumer_pull.py` simultaneously against the same
   shard, what happens? Is this equivalent to a Kafka consumer group or not? Why?
   
   **Answer:**
   Both instances will read **all records from the beginning** independently. There's no coordination, so both get duplicates of every record. This is **not** equivalent to a Kafka consumer group, which automatically distributes partitions among consumers. In Kinesis, you need to use KCL (Kinesis Client Library) to achieve consumer group behavior with automatic shard assignment.

> **Kinesis vs Kafka — key difference to internalize:**
> Kafka tracks offsets per consumer group on the broker.
> Kinesis does not track consumer position at the stream level unless you use
> the Kinesis Client Library (KCL), which stores checkpoints in DynamoDB.
> Without KCL, every pull consumer independently manages its own iterator position.

---

### Part 4 — Analytics in Databricks (40 min)

**Goal:** Mount the S3 bucket in Databricks, read the raw JSON files, run a
Bronze → Silver → Gold pipeline, and answer three analytical questions using
Spark SQL.

#### 4.1 Configure S3 Access in Databricks

Open Databricks Community Edition. Create a new notebook named `taller3_analytics`.

In the first cell, configure credentials (copy from AWS Details):

```python
# Cell 1 — Configure S3 credentials
# Do NOT commit this cell to a shared repository (contains secrets)

ACCESS_KEY   = "ASIA..."         # replace with your session values
SECRET_KEY   = "..."
SESSION_TOKEN = "..."
BUCKET       = "de-workshop3-<your-lastname>"

spark.conf.set("fs.s3a.access.key",         ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key",         SECRET_KEY)
spark.conf.set("fs.s3a.session.token",      SESSION_TOKEN)
spark.conf.set("fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark.conf.set("fs.s3a.endpoint",           "s3.amazonaws.com")

print("Credentials configured.")
```

> **AWS Academy pitfall:** Session tokens expire. If you see
> `com.amazonaws.AmazonClientException: Unable to load credentials` after resuming
> your Databricks notebook, return to the Learner Lab, refresh the credentials
> from **AWS Details**, and re-run Cell 1.

#### 4.2 Bronze — Ingest Raw JSON

```python
# Cell 2 — Bronze layer

S3_RAW = f"s3a://{BUCKET}/raw/orders/"

df_bronze = (
    spark.read
    .option("multiline", "false")
    .json(S3_RAW)
)

print(f"Schema:")
df_bronze.printSchema()
print(f"\nRecord count: {df_bronze.count():,}")
df_bronze.show(5, truncate=False)
```

#### 4.3 Silver — Clean and Enrich

```python
# Cell 3 — Silver layer
from pyspark.sql import functions as F

df_silver = (
    df_bronze
    # Drop any records with null order_id or negative totals (data quality)
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("total") > 0)
    # Remove duplicates: same order_id may appear if Lambda retried
    .dropDuplicates(["order_id"])
    # Parse timestamp
    .withColumn("event_ts",   F.to_timestamp("event_ts"))
    .withColumn("fecha",      F.to_date("event_ts"))
    .withColumn("hora",       F.hour("event_ts"))
    .withColumn("dia_semana", F.date_format("event_ts", "EEEE"))
    # Derived metrics
    .withColumn("descuento",  F.lit(0.0))   # placeholder for future enrichment
    .withColumn("total_neto", F.col("total"))
)

print(f"Silver record count: {df_silver.count():,}")
df_silver.write.format("delta").mode("overwrite").save("/tmp/taller3/silver/orders/")
print("Silver saved.")
```

> **Deduplication note:** The `.dropDuplicates(["order_id"])` call here is exactly
> the consumer-side deduplication that compensates for at-least-once delivery.
> This is the standard pattern used in production Lakehouse architectures.

#### 4.4 Gold — Analytical Queries

```python
# Cell 4 — Gold layer
df_silver = spark.read.format("delta").load("/tmp/taller3/silver/orders/")
df_silver.createOrReplaceTempView("silver_orders")

# Gold 1: Revenue by category
gold_cat = spark.sql("""
    SELECT
        category,
        COUNT(*)                          AS num_orders,
        SUM(quantity)                     AS units_sold,
        ROUND(SUM(total_neto), 2)         AS revenue,
        ROUND(AVG(total_neto), 2)         AS avg_order_value
    FROM silver_orders
    GROUP BY category
    ORDER BY revenue DESC
""")
print("=== Revenue by category ===")
display(gold_cat)

# Gold 2: Hourly order volume (detect peak hours)
gold_hourly = spark.sql("""
    SELECT
        hora,
        COUNT(*) AS num_orders,
        ROUND(SUM(total_neto), 2) AS revenue
    FROM silver_orders
    GROUP BY hora
    ORDER BY hora
""")
print("=== Orders by hour ===")
display(gold_hourly)

# Gold 3: Top 10 customers by spend
gold_customers = spark.sql("""
    SELECT
        customer_id,
        COUNT(*)                    AS num_orders,
        ROUND(SUM(total_neto), 2)   AS total_spend,
        ROUND(AVG(total_neto), 2)   AS avg_order
    FROM silver_orders
    GROUP BY customer_id
    ORDER BY total_spend DESC
    LIMIT 10
""")
print("=== Top 10 customers ===")
display(gold_customers)
```

#### 4.5 Save Gold to S3

```python
# Cell 5 — Write Gold back to S3

gold_cat.write \
    .format("json") \
    .mode("overwrite") \
    .save(f"s3a://{BUCKET}/gold/revenue_by_category/")

gold_customers.write \
    .format("json") \
    .mode("overwrite") \
    .save(f"s3a://{BUCKET}/gold/top_customers/")

print("Gold layers written to S3.")
```

---

## 4. Exercises & Challenges

---

### Exercise 1 — Partitioned Producer (Baseline)

**Context:** Your manager says the current producer sends all categories through
the same path, making it impossible to know which shard handles which business domain.

**Task:**
Modify `producer.py` to use `category` as the partition key instead of `customer_id`.
Run the modified producer with 300 records, then use `consumer_pull.py` to verify
that all records for the same category land on the same shard.

Add a print statement in the consumer that groups records by `_shard` and prints
a count per category per shard.

**Acceptance criteria:**
- The producer runs successfully and sends 300 records.
- The consumer output shows that each category appears in **only one** shard
  (not split across both), confirming partition key routing.
- A markdown cell in the notebook explains in 3–5 sentences why this partition
  key strategy could be problematic in a real system
  *(hint: think about hot shards if one category dominates volume)*.

---

### Exercise 2 — Duplicate Detection in Silver (Baseline)

**Context:** Lambda is configured with `TRIM_HORIZON` and retries on error.
After a simulated failure, the same records are delivered twice to S3.

**Task:**
1. In `producer.py`, produce 200 records where **20 of them have a repeated `order_id`**
   (i.e., generate records 1–180 normally, then re-send records 1–20 with identical
   `order_id` values but a different `event_ts`).
2. Verify in S3 that both copies of the duplicate records exist as separate files.
3. In the Databricks notebook, add a cell that:
   a. Counts total raw records vs. distinct `order_id` values in Bronze.
   b. Confirms that Silver has **no duplicates** after `dropDuplicates`.
   c. Prints the 5 `order_id` values that appeared more than once in Bronze.

**Acceptance criteria:**
- Bronze count > Silver count (duplicates were removed).
- A query on Silver returns 0 rows with duplicated `order_id`.
- The notebook cell prints exactly the 20 duplicated order IDs.
- A markdown cell explains the difference between at-least-once and exactly-once,
  and states which guarantee Kinesis provides by default.

---

### Challenge — Idempotent Consumer with DynamoDB (Advanced)

**Context:** You have been asked to eliminate duplicates **at the consumer level**,
before records even reach S3, so the raw layer itself is clean.

**Task:**
Build a new Lambda function `orders-processor-idempotent` that:

1. Before writing a record to S3, checks a DynamoDB table `processed-orders`
   for the `order_id`.
2. If the record already exists in DynamoDB → skip writing to S3 (log a warning).
3. If the record is new → write to S3 **and** insert the `order_id` into DynamoDB
   with a TTL of 48 hours (so the table does not grow unbounded).

Requirements:
- DynamoDB table name: `processed-orders`, partition key: `order_id` (String).
- The Lambda must handle the case where DynamoDB is temporarily unavailable
  (catch `ProvisionedThroughputExceededException` and re-raise to trigger retry).
- Produce 100 records, then trigger the Lambda twice on the same records
  (by deleting and re-adding the Kinesis trigger with `TRIM_HORIZON`).
- Verify in S3 that the second run produced **0 new files**.

**Acceptance criteria:**
- CloudWatch logs from the second Lambda run show `SKIPPED (duplicate)` for all records.
- S3 object count after run 1 equals S3 object count after run 2.
- A markdown cell in the notebook formally defines **exactly-once semantics** and
  explains why the combination of at-least-once delivery + idempotent consumer
  is equivalent in practice.

> **Hint for DynamoDB conditional write:**
> ```python
> import boto3
> from botocore.exceptions import ClientError
>
> ddb = boto3.resource("dynamodb")
> table = ddb.Table("processed-orders")
>
> try:
>     table.put_item(
>         Item={"order_id": order_id, "ttl": int(time.time()) + 172800},
>         ConditionExpression="attribute_not_exists(order_id)",
>     )
>     return True   # new record, safe to write to S3
> except ClientError as e:
>     if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
>         return False  # already processed
>     raise
> ```

---

## 5. Deliverables & Grading Rubric

### What to Submit

Submit a **single ZIP file** named `taller3_<apellido>.zip` containing:

```
taller3_<apellido>/
├── producer.py                  # original + any modifications from exercises
├── consumer_pull.py             # pull consumer script
├── taller3_analytics.ipynb      # exported from Databricks (File → Export → IPython Notebook)
├── screenshots/
│   ├── 01_kinesis_stream_active.png      # Kinesis console showing stream Active
│   ├── 02_kinesis_monitoring.png         # Monitoring tab with non-zero metrics
│   ├── 03_lambda_trigger.png             # Lambda showing Kinesis trigger enabled
│   ├── 04_s3_raw_files.png               # S3 raw/orders/ prefix with JSON files
│   ├── 05_cloudwatch_logs.png            # Lambda CloudWatch log showing batch results
│   └── 06_databricks_gold.png            # Databricks display() output of Gold tables
└── report.md                    # Short analytical report (see below)
```

**`report.md` structure (max 400 words):**
1. Describe the partition key strategy you used and justify the choice.
2. State how many duplicate records you found in Bronze and how Silver handled them.
3. Answer the three discussion questions from Part 3.
4. One insight from the Gold analytics (e.g., peak hour, top category, top customer pattern).

---

### Grading Rubric

| Criterion | Weight | Description |
|---|---|---|
| **Stream provisioned correctly** | 10% | 2-shard Kinesis stream exists; screenshot shows Active status and non-zero metrics in Monitoring tab |
| **Producer sends records to both shards** | 10% | Console output shows records distributed across `shardId-000000000000` and `shardId-000000000001` |
| **Lambda consumer writes to S3** | 15% | S3 bucket contains JSON files under `raw/orders/year=/month=/day=/`; CloudWatch logs confirm batch processing |
| **Pull consumer runs and prints results** | 10% | `consumer_pull.py` output shows per-shard record counts and revenue by category |
| **Exercise 1 — Partition key strategy** | 15% | Modified producer uses `category` as key; consumer output confirms single-shard routing per category; markdown justification present |
| **Exercise 2 — Duplicate detection** | 15% | Bronze count > Silver count; notebook prints 20 duplicated `order_id` values; markdown defines at-least-once vs exactly-once |
| **Databricks pipeline (Bronze→Silver→Gold)** | 15% | Notebook runs end-to-end; three Gold queries display correct results; Gold written back to S3 |
| **Report quality** | 10% | Answers are precise, technically grounded, and reference course concepts (CAP, delivery semantics, shard routing) |
| **Advanced challenge (bonus)** | +15% | Idempotent Lambda with DynamoDB passes both acceptance criteria; second run produces 0 new S3 files |

---

## 6. Instructor Notes

### Pre-Session Setup Checklist

- [ ] Verify all students have active AWS Academy Learner Lab accounts and can reach the console.
- [ ] Confirm `us-east-1` is the default region in the lab environment (some cohorts have `us-west-2`).
- [ ] Create a sample bucket `de-workshop3-demo` in your instructor account and pre-populate
      it with ~50 records so students can run the Databricks notebook even if their
      Lambda is not yet working.
- [ ] Share the `producer.py` starter file via the course LMS before the session
      so students do not need to type it from scratch.
- [ ] Confirm Databricks Community Edition clusters are available (they auto-terminate after
      2 hours of inactivity; students must restart the cluster, not the notebook).

### Known AWS Academy Limitations

| Limitation | Impact | Workaround |
|---|---|---|
| IAM restricted to `LabRole` | Students cannot create new IAM roles or policies | Always select **Use existing role → LabRole** when creating Lambda |
| No Billing / Cost Explorer access | Students cannot verify Free Tier usage | Remind them: 1 Kinesis shard costs ~$0.015/hour; 2 shards for 4 hours ≈ $0.12 |
| Session expires after ~4 hours | Credentials in boto3 and Databricks become invalid | Students must export new credentials and re-run Cell 1 in Databricks |
| Kinesis shard limit | Academy accounts are typically limited to 5–10 shards total | 2 shards per student is safe; warn students not to create multiple streams |
| Lambda concurrency limit | Default 10 concurrent executions per account | Unlikely to hit this with batch size 10, but reduce batch size to 5 if Lambda is throttled |
| No Kinesis Data Firehose in some accounts | Can't use managed S3 delivery | This workshop avoids Firehose intentionally; Lambda → S3 is the pattern used |
| CloudWatch log retention | Logs are kept for 1 day by default in Academy | Students should take screenshots before the session ends |

### Common Session-Timeout Scenarios

**Scenario 1:** Student resumes work the next day with a new session.
- Credentials in `~/.aws/credentials` are invalid.
- Fix: Copy new credentials from **AWS Details** and re-export environment variables.
- In Databricks: Re-run Cell 1 with the new token.

**Scenario 2:** Kinesis stream exists from a previous session but the student gets
`ResourceNotFoundException`.
- The stream likely still exists (Kinesis streams persist across sessions).
- Fix: Go to the Kinesis console and confirm the stream is Active. If not, recreate it.

**Scenario 3:** Lambda trigger shows **Disabled** or **Problem** state.
- This often happens when the Lambda's execution role lost Kinesis permissions during session reset.
- Fix: Delete the trigger, wait 30 seconds, and re-add it.

### Tips for a Smooth Session

- **Time management:** Parts 1 and 2 take the most time. If students are falling behind,
  they can skip Part 3 (pull consumer) and come back to it — the Databricks analytics
  in Part 4 only depend on data existing in S3.
- **Debugging Lambda:** The fastest way to debug is CloudWatch Logs. Train students to open
  CloudWatch in a second browser tab as soon as they create the Lambda.
- **Cost alert:** Delete the Kinesis stream at the end of the session.
  `aws kinesis delete-stream --stream-name orders-stream`. Two idle shards cost nothing
  in terms of data but are billed for provisioned capacity.
- **Databricks session:** The free cluster auto-terminates after 2 hours of inactivity.
  Students should save their notebook to DBFS (`/FileStore/`) and not rely on
  in-memory DataFrames across long breaks.
- **For the advanced challenge:** The DynamoDB table needs the TTL attribute enabled
  (`ttl` field). Remind students to go to DynamoDB → table → **Additional settings** →
  **Time to live** and set the attribute name to `ttl`.

### Conceptual Connections to Reinforce During Debrief

At the end of the session, spend 10 minutes on these connections:

| What they did | Kafka equivalent |
|---|---|
| Kinesis shard | Kafka partition |
| `PartitionKey` | Kafka message key |
| `SequenceNumber` | Kafka offset |
| `TRIM_HORIZON` iterator | `auto.offset.reset = earliest` |
| `LATEST` iterator | `auto.offset.reset = latest` |
| Two independent consumers (Lambda + Python script) | Two consumer groups on the same topic |
| KCL checkpoints in DynamoDB | Kafka `__consumer_offsets` topic |
| Shard throughput limit (1 MB/s) | Kafka partition throughput limit (depends on broker config) |

This mapping is the core conceptual takeaway of the workshop.
