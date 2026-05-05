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
        StreamName=STREAM_NAME, #Estaba en minuscula, lo corregí
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
    shard_category_counts = defaultdict(lambda: defaultdict(int))
    for shard_id in shard_ids:
        records = consume_shard(shard_id, iterator_type)
        for r in records:
            totals[r["category"]] += r["total"]
            shard_category_counts[shard_id][r["category"]] += 1
        print(f"  {shard_id}: {len(records)} records consumed")

    print("\n--- Category count by shard ---")
    for shard_id in shard_ids:
        counts = shard_category_counts[shard_id]
        if not counts:
            print(f"  {shard_id}: no records")
            continue
        for category, count in sorted(counts.items()):
            print(f"  {shard_id:<28} {category:<15} {count:>5}")

    print("\n--- Revenue by category ---")
    for cat, rev in sorted(totals.items(), key=lambda x: -x[1]):
        print(f"  {cat:<20} ${rev:>10,.2f}")
    print(f"\nTotal records: {sum(len(consume_shard(s)) for s in shard_ids)}")


if __name__ == "__main__":
    # TRIM_HORIZON = replay from the oldest record → demonstrates at-least-once
    # Change to "LATEST" to only see new records
    consume_stream(iterator_type="TRIM_HORIZON")