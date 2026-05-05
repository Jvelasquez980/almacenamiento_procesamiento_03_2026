import base64
import boto3
import json
import os
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb", region_name="us-east-1")

BUCKET = os.environ["S3_BUCKET"]
PROCESSED_ORDERS_TABLE = "processed-orders"


def get_table():
    return ddb.Table(PROCESSED_ORDERS_TABLE)


def mark_order_processed(order_id: str) -> bool:
    """
    Attempt to insert order_id into DynamoDB with TTL.
    Returns True if inserted (new), False if already existed (duplicate).
    Raises ProvisionedThroughputExceededException to trigger Lambda retry.
    """
    table = get_table()
    ttl = int(time.time()) + 172800  # 48 hours
    try:
        table.put_item(
            Item={"order_id": order_id, "ttl": ttl},
            ConditionExpression="attribute_not_exists(order_id)",
        )
        return True  # Successfully inserted (new record)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False  # Already exists (duplicate)
        elif e.response["Error"]["Code"] == "ProvisionedThroughputExceededException":
            raise  # Re-raise to trigger Lambda retry
        print(f"ERROR writing to DynamoDB: {e}")
        raise


def lambda_handler(event, context):
    records_written = 0
    records_skipped = 0
    errors = 0

    for rec in event["Records"]:
        try:
            payload = json.loads(base64.b64decode(rec["kinesis"]["data"]))
            order_id = payload.get("order_id")

            # Check if order was already processed (idempotent write)
            if not mark_order_processed(order_id):
                print(f"SKIPPED (duplicate): {order_id}")
                records_skipped += 1
                continue

            # Order is new, write to S3
            now = datetime.now(timezone.utc)
            s3_key = (
                f"raw/orders/"
                f"year={now.year}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}/"
                f"{order_id}.json"
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
            # Catch non-throughput errors to process rest of batch (at-least-once)

    print(f"Batch complete: written={records_written}  skipped={records_skipped}  errors={errors}")
    return {"written": records_written, "skipped": records_skipped, "errors": errors}
