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