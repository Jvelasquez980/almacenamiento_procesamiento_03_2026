import base64
import boto3
import json
import os
from datetime import datetime, timezone

s3 = boto3.client("s3")
BUCKET = os.environ["S3_BUCKET"]


def _safe_timestamp(value: str) -> str:
    return (
        value.replace("-", "")
        .replace(":", "")
        .replace(".", "")
        .replace("+", "")
        .replace("Z", "")
        .replace("T", "_")
    )


def lambda_handler(event, context):
    records_written = 0
    errors = 0

    for rec in event["Records"]:
        try:
            payload = json.loads(base64.b64decode(rec["kinesis"]["data"]))
            now = datetime.now(timezone.utc)
            safe_event_ts = _safe_timestamp(payload.get("event_ts", now.isoformat()))
            s3_key = (
                f"raw/orders/"
                f"year={now.year}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}/"
                f"{payload['order_id']}_{safe_event_ts}.json"
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

    print(f"Batch complete: written={records_written}  errors={errors}")
    return {"written": records_written, "errors": errors}