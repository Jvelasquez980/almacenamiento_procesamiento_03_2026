# Reporte Taller 3

1. **Partition key strategy.** I used `customer_id` as the default partition key because it spreads traffic across shards while keeping the order of events for each customer. In Exercise 1, I switched to `category` to prove that routing can be tied to a business domain, but that approach can create hot shards if one category dominates the traffic.

2. **Duplicates in Bronze and Silver.** In Exercise 2, I found **20 duplicated `order_id` values** in Bronze because the same 1–20 records were replayed with a different `event_ts`. Silver handled them with `dropDuplicates(["order_id"])`, so only one record per `order_id` remained. In the idempotent consumer challenge, duplicates were blocked before S3 by checking DynamoDB, so the raw layer stayed clean.

3. **Part 3 discussion answers.** Kinesis does not expose `commit()` because it does not manage consumer-group offsets centrally like Kafka; the equivalent is storing `SequenceNumber` checkpoints externally, usually in DynamoDB or through KCL. If two instances of `consumer_pull.py` read the same shard, both consume the same records independently, so that is not a Kafka-style consumer group unless KCL is used. Kinesis defaults to **at-least-once** delivery, so duplicates are possible and the consumer must be idempotent.

4. **Gold insight.** The Gold layer showed that `Electronica` was the top revenue category, which suggests it was the strongest demand segment in the simulated dataset. That pattern is useful for prioritizing inventory and operational focus.
