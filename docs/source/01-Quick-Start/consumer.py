# -*- coding: utf-8 -*-

"""
Quick Start — Consumer

Polls the Kinesis stream every 1 second and prints each record.

AWS Kinesis allows up to 5 ``GetRecords`` calls per second per shard,
so a 1-second interval is well within the limit.

Usage::

    python docs/source/01-Quick-Start/consumer.py

Press Ctrl-C to stop.
"""

import time
import shutil
import dataclasses
from pathlib import Path

from unistream.checkpoints.simple import SimpleCheckpoint

from unistream_aws_kinesis.api import KinesisStreamShard
from unistream_aws_kinesis.api import AwsKinesisStreamConsumer

from shared import MyRecord
from shared import STREAM_NAME
from shared import kinesis_client

# --- Data directory ---
dir_demo = Path(__file__).absolute().parent / ".consumer_data"
shutil.rmtree(dir_demo, ignore_errors=True)
dir_demo.mkdir(parents=True, exist_ok=True)

# --- Discover shard ---
res = kinesis_client.list_shards(StreamName=STREAM_NAME)
shards = KinesisStreamShard.from_list_shards_response(res)
shard_id = shards[0].ShardId
consumer_id = f"{STREAM_NAME}-{shard_id}"
print(f"Consuming from shard: {shard_id}")

# --- Get shard iterator (LATEST = only new records from now on) ---
res = kinesis_client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=shard_id,
    ShardIteratorType="LATEST",
)
shard_iterator = res["ShardIterator"]

# --- Checkpoint ---
path_checkpoint = dir_demo / f"{consumer_id}.checkpoint.json"
path_records = dir_demo / f"{consumer_id}.records.json"

checkpoint = SimpleCheckpoint.load(
    checkpoint_file=str(path_checkpoint),
    records_file=str(path_records),
    lock_expire=900,
    max_attempts=3,
    initial_pointer=shard_iterator,
    start_pointer=shard_iterator,
)


# --- Consumer ---
@dataclasses.dataclass
class MyConsumer(AwsKinesisStreamConsumer):
    def process_record(self, record: MyRecord) -> str:
        s = record.serialize()
        print(f"  received: ith={record.ith}  id={record.id}  tag={record.tag}")
        return s

    def process_failed_record(self, record: MyRecord) -> str:
        s = record.serialize()
        print(f"  [DLQ] ith={record.ith}  id={record.id}  tag={record.tag}")
        return s


consumer = MyConsumer.new(
    record_class=MyRecord,
    consumer_id=consumer_id,
    kinesis_client=kinesis_client,
    stream_name=STREAM_NAME,
    shard_id=shard_id,
    checkpoint=checkpoint,
    limit=100,
    delay=1,
)

# --- Run ---
print("Consumer started. Polling every 1 second. Press Ctrl-C to stop.\n")
try:
    consumer.run(verbose=True)
except KeyboardInterrupt:
    print("\nConsumer stopped.")
