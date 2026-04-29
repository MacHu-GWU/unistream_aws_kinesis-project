---
name: unistream-aws-kinesis
description: >
  Self-contained API reference for the unistream_aws_kinesis plugin (>=0.1.1) —
  AWS Kinesis Data Streams producer and consumer built on the unistream
  abstraction layer. Use when writing Kinesis producers, consumers, or data
  models for streaming applications. Covers KinesisRecord, AwsKinesisStreamProducer,
  AwsKinesisStreamConsumer, KinesisStreamShard, and end-to-end usage patterns.
---

# unistream_aws_kinesis Plugin Reference (>= 0.1.1)

`unistream_aws_kinesis` is a plugin for the `unistream` library that provides
AWS Kinesis Data Streams producer and consumer implementations.

**Install:** `pip install "unistream_aws_kinesis>=0.1.1"`

**Import convention:** `from unistream_aws_kinesis import api as unistream_aws_kinesis`

**Dependencies:** `unistream>=0.1.2`, `boto3>=1.41.5`, `typing_extensions>=4.0` (Python 3.10 only)

---

## Architecture

This plugin provides **Layer 3** (concrete implementation) classes that plug
into the `unistream` framework:

```
unistream (core)                  unistream_aws_kinesis (this plugin)
─────────────────                 ──────────────────────────────────
DataClassRecord  ──────────────>  KinesisRecord
BaseProducer     ──────────────>  AwsKinesisStreamProducer
BaseConsumer     ──────────────>  BaseAwsKinesisStreamConsumer
                                    └── AwsKinesisStreamConsumer
```

The plugin uses `unistream` core components for buffering and checkpointing:

- **FileBuffer** (from `unistream`) — WAL-based local buffer for the producer
- **SimpleCheckpoint** (from `unistream`) — JSON-file-based checkpoint for the consumer
- **RetryConfig** (from `unistream`) — exponential backoff config for the producer

---

## 1. KinesisRecord — Data Model

Extends `DataClassRecord` (frozen dataclass with JSON serialization, auto `id`
and `create_at`) with Kinesis binary encoding.

```python
import dataclasses
from unistream_aws_kinesis.api import KinesisRecord

@dataclasses.dataclass(frozen=True)
class MyRecord(KinesisRecord):
    user_id: str = ""
    event_type: str = ""
    value: int = 0
```

**IMPORTANT:** Must use `@dataclasses.dataclass(frozen=True)` because
`DataClassRecord` is frozen — Python forbids inheriting non-frozen from frozen.

### Inherited from DataClassRecord

| Field/Method | Description |
|---|---|
| `id: str` | Auto-generated UUID4 string |
| `create_at: str` | Auto-generated UTC ISO 8601 timestamp |
| `serialize() -> str` | JSON string |
| `deserialize(data: str) -> Self` | Reconstruct from JSON string (classmethod) |

### Added by KinesisRecord

| Method | Description |
|---|---|
| `to_put_record_data() -> bytes` | Base64-encode serialized JSON for `put_records` API |
| `from_get_record_data(data: bytes) -> Self` | Decode `get_records` API response data (classmethod) |
| `partition_key -> str` | Property, defaults to `self.id`. Override for custom partitioning. |

### Custom Partition Key

```python
@dataclasses.dataclass(frozen=True)
class MyRecord(KinesisRecord):
    tenant_id: str = ""

    @property
    def partition_key(self) -> str:
        return self.tenant_id  # route by tenant
```

---

## 2. AwsKinesisStreamProducer

Extends `BaseProducer`. Implements `send()` via `kinesis_client.put_records()`.

### Constructor

Use the `new()` factory method:

```python
import boto3
from pathlib import Path
from unistream.api import FileBuffer
from unistream.api import RetryConfig
from unistream_aws_kinesis.api import AwsKinesisStreamProducer

kinesis_client = boto3.Session(profile_name="my_profile").client("kinesis")

producer = AwsKinesisStreamProducer.new(
    buffer=FileBuffer.new(
        record_class=MyRecord,
        path_wal=Path("/tmp/producer_buffer.log"),
        max_records=100,       # flush every 100 records
        max_bytes=1_000_000,   # or every 1 MB
    ),
    retry_config=RetryConfig(
        exp_backoff=[1, 2, 4, 8, 15, 30, 60],  # seconds between retries
    ),
    kinesis_client=kinesis_client,
    stream_name="my-stream",
)
```

### Fields

| Field | Type | Description |
|---|---|---|
| `buffer` | `AbcBuffer` | Buffer backend (inherited from BaseProducer) |
| `retry_config` | `RetryConfig` | Retry config (inherited from BaseProducer) |
| `kinesis_client` | `KinesisClient` | boto3 Kinesis client |
| `stream_name` | `str` | Kinesis stream name |

### Usage

```python
# Simple: put records one by one. Buffer + retry handled automatically.
for i in range(100):
    producer.put(MyRecord(user_id=f"u-{i}", event_type="click"), verbose=True)

# The producer internally:
# 1. Appends record to FileBuffer (WAL on disk)
# 2. When buffer is full (max_records or max_bytes), emits a batch
# 3. Calls send(batch) -> kinesis_client.put_records(...)
# 4. On failure, retries with exponential backoff (non-blocking)
# 5. On success, commits (deletes WAL)
```

### Subclassing (Optional)

Override `send()` to add custom logic (e.g., inject errors for testing):

```python
@dataclasses.dataclass
class MyProducer(AwsKinesisStreamProducer):
    def send(self, records: list):
        # custom pre-processing, logging, etc.
        return super().send(records)
```

---

## 3. AwsKinesisStreamConsumer

Extends `BaseConsumer`. Implements `get_records()` via Kinesis shard iteration.

### Constructor

```python
from unistream.api import SimpleCheckpoint
from unistream_aws_kinesis.api import KinesisStreamShard
from unistream_aws_kinesis.api import AwsKinesisStreamConsumer

# Discover shards
res = kinesis_client.list_shards(StreamName="my-stream")
shards = KinesisStreamShard.from_list_shards_response(res)
shard_id = shards[0].ShardId

# Get shard iterator
res = kinesis_client.get_shard_iterator(
    StreamName="my-stream",
    ShardId=shard_id,
    ShardIteratorType="LATEST",  # or "TRIM_HORIZON" for all records
)
shard_iterator = res["ShardIterator"]

# Create checkpoint
checkpoint = SimpleCheckpoint.load(
    checkpoint_file="/tmp/checkpoint.json",
    records_file="/tmp/records.json",
    lock_expire=900,
    max_attempts=3,
    initial_pointer=shard_iterator,
    start_pointer=shard_iterator,
)
```

### Subclassing (Required)

You **must** override `process_record()`. Override `process_failed_record()`
for DLQ handling.

```python
import dataclasses

@dataclasses.dataclass
class MyConsumer(AwsKinesisStreamConsumer):
    # extra fields with defaults (parent fields use REQ sentinel)
    path_output: Path = dataclasses.field(default=None)
    path_dlq: Path = dataclasses.field(default=None)

    def process_record(self, record: MyRecord) -> str:
        # Your business logic here.
        # Raise an exception to mark the record as failed.
        print(f"Processing: {record.id}")
        with self.path_output.open("a") as f:
            f.write(record.serialize() + "\n")
        return record.serialize()

    def process_failed_record(self, record: MyRecord) -> str:
        # Called when a record exhausts all retry attempts.
        # Write to DLQ, send to SQS, etc.
        with self.path_dlq.open("a") as f:
            f.write(record.serialize() + "\n")
        return record.serialize()
```

### Instantiation

Use the `new()` factory method. Pass extra fields via `additional_kwargs`:

```python
consumer = MyConsumer.new(
    record_class=MyRecord,
    consumer_id=f"my-stream-{shard_id}",
    kinesis_client=kinesis_client,
    stream_name="my-stream",
    shard_id=shard_id,
    checkpoint=checkpoint,
    limit=100,                  # max records per GetRecords call
    exp_backoff_multiplier=1,   # tenacity retry params
    exp_backoff_base=2,
    exp_backoff_min=1,
    exp_backoff_max=60,
    skip_error=True,            # skip failed records (don't crash)
    delay=1,                    # seconds between polls
    additional_kwargs=dict(     # pass extra fields here
        path_output=Path("/tmp/output.log"),
        path_dlq=Path("/tmp/dlq.log"),
    ),
)
```

### Fields

| Field | Type | Description |
|---|---|---|
| `record_class` | `type[KinesisRecord]` | Record class for deserialization |
| `kinesis_client` | `KinesisClient` | boto3 Kinesis client |
| `stream_name` | `str` | Kinesis stream name |
| `shard_id` | `str` | Shard ID to consume from |
| `checkpoint` | `BaseCheckPoint` | Checkpoint instance (inherited) |
| `limit` | `int` | Max records per batch (inherited) |
| `delay` | `int \| float` | Seconds between polls (inherited) |
| `skip_error` | `bool` | Skip failed records (inherited) |

### Running

```python
# Option A: Run forever (polls every `delay` seconds)
consumer.run(verbose=True)

# Option B: Process one batch at a time (for manual control)
consumer.process_batch(verbose=True)
```

**Rate limit note:** AWS Kinesis allows up to **5 `GetRecords` calls per second
per shard**. A `delay=1` (1-second interval) is safe. For higher throughput,
reduce delay but stay under 5 calls/sec.

---

## 4. KinesisStreamShard — Helper

Parses the `list_shards` API response into typed objects.

```python
from unistream_aws_kinesis.api import KinesisStreamShard

res = kinesis_client.list_shards(StreamName="my-stream")
shards = KinesisStreamShard.from_list_shards_response(res)

for shard in shards:
    print(shard.ShardId)           # "shardId-000000000000"
    print(shard.HashKeyRange)      # {"StartingHashKey": "0", ...}
    print(shard.SequenceNumberRange)
```

---

## 5. KinesisGetRecordsResponseRecord — Helper

Parses raw `get_records` API response. Used internally by the consumer — you
rarely need this directly.

```python
from unistream_aws_kinesis.api import KinesisGetRecordsResponseRecord

res = kinesis_client.get_records(ShardIterator=iterator, Limit=100)
response_records = KinesisGetRecordsResponseRecord.from_get_records_response(res)

for rr in response_records:
    record = MyRecord.from_get_record_data(rr.data)
```

---

## Complete End-to-End Example

### Producer Script

```python
import time
import dataclasses
from pathlib import Path
import boto3
from unistream.api import FileBuffer
from unistream.api import RetryConfig
from unistream_aws_kinesis.api import KinesisRecord
from unistream_aws_kinesis.api import AwsKinesisStreamProducer

@dataclasses.dataclass(frozen=True)
class EventRecord(KinesisRecord):
    ith: int = 0

kinesis_client = boto3.Session(profile_name="my_profile").client("kinesis")

producer = AwsKinesisStreamProducer.new(
    buffer=FileBuffer.new(
        record_class=EventRecord,
        path_wal=Path("/tmp/buffer.log"),
        max_records=3,
    ),
    retry_config=RetryConfig(exp_backoff=[1, 2, 4, 8]),
    kinesis_client=kinesis_client,
    stream_name="my-stream",
)

for i in range(1, 101):
    producer.put(EventRecord(ith=i), verbose=True)
    time.sleep(5)
```

### Consumer Script

```python
import dataclasses
from pathlib import Path
import boto3
from unistream.api import SimpleCheckpoint
from unistream_aws_kinesis.api import KinesisRecord
from unistream_aws_kinesis.api import KinesisStreamShard
from unistream_aws_kinesis.api import AwsKinesisStreamConsumer

@dataclasses.dataclass(frozen=True)
class EventRecord(KinesisRecord):
    ith: int = 0

kinesis_client = boto3.Session(profile_name="my_profile").client("kinesis")

# Discover shard
res = kinesis_client.list_shards(StreamName="my-stream")
shard_id = KinesisStreamShard.from_list_shards_response(res)[0].ShardId

# Get iterator
res = kinesis_client.get_shard_iterator(
    StreamName="my-stream", ShardId=shard_id, ShardIteratorType="LATEST",
)
shard_iterator = res["ShardIterator"]

# Checkpoint
checkpoint = SimpleCheckpoint.load(
    checkpoint_file="/tmp/checkpoint.json",
    records_file="/tmp/records.json",
    lock_expire=900, max_attempts=3,
    initial_pointer=shard_iterator, start_pointer=shard_iterator,
)

# Consumer with business logic
@dataclasses.dataclass
class MyConsumer(AwsKinesisStreamConsumer):
    def process_record(self, record: EventRecord) -> str:
        print(f"  ith={record.ith}  id={record.id}")
        return record.serialize()

    def process_failed_record(self, record: EventRecord) -> str:
        print(f"  [DLQ] ith={record.ith}  id={record.id}")
        return record.serialize()

consumer = MyConsumer.new(
    record_class=EventRecord,
    consumer_id=f"my-stream-{shard_id}",
    kinesis_client=kinesis_client,
    stream_name="my-stream",
    shard_id=shard_id,
    checkpoint=checkpoint,
    limit=100, delay=1,
)

consumer.run(verbose=True)  # polls forever
```

---

## Public API Summary (`unistream_aws_kinesis.api`)

```python
from unistream_aws_kinesis import api

# Record
api.KinesisRecord                    # frozen dataclass, extends DataClassRecord
api.KinesisGetRecordsResponseRecord  # get_records response parser

# Producer
api.AwsKinesisStreamProducer         # extends BaseProducer

# Consumer
api.KinesisStreamShard               # list_shards response parser
api.BaseAwsKinesisStreamConsumer     # base class (implements get_records)
api.AwsKinesisStreamConsumer         # ready-to-subclass consumer
```

---

## Testing with moto

Use `moto.mock_aws()` to test without real AWS credentials:

```python
import moto
import boto3

with moto.mock_aws():
    kinesis_client = boto3.Session(region_name="us-east-1").client("kinesis")
    kinesis_client.create_stream(StreamName="test", ShardCount=1)
    # use kinesis_client with AwsKinesisStreamProducer / Consumer as normal
```

---

## Key Design Decisions

- **`kinesis_client` not `BotoSesManager`**: The plugin takes a raw boto3
  Kinesis client, not a `BotoSesManager`. This keeps the dependency minimal.
- **`partition_key` as a property**: Override it on your record subclass for
  custom partitioning strategies (e.g., by tenant, by region).
- **`additional_kwargs` in `Consumer.new()`**: Extra dataclass fields on your
  consumer subclass are passed via this dict parameter.
- **Frozen records**: All `KinesisRecord` subclasses must be `frozen=True`.