---
name: unistream
description: >
  Self-contained API reference for the unistream library (>=0.1.2) — a universal
  stream Producer/Consumer abstraction layer. Use when building custom producers,
  consumers, buffers, checkpoints, or records for any streaming backend. Covers
  all five core abstractions, their protocols, built-in implementations, and
  concrete usage examples.
---

# unistream Library Reference (>= 0.1.2)

`unistream` is a universal Producer/Consumer abstraction layer for stream systems.
It solves three cross-cutting problems: **batching with fault tolerance** (Buffer + WAL),
**graceful retry** (exponential backoff), and **exactly-once consumption** (Checkpoint + Tracker + Lock).

**Install:** `pip install "unistream>=0.1.2"`

**Import convention:** `import unistream.api as unistream`

---

## Architecture Overview

```
Layer 1 — ABC (abstraction.py)     Defines the protocol (what methods must exist)
Layer 2 — Base class                Implements shared logic (retry, state machine, etc.)
Layer 3 — Concrete implementation   Plugs into a specific backend (file, Kinesis, DynamoDB, ...)
```

The five core abstractions and their inheritance chains:

```
AbcRecord    -> BaseRecord    -> DataClassRecord
AbcBuffer    -> BaseBuffer    -> FileBuffer
AbcProducer  -> BaseProducer  -> SimpleProducer / YourProducer
AbcCheckPoint -> BaseCheckPoint -> SimpleCheckpoint / YourCheckpoint
AbcConsumer  -> BaseConsumer  -> SimpleConsumer / YourConsumer
```

### Two Audiences

**Plugin / Backend Developers** implement backend-specific methods:
- Producer: `send()`, `new()`
- Buffer: all methods (`new`, `put`, `should_i_emit`, `emit`, `commit`)
- Checkpoint: `dump`, `load`, `dump_records`, `load_records`, `dump_as_*`
- Consumer: `get_records()`, `new()`

**End Users (Application Developers)** implement business logic and call API:
- Producer: call `put(record)` — buffer, retry, `send()` handled automatically
- Consumer: implement `process_record(record)`, optionally `process_failed_record(record)`, call `process_batch()` or `run()`
- Checkpoint: optionally call `get_tracker()`, `get_not_succeeded_records()` for inspection/DLQ

Data flow:

```
Producer              Stream System           Consumer
(holds Buffer)  --->  (Kinesis, file, etc.)  --->  (holds CheckPoint)
    |                                                  |
    put(record)                                        get_records()
    |                                                  |
    Buffer.emit() -> send(records)                     process_record(record)
```

---

## 1. Record

### Protocol

Every record must have:

- `id: str` — unique identifier
- `create_at: str` — ISO 8601, timezone-aware
- `serialize() -> str` — convert to string
- `deserialize(data: str) -> AbcRecord` (classmethod) — reconstruct from string

### Built-in: DataClassRecord

A frozen dataclass with auto-generated `id` (UUID4) and `create_at` (UTC now).
Serializes to JSON via `dataclasses.asdict`.

```python
import dataclasses
import unistream.api as unistream

@dataclasses.dataclass(frozen=True)
class MyRecord(unistream.DataClassRecord):
    user_id: str = ""
    event_type: str = ""

record = MyRecord(user_id="u-1", event_type="click")
s = record.serialize()       # '{"id": "...", "create_at": "...", "user_id": "u-1", "event_type": "click"}'
r = MyRecord.deserialize(s)  # round-trip
```

### Custom Record

```python
import dataclasses
import unistream.api as unistream
from func_args.api import BaseFrozenModel

@dataclasses.dataclass(frozen=True)
class MyRecord(unistream.BaseRecord, BaseFrozenModel):
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    create_at: str = dataclasses.field(default_factory=lambda: get_utc_now().isoformat())
    payload: str = ""

    def serialize(self) -> str:
        return json.dumps({"id": self.id, "create_at": self.create_at, "payload": self.payload})

    @classmethod
    def deserialize(cls, data: str) -> "MyRecord":
        return cls(**json.loads(data))
```

---

## 2. Buffer

### Protocol (AbcBuffer)

| Method | Description |
|--------|-------------|
| `new(cls, **kwargs)` | Factory method. Should recover unsent records from persistence. |
| `put(record)` | Append record to WAL (disk) and in-memory queue. |
| `should_i_emit() -> bool` | True when batch is full (by count or bytes). |
| `emit() -> list[AbcRecord]` | Return the oldest batch of records (FIFO). |
| `commit()` | Delete WAL file after downstream confirms receipt. |

### Built-in: FileBuffer

Uses local log file as Write-Ahead Log. On crash, unsent records are recovered from disk on restart.

```python
from pathlib import Path
import unistream.api as unistream

buffer = unistream.FileBuffer.new(
    record_class=MyRecord,
    path_wal=Path("/tmp/my_buffer.log"),
    max_records=100,        # emit when 100 records accumulated
    max_bytes=1_000_000,    # or when 1MB accumulated
)

buffer.put(record)
if buffer.should_i_emit():
    records = buffer.emit()
    # send records to target...
    buffer.commit()  # delete WAL after confirmed
```

---

## 3. Producer

### Protocol (AbcProducer)

| Method | Description |
|--------|-------------|
| `new(cls, **kwargs)` | Factory method. |
| `send(records)` | **[Plugin Developer]** Send batch records to target. Subclass must implement this. |
| `put(record, skip_error=True, verbose=False)` | **[End User API]** Manages buffer + retry internally. |

### Built-in: BaseProducer

Implements the full `put()` event loop with non-blocking exponential backoff.
Subclasses only need to implement `send()`.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `buffer` | `AbcBuffer` | The buffer backend. |
| `retry_config` | `RetryConfig` | Retry behavior config. |

### RetryConfig

```python
import unistream.api as unistream

rc = unistream.RetryConfig(
    exp_backoff=[1, 2, 4, 8, 15, 30, 60],  # seconds between retries (default)
)
```

When `attempts >= len(exp_backoff)`, the last value is used forever.
Retry is **non-blocking**: `shall_we_retry()` checks elapsed time, never sleeps.

### Built-in: SimpleProducer

Appends records to a local file. Good for testing and demos.

```python
from pathlib import Path
import unistream.api as unistream

producer = unistream.SimpleProducer.new(
    buffer=unistream.FileBuffer.new(
        record_class=MyRecord,
        path_wal=Path("/tmp/buffer.log"),
        max_records=10,
    ),
    retry_config=unistream.RetryConfig(),
    path_sink=Path("/tmp/output.log"),
)

producer.put(MyRecord(user_id="u-1", event_type="click"))
```

### Custom Producer

```python
import dataclasses
import unistream.api as unistream

@dataclasses.dataclass
class KinesisProducer(unistream.BaseProducer):
    stream_name: str = dataclasses.field(default=REQ)
    bsm: "BotoSesManager" = dataclasses.field(default=REQ)

    @classmethod
    def new(cls, buffer, retry_config, stream_name, bsm):
        return cls(buffer=buffer, retry_config=retry_config,
                   stream_name=stream_name, bsm=bsm)

    def send(self, records):
        self.bsm.kinesis_client.put_records(
            Records=[
                {"Data": record.serialize().encode(), "PartitionKey": record.id}
                for record in records
            ],
            StreamName=self.stream_name,
        )
```

---

## 4. Checkpoint

### Protocol (AbcCheckPoint)

Persistence methods — **plugin/backend developers must implement all of these**:

| Method | Description |
|--------|-------------|
| `dump()` | Persist checkpoint metadata. |
| `load(cls, **kwargs)` | Load checkpoint from persistence (handle "not found" case). |
| `dump_records(records)` | Persist batch records as backup. |
| `load_records(record_class, **kwargs)` | Load batch records from persistence. |
| `dump_as_in_progress(record)` | Persist after marking in-progress. |
| `dump_as_failed_or_exhausted(record)` | Persist after marking failed/exhausted. |
| `dump_as_succeeded(record)` | Persist after marking succeeded. |

### Built-in: BaseCheckPoint

Implements the full state machine. **Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `lock_expire` | `int` | Lock expiration (passed to `timedelta()` — units are **days**). |
| `max_attempts` | `int` | Max retry attempts per record. |
| `initial_pointer` | `str \| int` | Stream starting point (for full reset). |
| `start_pointer` | `str \| int` | Current batch start. |
| `next_pointer` | `str \| int \| None` | Next batch start after commit. `None` = stream closed. |
| `batch_sequence` | `int` | Nth batch being processed. |
| `batch` | `dict[str, Tracker]` | Per-record status tracking (keyed by `record.id`). |

**Key methods (already implemented — framework internal):**

| Method | Description |
|--------|-------------|
| `mark_as_in_progress(record)` | **[Framework Internal]** Lock record, set status to `in_progress`, increment attempts. |
| `mark_as_succeeded(record)` | **[Framework Internal]** Set status to `succeeded`, release lock. |
| `mark_as_failed_or_exhausted(record, e)` | **[Framework Internal]** Set `failed` or `exhausted` based on attempt count, store error. |
| `is_ready_for_next_batch() -> bool` | **[Framework Internal]** True when all records in batch reached terminal status. |
| `update_for_new_batch(records, next_pointer)` | **[Framework Internal]** Create trackers for new batch, set next_pointer. |
| `is_record_locked(record, lock, now) -> bool` | **[Framework Internal]** Check if record is locked by another worker. |
| `get_not_succeeded_records(record_class, records)` | **[End User]** Return records that didn't succeed (for DLQ). |

### Tracker

Per-record status tracker stored in `checkpoint.batch[record_id]`.

```python
class StatusEnum(BetterIntEnum):
    pending = 0       # not started
    in_progress = 10  # processing (locked)
    failed = 20       # failed, can retry
    exhausted = 30    # failed too many times
    succeeded = 40    # done
    ignored = 50      # skipped
```

**Terminal statuses** (trigger `is_ready_for_next_batch`): `exhausted`, `succeeded`, `ignored`.

### Built-in: SimpleCheckpoint

Uses local JSON files. Two files: one for metadata, one for records backup.

```python
import unistream.api as unistream

# Create or load from disk
checkpoint = unistream.SimpleCheckpoint.load(
    checkpoint_file="/tmp/checkpoint.json",
    records_file="/tmp/records.json",
    lock_expire=900,
    max_attempts=3,
    initial_pointer=0,
)
```

### Custom Checkpoint

```python
import dataclasses
import unistream.api as unistream

@dataclasses.dataclass
class DynamoDBS3CheckPoint(unistream.BaseCheckPoint):
    table_name: str = dataclasses.field(default=REQ)
    bucket_name: str = dataclasses.field(default=REQ)
    checkpoint_id: str = dataclasses.field(default=REQ)
    bsm: "BotoSesManager" = dataclasses.field(default=REQ)

    def dump(self):
        self.bsm.dynamodb_client.put_item(
            TableName=self.table_name,
            Item=encode_dynamodb_item(self.to_dict()),
        )

    @classmethod
    def load(cls, table_name, bucket_name, checkpoint_id, bsm, **defaults):
        # try to load from DynamoDB, fallback to creating new
        ...

    def dump_records(self, records):
        self.bsm.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f"checkpoints/{self.checkpoint_id}/records.json",
            Body="\n".join(r.serialize() for r in records),
        )

    def load_records(self, record_class, **kwargs):
        resp = self.bsm.s3_client.get_object(...)
        return [record_class.deserialize(line) for line in resp["Body"].read().decode().splitlines()]

    def dump_as_in_progress(self, record):
        self.dump()

    def dump_as_failed_or_exhausted(self, record):
        self.dump()

    def dump_as_succeeded(self, record):
        self.dump()
```

---

## 5. Consumer

### Protocol (AbcConsumer)

| Method | Description |
|--------|-------------|
| `new(cls, **kwargs)` | Factory method. |
| `get_records(limit) -> (list[AbcRecord], T_POINTER)` | **[Plugin Developer]** Pull batch from stream. Subclass must implement. |
| `process_record(record)` | **[End User]** Process one record. Raise on failure. Must implement. |
| `process_failed_record(record)` | **[End User]** DLQ hook for exhausted records. Default: no-op. |

### Built-in: BaseConsumer

Implements the full consumption loop with tenacity retry.

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `record_class` | `type[AbcRecord]` | REQ | The record class for deserialization. |
| `limit` | `int` | REQ | Max records per batch. |
| `checkpoint` | `BaseCheckPoint` | REQ | Checkpoint instance. |
| `exp_backoff_multiplier` | `int` | REQ | Tenacity exponential backoff multiplier. |
| `exp_backoff_base` | `int` | REQ | Tenacity exponential backoff base. |
| `exp_backoff_min` | `int` | REQ | Min wait seconds. |
| `exp_backoff_max` | `int` | REQ | Max wait seconds. |
| `skip_error` | `bool` | REQ | If True, skip failed records; if False, raise. |
| `delay` | `int \| float` | REQ | Seconds between batch pulls. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `process_batch(verbose=False)` | **[End User API]** Pull and process one batch. |
| `run(verbose=False)` | **[End User API]** Infinite loop calling `process_batch()`. |
| `commit()` | **[Framework Internal]** Advance `start_pointer = next_pointer` and persist. |

### Built-in: SimpleConsumer

Reads from a local file written by `SimpleProducer`.

```python
import unistream.api as unistream

consumer = unistream.SimpleConsumer.new(
    record_class=MyRecord,
    path_source=Path("/tmp/output.log"),
    path_dlq=Path("/tmp/dlq.log"),
    checkpoint=unistream.SimpleCheckpoint.load(
        checkpoint_file="/tmp/checkpoint.json",
        records_file="/tmp/records.json",
    ),
    limit=100,
)

# Process one batch
consumer.process_batch(verbose=True)

# Or run forever
# consumer.run(verbose=True)
```

### Custom Consumer

```python
import dataclasses
import unistream.api as unistream

@dataclasses.dataclass
class KinesisConsumer(unistream.BaseConsumer):
    stream_name: str = dataclasses.field(default=REQ)
    shard_id: str = dataclasses.field(default=REQ)
    bsm: "BotoSesManager" = dataclasses.field(default=REQ)

    @classmethod
    def new(cls, record_class, checkpoint, stream_name, shard_id, bsm, **kwargs):
        return cls(
            record_class=record_class, checkpoint=checkpoint,
            stream_name=stream_name, shard_id=shard_id, bsm=bsm,
            limit=kwargs.get("limit", 100),
            exp_backoff_multiplier=1, exp_backoff_base=2,
            exp_backoff_min=1, exp_backoff_max=60,
            skip_error=True, delay=1,
        )

    def get_records(self, limit=None):
        # Use Kinesis GetRecords API with shard iterator
        # Return (records, next_shard_iterator)
        ...

    def process_record(self, record):
        # Your business logic here
        ...

    def process_failed_record(self, record):
        # Send to DLQ (SQS, another Kinesis stream, etc.)
        ...
```

---

## Complete End-to-End Example

```python
import dataclasses
from pathlib import Path
import unistream.api as unistream

# --- Define a record ---
@dataclasses.dataclass(frozen=True)
class EventRecord(unistream.DataClassRecord):
    event_type: str = "unknown"

# --- Producer side ---
path_sink = Path("/tmp/stream.log")
producer = unistream.SimpleProducer.new(
    buffer=unistream.FileBuffer.new(
        record_class=EventRecord,
        path_wal=Path("/tmp/producer_buffer.log"),
        max_records=5,
    ),
    retry_config=unistream.RetryConfig(exp_backoff=[1, 2, 4]),
    path_sink=path_sink,
)

for i in range(10):
    producer.put(EventRecord(event_type=f"type-{i}"), verbose=True)

# --- Consumer side ---
checkpoint = unistream.SimpleCheckpoint.load(
    checkpoint_file="/tmp/consumer_checkpoint.json",
    records_file="/tmp/consumer_records.json",
    max_attempts=3,
)

@dataclasses.dataclass
class MyConsumer(unistream.SimpleConsumer):
    def process_record(self, record):
        print(f"Processing: {record.id} - {record.event_type}")

    def process_failed_record(self, record):
        print(f"DLQ: {record.id}")

consumer = MyConsumer.new(
    record_class=EventRecord,
    path_source=path_sink,
    path_dlq=Path("/tmp/dlq.log"),
    checkpoint=checkpoint,
    limit=5,
)

consumer.process_batch(verbose=True)
```

---

## Public API Summary (`unistream.api`)

```python
import unistream.api as unistream

# Exceptions
unistream.BufferIsEmptyError
unistream.SendError
unistream.ProcessError
unistream.StreamIsClosedError

# Logger
unistream.logger

# TypeVars (for generic utility functions)
unistream.T_RECORD
unistream.T_BUFFER
unistream.T_PRODUCER
unistream.T_CHECK_POINT
unistream.T_CONSUMER
unistream.T_POINTER       # str | int
unistream.T_TRACKER

# Enums
unistream.StatusEnum       # pending=0, in_progress=10, failed=20, exhausted=30, succeeded=40, ignored=50

# Base classes
unistream.BaseRecord
unistream.BaseBuffer
unistream.RetryConfig
unistream.BaseProducer
unistream.Tracker
unistream.BaseCheckPoint
unistream.BaseConsumer

# Built-in implementations
unistream.DataClassRecord
unistream.T_DATA_CLASS_RECORD
unistream.FileBuffer
unistream.SimpleProducer
unistream.SimpleCheckpoint
unistream.SimpleConsumer
```

---

## Dependency Note

- **Core:** `unistream` depends only on `tenacity` and `func_args`.
- **Records:** Use `func_args.api.BaseFrozenModel` for frozen dataclass records.
- **Framework classes:** Use `func_args.api.BaseModel` and `func_args.api.REQ` for required fields.
- **Python:** Requires >= 3.10.
