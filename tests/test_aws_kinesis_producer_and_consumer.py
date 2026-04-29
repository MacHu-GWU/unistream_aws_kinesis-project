# -*- coding: utf-8 -*-

"""
Test AWS Kinesis producer and consumer using moto mock.
"""

import typing as T
import shutil
import dataclasses
from pathlib import Path

import boto3

from unistream.buffers.file_buffer import FileBuffer
from unistream.checkpoints.simple import SimpleCheckpoint
from unistream.producer import RetryConfig

from unistream_aws_kinesis.api import (
    KinesisRecord,
    KinesisGetRecordsResponseRecord,
    AwsKinesisStreamProducer,
    KinesisStreamShard,
    AwsKinesisStreamConsumer,
)
from unistream_aws_kinesis.tests.mock_aws import BaseMockAwsTest


# --- Test data directory ---
dir_here = Path(__file__).absolute().parent
dir_data = dir_here / "test_aws_kinesis_data"

# --- Stream config ---
STREAM_NAME = "test-kinesis-stream"
SHARD_COUNT = 1


@dataclasses.dataclass(frozen=True)
class MyRecord(KinesisRecord):
    """A simple test record with a value field."""

    value: int = dataclasses.field(default=0)


@dataclasses.dataclass
class MyConsumer(AwsKinesisStreamConsumer):
    """
    A test consumer that writes processed records to a target file
    and failed records to a DLQ file.
    """

    path_target: Path = dataclasses.field(default=None)
    path_dlq: Path = dataclasses.field(default=None)

    def process_record(self, record: MyRecord) -> str:
        s = record.serialize()
        with self.path_target.open("a") as f:
            f.write(f"{s}\n")
        return s

    def process_failed_record(self, record: MyRecord) -> str:
        s = record.serialize()
        with self.path_dlq.open("a") as f:
            f.write(f"{s}\n")
        return s


class TestKinesisRecord:
    def test_serialize_deserialize(self):
        record = MyRecord(id="rec-1", value=42)
        serialized = record.serialize()
        deserialized = MyRecord.deserialize(serialized)
        assert deserialized.id == "rec-1"
        assert deserialized.value == 42

    def test_to_put_record_data_and_from_get_record_data(self):
        record = MyRecord(id="rec-1", value=99)
        data = record.to_put_record_data()
        assert isinstance(data, bytes)
        restored = MyRecord.from_get_record_data(data)
        assert restored.id == "rec-1"
        assert restored.value == 99

    def test_partition_key_defaults_to_id(self):
        record = MyRecord(id="pk-test", value=1)
        assert record.partition_key == "pk-test"


class TestKinesisGetRecordsResponseRecord:
    def test_from_get_records_response(self):
        res = {
            "Records": [
                {
                    "SequenceNumber": "seq-1",
                    "ApproximateArrivalTimestamp": "2024-01-01T00:00:00Z",
                    "Data": b"dGVzdA==",
                    "PartitionKey": "pk-1",
                    "EncryptionType": "NONE",
                },
            ],
        }
        records = KinesisGetRecordsResponseRecord.from_get_records_response(res)
        assert len(records) == 1
        assert records[0].sequence_number == "seq-1"
        assert records[0].partition_key == "pk-1"

    def test_from_get_records_response_empty(self):
        res = {"Records": []}
        records = KinesisGetRecordsResponseRecord.from_get_records_response(res)
        assert len(records) == 0


class TestKinesisStreamShard:
    def test_from_list_shards_response(self):
        res = {
            "Shards": [
                {
                    "ShardId": "shardId-000000000000",
                    "HashKeyRange": {
                        "StartingHashKey": "0",
                        "EndingHashKey": "340282366920938463463374607431768211455",
                    },
                    "SequenceNumberRange": {
                        "StartingSequenceNumber": "49000000000000000000000000000000000000000000000000000000",
                    },
                },
            ]
        }
        shards = KinesisStreamShard.from_list_shards_response(res)
        assert len(shards) == 1
        assert shards[0].ShardId == "shardId-000000000000"


class TestAwsKinesisProducer(BaseMockAwsTest):
    use_mock = True

    @classmethod
    def setup_class_post_hook(cls):
        cls.kinesis_client = cls.boto_ses.client("kinesis")
        cls.kinesis_client.create_stream(
            StreamName=STREAM_NAME,
            ShardCount=SHARD_COUNT,
        )
        # Clean up test data directory
        shutil.rmtree(dir_data, ignore_errors=True)
        dir_data.mkdir(parents=True, exist_ok=True)

    def test_producer_send(self):
        """Test that producer can send records to Kinesis."""
        path_wal = dir_data / "producer_test_send.log"
        buffer = FileBuffer.new(
            record_class=MyRecord,
            path_wal=path_wal,
            max_records=3,
        )
        producer = AwsKinesisStreamProducer.new(
            buffer=buffer,
            retry_config=RetryConfig(exp_backoff=[1, 2, 4]),
            kinesis_client=self.kinesis_client,
            stream_name=STREAM_NAME,
        )

        # Put records — buffer accumulates until max_records is reached
        for i in range(1, 6):
            producer.put(MyRecord(id=f"id_{i}", value=i), verbose=True)

    def test_producer_put_and_send_cycle(self):
        """Test a full put-buffer-send cycle."""
        path_wal = dir_data / "producer_test_cycle.log"
        buffer = FileBuffer.new(
            record_class=MyRecord,
            path_wal=path_wal,
            max_records=2,
        )
        producer = AwsKinesisStreamProducer.new(
            buffer=buffer,
            retry_config=RetryConfig(exp_backoff=[1, 2, 4]),
            kinesis_client=self.kinesis_client,
            stream_name=STREAM_NAME,
        )

        # Put 4 records with max_records=2. This should trigger 2 sends.
        for i in range(1, 5):
            producer.put(MyRecord(id=f"cycle_{i}", value=i * 10), verbose=True)


class TestAwsKinesisConsumer(BaseMockAwsTest):
    use_mock = True

    @classmethod
    def setup_class_post_hook(cls):
        cls.kinesis_client = cls.boto_ses.client("kinesis")
        cls.kinesis_client.create_stream(
            StreamName=STREAM_NAME,
            ShardCount=SHARD_COUNT,
        )

        # Clean up test data directory
        shutil.rmtree(dir_data, ignore_errors=True)
        dir_data.mkdir(parents=True, exist_ok=True)

        # Get shard info
        res = cls.kinesis_client.list_shards(StreamName=STREAM_NAME)
        cls.shards = KinesisStreamShard.from_list_shards_response(res)
        cls.shard_id = cls.shards[0].ShardId

    def _put_records_to_stream(self, records: T.List[MyRecord]):
        """Helper to put records directly to Kinesis for consumer testing."""
        self.kinesis_client.put_records(
            Records=[
                dict(
                    Data=record.to_put_record_data(),
                    PartitionKey=record.partition_key,
                )
                for record in records
            ],
            StreamName=STREAM_NAME,
        )

    def test_consumer_get_and_process_records(self):
        """Test that consumer can get and process records from Kinesis."""
        # Put some records to the stream first
        records_to_send = [
            MyRecord(id=f"cons_{i}", value=i * 100)
            for i in range(1, 4)
        ]
        self._put_records_to_stream(records_to_send)

        # Get a shard iterator
        res = self.kinesis_client.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=self.shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        shard_iterator = res["ShardIterator"]

        # Create checkpoint
        path_checkpoint = dir_data / "consumer_test.checkpoint.json"
        path_records = dir_data / "consumer_test.records.json"
        path_target = dir_data / "consumer_test.target.json"
        path_dlq = dir_data / "consumer_test.dlq.json"

        checkpoint = SimpleCheckpoint.load(
            checkpoint_file=str(path_checkpoint),
            records_file=str(path_records),
            lock_expire=900,
            max_attempts=3,
            initial_pointer=shard_iterator,
            start_pointer=shard_iterator,
        )

        consumer = MyConsumer.new(
            record_class=MyRecord,
            consumer_id=f"{STREAM_NAME}-{self.shard_id}",
            kinesis_client=self.kinesis_client,
            stream_name=STREAM_NAME,
            shard_id=self.shard_id,
            checkpoint=checkpoint,
            limit=10,
            additional_kwargs=dict(
                path_target=path_target,
                path_dlq=path_dlq,
            ),
        )

        # Process one batch
        consumer.process_batch(verbose=True)

        # Verify records were processed
        assert path_target.exists()
        lines = path_target.read_text().strip().split("\n")
        assert len(lines) == 3
        for line in lines:
            record = MyRecord.deserialize(line)
            assert record.id.startswith("cons_")

    def test_consumer_empty_stream(self):
        """Test consumer handles empty stream gracefully."""
        # Use a fresh stream with no records — use LATEST iterator type
        res = self.kinesis_client.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=self.shard_id,
            ShardIteratorType="LATEST",
        )
        shard_iterator = res["ShardIterator"]

        path_checkpoint = dir_data / "consumer_empty.checkpoint.json"
        path_records = dir_data / "consumer_empty.records.json"
        path_target = dir_data / "consumer_empty.target.json"
        path_dlq = dir_data / "consumer_empty.dlq.json"

        checkpoint = SimpleCheckpoint.load(
            checkpoint_file=str(path_checkpoint),
            records_file=str(path_records),
            lock_expire=900,
            max_attempts=3,
            initial_pointer=shard_iterator,
            start_pointer=shard_iterator,
        )

        consumer = MyConsumer.new(
            record_class=MyRecord,
            consumer_id=f"{STREAM_NAME}-{self.shard_id}-empty",
            kinesis_client=self.kinesis_client,
            stream_name=STREAM_NAME,
            shard_id=self.shard_id,
            checkpoint=checkpoint,
            limit=10,
            additional_kwargs=dict(
                path_target=path_target,
                path_dlq=path_dlq,
            ),
        )

        # Should handle empty batch gracefully
        consumer.process_batch(verbose=True)

        # Target file should not exist (no records processed)
        assert not path_target.exists()


class TestProducerConsumerIntegration(BaseMockAwsTest):
    """End-to-end test: produce records then consume them."""

    use_mock = True

    @classmethod
    def setup_class_post_hook(cls):
        cls.kinesis_client = cls.boto_ses.client("kinesis")
        cls.kinesis_client.create_stream(
            StreamName=STREAM_NAME,
            ShardCount=SHARD_COUNT,
        )

        shutil.rmtree(dir_data, ignore_errors=True)
        dir_data.mkdir(parents=True, exist_ok=True)

        res = cls.kinesis_client.list_shards(StreamName=STREAM_NAME)
        cls.shards = KinesisStreamShard.from_list_shards_response(res)
        cls.shard_id = cls.shards[0].ShardId

    def test_produce_then_consume(self):
        """Produce N records then consume and verify them."""
        # --- Producer ---
        path_wal = dir_data / "integ_producer.log"
        buffer = FileBuffer.new(
            record_class=MyRecord,
            path_wal=path_wal,
            max_records=3,
        )
        producer = AwsKinesisStreamProducer.new(
            buffer=buffer,
            retry_config=RetryConfig(exp_backoff=[1, 2, 4]),
            kinesis_client=self.kinesis_client,
            stream_name=STREAM_NAME,
        )

        n_records = 6
        for i in range(1, n_records + 1):
            producer.put(MyRecord(id=f"integ_{i}", value=i), verbose=True)

        # --- Consumer ---
        res = self.kinesis_client.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=self.shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        shard_iterator = res["ShardIterator"]

        path_checkpoint = dir_data / "integ_consumer.checkpoint.json"
        path_records = dir_data / "integ_consumer.records.json"
        path_target = dir_data / "integ_consumer.target.json"
        path_dlq = dir_data / "integ_consumer.dlq.json"

        checkpoint = SimpleCheckpoint.load(
            checkpoint_file=str(path_checkpoint),
            records_file=str(path_records),
            lock_expire=900,
            max_attempts=3,
            initial_pointer=shard_iterator,
            start_pointer=shard_iterator,
        )

        consumer = MyConsumer.new(
            record_class=MyRecord,
            consumer_id=f"{STREAM_NAME}-{self.shard_id}-integ",
            kinesis_client=self.kinesis_client,
            stream_name=STREAM_NAME,
            shard_id=self.shard_id,
            checkpoint=checkpoint,
            limit=100,
            additional_kwargs=dict(
                path_target=path_target,
                path_dlq=path_dlq,
            ),
        )

        # Process batches until all records are consumed
        consumer.process_batch(verbose=True)

        # Verify
        assert path_target.exists()
        lines = path_target.read_text().strip().split("\n")
        consumed_ids = {MyRecord.deserialize(line).id for line in lines}
        expected_ids = {f"integ_{i}" for i in range(1, n_records + 1)}
        assert consumed_ids == expected_ids


if __name__ == "__main__":
    from unistream_aws_kinesis.tests import run_cov_test

    run_cov_test(
        __file__,
        "unistream_aws_kinesis",
        is_folder=True,
        preview=False,
    )
