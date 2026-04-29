# -*- coding: utf-8 -*-

"""
Test KinesisRecord data conversion and AwsKinesisStreamProducer.
"""

import shutil
import dataclasses
from pathlib import Path

from unistream.api import FileBuffer
from unistream.api import RetryConfig

from unistream_aws_kinesis.api import KinesisRecord
from unistream_aws_kinesis.api import AwsKinesisStreamProducer
from unistream_aws_kinesis.tests.mock_aws import BaseMockAwsTest


# --- Test data directory ---
dir_here = Path(__file__).absolute().parent
dir_data = dir_here / "test_producer_data"

# --- Stream config ---
STREAM_NAME = "test-kinesis-stream"
SHARD_COUNT = 1


@dataclasses.dataclass(frozen=True)
class MyRecord(KinesisRecord):
    """A simple test record with a value field."""

    value: int = dataclasses.field(default=0)


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


if __name__ == "__main__":
    from unistream_aws_kinesis.tests import run_cov_test

    run_cov_test(
        __file__,
        "unistream_aws_kinesis.producer",
        preview=False,
    )
