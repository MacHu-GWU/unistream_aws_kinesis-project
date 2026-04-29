# -*- coding: utf-8 -*-

"""
Integration test: produce records then consume them in the same moto process.

This is necessary because moto mocks are process-local — a producer and
consumer cannot run in separate threads/processes and share the same mock
Kinesis stream.
"""

import shutil
import dataclasses
from pathlib import Path

from unistream.api import FileBuffer
from unistream.api import SimpleCheckpoint
from unistream.api import RetryConfig

from unistream_aws_kinesis.api import KinesisRecord
from unistream_aws_kinesis.api import AwsKinesisStreamProducer
from unistream_aws_kinesis.api import KinesisStreamShard
from unistream_aws_kinesis.api import AwsKinesisStreamConsumer
from unistream_aws_kinesis.tests.mock_aws import BaseMockAwsTest


# --- Test data directory ---
dir_here = Path(__file__).absolute().parent
dir_data = dir_here / "test_aws_kinesis_producer_and_consumer_data"

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
